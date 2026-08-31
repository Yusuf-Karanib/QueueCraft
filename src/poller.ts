/**
 * QueueCraft — core polling engine
 *
 * Ties the pieces together: long-polls SQS for work, gates concurrency with the
 * Semaphore, suppresses duplicate logical jobs with the IdempotencyStore, and
 * commits or retries each message based on the handler's outcome.
 *
 *   receive -> acquireLock -> handler
 *                              |-- ok   --> deleteMessage + markComplete
 *                              `-- err  --> releaseLock (SQS redelivers)
 */
import {
  ChangeMessageVisibilityCommand,
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  type Message,
} from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";
import type { WorkerOptions } from "./types";
import type { Semaphore } from "./semaphore";
import type { ExecutionLease, IdempotencyStore } from "./idempotency";
import { IDEMPOTENCY_ATTRIBUTE } from "./publisher";

/** SQS hard limit on messages returned per `ReceiveMessage` call. */
const MAX_SQS_BATCH = 10;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 30_000;
const MAX_ABORT_CLEANUP_MS = 1_000;

/**
 * User-supplied unit of work. Receives the raw SQS message so the caller owns
 * body parsing/validation. Throwing (or rejecting) signals failure, which
 * triggers a lease release and SQS redelivery.
 */
export interface JobContext {
  /** Stable logical-job key selected by the producer. */
  readonly idempotencyKey: string;

  /** SQS receive count for this transport message. */
  readonly attempt: number;

  /** Aborted when QueueCraft can no longer prove it owns the job lease. */
  readonly signal: AbortSignal;
}

export type JobHandler = (
  message: Message,
  context: JobContext,
) => Promise<void> | void;

export interface QueueCraftPollerOptions {
  readonly sqsClient: SQSClient;
  readonly semaphore: Semaphore;
  readonly idempotency: IdempotencyStore;

  /** URL of the SQS queue to poll. */
  readonly queueUrl: string;

  /** Business logic invoked for each successfully leased message. */
  readonly handler: JobHandler;

  /**
   * Concurrency + polling tuning. `concurrency` MUST match the max used to
   * construct the injected Semaphore — it is the capacity ceiling this poller
   * checks before fetching.
   */
  readonly worker: WorkerOptions;

  /** Optional observer for handler/commit/receive errors. Never throws. */
  readonly onError?: (error: unknown, message?: Message) => void;

  /** Message attribute containing the stable application idempotency key. */
  readonly idempotencyAttribute?: string;
}

export class QueueCraftPoller {
  private readonly sqs: SQSClient;
  private readonly semaphore: Semaphore;
  private readonly idempotency: IdempotencyStore;
  private readonly queueUrl: string;
  private readonly handler: JobHandler;
  private readonly onError?: (error: unknown, message?: Message) => void;
  private readonly idempotencyAttribute: string;

  private readonly maxConcurrency: number;
  private readonly pollIntervalMs: number;
  private readonly waitTimeSeconds: number;
  private readonly batchSize: number;
  private readonly visibilityTimeoutSeconds: number;
  private readonly heartbeatIntervalMs: number;
  private readonly shutdownTimeoutMs: number;

  private running = false;
  private readonly inflight = new Set<Promise<void>>();
  private readonly activeExecutions = new Map<
    AbortController,
    AbortController
  >();
  private abortController?: AbortController;
  private activeReceive?: Promise<Message[]>;
  private sleepController?: AbortController;
  private shutdownPromise?: Promise<void>;

  constructor(options: QueueCraftPollerOptions) {
    this.sqs = options.sqsClient;
    this.semaphore = options.semaphore;
    this.idempotency = options.idempotency;
    this.queueUrl = options.queueUrl;
    this.handler = options.handler;
    this.onError = options.onError;
    this.idempotencyAttribute =
      options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;

    this.maxConcurrency = options.worker.concurrency;
    this.pollIntervalMs = options.worker.pollIntervalMs;
    this.waitTimeSeconds = options.worker.waitTimeSeconds ?? 20;
    this.batchSize = options.worker.batchSize ?? MAX_SQS_BATCH;
    this.visibilityTimeoutSeconds =
      options.worker.visibilityTimeoutSeconds ?? 60;
    this.heartbeatIntervalMs =
      options.worker.heartbeatIntervalMs ??
      Math.floor((this.visibilityTimeoutSeconds * 1000) / 2);
    this.shutdownTimeoutMs =
      options.worker.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;

    this.validateOptions(options.worker.concurrency);
  }

  /** Whether the poll loop is currently active. */
  get isRunning(): boolean {
    return this.running;
  }

  /**
   * Run the continuous poll loop until `stop()` is called. Resolves once the
   * loop has exited and active jobs have drained or reached the configured
   * shutdown timeout.
   */
  async start(): Promise<void> {
    if (this.running) return;
    this.shutdownPromise = undefined;
    this.running = true;

    while (this.running) {
      // (2) Only fetch what we have room to process, so we never pull messages
      //     whose visibility timeout would lapse while they sit unhandled.
      const capacity = this.availableCapacity();
      if (capacity <= 0) {
        await this.sleep(this.pollIntervalMs);
        continue;
      }

      let messages: Message[];
      try {
        messages = await this.receive(capacity);
      } catch (err) {
        if (!this.running) break; // long-poll aborted by stop()
        this.reportError(err);
        await this.sleep(this.pollIntervalMs);
        continue;
      }

      if (!this.running) {
        await this.returnUnstartedMessages(messages);
        break;
      }

      // Dispatch without awaiting so the loop keeps the pipeline full up to the
      // concurrency ceiling; each job owns its own semaphore slot.
      for (const message of messages) {
        this.dispatch(message);
      }
    }

    await this.shutdownAndDrain();
  }

  /**
   * Stop polling, allow active jobs to finish within the configured grace
   * period, then cancel them and return without waiting forever.
   */
  async stop(): Promise<void> {
    this.running = false;
    this.abortController?.abort();
    this.sleepController?.abort();
    await this.activeReceive?.catch(() => undefined);
    await this.shutdownAndDrain();
  }

  /** Free slots = ceiling minus in-use, clamped to the SQS batch limit. */
  private availableCapacity(): number {
    const inUse = this.semaphore.activeCount + this.semaphore.pendingCount;
    const free = this.maxConcurrency - inUse;
    return Math.max(0, Math.min(free, this.batchSize, MAX_SQS_BATCH));
  }

  private async receive(max: number): Promise<Message[]> {
    this.abortController = new AbortController();
    const request = this.sqs
      .send(
        new ReceiveMessageCommand({
          QueueUrl: this.queueUrl,
          MaxNumberOfMessages: max,
          WaitTimeSeconds: this.waitTimeSeconds,
          VisibilityTimeout: this.visibilityTimeoutSeconds,
          MessageAttributeNames: [this.idempotencyAttribute],
          MessageSystemAttributeNames: ["ApproximateReceiveCount"],
        }),
        { abortSignal: this.abortController.signal },
      )
      .then((result) => result.Messages ?? []);

    this.activeReceive = request;
    try {
      return await request;
    } finally {
      if (this.activeReceive === request) {
        this.activeReceive = undefined;
      }
    }
  }

  private dispatch(message: Message): void {
    const task = this.runWithSlot(message).catch((error) => {
      this.reportError(error, message);
    });
    this.inflight.add(task);
    void task.then(() => this.inflight.delete(task));
  }

  /** Hold a concurrency slot for the full lifetime of one message. */
  private async runWithSlot(message: Message): Promise<void> {
    await this.semaphore.acquire();
    try {
      await this.process(message);
    } finally {
      this.semaphore.release();
    }
  }

  private async process(message: Message): Promise<void> {
    const messageId = message.MessageId;
    const receiptHandle = message.ReceiptHandle;

    // Nothing safe to act on — let visibility lapse so SQS redelivers/DLQs it.
    if (!messageId || !receiptHandle) {
      this.reportError(
        new Error("SQS message missing MessageId or ReceiptHandle"),
        message,
      );
      return;
    }

    const idempotencyKey =
      message.MessageAttributes?.[this.idempotencyAttribute]?.StringValue ??
      messageId;
    const ownerId = randomUUID();

    // One worker owns an active lease. Expired leases can be taken over.
    const acquisition = await this.idempotency.acquireLock(
      idempotencyKey,
      ownerId,
    );

    if (acquisition.status === "completed") {
      // The logical job already committed. Acknowledge this transport-level
      // duplicate so it does not circulate until the DLQ.
      await this.deleteMessage(receiptHandle);
      return;
    }

    if (acquisition.status !== "acquired") {
      // Another worker still owns it, or it is terminally failed. Do not delete
      // the message; queue redrive policy remains responsible for failures.
      return;
    }

    const lease = acquisition.lease;
    const handlerController = new AbortController();
    const heartbeatController = new AbortController();
    this.activeExecutions.set(handlerController, heartbeatController);
    let heartbeatError: unknown;

    const heartbeat = this.runHeartbeat(
      lease,
      receiptHandle,
      heartbeatController.signal,
    ).catch((error) => {
      heartbeatError = error;
      handlerController.abort(error);
      this.reportError(error, message);
    });

    const attempt = this.receiveCount(message);
    let handlerError: unknown;

    try {
      await this.handler(message, {
        idempotencyKey,
        attempt,
        signal: handlerController.signal,
      });
    } catch (error) {
      handlerError = error;
    } finally {
      heartbeatController.abort();
      await heartbeat;
      this.activeExecutions.delete(handlerController);
    }

    if (heartbeatError !== undefined) {
      // Ownership is uncertain. Never settle with a possibly stale receipt
      // handle or release a lease that another worker may now own.
      return;
    }

    if (handlerError !== undefined) {
      // Drop the owned lease and leave the message for SQS retry/redrive.
      await this.safeRelease(lease);
      this.reportError(handlerError, message);
      return;
    }

    if (handlerController.signal.aborted) {
      await this.safeRelease(lease);
      this.reportError(
        handlerController.signal.reason ??
          new Error("QueueCraft handler cancelled during shutdown."),
        message,
      );
      return;
    }

    // Commit the durable completion record before acknowledging SQS. If the
    // delete fails or the worker crashes, redelivery sees COMPLETED and safely
    // acknowledges the duplicate without running the handler again.
    try {
      await this.idempotency.markComplete(lease);
      await this.deleteMessage(receiptHandle);
    } catch (err) {
      // Do not release the lease: the handler already returned successfully.
      // A retry can observe COMPLETED or take over only after an expired lease.
      this.reportError(err, message);
    }
  }

  private async deleteMessage(receiptHandle: string): Promise<void> {
    await this.sqs.send(
      new DeleteMessageCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  private async changeVisibility(
    receiptHandle: string,
    visibilityTimeout = this.visibilityTimeoutSeconds,
  ): Promise<void> {
    await this.sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: visibilityTimeout,
      }),
    );
  }

  private async runHeartbeat(
    lease: ExecutionLease,
    receiptHandle: string,
    signal: AbortSignal,
  ): Promise<void> {
    while (await this.waitForHeartbeat(signal)) {
      await this.idempotency.renewLease(lease);
      await this.changeVisibility(receiptHandle);
    }
  }

  private waitForHeartbeat(signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) return Promise.resolve(false);

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        signal.removeEventListener("abort", onAbort);
        resolve(true);
      }, this.heartbeatIntervalMs);

      const onAbort = () => {
        clearTimeout(timeout);
        resolve(false);
      };

      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  private receiveCount(message: Message): number {
    const parsed = Number(message.Attributes?.ApproximateReceiveCount ?? "1");
    return Number.isInteger(parsed) && parsed > 0 ? parsed : 1;
  }

  private async returnUnstartedMessages(messages: Message[]): Promise<void> {
    const returns = messages.flatMap((message) =>
      message.ReceiptHandle
        ? [this.changeVisibility(message.ReceiptHandle, 0)]
        : [],
    );
    const results = await Promise.allSettled(returns);
    for (const result of results) {
      if (result.status === "rejected") {
        this.reportError(result.reason);
      }
    }
  }

  private async safeRelease(lease: ExecutionLease): Promise<void> {
    try {
      await this.idempotency.releaseLock(lease);
    } catch (err) {
      this.reportError(err);
    }
  }

  private validateOptions(concurrency: number): void {
    this.assertIntegerInRange(concurrency, "concurrency", 1, Number.MAX_SAFE_INTEGER);
    if (concurrency !== this.semaphore.capacity) {
      throw new RangeError(
        "worker.concurrency must match the injected Semaphore capacity.",
      );
    }

    this.assertIntegerInRange(
      this.pollIntervalMs,
      "pollIntervalMs",
      0,
      Number.MAX_SAFE_INTEGER,
    );
    this.assertIntegerInRange(this.waitTimeSeconds, "waitTimeSeconds", 0, 20);
    this.assertIntegerInRange(this.batchSize, "batchSize", 1, MAX_SQS_BATCH);
    this.assertIntegerInRange(
      this.visibilityTimeoutSeconds,
      "visibilityTimeoutSeconds",
      1,
      43_200,
    );
    this.assertIntegerInRange(
      this.heartbeatIntervalMs,
      "heartbeatIntervalMs",
      1,
      this.visibilityTimeoutSeconds * 1000 - 1,
    );
    this.assertIntegerInRange(
      this.shutdownTimeoutMs,
      "shutdownTimeoutMs",
      0,
      Number.MAX_SAFE_INTEGER,
    );
  }

  private assertIntegerInRange(
    value: number,
    name: string,
    minimum: number,
    maximum: number,
  ): void {
    if (!Number.isInteger(value) || value < minimum || value > maximum) {
      throw new RangeError(
        `${name} must be an integer between ${minimum} and ${maximum}.`,
      );
    }
  }

  private reportError(error: unknown, message?: Message): void {
    try {
      this.onError?.(error, message);
    } catch {
      // Observability callbacks must never crash the worker runtime.
    }
  }

  private async drain(): Promise<void> {
    await Promise.allSettled([...this.inflight]);
  }

  private shutdownAndDrain(): Promise<void> {
    this.shutdownPromise ??= this.performBoundedDrain();
    return this.shutdownPromise;
  }

  private async performBoundedDrain(): Promise<void> {
    if (this.inflight.size === 0) return;

    const drainedNaturally = await this.drainWithin(this.shutdownTimeoutMs);
    if (drainedNaturally) return;

    const reason = new Error(
      `QueueCraft graceful shutdown timed out after ${this.shutdownTimeoutMs}ms.`,
    );
    for (const [handlerController, heartbeatController] of
      this.activeExecutions) {
      heartbeatController.abort(reason);
      handlerController.abort(reason);
    }

    const cleanupMs = Math.min(
      MAX_ABORT_CLEANUP_MS,
      Math.max(10, this.shutdownTimeoutMs),
    );
    await this.drainWithin(cleanupMs);
  }

  private drainWithin(timeoutMs: number): Promise<boolean> {
    if (this.inflight.size === 0) return Promise.resolve(true);

    return new Promise((resolve) => {
      let settled = false;
      const timeout = setTimeout(() => {
        settled = true;
        resolve(false);
      }, timeoutMs);

      void this.drain().then(() => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve(true);
      });
    });
  }

  private sleep(ms: number): Promise<void> {
    this.sleepController = new AbortController();
    const controller = this.sleepController;

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        controller.signal.removeEventListener("abort", onAbort);
        if (this.sleepController === controller) {
          this.sleepController = undefined;
        }
        resolve();
      }, ms);

      const onAbort = () => {
        clearTimeout(timeout);
        if (this.sleepController === controller) {
          this.sleepController = undefined;
        }
        resolve();
      };

      controller.signal.addEventListener("abort", onAbort, { once: true });
    });
  }
}
