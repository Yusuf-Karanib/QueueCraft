/**
 * QueueCraft — core polling engine
 *
 * Ties the pieces together: long-polls SQS for work, gates concurrency with the
 * Semaphore, enforces exactly-once execution with the IdempotencyStore, and
 * commits or retries each message based on the handler's outcome.
 *
 *   receive -> acquireLock -> handler
 *                              |-- ok   --> deleteMessage + markComplete
 *                              `-- err  --> releaseLock (SQS redelivers)
 */
import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  type Message,
} from "@aws-sdk/client-sqs";
import type { WorkerOptions } from "./types";
import type { Semaphore } from "./semaphore";
import type { IdempotencyStore } from "./idempotency";

/** SQS hard limit on messages returned per `ReceiveMessage` call. */
const MAX_SQS_BATCH = 10;

/**
 * User-supplied unit of work. Receives the raw SQS message so the caller owns
 * body parsing/validation. Throwing (or rejecting) signals failure, which
 * triggers a lease release and SQS redelivery.
 */
export type JobHandler = (message: Message) => Promise<void> | void;

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
}

export class QueueCraftPoller {
  private readonly sqs: SQSClient;
  private readonly semaphore: Semaphore;
  private readonly idempotency: IdempotencyStore;
  private readonly queueUrl: string;
  private readonly handler: JobHandler;
  private readonly onError?: (error: unknown, message?: Message) => void;

  private readonly maxConcurrency: number;
  private readonly pollIntervalMs: number;
  private readonly waitTimeSeconds: number;
  private readonly batchSize: number;

  private running = false;
  private readonly inflight = new Set<Promise<void>>();
  private abortController?: AbortController;

  constructor(options: QueueCraftPollerOptions) {
    this.sqs = options.sqsClient;
    this.semaphore = options.semaphore;
    this.idempotency = options.idempotency;
    this.queueUrl = options.queueUrl;
    this.handler = options.handler;
    this.onError = options.onError;

    this.maxConcurrency = options.worker.concurrency;
    this.pollIntervalMs = options.worker.pollIntervalMs;
    this.waitTimeSeconds = options.worker.waitTimeSeconds ?? 20;
    this.batchSize = options.worker.batchSize ?? MAX_SQS_BATCH;
  }

  /** Whether the poll loop is currently active. */
  get isRunning(): boolean {
    return this.running;
  }

  /**
   * Run the continuous poll loop until `stop()` is called. Resolves once the
   * loop has exited and all in-flight jobs have drained.
   */
  async start(): Promise<void> {
    if (this.running) return;
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
        this.onError?.(err);
        await this.sleep(this.pollIntervalMs);
        continue;
      }

      // Dispatch without awaiting so the loop keeps the pipeline full up to the
      // concurrency ceiling; each job owns its own semaphore slot.
      for (const message of messages) {
        this.dispatch(message);
      }
    }

    await this.drain();
  }

  /** Signal the loop to stop, interrupt any in-flight long poll, and drain. */
  async stop(): Promise<void> {
    this.running = false;
    this.abortController?.abort();
    await this.drain();
  }

  /** Free slots = ceiling minus in-use, clamped to the SQS batch limit. */
  private availableCapacity(): number {
    const inUse = this.semaphore.activeCount + this.semaphore.pendingCount;
    const free = this.maxConcurrency - inUse;
    return Math.max(0, Math.min(free, this.batchSize, MAX_SQS_BATCH));
  }

  private async receive(max: number): Promise<Message[]> {
    this.abortController = new AbortController();
    const result = await this.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: max,
        WaitTimeSeconds: this.waitTimeSeconds,
      }),
      { abortSignal: this.abortController.signal },
    );
    return result.Messages ?? [];
  }

  private dispatch(message: Message): void {
    const task = this.runWithSlot(message);
    this.inflight.add(task);
    void task.finally(() => this.inflight.delete(task));
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
      this.onError?.(
        new Error("SQS message missing MessageId or ReceiptHandle"),
        message,
      );
      return;
    }

    // (4) Idempotency gate: exactly one worker wins the lease for this id.
    const acquired = await this.idempotency.acquireLock(messageId);
    if (!acquired) {
      // In-flight/completed/failed elsewhere. The lease owner drives its
      // lifecycle; leave this copy for SQS to reconcile.
      return;
    }

    try {
      // (5) Execute the provided handler under the lease.
      await this.handler(message);
    } catch (err) {
      // (7) Failure: drop the lease and don't delete, so the message becomes
      //     visible again and SQS retries (subject to the queue's redrive policy).
      await this.safeRelease(messageId);
      this.onError?.(err, message);
      return;
    }

    // (6) Success: commit. Delete from SQS *before* markComplete so a crash in
    //     this window can't leave a permanent COMPLETED tombstone blocking the
    //     message — the PENDING lease simply expires via its TTL instead.
    try {
      await this.deleteMessage(receiptHandle);
      await this.idempotency.markComplete(messageId);
    } catch (err) {
      // Job already ran; the lease will TTL out. Surface for observability.
      this.onError?.(err, message);
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

  private async safeRelease(messageId: string): Promise<void> {
    try {
      await this.idempotency.releaseLock(messageId);
    } catch (err) {
      this.onError?.(err);
    }
  }

  private async drain(): Promise<void> {
    await Promise.allSettled([...this.inflight]);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}