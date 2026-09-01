import type { Message } from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";
import type { ExecutionLease, IdempotencyStore } from "./idempotency";
import type { JobContext, JobHandler, QueueCraftEvent } from "./poller";
import { IDEMPOTENCY_ATTRIBUTE } from "./publisher";
import { Semaphore } from "./semaphore";
import {
  runInstrumentedJob,
  type QueueCraftJobInstrumentation,
} from "./instrumentation";

export interface LambdaSqsMessageAttribute {
  readonly dataType: string;
  readonly stringValue?: string;
  readonly binaryValue?: string;
}

export interface LambdaSqsRecord {
  readonly messageId: string;
  readonly receiptHandle?: string;
  readonly body: string;
  readonly attributes?: Readonly<Record<string, string>>;
  readonly messageAttributes?: Readonly<
    Record<string, LambdaSqsMessageAttribute>
  >;
}

export interface LambdaSqsEvent {
  readonly Records: readonly LambdaSqsRecord[];
}

export interface LambdaBatchItemFailure {
  readonly itemIdentifier: string;
}

export interface LambdaSqsBatchResponse {
  readonly batchItemFailures: readonly LambdaBatchItemFailure[];
}

export interface QueueCraftLambdaProcessorOptions {
  readonly idempotency: IdempotencyStore;
  readonly handler: JobHandler;
  readonly instrumentation?: QueueCraftJobInstrumentation;
  readonly concurrency?: number;
  readonly idempotencyAttribute?: string;
  readonly onError?: (error: unknown, record?: LambdaSqsRecord) => void;
  readonly onEvent?: (event: QueueCraftEvent) => void;
}

export interface LambdaProcessOptions {
  readonly signal?: AbortSignal;
}

/**
 * Processes SQS event-source batches inside AWS Lambda while preserving
 * QueueCraft's DynamoDB duplicate protection. Lambda remains responsible for
 * receiving, deleting, retrying, and redriving SQS messages.
 */
export class QueueCraftLambdaProcessor {
  private readonly idempotency: IdempotencyStore;
  private readonly handler: JobHandler;
  private readonly instrumentation?: QueueCraftJobInstrumentation;
  private readonly semaphore: Semaphore;
  private readonly idempotencyAttribute: string;
  private readonly onError?: (
    error: unknown,
    record?: LambdaSqsRecord,
  ) => void;
  private readonly onEvent?: (event: QueueCraftEvent) => void;

  constructor(options: QueueCraftLambdaProcessorOptions) {
    const concurrency = options.concurrency ?? 10;
    this.idempotency = options.idempotency;
    this.handler = options.handler;
    this.instrumentation = options.instrumentation;
    this.semaphore = new Semaphore(concurrency);
    this.idempotencyAttribute =
      options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
    this.onError = options.onError;
    this.onEvent = options.onEvent;
  }

  async process(
    event: LambdaSqsEvent,
    options: LambdaProcessOptions = {},
  ): Promise<LambdaSqsBatchResponse> {
    const signal = options.signal ?? new AbortController().signal;
    if (event.Records.length > 0) {
      this.reportEvent({
        type: "messages_received",
        count: event.Records.length,
      });
    }
    const results = await Promise.all(
      event.Records.map((record) =>
        this.semaphore.run(() => this.processRecord(record, signal)),
      ),
    );

    return {
      batchItemFailures: event.Records.flatMap((record, index) =>
        results[index] ? [] : [{ itemIdentifier: record.messageId }],
      ),
    };
  }

  private async processRecord(
    record: LambdaSqsRecord,
    signal: AbortSignal,
  ): Promise<boolean> {
    if (!record.messageId || signal.aborted) {
      return false;
    }

    const idempotencyKey =
      record.messageAttributes?.[this.idempotencyAttribute]?.stringValue ??
      record.messageId;

    let acquisition: Awaited<ReturnType<IdempotencyStore["acquireLock"]>>;
    try {
      acquisition = await this.idempotency.acquireLock(
        idempotencyKey,
        randomUUID(),
      );
    } catch (error) {
      this.reportError(error, record);
      return false;
    }

    if (acquisition.status === "completed") {
      this.reportEvent({
        type: "job_duplicate",
        idempotencyKey,
        state: "completed",
      });
      return true;
    }

    if (acquisition.status !== "acquired") {
      this.reportEvent({
        type: "job_duplicate",
        idempotencyKey,
        state: acquisition.status,
      });
      return false;
    }

    const lease = acquisition.lease;
    let handlerReturned = false;
    const attempt = this.receiveCount(record);
    const startedAt = Date.now();
    this.reportEvent({ type: "job_started", idempotencyKey, attempt });

    try {
      const message = this.toSdkMessage(record);
      const context: JobContext = {
        idempotencyKey,
        attempt,
        signal,
      };

      await runInstrumentedJob({
        instrumentation: this.instrumentation,
        context: {
          runtime: "lambda",
          attempt,
          signal: context.signal,
        },
        operation: async () => {
          await this.handler(message, context);
          handlerReturned = true;
        },
        onInstrumentationError: (instrumentationError) =>
          this.reportError(instrumentationError, record),
      });

      if (signal.aborted) {
        throw new Error("Lambda invocation is ending before job completion.");
      }

      await this.idempotency.markComplete(lease);
      this.reportEvent({
        type: "job_completed",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt,
      });
      return true;
    } catch (error) {
      if (!handlerReturned) {
        await this.safeRelease(lease, record);
      }
      this.reportEvent({
        type: signal.aborted ? "job_cancelled" : "job_failed",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt,
      });
      this.reportError(error, record);
      return false;
    }
  }

  private toSdkMessage(record: LambdaSqsRecord): Message {
    const idempotencyValue =
      record.messageAttributes?.[this.idempotencyAttribute];

    return {
      MessageId: record.messageId,
      ReceiptHandle: record.receiptHandle,
      Body: record.body,
      Attributes: record.attributes
        ? { ...record.attributes }
        : undefined,
      MessageAttributes: idempotencyValue
        ? {
            [this.idempotencyAttribute]: {
              DataType: idempotencyValue.dataType,
              StringValue: idempotencyValue.stringValue,
              BinaryValue: idempotencyValue.binaryValue
                ? Buffer.from(idempotencyValue.binaryValue, "base64")
                : undefined,
            },
          }
        : undefined,
    };
  }

  private receiveCount(record: LambdaSqsRecord): number {
    const parsed = Number(record.attributes?.ApproximateReceiveCount ?? "1");
    return Number.isInteger(parsed) && parsed > 0 ? parsed : 1;
  }

  private async safeRelease(
    lease: ExecutionLease,
    record: LambdaSqsRecord,
  ): Promise<void> {
    try {
      await this.idempotency.releaseLock(lease);
    } catch (error) {
      this.reportError(error, record);
    }
  }

  private reportError(error: unknown, record?: LambdaSqsRecord): void {
    try {
      this.onError?.(error, record);
    } catch {
      // Observability callbacks must never fail a Lambda batch.
    }
  }

  private reportEvent(event: QueueCraftEvent): void {
    try {
      this.onEvent?.(event);
    } catch {
      // Observability callbacks must never fail a Lambda batch.
    }
  }
}
