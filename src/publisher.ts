/**
 * QueueCraft — publisher
 *
 * Serializes a payload and enqueues it on SQS. Every message carries a
 * client-generated idempotency key in its attributes, so the worker can
 * suppress duplicate logical jobs using a key the producer controls,
 * independent of the SQS-assigned MessageId.
 */
import {
  SQSClient,
  SendMessageCommand,
  type MessageAttributeValue,
} from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";
import {
  normalizeInjectedTraceCarrier,
  TRACEPARENT_ATTRIBUTE,
  TRACESTATE_ATTRIBUTE,
  type QueueCraftTraceCarrier,
  type QueueCraftTraceContextInjector,
} from "./trace-context";

/**
 * Message-attribute name carrying QueueCraft's idempotency key.
 *
 * Import this in the worker so producer and consumer agree on the name. The
 * poller must (a) request it on receive via `MessageAttributeNames` and
 * (b) use its value as the `acquireLock` key.
 */
export const IDEMPOTENCY_ATTRIBUTE = "QueueCraftIdempotencyKey";

export interface QueueCraftPublisherOptions {
  /** A configured SQS client (region/credentials handled by the caller). */
  readonly sqsClient: SQSClient;

  /** Full URL of the destination SQS queue. */
  readonly queueUrl: string;

  /** Override the attribute name used for the idempotency key. */
  readonly idempotencyAttribute?: string;

  /** Optional W3C trace-context injector for producer-to-worker traces. */
  readonly traceContext?: QueueCraftTraceContextInjector;

  /** Receives trace-injection errors. Trace failures never block publishing. */
  readonly onTraceContextError?: (error: unknown) => void;
}

/** Optional per-message knobs. */
export interface PublishOptions {
  /**
   * Stable application-level identifier for this logical job.
   *
   * Reuse the same value when retrying a publish, such as a webhook event ID.
   * A UUID is generated only when the caller does not provide one.
   */
  readonly idempotencyKey?: string;

  /** Delay before the message becomes visible, in seconds (0–900). Standard queues only. */
  readonly delaySeconds?: number;

  /** FIFO only: partitions ordering. Required when publishing to a `.fifo` queue. */
  readonly messageGroupId?: string;

  /** FIFO only: deduplication id. Defaults to the generated idempotency key. */
  readonly deduplicationId?: string;
}

export interface PublishResult {
  /** Client-generated idempotency key placed in the message attributes. */
  readonly messageId: string;

  /** SQS-assigned message id (distinct from `messageId`), if returned. */
  readonly sqsMessageId?: string;
}

export class QueueCraftPublisher {
  private readonly sqs: SQSClient;
  private readonly queueUrl: string;
  private readonly idempotencyAttribute: string;
  private readonly traceContext?: QueueCraftTraceContextInjector;
  private readonly onTraceContextError?: (error: unknown) => void;
  private readonly isFifo: boolean;

  constructor(options: QueueCraftPublisherOptions) {
    if (!options.queueUrl) {
      throw new Error("QueueCraftPublisher requires a non-empty queueUrl.");
    }
    this.sqs = options.sqsClient;
    this.queueUrl = options.queueUrl;
    this.idempotencyAttribute =
      options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
    this.traceContext = options.traceContext;
    this.onTraceContextError = options.onTraceContextError;
    if (
      this.traceContext &&
      [TRACEPARENT_ATTRIBUTE, TRACESTATE_ATTRIBUTE].includes(
        this.idempotencyAttribute,
      )
    ) {
      throw new RangeError(
        "idempotencyAttribute cannot use a reserved W3C trace attribute name.",
      );
    }
    this.isFifo = options.queueUrl.endsWith(".fifo");
  }

  /**
   * Serialize and enqueue a payload. Generates a unique idempotency key,
   * attaches it as a message attribute, and returns it to the caller so the
   * publish can be correlated or safely retried.
   */
  async publish(
    payload: unknown,
    options: PublishOptions = {},
  ): Promise<PublishResult> {
    const body = JSON.stringify(payload);
    if (body === undefined) {
      throw new TypeError(
        "publish(payload): payload must be JSON-serializable and not undefined.",
      );
    }

    const messageId = options.idempotencyKey ?? randomUUID();
    if (!messageId) {
      throw new TypeError("idempotencyKey must be a non-empty string.");
    }

    const attributes: Record<string, MessageAttributeValue> = {
      [this.idempotencyAttribute]: {
        DataType: "String",
        StringValue: messageId,
      },
    };

    const traceCarrier = this.injectTraceContext();
    if (traceCarrier) {
      attributes[TRACEPARENT_ATTRIBUTE] = {
        DataType: "String",
        StringValue: traceCarrier.traceparent,
      };
      if (traceCarrier.tracestate) {
        attributes[TRACESTATE_ATTRIBUTE] = {
          DataType: "String",
          StringValue: traceCarrier.tracestate,
        };
      }
    }

    const result = await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: body,
        MessageAttributes: attributes,
        // Per-message delay is a standard-queue feature only.
        DelaySeconds: this.isFifo ? undefined : options.delaySeconds,
        // FIFO-only fields; omitted entirely for standard queues.
        MessageGroupId: this.isFifo ? options.messageGroupId : undefined,
        MessageDeduplicationId: this.isFifo
          ? options.deduplicationId ?? messageId
          : undefined,
      }),
    );

    return { messageId, sqsMessageId: result.MessageId };
  }

  private injectTraceContext(): QueueCraftTraceCarrier | undefined {
    if (!this.traceContext) return undefined;

    try {
      const injected = this.traceContext.inject();
      const normalized = normalizeInjectedTraceCarrier(injected);
      if (injected && !normalized) {
        this.reportTraceContextError(
          new Error("Trace injector returned an invalid W3C traceparent."),
        );
      }
      return normalized;
    } catch (error) {
      this.reportTraceContextError(error);
      return undefined;
    }
  }

  private reportTraceContextError(error: unknown): void {
    try {
      this.onTraceContextError?.(error);
    } catch {
      // Observability callbacks must never prevent publishing a job.
    }
  }
}
