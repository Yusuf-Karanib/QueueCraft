/**
 * QueueCraft — publisher
 *
 * Serializes a payload and enqueues it on SQS. Every message carries a
 * client-generated idempotency key in its attributes, so the worker can enforce
 * exactly-once processing via the IdempotencyStore using a key the *producer*
 * controls — independent of the SQS-assigned MessageId.
 */
import {
  SQSClient,
  SendMessageCommand,
  type MessageAttributeValue,
} from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";

/**
 * Message-attribute name carrying QueueCraft's idempotency key.
 *
 * Import this in the worker so producer and consumer agree on the name. The
 * poller must (a) request it on receive via `MessageAttributeNames` and
 * (b) use its value as the `acquireLock` key.
 */
export const IDEMPOTENCY_ATTRIBUTE = "MessageId";

export interface QueueCraftPublisherOptions {
  /** A configured SQS client (region/credentials handled by the caller). */
  readonly sqsClient: SQSClient;

  /** Full URL of the destination SQS queue. */
  readonly queueUrl: string;

  /** Override the attribute name used for the idempotency key. */
  readonly idempotencyAttribute?: string;
}

/** Optional per-message knobs. */
export interface PublishOptions {
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
  private readonly isFifo: boolean;

  constructor(options: QueueCraftPublisherOptions) {
    if (!options.queueUrl) {
      throw new Error("QueueCraftPublisher requires a non-empty queueUrl.");
    }
    this.sqs = options.sqsClient;
    this.queueUrl = options.queueUrl;
    this.idempotencyAttribute =
      options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
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

    const messageId = randomUUID();

    const attributes: Record<string, MessageAttributeValue> = {
      [this.idempotencyAttribute]: {
        DataType: "String",
        StringValue: messageId,
      },
    };

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
}