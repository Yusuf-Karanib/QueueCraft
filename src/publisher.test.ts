import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@aws-sdk/client-sqs", () => {
  class SQSClient {
    send = vi.fn();
  }
  class SendMessageCommand {
    constructor(public readonly input: unknown) {}
  }
  return { SQSClient, SendMessageCommand };
});

import { SQSClient } from "@aws-sdk/client-sqs";
import {
  IDEMPOTENCY_ATTRIBUTE,
  QueueCraftPublisher,
} from "./publisher";

describe("QueueCraftPublisher", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("reuses a caller-provided idempotency key", async () => {
    const send = vi.fn().mockResolvedValue({ MessageId: "sqs-id" });
    const publisher = new QueueCraftPublisher({
      sqsClient: { send } as unknown as SQSClient,
      queueUrl:
        "https://sqs.me-central-1.amazonaws.com/123456789012/bookings",
    });

    const result = await publisher.publish(
      { type: "booking_request" },
      { idempotencyKey: "wamid.abc123" },
    );

    expect(result).toEqual({
      messageId: "wamid.abc123",
      sqsMessageId: "sqs-id",
    });
    expect(send.mock.calls[0][0].input).toMatchObject({
      MessageAttributes: {
        [IDEMPOTENCY_ATTRIBUTE]: {
          DataType: "String",
          StringValue: "wamid.abc123",
        },
      },
    });
  });

  it("uses the stable key as the default FIFO deduplication id", async () => {
    const send = vi.fn().mockResolvedValue({ MessageId: "sqs-id" });
    const publisher = new QueueCraftPublisher({
      sqsClient: { send } as unknown as SQSClient,
      queueUrl:
        "https://sqs.me-central-1.amazonaws.com/123456789012/bookings.fifo",
    });

    await publisher.publish(
      { type: "booking_request" },
      { idempotencyKey: "wamid.abc123", messageGroupId: "customer-1" },
    );

    expect(send.mock.calls[0][0].input).toMatchObject({
      MessageGroupId: "customer-1",
      MessageDeduplicationId: "wamid.abc123",
    });
  });
});
