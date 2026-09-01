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
import {
  TRACEPARENT_ATTRIBUTE,
  TRACESTATE_ATTRIBUTE,
} from "./trace-context";

const TRACEPARENT =
  "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

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

  it("adds only valid W3C trace fields to the single SQS send", async () => {
    const send = vi.fn().mockResolvedValue({ MessageId: "sqs-id" });
    const traceContext = {
      inject: () => ({
        traceparent: TRACEPARENT,
        tracestate: "vendor=value",
        baggage: "customer_id=private",
      }),
    };
    const publisher = new QueueCraftPublisher({
      sqsClient: { send } as unknown as SQSClient,
      queueUrl:
        "https://sqs.me-central-1.amazonaws.com/123456789012/bookings",
      traceContext,
    });

    await publisher.publish(
      { type: "booking_request" },
      { idempotencyKey: "wamid.abc123" },
    );

    expect(send).toHaveBeenCalledOnce();
    expect(send.mock.calls[0][0].input.MessageAttributes).toEqual({
      [IDEMPOTENCY_ATTRIBUTE]: {
        DataType: "String",
        StringValue: "wamid.abc123",
      },
      [TRACEPARENT_ATTRIBUTE]: {
        DataType: "String",
        StringValue: TRACEPARENT,
      },
      [TRACESTATE_ATTRIBUTE]: {
        DataType: "String",
        StringValue: "vendor=value",
      },
    });
  });

  it("publishes once without trace fields when injection fails", async () => {
    const send = vi.fn().mockResolvedValue({ MessageId: "sqs-id" });
    const traceError = new Error("trace provider unavailable");
    const onTraceContextError = vi.fn();
    const publisher = new QueueCraftPublisher({
      sqsClient: { send } as unknown as SQSClient,
      queueUrl:
        "https://sqs.me-central-1.amazonaws.com/123456789012/bookings",
      traceContext: {
        inject() {
          throw traceError;
        },
      },
      onTraceContextError,
    });

    await publisher.publish(
      { type: "booking_request" },
      { idempotencyKey: "wamid.abc123" },
    );

    expect(send).toHaveBeenCalledOnce();
    expect(send.mock.calls[0][0].input.MessageAttributes).toEqual({
      [IDEMPOTENCY_ATTRIBUTE]: {
        DataType: "String",
        StringValue: "wamid.abc123",
      },
    });
    expect(onTraceContextError).toHaveBeenCalledWith(traceError);
  });

  it("rejects an idempotency attribute that collides with trace context", () => {
    expect(
      () =>
        new QueueCraftPublisher({
          sqsClient: { send: vi.fn() } as unknown as SQSClient,
          queueUrl:
            "https://sqs.me-central-1.amazonaws.com/123456789012/bookings",
          idempotencyAttribute: TRACEPARENT_ATTRIBUTE,
          traceContext: {
            inject: () => undefined,
          },
        }),
    ).toThrow(/reserved W3C trace attribute/);
  });
});
