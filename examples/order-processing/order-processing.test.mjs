import { describe, expect, it, vi } from "vitest";
import { IDEMPOTENCY_ATTRIBUTE } from "@yusufkaranib/queuecraft";
import {
  createOrderMessageHandler,
  createOrderPublisher,
  orderEventIdempotencyKey,
  orderTotalCents,
  parseOrderJob,
} from "./order-processing.mjs";

const order = {
  type: "order.created",
  orderId: "ORDER-1001",
  currency: "AED",
  items: [
    { sku: "COFFEE-1", quantity: 2, unitPriceCents: 1_500 },
    { sku: "MUG-2", quantity: 1, unitPriceCents: 2_000 },
  ],
};

describe("order-processing reference example", () => {
  it("validates a safe order and calculates its total", () => {
    expect(parseOrderJob(order)).toEqual(order);
    expect(orderTotalCents(order)).toBe(5_000);
  });

  it.each([
    [null, "object"],
    [{ ...order, type: "booking.created" }, "order.created"],
    [{ ...order, orderId: "contains spaces" }, "orderId"],
    [{ ...order, currency: "aed" }, "currency"],
    [{ ...order, items: [] }, "items"],
    [
      { ...order, items: [{ sku: "COFFEE-1", quantity: 0, unitPriceCents: 100 }] },
      "quantity",
    ],
  ])("rejects malformed business input", (value, message) => {
    expect(() => parseOrderJob(value)).toThrow(message);
  });

  it("uses the source event ID as a stable idempotency key", async () => {
    const send = vi.fn(async () => ({ MessageId: "sqs-message-id" }));
    const publisher = createOrderPublisher({
      sqsClient: { send },
      queueUrl: "https://sqs.example.test/order-demo",
    });

    const result = await publisher.publishOrder({ eventId: "evt-1001", order });

    expect(result.messageId).toBe("order-created:evt-1001");
    expect(orderEventIdempotencyKey("evt-1001")).toBe(
      "order-created:evt-1001",
    );
    expect(send).toHaveBeenCalledOnce();
    const command = send.mock.calls[0][0];
    expect(command.input.MessageBody).toBe(JSON.stringify(order));
    expect(
      command.input.MessageAttributes[IDEMPOTENCY_ATTRIBUTE].StringValue,
    ).toBe("order-created:evt-1001");
  });

  it("passes only validated orders to the injected business handler", async () => {
    const fulfillOrder = vi.fn(async () => "fulfilled");
    const handler = createOrderMessageHandler({ fulfillOrder });
    const context = { signal: new AbortController().signal };

    await expect(handler({ Body: JSON.stringify(order) }, context)).resolves.toBe(
      "fulfilled",
    );
    expect(fulfillOrder).toHaveBeenCalledWith(order, context);

    await expect(handler({ Body: "not-json" }, context)).rejects.toThrow(
      "valid JSON",
    );
    expect(fulfillOrder).toHaveBeenCalledOnce();
  });

  it("keeps a business failure visible to QueueCraft for retry", async () => {
    const failure = new Error("inventory service unavailable");
    const handler = createOrderMessageHandler({
      fulfillOrder: async () => {
        throw failure;
      },
    });

    await expect(
      handler(
        { Body: JSON.stringify(order) },
        { signal: new AbortController().signal },
      ),
    ).rejects.toBe(failure);
  });
});
