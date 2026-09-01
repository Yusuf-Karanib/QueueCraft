import {
  IdempotencyStore,
  QueueCraftPoller,
  QueueCraftPublisher,
  Semaphore,
} from "@yusufkaranib/queuecraft";

export const ORDER_EVENT_TYPE = "order.created";

const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SKU_PATTERN = /^[A-Z0-9][A-Z0-9._-]{0,63}$/;

export function orderEventIdempotencyKey(eventId) {
  assertIdentifier(eventId, "eventId");
  return `order-created:${eventId}`;
}

export function parseOrderJob(value) {
  if (!isRecord(value)) throw new TypeError("Order job must be an object.");
  if (value.type !== ORDER_EVENT_TYPE) {
    throw new TypeError(`Order job type must be ${ORDER_EVENT_TYPE}.`);
  }

  assertIdentifier(value.orderId, "orderId");
  if (typeof value.currency !== "string" || !/^[A-Z]{3}$/.test(value.currency)) {
    throw new TypeError("currency must be a three-letter uppercase code.");
  }
  if (!Array.isArray(value.items) || value.items.length < 1 || value.items.length > 20) {
    throw new TypeError("items must contain between 1 and 20 entries.");
  }

  const items = value.items.map((item, index) => {
    if (!isRecord(item)) throw new TypeError(`items[${index}] must be an object.`);
    if (typeof item.sku !== "string" || !SKU_PATTERN.test(item.sku)) {
      throw new TypeError(`items[${index}].sku is invalid.`);
    }
    if (!Number.isInteger(item.quantity) || item.quantity < 1 || item.quantity > 100) {
      throw new TypeError(`items[${index}].quantity must be an integer from 1 to 100.`);
    }
    if (
      !Number.isSafeInteger(item.unitPriceCents) ||
      item.unitPriceCents < 0 ||
      item.unitPriceCents > 10_000_000
    ) {
      throw new TypeError(
        `items[${index}].unitPriceCents must be a safe integer from 0 to 10000000.`,
      );
    }
    return {
      sku: item.sku,
      quantity: item.quantity,
      unitPriceCents: item.unitPriceCents,
    };
  });

  return {
    type: ORDER_EVENT_TYPE,
    orderId: value.orderId,
    currency: value.currency,
    items,
  };
}

export function orderTotalCents(order) {
  const parsed = parseOrderJob(order);
  return parsed.items.reduce(
    (total, item) => total + item.quantity * item.unitPriceCents,
    0,
  );
}

export function createOrderPublisher({ sqsClient, queueUrl, traceContext }) {
  const publisher = new QueueCraftPublisher({
    sqsClient,
    queueUrl,
    traceContext,
  });

  return {
    publishOrder({ eventId, order }) {
      return publisher.publish(parseOrderJob(order), {
        idempotencyKey: orderEventIdempotencyKey(eventId),
      });
    },
  };
}

export function createOrderMessageHandler({ fulfillOrder }) {
  if (typeof fulfillOrder !== "function") {
    throw new TypeError("fulfillOrder must be a function.");
  }

  return async (message, context) => {
    let body;
    try {
      body = JSON.parse(message.Body ?? "");
    } catch {
      throw new TypeError("Order message body must contain valid JSON.");
    }
    return fulfillOrder(parseOrderJob(body), context);
  };
}

export function createOrderPoller({
  sqsClient,
  dynamodbClient,
  queueUrl,
  tableName,
  fulfillOrder,
  worker = {},
  idempotency = {},
  instrumentation,
  traceContext,
  onEvent,
  onError,
}) {
  const concurrency = worker.concurrency ?? 2;
  return new QueueCraftPoller({
    sqsClient,
    queueUrl,
    semaphore: new Semaphore(concurrency),
    idempotency: new IdempotencyStore({
      ...idempotency,
      client: dynamodbClient,
      tableName,
    }),
    worker: {
      concurrency,
      pollIntervalMs: 250,
      shutdownTimeoutMs: 30_000,
      ...worker,
    },
    handler: createOrderMessageHandler({ fulfillOrder }),
    instrumentation,
    traceContext,
    onEvent,
    onError,
  });
}

function assertIdentifier(value, name) {
  if (typeof value !== "string" || !IDENTIFIER_PATTERN.test(value)) {
    throw new TypeError(`${name} must be a safe identifier of 1-128 characters.`);
  }
}

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
