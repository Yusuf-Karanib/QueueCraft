import { describe, expect, it, vi } from "vitest";
import { AsyncLocalStorage } from "node:async_hooks";
import type { IdempotencyStore } from "./idempotency";
import { QueueCraftLambdaProcessor, type LambdaSqsRecord } from "./lambda-processor";
import { IDEMPOTENCY_ATTRIBUTE } from "./publisher";
import type { QueueCraftJobInstrumentation } from "./instrumentation";
import {
  TRACEPARENT_ATTRIBUTE,
  TRACESTATE_ATTRIBUTE,
  type QueueCraftTraceContextPropagation,
} from "./trace-context";

const lease = { messageId: "meta-message-1", ownerId: "owner-1" };
const TRACEPARENT =
  "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const SECOND_TRACEPARENT =
  "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01";

function record(messageId = "sqs-message-1"): LambdaSqsRecord {
  return {
    messageId,
    receiptHandle: "receipt-1",
    body: JSON.stringify({ hello: "world" }),
    attributes: { ApproximateReceiveCount: "2" },
    messageAttributes: {
      [IDEMPOTENCY_ATTRIBUTE]: {
        dataType: "String",
        stringValue: "meta-message-1",
      },
    },
  };
}

function harness(
  status: "acquired" | "completed" | "in_progress" = "acquired",
  instrumentation?: QueueCraftJobInstrumentation,
  traceContext?: QueueCraftTraceContextPropagation,
) {
  const acquireLock = vi.fn().mockResolvedValue(
    status === "acquired" ? { status, lease } : { status },
  );
  const markComplete = vi.fn().mockResolvedValue(undefined);
  const releaseLock = vi.fn().mockResolvedValue(undefined);
  const handler = vi.fn().mockResolvedValue(undefined);
  const onError = vi.fn();
  const onEvent = vi.fn();
  const idempotency = {
    acquireLock,
    markComplete,
    releaseLock,
  } as unknown as IdempotencyStore;
  const processor = new QueueCraftLambdaProcessor({
    idempotency,
    handler,
    instrumentation,
    traceContext,
    concurrency: 2,
    onError,
    onEvent,
  });

  return {
    processor,
    acquireLock,
    markComplete,
    releaseLock,
    handler,
    onError,
    onEvent,
  };
}

describe("QueueCraftLambdaProcessor", () => {
  it("uses the stable producer key and completes a Lambda-delivered job", async () => {
    const test = harness();
    const input = record();

    await expect(test.processor.process({ Records: [input] })).resolves.toEqual({
      batchItemFailures: [],
    });
    expect(test.acquireLock).toHaveBeenCalledWith(
      "meta-message-1",
      expect.any(String),
    );
    expect(test.handler).toHaveBeenCalledWith(
      expect.objectContaining({ MessageId: "sqs-message-1", Body: input.body }),
      expect.objectContaining({
        idempotencyKey: "meta-message-1",
        attempt: 2,
        signal: expect.any(AbortSignal),
      }),
    );
    expect(test.markComplete).toHaveBeenCalledWith(lease);
    expect(test.releaseLock).not.toHaveBeenCalled();
    expect(test.onEvent.mock.calls.map((call) => call[0].type)).toEqual([
      "messages_received",
      "job_started",
      "job_completed",
    ]);
  });

  it("does not expose trace attributes to the handler unless opted in", async () => {
    const test = harness();
    const base = record();
    const input: LambdaSqsRecord = {
      ...base,
      messageAttributes: {
        ...base.messageAttributes,
        [TRACEPARENT_ATTRIBUTE]: {
          dataType: "String",
          stringValue: TRACEPARENT,
        },
        [TRACESTATE_ATTRIBUTE]: {
          dataType: "String",
          stringValue: "vendor=value",
        },
      },
    };

    await test.processor.process({ Records: [input] });

    expect(test.handler.mock.calls[0][0].MessageAttributes).toEqual({
      [IDEMPOTENCY_ATTRIBUTE]: {
        DataType: "String",
        StringValue: "meta-message-1",
      },
    });
  });

  it("runs an acquired Lambda handler inside privacy-safe instrumentation", async () => {
    const run = vi.fn(async (context, execute: () => Promise<void>) => {
      expect(context).toEqual({
        runtime: "lambda",
        attempt: 2,
        signal: expect.any(AbortSignal),
      });
      expect(context).not.toHaveProperty("idempotencyKey");
      await execute();
    });
    const test = harness("acquired", { run });

    await expect(test.processor.process({ Records: [record()] })).resolves.toEqual({
      batchItemFailures: [],
    });

    expect(run).toHaveBeenCalledTimes(1);
    expect(test.handler).toHaveBeenCalledTimes(1);
    expect(test.markComplete).toHaveBeenCalledWith(lease);
  });

  it("keeps concurrent Lambda records inside their own producer contexts", async () => {
    const storage = new AsyncLocalStorage<string>();
    const traceContext: QueueCraftTraceContextPropagation = {
      inject: () => ({ traceparent: TRACEPARENT }),
      run: (carrier, operation) =>
        storage.run(carrier.traceparent, operation),
    };
    const test = harness("acquired", undefined, traceContext);
    const firstBase = record("first-message");
    const secondBase = record("second-message");
    const first: LambdaSqsRecord = {
      ...firstBase,
      messageAttributes: {
        ...firstBase.messageAttributes,
        [IDEMPOTENCY_ATTRIBUTE]: {
          dataType: "String",
          stringValue: "first-job",
        },
        [TRACEPARENT_ATTRIBUTE]: {
          dataType: "String",
          stringValue: TRACEPARENT,
        },
        [TRACESTATE_ATTRIBUTE]: {
          dataType: "String",
          stringValue: "first=value",
        },
        baggage: {
          dataType: "String",
          stringValue: "customer_id=private",
        },
      },
    };
    const second: LambdaSqsRecord = {
      ...secondBase,
      messageAttributes: {
        ...secondBase.messageAttributes,
        [IDEMPOTENCY_ATTRIBUTE]: {
          dataType: "String",
          stringValue: "second-job",
        },
        [TRACEPARENT_ATTRIBUTE]: {
          dataType: "String",
          stringValue: SECOND_TRACEPARENT,
        },
      },
    };
    const seen = new Map<string, string | undefined>();
    test.handler.mockImplementation(async (message) => {
      await Promise.resolve();
      seen.set(message.MessageId!, storage.getStore());
      expect(
        message.MessageAttributes?.[TRACEPARENT_ATTRIBUTE]?.StringValue,
      ).toBe(storage.getStore());
      expect(message.MessageAttributes).not.toHaveProperty("baggage");
    });

    await expect(
      test.processor.process({ Records: [first, second] }),
    ).resolves.toEqual({ batchItemFailures: [] });

    expect(seen).toEqual(
      new Map([
        ["first-message", TRACEPARENT],
        ["second-message", SECOND_TRACEPARENT],
      ]),
    );
    expect(storage.getStore()).toBeUndefined();
  });

  it("does not retry successful Lambda work after instrumentation fails", async () => {
    const tracingError = new Error("active context cleanup failed");
    const test = harness("acquired", {
      async run(_context, execute) {
        await execute();
        throw tracingError;
      },
    });

    await expect(test.processor.process({ Records: [record()] })).resolves.toEqual({
      batchItemFailures: [],
    });

    expect(test.handler).toHaveBeenCalledTimes(1);
    expect(test.markComplete).toHaveBeenCalledWith(lease);
    expect(test.releaseLock).not.toHaveBeenCalled();
    await vi.waitFor(() =>
      expect(test.onError).toHaveBeenCalledWith(tracingError, record()),
    );
  });

  it("acknowledges an already completed duplicate", async () => {
    const test = harness("completed");

    await expect(test.processor.process({ Records: [record()] })).resolves.toEqual({
      batchItemFailures: [],
    });
    expect(test.handler).not.toHaveBeenCalled();
    expect(test.onEvent).toHaveBeenCalledWith({
      type: "job_duplicate",
      idempotencyKey: "meta-message-1",
      state: "completed",
    });
  });

  it("does not instrument a completed Lambda duplicate", async () => {
    const run = vi.fn();
    const test = harness("completed", { run });

    await test.processor.process({ Records: [record()] });

    expect(run).not.toHaveBeenCalled();
  });

  it("does not restore trace context for a completed Lambda duplicate", async () => {
    const run = vi.fn((_carrier, operation: () => Promise<void>) => operation());
    const test = harness("completed", undefined, {
      inject: () => ({ traceparent: TRACEPARENT }),
      run,
    });
    const base = record();
    const input: LambdaSqsRecord = {
      ...base,
      messageAttributes: {
        ...base.messageAttributes,
        [TRACEPARENT_ATTRIBUTE]: {
          dataType: "String",
          stringValue: TRACEPARENT,
        },
      },
    };

    await test.processor.process({ Records: [input] });

    expect(run).not.toHaveBeenCalled();
  });

  it("returns an in-progress duplicate for a later retry", async () => {
    const test = harness("in_progress");

    await expect(test.processor.process({ Records: [record()] })).resolves.toEqual({
      batchItemFailures: [{ itemIdentifier: "sqs-message-1" }],
    });
    expect(test.handler).not.toHaveBeenCalled();
  });

  it("releases its lease and reports only the failed record", async () => {
    const test = harness();
    test.handler
      .mockRejectedValueOnce(new Error("booking failed"))
      .mockResolvedValueOnce(undefined);

    await expect(
      test.processor.process({
        Records: [record("failed-message"), record("good-message")],
      }),
    ).resolves.toEqual({
      batchItemFailures: [{ itemIdentifier: "failed-message" }],
    });
    expect(test.releaseLock).toHaveBeenCalledTimes(1);
    expect(test.onError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "booking failed" }),
      expect.objectContaining({ messageId: "failed-message" }),
    );
    expect(test.onEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "job_failed",
        idempotencyKey: "meta-message-1",
        attempt: 2,
      }),
    );
  });

  it("does not start work after the invocation aborts", async () => {
    const test = harness();
    const controller = new AbortController();
    controller.abort();

    await expect(
      test.processor.process(
        { Records: [record()] },
        { signal: controller.signal },
      ),
    ).resolves.toEqual({
      batchItemFailures: [{ itemIdentifier: "sqs-message-1" }],
    });
    expect(test.acquireLock).not.toHaveBeenCalled();
  });

  it("does not let a failing event observer fail a Lambda batch", async () => {
    const test = harness();
    test.onEvent.mockImplementation(() => {
      throw new Error("observer failed");
    });

    await expect(test.processor.process({ Records: [record()] })).resolves.toEqual({
      batchItemFailures: [],
    });
    expect(test.handler).toHaveBeenCalledOnce();
  });
});
