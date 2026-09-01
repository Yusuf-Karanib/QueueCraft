import { AsyncLocalStorage } from "node:async_hooks";
import { describe, expect, it, vi } from "vitest";
import {
  normalizeInjectedTraceCarrier,
  QueueCraftW3CTraceContext,
  readQueueCraftTraceCarrier,
  runWithQueueCraftTraceContext,
  TRACEPARENT_ATTRIBUTE,
  TRACESTATE_ATTRIBUTE,
  type QueueCraftTraceCarrier,
  type QueueCraftTraceContextPropagation,
} from "./trace-context";

const TRACEPARENT =
  "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const SECOND_TRACEPARENT =
  "00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01";

describe("QueueCraft W3C trace context", () => {
  it("allowlists valid W3C fields and drops baggage", () => {
    expect(
      normalizeInjectedTraceCarrier({
        TraceParent: TRACEPARENT,
        TraceState: "vendor=value",
        baggage: "customer_id=private",
      }),
    ).toEqual({
      traceparent: TRACEPARENT,
      tracestate: "vendor=value",
    });
  });

  it("rejects invalid parents and drops invalid state without rejecting work", () => {
    expect(
      normalizeInjectedTraceCarrier({
        traceparent:
          "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
      }),
    ).toBeUndefined();

    expect(
      normalizeInjectedTraceCarrier({
        traceparent: TRACEPARENT,
        tracestate: "x".repeat(513),
      }),
    ).toEqual({ traceparent: TRACEPARENT });

    expect(
      normalizeInjectedTraceCarrier({
        traceparent: TRACEPARENT,
        tracestate: "hello world",
      }),
    ).toEqual({ traceparent: TRACEPARENT });

    const tooManyMembers = Array.from(
      { length: 33 },
      (_, index) => `vendor${index}=value`,
    ).join(",");
    expect(
      normalizeInjectedTraceCarrier({
        traceparent: TRACEPARENT,
        tracestate: tooManyMembers,
      }),
    ).toEqual({ traceparent: TRACEPARENT });

    for (const invalidState of [
      "vendor =value",
      "vendor=\tvalue",
      "\nvendor=value",
      "vendor=value=extra",
      "Vendor=value",
    ]) {
      expect(
        normalizeInjectedTraceCarrier({
          traceparent: TRACEPARENT,
          tracestate: invalidState,
        }),
      ).toEqual({ traceparent: TRACEPARENT });
    }

    expect(
      normalizeInjectedTraceCarrier({
        traceparent:
          "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
      }),
    ).toBeUndefined();

    expect(
      normalizeInjectedTraceCarrier({
        traceparent: TRACEPARENT,
        tracestate: "vendor=value, ,other=ok",
      }),
    ).toEqual({
      traceparent: TRACEPARENT,
      tracestate: "vendor=value, ,other=ok",
    });

    expect(
      normalizeInjectedTraceCarrier({
        traceparent: TRACEPARENT,
        tracestate: "vendor=first,vendor=second",
      }),
    ).toEqual({ traceparent: TRACEPARENT });
  });

  it("reads only the two reserved SQS attributes", () => {
    const values: Record<string, string> = {
      [TRACEPARENT_ATTRIBUTE]: TRACEPARENT,
      [TRACESTATE_ATTRIBUTE]: "vendor=value",
      baggage: "customer_id=private",
    };

    expect(readQueueCraftTraceCarrier((name) => values[name])).toEqual({
      traceparent: TRACEPARENT,
      tracestate: "vendor=value",
    });
  });

  it("bridges generic context and propagation APIs across an await", async () => {
    const storage = new AsyncLocalStorage<string>();
    const contextApi = {
      active: () => storage.getStore() ?? "root",
      with: <T>(value: string, operation: () => T): T =>
        storage.run(value, operation),
    };
    const propagationApi = {
      inject: (_context: string, carrier: Record<string, string>) => {
        carrier.traceparent = TRACEPARENT;
        carrier.tracestate = "vendor=value";
        carrier.baggage = "must-not-cross-sqs";
      },
      extract: (_root: string, carrier: QueueCraftTraceCarrier) =>
        carrier.traceparent,
    };
    const bridge = new QueueCraftW3CTraceContext({
      context: contextApi,
      propagation: propagationApi,
      rootContext: "clean-root",
    });

    expect(bridge.inject()).toEqual({
      traceparent: TRACEPARENT,
      tracestate: "vendor=value",
    });

    await bridge.run({ traceparent: SECOND_TRACEPARENT }, async () => {
      expect(storage.getStore()).toBe(SECOND_TRACEPARENT);
      await Promise.resolve();
      expect(storage.getStore()).toBe(SECOND_TRACEPARENT);
    });
    expect(storage.getStore()).toBeUndefined();
  });
});

describe("trace propagation failure isolation", () => {
  const carrier: QueueCraftTraceCarrier = { traceparent: TRACEPARENT };

  it("runs the operation once when propagation calls it twice", async () => {
    const operation = vi.fn().mockResolvedValue(undefined);
    const onError = vi.fn();
    const traceContext: QueueCraftTraceContextPropagation = {
      inject: () => carrier,
      async run(_carrier, run) {
        await run();
        await run();
      },
    };

    await runWithQueueCraftTraceContext({
      traceContext,
      carrier,
      operation,
      onError,
    });

    expect(operation).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ message: expect.stringContaining("more than once") }),
    );
  });

  it("runs the operation once if propagation fails before entering it", async () => {
    const propagationError = new Error("extract failed");
    const operation = vi.fn().mockResolvedValue(undefined);
    const onError = vi.fn();
    const traceContext: QueueCraftTraceContextPropagation = {
      inject: () => carrier,
      run() {
        throw propagationError;
      },
    };

    await runWithQueueCraftTraceContext({
      traceContext,
      carrier,
      operation,
      onError,
    });

    expect(operation).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(propagationError);
  });

  it("runs the operation once if propagation never enters it", async () => {
    const operation = vi.fn().mockResolvedValue(undefined);
    const onError = vi.fn();
    const traceContext: QueueCraftTraceContextPropagation = {
      inject: () => carrier,
      run: () => undefined,
    };

    await runWithQueueCraftTraceContext({
      traceContext,
      carrier,
      operation,
      onError,
    });

    expect(operation).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining("did not start"),
      }),
    );
  });

  it("does not wait for propagation cleanup after the operation settles", async () => {
    const operation = vi.fn().mockResolvedValue(undefined);
    const traceContext: QueueCraftTraceContextPropagation = {
      inject: () => carrier,
      run(_carrier, run) {
        void run();
        return new Promise<void>(() => undefined);
      },
    };

    await expect(
      runWithQueueCraftTraceContext({ traceContext, carrier, operation }),
    ).resolves.toBeUndefined();
    expect(operation).toHaveBeenCalledOnce();
  });

  it("keeps the operation's exact rejection authoritative", async () => {
    const jobError = new Error("job failed");
    const operation = vi.fn().mockRejectedValue(jobError);
    const onError = vi.fn();
    const traceContext: QueueCraftTraceContextPropagation = {
      inject: () => carrier,
      run: (_carrier, run) => run(),
    };

    await expect(
      runWithQueueCraftTraceContext({
        traceContext,
        carrier,
        operation,
        onError,
      }),
    ).rejects.toBe(jobError);
    expect(operation).toHaveBeenCalledOnce();
    expect(onError).not.toHaveBeenCalled();
  });
});
