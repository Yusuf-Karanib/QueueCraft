import { AsyncLocalStorage } from "node:async_hooks";
import { describe, expect, it, vi } from "vitest";
import {
  QueueCraftActiveTracing,
  QueueCraftTracingObserver,
  type QueueCraftActiveTracer,
  type QueueCraftSpanAttributeValue,
  type QueueCraftTraceSpan,
  type QueueCraftTracer,
} from "./tracing";

interface TestSpan extends QueueCraftTraceSpan {
  readonly attributes: Map<string, QueueCraftSpanAttributeValue>;
  readonly end: ReturnType<typeof vi.fn<() => void>>;
}

function tracingHarness() {
  const spans: TestSpan[] = [];
  const startSpan = vi.fn((_name: string, options?: { attributes?: object }) => {
    const span: TestSpan = {
      attributes: new Map(Object.entries(options?.attributes ?? {})),
      setAttribute(name, value) {
        this.attributes.set(name, value);
      },
      end: vi.fn<() => void>(),
    };
    spans.push(span);
    return span;
  });
  return {
    spans,
    tracer: { startSpan } as QueueCraftTracer,
    startSpan,
  };
}

function activeTracingHarness() {
  const storage = new AsyncLocalStorage<TestSpan>();
  const spans: TestSpan[] = [];
  const startActiveSpan = vi.fn(
    <T>(
      _name: string,
      options: { attributes?: object },
      operation: (span: QueueCraftTraceSpan) => T,
    ): T => {
      const span: TestSpan = {
        attributes: new Map(Object.entries(options.attributes ?? {})),
        setAttribute(name, value) {
          this.attributes.set(name, value);
        },
        end: vi.fn<() => void>(),
      };
      spans.push(span);
      return storage.run(span, () => operation(span));
    },
  );
  return {
    spans,
    storage,
    startActiveSpan,
    tracer: { startActiveSpan } as QueueCraftActiveTracer,
  };
}

describe("QueueCraftActiveTracing", () => {
  it("keeps the handler inside an active span across awaits", async () => {
    const test = activeTracingHarness();
    const tracing = new QueueCraftActiveTracing({
      tracer: test.tracer,
      attributes: { service: "booking-worker" },
    });

    await tracing.run(
      {
        runtime: "poller",
        attempt: 2,
        signal: new AbortController().signal,
      },
      async () => {
        expect(test.storage.getStore()).toBe(test.spans[0]);
        await Promise.resolve();
        expect(test.storage.getStore()).toBe(test.spans[0]);
      },
    );

    expect(test.startActiveSpan).toHaveBeenCalledWith(
      "queuecraft.handler",
      {
        attributes: {
          service: "booking-worker",
          "messaging.system": "aws.sqs",
          "messaging.operation.type": "process",
          "queuecraft.runtime": "poller",
          "queuecraft.attempt": 2,
        },
      },
      expect.any(Function),
    );
    expect(JSON.stringify(test.startActiveSpan.mock.calls)).not.toContain(
      "private-customer-reference",
    );
    expect(test.spans[0].attributes.get("queuecraft.outcome")).toBe(
      "completed",
    );
    expect(test.spans[0].attributes.get("queuecraft.duration_ms")).toEqual(
      expect.any(Number),
    );
    expect(test.spans[0].end).toHaveBeenCalledOnce();
  });

  it("rethrows the same handler error and closes the span", async () => {
    const test = activeTracingHarness();
    const tracing = new QueueCraftActiveTracing({ tracer: test.tracer });
    const handlerError = new Error("booking failed");

    await expect(
      tracing.run(
        {
          runtime: "lambda",
          attempt: 1,
          signal: new AbortController().signal,
        },
        async () => {
          throw handlerError;
        },
      ),
    ).rejects.toBe(handlerError);

    expect(test.spans[0].attributes.get("queuecraft.outcome")).toBe("failed");
    expect(test.spans[0].end).toHaveBeenCalledOnce();
  });

  it("records cancellation from the privacy-safe signal", async () => {
    const test = activeTracingHarness();
    const tracing = new QueueCraftActiveTracing({ tracer: test.tracer });
    const controller = new AbortController();
    controller.abort();

    await tracing.run(
      { runtime: "poller", attempt: 1, signal: controller.signal },
      async () => undefined,
    );

    expect(test.spans[0].attributes.get("queuecraft.outcome")).toBe(
      "cancelled",
    );
  });

  it("runs the handler if the tracer fails before activating it", async () => {
    const tracerError = new Error("tracer unavailable");
    const onError = vi.fn();
    const operation = vi.fn(async () => undefined);
    const tracing = new QueueCraftActiveTracing({
      tracer: {
        startActiveSpan() {
          throw tracerError;
        },
      },
      onError,
    });

    await tracing.run(
      {
        runtime: "poller",
        attempt: 1,
        signal: new AbortController().signal,
      },
      operation,
    );

    expect(operation).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledWith(tracerError);
  });

  it("does not fail successful work when span cleanup fails", async () => {
    const onError = vi.fn();
    const tracing = new QueueCraftActiveTracing({
      tracer: {
        startActiveSpan(_name, _options, operation) {
          return operation({
            setAttribute() {
              throw new Error("attribute failed");
            },
            end() {
              throw new Error("end failed");
            },
          });
        },
      },
      onError,
    });

    await expect(
      tracing.run(
        {
          runtime: "lambda",
          attempt: 1,
          signal: new AbortController().signal,
        },
        async () => undefined,
      ),
    ).resolves.toBeUndefined();

    expect(onError).toHaveBeenCalledTimes(2);
  });
});

describe("QueueCraftTracingObserver", () => {
  it("creates and finishes a job span without exporting its stable key", () => {
    const test = tracingHarness();
    const observer = new QueueCraftTracingObserver({
      tracer: test.tracer,
      attributes: { service: "booking-worker" },
    });

    observer.onEvent({
      type: "job_started",
      idempotencyKey: "private-customer-reference",
      attempt: 2,
    });
    observer.onEvent({
      type: "job_completed",
      idempotencyKey: "private-customer-reference",
      attempt: 2,
      durationMs: 125,
    });

    expect(test.startSpan).toHaveBeenCalledWith("queuecraft.job", {
      attributes: {
        service: "booking-worker",
        "messaging.system": "aws.sqs",
        "queuecraft.attempt": 2,
      },
    });
    expect(JSON.stringify(test.startSpan.mock.calls)).not.toContain(
      "private-customer-reference",
    );
    expect(test.spans[0].attributes.get("queuecraft.outcome")).toBe("completed");
    expect(test.spans[0].attributes.get("queuecraft.duration_ms")).toBe(125);
    expect(test.spans[0].end).toHaveBeenCalledOnce();
    expect(observer.activeSpanCount).toBe(0);
  });

  it("records bounded duplicate state as an instant span", () => {
    const test = tracingHarness();
    const observer = new QueueCraftTracingObserver({ tracer: test.tracer });

    observer.onEvent({
      type: "job_duplicate",
      idempotencyKey: "private-key",
      state: "completed",
    });

    expect(test.startSpan).toHaveBeenCalledWith("queuecraft.job.duplicate", {
      attributes: {
        "messaging.system": "aws.sqs",
        "queuecraft.duplicate_state": "completed",
      },
    });
    expect(test.spans[0].end).toHaveBeenCalledOnce();
  });

  it("ends unfinished spans when the observer closes", () => {
    const test = tracingHarness();
    const observer = new QueueCraftTracingObserver({ tracer: test.tracer });
    observer.onEvent({ type: "job_started", idempotencyKey: "key-1", attempt: 1 });

    observer.close();

    expect(test.spans[0].attributes.get("queuecraft.outcome")).toBe(
      "observer_closed",
    );
    expect(test.spans[0].end).toHaveBeenCalledOnce();
    expect(observer.activeSpanCount).toBe(0);
  });

  it("never lets a tracer failure escape the observer", () => {
    const onError = vi.fn(() => {
      throw new Error("error observer also failed");
    });
    const observer = new QueueCraftTracingObserver({
      tracer: {
        startSpan() {
          throw new Error("tracer failed");
        },
      },
      onError,
    });

    expect(() =>
      observer.onEvent({
        type: "job_started",
        idempotencyKey: "key-1",
        attempt: 1,
      }),
    ).not.toThrow();
    expect(onError).toHaveBeenCalledOnce();
  });
});
