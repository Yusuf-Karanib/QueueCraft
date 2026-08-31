import { describe, expect, it, vi } from "vitest";
import {
  QueueCraftTracingObserver,
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
