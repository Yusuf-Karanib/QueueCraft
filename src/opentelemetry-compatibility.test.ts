import { context, propagation, ROOT_CONTEXT, trace } from "@opentelemetry/api";
import { describe, expect, it } from "vitest";
import type { QueueCraftActiveTracer } from "./tracing";
import { QueueCraftW3CTraceContext } from "./trace-context";

describe("OpenTelemetry compatibility", () => {
  it("accepts the official OpenTelemetry tracer interface", () => {
    const tracer: QueueCraftActiveTracer = trace.getTracer(
      "queuecraft-compatibility-test",
    );

    expect(tracer.startActiveSpan).toBeTypeOf("function");
  });

  it("accepts the official context and propagation APIs", () => {
    const traceContext = new QueueCraftW3CTraceContext({
      context,
      propagation,
      rootContext: ROOT_CONTEXT,
    });

    expect(traceContext.inject()).toBeUndefined();
  });
});
