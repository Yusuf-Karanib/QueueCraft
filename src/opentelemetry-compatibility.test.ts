import { trace } from "@opentelemetry/api";
import { describe, expect, it } from "vitest";
import type { QueueCraftActiveTracer } from "./tracing";

describe("OpenTelemetry compatibility", () => {
  it("accepts the official OpenTelemetry tracer interface", () => {
    const tracer: QueueCraftActiveTracer = trace.getTracer(
      "queuecraft-compatibility-test",
    );

    expect(tracer.startActiveSpan).toBeTypeOf("function");
  });
});
