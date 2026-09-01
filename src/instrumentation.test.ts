import { describe, expect, it, vi } from "vitest";
import {
  runInstrumentedJob,
  type QueueCraftJobInstrumentation,
} from "./instrumentation";

const context = {
  runtime: "poller" as const,
  attempt: 2,
  signal: new AbortController().signal,
};

describe("runInstrumentedJob", () => {
  it("runs a normal wrapper and handler once", async () => {
    const operation = vi.fn(async () => undefined);
    const instrumentation: QueueCraftJobInstrumentation = {
      async run(received, execute) {
        expect(received).toBe(context);
        await execute();
      },
    };

    await runInstrumentedJob({ instrumentation, context, operation });

    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("does not repeat the handler when a wrapper calls it twice", async () => {
    const operation = vi.fn(async () => undefined);
    const onInstrumentationError = vi.fn();
    const instrumentation: QueueCraftJobInstrumentation = {
      async run(_received, execute) {
        await Promise.all([execute(), execute()]);
      },
    };

    await runInstrumentedJob({
      instrumentation,
      context,
      operation,
      onInstrumentationError,
    });

    expect(operation).toHaveBeenCalledTimes(1);
    expect(onInstrumentationError).toHaveBeenCalledTimes(1);
  });

  it("runs the handler once when instrumentation fails before it", async () => {
    const tracingError = new Error("tracer unavailable");
    const operation = vi.fn(async () => undefined);
    const onInstrumentationError = vi.fn();

    await runInstrumentedJob({
      instrumentation: {
        run() {
          throw tracingError;
        },
      },
      context,
      operation,
      onInstrumentationError,
    });

    expect(operation).toHaveBeenCalledTimes(1);
    await vi.waitFor(() =>
      expect(onInstrumentationError).toHaveBeenCalledWith(tracingError),
    );
  });

  it("keeps a successful handler successful when instrumentation fails later", async () => {
    const tracingError = new Error("span cleanup failed");
    const operation = vi.fn(async () => undefined);
    const onInstrumentationError = vi.fn();

    await runInstrumentedJob({
      instrumentation: {
        async run(_received, execute) {
          await execute();
          throw tracingError;
        },
      },
      context,
      operation,
      onInstrumentationError,
    });

    expect(operation).toHaveBeenCalledTimes(1);
    await vi.waitFor(() =>
      expect(onInstrumentationError).toHaveBeenCalledWith(tracingError),
    );
  });

  it("preserves the original handler error when instrumentation replaces it", async () => {
    const handlerError = new Error("business failure");
    const tracingError = new Error("wrapper failure");
    const onInstrumentationError = vi.fn();

    await expect(
      runInstrumentedJob({
        instrumentation: {
          async run(_received, execute) {
            try {
              await execute();
            } catch {
              throw tracingError;
            }
          },
        },
        context,
        operation: async () => {
          throw handlerError;
        },
        onInstrumentationError,
      }),
    ).rejects.toBe(handlerError);

    await vi.waitFor(() =>
      expect(onInstrumentationError).toHaveBeenCalledWith(tracingError),
    );
  });

  it("runs the handler when instrumentation forgets to invoke it", async () => {
    const operation = vi.fn(async () => undefined);
    const onInstrumentationError = vi.fn();

    await runInstrumentedJob({
      instrumentation: { run: () => undefined },
      context,
      operation,
      onInstrumentationError,
    });

    expect(operation).toHaveBeenCalledTimes(1);
    expect(onInstrumentationError).toHaveBeenCalledTimes(1);
  });

  it("does not wait forever when instrumentation hangs after the handler", async () => {
    const operation = vi.fn(async () => undefined);

    await runInstrumentedJob({
      instrumentation: {
        async run(_received, execute) {
          await execute();
          await new Promise<void>(() => undefined);
        },
      },
      context,
      operation,
    });

    expect(operation).toHaveBeenCalledTimes(1);
  });

  it("starts the handler directly when instrumentation never invokes it", async () => {
    const operation = vi.fn(async () => undefined);
    const onInstrumentationError = vi.fn();

    await runInstrumentedJob({
      instrumentation: {
        run() {
          return new Promise<void>(() => undefined);
        },
      },
      context,
      operation,
      onInstrumentationError,
    });

    expect(operation).toHaveBeenCalledTimes(1);
    expect(onInstrumentationError).toHaveBeenCalledTimes(1);
  });
});
