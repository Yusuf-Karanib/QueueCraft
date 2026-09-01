export type QueueCraftJobRuntime = "poller" | "lambda";

/** Privacy-safe information available to handler instrumentation. */
export interface QueueCraftJobInstrumentationContext {
  readonly runtime: QueueCraftJobRuntime;
  readonly attempt: number;
  readonly signal: AbortSignal;
}

/**
 * Optional wrapper for running a business handler inside an active context.
 * Implementations must invoke `operation` synchronously, await it, then settle.
 */
export interface QueueCraftJobInstrumentation {
  run(
    context: QueueCraftJobInstrumentationContext,
    operation: () => Promise<void>,
  ): Promise<void> | void;
}

interface RunInstrumentedJobOptions {
  readonly instrumentation?: QueueCraftJobInstrumentation;
  readonly context: QueueCraftJobInstrumentationContext;
  readonly operation: () => Promise<void>;
  readonly onInstrumentationError?: (error: unknown) => void;
}

type OperationOutcome =
  | { readonly status: "completed" }
  | { readonly status: "failed"; readonly error: unknown };

/**
 * Runs one handler exactly once and makes its result authoritative. A broken
 * tracing wrapper must never turn successful business work into an SQS retry.
 */
export async function runInstrumentedJob(
  options: RunInstrumentedJobOptions,
): Promise<void> {
  if (!options.instrumentation) {
    await options.operation();
    return;
  }

  let operationPromise: Promise<void> | undefined;
  let outcomePromise: Promise<OperationOutcome> | undefined;
  let operationCalls = 0;

  const report = (error: unknown): void => {
    try {
      options.onInstrumentationError?.(error);
    } catch {
      // Instrumentation reporting must not change handler behavior.
    }
  };

  const runOperation = (): Promise<void> => {
    operationCalls += 1;
    if (operationPromise) {
      report(
        new Error(
          "QueueCraft instrumentation called the handler operation more than once.",
        ),
      );
      return operationPromise;
    }

    operationPromise = Promise.resolve().then(options.operation);
    outcomePromise = operationPromise.then(
      (): OperationOutcome => ({ status: "completed" }),
      (error: unknown): OperationOutcome => ({ status: "failed", error }),
    );
    return operationPromise;
  };

  let instrumentationResult: Promise<void> | void = undefined;
  let synchronousInstrumentationError: unknown;
  let instrumentationFailedSynchronously = false;
  try {
    instrumentationResult = options.instrumentation.run(
      options.context,
      runOperation,
    );
  } catch (error) {
    instrumentationFailedSynchronously = true;
    synchronousInstrumentationError = error;
  }

  if (operationCalls === 0) {
    if (!instrumentationFailedSynchronously) {
      report(
        new Error(
          "QueueCraft instrumentation did not start the handler operation synchronously.",
        ),
      );
    }
    runOperation();
  }

  if (!instrumentationFailedSynchronously) {
    const finalOutcomePromise = outcomePromise!;
    void Promise.resolve(instrumentationResult).catch(
      async (instrumentationError: unknown) => {
        const finalOutcome = await finalOutcomePromise;
        if (
          !(
            finalOutcome.status === "failed" &&
            instrumentationError === finalOutcome.error
          )
        ) {
          report(instrumentationError);
        }
      },
    );
  }

  const outcome = await outcomePromise!;

  if (
    instrumentationFailedSynchronously &&
    !(
      outcome.status === "failed" &&
      synchronousInstrumentationError === outcome.error
    )
  ) {
    report(synchronousInstrumentationError);
  }

  if (outcome.status === "failed") {
    throw outcome.error;
  }
}
