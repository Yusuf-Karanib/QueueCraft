/** SQS attribute carrying the W3C `traceparent` value. */
export const TRACEPARENT_ATTRIBUTE = "traceparent";

/** SQS attribute carrying the optional W3C `tracestate` value. */
export const TRACESTATE_ATTRIBUTE = "tracestate";

const TRACEPARENT_PATTERN =
  /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(?:-(.+))?$/;
const ALL_ZERO_TRACE_ID = "00000000000000000000000000000000";
const ALL_ZERO_PARENT_ID = "0000000000000000";
const MAX_TRACEPARENT_LENGTH = 512;
const MAX_TRACESTATE_LENGTH = 512;
const MAX_TRACESTATE_MEMBERS = 32;
const SIMPLE_TRACESTATE_KEY = /^[a-z][a-z0-9_\-*\/]{0,255}$/;
const TENANT_TRACESTATE_KEY = /^[a-z0-9][a-z0-9_\-*\/]{0,240}$/;
const SYSTEM_TRACESTATE_KEY = /^[a-z][a-z0-9_\-*\/]{0,13}$/;
const TRACESTATE_VALUE = /^[\x20-\x2b\x2d-\x3c\x3e-\x7e]+$/;

/** Privacy-limited W3C fields that QueueCraft may carry across SQS. */
export interface QueueCraftTraceCarrier {
  readonly traceparent: string;
  readonly tracestate?: string;
}

/** Supplies the current W3C carrier to a QueueCraft publisher. */
export interface QueueCraftTraceContextInjector {
  inject(): QueueCraftTraceCarrier | undefined;
}

/** Restores an upstream W3C carrier around one worker operation. */
export interface QueueCraftTraceContextExtractor {
  run(
    carrier: QueueCraftTraceCarrier,
    operation: () => Promise<void>,
  ): Promise<void> | void;
}

/** Combined injector/extractor implemented by the built-in adapter. */
export interface QueueCraftTraceContextPropagation
  extends QueueCraftTraceContextInjector,
    QueueCraftTraceContextExtractor {}

/** Structural subset of an OpenTelemetry-compatible context API. */
export interface QueueCraftContextApi<TContext> {
  active(): TContext;
  with<T>(
    context: TContext,
    operation: () => T,
  ): T;
}

/** Structural subset of an OpenTelemetry-compatible propagation API. */
export interface QueueCraftPropagationApi<TContext> {
  inject(context: TContext, carrier: Record<string, string>): void;
  extract(
    context: TContext,
    carrier: QueueCraftTraceCarrier,
  ): TContext;
}

export interface QueueCraftW3CTraceContextOptions<TContext> {
  readonly context: QueueCraftContextApi<TContext>;
  readonly propagation: QueueCraftPropagationApi<TContext>;
  /** Clean base used when extracting a remote producer context. */
  readonly rootContext: TContext;
}

/**
 * Bridges QueueCraft to the official OpenTelemetry context and propagation
 * APIs without adding OpenTelemetry as a runtime dependency.
 */
export class QueueCraftW3CTraceContext<TContext>
  implements QueueCraftTraceContextPropagation
{
  private readonly context: QueueCraftContextApi<TContext>;
  private readonly propagation: QueueCraftPropagationApi<TContext>;
  private readonly rootContext: TContext;

  constructor(options: QueueCraftW3CTraceContextOptions<TContext>) {
    this.context = options.context;
    this.propagation = options.propagation;
    this.rootContext = options.rootContext;
  }

  inject(): QueueCraftTraceCarrier | undefined {
    const carrier: Record<string, string> = {};
    this.propagation.inject(this.context.active(), carrier);
    return normalizeInjectedTraceCarrier(carrier);
  }

  run(
    carrier: QueueCraftTraceCarrier,
    operation: () => Promise<void>,
  ): Promise<void> {
    const safeCarrier = normalizeInjectedTraceCarrier(carrier);
    if (!safeCarrier) return operation();

    const parent = this.propagation.extract(
      this.rootContext,
      safeCarrier,
    );
    return this.context.with(parent, operation);
  }
}

interface RunWithTraceContextOptions {
  readonly traceContext?: QueueCraftTraceContextExtractor;
  readonly carrier?: QueueCraftTraceCarrier;
  readonly operation: () => Promise<void>;
  readonly onError?: (error: unknown) => void;
}

type TraceOperationOutcome =
  | { readonly status: "completed" }
  | { readonly status: "failed"; readonly error: unknown };

/**
 * Restores a remote context without letting a broken propagation adapter run
 * the business operation twice or hold job settlement open.
 */
export async function runWithQueueCraftTraceContext(
  options: RunWithTraceContextOptions,
): Promise<void> {
  if (!options.traceContext || !options.carrier) {
    await options.operation();
    return;
  }

  let operationPromise: Promise<void> | undefined;
  let outcomePromise: Promise<TraceOperationOutcome> | undefined;
  let operationCalls = 0;

  const report = (error: unknown): void => {
    try {
      options.onError?.(error);
    } catch {
      // Trace reporting must not change business behavior.
    }
  };

  const runOperation = (): Promise<void> => {
    operationCalls += 1;
    if (operationPromise) {
      report(
        new Error(
          "QueueCraft trace propagation called the job operation more than once.",
        ),
      );
      return operationPromise;
    }

    operationPromise = Promise.resolve().then(options.operation);
    outcomePromise = operationPromise.then(
      (): TraceOperationOutcome => ({ status: "completed" }),
      (error: unknown): TraceOperationOutcome => ({ status: "failed", error }),
    );
    return operationPromise;
  };

  let propagationResult: Promise<void> | void;
  let synchronousPropagationError: unknown;
  let propagationFailedSynchronously = false;
  try {
    propagationResult = options.traceContext.run(
      options.carrier,
      runOperation,
    );
  } catch (error) {
    propagationFailedSynchronously = true;
    synchronousPropagationError = error;
  }

  if (operationCalls === 0) {
    if (!propagationFailedSynchronously) {
      report(
        new Error(
          "QueueCraft trace propagation did not start the job operation synchronously.",
        ),
      );
    }
    runOperation();
  }

  if (!propagationFailedSynchronously) {
    const finalOutcomePromise = outcomePromise!;
    void Promise.resolve(propagationResult!).catch(
      async (propagationError: unknown) => {
        const finalOutcome = await finalOutcomePromise;
        if (
          !(
            finalOutcome.status === "failed" &&
            propagationError === finalOutcome.error
          )
        ) {
          report(propagationError);
        }
      },
    );
  }

  const outcome = await outcomePromise!;

  if (
    propagationFailedSynchronously &&
    !(
      outcome.status === "failed" &&
      synchronousPropagationError === outcome.error
    )
  ) {
    report(synchronousPropagationError);
  }

  if (outcome.status === "failed") throw outcome.error;
}

/** Read and validate trace fields from QueueCraft SQS message attributes. */
export function readQueueCraftTraceCarrier(
  readAttribute: (name: string) => string | undefined,
): QueueCraftTraceCarrier | undefined {
  const traceparent = readAttribute(TRACEPARENT_ATTRIBUTE);
  if (!isValidTraceparent(traceparent)) return undefined;

  const tracestate = readAttribute(TRACESTATE_ATTRIBUTE);
  return isValidTracestate(tracestate)
    ? { traceparent, tracestate }
    : { traceparent };
}

/** Validate an injector result before it is copied to SQS. */
export function normalizeInjectedTraceCarrier(
  carrier:
    | Readonly<Record<string, string | undefined>>
    | QueueCraftTraceCarrier
    | undefined,
): QueueCraftTraceCarrier | undefined {
  if (!carrier) return undefined;

  const entries = Object.entries(carrier).reduce<Record<string, string>>(
    (normalized, [key, value]) => {
      if (typeof value === "string") normalized[key.toLowerCase()] = value;
      return normalized;
    },
    {},
  );

  const traceparent = entries.traceparent;
  if (!isValidTraceparent(traceparent)) return undefined;

  const tracestate = entries.tracestate;
  return isValidTracestate(tracestate)
    ? { traceparent, tracestate }
    : { traceparent };
}

function isValidTraceparent(value: string | undefined): value is string {
  if (!value || value.length > MAX_TRACEPARENT_LENGTH) return false;
  const match = TRACEPARENT_PATTERN.exec(value);
  return Boolean(
    match &&
      match[1] !== "ff" &&
      (match[1] !== "00" || match[5] === undefined) &&
      match[2] !== ALL_ZERO_TRACE_ID &&
      match[3] !== ALL_ZERO_PARENT_ID,
  );
}

function isValidTracestate(value: string | undefined): value is string {
  if (!value || value.length > MAX_TRACESTATE_LENGTH) return false;

  const members = value.split(",");
  if (members.length > MAX_TRACESTATE_MEMBERS) return false;

  const seenKeys = new Set<string>();
  for (const rawMember of members) {
    const member = trimTracestateOptionalWhitespace(rawMember);
    if (!member) continue;

    const separator = member.indexOf("=");
    if (separator <= 0 || separator !== member.lastIndexOf("=")) return false;

    const key = member.slice(0, separator);
    const memberValue = member.slice(separator + 1);
    if (
      seenKeys.has(key) ||
      !isValidTracestateKey(key) ||
      memberValue.length > 256 ||
      !TRACESTATE_VALUE.test(memberValue) ||
      memberValue.endsWith(" ")
    ) {
      return false;
    }
    seenKeys.add(key);
  }

  return seenKeys.size > 0;
}

function trimTracestateOptionalWhitespace(value: string): string {
  return value.replace(/^[ \t]+|[ \t]+$/g, "");
}

function isValidTracestateKey(key: string): boolean {
  const tenantSeparator = key.indexOf("@");
  if (tenantSeparator < 0) return SIMPLE_TRACESTATE_KEY.test(key);
  if (tenantSeparator !== key.lastIndexOf("@")) return false;

  return (
    TENANT_TRACESTATE_KEY.test(key.slice(0, tenantSeparator)) &&
    SYSTEM_TRACESTATE_KEY.test(key.slice(tenantSeparator + 1))
  );
}
