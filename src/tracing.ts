import type { QueueCraftEvent } from "./poller";

export type QueueCraftSpanAttributeValue = string | number | boolean;
export type QueueCraftSpanAttributes = Readonly<
  Record<string, QueueCraftSpanAttributeValue>
>;

/** Small structural subset implemented by OpenTelemetry spans. */
export interface QueueCraftTraceSpan {
  setAttribute(name: string, value: QueueCraftSpanAttributeValue): void;
  end(): void;
}

/** Small structural subset implemented by an OpenTelemetry tracer. */
export interface QueueCraftTracer {
  startSpan(
    name: string,
    options?: { readonly attributes?: QueueCraftSpanAttributes },
  ): QueueCraftTraceSpan;
}

export interface QueueCraftTracingObserverOptions {
  readonly tracer: QueueCraftTracer;
  readonly spanName?: string;
  readonly attributes?: QueueCraftSpanAttributes;
  readonly onError?: (error: unknown) => void;
}

interface ActiveSpan {
  readonly span: QueueCraftTraceSpan;
}

/**
 * Converts QueueCraft lifecycle events into spans without attaching message
 * bodies or idempotency keys as span attributes.
 */
export class QueueCraftTracingObserver {
  private readonly tracer: QueueCraftTracer;
  private readonly spanName: string;
  private readonly attributes: QueueCraftSpanAttributes;
  private readonly onError?: (error: unknown) => void;
  private readonly active = new Map<string, ActiveSpan>();
  private closed = false;

  constructor(options: QueueCraftTracingObserverOptions) {
    this.tracer = options.tracer;
    this.spanName = options.spanName ?? "queuecraft.job";
    this.attributes = options.attributes ?? {};
    this.onError = options.onError;

    if (!this.spanName.trim()) {
      throw new RangeError("spanName must be non-empty.");
    }
  }

  /** Synchronous, failure-isolated observer for QueueCraft lifecycle events. */
  readonly onEvent = (event: QueueCraftEvent): void => {
    if (this.closed) return;

    try {
      switch (event.type) {
        case "job_started":
          this.startJob(event.idempotencyKey, event.attempt);
          break;
        case "job_completed":
        case "job_failed":
        case "job_cancelled":
          this.finishJob(
            event.idempotencyKey,
            event.type.slice("job_".length),
            event.attempt,
            event.durationMs,
          );
          break;
        case "job_duplicate":
          this.recordInstantSpan(`${this.spanName}.duplicate`, {
            "queuecraft.duplicate_state": event.state,
          });
          break;
        case "shutdown_timeout":
          this.recordInstantSpan(`${this.spanName}.shutdown_timeout`, {
            "queuecraft.active_jobs": event.activeJobs,
            "queuecraft.timeout_ms": event.timeoutMs,
          });
          break;
        case "messages_received":
          break;
      }
    } catch (error) {
      this.reportError(error);
    }
  };

  /** Ends any spans that never received a terminal QueueCraft event. */
  close(): void {
    if (this.closed) return;
    this.closed = true;

    for (const { span } of this.active.values()) {
      this.finishSpan(span, "observer_closed");
    }
    this.active.clear();
  }

  get activeSpanCount(): number {
    return this.active.size;
  }

  private startJob(idempotencyKey: string, attempt: number): void {
    const previous = this.active.get(idempotencyKey);
    if (previous) {
      this.finishSpan(previous.span, "superseded");
    }

    const span = this.tracer.startSpan(this.spanName, {
      attributes: {
        ...this.attributes,
        "messaging.system": "aws.sqs",
        "queuecraft.attempt": attempt,
      },
    });
    this.active.set(idempotencyKey, { span });
  }

  private finishJob(
    idempotencyKey: string,
    outcome: string,
    attempt: number,
    durationMs: number,
  ): void {
    const active = this.active.get(idempotencyKey);
    const span =
      active?.span ??
      this.tracer.startSpan(this.spanName, {
        attributes: {
          ...this.attributes,
          "messaging.system": "aws.sqs",
          "queuecraft.attempt": attempt,
          "queuecraft.late_start": true,
        },
      });

    this.active.delete(idempotencyKey);
    this.finishSpan(span, outcome, durationMs);
  }

  private recordInstantSpan(
    name: string,
    attributes: QueueCraftSpanAttributes,
  ): void {
    const span = this.tracer.startSpan(name, {
      attributes: {
        ...this.attributes,
        "messaging.system": "aws.sqs",
        ...attributes,
      },
    });
    this.finishSpan(span, "observed");
  }

  private finishSpan(
    span: QueueCraftTraceSpan,
    outcome: string,
    durationMs?: number,
  ): void {
    try {
      span.setAttribute("queuecraft.outcome", outcome);
      if (durationMs !== undefined) {
        span.setAttribute("queuecraft.duration_ms", durationMs);
      }
    } catch (error) {
      this.reportError(error);
    } finally {
      try {
        span.end();
      } catch (error) {
        this.reportError(error);
      }
    }
  }

  private reportError(error: unknown): void {
    try {
      this.onError?.(error);
    } catch {
      // Observability callbacks must never crash the worker runtime.
    }
  }
}
