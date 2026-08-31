import {
  PutMetricDataCommand,
  type Dimension,
  type MetricDatum,
} from "@aws-sdk/client-cloudwatch";
import type { QueueCraftEvent } from "./poller";

const DEFAULT_NAMESPACE = "QueueCraft";
const DEFAULT_BATCH_SIZE = 20;
const DEFAULT_FLUSH_INTERVAL_MS = 10_000;
const MAX_METRICS_PER_REQUEST = 1_000;
const MAX_USER_DIMENSIONS = 29;

export interface QueueCraftCloudWatchMetricMappingOptions {
  /** Low-cardinality dimensions applied to every metric. */
  readonly dimensions?: Readonly<Record<string, string>>;

  /** Clock override, primarily for deterministic tests. */
  readonly now?: () => Date;
}

export interface QueueCraftCloudWatchClient {
  send(command: PutMetricDataCommand): Promise<unknown>;
}

export interface QueueCraftCloudWatchMetricsOptions
  extends QueueCraftCloudWatchMetricMappingOptions {
  readonly client: QueueCraftCloudWatchClient;

  /** Custom metric namespace. Must not start with the reserved `AWS/` prefix. */
  readonly namespace?: string;

  /** Number of metric data points sent in one request. Valid range: 1-1000. */
  readonly maxBatchSize?: number;

  /** Maximum time metrics remain buffered. Set to 0 for manual flushing only. */
  readonly flushIntervalMs?: number;

  /** Optional observer for CloudWatch delivery errors. Never throws. */
  readonly onError?: (error: unknown) => void;
}

/**
 * Converts one payload-free QueueCraft event into low-cardinality CloudWatch
 * metric data. Idempotency keys are deliberately never copied into metrics.
 */
export function mapQueueCraftEventToCloudWatchMetrics(
  event: QueueCraftEvent,
  options: QueueCraftCloudWatchMetricMappingOptions = {},
): readonly MetricDatum[] {
  validateDimensions(options.dimensions);
  const dimensions = toDimensions(options.dimensions);
  const timestamp = (options.now ?? (() => new Date()))();
  const metric = (
    name: string,
    value: number,
    unit: "Count" | "Milliseconds",
    extraDimensions: readonly Dimension[] = [],
  ): MetricDatum => ({
    MetricName: name,
    Value: value,
    Unit: unit,
    Timestamp: timestamp,
    Dimensions: [...dimensions, ...extraDimensions],
  });

  switch (event.type) {
    case "messages_received":
      return [metric("MessagesReceived", event.count, "Count")];
    case "job_started":
      return [metric("JobsStarted", 1, "Count")];
    case "job_completed":
    case "job_failed":
    case "job_cancelled": {
      const outcome = event.type.slice("job_".length);
      const metricName =
        event.type === "job_completed"
          ? "JobsCompleted"
          : event.type === "job_failed"
            ? "JobsFailed"
            : "JobsCancelled";
      return [
        metric(metricName, 1, "Count"),
        metric("JobDuration", event.durationMs, "Milliseconds", [
          { Name: "Outcome", Value: outcome },
        ]),
      ];
    }
    case "job_duplicate":
      return [
        metric("JobsDuplicate", 1, "Count", [
          { Name: "DuplicateState", Value: event.state },
        ]),
      ];
    case "shutdown_timeout":
      return [
        metric("ShutdownTimeouts", 1, "Count"),
        metric("ActiveJobsAtShutdown", event.activeJobs, "Count"),
        metric("ShutdownTimeout", event.timeoutMs, "Milliseconds"),
      ];
  }
}

/** Buffered CloudWatch writer designed to be passed directly to `onEvent`. */
export class QueueCraftCloudWatchMetrics {
  private readonly client: QueueCraftCloudWatchClient;
  private readonly namespace: string;
  private readonly mappingOptions: QueueCraftCloudWatchMetricMappingOptions;
  private readonly maxBatchSize: number;
  private readonly flushIntervalMs: number;
  private readonly onError?: (error: unknown) => void;
  private readonly pending: MetricDatum[] = [];
  private timer?: ReturnType<typeof setTimeout>;
  private activeFlush?: Promise<void>;
  private closed = false;

  constructor(options: QueueCraftCloudWatchMetricsOptions) {
    this.client = options.client;
    this.namespace = options.namespace ?? DEFAULT_NAMESPACE;
    this.maxBatchSize = options.maxBatchSize ?? DEFAULT_BATCH_SIZE;
    this.flushIntervalMs =
      options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.onError = options.onError;
    this.mappingOptions = {
      dimensions: options.dimensions,
      now: options.now,
    };

    this.validateOptions(options.dimensions);
  }

  /** Synchronous, failure-isolated observer for `QueueCraftPoller.onEvent`. */
  readonly onEvent = (event: QueueCraftEvent): void => {
    if (this.closed) return;

    try {
      this.pending.push(
        ...mapQueueCraftEventToCloudWatchMetrics(event, this.mappingOptions),
      );
      if (this.pending.length >= this.maxBatchSize) {
        this.clearTimer();
        void this.flush().catch((error) => this.reportError(error));
      } else {
        this.scheduleFlush();
      }
    } catch (error) {
      this.reportError(error);
    }
  };

  /** Sends all currently buffered metrics. Failed batches stay queued for retry. */
  async flush(): Promise<void> {
    if (this.activeFlush) {
      await this.activeFlush;
      return;
    }

    this.clearTimer();
    const operation = this.flushPending();
    this.activeFlush = operation;
    try {
      await operation;
    } finally {
      if (this.activeFlush === operation) {
        this.activeFlush = undefined;
      }
      if (!this.closed && this.pending.length > 0) {
        this.scheduleFlush();
      }
    }
  }

  /** Stops the timer and flushes remaining metrics. */
  async close(): Promise<void> {
    this.closed = true;
    this.clearTimer();
    await this.flush();
  }

  get pendingMetricCount(): number {
    return this.pending.length;
  }

  private async flushPending(): Promise<void> {
    while (this.pending.length > 0) {
      const batch = this.pending.splice(0, this.maxBatchSize);
      try {
        await this.client.send(
          new PutMetricDataCommand({
            Namespace: this.namespace,
            MetricData: batch,
          }),
        );
      } catch (error) {
        this.pending.unshift(...batch);
        throw error;
      }
    }
  }

  private scheduleFlush(): void {
    if (this.timer || this.flushIntervalMs === 0 || this.closed) return;

    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.flush().catch((error) => this.reportError(error));
    }, this.flushIntervalMs);
    this.timer.unref?.();
  }

  private clearTimer(): void {
    if (!this.timer) return;
    clearTimeout(this.timer);
    this.timer = undefined;
  }

  private reportError(error: unknown): void {
    try {
      this.onError?.(error);
    } catch {
      // Observability callbacks must never crash the worker runtime.
    }
  }

  private validateOptions(
    dimensions: Readonly<Record<string, string>> | undefined,
  ): void {
    if (!this.namespace.trim() || this.namespace.startsWith("AWS/")) {
      throw new RangeError(
        "namespace must be non-empty and must not start with the reserved AWS/ prefix.",
      );
    }
    if (
      !Number.isInteger(this.maxBatchSize) ||
      this.maxBatchSize < 1 ||
      this.maxBatchSize > MAX_METRICS_PER_REQUEST
    ) {
      throw new RangeError("maxBatchSize must be an integer between 1 and 1000.");
    }
    if (!Number.isInteger(this.flushIntervalMs) || this.flushIntervalMs < 0) {
      throw new RangeError("flushIntervalMs must be a non-negative integer.");
    }

    validateDimensions(dimensions);
  }
}

function validateDimensions(
  dimensions: Readonly<Record<string, string>> | undefined,
): void {
  const entries = Object.entries(dimensions ?? {});
  if (entries.length > MAX_USER_DIMENSIONS) {
    throw new RangeError(
      "dimensions can contain at most 29 entries so QueueCraft can add one bounded event dimension.",
    );
  }
  for (const [name, value] of entries) {
    if (!name.trim() || !value.trim()) {
      throw new RangeError("dimension names and values must be non-empty.");
    }
  }
}

function toDimensions(
  dimensions: Readonly<Record<string, string>> | undefined,
): readonly Dimension[] {
  return Object.entries(dimensions ?? {}).map(([Name, Value]) => ({
    Name,
    Value,
  }));
}
