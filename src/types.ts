/**
 * QueueCraft — core type definitions
 *
 * An AWS SQS background worker with DynamoDB-backed job state.
 */

/** Epoch time in milliseconds, as produced by `Date.now()`. */
export type EpochMillis = number;

/** Lifecycle state of a job as it moves through the queue. */
export type JobStatus = "pending" | "processing" | "completed" | "failed";

/**
 * A unit of work pulled from SQS and tracked in DynamoDB.
 *
 * @typeParam TPayload - Shape of the job's application-specific data.
 *                       Defaults to `unknown` to force explicit narrowing.
 */
export interface Job<TPayload = unknown> {
  /** Unique job identifier (e.g. a UUID v4). */
  readonly id: string;

  /** Application-defined data required to process the job. */
  readonly payload: TPayload;

  /** Current lifecycle state. */
  readonly status: JobStatus;

  /** Number of processing attempts made so far. */
  readonly attempts: number;

  /** When the job was first created. */
  readonly createdAt: EpochMillis;

  /** When the job record was last updated. */
  readonly updatedAt: EpochMillis;
}

/**
 * Static configuration for a QueueCraft instance.
 *
 * All fields point at concrete AWS resources and are required at startup.
 */
export interface QueueCraftConfig {
  /** AWS region hosting the queue and table (e.g. "me-central-1"). */
  readonly region: string;

  /** Full URL of the SQS queue to poll. */
  readonly queueUrl: string;

  /** Name of the DynamoDB table used for job state and idempotency. */
  readonly tableName: string;
}

/**
 * Tunable runtime behaviour for a worker process.
 *
 * Required fields govern throughput; optional fields map onto SQS
 * receive-message parameters and retry policy with sensible defaults.
 */
export interface WorkerOptions {
  /** Maximum number of jobs processed concurrently. Must be >= 1. */
  readonly concurrency: number;

  /** Delay between polls when the queue is empty, in milliseconds. */
  readonly pollIntervalMs: number;

  /** SQS long-poll wait time, in seconds. Valid range: 0–20. */
  readonly waitTimeSeconds?: number;

  /** Messages requested per poll. Valid range: 1–10. */
  readonly batchSize?: number;

  /** Maximum attempts before a job is marked `failed`. */
  readonly maxRetries?: number;
}