import { Message, SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';

/**
 * QueueCraft — core type definitions
 *
 * An AWS SQS background worker with DynamoDB-backed job state.
 */
/** Epoch time in milliseconds, as produced by `Date.now()`. */
type EpochMillis = number;
/** Lifecycle state of a job as it moves through the queue. */
type JobStatus = "pending" | "processing" | "completed" | "failed";
/**
 * A unit of work pulled from SQS and tracked in DynamoDB.
 *
 * @typeParam TPayload - Shape of the job's application-specific data.
 *                       Defaults to `unknown` to force explicit narrowing.
 */
interface Job<TPayload = unknown> {
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
interface QueueCraftConfig {
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
interface WorkerOptions {
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

/**
 * QueueCraft — concurrency control
 *
 * A counting semaphore that bounds how many tasks may run at the same time.
 * Backpressure is handled by queuing callers: `acquire()` resolves immediately
 * while slots are free, and otherwise waits until a slot is released.
 */
declare class Semaphore {
    /** Maximum number of permits that may be held simultaneously. */
    private readonly maxConcurrency;
    /** Number of permits currently held (i.e. tasks running right now). */
    private active;
    /** FIFO queue of callers waiting for a permit. */
    private readonly waiters;
    /**
     * @param maxConcurrency - Upper bound on concurrent tasks. Must be a
     *                         positive integer (see `WorkerOptions.concurrency`).
     */
    constructor(maxConcurrency: number);
    /** Number of tasks currently holding a permit. */
    get activeCount(): number;
    /** Number of callers queued and waiting for a permit. */
    get pendingCount(): number;
    /**
     * Acquire a permit. Resolves immediately if a slot is free, otherwise
     * resolves once another holder calls `release()`.
     *
     * Every successful `acquire()` must be paired with exactly one `release()`.
     * Prefer `run()` where possible so releases are guaranteed.
     */
    acquire(): Promise<void>;
    /**
     * Release a permit. If callers are waiting, the freed slot is handed
     * directly to the next one in line (the active count is unchanged);
     * otherwise the active count is decremented.
     */
    release(): void;
    /**
     * Run a task under a permit, releasing automatically even if it throws.
     * This is the safe, preferred way to use the semaphore.
     *
     * @typeParam T - Resolved value of the task.
     */
    run<T>(task: () => Promise<T>): Promise<T>;
}

/**
 * QueueCraft — idempotency / execution leases
 *
 * Uses a DynamoDB conditional write to guarantee that a given SQS message is
 * processed at most once, even when the same message is delivered more than
 * once (SQS standard queues are at-least-once).
 *
 * Lease lifecycle for a `messageId`:
 *   acquireLock -> PENDING        (in-flight; auto-expires via TTL if the worker dies)
 *     |-- success --> markComplete -> COMPLETED   (terminal; duplicates are skipped)
 *     |-- fatal   --> markFailed   -> FAILED       (terminal; sent to your DLQ logic)
 *     `-- transient/crash --> releaseLock          (record deleted; job may retry)
 */

/** The three states an execution lease can hold. */
declare const LeaseState: {
    readonly Pending: "PENDING";
    readonly Completed: "COMPLETED";
    readonly Failed: "FAILED";
};
type LeaseState = (typeof LeaseState)[keyof typeof LeaseState];
interface IdempotencyStoreOptions {
    /** A configured DynamoDB client (region/credentials handled by the caller). */
    readonly client: DynamoDBClient;
    /** Table whose partition key is `messageId` (String). */
    readonly tableName: string;
    /**
     * Optional lifetime for PENDING leases, in seconds. If the table has TTL
     * enabled on the `expiresAt` attribute, stale locks left behind by a crashed
     * worker are reclaimed automatically after this window. Strongly recommended.
     */
    readonly leaseTtlSeconds?: number;
}
declare class IdempotencyStore {
    private readonly client;
    private readonly tableName;
    private readonly leaseTtlSeconds?;
    constructor(options: IdempotencyStoreOptions);
    /**
     * Attempt to claim an exclusive lease for `messageId`.
     *
     * Backed by a conditional `PutItem` that only writes when no record exists,
     * so concurrent workers racing on the same message are resolved atomically
     * by DynamoDB — exactly one wins.
     *
     * @returns `true` if the lease was acquired, `false` if it already exists
     *          (i.e. the message is in-flight, completed, or failed elsewhere).
     */
    acquireLock(messageId: string): Promise<boolean>;
    /**
     * Mark a lease `COMPLETED` (terminal success). Also clears the TTL so the
     * record persists as a tombstone and future duplicate deliveries of the same
     * message are rejected by `acquireLock`.
     *
     * Guarded by `attribute_exists` so a lost/expired lease surfaces as an error
     * rather than silently resurrecting the record.
     */
    markComplete(messageId: string): Promise<void>;
    /**
     * Mark a lease `FAILED` (terminal failure). Like `markComplete`, the record
     * is kept as a tombstone; route these to your dead-letter handling.
     */
    markFailed(messageId: string): Promise<void>;
    /**
     * Delete the lease so the job becomes eligible for reprocessing. Use this on
     * transient failures or during graceful shutdown. `DeleteItem` is idempotent,
     * so calling it on a missing record is a safe no-op.
     */
    releaseLock(messageId: string): Promise<void>;
    /** Shared transition to a terminal state; clears the pending-lease TTL. */
    private transition;
}

/**
 * QueueCraft — core polling engine
 *
 * Ties the pieces together: long-polls SQS for work, gates concurrency with the
 * Semaphore, enforces exactly-once execution with the IdempotencyStore, and
 * commits or retries each message based on the handler's outcome.
 *
 *   receive -> acquireLock -> handler
 *                              |-- ok   --> deleteMessage + markComplete
 *                              `-- err  --> releaseLock (SQS redelivers)
 */

/**
 * User-supplied unit of work. Receives the raw SQS message so the caller owns
 * body parsing/validation. Throwing (or rejecting) signals failure, which
 * triggers a lease release and SQS redelivery.
 */
type JobHandler = (message: Message) => Promise<void> | void;
interface QueueCraftPollerOptions {
    readonly sqsClient: SQSClient;
    readonly semaphore: Semaphore;
    readonly idempotency: IdempotencyStore;
    /** URL of the SQS queue to poll. */
    readonly queueUrl: string;
    /** Business logic invoked for each successfully leased message. */
    readonly handler: JobHandler;
    /**
     * Concurrency + polling tuning. `concurrency` MUST match the max used to
     * construct the injected Semaphore — it is the capacity ceiling this poller
     * checks before fetching.
     */
    readonly worker: WorkerOptions;
    /** Optional observer for handler/commit/receive errors. Never throws. */
    readonly onError?: (error: unknown, message?: Message) => void;
}
declare class QueueCraftPoller {
    private readonly sqs;
    private readonly semaphore;
    private readonly idempotency;
    private readonly queueUrl;
    private readonly handler;
    private readonly onError?;
    private readonly maxConcurrency;
    private readonly pollIntervalMs;
    private readonly waitTimeSeconds;
    private readonly batchSize;
    private running;
    private readonly inflight;
    private abortController?;
    constructor(options: QueueCraftPollerOptions);
    /** Whether the poll loop is currently active. */
    get isRunning(): boolean;
    /**
     * Run the continuous poll loop until `stop()` is called. Resolves once the
     * loop has exited and all in-flight jobs have drained.
     */
    start(): Promise<void>;
    /** Signal the loop to stop, interrupt any in-flight long poll, and drain. */
    stop(): Promise<void>;
    /** Free slots = ceiling minus in-use, clamped to the SQS batch limit. */
    private availableCapacity;
    private receive;
    private dispatch;
    /** Hold a concurrency slot for the full lifetime of one message. */
    private runWithSlot;
    private process;
    private deleteMessage;
    private safeRelease;
    private drain;
    private sleep;
}

export { type EpochMillis, IdempotencyStore, type IdempotencyStoreOptions, type Job, type JobHandler, type JobStatus, LeaseState, type QueueCraftConfig, QueueCraftPoller, type QueueCraftPollerOptions, Semaphore, type WorkerOptions };
