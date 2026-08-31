import { SQSClient, Message } from '@aws-sdk/client-sqs';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';

/**
 * QueueCraft — publisher
 *
 * Serializes a payload and enqueues it on SQS. Every message carries a
 * client-generated idempotency key in its attributes, so the worker can
 * suppress duplicate logical jobs using a key the producer controls,
 * independent of the SQS-assigned MessageId.
 */

/**
 * Message-attribute name carrying QueueCraft's idempotency key.
 *
 * Import this in the worker so producer and consumer agree on the name. The
 * poller must (a) request it on receive via `MessageAttributeNames` and
 * (b) use its value as the `acquireLock` key.
 */
declare const IDEMPOTENCY_ATTRIBUTE = "QueueCraftIdempotencyKey";
interface QueueCraftPublisherOptions {
    /** A configured SQS client (region/credentials handled by the caller). */
    readonly sqsClient: SQSClient;
    /** Full URL of the destination SQS queue. */
    readonly queueUrl: string;
    /** Override the attribute name used for the idempotency key. */
    readonly idempotencyAttribute?: string;
}
/** Optional per-message knobs. */
interface PublishOptions {
    /**
     * Stable application-level identifier for this logical job.
     *
     * Reuse the same value when retrying a publish, such as a webhook event ID.
     * A UUID is generated only when the caller does not provide one.
     */
    readonly idempotencyKey?: string;
    /** Delay before the message becomes visible, in seconds (0–900). Standard queues only. */
    readonly delaySeconds?: number;
    /** FIFO only: partitions ordering. Required when publishing to a `.fifo` queue. */
    readonly messageGroupId?: string;
    /** FIFO only: deduplication id. Defaults to the generated idempotency key. */
    readonly deduplicationId?: string;
}
interface PublishResult {
    /** Client-generated idempotency key placed in the message attributes. */
    readonly messageId: string;
    /** SQS-assigned message id (distinct from `messageId`), if returned. */
    readonly sqsMessageId?: string;
}
declare class QueueCraftPublisher {
    private readonly sqs;
    private readonly queueUrl;
    private readonly idempotencyAttribute;
    private readonly isFifo;
    constructor(options: QueueCraftPublisherOptions);
    /**
     * Serialize and enqueue a payload. Generates a unique idempotency key,
     * attaches it as a message attribute, and returns it to the caller so the
     * publish can be correlated or safely retried.
     */
    publish(payload: unknown, options?: PublishOptions): Promise<PublishResult>;
}

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
    /** Per-receive SQS visibility timeout. Valid range: 1–43,200 seconds. */
    readonly visibilityTimeoutSeconds?: number;
    /**
     * How often the worker renews SQS visibility and the DynamoDB lease.
     * Must be shorter than the visibility timeout. Defaults to half of it.
     */
    readonly heartbeatIntervalMs?: number;
    /**
     * Time to let active handlers finish after stop() is called before their
     * AbortSignals are cancelled. Defaults to 30 seconds.
     */
    readonly shutdownTimeoutMs?: number;
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
    /** Maximum number of permits this semaphore can issue. */
    get capacity(): number;
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
 * DynamoDB-backed execution leases.
 *
 * DynamoDB TTL is only used for eventual cleanup. Correctness comes from the
 * explicit `leaseUntil` timestamp and an owner token checked on every write.
 */

declare const LeaseState: {
    readonly InProgress: "IN_PROGRESS";
    readonly Completed: "COMPLETED";
    readonly Failed: "FAILED";
};
type LeaseState = (typeof LeaseState)[keyof typeof LeaseState];
interface ExecutionLease {
    readonly messageId: string;
    readonly ownerId: string;
}
type AcquireLockResult = {
    readonly status: "acquired";
    readonly lease: ExecutionLease;
} | {
    readonly status: "in_progress";
} | {
    readonly status: "completed";
} | {
    readonly status: "failed";
};
interface IdempotencyStoreOptions {
    readonly client: DynamoDBClient;
    readonly tableName: string;
    /** How long one worker owns a job before another worker may take it over. */
    readonly leaseDurationSeconds?: number;
    /** How long terminal records remain available for duplicate detection. */
    readonly recordTtlSeconds?: number;
    /** Test hook. Production callers should leave this unset. */
    readonly now?: () => number;
}
declare class IdempotencyStore {
    private readonly client;
    private readonly tableName;
    private readonly leaseDurationSeconds;
    private readonly recordTtlSeconds;
    private readonly now;
    constructor(options: IdempotencyStoreOptions);
    /** Claim a new job or take over an expired IN_PROGRESS lease. */
    acquireLock(messageId: string, ownerId: string): Promise<AcquireLockResult>;
    renewLease(lease: ExecutionLease): Promise<void>;
    markComplete(lease: ExecutionLease): Promise<void>;
    markFailed(lease: ExecutionLease): Promise<void>;
    releaseLock(lease: ExecutionLease): Promise<void>;
    private transition;
    private readState;
    private toAcquireStatus;
    private assertIdentifier;
}

/**
 * QueueCraft — core polling engine
 *
 * Ties the pieces together: long-polls SQS for work, gates concurrency with the
 * Semaphore, suppresses duplicate logical jobs with the IdempotencyStore, and
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
interface JobContext {
    /** Stable logical-job key selected by the producer. */
    readonly idempotencyKey: string;
    /** SQS receive count for this transport message. */
    readonly attempt: number;
    /** Aborted when QueueCraft can no longer prove it owns the job lease. */
    readonly signal: AbortSignal;
}
type JobHandler = (message: Message, context: JobContext) => Promise<void> | void;
/** Structured, payload-free lifecycle events for logs and metrics. */
type QueueCraftEvent = {
    readonly type: "messages_received";
    readonly count: number;
} | {
    readonly type: "job_started";
    readonly idempotencyKey: string;
    readonly attempt: number;
} | {
    readonly type: "job_completed" | "job_failed" | "job_cancelled";
    readonly idempotencyKey: string;
    readonly attempt: number;
    readonly durationMs: number;
} | {
    readonly type: "job_duplicate";
    readonly idempotencyKey: string;
    readonly state: "completed" | "in_progress" | "failed";
} | {
    readonly type: "shutdown_timeout";
    readonly activeJobs: number;
    readonly timeoutMs: number;
};
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
    /** Optional observer for structured lifecycle events. Never throws. */
    readonly onEvent?: (event: QueueCraftEvent) => void;
    /** Message attribute containing the stable application idempotency key. */
    readonly idempotencyAttribute?: string;
}
declare class QueueCraftPoller {
    private readonly sqs;
    private readonly semaphore;
    private readonly idempotency;
    private readonly queueUrl;
    private readonly handler;
    private readonly onError?;
    private readonly onEvent?;
    private readonly idempotencyAttribute;
    private readonly maxConcurrency;
    private readonly pollIntervalMs;
    private readonly waitTimeSeconds;
    private readonly batchSize;
    private readonly visibilityTimeoutSeconds;
    private readonly heartbeatIntervalMs;
    private readonly shutdownTimeoutMs;
    private running;
    private readonly inflight;
    private readonly activeExecutions;
    private abortController?;
    private activeReceive?;
    private sleepController?;
    private shutdownPromise?;
    constructor(options: QueueCraftPollerOptions);
    /** Whether the poll loop is currently active. */
    get isRunning(): boolean;
    /**
     * Run the continuous poll loop until `stop()` is called. Resolves once the
     * loop has exited and active jobs have drained or reached the configured
     * shutdown timeout.
     */
    start(): Promise<void>;
    /**
     * Stop polling, allow active jobs to finish within the configured grace
     * period, then cancel them and return without waiting forever.
     */
    stop(): Promise<void>;
    /** Free slots = ceiling minus in-use, clamped to the SQS batch limit. */
    private availableCapacity;
    private receive;
    private dispatch;
    /** Hold a concurrency slot for the full lifetime of one message. */
    private runWithSlot;
    private process;
    private deleteMessage;
    private changeVisibility;
    private runHeartbeat;
    private waitForHeartbeat;
    private receiveCount;
    private returnUnstartedMessages;
    private safeRelease;
    private validateOptions;
    private assertIntegerInRange;
    private reportError;
    private reportEvent;
    private drain;
    private shutdownAndDrain;
    private performBoundedDrain;
    private drainWithin;
    private sleep;
}

interface LambdaSqsMessageAttribute {
    readonly dataType: string;
    readonly stringValue?: string;
    readonly binaryValue?: string;
}
interface LambdaSqsRecord {
    readonly messageId: string;
    readonly receiptHandle?: string;
    readonly body: string;
    readonly attributes?: Readonly<Record<string, string>>;
    readonly messageAttributes?: Readonly<Record<string, LambdaSqsMessageAttribute>>;
}
interface LambdaSqsEvent {
    readonly Records: readonly LambdaSqsRecord[];
}
interface LambdaBatchItemFailure {
    readonly itemIdentifier: string;
}
interface LambdaSqsBatchResponse {
    readonly batchItemFailures: readonly LambdaBatchItemFailure[];
}
interface QueueCraftLambdaProcessorOptions {
    readonly idempotency: IdempotencyStore;
    readonly handler: JobHandler;
    readonly concurrency?: number;
    readonly idempotencyAttribute?: string;
    readonly onError?: (error: unknown, record?: LambdaSqsRecord) => void;
}
interface LambdaProcessOptions {
    readonly signal?: AbortSignal;
}
/**
 * Processes SQS event-source batches inside AWS Lambda while preserving
 * QueueCraft's DynamoDB duplicate protection. Lambda remains responsible for
 * receiving, deleting, retrying, and redriving SQS messages.
 */
declare class QueueCraftLambdaProcessor {
    private readonly idempotency;
    private readonly handler;
    private readonly semaphore;
    private readonly idempotencyAttribute;
    private readonly onError?;
    constructor(options: QueueCraftLambdaProcessorOptions);
    process(event: LambdaSqsEvent, options?: LambdaProcessOptions): Promise<LambdaSqsBatchResponse>;
    private processRecord;
    private toSdkMessage;
    private receiveCount;
    private safeRelease;
    private reportError;
}

interface QueueCraftDashboardOptions {
    readonly sqsClient: SQSClient;
    readonly queueUrl: string;
    readonly dlqUrl: string;
    readonly host?: string;
    readonly port?: number;
    readonly title?: string;
    readonly replayCacheTtlMs?: number;
    /** Receives server-side failures without exposing AWS details to the page. */
    readonly onError?: (error: unknown) => void;
}
interface QueueCraftDashboard {
    readonly url: string;
    close(): Promise<void>;
}
declare function createQueueCraftDashboard(options: QueueCraftDashboardOptions): Promise<QueueCraftDashboard>;

export { type AcquireLockResult, type EpochMillis, type ExecutionLease, IDEMPOTENCY_ATTRIBUTE, IdempotencyStore, type IdempotencyStoreOptions, type Job, type JobContext, type JobHandler, type JobStatus, type LambdaBatchItemFailure, type LambdaProcessOptions, type LambdaSqsBatchResponse, type LambdaSqsEvent, type LambdaSqsMessageAttribute, type LambdaSqsRecord, LeaseState, type PublishOptions, type PublishResult, type QueueCraftConfig, type QueueCraftDashboard, type QueueCraftDashboardOptions, type QueueCraftEvent, QueueCraftLambdaProcessor, type QueueCraftLambdaProcessorOptions, QueueCraftPoller, type QueueCraftPollerOptions, QueueCraftPublisher, type QueueCraftPublisherOptions, Semaphore, type WorkerOptions, createQueueCraftDashboard };
