/**
 * QueueCraft public API.
 *
 * Consumers import package functionality from the package root:
 *
 * import {
 *   QueueCraftPublisher,
 *   QueueCraftPoller,
 *   Semaphore,
 *   IdempotencyStore,
 * } from "@yusufkaranib/queuecraft";
 */

// Publisher
export {
  QueueCraftPublisher,
  IDEMPOTENCY_ATTRIBUTE,
} from "./publisher";

export type {
  QueueCraftPublisherOptions,
  PublishOptions,
  PublishResult,
} from "./publisher";

// Core engine
export { QueueCraftPoller } from "./poller";

export type {
  JobContext,
  QueueCraftPollerOptions,
  QueueCraftEvent,
  JobHandler,
} from "./poller";

// AWS Lambda SQS event-source adapter
export { QueueCraftLambdaProcessor } from "./lambda-processor";

export type {
  LambdaBatchItemFailure,
  LambdaProcessOptions,
  LambdaSqsBatchResponse,
  LambdaSqsEvent,
  LambdaSqsMessageAttribute,
  LambdaSqsRecord,
  QueueCraftLambdaProcessorOptions,
} from "./lambda-processor";

// Concurrency
export { Semaphore } from "./semaphore";

// Idempotency and execution leases
export {
  IdempotencyStore,
  LeaseState,
} from "./idempotency";

export type {
  AcquireLockResult,
  ExecutionLease,
  IdempotencyStoreOptions,
} from "./idempotency";

// Shared domain types
export type {
  Job,
  JobStatus,
  EpochMillis,
  QueueCraftConfig,
  WorkerOptions,
} from "./types";
