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
 * } from "queuecraft";
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
  QueueCraftPollerOptions,
  JobHandler,
} from "./poller";

// Concurrency
export { Semaphore } from "./semaphore";

// Idempotency and execution leases
export {
  IdempotencyStore,
  LeaseState,
} from "./idempotency";

export type {
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