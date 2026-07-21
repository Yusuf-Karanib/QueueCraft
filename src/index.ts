/**
 * QueueCraft — public API surface.
 *
 * Entry point for consumers. Everything needed to assemble and run a worker is
 * re-exported here, so downstream code imports from the package root:
 *
 *   import {
 *     QueueCraftPoller,
 *     Semaphore,
 *     IdempotencyStore,
 *     type QueueCraftConfig,
 *     type WorkerOptions,
 *     type JobHandler,
 *   } from "queuecraft";
 */

// Core engine
export { QueueCraftPoller } from "./poller";
export type { QueueCraftPollerOptions, JobHandler } from "./poller";

// Concurrency primitive
export { Semaphore } from "./semaphore";

// Idempotency / execution leases
export { IdempotencyStore, LeaseState } from "./idempotency";
export type { IdempotencyStoreOptions } from "./idempotency";

// Shared domain types
export type {
  Job,
  JobStatus,
  EpochMillis,
  QueueCraftConfig,
  WorkerOptions,
} from "./types";