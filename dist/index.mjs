// src/poller.ts
import {
  ReceiveMessageCommand,
  DeleteMessageCommand
} from "@aws-sdk/client-sqs";
var MAX_SQS_BATCH = 10;
var QueueCraftPoller = class {
  sqs;
  semaphore;
  idempotency;
  queueUrl;
  handler;
  onError;
  maxConcurrency;
  pollIntervalMs;
  waitTimeSeconds;
  batchSize;
  running = false;
  inflight = /* @__PURE__ */ new Set();
  abortController;
  constructor(options) {
    this.sqs = options.sqsClient;
    this.semaphore = options.semaphore;
    this.idempotency = options.idempotency;
    this.queueUrl = options.queueUrl;
    this.handler = options.handler;
    this.onError = options.onError;
    this.maxConcurrency = options.worker.concurrency;
    this.pollIntervalMs = options.worker.pollIntervalMs;
    this.waitTimeSeconds = options.worker.waitTimeSeconds ?? 20;
    this.batchSize = options.worker.batchSize ?? MAX_SQS_BATCH;
  }
  /** Whether the poll loop is currently active. */
  get isRunning() {
    return this.running;
  }
  /**
   * Run the continuous poll loop until `stop()` is called. Resolves once the
   * loop has exited and all in-flight jobs have drained.
   */
  async start() {
    var _a;
    if (this.running) return;
    this.running = true;
    while (this.running) {
      const capacity = this.availableCapacity();
      if (capacity <= 0) {
        await this.sleep(this.pollIntervalMs);
        continue;
      }
      let messages;
      try {
        messages = await this.receive(capacity);
      } catch (err) {
        if (!this.running) break;
        (_a = this.onError) == null ? void 0 : _a.call(this, err);
        await this.sleep(this.pollIntervalMs);
        continue;
      }
      for (const message of messages) {
        this.dispatch(message);
      }
    }
    await this.drain();
  }
  /** Signal the loop to stop, interrupt any in-flight long poll, and drain. */
  async stop() {
    var _a;
    this.running = false;
    (_a = this.abortController) == null ? void 0 : _a.abort();
    await this.drain();
  }
  /** Free slots = ceiling minus in-use, clamped to the SQS batch limit. */
  availableCapacity() {
    const inUse = this.semaphore.activeCount + this.semaphore.pendingCount;
    const free = this.maxConcurrency - inUse;
    return Math.max(0, Math.min(free, this.batchSize, MAX_SQS_BATCH));
  }
  async receive(max) {
    this.abortController = new AbortController();
    const result = await this.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: max,
        WaitTimeSeconds: this.waitTimeSeconds
      }),
      { abortSignal: this.abortController.signal }
    );
    return result.Messages ?? [];
  }
  dispatch(message) {
    const task = this.runWithSlot(message);
    this.inflight.add(task);
    void task.finally(() => this.inflight.delete(task));
  }
  /** Hold a concurrency slot for the full lifetime of one message. */
  async runWithSlot(message) {
    await this.semaphore.acquire();
    try {
      await this.process(message);
    } finally {
      this.semaphore.release();
    }
  }
  async process(message) {
    var _a, _b, _c;
    const messageId = message.MessageId;
    const receiptHandle = message.ReceiptHandle;
    if (!messageId || !receiptHandle) {
      (_a = this.onError) == null ? void 0 : _a.call(
        this,
        new Error("SQS message missing MessageId or ReceiptHandle"),
        message
      );
      return;
    }
    const acquired = await this.idempotency.acquireLock(messageId);
    if (!acquired) {
      return;
    }
    try {
      await this.handler(message);
    } catch (err) {
      await this.safeRelease(messageId);
      (_b = this.onError) == null ? void 0 : _b.call(this, err, message);
      return;
    }
    try {
      await this.deleteMessage(receiptHandle);
      await this.idempotency.markComplete(messageId);
    } catch (err) {
      (_c = this.onError) == null ? void 0 : _c.call(this, err, message);
    }
  }
  async deleteMessage(receiptHandle) {
    await this.sqs.send(
      new DeleteMessageCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: receiptHandle
      })
    );
  }
  async safeRelease(messageId) {
    var _a;
    try {
      await this.idempotency.releaseLock(messageId);
    } catch (err) {
      (_a = this.onError) == null ? void 0 : _a.call(this, err);
    }
  }
  async drain() {
    await Promise.allSettled([...this.inflight]);
  }
  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
};

// src/semaphore.ts
var Semaphore = class {
  /** Maximum number of permits that may be held simultaneously. */
  maxConcurrency;
  /** Number of permits currently held (i.e. tasks running right now). */
  active = 0;
  /** FIFO queue of callers waiting for a permit. */
  waiters = [];
  /**
   * @param maxConcurrency - Upper bound on concurrent tasks. Must be a
   *                         positive integer (see `WorkerOptions.concurrency`).
   */
  constructor(maxConcurrency) {
    if (!Number.isInteger(maxConcurrency) || maxConcurrency < 1) {
      throw new RangeError(
        `maxConcurrency must be a positive integer, received: ${maxConcurrency}`
      );
    }
    this.maxConcurrency = maxConcurrency;
  }
  /** Number of tasks currently holding a permit. */
  get activeCount() {
    return this.active;
  }
  /** Number of callers queued and waiting for a permit. */
  get pendingCount() {
    return this.waiters.length;
  }
  /**
   * Acquire a permit. Resolves immediately if a slot is free, otherwise
   * resolves once another holder calls `release()`.
   *
   * Every successful `acquire()` must be paired with exactly one `release()`.
   * Prefer `run()` where possible so releases are guaranteed.
   */
  acquire() {
    if (this.active < this.maxConcurrency) {
      this.active++;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }
  /**
   * Release a permit. If callers are waiting, the freed slot is handed
   * directly to the next one in line (the active count is unchanged);
   * otherwise the active count is decremented.
   */
  release() {
    const next = this.waiters.shift();
    if (next) {
      next();
    } else if (this.active > 0) {
      this.active--;
    }
  }
  /**
   * Run a task under a permit, releasing automatically even if it throws.
   * This is the safe, preferred way to use the semaphore.
   *
   * @typeParam T - Resolved value of the task.
   */
  async run(task) {
    await this.acquire();
    try {
      return await task();
    } finally {
      this.release();
    }
  }
};

// src/idempotency.ts
import {
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  ConditionalCheckFailedException
} from "@aws-sdk/client-dynamodb";
var LeaseState = {
  Pending: "PENDING",
  Completed: "COMPLETED",
  Failed: "FAILED"
};
var IdempotencyStore = class {
  client;
  tableName;
  leaseTtlSeconds;
  constructor(options) {
    if (!options.tableName) {
      throw new Error("IdempotencyStore requires a non-empty tableName.");
    }
    this.client = options.client;
    this.tableName = options.tableName;
    this.leaseTtlSeconds = options.leaseTtlSeconds;
  }
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
  async acquireLock(messageId) {
    const now = Date.now();
    const item = {
      messageId: { S: messageId },
      state: { S: LeaseState.Pending },
      createdAt: { N: String(now) },
      updatedAt: { N: String(now) }
    };
    if (this.leaseTtlSeconds !== void 0) {
      item.expiresAt = {
        N: String(Math.floor(now / 1e3) + this.leaseTtlSeconds)
      };
    }
    try {
      await this.client.send(
        new PutItemCommand({
          TableName: this.tableName,
          Item: item,
          ConditionExpression: "attribute_not_exists(messageId)"
        })
      );
      return true;
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) {
        return false;
      }
      throw err;
    }
  }
  /**
   * Mark a lease `COMPLETED` (terminal success). Also clears the TTL so the
   * record persists as a tombstone and future duplicate deliveries of the same
   * message are rejected by `acquireLock`.
   *
   * Guarded by `attribute_exists` so a lost/expired lease surfaces as an error
   * rather than silently resurrecting the record.
   */
  async markComplete(messageId) {
    await this.transition(messageId, LeaseState.Completed);
  }
  /**
   * Mark a lease `FAILED` (terminal failure). Like `markComplete`, the record
   * is kept as a tombstone; route these to your dead-letter handling.
   */
  async markFailed(messageId) {
    await this.transition(messageId, LeaseState.Failed);
  }
  /**
   * Delete the lease so the job becomes eligible for reprocessing. Use this on
   * transient failures or during graceful shutdown. `DeleteItem` is idempotent,
   * so calling it on a missing record is a safe no-op.
   */
  async releaseLock(messageId) {
    await this.client.send(
      new DeleteItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: messageId } }
      })
    );
  }
  /** Shared transition to a terminal state; clears the pending-lease TTL. */
  async transition(messageId, state) {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: messageId } },
        UpdateExpression: "SET #state = :state, #updatedAt = :updatedAt REMOVE expiresAt",
        ConditionExpression: "attribute_exists(messageId)",
        ExpressionAttributeNames: {
          "#state": "state",
          "#updatedAt": "updatedAt"
        },
        ExpressionAttributeValues: {
          ":state": { S: state },
          ":updatedAt": { N: String(Date.now()) }
        }
      })
    );
  }
};
export {
  IdempotencyStore,
  LeaseState,
  QueueCraftPoller,
  Semaphore
};
