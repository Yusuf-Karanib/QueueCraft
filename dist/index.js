// src/publisher.ts
import {
  SendMessageCommand
} from "@aws-sdk/client-sqs";
import { randomUUID } from "crypto";
var IDEMPOTENCY_ATTRIBUTE = "QueueCraftIdempotencyKey";
var QueueCraftPublisher = class {
  sqs;
  queueUrl;
  idempotencyAttribute;
  isFifo;
  constructor(options) {
    if (!options.queueUrl) {
      throw new Error("QueueCraftPublisher requires a non-empty queueUrl.");
    }
    this.sqs = options.sqsClient;
    this.queueUrl = options.queueUrl;
    this.idempotencyAttribute = options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
    this.isFifo = options.queueUrl.endsWith(".fifo");
  }
  /**
   * Serialize and enqueue a payload. Generates a unique idempotency key,
   * attaches it as a message attribute, and returns it to the caller so the
   * publish can be correlated or safely retried.
   */
  async publish(payload, options = {}) {
    const body = JSON.stringify(payload);
    if (body === void 0) {
      throw new TypeError(
        "publish(payload): payload must be JSON-serializable and not undefined."
      );
    }
    const messageId = options.idempotencyKey ?? randomUUID();
    if (!messageId) {
      throw new TypeError("idempotencyKey must be a non-empty string.");
    }
    const attributes = {
      [this.idempotencyAttribute]: {
        DataType: "String",
        StringValue: messageId
      }
    };
    const result = await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: body,
        MessageAttributes: attributes,
        // Per-message delay is a standard-queue feature only.
        DelaySeconds: this.isFifo ? void 0 : options.delaySeconds,
        // FIFO-only fields; omitted entirely for standard queues.
        MessageGroupId: this.isFifo ? options.messageGroupId : void 0,
        MessageDeduplicationId: this.isFifo ? options.deduplicationId ?? messageId : void 0
      })
    );
    return { messageId, sqsMessageId: result.MessageId };
  }
};

// src/poller.ts
import {
  ChangeMessageVisibilityCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand
} from "@aws-sdk/client-sqs";
import { randomUUID as randomUUID2 } from "crypto";
var MAX_SQS_BATCH = 10;
var QueueCraftPoller = class {
  sqs;
  semaphore;
  idempotency;
  queueUrl;
  handler;
  onError;
  idempotencyAttribute;
  maxConcurrency;
  pollIntervalMs;
  waitTimeSeconds;
  batchSize;
  visibilityTimeoutSeconds;
  heartbeatIntervalMs;
  running = false;
  inflight = /* @__PURE__ */ new Set();
  abortController;
  activeReceive;
  constructor(options) {
    this.sqs = options.sqsClient;
    this.semaphore = options.semaphore;
    this.idempotency = options.idempotency;
    this.queueUrl = options.queueUrl;
    this.handler = options.handler;
    this.onError = options.onError;
    this.idempotencyAttribute = options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
    this.maxConcurrency = options.worker.concurrency;
    this.pollIntervalMs = options.worker.pollIntervalMs;
    this.waitTimeSeconds = options.worker.waitTimeSeconds ?? 20;
    this.batchSize = options.worker.batchSize ?? MAX_SQS_BATCH;
    this.visibilityTimeoutSeconds = options.worker.visibilityTimeoutSeconds ?? 60;
    this.heartbeatIntervalMs = options.worker.heartbeatIntervalMs ?? Math.floor(this.visibilityTimeoutSeconds * 1e3 / 2);
    this.validateOptions(options.worker.concurrency);
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
        this.reportError(err);
        await this.sleep(this.pollIntervalMs);
        continue;
      }
      if (!this.running) {
        await this.returnUnstartedMessages(messages);
        break;
      }
      for (const message of messages) {
        this.dispatch(message);
      }
    }
    await this.drain();
  }
  /** Signal the loop to stop, interrupt any in-flight long poll, and drain. */
  async stop() {
    this.running = false;
    this.abortController?.abort();
    await this.activeReceive?.catch(() => void 0);
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
    const request = this.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: max,
        WaitTimeSeconds: this.waitTimeSeconds,
        VisibilityTimeout: this.visibilityTimeoutSeconds,
        MessageAttributeNames: [this.idempotencyAttribute],
        MessageSystemAttributeNames: ["ApproximateReceiveCount"]
      }),
      { abortSignal: this.abortController.signal }
    ).then((result) => result.Messages ?? []);
    this.activeReceive = request;
    try {
      return await request;
    } finally {
      if (this.activeReceive === request) {
        this.activeReceive = void 0;
      }
    }
  }
  dispatch(message) {
    const task = this.runWithSlot(message).catch((error) => {
      this.reportError(error, message);
    });
    this.inflight.add(task);
    void task.then(() => this.inflight.delete(task));
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
    const messageId = message.MessageId;
    const receiptHandle = message.ReceiptHandle;
    if (!messageId || !receiptHandle) {
      this.reportError(
        new Error("SQS message missing MessageId or ReceiptHandle"),
        message
      );
      return;
    }
    const idempotencyKey = message.MessageAttributes?.[this.idempotencyAttribute]?.StringValue ?? messageId;
    const ownerId = randomUUID2();
    const acquisition = await this.idempotency.acquireLock(
      idempotencyKey,
      ownerId
    );
    if (acquisition.status === "completed") {
      await this.deleteMessage(receiptHandle);
      return;
    }
    if (acquisition.status !== "acquired") {
      return;
    }
    const lease = acquisition.lease;
    const handlerController = new AbortController();
    const heartbeatController = new AbortController();
    let heartbeatError;
    const heartbeat = this.runHeartbeat(
      lease,
      receiptHandle,
      heartbeatController.signal
    ).catch((error) => {
      heartbeatError = error;
      handlerController.abort(error);
      this.reportError(error, message);
    });
    const attempt = this.receiveCount(message);
    let handlerError;
    try {
      await this.handler(message, {
        idempotencyKey,
        attempt,
        signal: handlerController.signal
      });
    } catch (error) {
      handlerError = error;
    } finally {
      heartbeatController.abort();
      await heartbeat;
    }
    if (heartbeatError !== void 0) {
      return;
    }
    if (handlerError !== void 0) {
      await this.safeRelease(lease);
      this.reportError(handlerError, message);
      return;
    }
    try {
      await this.idempotency.markComplete(lease);
      await this.deleteMessage(receiptHandle);
    } catch (err) {
      this.reportError(err, message);
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
  async changeVisibility(receiptHandle, visibilityTimeout = this.visibilityTimeoutSeconds) {
    await this.sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: visibilityTimeout
      })
    );
  }
  async runHeartbeat(lease, receiptHandle, signal) {
    while (await this.waitForHeartbeat(signal)) {
      await this.idempotency.renewLease(lease);
      await this.changeVisibility(receiptHandle);
    }
  }
  waitForHeartbeat(signal) {
    if (signal.aborted) return Promise.resolve(false);
    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        signal.removeEventListener("abort", onAbort);
        resolve(true);
      }, this.heartbeatIntervalMs);
      const onAbort = () => {
        clearTimeout(timeout);
        resolve(false);
      };
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }
  receiveCount(message) {
    const parsed = Number(message.Attributes?.ApproximateReceiveCount ?? "1");
    return Number.isInteger(parsed) && parsed > 0 ? parsed : 1;
  }
  async returnUnstartedMessages(messages) {
    const returns = messages.flatMap(
      (message) => message.ReceiptHandle ? [this.changeVisibility(message.ReceiptHandle, 0)] : []
    );
    const results = await Promise.allSettled(returns);
    for (const result of results) {
      if (result.status === "rejected") {
        this.reportError(result.reason);
      }
    }
  }
  async safeRelease(lease) {
    try {
      await this.idempotency.releaseLock(lease);
    } catch (err) {
      this.reportError(err);
    }
  }
  validateOptions(concurrency) {
    this.assertIntegerInRange(concurrency, "concurrency", 1, Number.MAX_SAFE_INTEGER);
    if (concurrency !== this.semaphore.capacity) {
      throw new RangeError(
        "worker.concurrency must match the injected Semaphore capacity."
      );
    }
    this.assertIntegerInRange(
      this.pollIntervalMs,
      "pollIntervalMs",
      0,
      Number.MAX_SAFE_INTEGER
    );
    this.assertIntegerInRange(this.waitTimeSeconds, "waitTimeSeconds", 0, 20);
    this.assertIntegerInRange(this.batchSize, "batchSize", 1, MAX_SQS_BATCH);
    this.assertIntegerInRange(
      this.visibilityTimeoutSeconds,
      "visibilityTimeoutSeconds",
      1,
      43200
    );
    this.assertIntegerInRange(
      this.heartbeatIntervalMs,
      "heartbeatIntervalMs",
      1,
      this.visibilityTimeoutSeconds * 1e3 - 1
    );
  }
  assertIntegerInRange(value, name, minimum, maximum) {
    if (!Number.isInteger(value) || value < minimum || value > maximum) {
      throw new RangeError(
        `${name} must be an integer between ${minimum} and ${maximum}.`
      );
    }
  }
  reportError(error, message) {
    try {
      this.onError?.(error, message);
    } catch {
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
  /** Maximum number of permits this semaphore can issue. */
  get capacity() {
    return this.maxConcurrency;
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
  ConditionalCheckFailedException,
  DeleteItemCommand,
  GetItemCommand,
  UpdateItemCommand
} from "@aws-sdk/client-dynamodb";
var LeaseState = {
  InProgress: "IN_PROGRESS",
  Completed: "COMPLETED",
  Failed: "FAILED"
};
var DEFAULT_LEASE_SECONDS = 60;
var DEFAULT_RECORD_TTL_SECONDS = 14 * 24 * 60 * 60;
var IdempotencyStore = class {
  client;
  tableName;
  leaseDurationSeconds;
  recordTtlSeconds;
  now;
  constructor(options) {
    if (!options.tableName) {
      throw new Error("IdempotencyStore requires a non-empty tableName.");
    }
    this.leaseDurationSeconds = options.leaseDurationSeconds ?? DEFAULT_LEASE_SECONDS;
    this.recordTtlSeconds = options.recordTtlSeconds ?? DEFAULT_RECORD_TTL_SECONDS;
    if (!Number.isInteger(this.leaseDurationSeconds) || this.leaseDurationSeconds < 1) {
      throw new RangeError("leaseDurationSeconds must be a positive integer.");
    }
    if (!Number.isInteger(this.recordTtlSeconds) || this.recordTtlSeconds < 1) {
      throw new RangeError("recordTtlSeconds must be a positive integer.");
    }
    this.client = options.client;
    this.tableName = options.tableName;
    this.now = options.now ?? Date.now;
  }
  /** Claim a new job or take over an expired IN_PROGRESS lease. */
  async acquireLock(messageId, ownerId) {
    this.assertIdentifier(messageId, "messageId");
    this.assertIdentifier(ownerId, "ownerId");
    for (let attempt = 0; attempt < 2; attempt++) {
      const nowSeconds = Math.floor(this.now() / 1e3);
      const leaseUntil = nowSeconds + this.leaseDurationSeconds;
      try {
        await this.client.send(
          new UpdateItemCommand({
            TableName: this.tableName,
            Key: { messageId: { S: messageId } },
            UpdateExpression: "SET #state = :inProgress, #ownerId = :ownerId, #leaseUntil = :leaseUntil, #updatedAt = :now, #createdAt = if_not_exists(#createdAt, :now), #expiresAt = :expiresAt",
            ConditionExpression: "attribute_not_exists(messageId) OR (#state = :inProgress AND #leaseUntil <= :now)",
            ExpressionAttributeNames: {
              "#state": "state",
              "#ownerId": "ownerId",
              "#leaseUntil": "leaseUntil",
              "#updatedAt": "updatedAt",
              "#createdAt": "createdAt",
              "#expiresAt": "expiresAt"
            },
            ExpressionAttributeValues: {
              ":inProgress": { S: LeaseState.InProgress },
              ":ownerId": { S: ownerId },
              ":leaseUntil": { N: String(leaseUntil) },
              ":now": { N: String(nowSeconds) },
              ":expiresAt": {
                N: String(nowSeconds + this.recordTtlSeconds)
              }
            }
          })
        );
        return {
          status: "acquired",
          lease: { messageId, ownerId }
        };
      } catch (error) {
        if (!(error instanceof ConditionalCheckFailedException)) {
          throw error;
        }
        const existingState = await this.readState(messageId);
        if (existingState === void 0) {
          continue;
        }
        return { status: this.toAcquireStatus(existingState) };
      }
    }
    return { status: "in_progress" };
  }
  async renewLease(lease) {
    const nowSeconds = Math.floor(this.now() / 1e3);
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: lease.messageId } },
        UpdateExpression: "SET #leaseUntil = :leaseUntil, #updatedAt = :now, #expiresAt = :expiresAt",
        ConditionExpression: "#state = :inProgress AND #ownerId = :ownerId",
        ExpressionAttributeNames: {
          "#state": "state",
          "#ownerId": "ownerId",
          "#leaseUntil": "leaseUntil",
          "#updatedAt": "updatedAt",
          "#expiresAt": "expiresAt"
        },
        ExpressionAttributeValues: {
          ":inProgress": { S: LeaseState.InProgress },
          ":ownerId": { S: lease.ownerId },
          ":leaseUntil": {
            N: String(nowSeconds + this.leaseDurationSeconds)
          },
          ":now": { N: String(nowSeconds) },
          ":expiresAt": {
            N: String(nowSeconds + this.recordTtlSeconds)
          }
        }
      })
    );
  }
  async markComplete(lease) {
    await this.transition(lease, LeaseState.Completed);
  }
  async markFailed(lease) {
    await this.transition(lease, LeaseState.Failed);
  }
  async releaseLock(lease) {
    await this.client.send(
      new DeleteItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: lease.messageId } },
        ConditionExpression: "#state = :inProgress AND #ownerId = :ownerId",
        ExpressionAttributeNames: {
          "#state": "state",
          "#ownerId": "ownerId"
        },
        ExpressionAttributeValues: {
          ":inProgress": { S: LeaseState.InProgress },
          ":ownerId": { S: lease.ownerId }
        }
      })
    );
  }
  async transition(lease, state) {
    const nowSeconds = Math.floor(this.now() / 1e3);
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: lease.messageId } },
        UpdateExpression: "SET #state = :state, #updatedAt = :now, #expiresAt = :expiresAt REMOVE #ownerId, #leaseUntil",
        ConditionExpression: "#state = :inProgress AND #ownerId = :ownerId",
        ExpressionAttributeNames: {
          "#state": "state",
          "#ownerId": "ownerId",
          "#leaseUntil": "leaseUntil",
          "#updatedAt": "updatedAt",
          "#expiresAt": "expiresAt"
        },
        ExpressionAttributeValues: {
          ":state": { S: state },
          ":inProgress": { S: LeaseState.InProgress },
          ":ownerId": { S: lease.ownerId },
          ":now": { N: String(nowSeconds) },
          ":expiresAt": {
            N: String(nowSeconds + this.recordTtlSeconds)
          }
        }
      })
    );
  }
  async readState(messageId) {
    const result = await this.client.send(
      new GetItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: messageId } },
        ConsistentRead: true,
        ProjectionExpression: "#state",
        ExpressionAttributeNames: { "#state": "state" }
      })
    );
    const state = result.Item?.state?.S;
    return Object.values(LeaseState).includes(state) ? state : void 0;
  }
  toAcquireStatus(state) {
    switch (state) {
      case LeaseState.Completed:
        return "completed";
      case LeaseState.Failed:
        return "failed";
      default:
        return "in_progress";
    }
  }
  assertIdentifier(value, name) {
    if (!value) {
      throw new Error(`${name} must be a non-empty string.`);
    }
  }
};
export {
  IDEMPOTENCY_ATTRIBUTE,
  IdempotencyStore,
  LeaseState,
  QueueCraftPoller,
  QueueCraftPublisher,
  Semaphore
};
