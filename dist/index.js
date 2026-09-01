import {
  createQueueCraftDashboard
} from "./chunk-AGN4MPTV.js";

// src/publisher.ts
import {
  SendMessageCommand
} from "@aws-sdk/client-sqs";
import { randomUUID } from "crypto";

// src/trace-context.ts
var TRACEPARENT_ATTRIBUTE = "traceparent";
var TRACESTATE_ATTRIBUTE = "tracestate";
var TRACEPARENT_PATTERN = /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(?:-(.+))?$/;
var ALL_ZERO_TRACE_ID = "00000000000000000000000000000000";
var ALL_ZERO_PARENT_ID = "0000000000000000";
var MAX_TRACEPARENT_LENGTH = 512;
var MAX_TRACESTATE_LENGTH = 512;
var MAX_TRACESTATE_MEMBERS = 32;
var SIMPLE_TRACESTATE_KEY = /^[a-z][a-z0-9_\-*\/]{0,255}$/;
var TENANT_TRACESTATE_KEY = /^[a-z0-9][a-z0-9_\-*\/]{0,240}$/;
var SYSTEM_TRACESTATE_KEY = /^[a-z][a-z0-9_\-*\/]{0,13}$/;
var TRACESTATE_VALUE = /^[\x20-\x2b\x2d-\x3c\x3e-\x7e]+$/;
var QueueCraftW3CTraceContext = class {
  context;
  propagation;
  rootContext;
  constructor(options) {
    this.context = options.context;
    this.propagation = options.propagation;
    this.rootContext = options.rootContext;
  }
  inject() {
    const carrier = {};
    this.propagation.inject(this.context.active(), carrier);
    return normalizeInjectedTraceCarrier(carrier);
  }
  run(carrier, operation) {
    const safeCarrier = normalizeInjectedTraceCarrier(carrier);
    if (!safeCarrier) return operation();
    const parent = this.propagation.extract(
      this.rootContext,
      safeCarrier
    );
    return this.context.with(parent, operation);
  }
};
async function runWithQueueCraftTraceContext(options) {
  if (!options.traceContext || !options.carrier) {
    await options.operation();
    return;
  }
  let operationPromise;
  let outcomePromise;
  let operationCalls = 0;
  const report = (error) => {
    try {
      options.onError?.(error);
    } catch {
    }
  };
  const runOperation = () => {
    operationCalls += 1;
    if (operationPromise) {
      report(
        new Error(
          "QueueCraft trace propagation called the job operation more than once."
        )
      );
      return operationPromise;
    }
    operationPromise = Promise.resolve().then(options.operation);
    outcomePromise = operationPromise.then(
      () => ({ status: "completed" }),
      (error) => ({ status: "failed", error })
    );
    return operationPromise;
  };
  let propagationResult;
  let synchronousPropagationError;
  let propagationFailedSynchronously = false;
  try {
    propagationResult = options.traceContext.run(
      options.carrier,
      runOperation
    );
  } catch (error) {
    propagationFailedSynchronously = true;
    synchronousPropagationError = error;
  }
  if (operationCalls === 0) {
    if (!propagationFailedSynchronously) {
      report(
        new Error(
          "QueueCraft trace propagation did not start the job operation synchronously."
        )
      );
    }
    runOperation();
  }
  if (!propagationFailedSynchronously) {
    const finalOutcomePromise = outcomePromise;
    void Promise.resolve(propagationResult).catch(
      async (propagationError) => {
        const finalOutcome = await finalOutcomePromise;
        if (!(finalOutcome.status === "failed" && propagationError === finalOutcome.error)) {
          report(propagationError);
        }
      }
    );
  }
  const outcome = await outcomePromise;
  if (propagationFailedSynchronously && !(outcome.status === "failed" && synchronousPropagationError === outcome.error)) {
    report(synchronousPropagationError);
  }
  if (outcome.status === "failed") throw outcome.error;
}
function readQueueCraftTraceCarrier(readAttribute) {
  const traceparent = readAttribute(TRACEPARENT_ATTRIBUTE);
  if (!isValidTraceparent(traceparent)) return void 0;
  const tracestate = readAttribute(TRACESTATE_ATTRIBUTE);
  return isValidTracestate(tracestate) ? { traceparent, tracestate } : { traceparent };
}
function normalizeInjectedTraceCarrier(carrier) {
  if (!carrier) return void 0;
  const entries = Object.entries(carrier).reduce(
    (normalized, [key, value]) => {
      if (typeof value === "string") normalized[key.toLowerCase()] = value;
      return normalized;
    },
    {}
  );
  const traceparent = entries.traceparent;
  if (!isValidTraceparent(traceparent)) return void 0;
  const tracestate = entries.tracestate;
  return isValidTracestate(tracestate) ? { traceparent, tracestate } : { traceparent };
}
function isValidTraceparent(value) {
  if (!value || value.length > MAX_TRACEPARENT_LENGTH) return false;
  const match = TRACEPARENT_PATTERN.exec(value);
  return Boolean(
    match && match[1] !== "ff" && (match[1] !== "00" || match[5] === void 0) && match[2] !== ALL_ZERO_TRACE_ID && match[3] !== ALL_ZERO_PARENT_ID
  );
}
function isValidTracestate(value) {
  if (!value || value.length > MAX_TRACESTATE_LENGTH) return false;
  const members = value.split(",");
  if (members.length > MAX_TRACESTATE_MEMBERS) return false;
  const seenKeys = /* @__PURE__ */ new Set();
  for (const rawMember of members) {
    const member = trimTracestateOptionalWhitespace(rawMember);
    if (!member) continue;
    const separator = member.indexOf("=");
    if (separator <= 0 || separator !== member.lastIndexOf("=")) return false;
    const key = member.slice(0, separator);
    const memberValue = member.slice(separator + 1);
    if (seenKeys.has(key) || !isValidTracestateKey(key) || memberValue.length > 256 || !TRACESTATE_VALUE.test(memberValue) || memberValue.endsWith(" ")) {
      return false;
    }
    seenKeys.add(key);
  }
  return seenKeys.size > 0;
}
function trimTracestateOptionalWhitespace(value) {
  return value.replace(/^[ \t]+|[ \t]+$/g, "");
}
function isValidTracestateKey(key) {
  const tenantSeparator = key.indexOf("@");
  if (tenantSeparator < 0) return SIMPLE_TRACESTATE_KEY.test(key);
  if (tenantSeparator !== key.lastIndexOf("@")) return false;
  return TENANT_TRACESTATE_KEY.test(key.slice(0, tenantSeparator)) && SYSTEM_TRACESTATE_KEY.test(key.slice(tenantSeparator + 1));
}

// src/publisher.ts
var IDEMPOTENCY_ATTRIBUTE = "QueueCraftIdempotencyKey";
var QueueCraftPublisher = class {
  sqs;
  queueUrl;
  idempotencyAttribute;
  traceContext;
  onTraceContextError;
  isFifo;
  constructor(options) {
    if (!options.queueUrl) {
      throw new Error("QueueCraftPublisher requires a non-empty queueUrl.");
    }
    this.sqs = options.sqsClient;
    this.queueUrl = options.queueUrl;
    this.idempotencyAttribute = options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
    this.traceContext = options.traceContext;
    this.onTraceContextError = options.onTraceContextError;
    if (this.traceContext && [TRACEPARENT_ATTRIBUTE, TRACESTATE_ATTRIBUTE].includes(
      this.idempotencyAttribute
    )) {
      throw new RangeError(
        "idempotencyAttribute cannot use a reserved W3C trace attribute name."
      );
    }
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
    const traceCarrier = this.injectTraceContext();
    if (traceCarrier) {
      attributes[TRACEPARENT_ATTRIBUTE] = {
        DataType: "String",
        StringValue: traceCarrier.traceparent
      };
      if (traceCarrier.tracestate) {
        attributes[TRACESTATE_ATTRIBUTE] = {
          DataType: "String",
          StringValue: traceCarrier.tracestate
        };
      }
    }
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
  injectTraceContext() {
    if (!this.traceContext) return void 0;
    try {
      const injected = this.traceContext.inject();
      const normalized = normalizeInjectedTraceCarrier(injected);
      if (injected && !normalized) {
        this.reportTraceContextError(
          new Error("Trace injector returned an invalid W3C traceparent.")
        );
      }
      return normalized;
    } catch (error) {
      this.reportTraceContextError(error);
      return void 0;
    }
  }
  reportTraceContextError(error) {
    try {
      this.onTraceContextError?.(error);
    } catch {
    }
  }
};

// src/poller.ts
import {
  ChangeMessageVisibilityCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand
} from "@aws-sdk/client-sqs";
import { randomUUID as randomUUID2 } from "crypto";

// src/instrumentation.ts
async function runInstrumentedJob(options) {
  if (!options.instrumentation) {
    await options.operation();
    return;
  }
  let operationPromise;
  let outcomePromise;
  let operationCalls = 0;
  const report = (error) => {
    try {
      options.onInstrumentationError?.(error);
    } catch {
    }
  };
  const runOperation = () => {
    operationCalls += 1;
    if (operationPromise) {
      report(
        new Error(
          "QueueCraft instrumentation called the handler operation more than once."
        )
      );
      return operationPromise;
    }
    operationPromise = Promise.resolve().then(options.operation);
    outcomePromise = operationPromise.then(
      () => ({ status: "completed" }),
      (error) => ({ status: "failed", error })
    );
    return operationPromise;
  };
  let instrumentationResult = void 0;
  let synchronousInstrumentationError;
  let instrumentationFailedSynchronously = false;
  try {
    instrumentationResult = options.instrumentation.run(
      options.context,
      runOperation
    );
  } catch (error) {
    instrumentationFailedSynchronously = true;
    synchronousInstrumentationError = error;
  }
  if (operationCalls === 0) {
    if (!instrumentationFailedSynchronously) {
      report(
        new Error(
          "QueueCraft instrumentation did not start the handler operation synchronously."
        )
      );
    }
    runOperation();
  }
  if (!instrumentationFailedSynchronously) {
    const finalOutcomePromise = outcomePromise;
    void Promise.resolve(instrumentationResult).catch(
      async (instrumentationError) => {
        const finalOutcome = await finalOutcomePromise;
        if (!(finalOutcome.status === "failed" && instrumentationError === finalOutcome.error)) {
          report(instrumentationError);
        }
      }
    );
  }
  const outcome = await outcomePromise;
  if (instrumentationFailedSynchronously && !(outcome.status === "failed" && synchronousInstrumentationError === outcome.error)) {
    report(synchronousInstrumentationError);
  }
  if (outcome.status === "failed") {
    throw outcome.error;
  }
}

// src/poller.ts
var MAX_SQS_BATCH = 10;
var DEFAULT_SHUTDOWN_TIMEOUT_MS = 3e4;
var MAX_ABORT_CLEANUP_MS = 1e3;
var QueueCraftPoller = class {
  sqs;
  semaphore;
  idempotency;
  queueUrl;
  handler;
  instrumentation;
  traceContext;
  onError;
  onEvent;
  idempotencyAttribute;
  maxConcurrency;
  pollIntervalMs;
  waitTimeSeconds;
  batchSize;
  visibilityTimeoutSeconds;
  heartbeatIntervalMs;
  shutdownTimeoutMs;
  running = false;
  inflight = /* @__PURE__ */ new Set();
  activeExecutions = /* @__PURE__ */ new Map();
  abortController;
  activeReceive;
  sleepController;
  shutdownPromise;
  constructor(options) {
    this.sqs = options.sqsClient;
    this.semaphore = options.semaphore;
    this.idempotency = options.idempotency;
    this.queueUrl = options.queueUrl;
    this.handler = options.handler;
    this.instrumentation = options.instrumentation;
    this.traceContext = options.traceContext;
    this.onError = options.onError;
    this.onEvent = options.onEvent;
    this.idempotencyAttribute = options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
    if (this.traceContext && [TRACEPARENT_ATTRIBUTE, TRACESTATE_ATTRIBUTE].includes(
      this.idempotencyAttribute
    )) {
      throw new RangeError(
        "idempotencyAttribute cannot use a reserved W3C trace attribute name."
      );
    }
    this.maxConcurrency = options.worker.concurrency;
    this.pollIntervalMs = options.worker.pollIntervalMs;
    this.waitTimeSeconds = options.worker.waitTimeSeconds ?? 20;
    this.batchSize = options.worker.batchSize ?? MAX_SQS_BATCH;
    this.visibilityTimeoutSeconds = options.worker.visibilityTimeoutSeconds ?? 60;
    this.heartbeatIntervalMs = options.worker.heartbeatIntervalMs ?? Math.floor(this.visibilityTimeoutSeconds * 1e3 / 2);
    this.shutdownTimeoutMs = options.worker.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    this.validateOptions(options.worker.concurrency);
  }
  /** Whether the poll loop is currently active. */
  get isRunning() {
    return this.running;
  }
  /**
   * Run the continuous poll loop until `stop()` is called. Resolves once the
   * loop has exited and active jobs have drained or reached the configured
   * shutdown timeout.
   */
  async start() {
    if (this.running) return;
    this.shutdownPromise = void 0;
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
        if (messages.length > 0) {
          this.reportEvent({ type: "messages_received", count: messages.length });
        }
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
    await this.shutdownAndDrain();
  }
  /**
   * Stop polling, allow active jobs to finish within the configured grace
   * period, then cancel them and return without waiting forever.
   */
  async stop() {
    this.running = false;
    this.abortController?.abort();
    this.sleepController?.abort();
    await this.activeReceive?.catch(() => void 0);
    await this.shutdownAndDrain();
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
        MessageAttributeNames: this.traceContext ? [
          .../* @__PURE__ */ new Set([
            this.idempotencyAttribute,
            TRACEPARENT_ATTRIBUTE,
            TRACESTATE_ATTRIBUTE
          ])
        ] : [this.idempotencyAttribute],
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
      this.reportEvent({
        type: "job_duplicate",
        idempotencyKey,
        state: "completed"
      });
      await this.deleteMessage(receiptHandle);
      return;
    }
    if (acquisition.status !== "acquired") {
      this.reportEvent({
        type: "job_duplicate",
        idempotencyKey,
        state: acquisition.status
      });
      return;
    }
    const lease = acquisition.lease;
    const handlerController = new AbortController();
    const heartbeatController = new AbortController();
    this.activeExecutions.set(handlerController, heartbeatController);
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
    const startedAt = Date.now();
    this.reportEvent({ type: "job_started", idempotencyKey, attempt });
    let handlerError;
    let handlerFailed = false;
    try {
      const context = {
        idempotencyKey,
        attempt,
        signal: handlerController.signal
      };
      await runWithQueueCraftTraceContext({
        traceContext: this.traceContext,
        carrier: readQueueCraftTraceCarrier(
          (name) => message.MessageAttributes?.[name]?.StringValue
        ),
        operation: () => runInstrumentedJob({
          instrumentation: this.instrumentation,
          context: {
            runtime: "poller",
            attempt,
            signal: context.signal
          },
          operation: async () => {
            await this.handler(message, context);
          },
          onInstrumentationError: (error) => this.reportError(error, message)
        }),
        onError: (error) => this.reportError(error, message)
      });
    } catch (error) {
      handlerFailed = true;
      handlerError = error;
    } finally {
      heartbeatController.abort();
      await heartbeat;
      this.activeExecutions.delete(handlerController);
    }
    if (heartbeatError !== void 0) {
      this.reportEvent({
        type: "job_cancelled",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt
      });
      return;
    }
    if (handlerFailed) {
      await this.safeRelease(lease);
      this.reportEvent({
        type: "job_failed",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt
      });
      this.reportError(handlerError, message);
      return;
    }
    if (handlerController.signal.aborted) {
      await this.safeRelease(lease);
      this.reportEvent({
        type: "job_cancelled",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt
      });
      this.reportError(
        handlerController.signal.reason ?? new Error("QueueCraft handler cancelled during shutdown."),
        message
      );
      return;
    }
    try {
      await this.idempotency.markComplete(lease);
      await this.deleteMessage(receiptHandle);
      this.reportEvent({
        type: "job_completed",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt
      });
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
    this.assertIntegerInRange(
      this.shutdownTimeoutMs,
      "shutdownTimeoutMs",
      0,
      Number.MAX_SAFE_INTEGER
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
  reportEvent(event) {
    try {
      this.onEvent?.(event);
    } catch {
    }
  }
  async drain() {
    await Promise.allSettled([...this.inflight]);
  }
  shutdownAndDrain() {
    this.shutdownPromise ??= this.performBoundedDrain();
    return this.shutdownPromise;
  }
  async performBoundedDrain() {
    if (this.inflight.size === 0) return;
    const drainedNaturally = await this.drainWithin(this.shutdownTimeoutMs);
    if (drainedNaturally) return;
    const reason = new Error(
      `QueueCraft graceful shutdown timed out after ${this.shutdownTimeoutMs}ms.`
    );
    this.reportEvent({
      type: "shutdown_timeout",
      activeJobs: this.activeExecutions.size,
      timeoutMs: this.shutdownTimeoutMs
    });
    for (const [handlerController, heartbeatController] of this.activeExecutions) {
      heartbeatController.abort(reason);
      handlerController.abort(reason);
    }
    const cleanupMs = Math.min(
      MAX_ABORT_CLEANUP_MS,
      Math.max(10, this.shutdownTimeoutMs)
    );
    await this.drainWithin(cleanupMs);
  }
  drainWithin(timeoutMs) {
    if (this.inflight.size === 0) return Promise.resolve(true);
    return new Promise((resolve) => {
      let settled = false;
      const timeout = setTimeout(() => {
        settled = true;
        resolve(false);
      }, timeoutMs);
      void this.drain().then(() => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve(true);
      });
    });
  }
  sleep(ms) {
    this.sleepController = new AbortController();
    const controller = this.sleepController;
    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        controller.signal.removeEventListener("abort", onAbort);
        if (this.sleepController === controller) {
          this.sleepController = void 0;
        }
        resolve();
      }, ms);
      const onAbort = () => {
        clearTimeout(timeout);
        if (this.sleepController === controller) {
          this.sleepController = void 0;
        }
        resolve();
      };
      controller.signal.addEventListener("abort", onAbort, { once: true });
    });
  }
};

// src/lambda-processor.ts
import { randomUUID as randomUUID3 } from "crypto";

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

// src/lambda-processor.ts
var QueueCraftLambdaProcessor = class {
  idempotency;
  handler;
  instrumentation;
  traceContext;
  semaphore;
  idempotencyAttribute;
  onError;
  onEvent;
  constructor(options) {
    const concurrency = options.concurrency ?? 10;
    this.idempotency = options.idempotency;
    this.handler = options.handler;
    this.instrumentation = options.instrumentation;
    this.traceContext = options.traceContext;
    this.semaphore = new Semaphore(concurrency);
    this.idempotencyAttribute = options.idempotencyAttribute ?? IDEMPOTENCY_ATTRIBUTE;
    if (this.traceContext && [TRACEPARENT_ATTRIBUTE, TRACESTATE_ATTRIBUTE].includes(
      this.idempotencyAttribute
    )) {
      throw new RangeError(
        "idempotencyAttribute cannot use a reserved W3C trace attribute name."
      );
    }
    this.onError = options.onError;
    this.onEvent = options.onEvent;
  }
  async process(event, options = {}) {
    const signal = options.signal ?? new AbortController().signal;
    if (event.Records.length > 0) {
      this.reportEvent({
        type: "messages_received",
        count: event.Records.length
      });
    }
    const results = await Promise.all(
      event.Records.map(
        (record) => this.semaphore.run(() => this.processRecord(record, signal))
      )
    );
    return {
      batchItemFailures: event.Records.flatMap(
        (record, index) => results[index] ? [] : [{ itemIdentifier: record.messageId }]
      )
    };
  }
  async processRecord(record, signal) {
    if (!record.messageId || signal.aborted) {
      return false;
    }
    const idempotencyKey = record.messageAttributes?.[this.idempotencyAttribute]?.stringValue ?? record.messageId;
    let acquisition;
    try {
      acquisition = await this.idempotency.acquireLock(
        idempotencyKey,
        randomUUID3()
      );
    } catch (error) {
      this.reportError(error, record);
      return false;
    }
    if (acquisition.status === "completed") {
      this.reportEvent({
        type: "job_duplicate",
        idempotencyKey,
        state: "completed"
      });
      return true;
    }
    if (acquisition.status !== "acquired") {
      this.reportEvent({
        type: "job_duplicate",
        idempotencyKey,
        state: acquisition.status
      });
      return false;
    }
    const lease = acquisition.lease;
    let handlerReturned = false;
    const attempt = this.receiveCount(record);
    const startedAt = Date.now();
    this.reportEvent({ type: "job_started", idempotencyKey, attempt });
    try {
      const message = this.toSdkMessage(record);
      const context = {
        idempotencyKey,
        attempt,
        signal
      };
      await runWithQueueCraftTraceContext({
        traceContext: this.traceContext,
        carrier: readQueueCraftTraceCarrier(
          (name) => record.messageAttributes?.[name]?.stringValue
        ),
        operation: () => runInstrumentedJob({
          instrumentation: this.instrumentation,
          context: {
            runtime: "lambda",
            attempt,
            signal: context.signal
          },
          operation: async () => {
            await this.handler(message, context);
            handlerReturned = true;
          },
          onInstrumentationError: (instrumentationError) => this.reportError(instrumentationError, record)
        }),
        onError: (traceContextError) => this.reportError(traceContextError, record)
      });
      if (signal.aborted) {
        throw new Error("Lambda invocation is ending before job completion.");
      }
      await this.idempotency.markComplete(lease);
      this.reportEvent({
        type: "job_completed",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt
      });
      return true;
    } catch (error) {
      if (!handlerReturned) {
        await this.safeRelease(lease, record);
      }
      this.reportEvent({
        type: signal.aborted ? "job_cancelled" : "job_failed",
        idempotencyKey,
        attempt,
        durationMs: Date.now() - startedAt
      });
      this.reportError(error, record);
      return false;
    }
  }
  toSdkMessage(record) {
    const attributeNames = this.traceContext ? [
      this.idempotencyAttribute,
      TRACEPARENT_ATTRIBUTE,
      TRACESTATE_ATTRIBUTE
    ] : [this.idempotencyAttribute];
    const messageAttributes = attributeNames.reduce((result, name) => {
      const value = record.messageAttributes?.[name];
      if (!value) return result;
      result[name] = {
        DataType: value.dataType,
        StringValue: value.stringValue,
        BinaryValue: value.binaryValue ? Buffer.from(value.binaryValue, "base64") : void 0
      };
      return result;
    }, {});
    return {
      MessageId: record.messageId,
      ReceiptHandle: record.receiptHandle,
      Body: record.body,
      Attributes: record.attributes ? { ...record.attributes } : void 0,
      MessageAttributes: Object.keys(messageAttributes).length > 0 ? messageAttributes : void 0
    };
  }
  receiveCount(record) {
    const parsed = Number(record.attributes?.ApproximateReceiveCount ?? "1");
    return Number.isInteger(parsed) && parsed > 0 ? parsed : 1;
  }
  async safeRelease(lease, record) {
    try {
      await this.idempotency.releaseLock(lease);
    } catch (error) {
      this.reportError(error, record);
    }
  }
  reportError(error, record) {
    try {
      this.onError?.(error, record);
    } catch {
    }
  }
  reportEvent(event) {
    try {
      this.onEvent?.(event);
    } catch {
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

// src/cloudwatch-metrics.ts
import {
  PutMetricDataCommand
} from "@aws-sdk/client-cloudwatch";
var DEFAULT_NAMESPACE = "QueueCraft";
var DEFAULT_BATCH_SIZE = 20;
var DEFAULT_FLUSH_INTERVAL_MS = 1e4;
var MAX_METRICS_PER_REQUEST = 1e3;
var MAX_USER_DIMENSIONS = 29;
function mapQueueCraftEventToCloudWatchMetrics(event, options = {}) {
  validateDimensions(options.dimensions);
  const dimensions = toDimensions(options.dimensions);
  const timestamp = (options.now ?? (() => /* @__PURE__ */ new Date()))();
  const metric = (name, value, unit, extraDimensions = []) => ({
    MetricName: name,
    Value: value,
    Unit: unit,
    Timestamp: timestamp,
    Dimensions: [...dimensions, ...extraDimensions]
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
      const metricName = event.type === "job_completed" ? "JobsCompleted" : event.type === "job_failed" ? "JobsFailed" : "JobsCancelled";
      return [
        metric(metricName, 1, "Count"),
        metric("JobDuration", event.durationMs, "Milliseconds", [
          { Name: "Outcome", Value: outcome }
        ])
      ];
    }
    case "job_duplicate":
      return [
        metric("JobsDuplicate", 1, "Count", [
          { Name: "DuplicateState", Value: event.state }
        ])
      ];
    case "shutdown_timeout":
      return [
        metric("ShutdownTimeouts", 1, "Count"),
        metric("ActiveJobsAtShutdown", event.activeJobs, "Count"),
        metric("ShutdownTimeout", event.timeoutMs, "Milliseconds")
      ];
  }
}
var QueueCraftCloudWatchMetrics = class {
  client;
  namespace;
  mappingOptions;
  maxBatchSize;
  flushIntervalMs;
  onError;
  pending = [];
  timer;
  activeFlush;
  closed = false;
  constructor(options) {
    this.client = options.client;
    this.namespace = options.namespace ?? DEFAULT_NAMESPACE;
    this.maxBatchSize = options.maxBatchSize ?? DEFAULT_BATCH_SIZE;
    this.flushIntervalMs = options.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
    this.onError = options.onError;
    this.mappingOptions = {
      dimensions: options.dimensions,
      now: options.now
    };
    this.validateOptions(options.dimensions);
  }
  /** Synchronous, failure-isolated observer for `QueueCraftPoller.onEvent`. */
  onEvent = (event) => {
    if (this.closed) return;
    try {
      this.pending.push(
        ...mapQueueCraftEventToCloudWatchMetrics(event, this.mappingOptions)
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
  async flush() {
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
        this.activeFlush = void 0;
      }
      if (!this.closed && this.pending.length > 0) {
        this.scheduleFlush();
      }
    }
  }
  /** Stops the timer and flushes remaining metrics. */
  async close() {
    this.closed = true;
    this.clearTimer();
    await this.flush();
  }
  get pendingMetricCount() {
    return this.pending.length;
  }
  async flushPending() {
    while (this.pending.length > 0) {
      const batch = this.pending.splice(0, this.maxBatchSize);
      try {
        await this.client.send(
          new PutMetricDataCommand({
            Namespace: this.namespace,
            MetricData: batch
          })
        );
      } catch (error) {
        this.pending.unshift(...batch);
        throw error;
      }
    }
  }
  scheduleFlush() {
    if (this.timer || this.flushIntervalMs === 0 || this.closed) return;
    this.timer = setTimeout(() => {
      this.timer = void 0;
      void this.flush().catch((error) => this.reportError(error));
    }, this.flushIntervalMs);
    this.timer.unref?.();
  }
  clearTimer() {
    if (!this.timer) return;
    clearTimeout(this.timer);
    this.timer = void 0;
  }
  reportError(error) {
    try {
      this.onError?.(error);
    } catch {
    }
  }
  validateOptions(dimensions) {
    if (!this.namespace.trim() || this.namespace.startsWith("AWS/")) {
      throw new RangeError(
        "namespace must be non-empty and must not start with the reserved AWS/ prefix."
      );
    }
    if (!Number.isInteger(this.maxBatchSize) || this.maxBatchSize < 1 || this.maxBatchSize > MAX_METRICS_PER_REQUEST) {
      throw new RangeError("maxBatchSize must be an integer between 1 and 1000.");
    }
    if (!Number.isInteger(this.flushIntervalMs) || this.flushIntervalMs < 0) {
      throw new RangeError("flushIntervalMs must be a non-negative integer.");
    }
    validateDimensions(dimensions);
  }
};
function validateDimensions(dimensions) {
  const entries = Object.entries(dimensions ?? {});
  if (entries.length > MAX_USER_DIMENSIONS) {
    throw new RangeError(
      "dimensions can contain at most 29 entries so QueueCraft can add one bounded event dimension."
    );
  }
  for (const [name, value] of entries) {
    if (!name.trim() || !value.trim()) {
      throw new RangeError("dimension names and values must be non-empty.");
    }
  }
}
function toDimensions(dimensions) {
  return Object.entries(dimensions ?? {}).map(([Name, Value]) => ({
    Name,
    Value
  }));
}

// src/tracing.ts
var QueueCraftActiveTracing = class {
  tracer;
  spanName;
  attributes;
  onError;
  constructor(options) {
    this.tracer = options.tracer;
    this.spanName = options.spanName ?? "queuecraft.handler";
    this.attributes = options.attributes ?? {};
    this.onError = options.onError;
    if (!this.spanName.trim()) {
      throw new RangeError("spanName must be non-empty.");
    }
  }
  async run(context, operation) {
    let operationEntered = false;
    try {
      await this.tracer.startActiveSpan(
        this.spanName,
        {
          attributes: {
            ...this.attributes,
            "messaging.system": "aws.sqs",
            "messaging.operation.type": "process",
            "queuecraft.runtime": context.runtime,
            "queuecraft.attempt": context.attempt
          }
        },
        async (span) => {
          operationEntered = true;
          const startedAt = Date.now();
          let outcome = "failed";
          try {
            await operation();
            outcome = context.signal.aborted ? "cancelled" : "completed";
          } catch (error) {
            outcome = context.signal.aborted ? "cancelled" : "failed";
            throw error;
          } finally {
            this.finishSpan(span, outcome, Date.now() - startedAt);
          }
        }
      );
    } catch (error) {
      if (!operationEntered) {
        this.reportError(error);
        await operation();
        return;
      }
      throw error;
    }
  }
  finishSpan(span, outcome, durationMs) {
    try {
      span.setAttribute("queuecraft.outcome", outcome);
      span.setAttribute("queuecraft.duration_ms", durationMs);
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
  reportError(error) {
    try {
      this.onError?.(error);
    } catch {
    }
  }
};
var QueueCraftTracingObserver = class {
  tracer;
  spanName;
  attributes;
  onError;
  active = /* @__PURE__ */ new Map();
  closed = false;
  constructor(options) {
    this.tracer = options.tracer;
    this.spanName = options.spanName ?? "queuecraft.job";
    this.attributes = options.attributes ?? {};
    this.onError = options.onError;
    if (!this.spanName.trim()) {
      throw new RangeError("spanName must be non-empty.");
    }
  }
  /** Synchronous, failure-isolated observer for QueueCraft lifecycle events. */
  onEvent = (event) => {
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
            event.durationMs
          );
          break;
        case "job_duplicate":
          this.recordInstantSpan(`${this.spanName}.duplicate`, {
            "queuecraft.duplicate_state": event.state
          });
          break;
        case "shutdown_timeout":
          this.recordInstantSpan(`${this.spanName}.shutdown_timeout`, {
            "queuecraft.active_jobs": event.activeJobs,
            "queuecraft.timeout_ms": event.timeoutMs
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
  close() {
    if (this.closed) return;
    this.closed = true;
    for (const { span } of this.active.values()) {
      this.finishSpan(span, "observer_closed");
    }
    this.active.clear();
  }
  get activeSpanCount() {
    return this.active.size;
  }
  startJob(idempotencyKey, attempt) {
    const previous = this.active.get(idempotencyKey);
    if (previous) {
      this.finishSpan(previous.span, "superseded");
    }
    const span = this.tracer.startSpan(this.spanName, {
      attributes: {
        ...this.attributes,
        "messaging.system": "aws.sqs",
        "queuecraft.attempt": attempt
      }
    });
    this.active.set(idempotencyKey, { span });
  }
  finishJob(idempotencyKey, outcome, attempt, durationMs) {
    const active = this.active.get(idempotencyKey);
    const span = active?.span ?? this.tracer.startSpan(this.spanName, {
      attributes: {
        ...this.attributes,
        "messaging.system": "aws.sqs",
        "queuecraft.attempt": attempt,
        "queuecraft.late_start": true
      }
    });
    this.active.delete(idempotencyKey);
    this.finishSpan(span, outcome, durationMs);
  }
  recordInstantSpan(name, attributes) {
    const span = this.tracer.startSpan(name, {
      attributes: {
        ...this.attributes,
        "messaging.system": "aws.sqs",
        ...attributes
      }
    });
    this.finishSpan(span, "observed");
  }
  finishSpan(span, outcome, durationMs) {
    try {
      span.setAttribute("queuecraft.outcome", outcome);
      if (durationMs !== void 0) {
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
  reportError(error) {
    try {
      this.onError?.(error);
    } catch {
    }
  }
};
export {
  IDEMPOTENCY_ATTRIBUTE,
  IdempotencyStore,
  LeaseState,
  QueueCraftActiveTracing,
  QueueCraftCloudWatchMetrics,
  QueueCraftLambdaProcessor,
  QueueCraftPoller,
  QueueCraftPublisher,
  QueueCraftTracingObserver,
  QueueCraftW3CTraceContext,
  Semaphore,
  TRACEPARENT_ATTRIBUTE,
  TRACESTATE_ATTRIBUTE,
  createQueueCraftDashboard,
  mapQueueCraftEventToCloudWatchMetrics
};
