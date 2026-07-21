/**
 * QueueCraftPoller unit tests.
 *
 * Both AWS SDK modules are mocked so nothing touches the network. The mocked
 * command classes tag each instance with a `__type` discriminant, letting the
 * fake `send()` implementations route by command and letting assertions filter
 * calls by the operation they represent (PutItem = acquireLock, UpdateItem =
 * markComplete, DeleteItem = releaseLock, DeleteMessage = SQS delete).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("@aws-sdk/client-sqs", () => {
  class SQSClient {
    send = vi.fn();
  }
  class ReceiveMessageCommand {
    readonly __type = "ReceiveMessage";
    constructor(public readonly input: unknown) {}
  }
  class DeleteMessageCommand {
    readonly __type = "DeleteMessage";
    constructor(public readonly input: unknown) {}
  }
  return { SQSClient, ReceiveMessageCommand, DeleteMessageCommand };
});

vi.mock("@aws-sdk/client-dynamodb", () => {
  class DynamoDBClient {
    send = vi.fn();
  }
  class PutItemCommand {
    readonly __type = "PutItem";
    constructor(public readonly input: unknown) {}
  }
  class UpdateItemCommand {
    readonly __type = "UpdateItem";
    constructor(public readonly input: unknown) {}
  }
  class DeleteItemCommand {
    readonly __type = "DeleteItem";
    constructor(public readonly input: unknown) {}
  }
  class ConditionalCheckFailedException extends Error {
    constructor(opts?: string | { message?: string }) {
      super(
        typeof opts === "string"
          ? opts
          : opts?.message ?? "conditional check failed",
      );
      this.name = "ConditionalCheckFailedException";
    }
  }
  return {
    DynamoDBClient,
    PutItemCommand,
    UpdateItemCommand,
    DeleteItemCommand,
    ConditionalCheckFailedException,
  };
});

import { SQSClient, type Message } from "@aws-sdk/client-sqs";
import {
  DynamoDBClient,
  ConditionalCheckFailedException,
} from "@aws-sdk/client-dynamodb";
import { QueueCraftPoller, type JobHandler } from "./poller";
import { Semaphore } from "./semaphore";
import { IdempotencyStore } from "./idempotency";
import type { WorkerOptions } from "./types";

const QUEUE_URL =
  "https://sqs.me-central-1.amazonaws.com/123456789012/queuecraft-test";

/** Resolve `value` on a macrotask so the poll loop yields between iterations. */
const delay = <T>(ms: number, value: T): Promise<T> =>
  new Promise((resolve) => setTimeout(() => resolve(value), ms));

type Fn = ReturnType<typeof vi.fn>;

/** Filter a mocked `send` fn's calls down to a single command type. */
const commandsOfType = (send: Fn, type: string) =>
  send.mock.calls.filter(([command]) => (command as { __type?: string }).__type === type);

interface Harness {
  poller: QueueCraftPoller;
  handler: Fn;
  onError: Fn;
  sqsSend: Fn;
  dynamoSend: Fn;
  message: Message;
}

function createHarness(handlerImpl: JobHandler): Harness {
  const message: Message = {
    MessageId: "msg-1",
    ReceiptHandle: "receipt-1",
    Body: JSON.stringify({ hello: "world" }),
  };

  // SQS: hand back the message on the first receive, then long-poll "empty".
  const sqsSend = vi.fn();
  let receiveCount = 0;
  sqsSend.mockImplementation((command: { __type: string }) => {
    if (command.__type === "ReceiveMessage") {
      receiveCount += 1;
      return receiveCount === 1
        ? delay(0, { Messages: [message] })
        : delay(5, { Messages: [] });
    }
    return Promise.resolve({}); // DeleteMessage, etc.
  });

  // DynamoDB: succeed by default; individual tests override acquireLock.
  const dynamoSend = vi.fn().mockResolvedValue({});

  const sqsClient = { send: sqsSend } as unknown as SQSClient;
  const dynamoClient = { send: dynamoSend } as unknown as DynamoDBClient;

  const semaphore = new Semaphore(5);
  const idempotency = new IdempotencyStore({
    client: dynamoClient,
    tableName: "queuecraft-leases",
  });

  const handler = vi.fn(handlerImpl);
  const onError = vi.fn();

  const worker: WorkerOptions = {
    concurrency: 5,
    pollIntervalMs: 5,
    waitTimeSeconds: 0,
    batchSize: 10,
  };

  const poller = new QueueCraftPoller({
    sqsClient,
    semaphore,
    idempotency,
    queueUrl: QUEUE_URL,
    handler,
    worker,
    onError,
  });

  return { poller, handler, onError, sqsSend, dynamoSend, message };
}

/**
 * Drive the loop through exactly one message. `start()` reaches its first
 * (pending) receive synchronously; `stop()` ends the loop, and `start()`'s
 * final drain awaits the dispatched job, so the returned promise settles only
 * once that message is fully processed.
 */
async function runOnce(poller: QueueCraftPoller): Promise<void> {
  const started = poller.start();
  await poller.stop();
  await started;
}

describe("QueueCraftPoller", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("processes a message end to end: acquire, handle, delete, mark complete", async () => {
    const { poller, handler, sqsSend, dynamoSend, message } = createHarness(
      async () => {
        /* success */
      },
    );

    await runOnce(poller);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(message);

    // acquireLock => conditional PutItem
    expect(commandsOfType(dynamoSend, "PutItem")).toHaveLength(1);

    // message removed from SQS with the correct receipt handle
    const deletes = commandsOfType(sqsSend, "DeleteMessage");
    expect(deletes).toHaveLength(1);
    expect((deletes[0][0] as { input: unknown }).input).toMatchObject({
      QueueUrl: QUEUE_URL,
      ReceiptHandle: "receipt-1",
    });

    // markComplete => UpdateItem
    expect(commandsOfType(dynamoSend, "UpdateItem")).toHaveLength(1);

    // nothing released on success
    expect(commandsOfType(dynamoSend, "DeleteItem")).toHaveLength(0);
  });

  it("skips a duplicate message when the lock cannot be acquired", async () => {
    const { poller, handler, sqsSend, dynamoSend } = createHarness(async () => {
      /* should never run */
    });

    // The conditional PutItem fails => acquireLock resolves false.
    dynamoSend.mockImplementation((command: { __type: string }) => {
      if (command.__type === "PutItem") {
        return Promise.reject(
          new ConditionalCheckFailedException({ message: "already exists" }),
        );
      }
      return Promise.resolve({});
    });

    await runOnce(poller);

    expect(handler).not.toHaveBeenCalled();
    expect(commandsOfType(dynamoSend, "PutItem")).toHaveLength(1);

    // no work committed: no SQS delete, no markComplete, no releaseLock
    expect(commandsOfType(sqsSend, "DeleteMessage")).toHaveLength(0);
    expect(commandsOfType(dynamoSend, "UpdateItem")).toHaveLength(0);
    expect(commandsOfType(dynamoSend, "DeleteItem")).toHaveLength(0);
  });

  it("releases the lock and leaves the message when the handler throws", async () => {
    const { poller, handler, onError, sqsSend, dynamoSend } = createHarness(
      async () => {
        throw new Error("job blew up");
      },
    );

    await runOnce(poller);

    expect(handler).toHaveBeenCalledTimes(1);

    // lock acquired (PutItem) then released (DeleteItem)
    expect(commandsOfType(dynamoSend, "PutItem")).toHaveLength(1);
    expect(commandsOfType(dynamoSend, "DeleteItem")).toHaveLength(1);

    // message NOT deleted and NOT marked complete => SQS will redeliver
    expect(commandsOfType(sqsSend, "DeleteMessage")).toHaveLength(0);
    expect(commandsOfType(dynamoSend, "UpdateItem")).toHaveLength(0);

    expect(onError).toHaveBeenCalledTimes(1);
  });
});