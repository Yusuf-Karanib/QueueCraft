import { beforeEach, describe, expect, it, vi } from "vitest";

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
  class ChangeMessageVisibilityCommand {
    readonly __type = "ChangeMessageVisibility";
    constructor(public readonly input: unknown) {}
  }
  class SendMessageCommand {
    readonly __type = "SendMessage";
    constructor(public readonly input: unknown) {}
  }
  return {
    SQSClient,
    ReceiveMessageCommand,
    DeleteMessageCommand,
    ChangeMessageVisibilityCommand,
    SendMessageCommand,
  };
});

vi.mock("@aws-sdk/client-dynamodb", () => {
  class DynamoDBClient {
    send = vi.fn();
  }
  class UpdateItemCommand {
    readonly __type = "UpdateItem";
    constructor(public readonly input: unknown) {}
  }
  class GetItemCommand {
    readonly __type = "GetItem";
    constructor(public readonly input: unknown) {}
  }
  class DeleteItemCommand {
    readonly __type = "DeleteItem";
    constructor(public readonly input: unknown) {}
  }
  class ConditionalCheckFailedException extends Error {
    constructor() {
      super("conditional check failed");
      this.name = "ConditionalCheckFailedException";
    }
  }
  return {
    DynamoDBClient,
    UpdateItemCommand,
    GetItemCommand,
    DeleteItemCommand,
    ConditionalCheckFailedException,
  };
});

import { DynamoDBClient, ConditionalCheckFailedException } from "@aws-sdk/client-dynamodb";
import { SQSClient, type Message } from "@aws-sdk/client-sqs";
import { IdempotencyStore, LeaseState } from "./idempotency";
import { QueueCraftPoller, type JobHandler } from "./poller";
import { IDEMPOTENCY_ATTRIBUTE } from "./publisher";
import { Semaphore } from "./semaphore";
import type { WorkerOptions } from "./types";

const QUEUE_URL =
  "https://sqs.me-central-1.amazonaws.com/123456789012/queuecraft-test";
const STABLE_JOB_ID = "whatsapp-message-123";

const delay = <T>(ms: number, value: T): Promise<T> =>
  new Promise((resolve) => setTimeout(() => resolve(value), ms));

type MockFunction = ReturnType<typeof vi.fn>;

const commandsOfType = (send: MockFunction, type: string) =>
  send.mock.calls.filter(
    (call) => (call[0] as { __type?: string } | undefined)?.__type === type,
  );

interface Harness {
  poller: QueueCraftPoller;
  handler: MockFunction;
  onError: MockFunction;
  onEvent: MockFunction;
  sqsSend: MockFunction;
  dynamoSend: MockFunction;
  message: Message;
}

function createHarness(
  handlerImpl: JobHandler,
  existingState?: (typeof LeaseState)[keyof typeof LeaseState],
  workerOverrides: Partial<WorkerOptions> = {},
): Harness {
  const message: Message = {
    MessageId: "sqs-message-1",
    ReceiptHandle: "receipt-1",
    Body: JSON.stringify({ hello: "world" }),
    MessageAttributes: {
      [IDEMPOTENCY_ATTRIBUTE]: {
        DataType: "String",
        StringValue: STABLE_JOB_ID,
      },
    },
    Attributes: { ApproximateReceiveCount: "2" },
  };

  const sqsSend = vi.fn();
  let receiveCount = 0;
  sqsSend.mockImplementation((command: { __type: string }) => {
    if (command.__type === "ReceiveMessage") {
      receiveCount += 1;
      return receiveCount === 1
        ? delay(0, { Messages: [message] })
        : delay(5, { Messages: [] });
    }
    return Promise.resolve({});
  });

  const dynamoSend = vi.fn();
  let firstUpdate = true;
  dynamoSend.mockImplementation((command: { __type: string }) => {
    if (command.__type === "UpdateItem" && firstUpdate) {
      firstUpdate = false;
      if (existingState !== undefined) {
        return Promise.reject(
          new ConditionalCheckFailedException({
            $metadata: {},
            message: "conditional check failed",
          }),
        );
      }
    }
    if (command.__type === "GetItem") {
      return Promise.resolve({ Item: { state: { S: existingState } } });
    }
    return Promise.resolve({});
  });

  const sqsClient = { send: sqsSend } as unknown as SQSClient;
  const dynamoClient = { send: dynamoSend } as unknown as DynamoDBClient;
  const handler = vi.fn(handlerImpl);
  const onError = vi.fn();
  const onEvent = vi.fn();
  const worker: WorkerOptions = {
    concurrency: 5,
    pollIntervalMs: 5,
    waitTimeSeconds: 0,
    batchSize: 10,
    ...workerOverrides,
  };

  const poller = new QueueCraftPoller({
    sqsClient,
    semaphore: new Semaphore(5),
    idempotency: new IdempotencyStore({
      client: dynamoClient,
      tableName: "queuecraft-leases",
      now: () => 1_700_000_000_000,
    }),
    queueUrl: QUEUE_URL,
    handler,
    worker,
    onError,
    onEvent,
  });

  return { poller, handler, onError, onEvent, sqsSend, dynamoSend, message };
}

async function runOnce(poller: QueueCraftPoller): Promise<void> {
  const started = poller.start();
  await delay(1, undefined);
  await poller.stop();
  await started;
}

describe("QueueCraftPoller", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses the producer's stable idempotency key and completes the job", async () => {
    const { poller, handler, onEvent, sqsSend, dynamoSend, message } = createHarness(
      async () => undefined,
    );

    await runOnce(poller);

    expect(handler).toHaveBeenCalledWith(
      message,
      expect.objectContaining({
        idempotencyKey: STABLE_JOB_ID,
        attempt: 2,
        signal: expect.any(AbortSignal),
      }),
    );
    expect(commandsOfType(sqsSend, "DeleteMessage")).toHaveLength(1);
    expect(commandsOfType(dynamoSend, "UpdateItem")).toHaveLength(2);

    const receiveInput = commandsOfType(sqsSend, "ReceiveMessage")[0][0].input;
    expect(receiveInput).toMatchObject({
      MessageAttributeNames: [IDEMPOTENCY_ATTRIBUTE],
      MessageSystemAttributeNames: ["ApproximateReceiveCount"],
      VisibilityTimeout: 60,
    });

    const acquireInput = commandsOfType(dynamoSend, "UpdateItem")[0][0].input;
    expect(acquireInput).toMatchObject({
      Key: { messageId: { S: STABLE_JOB_ID } },
    });
    expect(onEvent.mock.calls.map((call) => call[0].type)).toEqual([
      "messages_received",
      "job_started",
      "job_completed",
    ]);
    expect(onEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "job_completed",
        idempotencyKey: STABLE_JOB_ID,
        attempt: 2,
        durationMs: expect.any(Number),
      }),
    );
  });

  it("acknowledges a completed duplicate without running the handler", async () => {
    const { poller, handler, sqsSend } = createHarness(
      async () => undefined,
      LeaseState.Completed,
    );

    await runOnce(poller);

    expect(handler).not.toHaveBeenCalled();
    expect(commandsOfType(sqsSend, "DeleteMessage")).toHaveLength(1);
  });

  it("does not let a failing event observer crash job processing", async () => {
    const harness = createHarness(async () => undefined);
    harness.onEvent.mockImplementation(() => {
      throw new Error("observer failed");
    });

    await runOnce(harness.poller);

    expect(harness.handler).toHaveBeenCalledTimes(1);
    expect(commandsOfType(harness.sqsSend, "DeleteMessage")).toHaveLength(1);
  });

  it("leaves an in-progress duplicate for its current owner", async () => {
    const { poller, handler, sqsSend } = createHarness(
      async () => undefined,
      LeaseState.InProgress,
    );

    await runOnce(poller);

    expect(handler).not.toHaveBeenCalled();
    expect(commandsOfType(sqsSend, "DeleteMessage")).toHaveLength(0);
  });

  it("releases only its owned lease when the handler throws", async () => {
    const { poller, handler, onError, sqsSend, dynamoSend } = createHarness(
      async () => {
        throw new Error("job blew up");
      },
    );

    await runOnce(poller);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(commandsOfType(sqsSend, "DeleteMessage")).toHaveLength(0);

    const releases = commandsOfType(dynamoSend, "DeleteItem");
    expect(releases).toHaveLength(1);
    expect(releases[0][0].input).toMatchObject({
      ConditionExpression: "#state = :inProgress AND #ownerId = :ownerId",
    });
    expect(onError).toHaveBeenCalledTimes(1);
  });

  it("renews both leases while a long-running handler is active", async () => {
    const { poller, handler, sqsSend, dynamoSend } = createHarness(
      async (_message, context) => {
        expect(context.attempt).toBe(2);
        await delay(18, undefined);
      },
      undefined,
      { visibilityTimeoutSeconds: 1, heartbeatIntervalMs: 5 },
    );

    await runOnce(poller);

    expect(handler).toHaveBeenCalledTimes(1);
    expect(
      commandsOfType(sqsSend, "ChangeMessageVisibility").length,
    ).toBeGreaterThanOrEqual(1);
    expect(commandsOfType(dynamoSend, "UpdateItem").length).toBeGreaterThanOrEqual(3);
    expect(commandsOfType(sqsSend, "DeleteMessage")).toHaveLength(1);
  });

  it("aborts the handler and refuses settlement after heartbeat loss", async () => {
    const harness = createHarness(
      async (_message, context) => {
        await new Promise<void>((resolve) => {
          context.signal.addEventListener("abort", () => resolve(), {
            once: true,
          });
        });
      },
      undefined,
      { visibilityTimeoutSeconds: 1, heartbeatIntervalMs: 5 },
    );

    let updateCount = 0;
    harness.dynamoSend.mockImplementation((command: { __type: string }) => {
      if (command.__type === "UpdateItem") {
        updateCount += 1;
        if (updateCount === 2) {
          return Promise.reject(new Error("lease renewal failed"));
        }
      }
      return Promise.resolve({});
    });

    await runOnce(harness.poller);

    expect(harness.handler).toHaveBeenCalledTimes(1);
    expect(commandsOfType(harness.sqsSend, "DeleteMessage")).toHaveLength(0);
    expect(commandsOfType(harness.dynamoSend, "DeleteItem")).toHaveLength(0);
    expect(harness.onError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "lease renewal failed" }),
      harness.message,
    );
    expect(harness.onEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "job_cancelled",
        idempotencyKey: STABLE_JOB_ID,
      }),
    );
  });

  it("returns messages received during shutdown without executing them", async () => {
    const harness = createHarness(async () => undefined);

    const started = harness.poller.start();
    await harness.poller.stop();
    await started;

    expect(harness.handler).not.toHaveBeenCalled();
    const visibilityChanges = commandsOfType(
      harness.sqsSend,
      "ChangeMessageVisibility",
    );
    expect(visibilityChanges).toHaveLength(1);
    expect(visibilityChanges[0][0].input).toMatchObject({
      ReceiptHandle: "receipt-1",
      VisibilityTimeout: 0,
    });
  });

  it("cancels a handler after the graceful-shutdown timeout", async () => {
    const harness = createHarness(
      async (_message, context) => {
        await new Promise<void>((resolve) => {
          context.signal.addEventListener("abort", () => resolve(), {
            once: true,
          });
        });
      },
      undefined,
      { shutdownTimeoutMs: 5 },
    );

    const started = harness.poller.start();
    await vi.waitFor(() => expect(harness.handler).toHaveBeenCalledTimes(1));
    await harness.poller.stop();
    await started;

    expect(commandsOfType(harness.sqsSend, "DeleteMessage")).toHaveLength(0);
    expect(commandsOfType(harness.dynamoSend, "DeleteItem")).toHaveLength(1);
    expect(harness.onError).toHaveBeenCalledWith(
      expect.objectContaining({
        message: "QueueCraft graceful shutdown timed out after 5ms.",
      }),
      harness.message,
    );
    expect(harness.onEvent).toHaveBeenCalledWith({
      type: "shutdown_timeout",
      activeJobs: 1,
      timeoutMs: 5,
    });
  });

  it("rejects a negative graceful-shutdown timeout", () => {
    expect(() =>
      createHarness(async () => undefined, undefined, {
        shutdownTimeoutMs: -1,
      }),
    ).toThrow("shutdownTimeoutMs must be an integer between 0");
  });
});
