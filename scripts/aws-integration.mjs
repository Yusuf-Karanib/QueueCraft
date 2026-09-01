import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import {
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
  IDEMPOTENCY_ATTRIBUTE,
  IdempotencyStore,
  QueueCraftPoller,
  QueueCraftPublisher,
  Semaphore,
  TRACEPARENT_ATTRIBUTE,
  TRACESTATE_ATTRIBUTE,
} from "../dist/index.js";

const REQUIRED_CONFIRMATION = "dedicated-queuecraft-test-stack";
const required = (name) => {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
};

if (process.env.QUEUECRAFT_AWS_TEST_CONFIRM !== REQUIRED_CONFIRMATION) {
  throw new Error(
    "Safety check failed. Use a dedicated QueueCraft test stack, then set " +
      `QUEUECRAFT_AWS_TEST_CONFIRM=${REQUIRED_CONFIRMATION}.`,
  );
}

const region = required("AWS_REGION");
const queueUrl = required("SQS_QUEUE_URL");
const dlqUrl = required("SQS_DLQ_URL");
const tableName = required("DYNAMODB_TABLE_NAME");
const sqs = new SQSClient({ region });
const dynamodb = new DynamoDBClient({ region });
const traceCarrier = {
  traceparent:
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  tracestate: "queuecraft=integration-test",
};
let traceRuns = 0;
const traceContext = {
  inject: () => traceCarrier,
  run(carrier, operation) {
    if (
      carrier.traceparent !== traceCarrier.traceparent ||
      carrier.tracestate !== traceCarrier.tracestate
    ) {
      throw new Error("SQS did not preserve the W3C trace carrier.");
    }
    traceRuns += 1;
    return operation();
  },
};
const publisher = new QueueCraftPublisher({
  sqsClient: sqs,
  queueUrl,
  traceContext,
});
const runId = `${Date.now()}-${crypto.randomUUID()}`;
const successKey = `queuecraft-aws-success-${runId}`;
const failureKey = `queuecraft-aws-failure-${runId}`;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(description, check, timeoutMs = 45_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await check();
    if (result) return result;
    await delay(250);
  }
  throw new Error(`Timed out waiting for ${description}.`);
}

async function assertQueueStartsEmpty(url, name) {
  const result = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: url,
      AttributeNames: [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
      ],
    }),
  );
  const count = Object.values(result.Attributes ?? {}).reduce(
    (total, value) => total + Number(value ?? 0),
    0,
  );
  if (count !== 0) {
    throw new Error(
      `${name} is not empty. Refusing to consume messages from a non-dedicated queue.`,
    );
  }
}

function createPoller(handler) {
  const concurrency = 1;
  return new QueueCraftPoller({
    sqsClient: sqs,
    queueUrl,
    semaphore: new Semaphore(concurrency),
    idempotency: new IdempotencyStore({
      client: dynamodb,
      tableName,
      leaseDurationSeconds: 5,
      recordTtlSeconds: 300,
    }),
    worker: {
      concurrency,
      pollIntervalMs: 50,
      waitTimeSeconds: 1,
      batchSize: 1,
      visibilityTimeoutSeconds: 1,
      heartbeatIntervalMs: 500,
      shutdownTimeoutMs: 2_000,
    },
    handler,
    traceContext,
    onError: () => undefined,
  });
}

async function stopPoller(poller, started) {
  await poller.stop();
  await started;
}

async function readState(key) {
  const result = await dynamodb.send(
    new GetItemCommand({
      TableName: tableName,
      Key: { messageId: { S: key } },
      ConsistentRead: true,
    }),
  );
  return result.Item?.state?.S;
}

async function waitForDlqMessage(key) {
  return waitFor("the failed job to reach the DLQ", async () => {
    const result = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: dlqUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        VisibilityTimeout: 1,
        MessageAttributeNames: [
          IDEMPOTENCY_ATTRIBUTE,
          TRACEPARENT_ATTRIBUTE,
          TRACESTATE_ATTRIBUTE,
        ],
      }),
    );
    const match = result.Messages?.find(
      (message) =>
        message.MessageAttributes?.[IDEMPOTENCY_ATTRIBUTE]?.StringValue === key,
    );
    if (!match?.ReceiptHandle) return undefined;
    if (
      match.MessageAttributes?.[TRACEPARENT_ATTRIBUTE]?.StringValue !==
        traceCarrier.traceparent ||
      match.MessageAttributes?.[TRACESTATE_ATTRIBUTE]?.StringValue !==
        traceCarrier.tracestate
    ) {
      throw new Error("The DLQ did not preserve the W3C trace carrier.");
    }
    await sqs.send(
      new DeleteMessageCommand({
        QueueUrl: dlqUrl,
        ReceiptHandle: match.ReceiptHandle,
      }),
    );
    return match;
  });
}

async function deleteState(key) {
  await dynamodb.send(
    new DeleteItemCommand({
      TableName: tableName,
      Key: { messageId: { S: key } },
    }),
  );
}

let activePoller;
let activeStart;
try {
  await assertQueueStartsEmpty(queueUrl, "Main queue");
  await assertQueueStartsEmpty(dlqUrl, "DLQ");

  let successfulRuns = 0;
  activePoller = createPoller(async (message) => {
    const body = JSON.parse(message.Body ?? "null");
    if (body?.runId !== runId) {
      throw new Error("The dedicated test queue received an unexpected job.");
    }
    successfulRuns += 1;
  });
  activeStart = activePoller.start();
  await publisher.publish({ runId, outcome: "success" }, {
    idempotencyKey: successKey,
  });
  await waitFor("the successful job to complete", async () =>
    (await readState(successKey)) === "COMPLETED",
  );
  await stopPoller(activePoller, activeStart);
  activePoller = undefined;
  activeStart = undefined;

  await publisher.publish({ runId, outcome: "duplicate" }, {
    idempotencyKey: successKey,
  });
  activePoller = createPoller(async () => {
    successfulRuns += 1;
  });
  activeStart = activePoller.start();
  await delay(2_500);
  await stopPoller(activePoller, activeStart);
  activePoller = undefined;
  activeStart = undefined;
  if (successfulRuns !== 1) {
    throw new Error(`Expected one execution after duplicate publish, got ${successfulRuns}.`);
  }

  let failedRuns = 0;
  activePoller = createPoller(async () => {
    failedRuns += 1;
    throw new Error("intentional integration-test failure");
  });
  activeStart = activePoller.start();
  await publisher.publish({ runId, outcome: "failure" }, {
    idempotencyKey: failureKey,
  });
  await waitForDlqMessage(failureKey);
  await stopPoller(activePoller, activeStart);
  activePoller = undefined;
  activeStart = undefined;
  if (failedRuns < 2) {
    throw new Error(`Expected at least two failed attempts, got ${failedRuns}.`);
  }
  const handlerRuns = successfulRuns + failedRuns;
  if (traceRuns !== handlerRuns) {
    throw new Error(
      `Expected trace context on all ${handlerRuns} handler runs, got ${traceRuns}.`,
    );
  }

  console.log("PASS: real SQS publish and worker completion");
  console.log("PASS: DynamoDB-backed duplicate suppression");
  console.log("PASS: SQS retry and DLQ redrive");
  console.log("PASS: W3C trace context survived SQS processing and DLQ redrive");
} finally {
  if (activePoller && activeStart) {
    await stopPoller(activePoller, activeStart).catch(() => undefined);
  }
  await Promise.allSettled([deleteState(successKey), deleteState(failureKey)]);
  sqs.destroy();
  dynamodb.destroy();
}
