import { createQueueCraftDashboard } from "../dist/index.js";

const MAIN_QUEUE = "https://sqs.example.test/queuecraft-demo";
const DLQ = "https://sqs.example.test/queuecraft-demo-dlq";
const messages = new Map([
  [
    "failed-booking-1",
    {
      MessageId: "failed-booking-1",
      ReceiptHandle: "receipt-1",
      Body: JSON.stringify({
        type: "booking_request",
        customerPhoneNumber: "971500000000",
        messageText: "Tomorrow at 3 PM",
        shopId: "demo-shop",
      }),
      Attributes: {
        ApproximateReceiveCount: "3",
        SentTimestamp: String(Date.now() - 4 * 60_000),
      },
      MessageAttributes: {
        QueueCraftIdempotencyKey: {
          DataType: "String",
          StringValue: "demo-whatsapp-message-1",
        },
      },
    },
  ],
  [
    "failed-booking-2",
    {
      MessageId: "failed-booking-2",
      ReceiptHandle: "receipt-2",
      Body: JSON.stringify({
        type: "booking_request",
        customerEmail: "customer@example.com",
        requestedTime: "2026-09-01T15:00:00+04:00",
      }),
      Attributes: {
        ApproximateReceiveCount: "5",
        SentTimestamp: String(Date.now() - 11 * 60_000),
      },
      MessageAttributes: {
        QueueCraftIdempotencyKey: {
          DataType: "String",
          StringValue: "demo-whatsapp-message-2",
        },
      },
    },
  ],
]);

let mainVisible = 12;
const sqsClient = {
  async send(command) {
    switch (command.constructor.name) {
      case "GetQueueAttributesCommand": {
        const isMain = command.input.QueueUrl === MAIN_QUEUE;
        return {
          Attributes: {
            ApproximateNumberOfMessages: String(
              isMain ? mainVisible : messages.size,
            ),
            ApproximateNumberOfMessagesNotVisible: isMain ? "3" : "0",
            ApproximateNumberOfMessagesDelayed: isMain ? "1" : "0",
          },
        };
      }
      case "ReceiveMessageCommand":
        return { Messages: [...messages.values()] };
      case "SendMessageCommand":
        mainVisible += 1;
        return { MessageId: "replayed-demo-message" };
      case "DeleteMessageCommand": {
        const match = [...messages.entries()].find(
          ([, message]) =>
            message.ReceiptHandle === command.input.ReceiptHandle,
        );
        if (match) messages.delete(match[0]);
        return {};
      }
      default:
        throw new Error("Unsupported demo command.");
    }
  },
};

const dashboard = await createQueueCraftDashboard({
  sqsClient,
  queueUrl: MAIN_QUEUE,
  dlqUrl: DLQ,
  title: "QueueCraft Demo",
});

console.log("QueueCraft dashboard demo: " + dashboard.url);
console.log("Press Ctrl+C to stop.");

process.once("SIGINT", () => void dashboard.close());
process.once("SIGTERM", () => void dashboard.close());
