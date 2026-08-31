import { describe, expect, it, vi } from "vitest";
import type { SQSClient } from "@aws-sdk/client-sqs";
import { createQueueCraftDashboard } from "./dashboard";

const MAIN_QUEUE = "https://sqs.eu-central-1.amazonaws.com/123/main";
const DLQ = "https://sqs.eu-central-1.amazonaws.com/123/main-dlq";

describe("QueueCraft dashboard", () => {
  it("shows queue counts, redacts bodies, and replays a cached DLQ job", async () => {
    const send = vi.fn(async (command: { constructor: { name: string }; input: any }) => {
      switch (command.constructor.name) {
        case "GetQueueAttributesCommand":
          return {
            Attributes:
              command.input.QueueUrl === MAIN_QUEUE
                ? {
                    ApproximateNumberOfMessages: "4",
                    ApproximateNumberOfMessagesNotVisible: "2",
                    ApproximateNumberOfMessagesDelayed: "1",
                  }
                : {
                    ApproximateNumberOfMessages: "1",
                    ApproximateNumberOfMessagesNotVisible: "0",
                    ApproximateNumberOfMessagesDelayed: "0",
                  },
          };
        case "ReceiveMessageCommand":
          return {
            Messages: [
              {
                MessageId: "failed-1",
                ReceiptHandle: "receipt-1",
                Body: JSON.stringify({
                  type: "booking_request",
                  customerPhoneNumber: "971500000000",
                  messageText: "Tomorrow at 3 PM",
                }),
                Attributes: {
                  ApproximateReceiveCount: "3",
                  SentTimestamp: "1700000000000",
                },
                MessageAttributes: {
                  QueueCraftIdempotencyKey: {
                    DataType: "String",
                    StringValue: "stable-1",
                  },
                },
              },
            ],
          };
        default:
          return {};
      }
    });

    const dashboard = await createQueueCraftDashboard({
      sqsClient: { send } as unknown as SQSClient,
      queueUrl: MAIN_QUEUE,
      dlqUrl: DLQ,
      title: "Test Queue",
      port: 0,
    });

    try {
      const home = await fetch(dashboard.url);
      expect(home.status).toBe(200);
      expect(home.headers.get("content-security-policy")).toContain(
        "default-src 'self'",
      );

      const overview = await fetch(dashboard.url + "/api/overview").then(
        (response) => response.json(),
      );
      expect(overview).toEqual({
        title: "Test Queue",
        main: { visible: 4, inFlight: 2, delayed: 1 },
        dlq: { visible: 1, inFlight: 0, delayed: 0 },
      });

      const listing = await fetch(dashboard.url + "/api/dlq").then((response) =>
        response.json(),
      );
      expect(listing.messages[0]).toMatchObject({
        id: "failed-1",
        receiveCount: 3,
      });
      expect(listing.messages[0].bodyPreview).toContain(
        '"customerPhoneNumber":"[redacted]"',
      );
      expect(listing.messages[0].bodyPreview).toContain(
        '"messageText":"[redacted]"',
      );
      expect(listing.messages[0].bodyPreview).not.toContain("971500000000");
      expect(listing.messages[0]).not.toHaveProperty("receiptHandle");

      const replay = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ messageId: "failed-1", confirm: "REPLAY" }),
      });
      expect(replay.status).toBe(200);
      await expect(replay.json()).resolves.toEqual({ replayed: true });

      const published = send.mock.calls.find(
        ([command]) => command.constructor.name === "SendMessageCommand",
      )?.[0];
      expect(published?.input).toMatchObject({
        QueueUrl: MAIN_QUEUE,
        MessageAttributes: {
          QueueCraftIdempotencyKey: {
            DataType: "String",
            StringValue: "stable-1",
          },
        },
      });

      const deleted = send.mock.calls.find(
        ([command]) => command.constructor.name === "DeleteMessageCommand",
      )?.[0];
      expect(deleted?.input).toEqual({
        QueueUrl: DLQ,
        ReceiptHandle: "receipt-1",
      });
    } finally {
      await dashboard.close();
    }
  });

  it("refuses to expose the dashboard on a non-loopback host", async () => {
    await expect(
      createQueueCraftDashboard({
        sqsClient: { send: vi.fn() } as unknown as SQSClient,
        queueUrl: MAIN_QUEUE,
        dlqUrl: DLQ,
        host: "0.0.0.0",
      }),
    ).rejects.toThrow("must bind to a loopback host");
  });

  it("reports AWS failures without exposing their details to the browser", async () => {
    const onError = vi.fn();
    const failure = new Error("credential failure for " + MAIN_QUEUE);
    const dashboard = await createQueueCraftDashboard({
      sqsClient: {
        send: vi.fn().mockRejectedValue(failure),
      } as unknown as SQSClient,
      queueUrl: MAIN_QUEUE,
      dlqUrl: DLQ,
      port: 0,
      onError,
    });

    try {
      const response = await fetch(dashboard.url + "/api/overview");
      expect(response.status).toBe(500);
      const body = await response.json();
      expect(body).toEqual({ error: "Dashboard request failed." });
      expect(JSON.stringify(body)).not.toContain(MAIN_QUEUE);
      expect(onError).toHaveBeenCalledWith(failure);
    } finally {
      await dashboard.close();
    }
  });
});
