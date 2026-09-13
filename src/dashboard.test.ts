import { describe, expect, it, vi } from "vitest";
import type { SQSClient } from "@aws-sdk/client-sqs";
import { request as httpRequest } from "node:http";
import { createQueueCraftDashboard } from "./dashboard";

const MAIN_QUEUE = "https://sqs.eu-central-1.amazonaws.com/123/main";
const DLQ = "https://sqs.eu-central-1.amazonaws.com/123/main-dlq";

function requestStatus(
  target: string,
  options: {
    readonly method?: string;
    readonly headers?: Readonly<Record<string, string>>;
    readonly body?: string;
  } = {},
): Promise<number> {
  const url = new URL(target);
  return new Promise((resolve, reject) => {
    const request = httpRequest(
      {
        host: url.hostname,
        port: url.port,
        path: url.pathname,
        method: options.method ?? "GET",
        headers: options.headers,
        agent: false,
      },
      (response) => {
        response.resume();
        response.once("end", () => resolve(response.statusCode ?? 0));
      },
    );
    request.once("error", reject);
    if (options.body) request.write(options.body);
    request.end();
  });
}

describe("QueueCraft dashboard", () => {
  it("shows safe metadata, hides bodies, and replays a cached DLQ job", async () => {
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
      expect(home.headers.get("content-security-policy")).toContain(
        "frame-ancestors 'none'",
      );
      expect(home.headers.get("x-frame-options")).toBe("DENY");
      const homeBody = await home.text();
      const writeToken = homeBody.match(/const WRITE_TOKEN="([^"]+)"/)?.[1];
      expect(writeToken).toMatch(/^[A-Za-z0-9_-]{40,}$/);

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
        bodyPreview: "(message body hidden)",
      });
      expect(JSON.stringify(listing)).not.toContain("971500000000");
      expect(JSON.stringify(listing)).not.toContain("Tomorrow at 3 PM");
      expect(JSON.stringify(listing)).not.toContain("booking_request");
      expect(listing.messages[0]).not.toHaveProperty("receiptHandle");

      const replay = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          origin: dashboard.url,
          "x-queuecraft-write-token": writeToken!,
        },
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

  it("rejects rebinding and cross-site write requests", async () => {
    const dashboard = await createQueueCraftDashboard({
      sqsClient: { send: vi.fn().mockResolvedValue({}) } as unknown as SQSClient,
      queueUrl: MAIN_QUEUE,
      dlqUrl: DLQ,
      port: 0,
    });

    try {
      const home = await fetch(dashboard.url);
      const page = await home.text();
      const writeToken = page.match(/const WRITE_TOKEN="([^"]+)"/)?.[1];
      expect(writeToken).toBeTruthy();

      const rebindingStatus = await requestStatus(
        dashboard.url + "/api/overview",
        { headers: { host: "attacker.example" } },
      );
      expect(rebindingStatus).toBe(403);

      const missingOrigin = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-queuecraft-write-token": writeToken!,
        },
        body: JSON.stringify({ messageId: "missing", confirm: "REPLAY" }),
      });
      expect(missingOrigin.status).toBe(403);

      const wrongOrigin = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          origin: "https://attacker.example",
          "x-queuecraft-write-token": writeToken!,
        },
        body: JSON.stringify({ messageId: "missing", confirm: "REPLAY" }),
      });
      expect(wrongOrigin.status).toBe(403);

      const wrongType = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: {
          "content-type": "text/plain",
          origin: dashboard.url,
          "x-queuecraft-write-token": writeToken!,
        },
        body: JSON.stringify({ messageId: "missing", confirm: "REPLAY" }),
      });
      expect(wrongType.status).toBe(415);

      const missingToken = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          origin: dashboard.url,
        },
        body: JSON.stringify({ messageId: "missing", confirm: "REPLAY" }),
      });
      expect(missingToken.status).toBe(403);

      const wrongToken = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          origin: dashboard.url,
          "x-queuecraft-write-token": "not-the-dashboard-token",
        },
        body: JSON.stringify({ messageId: "missing", confirm: "REPLAY" }),
      });
      expect(wrongToken.status).toBe(403);

      const jsonWithCharset = await fetch(dashboard.url + "/api/dlq/replay", {
        method: "POST",
        headers: {
          "content-type": "application/json; charset=utf-8",
          origin: dashboard.url,
          "x-queuecraft-write-token": writeToken!,
        },
        body: JSON.stringify({ messageId: "missing", confirm: "REPLAY" }),
      });
      expect(jsonWithCharset.status).toBe(409);
    } finally {
      await dashboard.close();
    }
  });

  it("coalesces concurrent replay requests even after the cache entry expires", async () => {
    let releaseDelete!: () => void;
    const deleteGate = new Promise<void>((resolve) => {
      releaseDelete = resolve;
    });
    const send = vi.fn(async (command: { constructor: { name: string } }) => {
      if (command.constructor.name === "ReceiveMessageCommand") {
        return {
          Messages: [
            {
              MessageId: "failed-1",
              ReceiptHandle: "receipt-1",
              Body: '{"private":"payload"}',
            },
          ],
        };
      }
      if (command.constructor.name === "DeleteMessageCommand") {
        await deleteGate;
      }
      return {};
    });
    const dashboard = await createQueueCraftDashboard({
      sqsClient: { send } as unknown as SQSClient,
      queueUrl: MAIN_QUEUE,
      dlqUrl: DLQ,
      port: 0,
      replayCacheTtlMs: 20,
    });

    try {
      const page = await fetch(dashboard.url).then((response) => response.text());
      const writeToken = page.match(/const WRITE_TOKEN="([^"]+)"/)?.[1];
      await fetch(dashboard.url + "/api/dlq");
      const replay = () =>
        requestStatus(dashboard.url + "/api/dlq/replay", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            origin: dashboard.url,
            "x-queuecraft-write-token": writeToken!,
          },
          body: JSON.stringify({ messageId: "failed-1", confirm: "REPLAY" }),
        });

      const first = replay();
      await vi.waitFor(() =>
        expect(
          send.mock.calls.filter(
            ([command]) => command.constructor.name === "DeleteMessageCommand",
          ),
        ).toHaveLength(1),
      );
      await new Promise((resolve) => setTimeout(resolve, 30));
      const second = replay();
      await new Promise((resolve) => setTimeout(resolve, 10));
      releaseDelete();

      const responses = await Promise.all([first, second]);
      expect(responses).toEqual([200, 200]);
      expect(
        send.mock.calls.filter(
          ([command]) => command.constructor.name === "SendMessageCommand",
        ),
      ).toHaveLength(1);
      expect(
        send.mock.calls.filter(
          ([command]) => command.constructor.name === "DeleteMessageCommand",
        ),
      ).toHaveLength(1);
    } finally {
      releaseDelete();
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
