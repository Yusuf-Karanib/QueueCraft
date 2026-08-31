import { describe, expect, it, vi } from "vitest";
import { CloudWatchClient } from "@aws-sdk/client-cloudwatch";
import {
  mapQueueCraftEventToCloudWatchMetrics,
  QueueCraftCloudWatchMetrics,
  type QueueCraftCloudWatchClient,
} from "./cloudwatch-metrics";

describe("CloudWatch metrics", () => {
  it("accepts the official AWS CloudWatch client", () => {
    const official = new CloudWatchClient({ region: "eu-central-1" });
    const compatible: QueueCraftCloudWatchClient = official;

    expect(compatible).toBe(official);
    official.destroy();
  });

  it("maps lifecycle events without exposing the idempotency key", () => {
    const metrics = mapQueueCraftEventToCloudWatchMetrics(
      {
        type: "job_completed",
        idempotencyKey: "private-customer-reference",
        attempt: 2,
        durationMs: 125,
      },
      {
        dimensions: { Service: "booking-worker" },
        now: () => new Date("2026-08-31T12:00:00Z"),
      },
    );

    expect(metrics).toEqual([
      expect.objectContaining({
        MetricName: "JobsCompleted",
        Value: 1,
        Unit: "Count",
        Dimensions: [{ Name: "Service", Value: "booking-worker" }],
      }),
      expect.objectContaining({
        MetricName: "JobDuration",
        Value: 125,
        Unit: "Milliseconds",
        Dimensions: [
          { Name: "Service", Value: "booking-worker" },
          { Name: "Outcome", Value: "completed" },
        ],
      }),
    ]);
    expect(JSON.stringify(metrics)).not.toContain("private-customer-reference");
  });

  it("batches metric writes and flushes remaining data on close", async () => {
    const send = vi.fn().mockResolvedValue({});
    const writer = new QueueCraftCloudWatchMetrics({
      client: { send } as QueueCraftCloudWatchClient,
      namespace: "YallaQueue/Workers",
      dimensions: { Environment: "test" },
      maxBatchSize: 2,
      flushIntervalMs: 0,
    });

    writer.onEvent({ type: "messages_received", count: 3 });
    writer.onEvent({
      type: "job_completed",
      idempotencyKey: "key-1",
      attempt: 1,
      durationMs: 20,
    });
    await writer.close();

    expect(send).toHaveBeenCalledTimes(2);
    const batches = send.mock.calls.map(
      ([command]) => command.input.MetricData as unknown[],
    );
    expect(batches.map((batch) => batch.length)).toEqual([2, 1]);
    expect(send.mock.calls[0][0].input.Namespace).toBe("YallaQueue/Workers");
    expect(writer.pendingMetricCount).toBe(0);
  });

  it("keeps a failed batch queued so an explicit retry can deliver it", async () => {
    const send = vi
      .fn()
      .mockRejectedValueOnce(new Error("CloudWatch unavailable"))
      .mockResolvedValueOnce({});
    const writer = new QueueCraftCloudWatchMetrics({
      client: { send } as QueueCraftCloudWatchClient,
      flushIntervalMs: 0,
    });
    writer.onEvent({ type: "job_started", idempotencyKey: "key-1", attempt: 1 });

    await expect(writer.flush()).rejects.toThrow("CloudWatch unavailable");
    expect(writer.pendingMetricCount).toBe(1);

    await expect(writer.flush()).resolves.toBeUndefined();
    expect(writer.pendingMetricCount).toBe(0);
  });

  it("rejects reserved namespaces and unbounded batches", () => {
    const client = { send: vi.fn() } as unknown as QueueCraftCloudWatchClient;
    expect(
      () => new QueueCraftCloudWatchMetrics({ client, namespace: "AWS/QueueCraft" }),
    ).toThrow("reserved AWS/ prefix");
    expect(
      () => new QueueCraftCloudWatchMetrics({ client, maxBatchSize: 1_001 }),
    ).toThrow("between 1 and 1000");
    expect(() =>
      mapQueueCraftEventToCloudWatchMetrics(
        { type: "messages_received", count: 1 },
        {
          dimensions: Object.fromEntries(
            Array.from({ length: 30 }, (_, index) => [`Dimension${index}`, "x"]),
          ),
        },
      ),
    ).toThrow("at most 29 entries");
  });
});
