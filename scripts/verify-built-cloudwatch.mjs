import assert from "node:assert/strict";
import test from "node:test";
import { QueueCraftCloudWatchMetrics } from "@yusufkaranib/queuecraft";

test("the built package sends the expected monitoring metric", async () => {
  const sent = [];
  const namespace = "QueueCraft/BuildCheck";
  const timestamp = new Date("2026-01-01T00:00:00Z");
  const writer = new QueueCraftCloudWatchMetrics({
    client: { async send(command) { sent.push(command.input); } },
    namespace,
    dimensions: { Environment: "CI" },
    now: () => timestamp,
    flushIntervalMs: 0,
  });

  writer.onEvent({ type: "messages_received", count: 3 });
  await writer.close();

  assert.deepEqual(sent, [{
    Namespace: namespace,
    MetricData: [{
      MetricName: "MessagesReceived",
      Value: 3,
      Unit: "Count",
      Timestamp: timestamp,
      Dimensions: [{ Name: "Environment", Value: "CI" }],
    }],
  }]);
});

const invalidInputs = [
  { field: "namespace", options: { namespace: ":QueueCraft" }, error: /namespace.*colon/i },
  { field: "dimension name", options: { dimensions: { ":Environment": "CI" } }, error: /dimension names.*colon/i },
];

for (const { field, options, error } of invalidInputs) {
  test("the built package rejects a colon-prefixed " + field + " before sending", () => {
    let sends = 0;
    const client = { async send() { sends += 1; } };

    assert.throws(
      () => new QueueCraftCloudWatchMetrics({ client, ...options }),
      { name: "RangeError", message: error },
    );
    assert.equal(sends, 0);
  });
}
