import { describe, expect, it, vi } from "vitest";
import type { DynamoDBClient, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { IdempotencyStore } from "./idempotency";

describe("IdempotencyStore lease deadlines", () => {
  it("grants the full lease duration near a whole-second boundary", async () => {
    const send = vi.fn().mockResolvedValue({});
    const store = new IdempotencyStore({
      client: { send } as unknown as DynamoDBClient,
      tableName: "leases",
      leaseDurationSeconds: 1,
      now: () => 1_700_000_000_999,
    });

    await store.acquireLock("message-1", "owner-1");
    await store.renewLease({ messageId: "message-1", ownerId: "owner-1" });

    const commands = send.mock.calls.map(
      ([command]) => (command as UpdateItemCommand).input,
    );
    expect(commands[0].ExpressionAttributeValues?.[":now"]?.N).toBe(
      "1700000000",
    );
    expect(commands[0].ExpressionAttributeValues?.[":leaseUntil"]?.N).toBe(
      "1700000002",
    );
    expect(commands[1].ExpressionAttributeValues?.[":leaseUntil"]?.N).toBe(
      "1700000002",
    );
  });
});
