/**
 * DynamoDB-backed execution leases.
 *
 * DynamoDB TTL is only used for eventual cleanup. Correctness comes from the
 * explicit `leaseUntil` timestamp and an owner token checked on every write.
 */
import {
  ConditionalCheckFailedException,
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";

export const LeaseState = {
  InProgress: "IN_PROGRESS",
  Completed: "COMPLETED",
  Failed: "FAILED",
} as const;

export type LeaseState = (typeof LeaseState)[keyof typeof LeaseState];

export interface ExecutionLease {
  readonly messageId: string;
  readonly ownerId: string;
}

export type AcquireLockResult =
  | { readonly status: "acquired"; readonly lease: ExecutionLease }
  | { readonly status: "in_progress" }
  | { readonly status: "completed" }
  | { readonly status: "failed" };

export interface IdempotencyStoreOptions {
  readonly client: DynamoDBClient;
  readonly tableName: string;

  /** How long one worker owns a job before another worker may take it over. */
  readonly leaseDurationSeconds?: number;

  /** How long terminal records remain available for duplicate detection. */
  readonly recordTtlSeconds?: number;

  /** Test hook. Production callers should leave this unset. */
  readonly now?: () => number;
}

const DEFAULT_LEASE_SECONDS = 60;
const DEFAULT_RECORD_TTL_SECONDS = 14 * 24 * 60 * 60;

export class IdempotencyStore {
  private readonly client: DynamoDBClient;
  private readonly tableName: string;
  private readonly leaseDurationSeconds: number;
  private readonly recordTtlSeconds: number;
  private readonly now: () => number;

  constructor(options: IdempotencyStoreOptions) {
    if (!options.tableName) {
      throw new Error("IdempotencyStore requires a non-empty tableName.");
    }

    this.leaseDurationSeconds =
      options.leaseDurationSeconds ?? DEFAULT_LEASE_SECONDS;
    this.recordTtlSeconds =
      options.recordTtlSeconds ?? DEFAULT_RECORD_TTL_SECONDS;

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
  async acquireLock(messageId: string, ownerId: string): Promise<AcquireLockResult> {
    this.assertIdentifier(messageId, "messageId");
    this.assertIdentifier(ownerId, "ownerId");

    for (let attempt = 0; attempt < 2; attempt++) {
      const nowSeconds = Math.floor(this.now() / 1000);
      const leaseUntil = nowSeconds + this.leaseDurationSeconds;

      try {
        await this.client.send(
          new UpdateItemCommand({
            TableName: this.tableName,
            Key: { messageId: { S: messageId } },
            UpdateExpression:
              "SET #state = :inProgress, #ownerId = :ownerId, " +
              "#leaseUntil = :leaseUntil, #updatedAt = :now, " +
              "#createdAt = if_not_exists(#createdAt, :now), " +
              "#expiresAt = :expiresAt",
            ConditionExpression:
              "attribute_not_exists(messageId) OR " +
              "(#state = :inProgress AND #leaseUntil <= :now)",
            ExpressionAttributeNames: {
              "#state": "state",
              "#ownerId": "ownerId",
              "#leaseUntil": "leaseUntil",
              "#updatedAt": "updatedAt",
              "#createdAt": "createdAt",
              "#expiresAt": "expiresAt",
            },
            ExpressionAttributeValues: {
              ":inProgress": { S: LeaseState.InProgress },
              ":ownerId": { S: ownerId },
              ":leaseUntil": { N: String(leaseUntil) },
              ":now": { N: String(nowSeconds) },
              ":expiresAt": {
                N: String(nowSeconds + this.recordTtlSeconds),
              },
            },
          }),
        );

        return {
          status: "acquired",
          lease: { messageId, ownerId },
        };
      } catch (error) {
        if (!(error instanceof ConditionalCheckFailedException)) {
          throw error;
        }

        const existingState = await this.readState(messageId);
        if (existingState === undefined) {
          continue;
        }

        return { status: this.toAcquireStatus(existingState) };
      }
    }

    return { status: "in_progress" };
  }

  async renewLease(lease: ExecutionLease): Promise<void> {
    const nowSeconds = Math.floor(this.now() / 1000);
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: lease.messageId } },
        UpdateExpression:
          "SET #leaseUntil = :leaseUntil, #updatedAt = :now, #expiresAt = :expiresAt",
        ConditionExpression:
          "#state = :inProgress AND #ownerId = :ownerId",
        ExpressionAttributeNames: {
          "#state": "state",
          "#ownerId": "ownerId",
          "#leaseUntil": "leaseUntil",
          "#updatedAt": "updatedAt",
          "#expiresAt": "expiresAt",
        },
        ExpressionAttributeValues: {
          ":inProgress": { S: LeaseState.InProgress },
          ":ownerId": { S: lease.ownerId },
          ":leaseUntil": {
            N: String(nowSeconds + this.leaseDurationSeconds),
          },
          ":now": { N: String(nowSeconds) },
          ":expiresAt": {
            N: String(nowSeconds + this.recordTtlSeconds),
          },
        },
      }),
    );
  }

  async markComplete(lease: ExecutionLease): Promise<void> {
    await this.transition(lease, LeaseState.Completed);
  }

  async markFailed(lease: ExecutionLease): Promise<void> {
    await this.transition(lease, LeaseState.Failed);
  }

  async releaseLock(lease: ExecutionLease): Promise<void> {
    await this.client.send(
      new DeleteItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: lease.messageId } },
        ConditionExpression:
          "#state = :inProgress AND #ownerId = :ownerId",
        ExpressionAttributeNames: {
          "#state": "state",
          "#ownerId": "ownerId",
        },
        ExpressionAttributeValues: {
          ":inProgress": { S: LeaseState.InProgress },
          ":ownerId": { S: lease.ownerId },
        },
      }),
    );
  }

  private async transition(
    lease: ExecutionLease,
    state: typeof LeaseState.Completed | typeof LeaseState.Failed,
  ): Promise<void> {
    const nowSeconds = Math.floor(this.now() / 1000);
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: lease.messageId } },
        UpdateExpression:
          "SET #state = :state, #updatedAt = :now, #expiresAt = :expiresAt " +
          "REMOVE #ownerId, #leaseUntil",
        ConditionExpression:
          "#state = :inProgress AND #ownerId = :ownerId",
        ExpressionAttributeNames: {
          "#state": "state",
          "#ownerId": "ownerId",
          "#leaseUntil": "leaseUntil",
          "#updatedAt": "updatedAt",
          "#expiresAt": "expiresAt",
        },
        ExpressionAttributeValues: {
          ":state": { S: state },
          ":inProgress": { S: LeaseState.InProgress },
          ":ownerId": { S: lease.ownerId },
          ":now": { N: String(nowSeconds) },
          ":expiresAt": {
            N: String(nowSeconds + this.recordTtlSeconds),
          },
        },
      }),
    );
  }

  private async readState(messageId: string): Promise<LeaseState | undefined> {
    const result = await this.client.send(
      new GetItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: messageId } },
        ConsistentRead: true,
        ProjectionExpression: "#state",
        ExpressionAttributeNames: { "#state": "state" },
      }),
    );

    const state = result.Item?.state?.S;
    return Object.values(LeaseState).includes(state as LeaseState)
      ? (state as LeaseState)
      : undefined;
  }

  private toAcquireStatus(
    state: LeaseState,
  ): Exclude<AcquireLockResult["status"], "acquired"> {
    switch (state) {
      case LeaseState.Completed:
        return "completed";
      case LeaseState.Failed:
        return "failed";
      default:
        return "in_progress";
    }
  }

  private assertIdentifier(value: string, name: string): void {
    if (!value) {
      throw new Error(`${name} must be a non-empty string.`);
    }
  }
}
