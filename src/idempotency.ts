/**
 * QueueCraft — idempotency / execution leases
 *
 * Uses a DynamoDB conditional write to guarantee that a given SQS message is
 * processed at most once, even when the same message is delivered more than
 * once (SQS standard queues are at-least-once).
 *
 * Lease lifecycle for a `messageId`:
 *   acquireLock -> PENDING        (in-flight; auto-expires via TTL if the worker dies)
 *     |-- success --> markComplete -> COMPLETED   (terminal; duplicates are skipped)
 *     |-- fatal   --> markFailed   -> FAILED       (terminal; sent to your DLQ logic)
 *     `-- transient/crash --> releaseLock          (record deleted; job may retry)
 */
import {
  DynamoDBClient,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  ConditionalCheckFailedException,
  type AttributeValue,
} from "@aws-sdk/client-dynamodb";

/** The three states an execution lease can hold. */
export const LeaseState = {
  Pending: "PENDING",
  Completed: "COMPLETED",
  Failed: "FAILED",
} as const;

export type LeaseState = (typeof LeaseState)[keyof typeof LeaseState];

export interface IdempotencyStoreOptions {
  /** A configured DynamoDB client (region/credentials handled by the caller). */
  readonly client: DynamoDBClient;

  /** Table whose partition key is `messageId` (String). */
  readonly tableName: string;

  /**
   * Optional lifetime for PENDING leases, in seconds. If the table has TTL
   * enabled on the `expiresAt` attribute, stale locks left behind by a crashed
   * worker are reclaimed automatically after this window. Strongly recommended.
   */
  readonly leaseTtlSeconds?: number;
}

export class IdempotencyStore {
  private readonly client: DynamoDBClient;
  private readonly tableName: string;
  private readonly leaseTtlSeconds?: number;

  constructor(options: IdempotencyStoreOptions) {
    if (!options.tableName) {
      throw new Error("IdempotencyStore requires a non-empty tableName.");
    }
    this.client = options.client;
    this.tableName = options.tableName;
    this.leaseTtlSeconds = options.leaseTtlSeconds;
  }

  /**
   * Attempt to claim an exclusive lease for `messageId`.
   *
   * Backed by a conditional `PutItem` that only writes when no record exists,
   * so concurrent workers racing on the same message are resolved atomically
   * by DynamoDB — exactly one wins.
   *
   * @returns `true` if the lease was acquired, `false` if it already exists
   *          (i.e. the message is in-flight, completed, or failed elsewhere).
   */
  async acquireLock(messageId: string): Promise<boolean> {
    const now = Date.now();
    const item: Record<string, AttributeValue> = {
      messageId: { S: messageId },
      state: { S: LeaseState.Pending },
      createdAt: { N: String(now) },
      updatedAt: { N: String(now) },
    };

    if (this.leaseTtlSeconds !== undefined) {
      item.expiresAt = {
        N: String(Math.floor(now / 1000) + this.leaseTtlSeconds),
      };
    }

    try {
      await this.client.send(
        new PutItemCommand({
          TableName: this.tableName,
          Item: item,
          ConditionExpression: "attribute_not_exists(messageId)",
        }),
      );
      return true;
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) {
        return false;
      }
      throw err;
    }
  }

  /**
   * Mark a lease `COMPLETED` (terminal success). Also clears the TTL so the
   * record persists as a tombstone and future duplicate deliveries of the same
   * message are rejected by `acquireLock`.
   *
   * Guarded by `attribute_exists` so a lost/expired lease surfaces as an error
   * rather than silently resurrecting the record.
   */
  async markComplete(messageId: string): Promise<void> {
    await this.transition(messageId, LeaseState.Completed);
  }

  /**
   * Mark a lease `FAILED` (terminal failure). Like `markComplete`, the record
   * is kept as a tombstone; route these to your dead-letter handling.
   */
  async markFailed(messageId: string): Promise<void> {
    await this.transition(messageId, LeaseState.Failed);
  }

  /**
   * Delete the lease so the job becomes eligible for reprocessing. Use this on
   * transient failures or during graceful shutdown. `DeleteItem` is idempotent,
   * so calling it on a missing record is a safe no-op.
   */
  async releaseLock(messageId: string): Promise<void> {
    await this.client.send(
      new DeleteItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: messageId } },
      }),
    );
  }

  /** Shared transition to a terminal state; clears the pending-lease TTL. */
  private async transition(
    messageId: string,
    state: LeaseState,
  ): Promise<void> {
    await this.client.send(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: { messageId: { S: messageId } },
        UpdateExpression:
          "SET #state = :state, #updatedAt = :updatedAt REMOVE expiresAt",
        ConditionExpression: "attribute_exists(messageId)",
        ExpressionAttributeNames: {
          "#state": "state",
          "#updatedAt": "updatedAt",
        },
        ExpressionAttributeValues: {
          ":state": { S: state },
          ":updatedAt": { N: String(Date.now()) },
        },
      }),
    );
  }
}