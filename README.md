# QueueCraft

QueueCraft is an early TypeScript toolkit for publishing and processing AWS
SQS jobs with DynamoDB-backed duplicate protection.

## Project status

QueueCraft is an alpha prototype. It is not ready for production use yet.

Implemented:

- SQS publisher with caller-controlled idempotency keys
- Long-polling worker with bounded concurrency
- Owner-based DynamoDB execution leases
- Safe takeover of expired leases
- SQS visibility and DynamoDB lease heartbeats for long jobs
- Handler cancellation signal when ownership is lost
- Duplicate acknowledgement after a completed job
- Unit tests for publishing, completion, retries, and duplicate handling
- CloudFormation for SQS, a dead-letter queue, DynamoDB, least-privilege IAM,
  and an optional email-backed dead-letter queue alarm

Still required before production use:

- Process-signal helpers and bounded graceful-shutdown timeouts
- Integration tests against LocalStack or real AWS resources
- Structured logging, metrics, and tracing

## Important delivery rule

SQS and webhook systems can deliver the same logical event more than once.
QueueCraft reduces duplicate execution, but it does not promise universal
"exactly once" side effects. Job handlers must still be designed so retrying
them is safe.

## Publishing a job

Use a stable identifier from the source event. For a WhatsApp webhook, use the
Meta message ID instead of generating a new value on every retry.

```ts
await publisher.publish(
  {
    type: "booking_request",
    phoneNumber: "971500000000",
    requestedTime: "2026-08-24T15:00:00+04:00",
  },
  { idempotencyKey: whatsappMessageId },
);
```

When no idempotency key is supplied, QueueCraft generates a UUID. That is only
appropriate when the publish operation will never be retried as the same job.

## DynamoDB table requirement

The idempotency table must use a String partition key named `messageId`. Enable
DynamoDB TTL on the Number attribute `expiresAt` for eventual record cleanup.
Lease correctness does not depend on prompt TTL deletion; QueueCraft checks the
explicit `leaseUntil` timestamp when taking over abandoned work.

## Development

```bash
npm ci
npm test
npm run typecheck
npm run build
```

The current tests mock AWS. A passing unit test suite does not replace the
planned end-to-end AWS test.

## AWS infrastructure

The first CloudFormation template and beginner deployment instructions are in
`infrastructure/`. The template creates a standard queue, DLQ, DynamoDB lease
table, and separate least-privilege publisher and worker policies.
