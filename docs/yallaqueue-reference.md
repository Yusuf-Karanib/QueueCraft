# YallaQueue reference architecture

YallaQueue is QueueCraft's first real consumer. It proves that QueueCraft is a
reusable engine rather than a queue built only for a demonstration script.

```text
Customer WhatsApp message
        |
        v
Meta signed webhook -> AWS Lambda web app -> QueueCraftPublisher -> SQS
                                                                    |
                                                                    v
                                                        SQS-triggered Lambda
                                                                    |
                                                                    v
                                                     QueueCraftLambdaProcessor
                                                        |       |       |
                                                        v       v       v
                                                    Supabase  WhatsApp  SES
```

## QueueCraft's responsibility

- attach the stable Meta message ID to the SQS job;
- process Lambda SQS batches with partial-message failure responses;
- use DynamoDB to lease jobs and remember completed stable keys;
- let SQS redrive repeatedly failing messages to the DLQ;
- provide separate producer and consumer IAM policies and queue alarms.

## YallaQueue's responsibility

- verify Meta's webhook signature;
- parse the booking request and business rules;
- reserve a non-overlapping appointment in Supabase;
- send the customer WhatsApp confirmation and barber email;
- keep customer and shop data protected by the application's own rules.

This boundary matters. QueueCraft does not know what an appointment is, and
YallaQueue does not reimplement queue leases, partial batch retries, or DLQ
infrastructure.

## Duplicate path

Meta can retry a webhook. YallaQueue republishes that retry with the same Meta
message ID. QueueCraft uses that stable key in DynamoDB. If the first execution
already completed, the repeated SQS message is acknowledged without running
the booking handler again.

The appointment database also enforces its own uniqueness rules. That second
layer is intentional because no queue library can guarantee exactly-once side
effects across every external system.

## Cost and scaling choice

YallaQueue uses the Lambda processor instead of an always-running poller. SQS
wakes the worker only when jobs exist, which fits a small pilot. A service with
steady traffic can use `QueueCraftPoller` in a long-running container instead.

## Current limit

QueueCraft protects the job execution record, but a provider can accept a
notification immediately before the process crashes. A retry could then send
that notification again. YallaQueue's appointment remains unique, but fully
idempotent provider-side notification delivery is future work.
