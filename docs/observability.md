# Observability

QueueCraft exposes structured lifecycle events through `onEvent`. Events do
not contain the SQS body or customer data, so they are safer to send to logs
and metrics than raw messages.

```ts
const poller = new QueueCraftPoller({
  // Other required options...
  onEvent(event) {
    console.log(JSON.stringify({ service: "booking-worker", ...event }));
  },
  onError(error, message) {
    console.error(JSON.stringify({
      service: "booking-worker",
      sqsMessageId: message?.MessageId,
      error: error instanceof Error ? error.message : "Unknown worker error",
    }));
  },
});
```

An observer that throws is ignored. Logging must never stop the worker.

## Event types

| Event | Meaning | Useful measurement |
| --- | --- | --- |
| `messages_received` | SQS returned work | received message count |
| `job_started` | QueueCraft acquired the execution lease | started job count |
| `job_completed` | completion was stored and SQS was acknowledged | completed count and duration |
| `job_failed` | the handler failed and the lease was released | failure count and duration |
| `job_cancelled` | ownership was lost or shutdown cancelled the handler | cancellation count |
| `job_duplicate` | the stable key already had state | duplicate count by state |
| `shutdown_timeout` | active work exceeded the shutdown grace period | shutdown health alert |

Useful application metrics are completion count, failure count, duplicate
count, cancellation count, and handler duration. Do not use phone numbers,
message text, or full idempotency keys as metric dimensions; high-cardinality
dimensions are expensive and can expose customer information.

The CloudFormation template also creates queue-level alarms for DLQ messages
and the age of the oldest unfinished message. These infrastructure alarms and
the application events answer different questions:

- SQS alarms show whether the queue is unhealthy.
- QueueCraft events show what the worker did with each job.

CloudWatch metric mapping and tracing adapters remain roadmap work. The event
API keeps that addition possible without forcing a specific logger today.
