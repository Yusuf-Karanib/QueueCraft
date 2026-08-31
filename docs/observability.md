# Observability

QueueCraft exposes structured lifecycle events through `onEvent`. Events never
contain the SQS body, but job events do contain the stable idempotency key so
adapters can match start and finish. Treat that key as private.

```ts
const poller = new QueueCraftPoller({
  // Other required options...
  onEvent(event) {
    const safeEvent = "idempotencyKey" in event
      ? { ...event, idempotencyKey: "[redacted]" }
      : event;
    console.log(JSON.stringify({ service: "booking-worker", ...safeEvent }));
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
The poller and Lambda processor expose the same event types.

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

## CloudWatch metrics

`QueueCraftCloudWatchMetrics` buffers events and sends them in batches. It never
copies an idempotency key or message body into CloudWatch.

```ts
import { CloudWatchClient } from "@aws-sdk/client-cloudwatch";
import {
  QueueCraftCloudWatchMetrics,
  QueueCraftPoller,
} from "@yusufkaranib/queuecraft";

const metrics = new QueueCraftCloudWatchMetrics({
  client: new CloudWatchClient({ region: process.env.AWS_REGION }),
  namespace: "queuecraft/dev",
  dimensions: {
    Service: "booking-worker",
    Environment: "dev",
  },
  onError(error) {
    console.error("CloudWatch metric delivery failed", error);
  },
});

const poller = new QueueCraftPoller({
  // Other required options...
  onEvent: metrics.onEvent,
});

await poller.start();
await metrics.close();
```

The writer sends after 20 metric data points or 10 seconds by default. Call
`close()` during shutdown so the final partial batch is sent. A failed batch
stays buffered and can be retried with `flush()`.

CloudWatch custom metrics may create AWS charges. Keep dimensions bounded and
enable only the measurements you intend to operate.

The infrastructure template grants the worker only
`cloudwatch:PutMetricData`, restricted to the stack's `MetricsNamespace`
output. Use that exact output as the adapter's `namespace`.

QueueCraft emits these metric names:

| Metric | Unit | Meaning |
| --- | --- | --- |
| `MessagesReceived` | Count | messages returned by SQS |
| `JobsStarted` | Count | execution leases acquired |
| `JobsCompleted` | Count | jobs committed and acknowledged |
| `JobsFailed` | Count | handlers that failed |
| `JobsCancelled` | Count | jobs cancelled after lost ownership or shutdown |
| `JobsDuplicate` | Count | duplicate jobs, split by bounded state |
| `JobDuration` | Milliseconds | processing time, split by bounded outcome |
| `ShutdownTimeouts` | Count | graceful shutdown deadlines exceeded |
| `ActiveJobsAtShutdown` | Count | active jobs at a shutdown timeout |
| `ShutdownTimeout` | Milliseconds | configured shutdown deadline |

## Tracing

`QueueCraftTracingObserver` accepts the small `startSpan`, `setAttribute`, and
`end` interface implemented by OpenTelemetry tracers and spans. QueueCraft does
not require an OpenTelemetry package, so the application remains in control of
its exporter and sampling configuration.

```ts
import { QueueCraftTracingObserver } from "@yusufkaranib/queuecraft";

const tracing = new QueueCraftTracingObserver({
  tracer, // Supply the tracer already configured by your application.
  attributes: {
    "service.name": "booking-worker",
    "deployment.environment": "dev",
  },
});

const onEvent = (event) => {
  metrics.onEvent(event);
  tracing.onEvent(event);
};

// Pass onEvent to QueueCraftPoller or QueueCraftLambdaProcessor.
// During shutdown:
tracing.close();
```

Job spans contain only the SQS system name, attempt, outcome, and duration.
QueueCraft uses the idempotency key only in memory to match start and finish
events; it never adds that key to span attributes.

This adapter records QueueCraft's lifecycle. It does not automatically make
database calls or other application work children of the QueueCraft span yet.

The CloudFormation template also creates queue-level alarms for DLQ messages
and the age of the oldest unfinished message. These infrastructure alarms and
the application events answer different questions:

- SQS alarms show whether the queue is unhealthy.
- QueueCraft events show what the worker did with each job.

Application metrics and queue alarms complement each other: metrics show what
the worker did, while alarms show whether work is building up or reaching the
DLQ.
