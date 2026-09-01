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
| `JobDuration` | Milliseconds | QueueCraft processing time after lease acquisition, including handler and settlement work, split by bounded outcome |
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

This observer records the full QueueCraft lifecycle, including lease and queue
settlement time. It does not create active context around the business handler.

## Active handler tracing

`QueueCraftActiveTracing` is passed as the poller or Lambda processor's
`instrumentation` option. It uses the official OpenTelemetry tracer shape and
runs the business handler inside `startActiveSpan`, so instrumented database and
API calls can become child spans.

This API is currently on `main` and planned for the next minor npm release. It
is not part of the published `0.2.0` package.

```ts
import {
  QueueCraftActiveTracing,
  QueueCraftPoller,
} from "@yusufkaranib/queuecraft";

const activeTracing = new QueueCraftActiveTracing({
  tracer,
  attributes: {
    "service.name": "booking-worker",
    "deployment.environment": "dev",
  },
  onError(error) {
    console.error("QueueCraft tracing failed", error);
  },
});

const poller = new QueueCraftPoller({
  // Other required options...
  instrumentation: activeTracing,
});
```

The active span receives only the runtime (`poller` or `lambda`), receive
attempt, caller-supplied static attributes, outcome, and duration. The
instrumentation context also receives the cancellation signal. It never
receives the SQS message, idempotency key, receipt handle, queue URL, or message
body.

QueueCraft makes the real handler result authoritative. If tracing fails before
the handler, the handler still runs once. If tracing fails after successful
business work, QueueCraft reports the tracing error but does not release the
lease or retry the business work.

A custom `QueueCraftJobInstrumentation` implementation must invoke the provided
operation synchronously, await it, then settle. QueueCraft prevents a second
invocation and does not let delayed tracer cleanup block job settlement.

This creates active context inside the consumer. It does not yet inject trace
headers when publishing or extract an upstream trace from SQS. That cross-queue
carrier needs a separate privacy and compatibility review.

The CloudFormation template also creates queue-level alarms for DLQ messages
and the approximate age of the oldest unprocessed message. It can optionally create a sustained
backlog alarm and private CloudWatch operations dashboard. These infrastructure
alarms and the application events answer different questions:

- SQS alarms show whether the queue is unhealthy.
- QueueCraft events show what the worker did with each job.

Application metrics and queue alarms complement each other: metrics show what
the worker did, while alarms show whether work is building up or reaching the
DLQ.

The optional dashboard and backlog alarm default to off because CloudWatch
charges may apply. Never publicly share the dashboard; a public link can expose
queue activity and create additional metric-data request charges.

`ApproximateAgeOfOldestMessage` is an SQS approximation, not a list of every
ready job. Standard queues can reorder repeatedly failed messages, and a poison
message can stop contributing to this age before SQS moves it to the DLQ. Read
the age alarm together with the DLQ and depth measurements.
