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

This API and the W3C propagation adapter below are included in version `0.3.0`.

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

## Producer-to-worker W3C trace context

`QueueCraftW3CTraceContext` connects QueueCraft to the application's existing
OpenTelemetry context and propagation APIs. QueueCraft does not import
OpenTelemetry at runtime and does not choose an exporter or sampling policy.

Install the optional API in the application, then configure an OpenTelemetry
SDK, asynchronous context manager, and W3C propagator by following the
[OpenTelemetry JavaScript setup](https://opentelemetry.io/docs/languages/js/getting-started/nodejs/):

```bash
npm install @opentelemetry/api
```

```ts
import { context, propagation, ROOT_CONTEXT, trace } from "@opentelemetry/api";
import {
  QueueCraftActiveTracing,
  QueueCraftPoller,
  QueueCraftPublisher,
  QueueCraftW3CTraceContext,
} from "@yusufkaranib/queuecraft";

const tracer = trace.getTracer("booking-worker");
const traceContext = new QueueCraftW3CTraceContext({
  context,
  propagation,
  rootContext: ROOT_CONTEXT,
});

const publisher = new QueueCraftPublisher({
  // Other required options...
  traceContext,
  onTraceContextError(error) {
    console.error("QueueCraft trace injection failed", error);
  },
});

const poller = new QueueCraftPoller({
  // Other required options...
  traceContext,
  instrumentation: new QueueCraftActiveTracing({ tracer }),
});
```

Use the same `traceContext` option with `QueueCraftLambdaProcessor`. Extraction
happens separately for each Lambda record, not once around the whole batch.
The remote producer context is restored first; active handler tracing then
creates the consumer span inside it.

The application must register a real OpenTelemetry SDK, asynchronous context
manager, and W3C propagator. The bare `@opentelemetry/api` package uses no-op
implementations and will not inject a carrier by itself.

QueueCraft transports only two fixed, lowercase SQS String attributes:
`traceparent` and optional `tracestate`. It deliberately drops baggage. With
the idempotency attribute, a traced QueueCraft job uses 3 of SQS's maximum 10
message attributes. Attribute bytes also count toward SQS's 1 MiB message
limit and can move a near-boundary message into another billed 64 KiB request
unit.

Treat trace context as private operational metadata, not authentication. A
queue writer can forge a valid trace ID, and vendor `tracestate` values can be
correlatable. Enable propagation only across a trusted queue boundary. Raw
carrier values never enter QueueCraft events, metrics, logs, idempotency
records, or span attributes. Malformed carriers are ignored and never fail a
job.

Never put customer, message, credential, or other personally identifiable data
in `tracestate`. Its vendor values are opaque to QueueCraft, so QueueCraft can
validate their wire format but cannot determine whether their content is safe.

QueueCraft does not create a producer/send span. For one-message processing,
the extracted producer context becomes the handler span's parent. Each retry
can therefore create a sibling consumer span under the same producer. A
duplicate stopped by the idempotency lease creates no handler span. QueueCraft
does not yet create span links for Lambda batches or replay operations.

The local DLQ replay copies the original message attributes, so it preserves
the original trace carrier. A later replay-specific span/link policy may make
that relationship more explicit. Propagation failures are isolated: the
publisher still sends once without trace fields, and a worker still runs the
handler once without the remote context.

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
