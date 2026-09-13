# Changelog

## 0.3.1 — 2026-09-13

- Hardened the loopback dashboard against DNS rebinding, cross-site writes,
  framing, and body disclosure, and coalesced simultaneous replay requests.
- Added DynamoDB lease heartbeats to Lambda batch processing and made both
  worker runtimes emit a terminal event when durable settlement fails.
- Made poller heartbeat timing respect both SQS visibility and DynamoDB leases,
  and rounded DynamoDB deadlines so configured leases are never shortened.
- Bounded the CloudWatch metric backlog, exposed a dropped-data-point counter,
  and closed tracing spans when job settlement fails.
- Documented the required Lambda `ReportBatchItemFailures` setting and clarified
  that the included AWS stack is for development and testing only.
- Reframed the repository as a maintained, feature-complete portfolio project
  and moved the safe local demo, screenshot, requirements, and AWS verification
  evidence into the main README.
- Improved the package description and release documentation.
- Added a second, business-neutral order-processing reference application with
  strict payload validation and stable source-event idempotency keys.
- Made the guarded real-AWS workflow verify the order example's success,
  duplicate suppression, retries, DLQ redrive, and W3C trace propagation.
- Added portfolio-ready architecture notes and a short demonstration script.

## 0.3.0 — 2026-09-01

- Added an opt-in private CloudWatch operations dashboard and sustained queue
  backlog alarm.
- Made the oldest-message alarm threshold and evaluation window configurable,
  with a default of 15 consecutive one-minute breaches to reduce noise.
- Added active OpenTelemetry-compatible handler tracing for poller and Lambda
  processors without exposing message bodies or idempotency keys.
- Isolated instrumentation failures so they cannot run a handler twice or turn
  successful business work into an SQS retry.
- Added opt-in W3C `traceparent` and `tracestate` propagation from publishers
  through SQS to poller and Lambda handlers without a runtime OpenTelemetry
  dependency or baggage propagation.

## 0.2.0 — 2026-09-01

- Added buffered CloudWatch lifecycle metrics with bounded, privacy-safe
  dimensions.
- Added an OpenTelemetry-compatible tracing observer that does not export job
  payloads or idempotency keys.
- Added lifecycle events to the Lambda SQS processor so poller and Lambda
  deployments share the same observability adapters.
- Restricted the generated worker policy to publishing metrics only in its
  QueueCraft CloudWatch namespace.

## 0.1.1 — 2026-08-31

- Corrected the public installation and release documentation after verifying
  the first anonymous npm install.
- Added an npm trusted-publishing workflow trigger for version tags.

## 0.1.0 — 2026-08-31

First public alpha:

- SQS publisher with stable idempotency keys;
- long-polling worker and Lambda batch processor;
- DynamoDB execution leases and duplicate suppression;
- visibility and lease heartbeats;
- bounded concurrency and graceful shutdown;
- structured worker lifecycle events;
- loopback-only queue dashboard with privacy-redacted DLQ replay;
- CloudFormation queue, DLQ, DynamoDB, IAM policies, and alarms;
- unit tests, GitHub CI, and a verified real-AWS integration runner.
