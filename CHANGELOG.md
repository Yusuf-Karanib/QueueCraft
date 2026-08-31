# Changelog

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
