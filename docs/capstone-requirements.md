# QueueCraft capstone requirements

Status: frozen baseline for maintenance work  
Date: 11 October 2026

## Purpose

QueueCraft proves that a reusable TypeScript library can move slow work from a
web request into AWS SQS while handling the reliability details that a basic
demo usually skips.

The capstone goal is not to add more features. It is to show that changes can
be specified, tested, reviewed, secured, observed, and explained.

## Current operating state

- Package version `0.3.2` is public on npm.
- The repository is maintained as a public alpha.
- AWS is offline to avoid ongoing charges.
- Real-AWS results in `verification.md` are historical evidence.
- YallaQueue is a reference consumer, not proof of commercial production use.

## Functional requirements

| ID | Requirement |
| --- | --- |
| F1 | The publisher must serialize a job, attach a stable caller-supplied idempotency key when provided, and send it to SQS. |
| F2 | The long-running poller must never process more jobs concurrently than its configured limit. |
| F3 | The Lambda processor must return individual failed record IDs so successful records are not retried with a failed record. |
| F4 | A worker must acquire a DynamoDB execution lease before running a handler and must not settle work after ownership becomes uncertain. |
| F5 | A completed logical job must be recorded before its SQS transport message is acknowledged. |
| F6 | Handler failure must leave the message available for SQS retry and eventual DLQ redrive. |
| F7 | Long-running work must renew the relevant lease and visibility deadline. |
| F8 | QueueCraft must emit bounded lifecycle events that can drive metrics and traces without copying message bodies into them. |
| F9 | The local dashboard must hide message bodies, bind only to loopback, and require explicit confirmation for DLQ replay. |
| F10 | The supplied CloudFormation template must remain limited to development and test environments. |

## Non-functional requirements

### Correctness

- QueueCraft must describe SQS as at-least-once delivery. It must not claim
  universal exactly-once side effects.
- Application handlers remain responsible for payload validation and
  retry-safe external side effects.
- A tracing, metrics, or logging failure must not run a business handler twice.

### Security and privacy

- No credential, customer payload, receipt handle, or secret may be committed.
- Built-in metrics and spans must not export message bodies or idempotency keys.
- Publisher, worker, dashboard, CI, and deployment identities must use separate
  least-privilege permissions.
- npm publishing and AWS CI must use short-lived identities rather than stored
  access tokens.

### Verification

- Tests, type checking, build, package inspection, and both dependency audits
  must pass before a release.
- Supported Node.js versions must be checked in CI.
- Real-AWS verification must use isolated temporary resources and must never
  use YallaQueue queues or customer data.
- Real-AWS testing must remain manual while the AWS test role is offline.

### Cost control

- Local work and ordinary CI must require no AWS resources.
- A person must deliberately confirm before a real-AWS test can create
  temporary resources.
- Optional dashboards and custom metrics must remain opt-in because they can
  create charges.

### Explainability

A maintainer must be able to explain:

- why SQS is used;
- why duplicate delivery is possible;
- what DynamoDB leases do and do not guarantee;
- what happens on handler, heartbeat, and settlement failure;
- which data belongs to QueueCraft and which belongs to the application; and
- why the project is called an alpha rather than production-ready.

## Acceptance evidence

| Requirement area | Primary evidence |
| --- | --- |
| Publisher and idempotency | `src/publisher.test.ts`, `src/idempotency.test.ts` |
| Poller reliability | `src/poller.test.ts` |
| Lambda partial failure | `src/lambda-processor.test.ts` |
| Metrics and trace privacy | `src/cloudwatch-metrics.test.ts`, `src/tracing.test.ts`, `src/trace-context.test.ts` |
| Dashboard boundary | `src/dashboard.test.ts`, `docs/dashboard.md` |
| Infrastructure boundary | `src/cloudformation.test.ts`, `infrastructure/cloudformation.yaml` |
| Real AWS behavior | `docs/verification.md` |
| Release safety | `.github/workflows/ci.yml`, `.github/workflows/publish.yml` |

## Non-goals

- commercial production-readiness claims;
- exactly-once external API calls;
- application-specific payload validation;
- a hosted multi-user operations dashboard;
- FIFO replay policy;
- another reference application;
- restarting YallaQueue or AWS merely for portfolio appearance; and
- new AI product features.

## Change rule

A maintenance change must identify the requirement it protects, add or update
evidence, avoid unrelated edits, and leave all required checks passing. A new
feature requires a real user or pilot requirement and is outside this phase.
