# Verification record

## 2026-09-01 — independent order-processing reference

Source commits: `58f8a07` and `68c7a94`

GitHub Actions runs:

- [CI `33528239336`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33528239336)
- [AWS integration `33528239353`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33528239353)

Disposable AWS stack: `queuecraft-ci-33528239353-1` in `eu-central-1`

Verified behavior:

- PASS: all 83 tests, type checking, and package building passed;
- PASS: the independent example used QueueCraft's public publisher and poller
  APIs with strict fake-order validation and a stable source-event key;
- PASS: a fake order was published to real SQS and its business handler ran;
- PASS: publishing the same source event again did not run the handler twice;
- PASS: a poison order failed and SQS redrove it to the real DLQ after repeated
  delivery;
- PASS: W3C `traceparent` and `tracestate` survived successful processing and
  DLQ redrive;
- PASS: the disposable stack deletion completed successfully;
- PASS: no YallaQueue or production resource was used or changed.

This verification changed the GitHub reference example only. The public npm
package remains `0.3.0`; no new package version was published.

## 2026-09-01 — public operations and trace-context release

Source commit and tag: `03d2f6b` / `v0.3.0`

GitHub Actions runs:

- [CI `33517614065`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33517614065)
- [AWS integration `33517614059`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33517614059)
- [trusted npm publish `33518234087`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33518234087)

Disposable AWS stack: `queuecraft-ci-33517614059-1` in `eu-central-1`

Verified behavior:

- PASS: all 73 tests, type checking, building, package inspection, public ESM
  import, and the production-dependency audit passed;
- PASS: the real AWS test covered successful SQS processing, duplicate
  suppression, repeated failure, DLQ redrive, and W3C trace-context survival;
- PASS: the disposable AWS stack deletion completed successfully;
- PASS: the version-tag-only workflow published `0.3.0` through npm trusted
  publishing without a stored npm token;
- PASS: the public registry reports `0.3.0` as the latest version;
- PASS: a clean temporary install downloaded exactly `0.3.0` and imported the
  publisher, poller, metrics, active tracing, W3C propagation, and dashboard
  APIs;
- PASS: the installed package has no runtime OpenTelemetry dependency;
- PASS: no YallaQueue or production resource was used or changed.

## 2026-09-01 — W3C trace context through SQS

Source commit: `5a13044`

GitHub Actions runs:

- [CI `33496094675`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33496094675)
- [AWS integration `33496094865`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33496094865)

Disposable AWS stack: `queuecraft-ci-33496094865-1` in `eu-central-1`

Verified behavior:

- PASS: all 73 tests, type checking, building, package inspection, public ESM
  import, and the production-dependency audit passed;
- PASS: the public structural interfaces accept the official OpenTelemetry API
  without adding OpenTelemetry as a production dependency;
- PASS: a valid lowercase W3C `traceparent` and `tracestate` crossed real SQS
  and were active while the successful handler ran;
- PASS: the same carrier survived at least two failed processing attempts and
  the real SQS dead-letter redrive;
- PASS: a duplicate job did not run its handler or restore trace context;
- PASS: invalid carriers are ignored, propagation failures cannot change queue
  settlement, and QueueCraft never propagates baggage;
- PASS: the guarded AWS workflow created only its isolated queue, DLQ, and
  DynamoDB table, then its deletion step completed successfully;
- PASS: no YallaQueue or production resource was used or changed.

This feature remains on `main` for the next deliberate minor release. The
public npm package remains `0.2.0`; this verification did not publish it.

## 2026-09-01 — operations dashboard and active handler tracing

Source commit: `5254dfb`

GitHub Actions runs:

- [CI `33460359246`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33460359246)
- [AWS integration `33460359247`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33460359247)

Additional disposable AWS stack:
`queuecraft-ops-verify-20260901-0543` in `eu-central-1`

The manual stack enabled `EnableOperationsDashboard` and
`EnableJobQueueDepthAlarm`. It used a 60-second age threshold with one
evaluation period and a one-message backlog threshold.

Verified behavior:

- PASS: all 54 tests, type checking, building, package inspection, and the
  production-dependency audit passed;
- PASS: active tracing kept the handler's asynchronous database/API work inside
  its OpenTelemetry-compatible active context without exposing message bodies
  or idempotency keys;
- PASS: AWS accepted and stored the opt-in dashboard with four widgets and seven
  CloudWatch metric-search expressions;
- PASS: the dashboard showed queue depth, approximate oldest unprocessed
  message age, worker outcomes, and average QueueCraft processing duration;
- PASS: AWS created healthy DLQ, oldest-job, and opt-in visible-backlog alarms
  with the requested thresholds;
- PASS: the guarded real SQS, DynamoDB, retry, and DLQ integration remained
  green;
- PASS: the manual stack deletion completed, CloudFormation then reported the
  stack as not found, and the temporary dashboard was absent;
- PASS: no YallaQueue or production stack was used or changed.

The dashboard and visible-backlog alarm remain disabled by default so a normal
QueueCraft deployment does not gain surprise CloudWatch resources or costs.

## 2026-09-01 — public observability release

Source commit and tag: `6627e38` / `v0.2.0`

GitHub Actions runs:

- [CI `33438508220`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33438508220)
- [AWS integration `33438507962`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33438507962)
- [trusted npm publish `33439508069`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33439508069)

Additional disposable AWS stack: `queuecraft-ci-20260901-20`

Verified behavior:

- PASS: the version-tag-only workflow published `0.2.0` through npm trusted
  publishing without a stored npm token;
- PASS: a clean anonymous install downloaded exactly version `0.2.0`;
- PASS: the installed package exported the publisher, poller, CloudWatch metrics,
  lifecycle tracing, and metric-mapping APIs;
- PASS: the installed package included the local dashboard command;
- PASS: both automated and manual disposable AWS tests covered real SQS
  processing, DynamoDB duplicate suppression, retry, and DLQ redrive;
- PASS: both disposable AWS stacks were deleted after verification.

## 2026-09-01 — CloudWatch metrics and tracing adapters

Source commit: `ee04054`

GitHub Actions runs:

- [CI `33417584405`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33417584405)
- [AWS integration `33417584478`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33417584478)

Verified behavior:

- PASS: 30 tests covered metric mapping, batching, retries, tracing lifecycle,
  Lambda events, and observer failure isolation;
- PASS: privacy assertions proved idempotency keys are not exported as metric
  data or span attributes;
- PASS: the adapter accepts the official AWS CloudWatch client;
- PASS: production dependencies reported no known vulnerabilities;
- PASS: AWS accepted the updated CloudFormation template;
- PASS: the existing real SQS, DynamoDB, retry, and DLQ integration still
  passed and its isolated stack was deleted.

The test did not publish a permanent custom CloudWatch metric or configure a
tracing exporter. Those actions can create lasting telemetry and AWS charges,
so applications opt into them explicitly.

## 2026-08-31 — automated isolated AWS integration

Source commit: `42c4744`

GitHub Actions run:
[`33414210743`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33414210743)

Temporary stack: `queuecraft-ci-33414210743-1`

Verified behavior:

- PASS: GitHub exchanged its repository identity for short-lived AWS credentials;
- PASS: the workflow created a unique SQS queue, DLQ, and DynamoDB table;
- PASS: QueueCraft completed a real SQS job and suppressed a duplicate through DynamoDB;
- PASS: an intentionally failed job was retried and moved to the real DLQ;
- PASS: the cleanup step completed and AWS no longer reports the temporary stack;
- PASS: the permanent role is limited to resources named `queuecraft-ci-*` and
  cannot manage IAM or YallaQueue resources.

No AWS access key is stored in GitHub.

## 2026-08-31 — real AWS integration

Source commit: `3c8c6fa`

Region: `eu-central-1`

Temporary stack: `queuecraft-integration-test`

Verified behavior:

- PASS: a job was published to real SQS and completed by QueueCraft;
- PASS: DynamoDB state prevented a repeated stable key from running twice;
- PASS: an intentionally failing job was retried and moved to the real DLQ;
- PASS: the runner removed its DynamoDB test records and DLQ message;
- PASS: the temporary CloudFormation stack was deleted after the run.

The existing `queuecraft-production` and YallaQueue stacks were not used or
changed by this test.

This was the first manual verification. The same guarded test is now automated
by the workflow recorded above.

## 2026-08-31 — public npm alpha

Published source commit: `4d9df0e`

Package: `@yusufkaranib/queuecraft@0.1.0`

Verified behavior:

- PASS: npm reports the scoped package as public and owned by `yusufkaranib`;
- PASS: a clean install succeeded with an empty npm configuration and fresh
  cache, so the test did not rely on the publisher's saved login;
- PASS: the installed package exported `QueueCraftPublisher`,
  `QueueCraftPoller`, and `createQueueCraftDashboard`;
- PASS: the installed package provided the `queuecraft-dashboard` command.

Search indexing is not used as release proof because npm documents that newly
published packages may take time to appear in search results.

## 2026-08-31 — trusted npm release

Source commit and tag: `3efa71b` / `v0.1.1`

GitHub Actions run:
[`33387393130`](https://github.com/Yusuf-Karanib/QueueCraft/actions/runs/33387393130)

Verified behavior:

- PASS: the version tag matched `package.json`;
- PASS: GitHub Actions completed tests, type checking, building, and publishing;
- PASS: npm accepted GitHub's short-lived OIDC identity without a stored npm
  token;
- PASS: a second clean, anonymous install downloaded version `0.1.1`;
- PASS: the installed public API, dashboard command, and corrected README were
  present.
