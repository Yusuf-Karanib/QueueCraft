# Verification record

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
