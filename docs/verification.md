# Verification record

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
