# Verification record

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

This is a manually triggered verification record. The next step is to run the
same guarded test from GitHub Actions using short-lived AWS credentials and an
automatically removed test stack.
