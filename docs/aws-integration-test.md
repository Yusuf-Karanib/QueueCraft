# Real AWS integration test

This test proves behavior that mocked unit tests cannot prove:

- a job can be published to real SQS and completed by the worker;
- publishing the same stable key again does not run the handler twice;
- a failing job is retried and moved by SQS to the real DLQ.

## Safety rule

Never run this against YallaQueue's live queue. The script consumes messages.
Use a separate QueueCraft test stack whose main queue and DLQ are empty.

## Prepare the test stack

Create a second CloudFormation stack from `infrastructure/cloudformation.yaml`.
Use these parameter values:

- `ProjectName`: `queuecrafttest`
- `Environment`: `dev`
- `VisibilityTimeoutSeconds`: `1`
- `MaxReceiveCount`: `2`
- `AlarmEmail`: leave blank

Copy `AwsRegion`, `QueueUrl`, `DeadLetterQueueUrl`, and
`IdempotencyTableName` from the stack Outputs tab.

Use temporary AWS credentials or an AWS profile with access to this dedicated
stack. Do not save access keys in this repository.

## Run it in PowerShell

```powershell
$env:AWS_REGION = "your stack region"
$env:SQS_QUEUE_URL = "your QueueUrl output"
$env:SQS_DLQ_URL = "your DeadLetterQueueUrl output"
$env:DYNAMODB_TABLE_NAME = "your IdempotencyTableName output"
$env:QUEUECRAFT_AWS_TEST_CONFIRM = "dedicated-queuecraft-test-stack"
npm run test:aws
```

The test refuses to start unless both queues appear empty. It deletes its own
DLQ message and DynamoDB records when finished. Delete the CloudFormation test
stack afterward so unused resources do not remain in the account.

## Verified run

The first real-AWS run passed on 2026-08-31 in `eu-central-1`. The temporary
stack was deleted immediately afterward. See `docs/verification.md` for the
recorded checks.
