# QueueCraft AWS infrastructure

The CloudFormation template creates the first real QueueCraft environment:

- One standard SQS job queue
- One dead-letter queue for repeatedly failing jobs
- One DynamoDB table for execution leases
- One publisher IAM policy
- One worker IAM policy
- One local dashboard IAM policy
- One dead-letter queue alarm
- One alarm for jobs that remain unfinished for more than five minutes
- An optional encrypted SNS topic and email subscription for that alarm

The queue and table use AWS-managed encryption. DynamoDB starts at five provisioned
read and write units so a small batch does not immediately throttle while the pilot
can remain inside the standard free allowance.
Paid point-in-time recovery is disabled by default.

## Important boundary

The template creates policies, not users or access keys. Do not put AWS keys in
GitHub. Decide how Replit will receive an AWS identity before creating keys.

GitHub's isolated integration tests use short-lived OIDC credentials instead
of access keys. The one-time trust template and its security boundary are
explained in [`../docs/aws-ci.md`](../docs/aws-ci.md).

## Deploy from the AWS console

1. Open CloudFormation in the AWS region you want to use.
2. Choose **Create stack** and **With new resources**.
3. Upload `cloudformation.yaml`.
4. Keep `Environment` set to `dev` for the first test.
5. Set `AlarmEmail` to the person responsible for failed jobs.
6. Acknowledge that the stack creates IAM resources.
7. Create the stack and wait for `CREATE_COMPLETE`.
8. Confirm the subscription from the email AWS sends.
9. Open the stack's **Outputs** tab.

The important outputs are:

- `AwsRegion`
- `QueueUrl`
- `IdempotencyTableName`
- `MetricsNamespace`
- `ProducerPolicyArn`
- `ConsumerPolicyArn`
- `DashboardPolicyArn`
- `DeadLetterAlarmName`
- `JobQueueAgeAlarmName`
- `AlarmTopicArn` when an alarm email was provided

Creating the resources does not start a worker. A separate consumer, such as an
SQS-triggered Lambda function, must use the queue and table outputs.

## Queue choice

The first environment deliberately uses a standard queue. It is designed to
exercise real at-least-once delivery and duplicate handling. FIFO ordering can
be added later after message-group behavior is specified and tested.

## Automation templates

- `integration-test.yaml` creates only temporary SQS and DynamoDB test
  resources.
- `github-oidc.yaml` creates the long-lived GitHub identity provider and its
  narrowly scoped test role.

Neither template grants access to YallaQueue's queues or tables.
