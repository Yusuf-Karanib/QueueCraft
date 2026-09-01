# QueueCraft AWS infrastructure

The CloudFormation template creates the first real QueueCraft environment:

- One standard SQS job queue
- One dead-letter queue for repeatedly failing jobs
- One DynamoDB table for execution leases
- One publisher IAM policy
- One worker IAM policy
- One local dashboard IAM policy
- One dead-letter queue alarm
- One configurable alarm for the approximate age of the oldest unprocessed message
- One optional sustained visible-backlog alarm
- One optional private CloudWatch operations dashboard
- An optional SNS topic and email subscription for the alarms

The queue and table use AWS-managed encryption. DynamoDB starts at five
provisioned read and write units so a small batch does not immediately throttle.
Those units may be billable depending on the account's other usage.
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
6. Keep `EnableOperationsDashboard` and `EnableJobQueueDepthAlarm` set to
   `false` unless you want those optional CloudWatch resources.
7. Set `JobQueueAgeAlarmThresholdSeconds`,
   `JobQueueAgeAlarmEvaluationPeriods`, and the optional depth threshold to
   match the business's normal throughput. The default age alarm requires the
   five-minute threshold to remain breached for 15 consecutive one-minute
   periods before it fires.
8. Acknowledge that the stack creates IAM resources.
9. Create the stack and wait for `CREATE_COMPLETE`.
10. Confirm the subscription from the email AWS sends.
11. Open the stack's **Outputs** tab.

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
- `JobQueueAgeAlarmThresholdSeconds`
- `JobQueueAgeAlarmEvaluationPeriods`
- `JobQueueDepthAlarmName` and `JobQueueDepthAlarmThresholdMessages` when the
  depth alarm is enabled
- `OperationsDashboardName` when the dashboard is enabled
- `AlarmTopicArn` when an alarm email was provided

Creating the resources does not start a worker. A separate consumer, such as an
SQS-triggered Lambda function, must use the queue and table outputs.

## Optional operations view

`EnableOperationsDashboard=true` creates a private CloudWatch dashboard for:

- ready, in-flight, delayed, and dead-letter message counts;
- the oldest unprocessed-message age and its alarm threshold;
- QueueCraft worker outcomes; and
- average QueueCraft processing duration by outcome.

Worker graphs stay empty until an application sends QueueCraft custom metrics
to the stack's `MetricsNamespace`. CloudWatch searches only find custom metrics
that have reported data recently.

Do not publicly share this dashboard. Public links expose queue activity and can
create `GetMetricData` charges. Dashboards, alarms, custom metrics, and metric
dimension combinations may also be billed. The DLQ and age alarms are always
created. The extra dashboard and backlog alarm default to `false` so the
operator makes that additional cost decision explicitly.

The optional SNS topic is not encrypted at rest by default. CloudWatch alarm
delivery through an encrypted SNS topic requires a customer-managed KMS key
whose policy permits the CloudWatch service; the AWS-managed SNS key cannot be
given that policy. QueueCraft alarm descriptions never include message bodies
or customer fields. If your policy requires SNS encryption, configure a
customer-managed key by following AWS's
[SNS key-management guidance](https://docs.aws.amazon.com/sns/latest/dg/sns-key-management.html).

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
