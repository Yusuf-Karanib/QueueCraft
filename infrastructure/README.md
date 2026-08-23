# QueueCraft AWS infrastructure

The CloudFormation template creates the first real QueueCraft environment:

- One standard SQS job queue
- One dead-letter queue for repeatedly failing jobs
- One DynamoDB table for execution leases
- One publisher IAM policy
- One worker IAM policy

The queue and table use AWS-managed encryption. DynamoDB uses on-demand billing,
and paid point-in-time recovery is disabled by default.

## Important boundary

The template creates policies, not users or access keys. Do not put AWS keys in
GitHub. Decide how Replit will receive an AWS identity before creating keys.

## Deploy from the AWS console

1. Open CloudFormation in the AWS region you want to use.
2. Choose **Create stack** and **With new resources**.
3. Upload `cloudformation.yaml`.
4. Keep `Environment` set to `dev` for the first test.
5. Acknowledge that the stack creates IAM resources.
6. Create the stack and wait for `CREATE_COMPLETE`.
7. Open the stack's **Outputs** tab.

The important outputs are:

- `AwsRegion`
- `QueueUrl`
- `IdempotencyTableName`
- `ProducerPolicyArn`
- `ConsumerPolicyArn`

Creating the resources does not start a worker. A separate Node.js process must
construct `QueueCraftPoller` and keep `start()` running.

## Queue choice

The first environment deliberately uses a standard queue. It is designed to
exercise real at-least-once delivery and duplicate handling. FIFO ordering can
be added later after message-group behavior is specified and tested.
