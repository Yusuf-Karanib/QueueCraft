# AWS account and production checklist

The included CloudFormation stack is for development and testing. This list is
the minimum account-side review before building a separate production stack.
Record owners and decisions, but never copy passwords, tokens, keys, customer
data, or secret values into this repository.

## Account security

- Protect the AWS root user with a hardware-backed MFA method, do not create a
  root access key, and use it only for account recovery tasks.
- Give people short-lived access through IAM Identity Center or an equivalent
  federation system. Do not share IAM users or access keys.
- Give each workload its own role and review QueueCraft's producer, consumer,
  and dashboard policies separately.
- Enable an organization trail or account CloudTrail trail, protect its storage,
  and decide who reviews security findings and sign-in alerts.
- Set current security, billing, and operations contacts on the account.
- Keep GitHub OIDC trust restricted to the expected repository, numeric owner
  and repository IDs, branch, and `sts.amazonaws.com` audience. Reuse the
  account's existing GitHub provider instead of creating a duplicate.

## Rotation and access review

- Keep an inventory containing each credential or trust, its owner, where it is
  used, and the event or date that triggers rotation.
- Prefer roles and OIDC over long-lived AWS keys. Remove unused keys and roles;
  rotate any unavoidable key through the service that owns it.
- Review GitHub collaborators, branch protection, tag protection, OIDC trust,
  and npm trusted-publisher settings after staff or repository changes.
- Test secret rotation in a non-production stack. Store only secret names or
  ARNs in runbooks, never their values.

## Budget and capacity

- Create an AWS Budget with more than one alert threshold and named recipients.
- Enable cost-anomaly monitoring, tag resources with owner, project, and
  environment, and review spend at a fixed interval.
- Estimate SQS requests, DynamoDB capacity, CloudWatch custom metrics and
  dashboards, SNS, backups, and data transfer. Free-tier allowance is not a
  budget control.
- Set capacity, concurrency, and service quotas from measured load. Alarm on
  throttling, old messages, queue depth, DLQ messages, and application errors.

## Retention and recovery

- Choose queue and DLQ retention from the incident-response window. The
  development template currently keeps normal messages for four days and DLQ
  messages for fourteen days.
- Choose the completed-record TTL from the longest possible producer retry
  period. DynamoDB TTL deletion is delayed, so correctness must use the stored
  lease and state rather than assume immediate deletion.
- Decide whether DynamoDB point-in-time recovery, backups, deletion protection,
  and `Retain` policies are required. They are not all enabled by the
  development template.
- Set log retention deliberately, restrict log access, and confirm that payloads
  and identifiers are excluded before increasing retention.
- Write and test restore, DLQ replay, credential-compromise, and region-failure
  procedures. Assign a person to every alarm.

## Production approval record

- Document data classification, chosen AWS Region, encryption requirements,
  recovery targets, expected peak load, and accepted residual risks.
- Review the final production template independently. Test stack updates,
  rollback, backup restore, overload behavior, lease loss, duplicate delivery,
  and partial-batch failure before customer traffic.
- Keep evidence of the review and test results without including secrets or
  customer payloads.
