# Automated real-AWS verification

QueueCraft's AWS workflow uses GitHub OIDC. GitHub receives a short-lived AWS
session for one workflow run; no AWS access key is stored in GitHub.

## Current offline maintenance state

The `queuecraft-github-trust` stack and its test role were deliberately removed
on 3 October 2026 to keep AWS offline. The recorded results in
[`verification.md`](verification.md) are historical evidence.

The workflow is now manual-only. It requires a maintainer to confirm that the
isolated test role has been restored and that temporary AWS test-resource
charges are accepted. Until then, leave the confirmation unchecked and use the
ordinary local and CI checks.

## Security boundary

The AWS role trusts only:

- GitHub owner `Yusuf-Karanib`, permanent ID `240980687`;
- repository `QueueCraft`, permanent ID `1308168974`;
- branch `main`;
- audience `sts.amazonaws.com`.

The numeric IDs prevent a renamed or deleted GitHub account or repository from
being replaced by an attacker who reuses its old name.

The role can manage only CloudFormation stacks, SQS queues, and DynamoDB tables
whose names begin with `queuecraft-ci-`. It cannot create IAM resources, read
YallaQueue resources, or access unrelated queues and tables.

## One-time AWS bootstrap

`infrastructure/github-oidc.yaml` creates the GitHub identity provider and the
least-privilege role. This stack remains in AWS while automated testing is in
use.

An AWS account can have only one GitHub OIDC provider. Before deployment, check
whether `token.actions.githubusercontent.com` already exists. If it does, set
`CreateGitHubOidcProvider` to `false` and supply its ARN through
`ExistingGitHubOidcProviderArn`.

Creating this trust is an account-security change. Use a temporary
administrator session for the bootstrap only. Do not use AWS root credentials
for normal QueueCraft work.

After the trust stack exists, normal runs use only the
`queuecraft-github-integration` role. It has no long-lived access key and cannot
manage IAM resources.

## Per-run resources

`.github/workflows/aws-integration.yml` creates a unique stack from
`infrastructure/integration-test.yaml`. That stack contains only:

- one SQS queue;
- one SQS dead-letter queue;
- one on-demand DynamoDB lease table.

The workflow builds and tests QueueCraft, runs the order-processing example
against those resources, and deletes the stack even when the example fails.
It verifies one successful order, one duplicate event, and one poison order
that reaches the DLQ. The stack name uses the GitHub run and attempt IDs, so it
cannot target YallaQueue's resources.

The workflow is started manually from the GitHub Actions page only after the
isolated role has been restored. Its first passing OIDC-backed run is recorded
in [`verification.md`](verification.md).
