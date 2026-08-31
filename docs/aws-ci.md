# Automated real-AWS verification

QueueCraft's AWS workflow uses GitHub OIDC. GitHub receives a short-lived AWS
session for one workflow run; no AWS access key is stored in GitHub.

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

When a standalone account has no non-root administrator,
`infrastructure/temporary-bootstrap-role.yaml` can create a temporary GitHub
OIDC role. `.github/workflows/aws-bootstrap.yml` uses it once to create only the
`queuecraft-github-trust` stack and `queuecraft-github-integration` role. Delete
the bootstrap stack and workflow immediately afterward. The temporary role must
never become a general-purpose deployment identity.

## Per-run resources

`.github/workflows/aws-integration.yml` creates a unique stack from
`infrastructure/integration-test.yaml`. That stack contains only:

- one SQS queue;
- one SQS dead-letter queue;
- one on-demand DynamoDB lease table.

The workflow builds and tests QueueCraft, runs the guarded test against those
resources, and deletes the stack even when the test fails. The stack name uses
the GitHub run and attempt IDs, so it cannot target YallaQueue's resources.

The workflow starts manually until the first OIDC-backed run passes. After that
proof, it can be limited to relevant changes on `main`.
