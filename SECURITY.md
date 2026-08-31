# Security policy

## Reporting a vulnerability

Do not open a public issue containing credentials, customer data, or an
unpatched exploit. Contact the repository owner privately through the contact
method listed on the GitHub profile and include the affected version, impact,
and a minimal reproduction.

## Credential rules

- Never commit AWS keys, npm tokens, Meta tokens, Supabase service keys, or
  customer data.
- Do not run QueueCraft deployment or dashboard operations as the AWS root
  user.
- Use separate least-privilege identities for publishers, workers, dashboards,
  and infrastructure deployment.
- Prefer short-lived AWS and npm credentials over stored access keys.
- GitHub Actions must use the immutable repository-ID OIDC trust documented in
  `docs/aws-ci.md`; do not replace it with a broad repository wildcard.
- Rotate a credential immediately if it appears in logs, screenshots, commits,
  or chat.

## Supported versions

QueueCraft is currently an alpha. Only the newest published alpha receives
security fixes. Production readiness is not claimed yet.
