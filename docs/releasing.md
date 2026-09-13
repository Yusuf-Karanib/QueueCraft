# Releasing QueueCraft

QueueCraft uses npm trusted publishing for automated releases. GitHub Actions
gets a short-lived identity for one workflow run. No permanent npm token
belongs in GitHub secrets.

## Initial release

Version `0.1.0` was published interactively with two-factor authentication on
2026-08-31. A clean installation with no saved npm login verified that the
package is public.

The first version had to exist before npm could attach a trusted publisher.

## Trusted publisher

The package trusts GitHub Actions from `Yusuf-Karanib/QueueCraft`, using
`.github/workflows/publish.yml`, for `npm publish`. Version `0.1.1` was the first
release published through this connection.

Do not paste an npm token into this repository or a chat.

## Release a new version

1. Update the version and `CHANGELOG.md`.
2. Run the complete local checks.
3. Commit and push to `main`.
4. Wait for CI to pass.
5. Tag that exact commit as `v` followed by the package version and push the
   tag. For example, package version `0.1.1` requires tag `v0.1.1`.
6. Wait for **Publish npm package** to pass, then verify a clean installation.

The workflow refuses a version tag that does not match `package.json`, a tag
whose commit is not contained in the repository's default branch, or a package
that fails tests, type checking, build, and the dry-run package inspection.
There is no manual publish trigger.

The publish workflow uses a GitHub-hosted runner, Node.js 24, `id-token: write`,
and npm's OIDC exchange. npm automatically records provenance for a public
package published from a public repository through trusted publishing.

## Repository-side controls

- Protect `main` and require the ordinary CI workflow before merging.
- Protect `v*` tags so only release maintainers can create or delete them.
- Keep npm trusted publishing limited to this repository and
  `.github/workflows/publish.yml`.
- Review automated action updates before changing the immutable action commit
  hashes in a workflow.
- After release, compare the npm version and provenance with the intended Git
  tag, and install it in a clean directory.
