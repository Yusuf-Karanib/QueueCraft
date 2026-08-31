# Releasing QueueCraft

QueueCraft uses npm trusted publishing for future releases. GitHub Actions gets
a short-lived identity for one workflow run. No permanent npm token belongs in
GitHub secrets.

## First release only

The first package version must exist before npm can attach a trusted publisher.

1. Enable two-factor authentication for authorization and writes on the
   `yusufkaranib` npm account.
2. From a clean, CI-passing `main` branch, run:

```powershell
npm publish --access public
```

3. Configure the trusted publisher:

```powershell
npm trust github @yusufkaranib/queuecraft --file publish.yml --repo Yusuf-Karanib/QueueCraft --allow-publish -y
```

4. Confirm the npm package page links to the public GitHub repository.
5. Tag the exact published commit and push the tag.

Do not paste an npm token into this repository or a chat.

## Later releases

1. Update the version and `CHANGELOG.md`.
2. Run the complete local checks.
3. Commit and push to `main`.
4. Wait for CI to pass.
5. In GitHub Actions, run **Publish npm package**.
6. Confirm the npm version, then tag the exact commit.

The publish workflow uses a GitHub-hosted runner, Node.js 24, `id-token: write`,
and npm's OIDC exchange. npm automatically records provenance for a public
package published from a public repository through trusted publishing.
