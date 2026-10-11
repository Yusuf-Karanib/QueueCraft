# QueueCraft capstone baseline

Audit date: 11 October 2026  
Audited commit: `6afd199` (`main`)  
Published package: `@yusufkaranib/queuecraft@0.3.1`

## Decision

QueueCraft is ready to be maintained as a **tested portfolio alpha** and used
as the capstone for learning production agent engineering.

It is not a production-proven service and it is not currently live on AWS.
Those limits are already stated honestly in the repository and must remain
visible.

The next work should improve correctness and evidence. It should not add new
product features.

## What was verified locally

| Check | Result |
| --- | --- |
| Git state before the report | Clean and synchronized with `origin/main` |
| Unit tests | 99 passed across 12 test files |
| Type checking | Passed |
| Package build | Passed |
| Package dry run | Passed; 9 files, 30.9 kB packed |
| Published-code dependency audit | Passed with zero known vulnerabilities |
| Full dependency audit | Passed with zero known vulnerabilities after the lockfile fix |
| Current tracked-file secret-pattern check | No high-confidence secret pattern found |

After this audit, the selected CloudWatch validation patch raised the current
suite to 118 passing tests. The fixed comparison and its final verification are
recorded in the [agent-workflow evaluation result](agent-evaluation-results.md).

The build and package checks needed wider local file access because the Codex
sandbox could not read the parent workspace. That was a sandbox limitation,
not a QueueCraft failure.

The secret check covered the current tracked tree only. It was not a complete
Git-history scan, and no dedicated secret-scanning tool is installed locally.

## Strong evidence already present

- The public API separates publishing, processing, execution leases,
  observability, and the local operator dashboard.
- The poller limits concurrency and does not receive more messages than it has
  room to process.
- SQS visibility and DynamoDB ownership are renewed while work runs.
- A worker stops settlement when it can no longer prove ownership.
- Completion is stored before an SQS message is acknowledged, reducing repeat
  execution after a delete failure.
- The Lambda processor returns per-record failures for partial-batch retry.
- Logging, metric, trace, and instrumentation callbacks are isolated from the
  business handler.
- Metrics and spans do not receive message bodies or idempotency keys.
- The local dashboard binds only to loopback, hides message bodies, validates
  origin and host, and requires a per-process write token for replay.
- GitHub Actions uses pinned action commits, minimal permissions, npm trusted
  publishing, and no stored npm publishing token.
- Historical disposable-AWS tests are recorded for successful processing,
  duplicate suppression, retry, DLQ redrive, and W3C trace propagation.
- Documentation correctly says that SQS is at-least-once and that application
  side effects must still be safe to retry.

## Resolved during this baseline

### Development dependency advisory

Before the lockfile repair, this development-only path was present:

```text
tsup 8.5.1 -> postcss 8.5.26 -> source-map-js 1.2.1
```

`source-map-js 1.2.1` had a high-severity denial-of-service advisory. It was not
shipped as a QueueCraft runtime dependency. The lockfile now resolves version
`1.2.2`. A clean install, both dependency audits, all 99 tests, type checking,
the build, and the package dry run pass after the update.

## Additional issues resolved during this baseline

### Offline real-AWS workflow

Before this audit, the repository automatically triggered
`.github/workflows/aws-integration.yml` after relevant pushes. The AWS shutdown
record says the
`queuecraft-github-trust` stack was deliberately deleted on 3 October 2026.
Therefore, the workflow cannot obtain its configured AWS role now.

The approved cost shutdown makes the intended state clear. The workflow is now
manual-only and requires a maintainer to confirm that the isolated role was
restored and temporary test-resource charges are accepted. The README and AWS
documentation label the existing real-AWS results as historical evidence. No
AWS resource was restored merely to make a badge green.

### Restore record and template mismatch

The restore record says to recreate `queuecraft-production` with environment
`production`. The current QueueCraft template accepts only `dev` or `test` and
explicitly says it is not a production baseline. Following the record exactly
would fail.

This was a documentation mismatch, not a reason to weaken the template. The
restore record now states that YallaQueue needs a separately reviewed
production deployment definition before it can return.

## Evidence gaps

These are not reasons to add more features.

| Evidence area | Current state | Small next improvement |
| --- | --- | --- |
| Requirements | Frozen functional, non-functional, evidence, and non-goal baseline added | Keep it synchronized with intentional maintenance changes |
| Threat review | Threat model added with trust boundaries, controls, and residual risks | Review it whenever a trust boundary changes |
| Test measurement | 118 tests pass; measured coverage is 86.68% statements, 77.60% branches, 89.08% functions, and 88.54% lines; CI floors are 85%, 75%, 85%, and 85% | Raise a floor only when meaningful tests justify it |
| Static consistency | Type checking exists | Decide whether one lint/format check adds useful evidence |
| Secret prevention | Current tree check is clean | Add automated current-tree and history-aware secret scanning |
| Agent evaluation | Controlled plain and structured runs completed from the same commit | Repeat only when a larger real maintenance task justifies the extra cost |
| Remote repository controls | Recommended in docs | Verify branch protection, tag protection, and npm trusted-publisher settings separately |

## Completed agent-workflow comparison

CloudWatch option validation was used as the fixed comparison task.

Before the comparison, QueueCraft rejected empty dimensions and too many
dimensions but did not enforce all CloudWatch API limits before buffering a
metric. The selected patch now validates documented character and length limits
locally, before any AWS request.

Official API references:

- <https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_Dimension.html>
- <https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html>

This is a suitable comparison issue because it is real, small, testable, and
does not change QueueCraft's product scope.

Acceptance criteria met:

- invalid namespaces fail during construction with a clear error;
- invalid dimension names and values fail before any AWS request;
- valid boundary values continue to work;
- focused tests are added first;
- all existing tests, type checking, build, package inspection, and both
  dependency audits pass afterward.

The plain run scored 98/100 and the structured run scored 94/100. The plain run
was selected because it produced more complete boundary tests with less time
and context. The structured run's explicit self-review was still useful, so
that habit was retained without installing a workflow package globally. See the
[full comparison result](agent-evaluation-results.md).

## Scope guard

Do not add another application, queue feature, dashboard redesign, AI feature,
or live AWS environment as part of this capstone phase. QueueCraft already has
enough functionality. The value now comes from proving that its changes are
specified, tested, reviewed, secure, observable, and explainable.
