# QueueCraft agent-workflow evaluation

Status: frozen protocol for the comparison  
Date: 11 October 2026

## Question

Does a structured planning, TDD, code-review, security-review, and verification
workflow produce a better small QueueCraft change than plain Codex?

This comparison is about correctness and discipline, not which run writes more
code or uses more tools.

## Method

This protocol follows OpenAI's basic evaluation loop: fixed prompt, captured
run and artifacts, deterministic checks, then a score. Success is defined
before either run, and both runs use the same starting state. See
[Testing Agent Skills Systematically with Evals](https://developers.openai.com/blog/eval-skills).

## Fixed task

Improve CloudWatch option validation without changing QueueCraft's public
scope.

QueueCraft currently checks that its CloudWatch namespace and dimensions are
not empty, but it does not enforce every AWS character and length restriction
before buffering a metric.

Acceptance criteria:

1. Reject an invalid namespace during construction.
2. Reject invalid dimension names and values before an AWS request.
3. Enforce the documented AWS ASCII, control-character, leading-colon, and
   length limits.
4. Keep valid boundary values working.
5. Add focused tests before or with the implementation.
6. Preserve the public API and avoid unrelated changes.
7. Pass tests, type checking, build, package inspection, and both dependency
   audits.

Official rules for the implementation:

- <https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_Dimension.html>
- <https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricData.html>

## Files supplied to each AI run

Give each AI the same clean QueueCraft repository state. The minimum files it
must inspect are:

- `src/cloudwatch-metrics.ts`
- `src/cloudwatch-metrics.test.ts`
- `package.json`
- `docs/capstone-requirements.md`
- `docs/threat-model.md`
- this file

Do not give the second run the first run's patch, review, or test output.

## Exact task prompt

```text
Fix QueueCraft's CloudWatch namespace and dimension validation so invalid input
is rejected locally according to the linked official AWS limits. Add focused
tests, preserve the public API, make no unrelated changes, and run every check
listed in docs/agent-evaluation.md. Explain the failure cases in beginner terms.
```

## Two controlled runs

### Run A: plain Codex

- Use the clean starting commit.
- Give only the fixed prompt and listed files.
- Use normal Codex behavior without installing a workflow package.

### Run B: structured workflow

- Use a separate clean worktree at the exact same starting commit.
- Use the same prompt and files.
- Apply the selected planning, TDD, code-review, security-review, and final
  verification steps.
- Do not install anything globally. Inspect the workflow files first and keep
  any trial isolated to the worktree.

## Required verification for both runs

```text
npm ci
npm test
npm run typecheck
npm run build
npm pack --dry-run
npm audit --audit-level=high
npm audit --omit=dev --audit-level=high
git diff --check
```

Record the exit result and short summary. Do not copy credentials, absolute
queue URLs, account IDs, or customer data into the result.

## Scoring rubric

Score each run out of 100.

| Area | Points | What earns the points |
| --- | ---: | --- |
| Correctness | 35 | Matches official limits, handles boundaries, and fails early |
| Tests | 20 | Focused positive and negative cases that would catch a regression |
| Scope discipline | 15 | No unrelated refactor, dependency, API, or formatting churn |
| Security and operations | 10 | No data exposure; errors are safe and actionable |
| Review quality | 10 | Finds its own weaknesses and distinguishes fact from assumption |
| Explanation | 5 | A beginner can explain what changed and why |
| Efficiency | 5 | Reaches the verified result without repeated failed methods or needless work |

Automatic failure conditions:

- required checks do not pass;
- public API compatibility is broken without a requirement;
- tests are weakened or skipped;
- secrets or customer data are exposed; or
- unrelated features are added.

## Evidence to capture

For each run, save:

- exact starting commit;
- exact prompt;
- workflow name and version, if any;
- elapsed time;
- final diff statistics;
- tests added and failures they detect;
- verification results;
- reviewer findings; and
- final score with one sentence of evidence for every category.

Token use may be recorded, but it is not a quality score by itself.

## Result template

| Measure | Plain Codex | Structured workflow |
| --- | --- | --- |
| Starting commit | | |
| Elapsed time | | |
| Files changed | | |
| Tests added | | |
| All checks pass | | |
| Correctness / 35 | | |
| Tests / 20 | | |
| Scope / 15 | | |
| Security / 10 | | |
| Review / 10 | | |
| Explanation / 5 | | |
| Efficiency / 5 | | |
| Total / 100 | | |

This protocol is now frozen. If the task or rubric changes after seeing Run A,
restart both runs from the same clean commit.
