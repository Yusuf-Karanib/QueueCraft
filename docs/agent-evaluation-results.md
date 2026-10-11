# QueueCraft agent-workflow evaluation result

- Date: 11 October 2026
- Starting commit: `8f1e96c`
- Task: CloudWatch namespace and dimension validation

## What was compared

Two isolated copies of QueueCraft started from the same commit and received the
same fixed task, acceptance criteria, verification commands, model, and
reasoning level.

Both used Codex CLI `0.149.1` with `gpt-5.6-sol` at high reasoning. The exact
task prompt remains in the frozen protocol.

- **Plain run:** normal Codex behavior with no workflow package.
- **Structured run:** the same Codex setup plus reviewed instructions derived
  from planning, TDD, code-review, security-review, and verification material in
  [ECC v2.2.3](https://github.com/affaan-m/ECC/tree/c05b2d6614f62f6db0047669aa4eefb223d478f9).

No ECC installer, hook, dependency, MCP server, or global configuration was
used. This was one controlled maintenance task, not a general benchmark of
Codex or ECC.

The method followed the fixed-prompt and deterministic-check pattern described
in OpenAI's
[Testing Agent Skills Systematically with Evals](https://developers.openai.com/blog/eval-skills).
The frozen QueueCraft-specific protocol is in
[agent-evaluation.md](agent-evaluation.md).

## Result

| Measure | Plain Codex | Structured workflow |
| --- | ---: | ---: |
| Elapsed time | 298.0 seconds | 386.6 seconds |
| Content files changed | 3 | 3 |
| Focused RED result | 12 failed, 15 passed | 11 failed, 9 passed |
| Focused test total | 27 | 20 |
| Full test total | 118 | 111 |
| Required checks | All passed | All passed |
| Input tokens | 822,256 total; 760,320 cached | 1,508,747 total; 1,435,264 cached |
| Output tokens | 13,141 | 12,856 |
| Correctness / 35 | 35 | 35 |
| Tests / 20 | 20 | 18 |
| Scope / 15 | 15 | 15 |
| Security / 10 | 10 | 10 |
| Review / 10 | 8 | 10 |
| Explanation / 5 | 5 | 4 |
| Efficiency / 5 | 5 | 2 |
| **Total / 100** | **98** | **94** |

Token use is recorded only as an efficiency signal, not as a quality score.

Both changes were correct, stayed within scope, used static error messages, and
passed the full verification list. The plain run won because it covered more
empty, whitespace, and exact-boundary cases, explicitly proved that invalid
construction makes no AWS call, finished faster, and used less context. The
structured run performed the stronger explicit self-review, but it also made
two avoidable failed read attempts before correcting its method.

## Decision

The plain patch was selected and independently re-run in the maintained
repository. QueueCraft now rejects invalid CloudWatch namespaces and dimensions
before an AWS request while preserving valid minimum and maximum boundaries.

The useful structured habits were retained:

- define success before coding;
- make the relevant tests fail first;
- review the patch against its requirements and security boundaries; and
- run one fixed verification list before calling the work complete.

The heavier workflow was not installed globally. One small task did not show
enough benefit to justify its extra time and context. It remains an option for
larger, ambiguous, or higher-risk work.

## Final verification of the selected patch

- 118 tests passed across 12 files;
- type checking passed;
- package build passed;
- package dry run passed with 9 files;
- full and published-code dependency audits found zero known vulnerabilities;
- `git diff --check` passed; and
- no live AWS resource was started or called.
