# QueueCraft threat model

Status: maintenance baseline  
Date: 11 October 2026

## Scope

This model covers the published QueueCraft package, its local dashboard,
GitHub release and test workflows, and the optional AWS SQS, DynamoDB, IAM, and
CloudWatch resources supplied by the repository.

YallaQueue business data, Meta, Supabase, WhatsApp, and SES need their own
application threat model. QueueCraft cannot enforce their authorization or
data-retention rules.

## Assets to protect

- AWS and npm identities;
- message bodies and customer data inside application jobs;
- SQS receipt handles and queue URLs;
- idempotency keys that may correlate business events;
- DynamoDB execution state;
- package and release integrity;
- availability of workers and queues; and
- the owner's AWS budget.

## Trust boundaries

```text
Application -> publisher -> SQS -> poller or Lambda processor -> handler
                              |                 |
                              |                 +-> DynamoDB leases
                              +-> DLQ

Developer -> GitHub Actions -> npm
Developer -> optional GitHub OIDC role -> temporary AWS test stack
Local browser -> loopback dashboard server -> SQS and DLQ
```

Data becomes untrusted when it enters from an application payload, SQS message,
trace carrier, browser request, package dependency, or CI input.

## Threats, controls, and remaining risk

| Threat | Existing control | Remaining risk |
| --- | --- | --- |
| The same logical job arrives more than once | Stable keys plus DynamoDB leases and completed records | An external action can succeed just before a crash; handlers must still be retry-safe |
| Two workers race for one job | Conditional DynamoDB writes and owner tokens | A handler that ignores cancellation can continue after lease loss |
| A poison message retries forever | SQS redrive policy and DLQ | Operators still need an incident and replay decision |
| One failed Lambda record hides successful records | Per-record batch failure response | Deployment must enable `ReportBatchItemFailures` |
| Settlement fails after the handler succeeds | Completion-before-delete ordering and settlement events | A failed completion write can lead to later repeated side effects |
| A malicious or malformed payload reaches a handler | QueueCraft gives parsing to the application | QueueCraft does not validate application schemas |
| Trace metadata is forged or used as authentication | Strict W3C syntax checks; docs say trace context is not authentication | A permitted queue writer can still choose valid fake trace IDs |
| Customer data leaks through telemetry | Built-in metrics and spans omit bodies and stable keys | Application callbacks can still log the raw message they receive |
| A website triggers local DLQ replay | Loopback binding, host/origin checks, JSON-only writes, random write token, confirmation | Malware running as the same user may still access the local operator session |
| One replay request publishes twice | In-process replay coalescing and cached publish state | A process crash after send but before local state is retained can still cause a repeat |
| A repository or workflow steals cloud credentials | Minimal workflow permissions, pinned actions, short-lived OIDC | Repository administrators and compromised dependencies remain trusted positions |
| A leaked npm token publishes malware | npm trusted publishing without a stored token | A compromised trusted workflow or maintainer can still publish |
| Secrets are committed | `.env*` ignored, security policy, and pinned Gitleaks CI with full Git history | Pattern-based detection can miss an unknown credential format; any real finding still requires rotation |
| Metrics create unbounded cost or expose identifiers | Bounded built-in dimensions and privacy guidance | Caller-supplied static dimensions can still be unsafe or high-cardinality |
| A test recreates cloud costs unexpectedly | Real-AWS workflow is manual with explicit confirmation | A confirmed run creates temporary billable resources until cleanup finishes |
| A development template is treated as production | Template accepts only `dev` and `test`; docs state the boundary | A separate production design and review do not yet exist |

## Highest residual risks

1. QueueCraft cannot guarantee exactly-once behavior across external systems.
2. Applications can mishandle raw payloads or log sensitive data.
3. A handler may ignore its cancellation signal after ownership is lost.
4. The package has not been proven under commercial production traffic.
5. Repository history and remote protection settings have not yet been
   independently verified in this baseline.

These risks are reasons for the `alpha` label. They should not be hidden by
adding more features.

## Security review checklist for a change

- Which trust boundary changes?
- Can the change expose a body, key, receipt handle, URL, credential, or trace
  carrier?
- Can it run a handler or external side effect more than once?
- What happens if AWS succeeds and the local process fails immediately after?
- Can an observer, tracer, browser, or dependency failure change job behavior?
- Does it widen IAM permissions or create a new billed resource?
- Is the failure tested, documented, and observable without customer data?

## Response basics

If a credential or private payload appears in Git, logs, screenshots, or chat:

1. stop the affected workflow or publisher;
2. revoke or rotate the credential at the service that owns it;
3. preserve only non-sensitive evidence of what happened;
4. review package, queue, and audit activity;
5. remove the exposure from current documentation and address repository
   history separately; and
6. add a prevention check before resuming work.

Do not paste the exposed value into an issue or incident document.
