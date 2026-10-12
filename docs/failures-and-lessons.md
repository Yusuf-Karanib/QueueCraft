# Failures and lessons

QueueCraft was not built by following one perfect tutorial from start to finish.
Several assumptions failed during tests, packaging, CI, and real-AWS
verification. This page records the important ones and what I changed because
of them.

These were development failures and pre-production near misses, not customer
outages. QueueCraft is a public alpha and has no claimed commercial production
traffic. I would rather show real evidence than invent a dramatic incident that
did not happen.

## 1. A one-second lease could last about one millisecond

**What went wrong**

The DynamoDB lease deadline originally converted milliseconds to seconds by
rounding down, then added the configured lease duration. If a one-second lease
started at `xx:xx:xx.999`, its stored deadline could arrive about one
millisecond later instead of one second later. Another worker could then believe
the first worker had lost ownership.

**What I changed**

I changed deadline calculation to round the deadline up and added a boundary
test using a timestamp ending in `999` milliseconds. Lease acquisition and
renewal now receive at least the configured duration.

**What I learned**

Distributed-system bugs often live at time-unit boundaries. A formula can look
correct with normal timestamps and still fail at the last millisecond of a
second. Time, leases, retries, and ownership need boundary tests, not only happy
paths.

Evidence: [processing-hardening commit](https://github.com/Yusuf-Karanib/QueueCraft/commit/9bc64e613b4304d1b56b6b8b4942d61cbefca835)

## 2. I confused SQS deliveries with handler executions

**What went wrong**

The real-AWS test originally required a poison job's business handler to run at
least twice before the message reached the dead-letter queue. That assertion
mixed up two different facts: SQS counts message receives for redrive, while the
application separately decides whether a business handler may run under its
lease and idempotency rules.

The important result was that the handler failed, SQS delivered the message
again, and the redrive policy moved it to the DLQ. Counting handler calls was
not reliable proof of the transport behavior.

**What I changed**

I required at least one intentional handler failure, then verified redelivery
and DLQ arrival independently. I also corrected the public wording so it did
not claim more handler executions than the test actually proved.

**What I learned**

Tests should assert the contract owned by each layer. SQS delivery count,
DynamoDB ownership, and business-handler execution are related, but they are
not the same measurement.

Evidence: [redrive-assertion correction](https://github.com/Yusuf-Karanib/QueueCraft/commit/68c7a94685f1a4223c3e278d16d753d46e4f0957)

## 3. The AWS bootstrap role could not inspect itself

**What went wrong**

I used a temporary GitHub OIDC role to create the permanent, restricted AWS test
role. The bootstrap workflow also checked its own IAM capabilities before doing
that work. The first policy could create and manage the target role, but it did
not include every read-only IAM action used by its self-checks.

This produced a useful least-privilege trap: a role may have enough permission
to perform its main action while still lacking permission to prove what it can
do.

**What I changed**

I added only the missing self-inspection actions, completed the capability
checks, created the permanent restricted role, and then deleted the temporary
bootstrap workflow and template.

**What I learned**

Bootstrap permissions and runtime permissions are different security problems.
A bootstrap identity should be short-lived, narrowly scoped, able to validate
its assumptions, and removed when its one job is complete.

Evidence: [self-inspection fix](https://github.com/Yusuf-Karanib/QueueCraft/commit/718ae16f024f3a29a36af27d427e4f3c65e4827e), [completed capability checks](https://github.com/Yusuf-Karanib/QueueCraft/commit/4ccc74c9e5780025157780a39f6bda038ba9fdba), and [bootstrap removal](https://github.com/Yusuf-Karanib/QueueCraft/commit/42c4744b3e46e3aa69b8a6c30656c304b5177ead)

## 4. Passing source tests did not prove the npm package worked

**What went wrong**

QueueCraft's library tests could pass while the installed command-line
dashboard still depended on package metadata and the exact files inside the npm
archive. The dashboard command needed its packaged `bin` path corrected from
`./dist/dashboard-cli.js` to `dist/dashboard-cli.js`.

**What I changed**

I corrected the package metadata and made `npm pack`, trusted publishing, and a
clean anonymous installation part of release verification. The clean install
checks the public API and dashboard command from the same artifact a user gets.

**What I learned**

The repository is not the product; the published archive is. Library tests,
builds, package inspection, and clean installation answer different questions,
so a release needs all four.

Evidence: [package-command fix](https://github.com/Yusuf-Karanib/QueueCraft/commit/4d9df0e11d1b22760adde8ec3122113e16aa25ea) and [release verification](verification.md)

## 5. Making a flaky test wait longer did not fix it

**What went wrong**

A dashboard test used a real 20-millisecond cache expiry and short sleeps to
prove that two concurrent replay requests were coalesced. It usually passed,
but parallel test and coverage runs slowed the machine enough to expose the
timing assumption.

My first adjustment gave the request more time. A later stress run still found
the test could fail earlier in the same sequence. The product behavior was not
the problem; the test depended on operating-system scheduling.

**What I changed**

I stopped increasing sleeps. I separated replay coordination from the HTTP
timing, injected a controlled clock, and tested the concurrent state transition
directly. Repeated parallel runs then passed without depending on machine speed.

**What I learned**

A longer sleep can hide a race without removing it. When a timing fix fails,
change the test design: control time and synchronize on state rather than hoping
the scheduler runs quickly enough.

Evidence: [initial timing adjustment](https://github.com/Yusuf-Karanib/QueueCraft/commit/de15bb5797a73704886aa3baa4ac32b71e63c975) and [deterministic replay test](https://github.com/Yusuf-Karanib/QueueCraft/commit/de57b45f7485ed46b4ddb9afaabe5cb3139b7566)

## The working method I kept

These failures led to a repeatable process:

1. Reproduce the failure and separate application behavior from test or
   environment behavior.
2. Write down the exact assumption that was wrong.
3. Fix the boundary where that assumption entered the system.
4. Add a deterministic regression check.
5. Verify the built and published artifact, not only the source tree.
6. Keep claims smaller than the evidence.

AI helped with implementation and review, but a suggestion was not accepted as
proof. Tests, real-AWS runs, package inspection, clean installs, and public CI
are the evidence used in this repository.
