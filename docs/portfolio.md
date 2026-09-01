# QueueCraft portfolio material

## The simple explanation

QueueCraft is a reusable TypeScript toolkit for AWS SQS. An application gives
QueueCraft a job, QueueCraft puts it on a queue, and a worker processes it in
the background. Slow work no longer keeps the original web request waiting.

It also handles the parts that are easy to get wrong: bounded concurrency,
DynamoDB-backed duplicate suppression, retries, dead-letter queues, graceful
shutdown, CloudWatch monitoring, and W3C trace propagation.

YallaQueue is one application using the engine. The independent order example
shows that the same engine can process another business workflow without
booking-specific code.

## Honest status

QueueCraft is a public alpha for controlled pilots and portfolio demonstration.
It is tested in isolation against real AWS resources, but it is not yet proven
by production traffic. SQS is at-least-once, so QueueCraft reduces duplicate
work rather than promising universal exactly-once side effects.

## One-minute recording script

Start with the repository README already open. Keep credentials, account IDs,
queue URLs, and customer data off screen.

**0–8 seconds — the problem**

“Web requests should answer quickly, but business work can be slow or fail.
QueueCraft moves that work into AWS SQS so it can run safely in the background.”

**8–18 seconds — the reusable engine**

Show the architecture diagram.

“The application publishes a job. QueueCraft processes it with controlled
concurrency, records completion in DynamoDB, retries failures, and lets SQS move
repeated failures to a dead-letter queue.”

**18–32 seconds — more than one application**

Open `examples/order-processing/order-processing.mjs` and briefly show the
order publisher and handler factories.

“YallaQueue uses the engine for bookings. This separate order example uses the
same public package, proving the engine is not tied to one app.”

**32–48 seconds — evidence**

Show a completed AWS integration workflow and its four PASS lines.

“Every relevant change creates disposable AWS resources and verifies a real
successful order, duplicate suppression, retries, DLQ redrive, and trace
context. The stack is then deleted.”

**48–60 seconds — close**

Show the npm page and GitHub repository.

“QueueCraft 0.3.0 is public on npm with 83 automated tests. It is an honest
public alpha, and my next step is validating it in controlled pilots.”

## Resume bullet

Built and published QueueCraft, a TypeScript toolkit for AWS SQS job processing
with bounded concurrency, DynamoDB-backed duplicate suppression, retries and
DLQ handling, CloudWatch observability, and W3C trace propagation; verified by
83 automated tests and isolated real-AWS CI.

## LinkedIn draft

I built QueueCraft to demonstrate more than a basic AWS deployment. It is a
reusable TypeScript toolkit for moving slow or failure-prone work into Amazon
SQS, with DynamoDB-backed duplicate suppression, retries and DLQ handling,
CloudWatch monitoring, and W3C trace propagation.

Version 0.3.0 is public on npm. I verified successful processing, duplicate
suppression, failure handling, DLQ redrive, and trace-context survival against
isolated real AWS resources through GitHub Actions.

YallaQueue uses QueueCraft for a booking workflow. I have now added an
independent order-processing reference to prove that the engine is not tied to
one application.

QueueCraft is still a public alpha, not a production-proven product. The next
step is testing it in controlled pilots and using that evidence to improve the
API and operational defaults.

GitHub: https://github.com/Yusuf-Karanib/QueueCraft

npm: https://www.npmjs.com/package/@yusufkaranib/queuecraft
