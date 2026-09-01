# Local dashboard

QueueCraft includes a small local operations page for:

- ready, in-flight, and delayed message counts;
- dead-letter queue count and message metadata;
- privacy-redacted JSON previews;
- confirmed replay from the DLQ to the main queue.

## Try it without AWS

```powershell
npm run dashboard:demo
```

Open the printed local address. The demo uses fake queue data and no cloud
credentials, so replaying its sample jobs is safe.

## Start it from this repository

```powershell
$env:AWS_PROFILE = "your-profile"
$env:AWS_REGION = "eu-central-1"
$env:SQS_QUEUE_URL = "your main queue URL"
$env:SQS_DLQ_URL = "your dead-letter queue URL"
$env:QUEUECRAFT_DASHBOARD_TITLE = "QueueCraft development"
npm run build
node dist/dashboard-cli.js
```

Open the printed address, normally `http://127.0.0.1:4173`. Press
`Ctrl+C` to stop it.

After the npm release, the last two commands become:

```powershell
npm install --global @yusufkaranib/queuecraft
queuecraft-dashboard
```

## Safety boundaries

- The server accepts only loopback hosts. It cannot be bound to `0.0.0.0`.
- AWS credentials remain in the Node.js process and are never sent to the page.
- The page never receives SQS receipt handles or full unredacted message bodies.
- JSON fields likely to contain phone numbers, email, message text, tokens, or
  secrets are replaced with `[redacted]`.
- Non-JSON message bodies are hidden.
- Replaying requires an explicit browser confirmation and a server-side cache
  entry created by refreshing the DLQ.
- FIFO replay is not supported yet because replay requires an application
  decision about message group ordering.

## Replay behavior

Replay copies the original body and message attributes to the main queue, then
deletes the DLQ message. If sending succeeds but deletion fails, QueueCraft
remembers that send for the short cache lifetime and retries only the deletion.
An existing `traceparent` and `tracestate` are therefore preserved; replay
currently continues the original producer trace rather than creating a new
replay span.

An application should keep the original stable idempotency attribute. That
lets the worker suppress a replay when the logical job already completed.

Use an AWS identity with only these permissions:

- `sqs:GetQueueAttributes` on the main queue and DLQ;
- `sqs:ReceiveMessage` and `sqs:DeleteMessage` on the DLQ;
- `sqs:SendMessage` on the main queue.

Do not run the dashboard with AWS root credentials.
