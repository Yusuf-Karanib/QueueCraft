# Order-processing reference example

This is QueueCraft's second example application. YallaQueue uses the engine for
WhatsApp bookings; this example uses the same engine for shop orders.

The application supplies the order rules. QueueCraft supplies SQS publishing,
bounded workers, DynamoDB duplicate suppression, retries, DLQ redrive, and
optional trace continuation.

## What the verified demo proves

The real-AWS demo:

1. publishes a fake order with a stable source event ID;
2. processes that order once;
3. publishes the same event again and suppresses the duplicate;
4. publishes a poison order that fails and is delivered again by SQS;
5. confirms SQS moved the poison order to the DLQ; and
6. confirms W3C trace attributes survived processing and redrive.

The fake payload contains only order IDs, product SKUs, quantities, prices, and
currency. Do not put real customer information in the demonstration.

## Run the safe unit tests

From the repository root:

```bash
npm test
```

## Run against real AWS

The recommended method is the repository's **AWS integration** GitHub Action.
It creates one isolated SQS queue, one DLQ, and one on-demand DynamoDB table,
runs this example, and deletes the complete stack even if the example fails.
It does not enable the optional CloudWatch dashboard or publish custom metrics.

The workflow runs automatically after relevant changes reach `main`. It can
also be started manually from **GitHub → Actions → AWS integration → Run
workflow**.

For a manual run, first follow the dedicated-stack instructions in
[`../../docs/aws-integration-test.md`](../../docs/aws-integration-test.md), then
run:

```bash
npm run demo:orders:aws
```

Never point the demo at YallaQueue or another live queue. It consumes messages
and deliberately publishes a failing job.

## Files

- `order-processing.mjs` contains strict order validation plus public
  QueueCraft publisher and poller factories.
- `order-processing.test.mjs` verifies validation, stable idempotency, and
  business-handler behavior without using AWS.
- `aws-demo.mjs` runs the guarded real-AWS demonstration.
