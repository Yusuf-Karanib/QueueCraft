#!/usr/bin/env node
import { SQSClient } from "@aws-sdk/client-sqs";
import { createQueueCraftDashboard } from "./dashboard";

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error("Missing required environment variable: " + name);
  return value;
}

const region = required("AWS_REGION");
const sqs = new SQSClient({ region });
const dashboard = await createQueueCraftDashboard({
  sqsClient: sqs,
  queueUrl: required("SQS_QUEUE_URL"),
  dlqUrl: required("SQS_DLQ_URL"),
  title: process.env.QUEUECRAFT_DASHBOARD_TITLE,
  port: process.env.PORT ? Number(process.env.PORT) : undefined,
  onError(error) {
    console.error(
      "QueueCraft dashboard error:",
      error instanceof Error ? error.message : "Unknown error",
    );
  },
});

console.log("QueueCraft dashboard: " + dashboard.url);

let stopping = false;
async function stop(): Promise<void> {
  if (stopping) return;
  stopping = true;
  await dashboard.close();
  sqs.destroy();
}

process.once("SIGINT", () => void stop());
process.once("SIGTERM", () => void stop());
