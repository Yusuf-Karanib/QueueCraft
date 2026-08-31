#!/usr/bin/env node
import {
  createQueueCraftDashboard
} from "./chunk-AGN4MPTV.js";

// src/dashboard-cli.ts
import { SQSClient } from "@aws-sdk/client-sqs";
function required(name) {
  const value = process.env[name];
  if (!value) throw new Error("Missing required environment variable: " + name);
  return value;
}
var region = required("AWS_REGION");
var sqs = new SQSClient({ region });
var dashboard = await createQueueCraftDashboard({
  sqsClient: sqs,
  queueUrl: required("SQS_QUEUE_URL"),
  dlqUrl: required("SQS_DLQ_URL"),
  title: process.env.QUEUECRAFT_DASHBOARD_TITLE,
  port: process.env.PORT ? Number(process.env.PORT) : void 0,
  onError(error) {
    console.error(
      "QueueCraft dashboard error:",
      error instanceof Error ? error.message : "Unknown error"
    );
  }
});
console.log("QueueCraft dashboard: " + dashboard.url);
var stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  await dashboard.close();
  sqs.destroy();
}
process.once("SIGINT", () => void stop());
process.once("SIGTERM", () => void stop());
