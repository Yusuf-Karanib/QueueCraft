import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const template = readFileSync(
  new URL("../infrastructure/cloudformation.yaml", import.meta.url),
  "utf8",
);

function topLevelBlock(name: string): string {
  const marker = `  ${name}:`;
  const start = template.indexOf(marker);
  if (start < 0) throw new Error(`Missing CloudFormation block: ${name}`);

  const remaining = template.slice(start + marker.length);
  const next = remaining.search(/\r?\n  [A-Za-z][A-Za-z0-9]*:/);
  return next < 0 ? template.slice(start) : template.slice(start, start + marker.length + next);
}

interface DashboardDefinition {
  widgets: Array<{
    type: string;
    properties: {
      region?: string;
      title?: string;
      metrics?: unknown[];
    };
  }>;
}

function renderedDashboard(): DashboardDefinition {
  const marker = "      DashboardBody: !Sub |";
  const markerStart = template.indexOf(marker);
  if (markerStart < 0) throw new Error("Missing DashboardBody.");

  const lines = template
    .slice(markerStart + marker.length)
    .replace(/^\r?\n/, "")
    .split(/\r?\n/);
  const body: string[] = [];
  for (const line of lines) {
    if (line.startsWith("        ")) {
      body.push(line.slice(8));
      continue;
    }
    if (!line.trim()) {
      body.push("");
      continue;
    }
    break;
  }

  const json = body
    .join("\n")
    .replaceAll("${AWS::Region}", "eu-central-1")
    .replaceAll("${ProjectName}", "queuecraft")
    .replaceAll("${Environment}", "dev")
    .replaceAll("${JobQueue.QueueName}", "queuecraft-dev-jobs")
    .replaceAll("${DeadLetterQueue.QueueName}", "queuecraft-dev-jobs-dlq")
    .replaceAll("${JobQueueAgeAlarmThresholdSeconds}", "300");

  if (json.includes("${")) {
    throw new Error("DashboardBody contains an unhandled substitution.");
  }

  return JSON.parse(json) as DashboardDefinition;
}

describe("CloudFormation operations controls", () => {
  it("keeps paid or noisy operations resources opt-in", () => {
    expect(topLevelBlock("EnableOperationsDashboard")).toContain(
      'Default: "false"',
    );
    expect(topLevelBlock("EnableJobQueueDepthAlarm")).toContain(
      'Default: "false"',
    );

    expect(topLevelBlock("OperationsDashboard")).toContain(
      "Condition: OperationsDashboardEnabled",
    );
    expect(topLevelBlock("JobQueueDepthAlarm")).toContain(
      "Condition: JobQueueDepthAlarmEnabled",
    );
    expect(topLevelBlock("OperationsDashboardName")).toContain(
      "Condition: OperationsDashboardEnabled",
    );
    expect(topLevelBlock("JobQueueDepthAlarmName")).toContain(
      "Condition: JobQueueDepthAlarmEnabled",
    );
  });

  it("uses configurable, sustained queue-health alarms", () => {
    const ageAlarm = topLevelBlock("JobQueueAgeAlarm");
    expect(ageAlarm).toContain(
      "Threshold: !Ref JobQueueAgeAlarmThresholdSeconds",
    );
    expect(ageAlarm).toContain(
      "EvaluationPeriods: !Ref JobQueueAgeAlarmEvaluationPeriods",
    );
    expect(ageAlarm).toContain(
      "DatapointsToAlarm: !Ref JobQueueAgeAlarmEvaluationPeriods",
    );
    expect(ageAlarm).toContain("oldest unprocessed QueueCraft message");
    expect(ageAlarm).not.toContain("unfinished");

    const depthAlarm = topLevelBlock("JobQueueDepthAlarm");
    expect(depthAlarm).toContain("EvaluationPeriods: 3");
    expect(depthAlarm).toContain("DatapointsToAlarm: 3");
    expect(depthAlarm).toContain("TreatMissingData: notBreaching");

    expect(topLevelBlock("AlarmTopic")).not.toContain("alias/aws/sns");
  });

  it("contains valid private dashboard JSON with bounded data", () => {
    expect(topLevelBlock("OperationsDashboard")).toContain(
      '${ProjectName}-${Environment}-${AWS::Region}-operations',
    );

    const dashboard = renderedDashboard();
    expect(dashboard.widgets).toHaveLength(4);
    for (const widget of dashboard.widgets) {
      expect(widget.type).toBe("metric");
      expect(widget.properties.region).toBe("eu-central-1");
    }

    const rendered = JSON.stringify(dashboard);
    expect(rendered).toContain("queuecraft-dev-jobs");
    expect(rendered).toContain("queuecraft-dev-jobs-dlq");
    expect(rendered).toContain("SEARCH");
    expect(rendered).toContain("JobsFailed");
    expect(rendered).toContain("JobDuration");
    expect(rendered).not.toContain("JobQueueDepthAlarm");
    expect(rendered.toLowerCase()).not.toContain("idempotency");
    expect(rendered.toLowerCase()).not.toContain("phone");
    expect(rendered.toLowerCase()).not.toContain("messagebody");
  });
});
