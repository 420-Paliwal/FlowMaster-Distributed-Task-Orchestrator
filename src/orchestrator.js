
const fs = require("fs");
const path = require("path");
const YAML = require("yaml");

const workflowDefinition = YAML.parse(
  fs.readFileSync(
    path.join(__dirname, "../config/loan-workflow.yaml"),
    "utf8"
  )
);

const verificationStep = workflowDefinition.steps.find(
  (step) => step.name === "verification"
);

const creditStep = workflowDefinition.steps.find(
  (step) => step.name === "credit"
);

if (!verificationStep || !creditStep) {
  throw new Error("Workflow YAML must define verification and credit steps");
}

const { kafka, producer } = require("./kafka");
const pool = require("./db");

const consumer = kafka.consumer({
  groupId: "flowmaster-orchestrator-group",
});


async function resumeIncompleteWorkflows() {
  const result = await pool.query(
    `SELECT id, workflow_type, status, current_step, input
     FROM workflows
     WHERE (status = 'PENDING' AND current_step = 'verification')
        OR (status = 'RUNNING' AND current_step = 'credit')`
  );

  console.log(
    `Recovery: found ${result.rowCount} incomplete workflow(s)`
  );

  for (const workflow of result.rows) {
    if (
      workflow.status === "PENDING" &&
      workflow.current_step === "verification"
    ) {
      await producer.send({
        topic: verificationStep.command_topic,
        messages: [
          {
            key: workflow.id,
            value: JSON.stringify({
              eventType: "WORKFLOW_STARTED",
              workflowId: workflow.id,
              input: workflow.input,
              recovered: true,
            }),
          },
        ],
      });

      console.log("Resumed verification:", workflow.id);
    } else if (
      workflow.status === "RUNNING" &&
      workflow.current_step === "credit"
    ) {
      await producer.send({
        topic: creditStep.command_topic,
        messages: [
          {
            key: workflow.id,
            value: JSON.stringify({
              eventType: "CREDIT_CHECK_REQUESTED",
              workflowId: workflow.id,
              input: workflow.input,
              recovered: true,
            }),
          },
        ],
      });

      console.log("Resumed credit check:", workflow.id);
    }
  }
}


async function startOrchestrator() {
  await producer.connect();
  console.log("Orchestrator producer connected");

  await consumer.connect();

  
await consumer.subscribe({
  topic: verificationStep.result_topic,
  fromBeginning: true,
});

await resumeIncompleteWorkflows();

console.log("Orchestrator listening for verification results...");


  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const result = JSON.parse(message.value.toString());
      const { workflowId, eventType, verified, reason } = result;

      console.log("Received verification result:", result);

      const client = await pool.connect();

      try {
        await client.query("BEGIN");

        const workflow = await client.query(
          "SELECT * FROM workflows WHERE id = $1 FOR UPDATE",
          [workflowId]
        );

        if (workflow.rowCount === 0) {
          await client.query("ROLLBACK");
          console.error("Workflow not found:", workflowId);
          return;
        }

        if (workflow.rows[0].current_step !== "verification") {
          await client.query("ROLLBACK");
          console.log("Verification result already processed:", workflowId);
          return;
        }

        if (eventType === verificationStep.success_event && verified) {
          await client.query(
            `UPDATE workflows
             SET status = 'RUNNING',
                 current_step = 'credit',
                 updated_at = NOW()
             WHERE id = $1`,
            [workflowId]
          );

          await client.query(
            `INSERT INTO workflow_events
             (workflow_id, event_type, step_name, payload)
             VALUES ($1, $2, $3, $4)`,
            [
              workflowId,
              verificationStep.success_event,
              "verification",
              JSON.stringify(result),
            ]
          );

          await client.query("COMMIT");

          await producer.send({
            topic: creditStep.command_topic,
            messages: [
              {
                key: workflowId,
                value: JSON.stringify({
                  eventType: "CREDIT_CHECK_REQUESTED",
                  workflowId,
                  input: workflow.rows[0].input,
                }),
              },
            ],
          });

          console.log("Credit check command published:", workflowId);
        } else {
          await client.query(
            `UPDATE workflows
             SET status = 'FAILED',
                 updated_at = NOW()
             WHERE id = $1`,
            [workflowId]
          );

          await client.query(
            `INSERT INTO workflow_events
             (workflow_id, event_type, step_name, payload)
             VALUES ($1, $2, $3, $4)`,
            [
  workflowId,
  verificationStep.failure_event,
  "verification",
  JSON.stringify({
    reason: reason || "Verification failed",
  }),
]
          );

          await client.query("COMMIT");

          console.log("Workflow failed verification:", workflowId);
        }
      } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    },
  });
}

startOrchestrator().catch((error) => {
  console.error("Orchestrator failed:", error);
  process.exit(1);
});
