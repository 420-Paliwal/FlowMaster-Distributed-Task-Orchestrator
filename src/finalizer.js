const { kafka, producer } = require("./kafka");
const pool = require("./db");

const consumer = kafka.consumer({
  groupId: "flowmaster-finalizer-group",
});

async function startFinalizer() {
  await producer.connect();
  console.log("Finalizer producer connected");

  await consumer.connect();
  await consumer.subscribe({
    topic: "credit.results",
    fromBeginning: true,
  });

  console.log("Finalizer listening for credit results...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const result = JSON.parse(message.value.toString());
      const { workflowId, eventType, approved, reason } = result;

      console.log("Received credit result:", result);

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

        if (
          workflow.rows[0].current_step !== "credit" ||
          workflow.rows[0].status !== "RUNNING"
        ) {
          await client.query("ROLLBACK");
          console.log("Workflow already finalized:", workflowId);
          return;
        }

        const success =
          eventType === "CREDIT_APPROVED" && approved === true;

        const finalStatus = success ? "COMPLETED" : "FAILED";

        await client.query(
          `UPDATE workflows
           SET status = $1,
               current_step = NULL,
               updated_at = NOW()
           WHERE id = $2`,
          [finalStatus, workflowId]
        );

        await client.query(
          `INSERT INTO workflow_events
           (workflow_id, event_type, step_name, payload)
           VALUES ($1, $2, $3, $4)`,
          [
            workflowId,
            success ? "WORKFLOW_COMPLETED" : "CREDIT_REJECTED",
            "credit",
            JSON.stringify({
              ...result,
              reason: reason || null,
            }),
          ]
        );

        await client.query("COMMIT");

        if (!success) {
  await producer.send({
    topic: "verification.compensation",
    messages: [
      {
        key: workflowId,
        value: JSON.stringify({
          eventType: "VERIFICATION_COMPENSATION_REQUESTED",
          workflowId,
          reason: reason || "Credit check rejected",
        }),
      },
    ],
  });

  console.log("Verification compensation requested:", workflowId);
}

        console.log(
          `Workflow ${workflowId} final status: ${finalStatus}`
        );
      } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    },
  });
}

startFinalizer().catch((error) => {
  console.error("Finalizer failed:", error);
  process.exit(1);
});
