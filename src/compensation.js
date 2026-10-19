
const { kafka } = require("./kafka");
const pool = require("./db");

const consumer = kafka.consumer({
  groupId: "flowmaster-compensation-group",
});

async function startCompensationService() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "credit.compensation",
    fromBeginning: true,
  });

  console.log("Compensation service listening...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString());

      if (event.eventType !== "CREDIT_COMPENSATION_REQUESTED") {
        return;
      }

      const client = await pool.connect();

      try {
        await client.query("BEGIN");

        await client.query(
          `INSERT INTO workflow_events
           (workflow_id, event_type, step_name, payload)
           VALUES ($1, $2, $3, $4)`,
          [
            event.workflowId,
            "CREDIT_COMPENSATION_COMPLETED",
            "credit",
            JSON.stringify({
              message: "Credit compensation handler executed",
              reason: event.reason || null,
            }),
          ]
        );

        await client.query("COMMIT");

        console.log(
          "Compensation recorded:",
          event.workflowId
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

startCompensationService().catch((error) => {
  console.error("Compensation service failed:", error);
  process.exit(1);
});
