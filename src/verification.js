
// const { kafka, producer } = require("./kafka");
// const consumer = kafka.consumer({
//   groupId: "flowmaster-verification-group",
// });

// async function startVerificationService() {

//     await producer.connect();
// console.log("Verification producer connected");

//   await consumer.connect();

//   await consumer.subscribe({
//     topic: "workflow.commands",
//     fromBeginning: true,
//   });

//   console.log("Verification service listening...");

//   await consumer.run({
//     eachMessage: async ({ message }) => {
//       if (!message.value) return;

//       const event = JSON.parse(message.value.toString());

//       if (event.eventType !== "WORKFLOW_STARTED") return;

//       console.log("Verifying workflow:", event.workflowId);

//       // Mock verification for demo purposes
//       const verified = Boolean(event.input?.applicantId);

//       const result = {
//         eventType: verified
//           ? "VERIFICATION_PASSED"
//           : "VERIFICATION_FAILED",
//         workflowId: event.workflowId,
//         verified,
//         reason: verified ? null : "Applicant ID is missing",
//       };

//       await producer.send({
//         topic: "verification.results",
//         messages: [
//           {
//             key: event.workflowId,
//             value: JSON.stringify(result),
//           },
//         ],
//       });

//       console.log("Verification result published:", result);
//     },
//   });
// }

// startVerificationService().catch((error) => {
//   console.error("Verification service failed:", error);
//   process.exit(1);
// });



const { kafka, producer } = require("./kafka");
const pool = require("./db");

const consumer = kafka.consumer({
  groupId: "flowmaster-verification-group",
});

const compensationConsumer = kafka.consumer({
  groupId: "flowmaster-verification-compensation-group",
});

async function startVerificationService() {
  await producer.connect();
  console.log("Verification producer connected");

  await consumer.connect();
  await consumer.subscribe({
    topic: "workflow.commands",
    fromBeginning: true,
  });

  await compensationConsumer.connect();
  await compensationConsumer.subscribe({
    topic: "verification.compensation",
    fromBeginning: true,
  });

  console.log("Verification service listening...");
  console.log("Verification compensation listener ready...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString());
      if (event.eventType !== "WORKFLOW_STARTED") return;

      console.log("Verifying workflow:", event.workflowId);

      const verified = Boolean(event.input?.applicantId);

      const result = {
        eventType: verified
          ? "VERIFICATION_PASSED"
          : "VERIFICATION_FAILED",
        workflowId: event.workflowId,
        verified,
        reason: verified ? null : "Applicant ID is missing",
      };

      await producer.send({
        topic: "verification.results",
        messages: [{
          key: event.workflowId,
          value: JSON.stringify(result),
        }],
      });

      console.log("Verification result published:", result);
    },
  });

  await compensationConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString());

      if (event.eventType !== "VERIFICATION_COMPENSATION_REQUESTED") {
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
            "VERIFICATION_COMPENSATED",
            "verification",
            JSON.stringify({
              message: "Successful verification flagged as compensated",
              reason: event.reason || "Downstream credit step failed",
            }),
          ]
        );

        await client.query("COMMIT");

        console.log(
          "Verification compensation recorded:",
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

startVerificationService().catch((error) => {
  console.error("Verification service failed:", error);
  process.exit(1);
});
