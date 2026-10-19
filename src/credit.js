
const { kafka, producer } = require("./kafka");

const consumer = kafka.consumer({
  groupId: "flowmaster-credit-group",
});

async function startCreditService() {
  await producer.connect();
  console.log("Credit producer connected");

  await consumer.connect();

  await consumer.subscribe({
    topic: "credit.commands",
    fromBeginning: true,
  });

  console.log("Credit service listening...");

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      const event = JSON.parse(message.value.toString());

      if (event.eventType !== "CREDIT_CHECK_REQUESTED") return;

      console.log("Processing credit check:", event.workflowId);

      // Mock credit check for assignment demonstration.
      
      // Mock credit check for assignment demonstration.
      const approved =
        Boolean(event.input?.applicantId) &&
        Number(event.input?.amount) <= 100000;

      let reason = null;

      if (!event.input?.applicantId) {
        reason = "Applicant ID is missing";
      } else if (Number(event.input?.amount) > 100000) {
        reason = "Requested amount exceeds credit limit";
      }

      const result = {
        eventType: approved ? "CREDIT_APPROVED" : "CREDIT_REJECTED",
        workflowId: event.workflowId,
        approved,
        reason,
      };

      await producer.send({
        topic: "credit.results",
        messages: [
          {
            key: event.workflowId,
            value: JSON.stringify(result),
          },
        ],
      });

      console.log("Credit result published:", result);
    },
  });
}

startCreditService().catch((error) => {
  console.error("Credit service failed:", error);
  process.exit(1);
});
