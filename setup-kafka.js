
const { kafka } = require("./src/kafka");

async function setupKafka() {
  const admin = kafka.admin();

  const topics = [
    "workflow.commands",
    "verification.results",
    "credit.commands",
    "credit.results",
    "verification.compensation",
  ];

  try {
    await admin.connect();

    await admin.createTopics({
      waitForLeaders: true,
      topics: topics.map((topic) => ({
        topic,
        numPartitions: 3,
        replicationFactor: 1,
      })),
    });

    console.log("Kafka topics created successfully!");

    const existingTopics = await admin.listTopics();
    console.log(existingTopics);
  } catch (error) {
    console.error("Kafka topic setup failed:", error.message);
    process.exitCode = 1;
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

setupKafka();
