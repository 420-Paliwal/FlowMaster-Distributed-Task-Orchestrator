
const { kafka } = require("./src/kafka");

async function testKafka() {
  const admin = kafka.admin();

  try {
    await admin.connect();

    const topics = await admin.listTopics();

    console.log("Kafka connection successful!");
    console.log("Existing topics:", topics);
  } catch (error) {
    console.error("Kafka connection failed:", error.message);
    process.exitCode = 1;
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

testKafka();
