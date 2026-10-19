
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "flowmaster",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
  retry: {
    initialRetryTime: 300,
    retries: 5,
  },
});

const producer = kafka.producer();

async function connectProducer() {
  await producer.connect();
  console.log("Kafka producer connected");
}

module.exports = {
  kafka,
  producer,
  connectProducer,
};
