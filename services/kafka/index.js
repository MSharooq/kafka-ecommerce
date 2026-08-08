import { Kafka } from "kafkajs";

const kafkaClient = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const admin = kafkaClient.admin();

const run = async () => {
  await admin.connect();
  await admin.createTopics({
    topics: [
      {
        topic: "payment-successful",
      },
      {
        topic: "order-successful",
      },
    ],
  });
  await admin.disconnect();
};

run();
