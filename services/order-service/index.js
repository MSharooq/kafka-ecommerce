import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "order-service" });
const producer = kafka.producer();

const run = async () => {
  try {
    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, cart } = JSON.parse(value);

        //we can create order
        const dummyOrderId = 567;
        await producer.send({
          topic: "order-successful",
          messages: [
            {
              value: JSON.stringify({ userId, orderId: dummyOrderId }),
            },
          ],
        });
      },
    });
  } catch (err) {
    console.log("Error connecting to payment topic " + err);
  }
};

run();
