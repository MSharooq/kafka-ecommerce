import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "email-service" });
const producer = kafka.producer();

const run = async () => {
  try {
    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({
      topic: "order-successful",
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        const { userId, orderId } = JSON.parse(value);

        //we can send email to the user
        const dummyEmail = 'email';
        console.log(`Email has been sent to the user ${dummyEmail}`);

        await producer.send({
          topic: "email-successful",
          messages: [
            {
              value: JSON.stringify({ userId, emailId: dummyEmail }),
            },
          ],
        });
        console.log(`Email has been sent to the user ${dummyEmail}`);
        
      },
    });
  } catch (err) {
    console.log("Error connecting to payment topic " + err);
  }
};

run();
