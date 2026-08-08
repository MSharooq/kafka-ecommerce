import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({ groupId: "analytic-service" });

const run = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topics: ["payment-successful", "order-successful", "email-successful"],
      fromBeginning: true,
    });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        switch (topic) {
          case "payment-successful":
            {
              const value = message.value.toString();
              const { userId, cart } = JSON.parse(value);

              const total = cart.reduce((acc, item) => acc + item.price, 0);

              console.log(
                `Logged payment-service: userId: ${userId} Amount: ${total}`,
              );
            }
            break;

          case "order-successful":
            {
              const value = message.value.toString();
              const { userId, orderId } = JSON.parse(value);

              console.log(
                `Logged orderService: userId: ${userId} orderId: ${orderId}`,
              );
            }
            break;

          case "email-successful":
            {
              const value = message.value.toString();
              const { userId, emailId } = JSON.parse(value);

              console.log(
                `Logged emailService: userId: ${userId} emailid: ${emailId}`,
              );
            }
            break;

          default:
            {
              console.log(`Default Case`);
            }
            break;
        }
      },
    });
  } catch (err) {
    console.log("Error connecting to payment topic " + err);
  }
};

run();
