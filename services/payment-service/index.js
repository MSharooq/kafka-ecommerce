import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

const app = express();

app.use(cors());

app.use(express.json());

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();
 
const connectToKafka = async () => {
  try {
    await producer.connect();
    console.log("Payment producer connected");
  } catch (err) {
    console.log("Error connecting to Kafka " + err);
  }
};

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;
  const userId = "123";

  //TODO: payemnt

  //add kafka here
  await producer.send({
    topic: "payment-successful",
    messages: [{value: JSON.stringify({userId, cart})}],
  });

  console.log(`Payment endpoint hit ${userId}`);
  return res.status(200).send("payment Success");
});

app.use((err, req, res, next) => {
  console.log(err);
  res.status(err.status || 500).send(err.message);
});

app.listen(8000, () => {
  connectToKafka();
  console.log("Payment service lsitening on port 8000");
});
