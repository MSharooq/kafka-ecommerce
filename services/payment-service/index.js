import express from "express";
import cors from "cors";

const app = express();

app.use(cors());

app.use(express.json());

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;
  const userId = "123";

  //TODO: payemnt

  //add kafka here
  console.log("Payment endpoint hit");
  return res.status(200).send("payment Success");
});

app.use((err, req, res, next) => {
  console.log(err);
  res.status(err.status || 500).send(err.message);
});

app.listen(8000, () => {
  console.log("Payment service lsitening on port 8000");
});
