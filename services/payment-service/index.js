import express from "express";

const app = express();

app.use(cors({
    origin:"https://localhost:3000",
}))

app.use(express.json)

app.use((err, req, res, next) => {
    res.status(err.status || 500).send(err.message)
})

app.listen(8000,  () => {
    console.log("Payment service lsitening on port 8000")
})