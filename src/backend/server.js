const express = require("express");
const cors = require("cors");
const app = express();
const port = 5000;

app.use(cors());
app.use(express.json()); // To parse JSON

// Sample endpoint
app.get("/", (req, res) => {
  res.send("ChatDB Backend is Running!");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
