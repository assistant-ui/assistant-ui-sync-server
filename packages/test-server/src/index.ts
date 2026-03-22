import express from "express";

const app = express();
app.use(express.json());

// Fake backend that streams numbered SSE chunks with delays
app.post("/api/chat", async (_req, res) => {
  res.setHeader("content-type", "text/event-stream");
  res.setHeader("cache-control", "no-cache");

  const totalChunks = 10;
  const delayMs = 300;

  for (let i = 1; i <= totalChunks; i++) {
    const data = JSON.stringify({ chunk: i, total: totalChunks, text: `Hello from chunk ${i}` });
    res.write(`data: ${data}\n\n`);
    if (i < totalChunks) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }

  res.write("data: [DONE]\n\n");
  res.end();
});

app.get("/api/health", (_req, res) => {
  res.json({ ok: true });
});

const port = parseInt(process.env.PORT || "9999");
app.listen(port, () => {
  console.log(`Test backend listening on port ${port}`);
});
