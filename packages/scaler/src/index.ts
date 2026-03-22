import express from "express";
import cors from "cors";
import { Redis } from "ioredis";
import { discoverServers } from "./discovery.js";
import { createHealthPoller, type ServerState } from "./health.js";

const app = express();
app.use(cors());
app.use(express.json({ limit: "50mb" }));

// --- Configuration ---

const RAM_EXHAUSTION_THRESHOLD = parseFloat(process.env.RAM_THRESHOLD || "0.5");
const HEALTH_POLL_INTERVAL = parseInt(process.env.HEALTH_POLL_MS || "5000");
const DISCOVERY_INTERVAL = parseInt(process.env.DISCOVERY_POLL_MS || "10000");
const THREAD_TTL = parseInt(process.env.THREAD_TTL_SECONDS || "120"); // 2 min default
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

// --- Redis ---

const redis = new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 3 });

redis.on("error", (err: Error) => {
  console.error("Redis error:", err.message);
});

async function connectRedis() {
  try {
    await redis.connect();
    console.log("Connected to Redis");
  } catch (err) {
    console.error("Failed to connect to Redis:", (err as Error).message);
    console.error("Thread pinning will not work — exiting");
    process.exit(1);
  }
}

function threadKey(threadId: string) {
  return `thread:${threadId}`;
}

async function pinThread(threadId: string, serverUrl: string) {
  await redis.set(threadKey(threadId), serverUrl, "EX", THREAD_TTL);
}

async function refreshPin(threadId: string) {
  await redis.expire(threadKey(threadId), THREAD_TTL);
}

async function getPin(threadId: string): Promise<string | null> {
  return redis.get(threadKey(threadId));
}

async function removePin(threadId: string) {
  await redis.del(threadKey(threadId));
}

// --- Server discovery + health ---

const servers = new Map<string, ServerState>();

async function refreshServers() {
  const urls = await discoverServers();
  // Add new servers
  for (const url of urls) {
    if (!servers.has(url)) {
      servers.set(url, {
        url,
        exhausted: false,
        activeThreads: 0,
        runningThreads: 0,
        memoryUsage: 0,
        healthy: false,
      });
    }
  }
  // Remove servers that are no longer discovered
  for (const url of servers.keys()) {
    if (!urls.includes(url)) {
      servers.delete(url);
    }
  }
}

const healthPoller = createHealthPoller(servers, RAM_EXHAUSTION_THRESHOLD);

// --- Server selection ---

function pickServer(): ServerState | null {
  const candidates = [...servers.values()]
    .filter((s) => s.healthy && !s.exhausted)
    .sort((a, b) => a.activeThreads - b.activeThreads);

  if (candidates.length > 0) return candidates[0]!;

  // Fallback: any healthy server (all exhausted)
  const fallback = [...servers.values()]
    .filter((s) => s.healthy)
    .sort((a, b) => a.memoryUsage - b.memoryUsage);

  return fallback[0] ?? null;
}

// --- Streaming proxy helper ---

async function proxyStream(
  targetUrl: string,
  body: unknown,
  reqHeaders: Record<string, string | string[] | undefined>,
  res: express.Response,
  threadId?: string,
) {
  const forwardHeaders: Record<string, string> = {};
  for (const [key, value] of Object.entries(reqHeaders)) {
    if (
      ["content-length", "accept-encoding", "host", "connection"].includes(
        key.toLowerCase(),
      )
    )
      continue;
    if (typeof value === "string") forwardHeaders[key] = value;
  }

  const upstream = await fetch(targetUrl, {
    method: "POST",
    headers: { ...forwardHeaders, "content-type": "application/json" },
    body: JSON.stringify(body),
  });

  // Helper to forward upstream headers to the client response (skip transport-level ones)
  const forwardUpstreamHeaders = () => {
    upstream.headers.forEach((value, key) => {
      if (["content-encoding", "content-length", "transfer-encoding"].includes(key.toLowerCase()))
        return;
      res.setHeader(key, value);
    });
  };

  if (!upstream.ok || !upstream.body) {
    // Non-streaming response (e.g. 204 not_found, 4xx, 5xx) — forward headers so
    // callers receive x-stream-status and other diagnostic headers.
    forwardUpstreamHeaders();
    const text = await upstream.text();
    res.status(upstream.status).send(text);
    return;
  }

  forwardUpstreamHeaders();

  // Pipe the stream, refreshing the Redis TTL periodically.
  // When the end-client disconnects, cancel the upstream reader so the scaler
  // stops consuming the sync-server connection (the sync-server switches to
  // buffer-only mode and keeps the stream alive for future resume clients).
  const reader = upstream.body.getReader();
  let clientConnected = true;

  const onClientClose = () => {
    clientConnected = false;
    reader.cancel().catch(() => {});
  };
  res.on("error", onClientClose);
  res.on("close", onClientClose);

  let lastRefresh = Date.now();
  const REFRESH_INTERVAL = (THREAD_TTL * 1000) / 3; // refresh TTL at 1/3 intervals

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (clientConnected) {
        try { res.write(value); } catch { clientConnected = false; }
      }
      // Periodically refresh the pin TTL while streaming
      if (threadId && Date.now() - lastRefresh > REFRESH_INTERVAL) {
        refreshPin(threadId).catch(() => {});
        lastRefresh = Date.now();
      }
    }
    if (clientConnected) res.end();
  } catch {
    // Expected when reader.cancel() fires (client disconnected mid-stream)
    if (clientConnected) {
      try { res.end(); } catch { /* already closed */ }
    }
  }
}

// --- JSON proxy helper ---

async function proxyJson(
  targetUrl: string,
  body: unknown,
  res: express.Response,
) {
  try {
    const upstream = await fetch(targetUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await upstream.text();
    res.status(upstream.status);
    upstream.headers.forEach((value, key) => {
      if (!["content-length", "transfer-encoding"].includes(key.toLowerCase())) {
        res.setHeader(key, value);
      }
    });
    res.send(data);
  } catch {
    res.status(502).json({ error: "Upstream sync server unavailable" });
  }
}

// --- Endpoints ---

app.post("/api/chat", async (req, res) => {
  const { threadId } = req.body;
  if (!threadId) {
    res.status(400).json({ error: "threadId is required" });
    return;
  }

  // Check Redis for existing pin
  let serverUrl = await getPin(threadId);

  if (!serverUrl) {
    const server = pickServer();
    if (!server) {
      res.status(503).json({ error: "No healthy sync servers available" });
      return;
    }
    serverUrl = server.url;
    await pinThread(threadId, serverUrl);
  }

  await proxyStream(`${serverUrl}/api/chat`, req.body, req.headers, res, threadId);
});

app.post("/api/resume", async (req, res) => {
  const { threadId } = req.body;
  const serverUrl = await getPin(threadId);

  if (!serverUrl) {
    res.setHeader("x-stream-status", "not_found");
    res.status(204).end();
    return;
  }

  await proxyStream(`${serverUrl}/api/resume`, req.body, req.headers, res, threadId);
});

app.post("/api/cancel", async (req, res) => {
  const { threadId } = req.body;
  const serverUrl = await getPin(threadId);

  if (!serverUrl) {
    res.json({ success: false, found: false });
    return;
  }

  await proxyJson(`${serverUrl}/api/cancel`, req.body, res);
  await removePin(threadId);
});

app.post("/api/status", async (req, res) => {
  const { threadId } = req.body;
  const serverUrl = await getPin(threadId);

  if (!serverUrl) {
    res.json({ isRunning: false, status: "not_found" });
    return;
  }

  await proxyJson(`${serverUrl}/api/status`, req.body, res);
});

app.get("/api/health", async (_req, res) => {
  const serverList = [...servers.values()].map((s) => ({
    url: s.url,
    healthy: s.healthy,
    exhausted: s.exhausted,
    activeThreads: s.activeThreads,
    runningThreads: s.runningThreads,
    memoryUsage: Math.round(s.memoryUsage * 100) + "%",
  }));

  let redisOk = false;
  try {
    await redis.ping();
    redisOk = true;
  } catch { /* redis down */ }

  res.json({
    redis: redisOk ? "connected" : "disconnected",
    servers: serverList,
  });
});

// --- Startup ---

async function start() {
  await connectRedis();
  await refreshServers();
  await healthPoller.poll();

  // Periodic discovery + health
  setInterval(async () => {
    await refreshServers();
  }, DISCOVERY_INTERVAL);
  setInterval(() => healthPoller.poll(), HEALTH_POLL_INTERVAL);

  const port = parseInt(process.env.PORT || "8788");
  app.listen(port, () => {
    console.log(`Scaler listening on port ${port}`);
    console.log(`Sync servers: ${[...servers.keys()].join(", ")}`);
    console.log(`RAM exhaustion threshold: ${RAM_EXHAUSTION_THRESHOLD * 100}%`);
    console.log(`Thread TTL: ${THREAD_TTL}s`);
  });
}

process.on("SIGTERM", () => {
  redis.disconnect();
  process.exit(0);
});

start().catch((err) => {
  console.error("Failed to start scaler:", err);
  process.exit(1);
});
