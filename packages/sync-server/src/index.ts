import express from "express";
import type { Request as ExpressRequest, Response as ExpressResponse } from "express";
import { readFileSync } from "fs";
import { ThreadSync } from "./ThreadSync.js";

// --- Memory ratio (cgroup-aware for containers) ---

function getContainerMemoryLimit(): number | null {
  try {
    const val = readFileSync("/sys/fs/cgroup/memory.max", "utf-8").trim();
    if (val !== "max") return parseInt(val);
  } catch {}
  try {
    const val = readFileSync("/sys/fs/cgroup/memory/memory.limit_in_bytes", "utf-8").trim();
    const n = parseInt(val);
    if (n < 9e18) return n;
  } catch {}
  return null;
}

function getMemoryRatio(): number {
  const mem = process.memoryUsage();
  const limit = getContainerMemoryLimit();
  if (limit !== null) return mem.rss / limit;
  return mem.heapUsed / mem.heapTotal;
}

const MEMORY_EVICTION_THRESHOLD = parseFloat(process.env.MEMORY_EVICTION_THRESHOLD || "0.9");
const MEMORY_EVICTION_PROGRESS_CHECK_DELAY_MS = parseInt(
  process.env.MEMORY_EVICTION_PROGRESS_CHECK_DELAY_MS || "10",
);

// --- Benign abort detection ---

function isBenignAbortReason(err: unknown): boolean {
  if (!err) return false;
  if (err instanceof Error && (err.name === "AbortError" || err.constructor.name === "AbortError")) {
    return true;
  }
  const msg = typeof err === "string" ? err : err instanceof Error ? err.message : "";
  return ["Stream cancelled by user", "New request started"].some(
    (r) => msg.toLowerCase().includes(r.toLowerCase()),
  );
}

// --- Thread store ---

const threads = new Map<string, ThreadSync>();
let memoryEvictionCheckQueued = false;

const getExistingThread = (id: string): ThreadSync | null => threads.get(id) ?? null;

const getOrCreateThread = (id: string): ThreadSync => {
  const existing = threads.get(id);
  if (existing) return existing;

  const ts = new ThreadSync(
    () => { threads.delete(id); },
    requestMemoryEvictionCheck,
  );
  threads.set(id, ts);
  return ts;
};

function evictLargestStreamIfNeeded(trigger: string) {
  const memoryRatio = getMemoryRatio();
  if (memoryRatio < MEMORY_EVICTION_THRESHOLD) return false;

  let largest:
    | { threadId: string; thread: ThreadSync; bufferedBytesEstimate: number }
    | null = null;

  for (const [threadId, thread] of threads) {
    const bufferedBytesEstimate = thread.getBufferedBytesEstimate();
    if (
      thread.getIsRunning() &&
      bufferedBytesEstimate > 0 &&
      (!largest || bufferedBytesEstimate > largest.bufferedBytesEstimate)
    ) {
      largest = { threadId, thread, bufferedBytesEstimate };
    }
  }

  if (!largest) {
    return false;
  }

  const evicted = largest.thread.evictBuffer(
    `Memory ratio ${memoryRatio.toFixed(3)} exceeded threshold ${MEMORY_EVICTION_THRESHOLD}`,
  );

  if (evicted) {
    console.warn(
      `Evicted stream buffer for ${largest.threadId} after ${trigger}; ` +
        `memoryRatio=${memoryRatio.toFixed(3)}, estimatedBytes=${largest.bufferedBytesEstimate}`,
    );
  }

  return evicted;
}

function requestMemoryEvictionCheck() {
  if (memoryEvictionCheckQueued) return;
  memoryEvictionCheckQueued = true;

  setTimeout(() => {
    memoryEvictionCheckQueued = false;
    evictLargestStreamIfNeeded("stream-progress");
  }, MEMORY_EVICTION_PROGRESS_CHECK_DELAY_MS).unref();
}

// --- Stream response helper ---
// Pipes a web Response body to an Express response using WritableStream,
// matching the demo-app's streaming pattern.

function pipeResponseToExpress(
  response: Response,
  res: ExpressResponse,
  threadId: string,
) {
  // Forward non-transport headers
  for (const [key, value] of response.headers.entries()) {
    if (!["content-encoding", "content-length", "transfer-encoding"].includes(key)) {
      res.setHeader(key, value);
    }
  }
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache, no-transform");

  if (!response.body) {
    res.status(204).end();
    return;
  }

  response.body
    .pipeTo(
      new WritableStream({
        write(chunk) {
          res.write(chunk);
        },
        close() {
          res.end();
        },
        abort(err) {
          if (!res.headersSent) {
            if (isBenignAbortReason(err)) {
              res.end();
            } else {
              res.status(500).json({ error: "Stream aborted" });
            }
          } else {
            res.end();
          }
        },
      }),
    )
    .catch(() => {
      if (!res.headersSent) {
        res.status(500).json({ error: "Error streaming response" });
      } else {
        res.end();
      }
    });
}

// --- Graceful shutdown ---

let shuttingDown = false;
const SHUTDOWN_TIMEOUT_MS = parseInt(process.env.SHUTDOWN_TIMEOUT_MS || "3600000");

function tryExit() {
  const running = [...threads.values()].filter((t) => t.getIsRunning()).length;
  if (running === 0) {
    console.log("Graceful shutdown complete — no active streams remain");
    process.exit(0);
  }
  console.log(`Draining ${running} active stream(s)...`);
}

process.on("SIGTERM", () => {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log("SIGTERM received — stopping new requests, draining active streams");

  const forceExit = setTimeout(() => {
    console.warn("Shutdown timeout reached — force exiting");
    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);
  forceExit.unref();

  tryExit();
});

// --- App ---

const app = express();
app.use(express.json({ limit: "50mb" }));

// --- Routes ---

app.post("/api/chat", async (req: ExpressRequest, res: ExpressResponse) => {
  if (shuttingDown) {
    res.status(503).json({ error: "Server is shutting down" });
    return;
  }

  const { threadId, backendUrl } = req.body;
  if (!threadId || !backendUrl) {
    res.status(400).json({ error: "threadId and backendUrl are required" });
    return;
  }

  const threadSync = getOrCreateThread(threadId);

  try {
    const response = await threadSync.fetch(req as unknown as Request, req.body);
    pipeResponseToExpress(response, res, threadId);
  } catch {
    if (!res.headersSent) {
      res.status(500).json({ error: "Internal server error" });
    } else {
      res.end();
    }
  }
});

app.post("/api/resume", (req: ExpressRequest, res: ExpressResponse) => {
  const { threadId } = req.body;
  const threadSync = getExistingThread(threadId);

  if (!threadSync) {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("X-Stream-Status", "not_found");
    res.status(200).end();
    return;
  }

  const response = threadSync.resume();

  if (response.status === 204) {
    const streamStatus = response.headers.get("X-Stream-Status");
    const evictedAt = response.headers.get("X-Evicted-At");
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("X-Stream-Status", streamStatus || "completed");
    if (evictedAt) res.setHeader("X-Evicted-At", evictedAt);
    res.status(200).end();
    return;
  }

  pipeResponseToExpress(response, res, threadId);
});

app.post("/api/cancel", async (req: ExpressRequest, res: ExpressResponse) => {
  const { threadId } = req.body;
  const threadSync = getExistingThread(threadId);

  if (!threadSync) {
    res.json({ success: true, found: false });
    return;
  }

  await threadSync.cancel();
  res.json({ success: true, found: true });
});

app.post("/api/status", (req: ExpressRequest, res: ExpressResponse) => {
  const { threadId } = req.body;
  const threadSync = getExistingThread(threadId);

  if (!threadSync) {
    res.json({
      isRunning: false,
      status: "not_found",
      message: "Thread not found - may not have started yet or already completed",
    });
    return;
  }

  const info = threadSync.getCompletionStatus();
  res.json({
    isRunning: info.isRunning,
    status: info.status,
    completedAt: info.completedAt,
    evictedAt: info.evictedAt,
  });
});

app.get("/api/health", (_req: ExpressRequest, res: ExpressResponse) => {
  const mem = process.memoryUsage();
  const runningThreads = [...threads.values()].filter((t) => t.getIsRunning()).length;
  const evictedThreads = [...threads.values()].filter((t) => {
    const info = t.getCompletionStatus();
    return info.isRunning && info.status === "evicted";
  }).length;
  const body = {
    activeThreads: threads.size,
    runningThreads,
    evictedThreads,
    shuttingDown,
    memoryRatio: getMemoryRatio(),
    memoryEvictionThreshold: MEMORY_EVICTION_THRESHOLD,
    memory: {
      rss: mem.rss,
      heapUsed: mem.heapUsed,
      heapTotal: mem.heapTotal,
    },
  };
  res.status(shuttingDown ? 503 : 200).json(body);
});

// --- Start server ---

const port = parseInt(process.env.PORT || "8787");
app.listen(port, () => {
  console.log(`Sync server listening on port ${port}`);
});
