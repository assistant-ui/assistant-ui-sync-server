export interface ServerState {
  url: string;
  exhausted: boolean;
  activeThreads: number;
  runningThreads: number;
  memoryUsage: number;
  healthy: boolean;
}

interface HealthResponse {
  activeThreads: number;
  runningThreads: number;
  memoryRatio?: number;
  memory: { heapUsed: number; heapTotal: number };
}

const MAX_THREADS = parseInt(process.env.MAX_THREADS_PER_SERVER ?? "100");

async function pollOne(server: ServerState, memThreshold: number) {
  try {
    const res = await fetch(`${server.url}/api/health`, {
      signal: AbortSignal.timeout(3000),
    });
    if (!res.ok) {
      server.healthy = false;
      return;
    }
    const data = (await res.json()) as HealthResponse;
    server.healthy = true;
    server.activeThreads = data.activeThreads;
    server.runningThreads = data.runningThreads;
    server.memoryUsage = data.memoryRatio ?? (data.memory.heapUsed / data.memory.heapTotal);
    server.exhausted =
      server.memoryUsage >= memThreshold ||
      server.runningThreads >= MAX_THREADS;
  } catch {
    server.healthy = false;
  }
}

export function createHealthPoller(
  servers: Map<string, ServerState>,
  threshold: number,
) {
  return {
    async poll() {
      await Promise.allSettled(
        [...servers.values()].map((s) => pollOne(s, threshold)),
      );
    },
  };
}
