# assistant-ui sync server

A resumable streaming proxy for long-running AI tasks. Clients can disconnect and reconnect at any time — the stream keeps running on the server and can be replayed in full on resume.

## Why

AI agent runs can take minutes to hours. Users close their browser, switch tabs, or lose connectivity. Without a sync layer, the stream is lost and the user has to start over.

This project sits between the client and the AI backend:

```
Client <-> Scaler <-> Sync Server <-> AI Backend
                 \-> Redis (thread routing)
```

The **sync server** holds the stream in memory using `ReadableStream.tee()`. If the client disconnects, the stream continues and is available for full replay on resume.

The **scaler** routes requests to the right sync server using Redis thread pinning, so the system scales horizontally across multiple sync server instances.

## Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐      ┌───────┐
                    │   Scaler    │─────▶│ Redis │
                    │  port 8788  │      └───────┘
                    └──────┬──────┘
                           │
                    ┌──────┴──────┐
                    │             │
              ┌─────▼─────┐ ┌────▼──────┐
              │ Sync Srv 1 │ │ Sync Srv 2 │
              │  port 8787 │ │  port 8787 │
              └─────┬──────┘ └─────┬──────┘
                    │              │
                    └──────┬───────┘
                           │
                    ┌──────▼──────┐
                    │ AI Backend  │
                    └─────────────┘
```

- **Scaler** — Reverse proxy. Looks up `threadId → sync-server` mapping in Redis. Picks the least-loaded server for new threads. Health-polls all sync servers. Stateless — runs as many replicas as needed.
- **Sync Server** — Holds in-flight streams in memory. Forwards `/api/chat` to the AI backend, tees the response stream for resumability. Reports memory usage and thread count to the scaler via `/api/health`.
- **Redis** — Stores `threadId → sync-server URL` with a TTL. That's it.

## API

All endpoints go through the scaler.

### `POST /api/chat`

Start a new streaming request. The scaler pins the thread to a sync server and proxies the stream.

```json
{ "threadId": "abc-123", "backendUrl": "https://your-ai-backend/api/chat", "...": "any other fields are forwarded to the backend" }
```

Returns a streaming response (SSE, NDJSON, or whatever your backend sends).

### `POST /api/resume`

Resume a stream after disconnect. Returns the full stream from the beginning (buffered via `tee()`), then continues with live data if still running.

```json
{ "threadId": "abc-123" }
```

Returns the same streaming response as `/api/chat`. If the thread is not found, returns `204` with `X-Stream-Status: not_found`.

### `POST /api/cancel`

Cancel an in-flight stream.

```json
{ "threadId": "abc-123" }
```

```json
{ "success": true, "found": true }
```

### `POST /api/status`

Check if a thread is still running.

```json
{ "threadId": "abc-123" }
```

```json
{ "isRunning": false, "status": "completed", "completedAt": 1710000000000 }
```

Status is one of: `running`, `completed`, `aborted`, `error`, `not_found`.

### `GET /api/health`

Health check. The scaler's response includes per-server metrics:

```json
{
  "redis": "connected",
  "servers": [
    { "url": "http://10.1.0.9:8787", "healthy": true, "exhausted": false, "activeThreads": 3, "runningThreads": 1, "memoryUsage": "18%" }
  ]
}
```

## Running locally

### Docker Compose

```bash
docker compose up -d --build
docker compose --profile test run --rm test-client
docker compose down
```

This starts redis, 2 sync servers, a scaler, and a fake AI backend (test-server). The test client runs 4 integration tests:

1. Disconnect after 3 chunks, wait for completion, resume — full replay
2. Disconnect after 2 chunks, resume immediately — mid-stream resume
3. Health check + concurrent stream routing across servers
4. Cancel mid-stream

### Kubernetes (Docker Desktop)

Enable Kubernetes in Docker Desktop, then:

```bash
# Build images
docker build -f packages/sync-server/Dockerfile -t assistant-ui/sync-server:latest .
docker build -f packages/scaler/Dockerfile -t assistant-ui/scaler:latest .
docker build -f packages/test-server/Dockerfile -t assistant-ui/test-server:latest .
docker build -f packages/test-client/Dockerfile -t assistant-ui/test-client:latest .

# Deploy
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/redis.yaml
kubectl apply -f k8s/sync-server.yaml
kubectl apply -f k8s/scaler.yaml

# Port-forward the scaler
kubectl -n sync-server port-forward svc/scaler 8788:80
```

The scaler auto-discovers sync-server pods via the K8s Endpoints API (headless service).

### Local dev (no containers)

```bash
pnpm install

# Terminal 1: Redis
docker run --rm -p 6379:6379 redis:7-alpine

# Terminal 2: Sync server
cd packages/sync-server && pnpm dev

# Terminal 3: Scaler
cd packages/scaler && pnpm dev

# Terminal 4: Test
cd packages/test-server && pnpm dev
cd packages/test-client && pnpm dev
```

## Configuration

### Scaler (`packages/scaler`)

| Env var | Default | Description |
|---|---|---|
| `PORT` | `8788` | Listen port |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `SYNC_SERVERS` | `http://localhost:8787` | Comma-separated sync server URLs (local dev fallback) |
| `K8S_SERVICE_NAME` | — | K8s headless service name for auto-discovery |
| `K8S_NAMESPACE` | `default` | K8s namespace |
| `RAM_THRESHOLD` | `0.5` | Memory ratio above which a server is marked exhausted |
| `MAX_THREADS_PER_SERVER` | `100` | Thread count above which a server is marked exhausted |
| `THREAD_TTL_SECONDS` | `120` | Redis key TTL for thread→server mapping |
| `HEALTH_POLL_MS` | `5000` | Health check interval |
| `DISCOVERY_POLL_MS` | `10000` | Server discovery interval |

### Sync Server (`packages/sync-server`)

| Env var | Default | Description |
|---|---|---|
| `PORT` | `8787` | Listen port |
| `SHUTDOWN_TIMEOUT_MS` | `3600000` | Max time to drain active streams on SIGTERM (1 hour) |

## Project structure

```
packages/
  sync-server/     # Stream buffering + resume (ThreadSync class)
  scaler/          # Redis routing + health polling + K8s discovery
  test-server/     # Fake AI backend (10 SSE chunks × 300ms)
  test-client/     # Integration test suite
k8s/               # Kubernetes manifests (namespace, redis, sync-server, scaler)
docker-compose.yml # Local stack with 2 sync servers
```

## How resume works

The sync server uses `ReadableStream.tee()` to fork the backend response:

1. **conn1** — stored for future resume calls (internally buffers all data)
2. **conn2** — lifecycle management (tracks completion, schedules GC)
3. **conn3** — sent to the original client

When a client calls `/api/resume`, the stored branch (`conn1`) is tee'd again. Because the tee buffer holds all unconsumed data, the new branch replays the full stream from the beginning, then continues with live data.

After the stream completes, the `ThreadSync` instance is kept for 50 seconds (20s GC delay + 30s retention) so late resumers can still get the completion status before it's garbage collected.

## How scaling works

1. Client sends `/api/chat` with a `threadId` to the scaler
2. Scaler checks Redis — if the thread is already pinned to a server, route there
3. Otherwise, pick the least-loaded healthy server (by active thread count) and pin the thread in Redis
4. The scaler proxies the streaming response back to the client, periodically refreshing the Redis TTL
5. On `/api/resume`, the scaler looks up the pin and routes to the same server
6. If the client disconnects from the scaler, the scaler closes its upstream connection — the sync server continues buffering for future resume clients

Exhaustion signals prevent overloading a single server:
- **Memory**: `memoryRatio >= 0.5` (RSS vs cgroup limit in containers, heap ratio otherwise)
- **Thread count**: `runningThreads >= MAX_THREADS_PER_SERVER`

When all servers are exhausted, the scaler falls back to the one with the lowest memory usage.
