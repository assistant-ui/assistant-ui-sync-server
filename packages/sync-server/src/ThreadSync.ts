import { randomUUID } from "node:crypto";

const COMPLETE_GC_DELAY = 20000;
export const REQUEST_TIMEOUT = 2 * 60 * 60 * 1000; // 2 hours

// How long to keep "recently completed" status available for late resumers
const COMPLETED_STATE_RETENTION = 30000; // 30 seconds

type CompletionStatus = "running" | "completed" | "aborted" | "error";
type VisibleStreamStatus = CompletionStatus | "evicted";

export class ThreadSync {
  private headers: Headers | undefined;
  private body: ReadableStream<Uint8Array> | null = null;
  private isRunning: boolean = false;
  private abortController: AbortController | null = null;

  private completionStatus: CompletionStatus = "running";
  private completedAt: number | null = null;
  private evictedAt: number | null = null;
  private runId: string | null = null;
  private initialState: unknown = null;

  // Generation counter to prevent old stream handlers from disposing newer streams.
  private generation: number = 0;

  // Total bytes of backend body observed for the current run. Snapshotted on
  // resume as `Aui-Replay-Content-Length` so the client can treat the first N
  // body bytes of the resume response as historical replay (no live side
  // effects) and everything after as live.
  private bytesReceived: number = 0;

  constructor(
    private disposeCallback: () => void,
    private memoryPressureCallback: () => void = () => {},
  ) {}

  private getVisibleStatus(): VisibleStreamStatus {
    if (this.isRunning && this.evictedAt !== null) return "evicted";
    return this.completionStatus;
  }

  private getActiveReadableStream() {
    if (!this.body) return null;

    try {
      const [conn1, conn2] = this.body.tee();
      this.body = conn1;
      return conn2;
    } catch {
      this.body = null;
      this.isRunning = false;
      this.completionStatus = "error";
      this.completedAt = Date.now();
      return null;
    }
  }

  async fetch(
    request: Request,
    {
      backendUrl,
      ...requestBody
    }: Record<string, unknown> & { backendUrl: string },
    timeoutSignal = AbortSignal.timeout(REQUEST_TIMEOUT),
  ): Promise<Response> {
    this.generation++;
    const currentGeneration = this.generation;
    this.runId = randomUUID();
    this.initialState = requestBody.state ?? null;

    // Reset state for new stream
    this.completionStatus = "running";
    this.completedAt = null;
    this.evictedAt = null;
    this.bytesReceived = 0;

    // Build headers from incoming request, stripping transport-level ones
    const headers = new Headers();
    if (request.headers) {
      const raw = request.headers as unknown as Record<string, string | string[] | undefined>;
      for (const [key, value] of Object.entries(raw)) {
        if (value) {
          headers.set(key, Array.isArray(value) ? value.join(", ") : value);
        }
      }
    }
    headers.delete("content-length");
    headers.delete("accept-encoding");
    headers.delete("host");
    headers.delete("connection");
    this.headers = headers;

    this.abortController?.abort("New request started");
    this.abortController = new AbortController();
    // @ts-ignore — AbortSignal.any exists in Node 20+
    const signal = AbortSignal.any([this.abortController.signal, timeoutSignal]);

    try {
      const result = await fetch(backendUrl, {
        method: "POST",
        headers,
        body: JSON.stringify(requestBody),
        signal,
      });

      if (!result.ok) {
        return new Response(
          JSON.stringify({
            error: `Backend error: ${result.status}`,
            body: JSON.stringify(requestBody),
            backendUrl,
          }),
          { status: result.status, headers: { "Content-Type": "application/json" } },
        );
      }

      if (!result.body) {
        return new Response(
          JSON.stringify({ error: "Backend returned no body" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        );
      }

      const [conn1, teeable] = result.body.tee();
      const [conn2, conn3] = teeable.tee();

      // conn2: continuously drains the source (so conn1's buffer keeps filling
      // even after the client disconnects), counts bytes for
      // `Aui-Replay-Content-Length`, and observes lifecycle to schedule GC.
      conn2
        .pipeTo(
          new WritableStream({
            write: (chunk) => {
              if (currentGeneration === this.generation) {
                this.bytesReceived += chunk.byteLength;
                this.memoryPressureCallback();
              }
            },
            abort: async () => {
              if (currentGeneration !== this.generation) return;
              this.isRunning = false;
              this.completionStatus = "aborted";
              this.completedAt = Date.now();

              await new Promise((resolve) =>
                setTimeout(resolve, COMPLETE_GC_DELAY + COMPLETED_STATE_RETENTION),
              );
              if (currentGeneration === this.generation) this.disposeCallback();
            },
            close: async () => {
              if (currentGeneration !== this.generation) return;
              this.isRunning = false;
              this.completionStatus = "completed";
              this.completedAt = Date.now();

              await new Promise((resolve) =>
                setTimeout(resolve, COMPLETE_GC_DELAY + COMPLETED_STATE_RETENTION),
              );
              if (currentGeneration === this.generation) this.disposeCallback();
            },
          }),
        )
        .catch(async () => {
          if (currentGeneration !== this.generation) return;
          if (this.completionStatus === "running") {
            this.completionStatus = "error";
            this.completedAt = Date.now();
            this.isRunning = false;
          }
          await new Promise((resolve) =>
            setTimeout(resolve, COMPLETE_GC_DELAY + COMPLETED_STATE_RETENTION),
          );
          if (currentGeneration === this.generation) this.disposeCallback();
        });

      this.body?.cancel();
      this.body = conn1;
      this.isRunning = true;
      this.evictedAt = null;

      // conn3: returned to the caller for streaming to the client
      return new Response(conn3, result);
    } catch {
      this.isRunning = false;
      return new Response(
        JSON.stringify({ error: "Failed to connect to backend" }),
        { status: 503, headers: { "Content-Type": "application/json" } },
      );
    }
  }

  getInitialState(): { runId: string; state: unknown } | null {
    if (this.runId === null) return null;

    return {
      runId: this.runId,
      state: this.initialState,
    };
  }

  resume(expectedRunId?: string): Response {
    if (expectedRunId !== undefined && expectedRunId !== this.runId) {
      return Response.json(
        { error: "The requested run no longer matches the active thread run" },
        {
          status: 409,
          headers: { "X-Stream-Status": "run_mismatch" },
        },
      );
    }

    const stream = this.getActiveReadableStream();

    if (stream) {
      const headers = new Headers(this.headers);
      // Bytes already buffered when the resume starts are historical replay;
      // anything after byte N is live. See spec.md / Aui-Replay-Content-Length.
      headers.set("Aui-Replay-Content-Length", String(this.bytesReceived));
      return new Response(stream, { headers });
    }

    if (this.isRunning && this.evictedAt !== null) {
      return new Response(null, {
        status: 204,
        headers: {
          "X-Stream-Status": "evicted",
          "X-Evicted-At": this.evictedAt.toString(),
        },
      });
    }

    // Stream recently completed/aborted — return 204 with status header
    if (this.completionStatus !== "running" && this.completedAt) {
      return new Response(null, {
        status: 204,
        headers: {
          "X-Stream-Status": this.completionStatus,
          "X-Completed-At": this.completedAt.toString(),
        },
      });
    }

    return new Response(null, {
      status: 204,
      headers: { "X-Stream-Status": "not_available" },
    });
  }

  getIsRunning() {
    return this.isRunning;
  }

  getCompletionStatus() {
    return {
      status: this.getVisibleStatus(),
      completedAt: this.completedAt,
      evictedAt: this.evictedAt,
      isRunning: this.isRunning,
    };
  }

  getBufferedBytesEstimate() {
    return this.body ? this.bytesReceived : 0;
  }

  evictBuffer(reason = "Stream buffer evicted under memory pressure") {
    if (!this.isRunning || !this.body) return false;

    const body = this.body;
    this.body = null;
    this.evictedAt = Date.now();
    void body.cancel(reason).catch(() => {});
    return true;
  }

  async cancel() {
    this.abortController?.abort("Stream cancelled by user");
    this.abortController = null;
    this.body?.cancel();
    this.body = null;
    this.isRunning = false;
    this.completionStatus = "aborted";
    this.completedAt = Date.now();
    this.evictedAt = null;
    this.disposeCallback();
  }
}
