const COMPLETE_GC_DELAY = 20000;
export const REQUEST_TIMEOUT = 2 * 60 * 60 * 1000; // 2 hours

// How long to keep "recently completed" status available for late resumers
const COMPLETED_STATE_RETENTION = 30000; // 30 seconds

type CompletionStatus = "running" | "completed" | "aborted" | "error";

export class ThreadSync {
  private headers: Headers | undefined;
  private body: ReadableStream<Uint8Array> | null = null;
  private isRunning: boolean = false;
  private abortController: AbortController | null = null;

  private completionStatus: CompletionStatus = "running";
  private completedAt: number | null = null;

  // Generation counter to prevent old stream handlers from disposing newer streams.
  private generation: number = 0;

  constructor(private disposeCallback: () => void) {}

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

    // Reset state for new stream
    this.completionStatus = "running";
    this.completedAt = null;

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
          JSON.stringify({ error: `Backend error: ${result.status}` }),
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

      // conn2: lifecycle management — tracks stream completion/abort and schedules GC
      conn2
        .pipeTo(
          new WritableStream({
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

  resume(): Response {
    const stream = this.getActiveReadableStream();

    if (stream) {
      return new Response(stream, { headers: this.headers });
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
      status: this.completionStatus,
      completedAt: this.completedAt,
      isRunning: this.isRunning,
    };
  }

  async cancel() {
    this.abortController?.abort("Stream cancelled by user");
    this.abortController = null;
    this.body?.cancel();
    this.body = null;
    this.isRunning = false;
    this.completionStatus = "aborted";
    this.completedAt = Date.now();
    this.disposeCallback();
  }
}
