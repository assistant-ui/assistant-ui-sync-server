const ENTRY_POINT = process.env.SYNC_URL || "http://localhost:8788"; // scaler by default
const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:9999/api/chat";

async function readStream(
  res: Response,
  label: string,
  opts?: { abortAfterChunks?: number },
): Promise<string[]> {
  const lines: string[] = [];
  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let chunkCount = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const text = decoder.decode(value, { stream: true });
      for (const line of text.split("\n")) {
        if (line.startsWith("data: ")) {
          lines.push(line);
          chunkCount++;
          console.log(`  [${label}] ${line}`);
        }
      }

      if (opts?.abortAfterChunks && chunkCount >= opts.abortAfterChunks) {
        console.log(`  [${label}] disconnecting after ${chunkCount} chunks`);
        await reader.cancel();
        break;
      }
    }
  } catch {
    // expected on cancel
  }

  return lines;
}

function assert(condition: boolean, msg: string) {
  if (!condition) {
    console.error(`FAIL: ${msg}`);
    process.exit(1);
  }
}

// --- Test 1: Replay after stream completes ---
async function testReplayAfterComplete() {
  const threadId = `test-complete-${Date.now()}`;
  console.log(`\n=== Test 1: disconnect, wait for completion, resume ===`);
  console.log(`threadId: ${threadId}\n`);

  console.log("1) Starting chat stream (will disconnect after 3 chunks)...");
  const chatRes = await fetch(`${ENTRY_POINT}/api/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId, backendUrl: BACKEND_URL }),
  });
  const chatLines = await readStream(chatRes, "chat", { abortAfterChunks: 3 });

  console.log("\n2) Waiting 5s for stream to complete on server...");
  await new Promise((r) => setTimeout(r, 5000));

  console.log("3) Checking thread status...");
  const statusRes = await fetch(`${ENTRY_POINT}/api/status`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId }),
  });
  const status = (await statusRes.json()) as { isRunning: boolean; status: string };
  console.log(`   Status: ${JSON.stringify(status)}`);
  assert(!status.isRunning, "stream should be completed");

  console.log("\n4) Resuming stream (expecting full replay)...");
  const resumeRes = await fetch(`${ENTRY_POINT}/api/resume`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId }),
  });
  const resumeLines = await readStream(resumeRes, "resume");

  const allDone = resumeLines.some((l) => l.includes("[DONE]"));
  console.log(`\n   Chat received:   ${chatLines.length} data lines`);
  console.log(`   Resume received: ${resumeLines.length} data lines`);
  console.log(`   Stream complete: ${allDone}`);
  assert(allDone, "resume should contain [DONE]");
  assert(resumeLines.length > chatLines.length, "resume should have more lines");
  console.log("   PASS");
}

// --- Test 2: Resume mid-stream ---
async function testResumeMidStream() {
  const threadId = `test-midstream-${Date.now()}`;
  console.log(`\n=== Test 2: disconnect and resume mid-stream ===`);
  console.log(`threadId: ${threadId}\n`);

  console.log("1) Starting chat stream (will disconnect after 2 chunks)...");
  const chatRes = await fetch(`${ENTRY_POINT}/api/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId, backendUrl: BACKEND_URL }),
  });
  const chatLines = await readStream(chatRes, "chat", { abortAfterChunks: 2 });

  console.log("\n2) Resuming immediately (stream still active)...");
  const resumeRes = await fetch(`${ENTRY_POINT}/api/resume`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId }),
  });
  const resumeLines = await readStream(resumeRes, "resume");

  const allDone = resumeLines.some((l) => l.includes("[DONE]"));
  console.log(`\n   Chat received:   ${chatLines.length} data lines`);
  console.log(`   Resume received: ${resumeLines.length} data lines`);
  console.log(`   Stream complete: ${allDone}`);
  assert(allDone, "resume should contain [DONE]");
  assert(resumeLines.length > chatLines.length, "resume should have more lines");
  console.log("   PASS");
}

// --- Test 3: Scaler routes to correct server + health check ---
async function testScalerHealth() {
  console.log(`\n=== Test 3: scaler health and routing ===\n`);

  console.log("1) Checking scaler health...");
  const healthRes = await fetch(`${ENTRY_POINT}/api/health`);
  const health = (await healthRes.json()) as {
    redis: string;
    servers: Array<{
      url: string;
      healthy: boolean;
      exhausted: boolean;
      activeThreads: number;
      memoryUsage: string;
    }>;
  };
  console.log(`   Redis: ${health.redis}`);
  for (const s of health.servers) {
    console.log(`   Server ${s.url}: healthy=${s.healthy} exhausted=${s.exhausted} threads=${s.activeThreads} mem=${s.memoryUsage}`);
  }

  assert(health.redis === "connected", "redis should be connected");
  const healthyCount = health.servers.filter((s) => s.healthy).length;
  assert(healthyCount > 0, "at least one server should be healthy");

  // Start two threads and verify they both get routed
  const t1 = `test-route-a-${Date.now()}`;
  const t2 = `test-route-b-${Date.now()}`;

  console.log("\n2) Starting two concurrent streams...");
  const [res1, res2] = await Promise.all([
    fetch(`${ENTRY_POINT}/api/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ threadId: t1, backendUrl: BACKEND_URL }),
    }),
    fetch(`${ENTRY_POINT}/api/chat`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ threadId: t2, backendUrl: BACKEND_URL }),
    }),
  ]);

  const [lines1, lines2] = await Promise.all([
    readStream(res1, "stream-a"),
    readStream(res2, "stream-b"),
  ]);

  assert(lines1.some((l) => l.includes("[DONE]")), "stream a should complete");
  assert(lines2.some((l) => l.includes("[DONE]")), "stream b should complete");

  console.log("\n3) Checking scaler health after streams...");
  const health2Res = await fetch(`${ENTRY_POINT}/api/health`);
  const health2 = (await health2Res.json()) as { redis: string };
  console.log(`   Redis: ${health2.redis}`);

  console.log("   PASS");
}

// --- Test 4: Cancel mid-stream ---
async function testCancelMidStream() {
  const threadId = `test-cancel-${Date.now()}`;
  console.log(`\n=== Test 4: cancel mid-stream ===`);
  console.log(`threadId: ${threadId}\n`);

  console.log("1) Starting chat stream (will disconnect after 2 chunks)...");
  const chatRes = await fetch(`${ENTRY_POINT}/api/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId, backendUrl: BACKEND_URL }),
  });
  await readStream(chatRes, "chat", { abortAfterChunks: 2 });

  console.log("\n2) Cancelling stream...");
  const cancelRes = await fetch(`${ENTRY_POINT}/api/cancel`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId }),
  });
  const cancelBody = (await cancelRes.json()) as { success: boolean; found: boolean };
  console.log(`   Cancel response: ${JSON.stringify(cancelBody)}`);
  assert(cancelBody.success, "cancel should succeed");
  assert(cancelBody.found, "cancel should find the thread");

  console.log("\n3) Checking status after cancel...");
  // Give the abort a moment to propagate
  await new Promise((r) => setTimeout(r, 500));
  const statusRes = await fetch(`${ENTRY_POINT}/api/status`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId }),
  });
  const status = (await statusRes.json()) as { isRunning: boolean; status: string };
  console.log(`   Status: ${JSON.stringify(status)}`);
  // After cancel the scaler removes the Redis pin, so the thread is unreachable via scaler
  assert(status.status === "not_found", "thread should be not_found after cancel (pin removed)");

  console.log("\n4) Resume after cancel should return not_found (pin removed from Redis)...");
  const resumeRes = await fetch(`${ENTRY_POINT}/api/resume`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId }),
  });
  const streamStatus = resumeRes.headers.get("x-stream-status");
  console.log(`   Resume status: ${resumeRes.status}, x-stream-status: ${streamStatus}`);
  assert(resumeRes.status === 200, "resume after cancel should return 200");
  assert(streamStatus === "not_found", "x-stream-status should be not_found");
  console.log("   PASS");
}

// --- Test 5: Initial state is tied to the resumed run ---
async function testResumeInitialState() {
  const threadId = `test-initial-state-${Date.now()}`;
  const initialState = { messages: [{ id: "user-1", content: "hello" }] };
  console.log(`\n=== Test 5: resume uses the retained initial state ===\n`);

  const chatRes = await fetch(`${ENTRY_POINT}/api/chat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      threadId,
      backendUrl: BACKEND_URL,
      state: initialState,
    }),
  });
  await readStream(chatRes, "initial-state-chat", { abortAfterChunks: 1 });

  const stateRes = await fetch(`${ENTRY_POINT}/api/initial-state`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId }),
  });
  assert(stateRes.ok, "initial state request should succeed");
  const snapshot = (await stateRes.json()) as {
    runId: string;
    state: unknown;
  };
  assert(typeof snapshot.runId === "string", "snapshot should identify its run");
  assert(
    JSON.stringify(snapshot.state) === JSON.stringify(initialState),
    "snapshot should preserve the state that started the run",
  );

  const mismatchRes = await fetch(`${ENTRY_POINT}/api/resume`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId, runId: "different-run" }),
  });
  assert(mismatchRes.status === 409, "a mismatched run should not be replayed");
  assert(
    mismatchRes.headers.get("x-stream-status") === "run_mismatch",
    "a mismatched run should report its status",
  );

  const resumeRes = await fetch(`${ENTRY_POINT}/api/resume`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ threadId, runId: snapshot.runId }),
  });
  assert(resumeRes.ok, "the matching run should resume");
  await readStream(resumeRes, "initial-state-resume");
  console.log("   PASS");
}

// --- Run ---
async function main() {
  await testReplayAfterComplete();
  await testResumeMidStream();
  await testScalerHealth();
  await testCancelMidStream();
  await testResumeInitialState();
  console.log("\n=== All tests passed ===\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
