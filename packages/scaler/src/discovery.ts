import { readFileSync } from "fs";

// --- Configuration ---

// K8s headless service discovery
const K8S_SERVICE_NAME = process.env.K8S_SERVICE_NAME; // e.g. "sync-server"
const K8S_NAMESPACE = process.env.K8S_NAMESPACE || "default";
const K8S_SERVICE_PORT = parseInt(process.env.K8S_SERVICE_PORT || "8787");

// Fallback for local dev
const STATIC_SYNC_SERVERS = process.env.SYNC_SERVERS || "http://localhost:8787";

// --- K8s in-cluster config ---
// TLS: the scaler K8s manifest sets NODE_EXTRA_CA_CERTS to the service-account CA bundle,
// so Node trusts the K8s API server cert without any extra code here.

const SA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount";

function getK8sToken(): string {
  return readFileSync(`${SA_PATH}/token`, "utf-8");
}

function getK8sApiHost(): string {
  return `https://${process.env.KUBERNETES_SERVICE_HOST}:${process.env.KUBERNETES_SERVICE_PORT}`;
}

// --- Endpoint discovery ---

interface K8sEndpoints {
  subsets?: Array<{
    addresses?: Array<{ ip: string; targetRef?: { name: string } }>;
    ports?: Array<{ port: number }>;
  }>;
}

async function discoverFromK8s(): Promise<string[]> {
  const token = getK8sToken();
  const apiHost = getK8sApiHost();
  const url = `${apiHost}/api/v1/namespaces/${K8S_NAMESPACE}/endpoints/${K8S_SERVICE_NAME}`;

  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(5000),
  });

  if (!res.ok) {
    throw new Error(`K8s API returned ${res.status}: ${await res.text()}`);
  }

  const data = (await res.json()) as K8sEndpoints;
  const urls: string[] = [];

  for (const subset of data.subsets ?? []) {
    const port = subset.ports?.[0]?.port ?? K8S_SERVICE_PORT;
    for (const addr of subset.addresses ?? []) {
      urls.push(`http://${addr.ip}:${port}`);
    }
  }

  return urls;
}

function discoverFromEnv(): string[] {
  return STATIC_SYNC_SERVERS.split(",").map((s) => s.trim()).filter(Boolean);
}

// --- Public API ---

export async function discoverServers(): Promise<string[]> {
  // Use K8s discovery if service name is configured and we're in-cluster
  if (K8S_SERVICE_NAME && process.env.KUBERNETES_SERVICE_HOST) {
    try {
      const urls = await discoverFromK8s();
      if (urls.length > 0) {
        console.log(`K8s discovery: found ${urls.length} sync server(s)`);
        return urls;
      }
      console.warn("K8s discovery: no endpoints found, falling back to env");
    } catch (err) {
      console.warn("K8s discovery failed, falling back to env:", (err as Error).message);
    }
  }

  return discoverFromEnv();
}
