// What the v2 drain steps need of the Kind cluster besides the browser: the
// cluster controller's drain v2 config, its replicas, and a fake EC2
// instance for each worker a v2 drain runs on.
//
// The API serves `:drain {engine: v2}` on Kind (values-kind.yaml turns
// SPACE_DRAIN_V2_ENABLED on), but only the cluster controller's v2 loop moves
// such a drain along, and it runs with SPACE_DRAIN_V2_ENABLED and the `aws`
// terminator, against Kind's fake EC2. A v2 drain also needs the node's EC2
// instance recorded, so its worker is re-registered under a fake AWS
// providerID first. This is the approach of `space_smoke/drain_v2.py`
// (`v2_controller`, `give_fake_instance`), under the same cluster-visible
// Leases, so a loop and a smoke never change the controller or cordon
// workers at once.
import { createHash, randomBytes } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import os from 'node:os';

import { isLoopback } from './config.ts';

export const NAMESPACE = 'space';
const CONTROLLER_SELECTOR = 'app.kubernetes.io/name=space-cluster-controller';
const KUBELET_FLAGS = '/var/lib/kubelet/kubeadm-flags.env';
const CONTEXT = /^kind-[a-z0-9][a-z0-9-]*$/;
const NODE_NAME = /^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/;

// The controller env a v2 run scopes to itself, and the only keys it touches.
export const V2_CONTROLLER_ENV: Record<string, string> = {
  SPACE_DRAIN_V2_ENABLED: 'true',
  SPACE_NODE_DRAIN_TERMINATOR_KIND: 'aws',
};
export const V2_CONTROLLER_KEYS = Object.keys(V2_CONTROLLER_ENV);

export const FAKE_PROVIDER_PREFIX = 'aws:///kind-a/';
const FAKE_PROVIDER = /^aws:\/\/\/kind-a\/(i-[0-9a-f]{17})$/;

// A new fake AWS providerID and the instance it names.
export function fakeProviderId(): { providerId: string; instanceId: string } {
  const instanceId = `i-${randomBytes(9).toString('hex').slice(0, 17)}`;
  return { providerId: `${FAKE_PROVIDER_PREFIX}${instanceId}`, instanceId };
}

// The instance a fake providerID names, or null for any other providerID.
export function fakeInstanceOf(providerId: string): string | null {
  return FAKE_PROVIDER.exec(providerId)?.[1] ?? null;
}

// kubeadm's kubelet flags with `--provider-id` set to `providerId`, which
// they must set exactly once.
export function withProviderId(flags: string, providerId: string): string {
  if (!FAKE_PROVIDER.test(providerId) && !providerId.startsWith('kind://')) throw new Error(`refusing providerID ${providerId}`);
  let count = 0;
  const rewritten = flags.replace(/--provider-id=[^\s"]*/g, () => {
    count += 1;
    return `--provider-id=${providerId}`;
  });
  if (count !== 1) throw new Error("the Kind worker's kubeadm-flags.env does not set --provider-id exactly once");
  return rewritten;
}

// The literal each key holds in a container's env, null when unset. A
// `valueFrom` entry can't be put back as a literal, so it is refused.
export function envLiterals(entries: unknown, keys: readonly string[]): Record<string, string | null> {
  const standing: Record<string, string | null> = Object.fromEntries(keys.map((key) => [key, null]));
  for (const entry of Array.isArray(entries) ? entries : []) {
    if (!entry || typeof entry !== 'object' || !keys.includes((entry as { name?: string }).name ?? '')) continue;
    const { name, value, valueFrom } = entry as { name: string; value?: unknown; valueFrom?: unknown };
    if (valueFrom !== undefined) throw new Error(`${name} is a valueFrom reference; refusing to override what a literal restore could not put back`);
    standing[name] = typeof value === 'string' ? value : '';
  }
  return standing;
}

// `kubectl set env` arguments for `values`, null unsetting a key.
export function envAssignments(values: Record<string, string | null>): string[] {
  return Object.entries(values).map(([key, value]) => (value === null ? `${key}-` : `${key}=${value}`));
}

export interface LeaseSpec {
  holderIdentity?: string;
  leaseDurationSeconds?: number;
  acquireTime?: string;
  renewTime?: string;
}

// Whether a Lease may be taken at `nowMs`: it has no holder, its holder's
// term has run out, or its holder is a console loop on this host whose
// process `alive` says is gone (an interrupted run).
export function leaseFree(spec: LeaseSpec, nowMs: number, host = os.hostname(), alive: (pid: number) => boolean = processAlive): boolean {
  if (!spec.holderIdentity) return true;
  const loop = /^console-loop;uid=\d+;pid=(\d+);host=([^;]+);/.exec(spec.holderIdentity);
  if (loop && loop[2] === host && !alive(Number(loop[1]))) return true;
  const renewed = Date.parse(spec.renewTime ?? spec.acquireTime ?? '');
  const duration = spec.leaseDurationSeconds;
  if (!Number.isFinite(renewed) || typeof duration !== 'number' || duration <= 0) return false;
  return nowMs >= renewed + duration * 1000;
}

function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}

// The smokes' Lease names: one per kind context and guarded resource
// (`acquire_retention_sweeper_mutation` and `acquire_worker_cordon_mutation`
// in `space_smoke/adapters.py`).
export function leaseName(kind: 'retention' | 'cordon', context: string): string {
  return `space-smoke-${kind}-${createHash('sha256').update(context).digest('hex').slice(0, 16)}`;
}

const kubeTime = (ms: number) => new Date(ms).toISOString().replace(/\.\d{3}Z$/, '.000000Z');

export interface Lease {
  name: string;
  uid: string;
  holder: string;
}

export class KindV2Control {
  readonly context: string;

  constructor(context: string) {
    if (!CONTEXT.test(context)) throw new Error(`refusing kube context "${context}": only a kind-* context`);
    this.context = context;
    const server = this.kubectl('config', 'view', '--minify', '-o', 'jsonpath={.clusters[0].cluster.server}').trim();
    if (!server || !isLoopback(server)) throw new Error(`refusing kube context ${context}: its API server is not on this host`);
  }

  private kubectl(...args: string[]): string {
    return this.kubectlIn(undefined, ...args);
  }

  private kubectlIn(input: string | undefined, ...args: string[]): string {
    return execFileSync('kubectl', ['--context', this.context, '-n', NAMESPACE, '--request-timeout=30s', ...args], {
      encoding: 'utf8',
      input,
      stdio: [input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'],
      timeout: 600_000,
    });
  }

  // The Kind node container behind a worker's Kubernetes Node, which must be
  // one of this cluster's workers.
  private workerContainer(node: string): string {
    if (!NODE_NAME.test(node)) throw new Error(`refusing Kubernetes node name "${node}"`);
    const cluster = this.context.slice('kind-'.length);
    if (!new RegExp(`^${cluster}-worker[0-9]*$`).test(node)) throw new Error(`refusing ${node}: not a Kind worker of ${cluster}`);
    return node;
  }

  private docker(input: string | undefined, ...args: string[]): string {
    return execFileSync('docker', args, { encoding: 'utf8', input, stdio: [input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'], timeout: 120_000 });
  }

  // --- Leases ------------------------------------------------------------

  // Creates or takes over the Lease, or throws while another holder's term runs.
  acquireLease(kind: 'retention' | 'cordon', durationSeconds = 3600): Lease {
    const name = leaseName(kind, this.context);
    const holder = `console-loop;uid=${os.userInfo().uid};pid=${process.pid};host=${os.hostname()};token=${randomBytes(16).toString('hex')}`;
    const now = Date.now();
    const spec: LeaseSpec = { holderIdentity: holder, leaseDurationSeconds: durationSeconds, acquireTime: kubeTime(now), renewTime: kubeTime(now) };
    let current: { metadata: { uid: string; resourceVersion: string }; spec: LeaseSpec } | null = null;
    try {
      current = JSON.parse(this.kubectl('get', 'lease', name, '-o', 'json'));
    } catch (error) {
      if (!/NotFound/i.test(String((error as { stderr?: unknown }).stderr ?? error))) throw error;
    }
    if (!current) {
      const created = JSON.parse(this.kubectlIn(JSON.stringify({
        apiVersion: 'coordination.k8s.io/v1', kind: 'Lease', metadata: { name, namespace: NAMESPACE }, spec,
      }), 'create', '-f', '-', '-o', 'json'));
      return { name, uid: created.metadata.uid, holder };
    }
    if (!leaseFree(current.spec ?? {}, now)) throw new Error(`another holder has the ${name} Lease (${current.spec.holderIdentity})`);
    const replaced = JSON.parse(this.kubectl('patch', 'lease', name, '--type=json', '-o', 'json', '--patch', JSON.stringify([
      { op: 'test', path: '/metadata/uid', value: current.metadata.uid },
      { op: 'test', path: '/metadata/resourceVersion', value: current.metadata.resourceVersion },
      { op: 'replace', path: '/spec', value: spec },
    ])));
    return { name, uid: replaced.metadata.uid, holder };
  }

  // Whether this run still holds the Lease.
  holds(lease: Lease): boolean {
    const current = JSON.parse(this.kubectl('get', 'lease', lease.name, '-o', 'json'));
    return current.metadata.uid === lease.uid && current.spec?.holderIdentity === lease.holder;
  }

  releaseLease(lease: Lease): void {
    const current = JSON.parse(this.kubectl('get', 'lease', lease.name, '-o', 'json'));
    if (current.metadata.uid !== lease.uid || current.spec?.holderIdentity !== lease.holder) {
      throw new Error(`refusing to release the ${lease.name} Lease: another holder has it`);
    }
    this.kubectl('patch', 'lease', lease.name, '--type=json', '--patch', JSON.stringify([
      { op: 'test', path: '/metadata/resourceVersion', value: current.metadata.resourceVersion },
      { op: 'replace', path: '/spec/holderIdentity', value: '' },
    ]));
  }

  private requireLease(lease: Lease | null, what: string): void {
    if (!lease || !this.holds(lease)) throw new Error(`refusing ${what} without this run's live controller Lease`);
  }

  // --- the cluster controller ---------------------------------------------

  controllerDeployment(): string {
    const name = this.kubectl('get', 'deploy', '-l', CONTROLLER_SELECTOR, '-o', 'jsonpath={.items[0].metadata.name}').trim();
    if (!name) throw new Error('no deployment matched the cluster-controller selector');
    return name;
  }

  controllerEnv(): Record<string, string | null> {
    const env = this.kubectl('get', 'deploy', this.controllerDeployment(), '-o', 'jsonpath={.spec.template.spec.containers[0].env}');
    return envLiterals(JSON.parse(env || '[]'), V2_CONTROLLER_KEYS);
  }

  // Writes `values` to the controller's env and waits for its rollout; a
  // value already in place writes nothing.
  setControllerEnv(lease: Lease | null, values: Record<string, string | null>): boolean {
    const unknown = Object.keys(values).filter((key) => !V2_CONTROLLER_KEYS.includes(key));
    if (unknown.length) throw new Error(`refusing controller env keys ${unknown.join(', ')}`);
    const current = this.controllerEnv();
    const changed = Object.fromEntries(Object.entries(values).filter(([key, value]) => current[key] !== value));
    if (!Object.keys(changed).length) return false;
    this.requireLease(lease, 'a cluster-controller env write');
    const name = this.controllerDeployment();
    const container = this.kubectl('get', 'deploy', name, '-o', 'jsonpath={.spec.template.spec.containers[0].name}').trim();
    this.kubectl('set', 'env', `deploy/${name}`, '-c', container, ...envAssignments(changed));
    this.kubectl('rollout', 'status', `deploy/${name}`, '--timeout=300s');
    return true;
  }

  controllerReplicas(): number {
    return Number(this.kubectl('get', 'deploy', this.controllerDeployment(), '-o', 'jsonpath={.spec.replicas}').trim() || '0');
  }

  // Scales the controller and waits: for its pods to go at 0, else for the
  // rollout. Returns the replicas it had.
  scaleController(lease: Lease | null, replicas: number): number {
    if (!Number.isInteger(replicas) || replicas < 0 || replicas > 2) throw new Error(`refusing to scale the cluster-controller to ${replicas}`);
    this.requireLease(lease, 'a cluster-controller scale');
    const previous = this.controllerReplicas();
    const name = this.controllerDeployment();
    this.kubectl('scale', `deploy/${name}`, `--replicas=${replicas}`);
    if (replicas === 0) {
      this.kubectl('wait', '--for=delete', 'pod', '-l', CONTROLLER_SELECTOR, '--timeout=180s');
    } else {
      this.kubectl('rollout', 'status', `deploy/${name}`, '--timeout=300s');
    }
    return previous;
  }

  // --- a worker's fake EC2 instance ---------------------------------------

  providerId(node: string): string {
    return this.kubectl('get', 'node', this.workerContainer(node), '-o', 'jsonpath={.spec.providerID}').trim();
  }

  // Pods the Kubernetes Node runs in the sandbox namespace: a worker with
  // any is not re-registered, which deletes its pods.
  sandboxPods(node: string): number {
    const out = execFileSync('kubectl', ['--context', this.context, '--request-timeout=30s', 'get', 'pods', '-A', '--field-selector', `spec.nodeName=${this.workerContainer(node)}`, '-o', 'json'], { encoding: 'utf8' });
    return (JSON.parse(out).items as { metadata: { namespace: string } }[]).filter((pod) => pod.metadata.namespace === 'sandboxes').length;
  }

  // The worker's fake instance: re-registers its Node under a new fake AWS
  // providerID unless it has one. A Node's providerID can't change once set,
  // so the kubelet registers a new Node: the old one is deleted, the kubelet
  // restarted with the rewritten flag, and the bring-up's labels put back.
  // It deletes the pods on the worker, so only an empty worker is accepted.
  giveFakeInstance(lease: Lease | null, node: string): { instanceId: string; reRegistered: boolean } {
    const container = this.workerContainer(node);
    const existing = fakeInstanceOf(this.providerId(container));
    if (existing) return { instanceId: existing, reRegistered: false };
    this.requireLease(lease, 're-registering a Kind worker');
    if (this.sandboxPods(container)) throw new Error(`refusing to re-register ${container}: it runs sandbox pods`);
    const labels = JSON.parse(this.kubectl('get', 'node', container, '-o', 'jsonpath={.metadata.labels}') || '{}') as Record<string, string>;
    const { providerId, instanceId } = fakeProviderId();
    const flags = withProviderId(this.docker(undefined, 'exec', container, 'cat', KUBELET_FLAGS), providerId);
    this.docker(flags, 'exec', '-i', container, 'sh', '-c', `cat > ${KUBELET_FLAGS}`);
    this.kubectl('delete', 'node', container, '--wait=true');
    this.docker(undefined, 'exec', container, 'systemctl', 'restart', 'kubelet');
    const deadline = Date.now() + 300_000;
    for (;;) {
      try {
        this.kubectl('get', 'node', container, '-o', 'name');
        break;
      } catch {
        if (Date.now() > deadline) throw new Error(`Kind worker ${container} did not re-register its Node`);
        execFileSync('sleep', ['2']);
      }
    }
    const current = JSON.parse(this.kubectl('get', 'node', container, '-o', 'jsonpath={.metadata.labels}') || '{}') as Record<string, string>;
    const missing = Object.entries(labels).filter(([key]) => !(key in current)).map(([key, value]) => `${key}=${value}`);
    if (missing.length) this.kubectl('label', 'node', container, ...missing);
    this.kubectl('wait', '--for=condition=Ready', `node/${container}`, '--timeout=300s');
    execFileSync('kubectl', ['--context', this.context, 'wait', '--for=condition=Ready', 'pod', '--all', '-n', NAMESPACE, '--field-selector', `spec.nodeName=${container}`, '--timeout=300s'], { encoding: 'utf8', timeout: 330_000 });
    if (this.providerId(container) !== providerId) throw new Error(`Kind worker ${container} registered another providerID`);
    return { instanceId, reRegistered: true };
  }

  // The worker's advertised sandbox network slots; a sandbox pod can't be
  // scheduled on it until node-ctl advertises them.
  networkSlots(node: string): number {
    const slots = this.kubectl('get', 'node', this.workerContainer(node), '-o', 'jsonpath={.status.allocatable.space\\.pplx\\.net/network-slots}').trim();
    return /^\d+$/.test(slots) ? Number(slots) : 0;
  }
}
