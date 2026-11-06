// What the node drain steps need besides the browser: which worker they may
// drain, and the `space.pplx.net/drain-keep` override on its Kubernetes Node.
//
// On the Kind stack the drained-node terminator deletes a drained node's
// Kubernetes Node within seconds (`SPACE_NODE_DRAIN_TERMINATOR_KIND:
// kube-delete`) unless the Node carries the override, so the steps set it
// before any drain and remove it only after the node is undrained, as the
// `local.forced_drain_evacuates_sandbox` smoke does.
import { execFileSync } from 'node:child_process';

import { isLoopback } from './config.ts';

export const DRAIN_KEEP_ANNOTATION = 'space.pplx.net/drain-keep';
export const DRAIN_KEEP_VALUE = 'console-loop';

// The admin node fields the steps read.
export interface AdminNode {
  id: string;
  name: string | null;
  role: string;
  managed_by: string;
  drain_phase: string | null;
  drain_mode: string | null;
  k8s_node_name: string | null;
  cordoned: boolean | null;
  sandboxes_active: number | null;
  sandboxes_local_snapshots: number | null;
}

// A Kubernetes-managed worker the steps may drain without moving anyone's
// sandbox: no drain on any node (a node left mid-drain makes every reading
// ambiguous), at least one other worker, and nothing on the chosen one, so a
// forced drain evacuates nothing and the seeded sandboxes keep their states.
// Occupancy must have been counted (`include_occupancy=true`).
export function drainCandidate(nodes: AdminNode[]): { node: AdminNode } | { reason: string } {
  const mid = nodes.filter((n) => n.drain_phase !== null);
  if (mid.length) return { reason: `a node is already in a drain phase (${mid.map((n) => `${n.name ?? n.id}: ${n.drain_phase}`).join(', ')})` };
  const workers = nodes.filter((n) => n.role === 'worker' && n.managed_by === 'k8s' && n.k8s_node_name);
  if (workers.length < 2) return { reason: `fewer than two Kubernetes workers (${workers.length})` };
  const empty = workers
    .filter((n) => n.sandboxes_active === 0 && n.sandboxes_local_snapshots === 0)
    .sort((a, b) => (a.k8s_node_name ?? '').localeCompare(b.k8s_node_name ?? ''));
  if (!empty.length) return { reason: 'every worker holds sandboxes or local snapshots, and a drain would move them' };
  return { node: empty[0] };
}

const CONTEXT = /^kind-[a-z0-9][a-z0-9-]*$/;
const NODE_NAME = /^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/;

// `kubectl` against the Kind context only, never the current context.
export class KindNodes {
  private readonly context: string;

  constructor(context: string) {
    if (!CONTEXT.test(context)) throw new Error(`refusing kube context "${context}": only a kind-* context`);
    this.context = context;
  }

  private kubectl(...args: string[]): string {
    return execFileSync('kubectl', ['--context', this.context, '--request-timeout=20s', ...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  }

  // The context's API server must be on this host, and the node must exist
  // there: the stack check proves the console and API are one stack, and
  // this ties the Kubernetes Node the steps annotate to a local cluster.
  requireLocalNode(name: string): void {
    if (!NODE_NAME.test(name)) throw new Error(`refusing Kubernetes node name "${name}"`);
    const server = this.kubectl('config', 'view', '--minify', '-o', 'jsonpath={.clusters[0].cluster.server}').trim();
    if (!server || !isLoopback(server)) throw new Error(`refusing kube context ${this.context}: its API server is not on this host`);
    this.kubectl('get', 'node', name, '-o', 'name');
  }

  // The Node's drain-keep annotation, or null without one.
  drainKeep(name: string): string | null {
    const node = JSON.parse(this.kubectl('get', 'node', name, '-o', 'json')) as { metadata?: { annotations?: Record<string, string> } };
    return node.metadata?.annotations?.[DRAIN_KEEP_ANNOTATION] ?? null;
  }

  setDrainKeep(name: string): void {
    this.kubectl('annotate', 'node', name, `${DRAIN_KEEP_ANNOTATION}=${DRAIN_KEEP_VALUE}`, '--overwrite');
  }

  clearDrainKeep(name: string): void {
    this.kubectl('annotate', 'node', name, `${DRAIN_KEEP_ANNOTATION}-`);
  }

  unschedulable(name: string): boolean {
    return this.kubectl('get', 'node', name, '-o', 'jsonpath={.spec.unschedulable}').trim() === 'true';
  }
}
