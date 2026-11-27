// What the node drain steps need besides the browser: which worker they may
// drain, and the `space.pplx.net/drain-keep` override on its Kubernetes Node.
//
// On the Kind stack the drained-node terminator deletes a drained node's
// Kubernetes Node within seconds (`SPACE_NODE_DRAIN_TERMINATOR_KIND:
// kube-delete`) unless the Node carries the override, so the steps set it
// before any drain and remove it only after the node is undrained, as the
// `local.forced_drain_evacuates_sandbox` smoke does.
import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';

import { isLoopback } from './config.ts';

export const DRAIN_KEEP_ANNOTATION = 'space.pplx.net/drain-keep';
export const DRAIN_KEEP_VALUE = 'console-loop';
// This run's id, picked once per run. Each claim's token adds a counter, so
// two runs, or two steps of one run, never write the same value.
const RUN_ID = randomBytes(6).toString('hex');
let claims = 0;

// The admin node fields the steps read.
export interface AdminNode {
  id: string;
  name: string | null;
  role: string;
  managed_by: string;
  drain_phase: string | null;
  drain_mode: string | null;
  drain_started_at: string | null;
  k8s_node_name: string | null;
  cordoned: boolean | null;
  sandboxes_active: number | null;
  sandboxes_local_snapshots: number | null;
  drain_engine?: string;
  drain?: ApiDrain;
  drain_last_error?: string | null;
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

  // Sets the override to a token new to this claim. Without `--overwrite` the
  // annotate fails when the key exists, so a claim never takes over another
  // run's, an earlier step's or an operator's override. An annotate that
  // reached the server but whose answer was lost reads back as this claim's
  // token, which counts as won. Returns the claim's token, or the value the
  // Node already carries.
  claimDrainKeep(name: string): { token: string } | { held: string } {
    const token = `${DRAIN_KEEP_VALUE}-${RUN_ID}-${++claims}`;
    try {
      this.kubectl('annotate', 'node', name, `${DRAIN_KEEP_ANNOTATION}=${token}`);
      return { token };
    } catch (error) {
      const held = this.drainKeep(name);
      if (held === token) return { token };
      if (held === null) throw error;
      return { held };
    }
  }

  // Removes the override only while it carries `token`; returns whether it
  // did. The read and the removal are two calls, so this can only race a run
  // that overwrote the token in between, which `claimDrainKeep` never does.
  releaseDrainKeep(name: string, token: string): boolean {
    if (this.drainKeep(name) !== token) return false;
    this.kubectl('annotate', 'node', name, `${DRAIN_KEEP_ANNOTATION}-`);
    return true;
  }

  unschedulable(name: string): boolean {
    return this.kubectl('get', 'node', name, '-o', 'jsonpath={.spec.unschedulable}').trim() === 'true';
  }
}

// The verdict a v2 drain preview's text or tag names, as the console shows
// it (`previewVerdict` in `ui/src/lib/drain-preview.ts`): "would finish",
// "would wait" or "would block"; null for text that names none, or more
// than one.
export function previewVerdictIn(text: string): 'finish' | 'wait' | 'block' | null {
  const named = [...new Set([...text.matchAll(/\bwould (finish|wait|block)\b/g)].map((m) => m[1]))];
  return named.length === 1 ? (named[0] as 'finish' | 'wait' | 'block') : null;
}

// The console's wait for one preview and its bound on the previews the nodes
// list runs at once (`DRAIN_PREVIEW_TIMEOUT_MS` and `PREVIEW_ALL_CONCURRENCY`
// in `ui/src/lib/drain-preview.ts`, which a unit test holds these to).
export const CONSOLE_PREVIEW_TIMEOUT_MS = 75_000;
export const CONSOLE_PREVIEW_CONCURRENCY = 4;

// What the preview steps allow the page beyond the console's own waits, to
// render an answer or a failure.
export const PREVIEW_WAIT_MARGIN_MS = 15_000;

// How long a step waits for one preview's answer: longer than the console
// waits for it, so the console settles it first.
export function previewWaitMs(): number {
  return CONSOLE_PREVIEW_TIMEOUT_MS + PREVIEW_WAIT_MARGIN_MS;
}

// How long a step waits for the nodes list to preview `workers` nodes: one
// console wait for each round of previews it runs at once.
export function previewAllWaitMs(workers: number): number {
  return Math.ceil(workers / CONSOLE_PREVIEW_CONCURRENCY) * CONSOLE_PREVIEW_TIMEOUT_MS + PREVIEW_WAIT_MARGIN_MS;
}

// A row of the nodes list's preview results as its verdict cell reads: a
// preview's verdict, or "error" for a preview that failed.
export type ResultVerdict = 'finish' | 'wait' | 'block' | 'error';

// The verdict a results row's verdict cell shows (`verdictTag` in
// `ui/src/components/drain-preview.tsx`), or null for a cell that shows none.
export function resultVerdictIn(text: string): ResultVerdict | null {
  return previewVerdictIn(text) ?? (/^error\b/.test(text.trim()) ? 'error' : null);
}

// Every row of a paginated table: `read` gives the rows of the page shown,
// and `next` turns to the following one, answering false on the last page.
// It reads at most `maxPages` pages; a table that has more is not complete.
export async function everyPage<T>(
  read: () => Promise<T[]>,
  next: () => Promise<boolean>,
  maxPages: number,
): Promise<{ rows: T[]; pages: number; complete: boolean }> {
  const rows: T[] = [];
  for (let pages = 1; ; pages += 1) {
    rows.push(...(await read()));
    if (!(await next())) return { rows, pages, complete: true };
    if (pages >= maxPages) return { rows, pages, complete: false };
  }
}

// What the nodes list's results, read from every page, get wrong against
// `expected`: each listed worker's verdict from the answer to the preview
// the page asked of it, or null for a worker it asked nothing. Empty when
// every worker has exactly one row, showing that verdict, and no other node
// has one.
export function previewResultsMismatches(
  expected: ReadonlyMap<string, ResultVerdict | null>,
  shown: readonly { nodeId: string | null; verdict: string }[],
): string[] {
  const mismatches: string[] = [];
  const seen = new Map<string, number>();
  for (const row of shown) {
    if (row.nodeId === null) {
      mismatches.push(`a row links to no node (${row.verdict || 'no verdict'})`);
      continue;
    }
    seen.set(row.nodeId, (seen.get(row.nodeId) ?? 0) + 1);
    if (!expected.has(row.nodeId)) {
      mismatches.push(`${row.nodeId} has a row but is not a listed worker`);
      continue;
    }
    const want = expected.get(row.nodeId) ?? null;
    const got = resultVerdictIn(row.verdict);
    if (want === null) mismatches.push(`${row.nodeId} has a row but the page asked no preview of it`);
    else if (got !== want) mismatches.push(`${row.nodeId} shows ${got ?? `"${row.verdict}"`}, but its preview answered ${want}`);
  }
  for (const [nodeId, rows] of seen) {
    if (rows > 1) mismatches.push(`${nodeId} has ${rows} rows`);
  }
  for (const nodeId of expected.keys()) {
    if (!seen.has(nodeId)) mismatches.push(`${nodeId} has no row on any page`);
  }
  return mismatches;
}

// ------------------------------------------------------- v2 drain progress

// A sandbox id as the panel and the API both name it, compared without its
// `sbx_` prefix and dashes.
export const bareSandboxId = (id: string): string => id.trim().toLowerCase().replace(/^sbx_/, '').replaceAll('-', '');

// The admin node response's `drain` (`AdminNodeDrain` in the admin
// protocol), as the steps read it. The node's seal isn't shown: a report
// stored under `generation` (`reported_at` set) is what the API shows of it.
export interface ApiDrain {
  generation: number;
  deadline: string | null;
  cancelling: boolean;
  fenced: boolean;
  reported_at: string | null;
  evacuated_grew_at: string | null;
  evacuated_total: string[];
  evacuated: { sandbox_id: string; artifact_kind: string; was_running: boolean; recorded: boolean }[];
  pending: { sandbox_id: string; wait: string; running: boolean; pool_member: boolean; reason: string | null; since: string | null }[];
  blocked: { reason: string }[];
}

// The wait a pending entry has while the drain keeps it running until its
// deadline.
export const HOLD_WAIT = 'hold';

// What the node page's Drain progress panel shows, read from its card.
export interface PanelRead {
  // Every tag in the card's header row, in order: the headline first.
  tags: string[];
  text: string;
  // Each pending row: its sandbox, whether it is labelled a warm-pool
  // member, its wait and whether it is marked running, and its State cell.
  pending: { sandbox: string; pool: boolean; wait: string; running: boolean; state: string }[];
  evacuated: { sandbox: string; artifact: string; wasRunning: string; controller: string }[];
}

// How long past its deadline a drain may evacuate nothing further before it
// is stalled (`STALL_BOUND_MS` in `ui/src/lib/drain-progress.ts`).
export const STALL_BOUND_MS = 15 * 60 * 1000;

const msOf = (iso: string | null | undefined): number | null => {
  if (!iso) return null;
  const parsed = Date.parse(iso);
  return Number.isNaN(parsed) ? null : parsed;
};

// The drain is past its deadline at `nowMs`; one without a deadline holds
// nothing.
const pastDeadline = (drain: Pick<ApiDrain, 'deadline'>, nowMs: number): boolean => {
  const deadline = msOf(drain.deadline);
  return deadline === null || nowMs >= deadline;
};

// The node's last report lists nothing pending or blocked, and every
// evacuation it lists is recorded. A drain with no report yet isn't.
export function drainFinished(drain: ApiDrain): boolean {
  return drain.reported_at !== null && drain.pending.length === 0 && drain.blocked.length === 0 && drain.evacuated.every((e) => e.recorded);
}

// The panel's counts: evacuated is every sandbox the controller recorded in
// this drain (`evacuated_total`, a released one included) with the ones the
// last report lists; pending each sandbox the report still lists, once.
export function drainCountsOf(drain: Pick<ApiDrain, 'evacuated' | 'evacuated_total' | 'pending' | 'blocked'>): {
  evacuated: number; recording: number; pending: number; held: number; poolMembers: number; total: number; blocked: number;
} {
  const evacuated = new Set([...drain.evacuated_total, ...drain.evacuated.map((e) => e.sandbox_id)].map(bareSandboxId));
  const waits = new Map<string, string[]>();
  const pool = new Set<string>();
  for (const entry of drain.pending) {
    const id = bareSandboxId(entry.sandbox_id);
    if (evacuated.has(id)) continue;
    waits.set(id, [...(waits.get(id) ?? []), entry.wait]);
    if (entry.pool_member) pool.add(id);
  }
  return {
    evacuated: evacuated.size,
    recording: drain.evacuated.filter((e) => !e.recorded).length,
    pending: waits.size,
    held: [...waits.values()].filter((list) => list.every((w) => w === HOLD_WAIT)).length,
    poolMembers: pool.size,
    total: evacuated.size + waits.size,
    blocked: drain.blocked.length,
  };
}

// When the drain became stalled, in ms, or null while it isn't: past its
// deadline, unfinished and not cancelling, with nothing evacuated for the
// stall bound since `evacuated_grew_at`.
export function stalledSince(drain: ApiDrain, nowMs: number): number | null {
  const grew = msOf(drain.evacuated_grew_at);
  if (grew === null || drain.cancelling || drainFinished(drain) || !pastDeadline(drain, nowMs)) return null;
  return nowMs >= grew + STALL_BOUND_MS ? grew + STALL_BOUND_MS : null;
}

// The stage of a v2 drain, from the API's answer at `nowMs`, as the panel's
// headline names it: cancelling, then finished, then either side of the
// deadline.
export type DrainStage = 'cancelling' | 'finished' | 'holding' | 'evacuating';

export function drainStageOf(drain: ApiDrain, nowMs: number): DrainStage {
  if (drain.cancelling) return 'cancelling';
  if (drainFinished(drain)) return 'finished';
  return pastDeadline(drain, nowMs) ? 'evacuating' : 'holding';
}

// What the panel's headline must say at each stage.
export const STAGE_HEADLINE: Record<DrainStage, RegExp> = {
  cancelling: /^Undrained: cancelling the drain on the node/,
  finished: /^Finished: everything on the node is durable elsewhere/,
  holding: /^deadline in \d/,
  evacuating: /^past the deadline/,
};

// How far the console's clock and the API's stamps may disagree before a
// reading on either side of a boundary counts as wrong.
const SLACK_MS = 6_000;

// What the panel gets wrong against `drain`, the answer the page rendered it
// from, at `nowMs`: its headline, fence and stall tags, its counts, and one
// row per pending entry and per evacuated sandbox, each as the node reported
// it. Empty when the panel matches.
export function panelMismatches(panel: PanelRead, drain: ApiDrain, nowMs: number): string[] {
  const out: string[] = [];
  const headline = panel.tags[0] ?? '';
  const stage = drainStageOf(drain, nowMs);
  const deadline = msOf(drain.deadline);
  const nearDeadline = deadline !== null && Math.abs(deadline - nowMs) < SLACK_MS;
  const stages: DrainStage[] = nearDeadline && (stage === 'holding' || stage === 'evacuating') ? ['holding', 'evacuating'] : [stage];
  if (!stages.some((s) => STAGE_HEADLINE[s].test(headline))) out.push(`the headline reads "${headline}", not the ${stage} stage`);
  const fencedTag = panel.tags.includes('instance fenced for termination');
  if (fencedTag !== drain.fenced) out.push(`the fence tag is ${fencedTag ? 'shown' : 'missing'} while fenced is ${drain.fenced}`);
  const stalledTag = panel.tags.some((tag) => /^stalled since /.test(tag));
  const stalled = stalledSince(drain, nowMs) !== null;
  const grew = msOf(drain.evacuated_grew_at);
  const nearStall = grew !== null && Math.abs(grew + STALL_BOUND_MS - nowMs) < SLACK_MS;
  if (stalledTag !== stalled && !nearStall && !nearDeadline) out.push(`the stall tag is ${stalledTag ? 'shown' : 'missing'} while the drain is ${stalled ? '' : 'not '}stalled`);
  const counts = drainCountsOf(drain);
  const progress = `${counts.evacuated}/${counts.total} evacuated`;
  if (!panel.text.includes(progress)) out.push(`the progress does not read "${progress}"`);
  if (counts.poolMembers && !panel.text.includes(`(${counts.poolMembers} warm-pool)`)) out.push(`the pending count does not say ${counts.poolMembers} are warm-pool members`);
  if (panel.pending.length !== drain.pending.length) out.push(`the pending table has ${panel.pending.length} rows, the report ${drain.pending.length}`);
  const holding = stage === 'holding' && !nearDeadline;
  for (const entry of drain.pending) {
    const id = bareSandboxId(entry.sandbox_id);
    const row = panel.pending.find((r) => bareSandboxId(r.sandbox) === id && r.wait === entry.wait);
    if (!row) {
      out.push(`no pending row for ${entry.sandbox_id} waiting ${entry.wait}`);
      continue;
    }
    if (row.running !== entry.running) out.push(`the row of ${entry.sandbox_id} is ${row.running ? '' : 'not '}marked running, but the report says running ${entry.running}`);
    if (row.pool !== entry.pool_member) out.push(`the row of ${entry.sandbox_id} is ${row.pool ? '' : 'not '}labelled a warm-pool member, but the report says pool_member ${entry.pool_member}`);
    if (entry.wait === HOLD_WAIT && holding && !/running until the deadline/.test(row.state)) out.push(`the hold of ${entry.sandbox_id} does not read "running until the deadline"`);
    if (entry.since && !/in this state since/.test(row.state)) out.push(`the row of ${entry.sandbox_id} does not say since when it is in its state`);
    // Displayed as given: compared with the report, never read.
    if (entry.reason && !row.state.includes(entry.reason)) out.push(`the row of ${entry.sandbox_id} does not show the node's reason`);
  }
  if (panel.evacuated.length !== drain.evacuated.length) out.push(`the evacuated table has ${panel.evacuated.length} rows, the report ${drain.evacuated.length}`);
  for (const entry of drain.evacuated) {
    const row = panel.evacuated.find((r) => bareSandboxId(r.sandbox) === bareSandboxId(entry.sandbox_id));
    if (!row) {
      out.push(`no evacuated row for ${entry.sandbox_id}`);
      continue;
    }
    const artifact = entry.artifact_kind === 'full' ? 'snapshot' : entry.artifact_kind;
    if (row.artifact !== artifact) out.push(`${entry.sandbox_id} reads artifact "${row.artifact}", not ${artifact}`);
    if ((row.wasRunning === 'yes') !== entry.was_running) out.push(`${entry.sandbox_id} reads was running "${row.wasRunning}", not ${entry.was_running}`);
    const controller = entry.recorded ? 'recorded' : 'recording';
    if (row.controller !== controller) out.push(`${entry.sandbox_id} reads "${row.controller}", not ${controller}`);
  }
  return out;
}

// What each sandbox's own row must be at a drain's report: held ones still
// running, an evacuated one `suspended` (a v2 drain never moves a stopped
// sandbox). Returns the
// mismatches against `statuses` (bare id → admin status).
export function rowMismatches(drain: ApiDrain, statuses: ReadonlyMap<string, string | null>): string[] {
  const out: string[] = [];
  const evacuated = new Set(drain.evacuated.map((e) => bareSandboxId(e.sandbox_id)));
  for (const entry of drain.evacuated) {
    const id = bareSandboxId(entry.sandbox_id);
    if (!statuses.has(id)) continue;
    if (statuses.get(id) !== 'suspended') out.push(`${entry.sandbox_id} is reported evacuated (${entry.artifact_kind}) but its row is ${statuses.get(id)}`);
  }
  for (const entry of drain.pending) {
    const id = bareSandboxId(entry.sandbox_id);
    if (evacuated.has(id) || !statuses.has(id) || entry.wait !== HOLD_WAIT) continue;
    if (statuses.get(id) !== 'started') out.push(`${entry.sandbox_id} is held but its row is ${statuses.get(id)}`);
  }
  return out;
}

// The controller stored the node's report under `generation`: what the API
// shows of a node answering the drain, since it doesn't show the seal.
export function reportedUnder(drain: ApiDrain | undefined, generation: number | undefined): boolean {
  return !!drain && generation !== undefined && drain.generation === generation && drain.reported_at !== null;
}
