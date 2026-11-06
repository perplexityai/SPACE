import type { AdminNodeResponse } from './types';

// What the node page's Drain section and the nodes list derive from the admin
// API's node, audit-event and node-sandbox reads. Each function takes the
// reads' JSON as the API sends it and no clock of its own.

// ------------------------------------------------------------- confirmation

// What the operator types to confirm a forced drain or a terminate: the name
// the page shows, or the ID of a node without one.
export function confirmationPhrase(node: Pick<AdminNodeResponse, 'id' | 'name'>): string {
  return node.name?.trim() || node.id;
}

// Exact, apart from surrounding whitespace: a prefix or another case of the
// name is not a confirmation.
export function typedConfirmationMatches(typed: string, phrase: string): boolean {
  return phrase.length > 0 && typed.trim() === phrase;
}

// ------------------------------------------------------------- refusals

// A refused node action as the page shows it: one line per reason.
export interface ActionRefusal {
  title: string;
  reasons: string[];
}

const plural = (n: number, one: string, many: string) => `${n} ${n === 1 ? one : many}`;
const count = (value: unknown): number | null => (typeof value === 'number' && Number.isFinite(value) ? value : null);

// The 409 bodies of `POST /v1/nodes/{id}:terminate` (`routes/admin/nodes.rs`):
// the occupancy guard's `terminate_conflict_body` (`sandboxes_active`,
// `sandboxes_drain_blockers`, and its alias `sandboxes_local_snapshots`) and
// the phase check's `current_drain_phase` and `allowed_from`. Any other
// answer, or a 409 body of another shape, is null and the caller shows the
// API's message.
export function terminateRefusal(err: unknown): ActionRefusal | null {
  const { status, body } = (typeof err === 'object' && err !== null ? err : {}) as { status?: unknown; body?: unknown };
  if (status !== 409 || !body || typeof body !== 'object') return null;
  const b = body as Record<string, unknown>;
  const reasons: string[] = [];
  const active = count(b.sandboxes_active);
  const blockers = count(b.sandboxes_drain_blockers) ?? count(b.sandboxes_local_snapshots);
  if (active !== null || blockers !== null) {
    if (active) {
      reasons.push(`${plural(active, 'sandbox still occupies', 'sandboxes still occupy')} this node; terminating would destroy ${active === 1 ? 'it' : 'them'}.`);
    }
    if (blockers) {
      reasons.push(
        `${plural(blockers, 'sandbox is', 'sandboxes are')} held for this node: a suspended sandbox whose only snapshot is on this node, a suspend or restore pinned to it, or an upload still in flight from it. For suspended sandboxes whose only snapshot is here, use Upload local snapshots on the node page; let any suspend, restore or upload in flight finish; then terminate again.`,
      );
    }
    if (!reasons.length) reasons.push('The API counted sandboxes on this node, but reported none in the refusal.');
    return { title: 'Terminate refused: the node still holds sandboxes', reasons };
  }
  if ('current_drain_phase' in b || Array.isArray(b.allowed_from)) {
    const current = typeof b.current_drain_phase === 'string' ? b.current_drain_phase : null;
    const allowed = Array.isArray(b.allowed_from) ? b.allowed_from.filter((v): v is string => typeof v === 'string') : [];
    reasons.push(current ? `The node is ${current}.` : 'The node is not draining.');
    if (allowed.length) reasons.push(`Terminate is accepted only from: ${allowed.join(', ')}.`);
    return { title: 'Terminate refused: the node is not drained', reasons };
  }
  return null;
}

// ------------------------------------------------------------- heartbeat

export type HeartbeatState = 'fresh' | 'stale' | 'no-status';

export interface Heartbeat {
  state: HeartbeatState;
  // Since the newer of the status snapshot's `observed_at` and
  // `last_heartbeat`, as the nodes list's "seen" age; null when neither is set.
  ageMs: number | null;
  // The node's own window, `stale_after - observed_at`; null without a status.
  windowMs: number | null;
}

const ms = (value: string | null | undefined): number | null => {
  if (!value) return null;
  const at = new Date(value).getTime();
  return Number.isFinite(at) ? at : null;
};

// Stale is the rule behind the API's own `status.stale` flag
// (`admin_node_status` in `routes/admin/nodes.rs`), not a client threshold:
// a node counts as fresh until its status snapshot's `stale_after`, which
// the cluster sets from the interval the node reports (two intervals plus
// 30 s, at most 10 minutes; `node_status_freshness.rs` in the node API
// client). Direct placement reads the same column, and so do the drain
// gates when the node-census residency discount is on
// (`node_heartbeat_fresh` in `sql/sandbox_residency.rs`); both also require
// `reachable`. Kubernetes placement does not read it. Compared with `nowMs`
// rather than the snapshot's `stale` flag, which is as old as the read. A
// node with no status snapshot reads `no-status`.
export function heartbeat(
  node: Pick<AdminNodeResponse, 'last_heartbeat' | 'status'>,
  nowMs: number,
): Heartbeat {
  const staleAfter = ms(node.status?.stale_after);
  const observed = ms(node.status?.observed_at);
  const beats = [observed, ms(node.last_heartbeat)].filter((at): at is number => at !== null);
  const ageMs = beats.length ? Math.max(0, nowMs - Math.max(...beats)) : null;
  if (staleAfter === null) return { state: 'no-status', ageMs, windowMs: null };
  return {
    state: nowMs < staleAfter ? 'fresh' : 'stale',
    ageMs,
    windowMs: observed === null ? null : Math.max(0, staleAfter - observed),
  };
}

// "45s", "3m 12s", "2h 5m", "3d 4h".
export function formatDuration(totalMs: number): string {
  const s = Math.max(0, Math.floor(totalMs / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

// How long the drain has run, from `drain_started_at`; null without one.
export function drainElapsedMs(node: Pick<AdminNodeResponse, 'drain_started_at'>, nowMs: number): number | null {
  const started = ms(node.drain_started_at);
  return started === null ? null : Math.max(0, nowMs - started);
}

// ------------------------------------------------------------- Kubernetes Node

// `cordoned` is the only field that comes from the API's Kubernetes Node
// cache. It is set when the cache holds the node's Node, so `present` is
// certain; its absence also covers a cache that has not listed yet and the
// Direct substrate, so absence is `unknown`, never "missing".
export function kubeNodePresence(node: Pick<AdminNodeResponse, 'k8s_node_name' | 'cordoned'>): 'present' | 'unknown' | 'none' {
  if (!node.k8s_node_name) return 'none';
  return typeof node.cordoned === 'boolean' ? 'present' : 'unknown';
}

// ------------------------------------------------------------- drain history

// `GET /v1/audit-events` rows, as far as the history reads them. The endpoint
// has no filters: it returns the newest `limit` admin requests (at most 250)
// across the whole admin API, and the history filters them by path here.
export interface AuditEventRow {
  id: string;
  actor_email: string | null;
  actor_user_id: string | null;
  method: string;
  path: string;
  status_code: number;
  success: boolean;
  created_at: string;
}

export const AUDIT_EVENT_MAX_LIMIT = 250;

// The node actions the admin API dispatches from `POST /v1/nodes/{id}:{action}`.
export const NODE_ACTIONS = ['drain', 'undrain', 'terminate', 'cordon', 'uncordon', 'force-upload-local-snapshots'] as const;

export interface DrainHistoryEntry {
  id: string;
  action: string;
  actor: string;
  actorUserId: string | null;
  at: string;
  statusCode: number;
  success: boolean;
}

// The recorded path is the request's path under the admin API's version
// mount, so only its end is matched: `/nodes/{id}:{action}`, with the node's
// ID exactly.
export function drainHistory(events: AuditEventRow[], nodeId: string): DrainHistoryEntry[] {
  const suffix = `/nodes/${nodeId.toLowerCase()}:`;
  const entries: DrainHistoryEntry[] = [];
  for (const event of events) {
    if (event.method !== 'POST') continue;
    const path = event.path.toLowerCase();
    const at = path.lastIndexOf(suffix);
    if (at < 0) continue;
    const action = path.slice(at + suffix.length);
    if (!(NODE_ACTIONS as readonly string[]).includes(action)) continue;
    entries.push({
      id: event.id,
      action,
      actor: event.actor_email || event.actor_user_id || 'unknown',
      actorUserId: event.actor_user_id,
      at: event.created_at,
      statusCode: event.status_code,
      success: event.success,
    });
  }
  return entries.sort((a, b) => (ms(b.at) ?? 0) - (ms(a.at) ?? 0));
}

// A full page means older requests, this node's among them, may exist beyond
// it.
export function historyMayBeTruncated(loaded: number, limit: number): boolean {
  return loaded >= limit;
}

// ------------------------------------------------------------- sandboxes

// Lifecycle counts of the loaded page of `GET /v1/nodes/{id}/sandboxes`, the
// most common first, then by name.
export function lifecycleCounts(sandboxes: { lifecycle: string }[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const { lifecycle } of sandboxes) counts.set(lifecycle, (counts.get(lifecycle) ?? 0) + 1);
  return [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}
