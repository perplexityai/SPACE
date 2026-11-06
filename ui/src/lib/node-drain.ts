import type { AdminNodeResponse } from './types';

// What the node page's Drain section and the nodes list derive from the admin
// API's node, audit-event, node-sandbox and node-hold reads. Each function takes the
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

// A refused node action as the page shows it: one line per reason, and for
// an occupancy refusal the counts it gave.
export interface ActionRefusal {
  title: string;
  reasons: string[];
  counts?: { active: number; blockers: number };
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
    return { title: 'Terminate refused: the node still holds sandboxes', reasons, counts: { active: active ?? 0, blockers: blockers ?? 0 } };
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
  // An unreachable status was observed at the failed poll, not at an answer,
  // so it counts from `last_heartbeat` alone.
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
  const answered = node.status?.reachable === false ? null : observed;
  const beats = [answered, ms(node.last_heartbeat)].filter((at): at is number => at !== null);
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

// ------------------------------------------------------------- holds

// What the "What holds this node" panel derives from `GET /v1/nodes/{id}/holds`
// (`routes/admin/nodes/holds.rs`; the rows, reasons and terms are
// `pplx_space_sandbox_store::node_holds`).

export type HoldReason =
  | 'census_absent'
  | 'evacuating'
  | 'upload_in_flight'
  | 'stuck_transient'
  | 'transient'
  | 'local_only_snapshot'
  | 'resident'
  | 'termination_blocking';

export interface NodeHold {
  sandbox_id: string;
  lifecycle: string;
  lifecycle_changed_at: string;
  placement: 'active' | 'target' | 'both' | 'upload_source';
  reason: HoldReason;
  // From the row's terms alone: the API does not know the cluster-controller's
  // termination-blocking gate setting, so the drain is given both ways. With
  // that gate on, the drain counts the rows `terminate` counts.
  counted_by: { terminate: boolean; drain: boolean; drain_with_termination_blocking_gate: boolean };
  terms: { resident: boolean; drain_blocker: boolean; active_resident: boolean };
  facts: {
    census_absent: boolean;
    durable_snapshot: boolean;
    upload_in_flight: boolean;
    upload_started_at: string | null;
    evacuation_restore_target: string | null;
    restore_attempt_count: number | null;
    next_restore_attempt_at: string | null;
    past_stuck_threshold: boolean;
  };
}

export interface NodeHoldsResponse {
  node_id: string;
  residency_evidence: string;
  stuck_transient_after_secs: number;
  total: number;
  truncated: boolean;
  reason_counts: Partial<Record<HoldReason, number>>;
  // `active_resident` is the `:terminate` 409's `sandboxes_active` and
  // `drain_blocker` its `sandboxes_drain_blockers`.
  term_counts: { resident: number; drain_blocker: number; active_resident: number };
  holds: NodeHold[];
}

// ------------------------------------------------------------- hold groups

export type HoldGroupKey = 'needs_you' | 'handled_automatically' | 'waiting_on_users' | 'blocks_terminate' | 'not_counted';

export const HOLD_GROUPS: { key: HoldGroupKey; title: string }[] = [
  { key: 'needs_you', title: 'Needs you' },
  { key: 'handled_automatically', title: 'Handled automatically' },
  { key: 'waiting_on_users', title: 'Waiting on users' },
  { key: 'blocks_terminate', title: 'Blocks terminate' },
  { key: 'not_counted', title: 'Listed, not counted' },
];

// Where the node's drain is, as the hold groups and sentences read it. The
// drain moves a node's sandboxes, uploads their snapshots or fails them, as
// its mode says, only while the node is `draining` (a missing mode is gentle,
// as everywhere else). Once the node is `drained` or `terminating` it is
// `parked`: the drain no longer acts on what still holds it, though restores
// of sandboxes it already evacuated continue.
export type DrainStage = 'none' | 'gentle' | 'forced' | 'parked';

export function drainStage(node: { drain_phase: string | null; drain_mode?: string | null }): DrainStage {
  if (!node.drain_phase) return 'none';
  if (node.drain_phase !== 'draining') return 'parked';
  return node.drain_mode === 'forced' ? 'forced' : 'gentle';
}

const countedByNone = ({ counted_by: c }: Pick<NodeHold, 'counted_by'>) =>
  !c.terminate && !c.drain && !c.drain_with_termination_blocking_gate;

// Who acts on the row next, under the node's drain stage. A row no gate
// counts is only listed.
export function holdGroup(hold: Pick<NodeHold, 'reason' | 'lifecycle' | 'counted_by'>, stage: DrainStage): HoldGroupKey {
  if (countedByNone(hold)) return 'not_counted';
  switch (hold.reason) {
    case 'census_absent':
      return 'needs_you';
    case 'evacuating':
      // Only a forced drain moves a stopped sandbox.
      return hold.lifecycle === 'stopped' && stage !== 'forced' ? 'blocks_terminate' : 'handled_automatically';
    case 'upload_in_flight':
    case 'transient':
      return 'handled_automatically';
    case 'stuck_transient':
      return stage === 'forced' ? 'handled_automatically' : 'needs_you';
    case 'local_only_snapshot':
      return stage === 'parked' ? 'needs_you' : 'handled_automatically';
    case 'resident':
      if (stage === 'forced') return 'handled_automatically';
      return stage === 'parked' ? 'needs_you' : 'waiting_on_users';
    case 'termination_blocking':
      return 'blocks_terminate';
  }
}

export interface HoldGroup {
  key: HoldGroupKey;
  title: string;
  rows: NodeHold[];
}

// The non-empty groups in `HOLD_GROUPS` order, each keeping the API's row
// order (rows a term counts first, then by reason, then the oldest change
// first).
export function groupHolds(holds: NodeHold[], stage: DrainStage): HoldGroup[] {
  return HOLD_GROUPS.map(({ key, title }) => ({ key, title, rows: holds.filter((h) => holdGroup(h, stage) === key) }))
    .filter((group) => group.rows.length > 0);
}

// ------------------------------------------------------------- hold sentences

const elapsed = (value: string | null | undefined, nowMs: number): string | null => {
  const at = ms(value);
  return at === null ? null : formatDuration(nowMs - at);
};

// Why a created, stopped, deleting or quarantined row keeps the node from
// terminating, and who can clear it in this stage. `phase` is the node's
// `drain_phase`.
function terminationBlockingSentence(lifecycle: string, changed: string, stage: DrainStage, phase: string | null): string {
  if (lifecycle === 'deleting') return `It has been deleting here${changed}; it releases the node when its delete completes.`;
  if (lifecycle === 'quarantined') {
    return `It has been quarantined here${changed}. No drain moves it; the node cannot be terminated until the quarantine is released or the sandbox is deleted.`;
  }
  switch (stage) {
    case 'none':
    case 'gentle':
      return `It has been ${lifecycle} here${changed}. The node cannot be terminated while it is ${lifecycle} here; its owner can start it, which lets a forced drain move it, or delete it.`;
    case 'forced':
      return lifecycle === 'stopped'
        ? `It has been stopped here${changed}. Its owner cannot start it during a forced drain. The drain moves it only if the cluster evacuates stopped sandboxes; otherwise Undrain lets its owner start it, or deleting it lets the node terminate.`
        : `It has been ${lifecycle} here${changed}. Its owner cannot start it during a forced drain; Undrain lets its owner start it, or deleting it lets the node terminate.`;
    case 'parked':
      // The API refuses Undrain on a terminating node.
      return phase === 'drained'
        ? `It has been ${lifecycle} here${changed}. While the node is drained its owner cannot start it; Undrain lets its owner start it, or deleting it lets the node terminate.`
        : `It has been ${lifecycle} here${changed}. While the node is ${phase}, its owner cannot start it, so only deleting it lets the node terminate.`;
  }
}

function heldSentence(hold: NodeHold, nowMs: number, stuckAfterSecs: number, stage: DrainStage, phase: string | null): string {
  const since = elapsed(hold.lifecycle_changed_at, nowMs);
  const changed = since ? ` for ${since}` : '';
  switch (hold.reason) {
    case 'census_absent':
      return `The database says this sandbox is ${hold.lifecycle} here, but the node's latest sandbox report does not list it: a row with nothing behind it on the node.`;
    case 'evacuating': {
      const retry = hold.facts.restore_attempt_count
        ? ` Its restore has failed ${hold.facts.restore_attempt_count} time${hold.facts.restore_attempt_count === 1 ? '' : 's'}${hold.facts.next_restore_attempt_at ? `; the next attempt is due ${new Date(hold.facts.next_restore_attempt_at).toLocaleString()}` : ''}.`
        : '';
      const target = hold.facts.evacuation_restore_target ?? 'as it was';
      if (hold.lifecycle === 'restoring' && (hold.placement === 'active' || hold.placement === 'both')) {
        return `An evacuation is restoring this sandbox onto this node, to come back ${target}; it has been restoring${changed}.${retry}`;
      }
      if (hold.lifecycle === 'stopped' && stage !== 'forced') {
        return `A forced drain began moving this stopped sandbox and did not finish; only a forced drain resumes it. ${terminationBlockingSentence('stopped', changed, stage, phase)}`;
      }
      return `An evacuation is moving this sandbox off this node, to come back ${target} elsewhere; it has been ${hold.lifecycle}${changed}.${retry}`;
    }
    case 'upload_in_flight': {
      const started = elapsed(hold.facts.upload_started_at, nowMs);
      return `Its snapshot is uploading from this node${started ? `, started ${started} ago` : ''}; the node is held until the upload finishes.`;
    }
    case 'stuck_transient': {
      const next: Record<DrainStage, string> = {
        forced: ' The forced drain marks it failed.',
        gentle: ' A gentle drain keeps waiting; Forced drain marks it failed.',
        none: ' A forced drain would mark it failed.',
        parked: ` The drain no longer acts on it now that the node is ${phase}.`,
      };
      return `It has been ${hold.lifecycle}${changed}, longer than the drain's ${formatDuration(stuckAfterSecs * 1000)} limit.${next[stage]}`;
    }
    case 'transient':
      return `It has been ${hold.lifecycle}${changed}; a transition in progress that usually settles on its own.`;
    case 'local_only_snapshot': {
      const onDisk = "It is suspended and its only snapshot is on this node's disk";
      const draining = `${onDisk}; the drain uploads it, or marks it failed when the snapshot is missing, before the node can go.`;
      const sentences: Record<DrainStage, string> = {
        gentle: `${draining} A snapshot a gentle drain cannot upload stays listed until Forced drain.`,
        forced: draining,
        none: `${onDisk}; a drain must upload it before the node can go.`,
        parked: `${onDisk}: use Upload local snapshots before the node can go.`,
      };
      return sentences[stage];
    }
    case 'resident': {
      const here = `It has been ${hold.lifecycle} here${changed}.`;
      const sentences: Record<DrainStage, string> = {
        none: `${here} A forced drain would move it to another node; a gentle drain would wait for its owner to suspend or delete it.`,
        gentle: `${here} A gentle drain waits for its owner to suspend or delete it; Forced drain moves it to another node now.`,
        forced: `${here} The forced drain moves it to another node.`,
        parked: `${here} The drain stopped moving sandboxes when the node became ${phase}; its owner can still suspend or delete it${phase === 'drained' ? ', or Undrain and start a Forced drain to move it' : ''}.`,
      };
      return sentences[stage];
    }
    case 'termination_blocking':
      return terminationBlockingSentence(hold.lifecycle, changed, stage, phase);
  }
}

// One plain sentence for why the row holds the node, under the node's drain
// stage and `drain_phase`. A row no gate counts says so.
export function holdSentence(hold: NodeHold, nowMs: number, stuckAfterSecs: number, stage: DrainStage, phase: string | null): string {
  const sentence = heldSentence(hold, nowMs, stuckAfterSecs, stage, phase);
  return countedByNone(hold) ? `${sentence} Under node-census residency neither gate counts it.` : sentence;
}

// Which gates count the row, as the panel's column says it.
export function holdGatesLabel(hold: Pick<NodeHold, 'counted_by'>): string {
  const { terminate, drain, drain_with_termination_blocking_gate: gated } = hold.counted_by;
  if (drain && terminate) return 'drain and terminate';
  if (drain) return 'drain';
  if (terminate && gated) return 'terminate; drain only with the termination-blocking gate on';
  if (terminate) return 'terminate';
  return 'neither';
}

// ------------------------------------------------------------- holds header

export function residencyLabel(mode: string): { label: string; tooltip: string } {
  if (mode === 'node_census_aware') {
    return {
      label: 'node census',
      tooltip: "The gates trust the node's own sandbox report: a row the node's fresh, complete report omits (census absent) is not counted as occupying the node. Such rows are still listed here, and the gates still count one that a suspend, restore, local-only snapshot or upload pins to this node; each row's Counted by says which gates count it.",
    };
  }
  return {
    label: 'rows only',
    tooltip: "The gates count the database rows that place a sandbox on this node, whatever the node reports: a row the node's report omits (census absent) still holds the node.",
  };
}
