// What the node page and the nodes list show of a v2 drain in flight: the
// admin node response's `drain` (`AdminNodeDrain` in the admin protocol),
// which the cluster controller stores from the node's last report. The
// deadline and the controller's stamps are by the database clock, a pending
// entry's `since` by the node's; the console compares them with its own.

import type { AdminNodeDrain, AdminNodeDrainEvacuated, AdminNodeDrainPending, AdminNodeResponse } from './types';

// The wait the node reports for a running sandbox the drain keeps running
// until its deadline.
export const HOLD_WAIT = 'hold';

// How long past its deadline a drain may evacuate nothing further before the
// cluster controller finds it stalled (its stall bound).
export const STALL_BOUND_MS = 15 * 60 * 1000;

// How a v2 drain stands, for its headline:
// - `cancelling`: undrained, until the controller has cancelled it on the node;
// - `finished`: the node's last report lists nothing pending or blocked, and
//   every evacuation is recorded;
// - `holding`: before the deadline, running sandboxes keep running;
// - `evacuating`: from the deadline on, the node suspends the sandboxes it
//   held too.
export type DrainProgressState = 'cancelling' | 'finished' | 'holding' | 'evacuating';

export interface DrainCounts {
  // Sandboxes durable elsewhere: every one the controller recorded as
  // evacuated in this drain, a released one included, and the ones the
  // node's last report lists evacuated.
  evacuated: number;
  // Of those, the ones the controller is still recording.
  recording: number;
  // Sandboxes the node still lists pending, each counted once.
  pending: number;
  // Of those, the running ones it holds until the deadline, with nothing
  // else under way for them.
  held: number;
  // Of the pending ones, warm-pool members rather than user sandboxes.
  poolMembers: number;
  // Every sandbox the drain has to move: evacuated or pending.
  total: number;
  // Node-wide problems that keep the drain from finishing.
  blocked: number;
}

export interface DrainProgress {
  state: DrainProgressState;
  // `deadline in 8m 3s`, `evacuating`, and the like.
  headline: string;
  // Milliseconds until the deadline, never below zero; null without one.
  deadlineInMs: number | null;
  // Since when the drain is stalled, in ms: past its deadline, unfinished,
  // with nothing evacuated for the stall bound since `evacuated_grew_at`;
  // null while it isn't.
  stalledSinceMs: number | null;
  counts: DrainCounts;
}

export function isV2Drain(node: Pick<AdminNodeResponse, 'drain_engine'>): boolean {
  return node.drain_engine === 'v2';
}

// An engine this console doesn't know, as the node reports it, or null for
// v1 (absent or `v1`) and v2. The page names it rather than reading the
// drain as a v1 one.
export function unknownEngine(node: Pick<AdminNodeResponse, 'drain_engine'>): string | null {
  const engine = node.drain_engine;
  if (engine === undefined || engine === null || engine === 'v1' || engine === 'v2') return null;
  return String(engine);
}

const text = (value: unknown): string | null => (typeof value === 'string' ? value : null);
const entries = (value: unknown): Record<string, unknown>[] =>
  (Array.isArray(value) ? value.filter((entry): entry is Record<string, unknown> => !!entry && typeof entry === 'object') : []);

// The admin node response's `drain`, read defensively: what the panel and
// the nodes list draw from, or null when there is none to draw. A field of
// another type than the protocol's reads as absent, and an entry without a
// sandbox id is left out, so an API newer or older than this console can't
// crash the page.
export function readDrain(raw: unknown): AdminNodeDrain | null {
  if (!raw || typeof raw !== 'object') return null;
  const d = raw as Record<string, unknown>;
  const generation = typeof d.generation === 'number' && Number.isFinite(d.generation) ? d.generation : null;
  if (generation === null) return null;
  return {
    generation,
    deadline: text(d.deadline),
    cancelling: d.cancelling === true,
    fenced: d.fenced === true,
    reported_at: text(d.reported_at),
    evacuated_grew_at: text(d.evacuated_grew_at),
    evacuated_total: Array.isArray(d.evacuated_total) ? d.evacuated_total.filter((id): id is string => typeof id === 'string') : [],
    evacuated: entries(d.evacuated)
      .filter((entry) => typeof entry.sandbox_id === 'string')
      .map((entry) => ({
        sandbox_id: entry.sandbox_id as string,
        artifact_kind: text(entry.artifact_kind) ?? 'unknown',
        was_running: entry.was_running === true,
        recorded: entry.recorded === true,
      })),
    pending: entries(d.pending)
      .filter((entry) => typeof entry.sandbox_id === 'string')
      .map((entry) => ({
        sandbox_id: entry.sandbox_id as string,
        wait: text(entry.wait) ?? 'unspecified',
        running: entry.running === true,
        pool_member: entry.pool_member === true,
        reason: text(entry.reason),
        since: text(entry.since),
      })),
    blocked: entries(d.blocked).map((entry) => ({ reason: text(entry.reason) ?? 'unknown' })),
  };
}

// `formatDuration` in `node-drain.ts`, which this module can't import: the
// console's tests load each module on its own.
function formatDuration(totalMs: number): string {
  const s = Math.max(0, Math.floor(totalMs / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

function ms(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const parsed = Date.parse(iso);
  return Number.isNaN(parsed) ? null : parsed;
}

// The drain is past its deadline by `nowMs`. A drain without a deadline
// holds nothing.
function deadlinePassed(drain: Pick<AdminNodeDrain, 'deadline'>, nowMs: number): boolean {
  const deadline = ms(drain.deadline);
  return deadline === null || nowMs >= deadline;
}

export function drainCounts(
  drain: Pick<AdminNodeDrain, 'evacuated' | 'evacuated_total' | 'pending' | 'blocked'>,
): DrainCounts {
  const evacuated = new Set([...drain.evacuated_total, ...drain.evacuated.map((entry) => entry.sandbox_id)]);
  // A sandbox the node lists more than once is one pending sandbox, and is
  // held only if every entry for it is a hold.
  const waits = new Map<string, string[]>();
  const poolMembers = new Set<string>();
  for (const entry of drain.pending) {
    if (evacuated.has(entry.sandbox_id)) continue;
    waits.set(entry.sandbox_id, [...(waits.get(entry.sandbox_id) ?? []), entry.wait]);
    if (entry.pool_member) poolMembers.add(entry.sandbox_id);
  }
  const held = [...waits.values()].filter((list) => list.every((wait) => wait === HOLD_WAIT)).length;
  return {
    evacuated: evacuated.size,
    recording: drain.evacuated.filter((entry) => !entry.recorded).length,
    pending: waits.size,
    held,
    poolMembers: poolMembers.size,
    total: evacuated.size + waits.size,
    blocked: drain.blocked.length,
  };
}

// The node's last report lists nothing pending or blocked, and every
// evacuation it lists is recorded. A drain with no report yet isn't.
function finished(drain: AdminNodeDrain): boolean {
  return (
    drain.reported_at !== null &&
    drain.pending.length === 0 &&
    drain.blocked.length === 0 &&
    drain.evacuated.every((entry) => entry.recorded)
  );
}

export function drainProgress(drain: AdminNodeDrain, nowMs: number): DrainProgress {
  const deadline = ms(drain.deadline);
  const deadlineInMs = deadline === null ? null : Math.max(0, deadline - nowMs);
  const passed = deadlinePassed(drain, nowMs);
  const done = finished(drain);
  let state: DrainProgressState;
  let headline: string;
  if (drain.cancelling) {
    state = 'cancelling';
    headline = 'Undrained: cancelling the drain on the node';
  } else if (done) {
    state = 'finished';
    headline = 'Finished: everything on the node is durable elsewhere';
  } else if (!passed) {
    state = 'holding';
    headline = `deadline in ${formatDuration(deadlineInMs ?? 0)}`;
  } else {
    state = 'evacuating';
    headline = 'past the deadline: the node suspends the sandboxes it held';
  }
  // The stall clock starts at the later of the last growth and the deadline.
  // The controller's stamp also includes when its first evacuating report was
  // stored, so the two agree once it stores one; a hold isn't a stall.
  const grew = ms(drain.evacuated_grew_at);
  const since = grew === null ? null : deadline === null ? grew : Math.max(grew, deadline);
  const stalledAt = since === null ? null : since + STALL_BOUND_MS;
  const stalled = !drain.cancelling && !done && passed && stalledAt !== null && nowMs >= stalledAt;
  return {
    state,
    headline,
    deadlineInMs,
    stalledSinceMs: stalled ? stalledAt : null,
    counts: drainCounts(drain),
  };
}

// A short duration for the nodes list: `45s`, `8m`, `3h 5m`, `2d 4h`.
export function compactDuration(totalMs: number): string {
  const s = Math.max(0, Math.floor(totalMs / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return m % 60 ? `${h}h ${m % 60}m` : `${h}h`;
  const d = Math.floor(h / 24);
  return h % 24 ? `${d}d ${h % 24}h` : `${d}d`;
}

// The nodes list's one-line view of a v2 drain: `12/20 evacuated · deadline 8m`.
export function compactDrainCell(drain: AdminNodeDrain, nowMs: number): string {
  const progress = drainProgress(drain, nowMs);
  const { evacuated, total } = progress.counts;
  const parts = [`${evacuated}/${total} evacuated`];
  switch (progress.state) {
    case 'cancelling':
      return 'cancelling on the node';
    case 'finished':
      parts.push('finished');
      break;
    case 'holding':
      parts.push(`deadline ${compactDuration(progress.deadlineInMs ?? 0)}`);
      break;
    case 'evacuating':
      parts.push('past the deadline');
      break;
  }
  if (progress.stalledSinceMs !== null) parts.push('stalled');
  return parts.join(' · ');
}

export interface PendingRow {
  key: string;
  sandbox_id: string;
  wait: string;
  running: boolean;
  // A warm-pool member, not a user's sandbox.
  poolMember: boolean;
  // The node's reason, displayed as given.
  reason: string | null;
  // When the row entered its current state, in ms by the node's clock.
  sinceMs: number | null;
  // `5m 2s`: how long it has been in that state, by the console's clock.
  inStateFor: string | null;
  // `running until the deadline` for a hold before it.
  note: string | null;
}

export function pendingRows(drain: AdminNodeDrain, nowMs: number): PendingRow[] {
  const holding = !drain.cancelling && !deadlinePassed(drain, nowMs);
  return drain.pending.map((entry: AdminNodeDrainPending, index) => {
    const sinceMs = ms(entry.since);
    return {
      key: `${entry.sandbox_id}:${entry.wait}:${index}`,
      sandbox_id: entry.sandbox_id,
      wait: entry.wait,
      running: entry.running,
      poolMember: entry.pool_member,
      reason: entry.reason,
      sinceMs,
      inStateFor: sinceMs === null ? null : formatDuration(Math.max(0, nowMs - sinceMs)),
      note: entry.wait === HOLD_WAIT && holding ? 'running until the deadline' : null,
    };
  });
}

// What a pending row is, for its label: a user's sandbox, or a warm-pool
// member the node holds for the pool.
export function pendingKind(row: Pick<PendingRow, 'poolMember'>): 'warm-pool member' | 'sandbox' {
  return row.poolMember ? 'warm-pool member' : 'sandbox';
}

export interface EvacuatedRow {
  sandbox_id: string;
  // `snapshot` for a suspended sandbox's full snapshot, `disk` for a stopped
  // one's disk.
  kind: string;
  wasRunning: boolean;
  status: 'recorded' | 'recording';
}

export function evacuatedRows(drain: Pick<AdminNodeDrain, 'evacuated'>): EvacuatedRow[] {
  return drain.evacuated.map((entry: AdminNodeDrainEvacuated) => ({
    sandbox_id: entry.sandbox_id,
    kind: entry.artifact_kind === 'full' ? 'snapshot' : entry.artifact_kind,
    wasRunning: entry.was_running,
    status: entry.recorded ? 'recorded' : 'recording',
  }));
}

// ------------------------------------------------------------- start and shorten

// Why a shorten to `deadlineMs` would be refused, or null: the API moves a
// v2 drain's deadline only earlier, so a hold that ends the drain later than
// `currentDeadline` is refused with a 409. A drain without a deadline
// already holds nothing.
export function shortenLengthens(deadlineMs: number | null, currentDeadline: string | null): string | null {
  const current = ms(currentDeadline);
  if (deadlineMs === null) return null;
  if (current === null) return 'This drain has no deadline to move: it already holds nothing.';
  return deadlineMs > current
    ? `That hold ends the drain after its deadline, ${new Date(current).toLocaleString()}; a deadline only moves earlier.`
    : null;
}

// The holds the console offers for a v2 drain, beside a custom value:
// `hold_seconds` on `:drain {engine: v2}` (`routes/admin/nodes/drain_v2.rs`).
export const HOLD_PRESETS: { label: string; seconds: number }[] = [
  { label: 'Now', seconds: 0 },
  { label: '10 minutes', seconds: 10 * 60 },
  { label: '1 hour', seconds: 60 * 60 },
];

// The longest hold the API takes, a year (`MAX_HOLD_SECONDS`).
export const MAX_HOLD_SECONDS = 365 * 24 * 60 * 60;

// A custom hold typed in seconds: a whole number from 0 to a year, or null.
export function parseHoldSeconds(input: string): number | null {
  const trimmed = input.trim();
  if (!/^\d+$/.test(trimmed)) return null;
  const seconds = Number(trimmed);
  return Number.isSafeInteger(seconds) && seconds <= MAX_HOLD_SECONDS ? seconds : null;
}

// The `:drain` body that starts a v2 drain: start-only, so it never repeats
// or escalates a drain somebody else started meanwhile.
export function v2StartBody(holdSeconds: number): { engine: 'v2'; hold_seconds: number; require_undrained: true } {
  return { engine: 'v2', hold_seconds: holdSeconds, require_undrained: true };
}

// The `:drain` body that moves the hold of the v2 drain in flight earlier.
// It names no engine, so the API reads it as a repeat of the v2 drain in
// flight and never starts one: on a node with no drain it is refused (a 409
// with drain v2 on, a 400 with it off).
export function v2HoldBody(holdSeconds: number): { hold_seconds: number } {
  return { hold_seconds: holdSeconds };
}

// The body of a typed hold action: a start's or a shorten's.
export function v2DrainBodyFor(hold: 'start' | 'shorten', holdSeconds: number): object {
  return hold === 'start' ? v2StartBody(holdSeconds) : v2HoldBody(holdSeconds);
}

// Whether a node action's body is a v2 drain's: one that names the engine,
// or a hold body.
export function isV2DrainBody(body: unknown): boolean {
  if (!body || typeof body !== 'object') return false;
  const b = body as Record<string, unknown>;
  return b.engine === 'v2' || ('hold_seconds' in b && !('engine' in b));
}

// The node's drain as a confirmation dialog saw it when it opened.
export interface DrainSeen {
  engine: string | null;
  phase: string | null;
  generation: number | null;
}

export function drainSeen(node: Pick<AdminNodeResponse, 'drain_engine' | 'drain_phase'> & { drain?: Pick<AdminNodeDrain, 'generation'> | null }): DrainSeen {
  return { engine: node.drain_engine ?? null, phase: node.drain_phase ?? null, generation: node.drain?.generation ?? null };
}

// Whether the node's drain changed since a dialog opened on it: its engine,
// phase or generation. A confirm on a changed drain could mean something the
// operator didn't read, so the dialog closes instead.
export function drainChanged(before: DrainSeen, now: DrainSeen): boolean {
  return before.engine !== now.engine || before.phase !== now.phase || before.generation !== now.generation;
}

export const DRAIN_CHANGED = "The node's drain changed while the dialog was open; reopen it.";

// Whether a typed confirmation closes unsent ([`DRAIN_CHANGED`]) because the
// node's drain changed since it opened: only for a v2 action (one with a
// hold, a v2 `:drain` body, or opened on a v2 drain, such as its Terminate).
// A v1 action's confirm sends as it always has.
export function closesOnDrainChange(item: { hold?: string; body?: unknown }, before: DrainSeen, now: DrainSeen): boolean {
  const v2 = item.hold !== undefined || isV2DrainBody(item.body) || before.engine === 'v2';
  return v2 && drainChanged(before, now);
}

// The deadline a hold gives, in ms: a start's counts from now; a shorten's
// from the drain's start in whole seconds, as the API stores it
// (`FLOOR(EXTRACT(EPOCH FROM drain_started_at))` plus the hold), so it may
// already have passed. Null for a shorten without a start.
export function holdDeadlineMs(holdSeconds: number, startedAt: string | null, shorten: boolean, nowMs: number): number | null {
  if (!shorten) return nowMs + holdSeconds * 1000;
  const started = ms(startedAt);
  return started === null ? null : Math.floor(started / 1000) * 1000 + holdSeconds * 1000;
}

// A refused node action as the node page shows it; the same shape as
// `ActionRefusal` in `node-drain.ts`.
export interface V2DrainRefusal {
  title: string;
  reasons: string[];
  // Sandboxes the refusal names, each linked to its page.
  sandboxIds?: string[];
}

// The refusals of a v2 `:drain` the page explains, from the API's answer
// (`routes/admin/nodes/drain_v2.rs`): the cap's 409 (`code`
// `drain_v2_evacuation_cap`, with `Retry-After`, `evacuating` and
// `max_evacuating`), the stale heartbeat's 409 (`drain_v2_node_heartbeat_stale`,
// with `Retry-After`), the up-front stopped-sandbox 409
// (`drain_v2_stopped_sandboxes`, with `stopped` and `stopped_sandbox_ids`),
// the 409 while an earlier v2 drain's undrain is still finishing,
// the 400 for `mode` on a v2 drain, the 400 while v2 drains are switched off,
// and the 409 for a hold that would end the drain later. The 400s carry no
// code, so their messages are matched. Any other answer is null, and the
// caller shows the API's message.
export function v2DrainRefusal(err: unknown): V2DrainRefusal | null {
  const { status, body, message, retryAfter } = (typeof err === 'object' && err !== null ? err : {}) as {
    status?: unknown; body?: unknown; message?: unknown; retryAfter?: unknown;
  };
  const b = body && typeof body === 'object' ? (body as Record<string, unknown>) : {};
  const said = typeof b.error === 'string' ? b.error : typeof message === 'string' ? message : '';
  const count = (value: unknown) => (typeof value === 'number' && Number.isFinite(value) ? value : null);
  if (status === 409 && b.code === 'drain_v2_evacuation_cap') {
    const evacuating = count(b.evacuating);
    const max = count(b.max_evacuating);
    const reasons = [said];
    if (evacuating !== null && max !== null) reasons.push(`${evacuating} of ${max} evacuation slots are held.`);
    const wait = typeof retryAfter === 'string' && /^\d+$/.test(retryAfter.trim()) ? Number(retryAfter.trim()) : null;
    if (wait !== null) reasons.push(`Retry after ${wait} s.`);
    return { title: 'v2 drain refused: every evacuation slot is held', reasons: reasons.filter(Boolean) };
  }
  if (status === 409 && b.code === 'drain_v2_stopped_sandboxes') {
    const stopped = count(b.stopped);
    const ids = Array.isArray(b.stopped_sandbox_ids)
      ? b.stopped_sandbox_ids.filter((id): id is string => typeof id === 'string').slice(0, 20)
      : [];
    const reasons = [said];
    if (stopped !== null) {
      reasons.push(
        `${stopped} stopped ${stopped === 1 ? 'sandbox' : 'sandboxes'}${ids.length && ids.length < stopped ? `, the first ${ids.length} listed` : ''}.`,
      );
    }
    reasons.push('A v2 drain doesn\'t move a stopped sandbox: delete them, or start them, then drain again.');
    return { title: 'v2 drain refused: the node holds stopped sandboxes', reasons: reasons.filter(Boolean), sandboxIds: ids };
  }
  if (status === 409 && b.code === 'drain_v2_node_heartbeat_stale') {
    const reasons = [said];
    const wait = typeof retryAfter === 'string' && /^\d+$/.test(retryAfter.trim()) ? Number(retryAfter.trim()) : null;
    if (wait !== null) reasons.push(`Retry after ${wait} s.`);
    return { title: "v2 drain refused: the node's heartbeat is stale", reasons: reasons.filter(Boolean) };
  }
  if (status === 409 && said.startsWith('undrain still finishing')) {
    return { title: "Drain refused: the last v2 drain's undrain is still finishing", reasons: [said] };
  }
  if (status === 409 && said.startsWith("a v2 drain's deadline only moves earlier")) {
    return { title: 'Shorten refused: the hold would end the drain later', reasons: [said] };
  }
  if (status === 400 && said.startsWith('a v2 drain takes no mode')) {
    return { title: 'v2 drain refused: a v2 drain takes no mode', reasons: [said] };
  }
  if (status === 400 && said === 'drain engine v2 is not enabled on this cluster') {
    return {
      title: 'v2 drains are switched off on this cluster',
      reasons: [said, 'The API starts v2 drains only with SPACE_DRAIN_V2_ENABLED on.'],
    };
  }
  return null;
}

// The refusal of a hold body (`v2HoldBody`) on a node with no drain left to
// shorten: drain v2 on, the API reads it as a v1 `:drain` and refuses it (409
// `drain_v1_refused_while_v2_enabled`); off, a hold on anything but a v2
// drain is a 400. Either way the v2 drain the dialog was for has ended. Null
// for any other answer.
export function v2HoldRefusal(err: unknown): V2DrainRefusal | null {
  const { status, body, message } = (typeof err === 'object' && err !== null ? err : {}) as {
    status?: unknown; body?: unknown; message?: unknown;
  };
  const b = body && typeof body === 'object' ? (body as Record<string, unknown>) : {};
  const said = typeof b.error === 'string' ? b.error : typeof message === 'string' ? message : '';
  const ended =
    (status === 409 && b.code === 'drain_v1_refused_while_v2_enabled') ||
    (status === 400 && said.startsWith('hold_seconds is for a drain with engine v2 only'));
  return ended ? { title: 'The v2 drain this was for has ended', reasons: [said].filter(Boolean) } : null;
}

// What a v2 node's holds panel says in place of the v1 drain's action
// sentences, which would tell the operator to force the drain or to start a
// stopped sandbox: the v2 drain's progress panel says what it waits on.
export const V2_HOLD_SENTENCE =
  "The v2 drain's progress panel says what it waits on; a stopped sandbox here holds the drain until it is deleted.";

// The holds panel's Why for a row of `node`: a v2 drain's neutral sentence,
// or the v1 drain's own (`v1`, `holdSentence` in `node-drain.ts`).
export function holdWhy(node: Pick<AdminNodeResponse, 'drain_engine'>, v1: () => string): string {
  return isV2Drain(node) ? V2_HOLD_SENTENCE : v1();
}

// The prefix of the `drain_last_error` the cluster controller writes when a
// terminating v2 node's fresh check refuses the fence, and it returns the
// node to `drained`.
export const TERMINATE_REFUSED_PREFIX = 'terminate refused:';

// Why the last `:terminate` of a v2 node didn't terminate it, when it didn't:
// `:terminate` moves a drained node to terminating at once, and the cluster
// controller's terminating tick makes one fresh `DrainNode` call; a failed
// call (an unreadable seal among them) or a report with anything pending or
// blocked returns the node to `drained`, with why in `drain_last_error`. Null
// otherwise. The node's own words are displayed, not read.
export function controllerTerminateRefusal(
  node: Pick<AdminNodeResponse, 'drain_engine' | 'drain_phase' | 'drain_last_error'>,
): V2DrainRefusal | null {
  const error = node.drain_last_error;
  if (!isV2Drain(node) || node.drain_phase !== 'drained' || !error?.startsWith(TERMINATE_REFUSED_PREFIX)) return null;
  return {
    title: 'The last terminate was refused: the node is back in drained',
    reasons: [error, 'Resolve what it names, then terminate again.'],
  };
}
