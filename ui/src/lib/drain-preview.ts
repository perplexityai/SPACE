// What the node page and the nodes list show of a v2 drain preview:
// `GET /v1/nodes/{id}/drain-v2-preview` (`routes/admin/nodes/drain_preview.rs`).
// The node answers what a forced v2 drain would find on it now, from the drain
// call's scans and planner, and changes nothing: it seals nothing, starts
// nothing and writes nothing.

export const PREVIEW_CHANGES_NOTHING =
  "A preview changes nothing on the node: it seals nothing, starts no suspend or upload, and writes nothing. It reports what a forced v2 drain would find on this node now. It plans as a forced drain. A gentle drain holds each started sandbox whose suspend is neither refused nor already running: it lists it pending with the action hold, whatever this preview lists for it (suspend, queued, disk, or blocked after repeated suspend failures), and the window slots that frees can start other work sooner. A row that has already sat in a transition the drain doesn't drive for over 15 minutes counts as blocked, as a drain would report it after waiting that long.";

// The preview buttons' tooltip: what a preview changes, and what it leaves
// to a drain.
export const PREVIEW_TOOLTIP = `${PREVIEW_CHANGES_NOTHING} It skips node-manager's orphan inventory, which a drain call reads, so a preview can't cut a drain call's scan short; the preview names what it skipped, and a drain can still find something there.`;

// How long the console waits for one preview. The API bounds a preview once
// it has authorized the caller, from its reads of the node's rows through
// its connect to the node's answer, at 60 s (`DRAIN_PREVIEW_DEADLINE` in
// `routes/admin/nodes/drain_preview.rs`). This waits longer, so it covers the
// whole request and the API's own answer, a timeout among them, arrives
// first.
export const DRAIN_PREVIEW_TIMEOUT_MS = 75_000;

// The bound on previews in flight at once from the nodes list.
export const PREVIEW_ALL_CONCURRENCY = 4;

export interface DrainPreviewBlocked {
  // Null for a node-level cause, such as a scan that failed.
  sandbox_id: string | null;
  reason: string;
}

export interface DrainPreviewPending {
  sandbox_id: string;
  // The planner's name for what a drain would do or wait for: `suspend`,
  // `publish_disk`, `upload`, `queued`, `disk`, `activation`,
  // `warm_pool_eviction` or `settle:<state>` (see `isWait`).
  action: string;
}

export interface DrainPreviewEvacuated {
  sandbox_id: string;
  prior: string;
  artifact_id: string;
}

export interface DrainPreview {
  node_id: string;
  // The epoch the node is sealed under now, which a preview leaves alone;
  // null when no drain holds it.
  sealed_epoch: number | null;
  // Nothing pending or blocked: a drain would finish at once. It says nothing
  // of a seal, which a preview never takes.
  would_finish: boolean;
  evacuated: DrainPreviewEvacuated[];
  pending: DrainPreviewPending[];
  blocked: DrainPreviewBlocked[];
  // The scans a drain call runs that this preview skipped, by name (today
  // `orphan_inventory`); `would_finish` doesn't account for them. Absent
  // from an API that predates it.
  unchecked?: string[];
}

export function previewPath(nodeId: string): string {
  return `/v1/nodes/${encodeURIComponent(nodeId)}/drain-v2-preview`;
}

export type PreviewVerdict = 'finish' | 'wait' | 'block';

// The pending items a drain would finish through: its own work (`suspend`,
// `publish_disk`, `upload`); `queued`, its window's own pacing, which blocks
// only once its action has failed as often as any work the drain drives may;
// and `activation`, one admitted before the seal, which settles on its own
// and never becomes a blocker.
const CLEARS = new Set(['suspend', 'publish_disk', 'upload', 'queued', 'activation']);

// Whether a pending item can turn into a blocker if it doesn't clear: `disk`,
// `warm_pool_eviction` and `settle:<state>`, each blocked past the drain's
// bound, and any item this console doesn't know.
export function isWait(action: string): boolean {
  return !CLEARS.has(action);
}

// A drain would block on anything blocked; otherwise it would wait when a
// pending item is a wait, and finish when every pending item clears as it
// runs, or nothing is pending.
export function previewVerdict(preview: Pick<DrainPreview, 'blocked' | 'pending'>): PreviewVerdict {
  if (preview.blocked.length) return 'block';
  return preview.pending.some(({ action }) => isWait(action)) ? 'wait' : 'finish';
}

const items = (n: number) => (n === 1 ? 'one item' : `${n} items`);

export function verdictText(
  preview: Pick<DrainPreview, 'blocked' | 'pending' | 'would_finish' | 'unchecked'>,
): string {
  switch (previewVerdict(preview)) {
    case 'block':
      return `A drain would block: ${preview.blocked.length} ${preview.blocked.length === 1 ? 'blocker' : 'blockers'}`;
    case 'wait': {
      const waits = preview.pending.filter(({ action }) => isWait(action)).length;
      return `A drain would wait on ${items(waits)} that can turn into ${waits === 1 ? 'a blocker' : 'blockers'}`;
    }
    default: {
      if (preview.would_finish) {
        const unchecked = preview.unchecked ?? [];
        return unchecked.length
          ? `A drain would finish at once, unless a scan the preview skipped blocks it: ${unchecked.join(', ')}`
          : 'A drain would finish at once';
      }
      const n = preview.pending.length;
      return `A drain would finish once its own work and the activations in flight are done: ${items(n)}`;
    }
  }
}

// What a drain would do or wait for, by action, the most common first, then
// by name.
export function pendingCounts(pending: Pick<DrainPreviewPending, 'action'>[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const { action } of pending) counts.set(action, (counts.get(action) ?? 0) + 1);
  return [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

// The first `limit` blockers as one line each, the sandbox first when there
// is one.
export function firstReasons(blocked: DrainPreviewBlocked[], limit = 2): string[] {
  return blocked.slice(0, limit).map((b) => (b.sandbox_id ? `${b.sandbox_id}: ${b.reason}` : b.reason));
}

export function sealText(sealedEpoch: number | null): string {
  return sealedEpoch === null
    ? 'Not sealed: no drain holds this node.'
    : `Sealed under epoch ${sealedEpoch}: a v2 drain already holds this node.`;
}

export type PreviewFailureKind = 'unreachable' | 'unsupported' | 'error';

export interface PreviewFailure {
  kind: PreviewFailureKind;
  message: string;
  // Every refusal `:drain` would give a v2 drain of the node, which the API
  // reports even when the node can't answer; empty when it has none, or
  // never got as far as reading them.
  refusals: string[];
}

// A failed preview read as the API answers it (`failure_with_refusals` in
// `routes/admin/nodes/drain_preview.rs`): its `reason` tells a node that
// can't be reached from one whose sandbox-manager or node-api is too old to
// preview, and `refusals` lists what `:drain` would refuse the node for.
// Anything else, a database timeout, a 404 or a network error among them, is
// an error with its message.
export function previewFailure(err: unknown): PreviewFailure {
  const { body, message } = (typeof err === 'object' && err !== null ? err : {}) as { body?: unknown; message?: unknown };
  const fields = body && typeof body === 'object' ? (body as Record<string, unknown>) : {};
  const refusals = Array.isArray(fields.refusals) ? fields.refusals.filter((r): r is string => typeof r === 'string') : [];
  const text = typeof message === 'string' && message ? message : 'Preview failed';
  if (fields.reason === 'node_unreachable') return { kind: 'unreachable', message: text, refusals };
  if (fields.reason === 'node_version_unsupported') {
    return { kind: 'unsupported', message: `${text}. Upgrade the node's sandbox-manager and node-api to preview it.`, refusals };
  }
  return { kind: 'error', message: text, refusals };
}

// The line a failed preview shows for its refusals: every one, since
// `:drain` names only the first it meets.
export function refusalsText(refusals: string[]): string | null {
  if (!refusals.length) return null;
  return `Whatever the node answers, :drain would refuse a v2 drain of it: ${refusals.join('; ')}`;
}

export function failureLabel(kind: PreviewFailureKind): string {
  switch (kind) {
    case 'unreachable':
      return 'unreachable';
    case 'unsupported':
      return 'version too old';
    default:
      return 'error';
  }
}

export type Settled<R> = { ok: true; value: R } | { ok: false; error: unknown };

// Run `run` over `items` with at most `limit` in flight, keeping the order of
// `items` in the result. `onSettled` sees each as it settles, with how many
// have. Once `signal` aborts, no further item starts, and each one not
// started settles as an error.
export async function runBounded<T, R>(
  items: T[],
  limit: number,
  run: (item: T) => Promise<R>,
  onSettled?: (index: number, result: Settled<R>, settled: number) => void,
  signal?: AbortSignal,
): Promise<Settled<R>[]> {
  const results: Settled<R>[] = new Array(items.length);
  let next = 0;
  let settled = 0;
  const settle = (index: number, result: Settled<R>) => {
    results[index] = result;
    settled += 1;
    onSettled?.(index, result, settled);
  };
  const worker = async () => {
    while (next < items.length) {
      const index = next;
      next += 1;
      if (signal?.aborted) {
        settle(index, { ok: false, error: new Error('cancelled') });
        continue;
      }
      try {
        settle(index, { ok: true, value: await run(items[index]) });
      } catch (error) {
        settle(index, { ok: false, error });
      }
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker));
  return results;
}

export interface PreviewRow {
  nodeId: string;
  name: string;
  verdict: PreviewVerdict | 'error';
  blockers: number;
  reasons: string[];
  pending: number;
  failure: PreviewFailure | null;
}

// One row of the nodes list's results table.
export function previewRow(node: { id: string; name: string | null }, result: Settled<DrainPreview>): PreviewRow {
  const name = node.name?.trim() || node.id;
  if (!result.ok) {
    const failure = previewFailure(result.error);
    const refusals = refusalsText(failure.refusals);
    const reasons = refusals ? [failure.message, refusals] : [failure.message];
    return { nodeId: node.id, name, verdict: 'error', blockers: 0, reasons, pending: 0, failure };
  }
  const preview = result.value;
  return {
    nodeId: node.id,
    name,
    verdict: previewVerdict(preview),
    blockers: preview.blocked.length,
    reasons: firstReasons(preview.blocked),
    pending: preview.pending.length,
    failure: null,
  };
}
