// What the node page and the nodes list show of a v2 drain preview:
// `GET /v1/nodes/{id}/drain-v2-preview` (`routes/admin/nodes/drain_preview.rs`).
// The node answers what an evacuating v2 drain would find on it now, and
// changes nothing: it seals nothing, starts nothing and writes nothing. A node
// a v2 drain holds isn't previewed.

export const PREVIEW_CHANGES_NOTHING =
  "A preview changes nothing on the node: it seals nothing, starts no suspend or upload, and writes nothing. It reports what a v2 drain evacuating now would find on this node: before its deadline a drain leaves each running sandbox it could suspend running, listed pending with the wait hold, whatever this preview lists for it. A sandbox that can't move yet is listed pending with what it waits on; only a node-wide problem, such as a scan that failed, blocks.";

// The fixed caveat of every preview: the node skips node-manager's orphan
// inventory for it, which a drain call reads.
export const PREVIEW_SKIPS_ORPHAN_INVENTORY =
  "A preview skips node-manager's orphan inventory (untracked runtime processes, orphan cache and run directories), which a drain call reads, so a drain can find more node-wide blockers than its preview.";

// The preview buttons' tooltip: what a preview changes, and what it leaves
// to a drain.
export const PREVIEW_TOOLTIP = `${PREVIEW_CHANGES_NOTHING} ${PREVIEW_SKIPS_ORPHAN_INVENTORY}`;

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
  // A node-wide problem, such as a scan that failed.
  reason: string;
}

// What a pending sandbox waits on, as the node's `DrainWait` says: `hold`, kept
// running until the drain evacuates; `moving`, the drain making it durable
// (a suspend, a publication, an upload, or a wait for a window slot or disk
// room for one); `external`, work the drain doesn't drive (an activation or a
// user's snapshot in flight, a warm-pool eviction, a transition that isn't
// moving); `unspecified`, a wait the API doesn't know. Any other value is one
// this console doesn't know.
export type DrainWait = 'hold' | 'moving' | 'external' | 'unspecified' | string;

export interface DrainPreviewPending {
  sandbox_id: string;
  wait: DrainWait;
  // A started VM at the node's planning read.
  running: boolean;
  // Human text for display: what keeps the sandbox from moving yet.
  reason?: string;
  // When the row entered the state it waits in, in Unix seconds by the
  // node's clock.
  since?: number;
}

export interface DrainPreviewEvacuated {
  sandbox_id: string;
  // `full` for a suspended sandbox's snapshot, `disk` for a stopped one's.
  artifact_kind: string;
  artifact_id: string;
}

export interface DrainPreview {
  node_id: string;
  evacuated: DrainPreviewEvacuated[];
  pending: DrainPreviewPending[];
  blocked: DrainPreviewBlocked[];
}

export function previewPath(nodeId: string): string {
  return `/v1/nodes/${encodeURIComponent(nodeId)}/drain-v2-preview`;
}

export type PreviewVerdict = 'finish' | 'wait' | 'block';

// The waits a drain finishes through on its own: its own work (`moving`), and
// a running sandbox it holds until its deadline (`hold`).
const CLEARS = new Set(['moving', 'hold']);

// Whether a pending sandbox waits on something the drain doesn't drive, and
// can stall it if it doesn't clear: `external`, and any wait this console
// doesn't know, `unspecified` among them.
export function isWait(wait: DrainWait): boolean {
  return !CLEARS.has(wait);
}

// A drain would block on anything blocked; otherwise it would wait when a
// pending sandbox waits on what the drain doesn't drive, and finish when every
// pending one moves as it runs, or nothing is pending.
export function previewVerdict(preview: Pick<DrainPreview, 'blocked' | 'pending'>): PreviewVerdict {
  if (preview.blocked.length) return 'block';
  return preview.pending.some(({ wait }) => isWait(wait)) ? 'wait' : 'finish';
}

const items = (n: number) => (n === 1 ? 'one sandbox' : `${n} sandboxes`);

export function verdictText(preview: Pick<DrainPreview, 'blocked' | 'pending'>): string {
  switch (previewVerdict(preview)) {
    case 'block':
      return `A drain would block: ${preview.blocked.length} ${preview.blocked.length === 1 ? 'blocker' : 'blockers'}`;
    case 'wait': {
      const waits = preview.pending.filter(({ wait }) => isWait(wait)).length;
      return `A drain would wait on ${items(waits)} it doesn't move itself`;
    }
    default: {
      const n = preview.pending.length;
      return n
        ? `A drain would finish once it has made ${items(n)} durable`
        : 'A drain would finish at once';
    }
  }
}

// What the pending sandboxes wait on, by wait, the most common first, then by
// name.
export function pendingCounts(pending: Pick<DrainPreviewPending, 'wait'>[]): [string, number][] {
  const counts = new Map<string, number>();
  for (const { wait } of pending) counts.set(wait, (counts.get(wait) ?? 0) + 1);
  return [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

// The first `limit` blockers' reasons, one line each, as the node gave them.
export function firstReasons(blocked: DrainPreviewBlocked[], limit = 2): string[] {
  return blocked.slice(0, limit).map((b) => b.reason);
}

export type PreviewFailureKind = 'unreachable' | 'unsupported' | 'busy' | 'draining' | 'orphaned_seal' | 'error';

export interface PreviewFailure {
  kind: PreviewFailureKind;
  message: string;
  // Every refusal `:drain` would give a v2 drain of the node, which the API
  // reports even when the node can't answer; empty when it has none, or
  // never got as far as reading them.
  refusals: string[];
}

// A failed preview read as the API answers it (`drain_preview.rs`): its
// `reason` tells a node that can't be reached from one too old to preview,
// one busy with another preview (retry it), one a v2 drain holds (never
// previewed), and one sealed with no drain recorded (an orphaned seal); and
// `refusals` lists what `:drain` would refuse the node for. Anything else, a
// database timeout, a 404 or a network error among them, is an error with
// its message.
export function previewFailure(err: unknown): PreviewFailure {
  const { body, message } = (typeof err === 'object' && err !== null ? err : {}) as { body?: unknown; message?: unknown };
  const fields = body && typeof body === 'object' ? (body as Record<string, unknown>) : {};
  const refusals = Array.isArray(fields.refusals) ? fields.refusals.filter((r): r is string => typeof r === 'string') : [];
  const text = typeof message === 'string' && message ? message : 'Preview failed';
  switch (fields.reason) {
    case 'node_unreachable':
      return { kind: 'unreachable', message: text, refusals };
    case 'node_version_unsupported':
      return { kind: 'unsupported', message: `${text}. Upgrade the node's sandbox-manager and node-api to preview it.`, refusals };
    case 'node_busy':
      return { kind: 'busy', message: `${text}. Preview again in a few seconds.`, refusals };
    case 'drain_v2_in_progress':
      return { kind: 'draining', message: 'A v2 drain holds this node: its Drain progress panel shows what it finds.', refusals };
    case 'orphaned_seal':
      return { kind: 'orphaned_seal', message: text, refusals };
    default:
      return { kind: 'error', message: text, refusals };
  }
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
    case 'busy':
      return 'busy, retry';
    case 'draining':
      return 'v2 drain in progress';
    case 'orphaned_seal':
      return 'orphaned seal';
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
