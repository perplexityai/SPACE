// The lifecycle states the admin API reports (`SandboxLifecycle` in
// crates/libs/common/core/src/types/sandbox.rs).
export const SANDBOX_LIFECYCLE_STATES = [
  'created', 'creating', 'starting', 'started',
  'pausing', 'paused', 'resuming',
  'suspending', 'suspended', 'restoring',
  'stopping', 'stopped',
  'checkpointing', 'quarantined', 'deleting', 'failed',
] as const;

// What the list's status filter offers. The list shows a node's reported
// state when a node reports one (`sandbox_to_cached_response` in
// crates/libs/api/src/controllers/sandbox/responses.rs), so it adds the
// node-only states (`SandboxLifecycleState` in
// crates/libs/sbx/mgr/protocol/src/lifecycle.rs), and `deleted`. A delete
// through the API removes the row (crates/libs/sandbox/teardown); a `deleted`
// row is one a Direct node's complete census no longer includes
// (crates/libs/internal/api/server/src/status.rs), kept for a retention
// period, and the admin list, which does not filter by lifecycle, returns it.
export const SANDBOX_STATUS_FILTER_STATES = [
  ...SANDBOX_LIFECYCLE_STATES, 'unspecified', 'cleanup_required', 'cleanup_failed', 'deleted',
] as const;

// What the paged list's lifecycle filter offers and sends: the stored
// lifecycle (`sandboxes.lifecycle`), every `SandboxLifecycle` state and the
// two tombstones. It is not the State column, which shows the node's reported
// state when there is one, so a row can match one lifecycle and show another
// state. The node-only states above are never stored, and the server refuses
// them.
export const STORED_LIFECYCLE_FILTER_STATES = [...SANDBOX_LIFECYCLE_STATES, 'deleted', 'cleanup_required'] as const;

const STORED_LIFECYCLES: ReadonlySet<string> = new Set(STORED_LIFECYCLE_FILTER_STATES);

// The selected values the paged list can send; a status chosen on a sampled
// list, such as `unknown`, is not sent rather than answered with a 400.
export function storedLifecycles(values: Iterable<string>): string[] {
  return [...values].filter((value) => STORED_LIFECYCLES.has(value));
}

// Which selected statuses a list applies. A paged list sends the stored
// lifecycles and cannot apply the rest, such as `unknown` from a link or a
// status chosen while the list was sampled; the console shows those apart,
// as not applied, rather than as filter chips. A sampled list matches every
// status against its rows.
export function splitStatusFilter(paged: boolean, values: string[]): { applied: string[]; notApplied: string[] } {
  if (!paged) return { applied: values, notApplied: [] };
  const applied = storedLifecycles(values);
  return { applied, notApplied: values.filter((value) => !STORED_LIFECYCLES.has(value)) };
}

// A row without a status shows, and filters, as `unknown`.
export const NO_STATUS = 'unknown';

export function statusFilterKey(status: string | null | undefined): string {
  return status || NO_STATUS;
}

// The status filter's options: the known states, then any other status the
// loaded rows carry, so no row is reachable only with the filter cleared.
export function statusFilterOptions(statuses: Iterable<string | null | undefined>): string[] {
  const known = new Set<string>(SANDBOX_STATUS_FILTER_STATES);
  const extra = new Set<string>();
  for (const status of statuses) {
    const key = statusFilterKey(status);
    if (!known.has(key)) extra.add(key);
  }
  return [...SANDBOX_STATUS_FILTER_STATES, ...[...extra].sort()];
}

// The action that brings a sandbox back up, named for what it does: a
// stopped sandbox starts, a paused one resumes in memory, a suspended one
// restores from its snapshot. It keys on the row's `status`. The admin list
// fills both `status` and `lifecycle` from the node's reported state when a
// node reports one, falling back to the stored lifecycle for both
// (`sandbox_to_cached_response`), so the two never disagree in a list row and
// the stored state is not available here. `null` when nothing brings it up
// from here.
export function bringUpAction(
  status: string | null,
): { action: 'start' | 'resume' | 'restore'; label: string } | null {
  switch (status) {
    case 'created':
    case 'stopped':
      return { action: 'start', label: 'Start' };
    case 'paused':
      return { action: 'resume', label: 'Resume' };
    case 'suspended':
      return { action: 'restore', label: 'Restore' };
    default:
      return null;
  }
}
