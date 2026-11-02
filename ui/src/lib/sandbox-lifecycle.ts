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
// crates/libs/api/src/controllers/sandbox/responses.rs), so it adds the two
// node-only states (`SandboxLifecycleState` in
// crates/libs/sbx/mgr/protocol/src/lifecycle.rs), and `deleted`. A delete
// through the API removes the row (crates/libs/sandbox/teardown); a `deleted`
// row is one a Direct node's complete census no longer includes
// (crates/libs/internal/api/server/src/status.rs), kept for a retention
// period, and the admin list, which does not filter by lifecycle, returns it.
export const SANDBOX_STATUS_FILTER_STATES = [
  ...SANDBOX_LIFECYCLE_STATES, 'cleanup_required', 'cleanup_failed', 'deleted',
] as const;


// The action that brings a sandbox back up, named for what it does: a
// stopped sandbox starts, a paused one resumes in memory, a suspended one
// restores from its snapshot. It keys on the stored `lifecycle` when the row
// has one: `status` is the node's reported state when a node reports one, so
// a suspended sandbox whose node reports it stopped still restores. Without a
// lifecycle it falls back to `status`. `null` when nothing brings it up from
// here.
export function bringUpAction(
  row: { lifecycle?: string | null; status: string | null },
): { action: 'start' | 'resume' | 'restore'; label: string } | null {
  switch (row.lifecycle || row.status) {
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
