// The lifecycle states the admin API reports (`SandboxLifecycle` in
// crates/libs/common/core/src/types/sandbox.rs).
export const SANDBOX_LIFECYCLE_STATES = [
  'created', 'creating', 'starting', 'started',
  'pausing', 'paused', 'resuming',
  'suspending', 'suspended', 'restoring',
  'stopping', 'stopped',
  'checkpointing', 'quarantined', 'deleting', 'failed',
] as const;

// The action that brings a sandbox in `status` back up, named for what it
// does: a stopped sandbox starts, a paused one resumes in memory, a suspended
// one restores from its snapshot. `null` when nothing brings it up from here.
export function bringUpAction(status: string | null): { action: 'start' | 'resume' | 'restore'; label: string } | null {
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
