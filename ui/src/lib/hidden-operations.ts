// Snapshot listing, rollback and enabling snapshots are hidden native
// operations. Unless the deployment enables them for the signed-in user's
// organization (SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS in docs/configuration.md),
// the API answers them exactly as a route or sandbox action that does not
// exist. The console reads that answer as "feature off", not as a failure.
export const SNAPSHOTS_NOT_ENABLED = 'Snapshots are not enabled for your organization.';
export const ROLLBACK_NOT_ENABLED = 'Snapshot rollback is not enabled for your organization.';

export function isHiddenOperationError(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false;
  const { status, message } = err as { status?: unknown; message?: unknown };
  return (
    (status === 404 && message === 'route not found') ||
    (status === 400 && message === 'invalid sandbox action')
  );
}
