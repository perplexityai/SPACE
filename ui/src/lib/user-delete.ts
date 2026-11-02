// What deleting a user does, and how a refused delete reads. The users row is
// deleted outright: their sessions go with it, while API keys they created
// stay and keep authenticating for the organization (`api_keys.created_by` is
// `ON DELETE SET NULL`). The database refuses the delete while the user still
// owns sandboxes or archived sandboxes; the admin API currently reports that
// refusal as a 500.
export const USER_DELETE_CONSEQUENCES =
  "Their sessions end. API keys they created are not revoked and keep authenticating for the organization; delete them on the API keys page first if this user's access should end. The delete is refused while they own sandboxes or archived sandboxes.";

export const USER_OWNS_SANDBOXES =
  'This user owns sandboxes. Delete them, or wait for them to be deleted, before deleting the user.';

const OWNERSHIP_REFUSAL =
  'The user could not be deleted. A user who owns sandboxes or archived sandboxes cannot be deleted; delete their sandboxes first.';

// The ownership explanation only where it is the likely cause: a 500 while
// the caller's own sandbox list shows the user owns sandboxes. The admin API
// answers the database's ownership refusal with a 500; this stays until it
// answers that refusal with its own status. Anything else keeps the API's
// message: a 409 is the API refusing to let callers delete themselves, and
// a 503 is retryable.
export function userDeleteFailureMessage(err: unknown, { ownsSandboxes = false }: { ownsSandboxes?: boolean } = {}): string {
  const status = typeof err === 'object' && err !== null ? (err as { status?: unknown }).status : undefined;
  if (status === 500 && ownsSandboxes) return OWNERSHIP_REFUSAL;
  return err instanceof Error && err.message ? err.message : 'The user could not be deleted.';
}

// Passwords are hashed with bcrypt, which reads at most 72 bytes.
export const MAX_PASSWORD_BYTES = 72;

export function passwordTooLong(password: string): boolean {
  return new TextEncoder().encode(password).length > MAX_PASSWORD_BYTES;
}
