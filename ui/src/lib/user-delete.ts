// What deleting a user does, and how a refused delete reads. The users row is
// deleted outright: sessions and credentials go with it, API keys the user
// created stay but lose their creator, and the database refuses the delete
// while the user still owns sandboxes or archived sandboxes (the admin API
// currently reports that refusal as a 500).
export const USER_DELETE_CONSEQUENCES =
  'Their sessions end. API keys they created stay, without a creator. The delete is refused while they own sandboxes or archived sandboxes.';

export const USER_OWNS_SANDBOXES =
  'This user owns sandboxes. Delete them, or wait for them to be deleted, before deleting the user.';

export function userDeleteFailureMessage(err: unknown): string {
  const status = typeof err === 'object' && err !== null ? (err as { status?: unknown }).status : undefined;
  if (status === 409 || (typeof status === 'number' && status >= 500)) {
    return 'The user could not be deleted. A user who owns sandboxes or archived sandboxes cannot be deleted; delete their sandboxes first.';
  }
  return err instanceof Error && err.message ? err.message : 'The user could not be deleted.';
}
