// What deleting a user does, and how a refused delete reads. The users row is
// deleted outright: their sessions go with it, while API keys they created
// stay and keep authenticating for the organization (`api_keys.created_by` is
// `ON DELETE SET NULL`). The database refuses the delete while the user still
// owns sandboxes, including deleted ones whose records are kept for
// retention, or archived sandboxes.
export const USER_DELETE_CONSEQUENCES =
  "Their sessions end. API keys they created are not revoked and keep authenticating for the organization; delete them on the API keys page first if this user's access should end. The delete is refused while they own sandboxes or archived sandboxes.";

export const USER_OWNS_SANDBOXES =
  'This user owns sandboxes. Delete them, or wait for them to be deleted, before deleting the user.';

// Releases that report the ownership refusal answer a 409 with a `reason`;
// older ones answer it with a 500 whose message is the generic "Internal
// server error" (or no message, which the console reads as "Request failed").
// A 500 with any other message is a real failure, shown as the API words it.
const OLD_RELEASE_REFUSAL =
  'The user could not be deleted. If they own sandboxes or archived sandboxes, delete those first.';

function isGenericServerError(message: string | null): boolean {
  return message === null || /^(internal server error|request failed)\.?$/i.test(message.trim());
}

function formatRetainedUntil(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const at = new Date(value);
  return Number.isNaN(at.getTime()) ? null : at.toLocaleString();
}

export function userDeleteFailureMessage(err: unknown): string {
  const { status, body } = (typeof err === 'object' && err !== null ? err : {}) as { status?: unknown; body?: unknown };
  const reason = body && typeof body === 'object' ? (body as { reason?: unknown }).reason : undefined;
  const apiMessage = err instanceof Error && err.message ? err.message : null;
  if (status === 409) {
    switch (reason) {
      case 'user_owns_sandboxes':
        return 'The user still owns sandboxes. Delete them before deleting the user.';
      case 'user_owns_retained_deleted_sandboxes': {
        const until = formatRetainedUntil((body as { retained_until?: unknown }).retained_until);
        return until
          ? `The user's sandboxes are deleted, but their records are kept until about ${until}. Delete the user after that.`
          : "The user's sandboxes are deleted, but their records are still kept for retention. Delete the user later.";
      }
      case 'user_owns_archived_sandboxes':
        return 'The user still owns archived sandboxes. They must be purged before the user can be deleted.';
    }
  }
  if (status === 500 && isGenericServerError(apiMessage)) return OLD_RELEASE_REFUSAL;
  // Anything else keeps the API's message: another 409 (deleting yourself),
  // a retryable 503, a 500 that says what failed.
  return apiMessage ?? 'The user could not be deleted.';
}

// Passwords are hashed with bcrypt, which reads at most 72 bytes.
export const MAX_PASSWORD_BYTES = 72;

export function passwordTooLong(password: string): boolean {
  return new TextEncoder().encode(password).length > MAX_PASSWORD_BYTES;
}
