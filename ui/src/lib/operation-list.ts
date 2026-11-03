// Helpers for the Operations page's reads of `GET /admin/api/v1/operations`.

interface OperationRow {
  id: string;
  created_at: string;
}

// The API's RFC 3339 timestamps carry up to nine fractional digits, with
// trailing zeros trimmed and a zero fraction left out, so neither their text
// nor Date.parse (whole milliseconds) orders them. Each compares as its
// milliseconds, then its fraction padded to nine digits.
function createdKey(at: string): [number, string] {
  const fraction = /T[^.]*\.(\d+)/.exec(at)?.[1] ?? '';
  return [Date.parse(at), fraction.slice(0, 9).padEnd(9, '0')];
}

// Newer first; the operation ID breaks only exact ties.
export function newerFirst(a: OperationRow, b: OperationRow): number {
  const [aMs, aFraction] = createdKey(a.created_at);
  const [bMs, bFraction] = createdKey(b.created_at);
  if (aMs !== bMs) return bMs - aMs;
  if (aFraction !== bFraction) return aFraction < bFraction ? 1 : -1;
  return b.id.localeCompare(a.id);
}

// Rows read under several target IDs, one per operation, newest first.
export function mergeNewest<T extends OperationRow>(pages: T[][], limit: number): T[] {
  const byId = new Map<string, T>();
  for (const op of pages.flat()) byId.set(op.id, op);
  return [...byId.values()].sort(newerFirst).slice(0, limit);
}

const SANDBOX_UUID = /^(?:sbx_)?([0-9a-f]{8})-?([0-9a-f]{4})-?([0-9a-f]{4})-?([0-9a-f]{4})-?([0-9a-f]{12})$/i;

// The target IDs one sandbox's operations are recorded under, for a typed
// `sbx_` ID or a bare UUID, dashed or not, in either case. Target IDs match
// exactly: every operation records a sandbox as `sbx_<uuid>` except a fork,
// which records its source by the bare lower-case UUID. Null for anything
// else, which is searched as typed.
export function sandboxTargetForms(typed: string): [string, string] | null {
  const parts = SANDBOX_UUID.exec(typed.trim());
  if (!parts) return null;
  const uuid = parts.slice(1).join('-').toLowerCase();
  return [`sbx_${uuid}`, uuid];
}

// The actors whose email the console must look up. Releases that report it
// send `actor_email` on each admin operation; a row that carries the field,
// even as null (the user is gone), needs no `GET /users/{id}`.
export function actorsToLookUp(
  operations: { actor_user_id: string | null; actor_email?: string | null }[],
): string[] {
  const ids = new Set<string>();
  for (const op of operations) {
    if (op.actor_user_id && op.actor_email === undefined) ids.add(op.actor_user_id);
  }
  return [...ids];
}
