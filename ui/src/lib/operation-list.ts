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
