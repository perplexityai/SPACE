// How the console tells an API release that predates a route, field or
// response shape from a real failure. The console can be deployed before the
// API it talks to, so each new call detects the older answer and falls back to
// what the console did before.

type ErrorShape = { status?: unknown; body?: unknown; text?: unknown; message?: unknown };

function errorShape(err: unknown): ErrorShape {
  return typeof err === 'object' && err !== null ? (err as ErrorShape) : {};
}

export function errorStatus(err: unknown): number | null {
  const { status } = errorShape(err);
  return typeof status === 'number' ? status : null;
}

// The parsed JSON error body, or null when the body was empty or not JSON.
export function errorBody(err: unknown): Record<string, unknown> | null {
  const { body } = errorShape(err);
  return body && typeof body === 'object' && !Array.isArray(body) ? (body as Record<string, unknown>) : null;
}

// A route the server does not have. An unknown path is a 404 from the API's
// fallback, `{ "code": 404, "message": "Endpoint not found" }` (a numeric
// code); a known path without the method (a GET on a POST-only route) is a
// 405. Handlers answer their own 404s with a string `code` (`"not_found"`)
// or an `error` ("Sandbox not found"), which this does not match.
export function isMissingRoute(err: unknown): boolean {
  const status = errorStatus(err);
  if (status === 405) return true;
  if (status !== 404) return false;
  const body = errorBody(err);
  return body === null || body.code === 404 || body.message === 'route not found';
}

// Axum rejects a body with a field the release does not know as a 422 whose
// plain-text body names it: "unknown field `user_id`, expected ...".
export function isUnknownFieldRejection(err: unknown, field: string): boolean {
  if (errorStatus(err) !== 422) return false;
  const { text, message } = errorShape(err);
  const haystack = `${typeof text === 'string' ? text : ''}\n${typeof message === 'string' ? message : ''}`;
  return haystack.includes(`unknown field \`${field}\``);
}

// Rows of a list that newer releases answer as a page object (`{ data,
// next_cursor }`) and older ones as a bare array.
export function pageRows<T>(body: unknown): { rows: T[]; nextCursor: string | null } {
  if (Array.isArray(body)) return { rows: body as T[], nextCursor: null };
  if (body && typeof body === 'object' && Array.isArray((body as { data?: unknown }).data)) {
    const { data, next_cursor: nextCursor } = body as { data: T[]; next_cursor?: unknown };
    return { rows: data, nextCursor: typeof nextCursor === 'string' && nextCursor ? nextCursor : null };
  }
  throw new Error('Unrecognized list response');
}

// Send `body` with an optional precondition field, for a route whose older
// releases refuse the field as unknown. On that refusal the request is sent
// once more without the field, and `fieldAccepted` is false: the caller must
// say that the precondition was not checked. Any other failure is thrown.
export async function sendWithOptionalField<B extends object, T>(
  send: (body: B & Record<string, unknown>) => Promise<T>,
  body: B,
  field: string,
  value: unknown,
): Promise<{ result: T; fieldAccepted: boolean }> {
  try {
    return { result: await send({ ...body, [field]: value }), fieldAccepted: true };
  } catch (err) {
    if (!isUnknownFieldRejection(err, field)) throw err;
    return { result: await send({ ...body } as B & Record<string, unknown>), fieldAccepted: false };
  }
}

// Where a sandbox's snapshots, and so its rollback, come from. Releases with
// the admin snapshot list serve every sandbox through it, and roll back
// through the admin `:force-rollback`, which takes the public rollback body.
// Older releases do not route the admin list, and their `:force-rollback`
// accepts the request but drops the snapshot name, so the operation fails:
// there the console keeps the public routes, which serve only the signed-in
// operator's organization (`readPublic` is null for any other).
export type SnapshotListing<T> =
  | { via: 'admin'; rows: T[] }
  | { via: 'public'; rows: T[] }
  | { via: 'none' };

export async function listSnapshotsWithFallback<T>(
  readAdmin: () => Promise<T[]>,
  readPublic: (() => Promise<T[]>) | null,
): Promise<SnapshotListing<T>> {
  try {
    return { via: 'admin', rows: await readAdmin() };
  } catch (err) {
    if (!isMissingRoute(err)) throw err;
  }
  if (!readPublic) return { via: 'none' };
  return { via: 'public', rows: await readPublic() };
}
