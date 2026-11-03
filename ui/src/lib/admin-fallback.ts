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

// A route the server does not have. An unknown admin path is a 404 from the
// admin API's fallback, `{ "code": 404, "message": "Endpoint not found" }` (a
// numeric code), or a 404 without a JSON body; a known path without the method
// (a GET on a POST-only route) is a 405. Handlers answer their own 404s with a
// string `code` (`"not_found"`) or an `error` ("Sandbox not found"), and the
// public API's hidden-operation 404 (`"route not found"`, see
// `isHiddenOperationError`) comes from no admin route: none of them match.
export function isMissingRoute(err: unknown): boolean {
  const status = errorStatus(err);
  if (status === 405) return true;
  if (status !== 404) return false;
  const body = errorBody(err);
  return body === null || body.code === 404;
}

// Axum rejects a body with a field the release does not know as a 422 whose
// plain-text body names it: "unknown field `user_id`, expected ...".
export function isUnknownFieldRejection(err: unknown, field: string): boolean {
  if (errorStatus(err) !== 422) return false;
  const { text, message } = errorShape(err);
  const haystack = `${typeof text === 'string' ? text : ''}\n${typeof message === 'string' ? message : ''}`;
  return haystack.includes(`unknown field \`${field}\``);
}

// The rows of a list answered either as a bare array (the template builds
// route) or as a page object, `{ data, next_cursor }` (the admin snapshot
// list). `nextCursor` is the page's cursor when more rows follow. No route
// this reads answers another shape, such as `{ sandboxes, next_cursor }`, so
// anything else is refused rather than read as empty.
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

// Move a release's tag only while it points at `expected` (the release that
// held it when the operator looked; null asks that the tag not exist yet).
// Releases that do not know `expected_current_release_id` refuse it as an
// unknown field and get one retry without it: `checked` is then false, and
// the caller must say so (`TAG_PRECONDITION_UNCHECKED`).
export async function moveReleaseTag(
  put: (path: string, body: unknown) => Promise<unknown>,
  releaseId: string,
  tag: string,
  expected: string | null,
): Promise<{ checked: boolean }> {
  const { fieldAccepted } = await sendWithOptionalField(
    (body) => put(`/v1/template-releases/${releaseId}/tag`, body),
    { tag },
    'expected_current_release_id',
    expected,
  );
  return { checked: fieldAccepted };
}

export const TAG_PRECONDITION_UNCHECKED =
  'This API release does not check where the tag pointed before moving it, so a concurrent move may have been overwritten.';

// A failed tag move in the operator's words. A 412 is the precondition
// refusing: the tag moved since it was read, and its `details` name the
// release it points at now when it exists.
export function tagMoveFailureMessage(err: unknown): string {
  if (errorStatus(err) !== 412) return err instanceof Error ? err.message : String(err);
  const details = errorBody(err)?.details;
  const current = Array.isArray(details)
    ? details.find((detail) => (detail as { key?: unknown })?.key === 'current_release_id')
    : undefined;
  const holder = typeof (current as { value?: unknown })?.value === 'string' ? (current as { value: string }).value : null;
  return `The tag moved since it was read${holder ? ` (it now names release ${holder})` : ''}; nothing was changed. Refresh and try again.`;
}

// Where a sandbox's snapshots, and so its rollback, come from.
// - The operator's own organization: the public routes, as before the admin
//   list existed. Their reads write no admin audit event, so a page may list
//   them again on every refresh, and the rollback gate answers there as a
//   hidden operation (`isHiddenOperationError`).
// - Another organization: the admin list, rolled back through the admin
//   `:force-rollback`. Every read of the admin list writes an admin audit
//   event, so callers read it when the view opens and on an explicit Refresh,
//   never on a timer. The page holds at most `ADMIN_SNAPSHOT_PAGE_LIMIT` rows;
//   `more` says that older ones were not read. A release without the admin
//   list leaves another organization's sandbox with none (`via: 'none'`): its
//   `:force-rollback` accepts the request but drops the snapshot name.
export type SnapshotListing<T> =
  | { via: 'admin'; rows: T[]; more: boolean }
  | { via: 'public'; rows: T[] }
  | { via: 'none' };

export const ADMIN_SNAPSHOT_PAGE_LIMIT = 1000;

// What makes a snapshot view list: it opens (or shows another sandbox), its
// own Refresh or Retry, a rollback it sent settles, or the page reloads (on a
// timer, the page's Refresh, and after each action). Every view lists on the
// first three. Another organization's snapshots come from the audited admin
// list, so its view never lists on the page's reloads, which include a timer.
export type SnapshotListingTrigger = 'open' | 'refresh' | 'rollback-settled' | 'page-reload';

export function listsSnapshotsOn(trigger: SnapshotListingTrigger, ownOrg: boolean): boolean {
  return ownOrg || trigger !== 'page-reload';
}

export async function listSnapshots<T>({
  ownOrg,
  readPublic,
  readAdmin,
}: {
  ownOrg: boolean;
  readPublic: () => Promise<T[]>;
  readAdmin: () => Promise<{ rows: T[]; nextCursor: string | null }>;
}): Promise<SnapshotListing<T>> {
  if (ownOrg) return { via: 'public', rows: await readPublic() };
  try {
    const { rows, nextCursor } = await readAdmin();
    return { via: 'admin', rows, more: nextCursor !== null };
  } catch (err) {
    if (!isMissingRoute(err)) throw err;
    return { via: 'none' };
  }
}
