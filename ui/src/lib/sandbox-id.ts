import type { ListMode } from './sandbox-list';

const SANDBOX_LABEL_LEN = 8;
const PUBLIC_SANDBOX_ID_PREFIX = 'sbx_';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const COMPACT_UUID_PATTERN = /^[0-9a-f]{32}$/;
// Sandbox pods are named `space-sbx-<uuid without dashes>-g<generation>`.
const POD_NAME_PATTERN = /^space-sbx-([0-9a-f]{32})(?:-g\d+)?$/;

export interface SandboxRef {
  // Lowercase dashed UUID, the form the admin debug and node routes take.
  uuid: string;
  // `sbx_<uuid>`, the form the admin sandbox routes take.
  publicId: string;
}

function dashed(compact: string): string {
  return [
    compact.slice(0, 8),
    compact.slice(8, 12),
    compact.slice(12, 16),
    compact.slice(16, 20),
    compact.slice(20),
  ].join('-');
}

// Resolve any identifier an operator may hold for a sandbox: the public
// `sbx_<uuid>` ID, a bare or undashed UUID, a `sandbox:<uuid>` log tag, or the
// sandbox pod name. Returns null for anything else, including partial IDs.
export function parseSandboxRef(input: string): SandboxRef | null {
  let value = input.trim().replace(/^['"`]+|['"`]+$/g, '').toLowerCase();
  if (value.startsWith(PUBLIC_SANDBOX_ID_PREFIX)) {
    value = value.slice(PUBLIC_SANDBOX_ID_PREFIX.length);
  } else if (value.startsWith('sandbox:')) {
    value = value.slice('sandbox:'.length);
  } else {
    const pod = POD_NAME_PATTERN.exec(value);
    if (pod) value = pod[1];
  }
  if (COMPACT_UUID_PATTERN.test(value)) value = dashed(value);
  if (!UUID_PATTERN.test(value)) return null;
  return { uuid: value, publicId: `${PUBLIC_SANDBOX_ID_PREFIX}${value}` };
}

// `decodeURIComponent` that returns its input unchanged when it is not valid
// percent-encoding, for identifiers taken from a URL path.
export function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export function sandboxUuidPart(sandboxId: string): string {
  return sandboxId.startsWith(PUBLIC_SANDBOX_ID_PREFIX)
    ? sandboxId.slice(PUBLIC_SANDBOX_ID_PREFIX.length)
    : sandboxId;
}

export function publicSandboxIdFromAny(sandboxId: string): string {
  const ref = parseSandboxRef(sandboxId);
  if (ref) return ref.publicId;
  return sandboxId.startsWith(PUBLIC_SANDBOX_ID_PREFIX)
    ? sandboxId
    : `${PUBLIC_SANDBOX_ID_PREFIX}${sandboxId}`;
}

export function sandboxLabelOrId(sandboxId: string): string {
  const compact = sandboxUuidPart(sandboxId).replaceAll('-', '').toLowerCase();
  if (!compact || !/^[0-9a-f]+$/.test(compact)) {
    return sandboxId;
  }
  if (compact.length <= SANDBOX_LABEL_LEN) {
    return compact;
  }
  return compact.slice(-SANDBOX_LABEL_LEN);
}

// The admin list returns the newest sandbox rows first, up to `limit` of
// them (the API's default limit is 1000 and its maximum 5000); the unscoped
// list also appends the node-reported rows described below.
export const DEFAULT_LIST_LIMIT = 1000;
export const MAX_LIST_LIMIT = 5000;
export const LIST_LIMIT_OPTIONS = [100, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT];

// The admin list returns at most `limit` sandbox rows. Without `org_id` it
// then appends rows the nodes report without a database row (rowless pool
// slots, observed-local sandboxes), which belong to no org; an org-scoped
// list does not. Only the sandbox rows count against the limit.
export function sandboxRowCount(rows: { resource_type?: string }[]): number {
  return rows.filter((row) => row.resource_type === 'sandbox').length;
}

export function listIsCapped(rows: { resource_type?: string }[], limit: number): boolean {
  return sandboxRowCount(rows) >= limit;
}

export function parseListLimit(raw: string | null): number {
  const value = raw === null ? NaN : Number(raw);
  if (!Number.isInteger(value) || value < 1) return DEFAULT_LIST_LIMIT;
  return Math.min(value, MAX_LIST_LIMIT);
}

// What the sandbox search box holds: nothing, one sandbox's full identifier
// in any accepted form, or free text matched against IDs and names.
export type SandboxQuery =
  | { kind: 'empty' }
  | { kind: 'ref'; ref: SandboxRef }
  | { kind: 'text'; text: string; compact: string };

export function sandboxQuery(input: string): SandboxQuery {
  const text = input.trim().toLowerCase();
  if (!text) return { kind: 'empty' };
  const ref = parseSandboxRef(text);
  if (ref) return { kind: 'ref', ref };
  return { kind: 'text', text, compact: text.replaceAll('-', '') };
}

export function matchesSandboxQuery(query: SandboxQuery, sandbox: { id: string; name?: string | null }): boolean {
  switch (query.kind) {
    case 'empty':
      return true;
    case 'ref':
      return publicSandboxIdFromAny(sandbox.id) === query.ref.publicId;
    case 'text':
      return (
        sandbox.id.replaceAll('-', '').toLowerCase().includes(query.compact) ||
        (sandbox.name || '').toLowerCase().includes(query.text)
      );
  }
}

// The longest sandbox name (`MAX_SANDBOX_NAME_BYTES` in
// crates/libs/public/api/src/limits.rs): a longer prefix starts no name.
const MAX_SANDBOX_NAME_BYTES = 256;
const UUID_DASH_POSITIONS = [8, 13, 18, 23];

// Whether `value` is the start of a sandbox ID as a paging server reads
// a `prefix`: hex digits, with dashes exactly where a dashed UUID has them
// if it has any.
function startsUuid(value: string): boolean {
  if (!value || value.length > 36) return false;
  const dashed = value.includes('-');
  if (!dashed && value.length > 32) return false;
  return [...value].every((character, position) =>
    dashed && UUID_DASH_POSITIONS.includes(position) ? character === '-' : /^[0-9a-f]$/.test(character));
}

// What the search box does on each list shape.
// - `ref`: a full identifier in any accepted form, looked up directly.
// - `prefix`: sent to a paging server as typed. It matches the start of
//   names case-sensitively, and the start of IDs: an `sbx_` ID prefix, or a
//   dashed one in the UUID's layout, with hex digits in either case. The
//   server reads only a lowercase `sbx_` as the ID's; `parseSandboxRef`
//   takes `SBX_` too, so an ID prefix behind it is sent behind `sbx_`, the
//   rest as typed. A name that starts with `SBX_` and then hex is then not
//   matched.
// - `page`: matched within the loaded rows, ignoring case, anywhere in the ID
//   or name. On a paging server this is undashed hex or a hex run with
//   dashes elsewhere, such as the ID column's label, which is the end of
//   the ID and not its start, and text no name can start with. A sampled
//   list matches all free text this way, as it did before paging.
export type SandboxSearch =
  | { kind: 'empty' }
  | { kind: 'ref'; ref: SandboxRef }
  | { kind: 'prefix'; prefix: string }
  | { kind: 'page'; query: Extract<SandboxQuery, { kind: 'text' }> };

export function sandboxSearch(input: string, mode: ListMode): SandboxSearch {
  const query = sandboxQuery(input);
  if (query.kind === 'empty') return query;
  if (query.kind === 'ref') return query;
  if (mode === 'sampled') return { kind: 'page', query };
  const typed = input.trim();
  const lower = typed.toLowerCase();
  // As typed: lowercasing would lose a name such as `sbx_AB-worker`.
  const afterMarker = typed.slice(PUBLIC_SANDBOX_ID_PREFIX.length);
  if (lower.startsWith(PUBLIC_SANDBOX_ID_PREFIX) && startsUuid(afterMarker.toLowerCase())) {
    return { kind: 'prefix', prefix: `${PUBLIC_SANDBOX_ID_PREFIX}${afterMarker}` };
  }
  if (lower.includes('-') && startsUuid(lower)) return { kind: 'prefix', prefix: typed };
  const idFragment = /^[0-9a-f-]+$/.test(lower) && /[0-9a-f]/.test(lower);
  const namePrefix = !typed.includes('\0') && new TextEncoder().encode(typed).length <= MAX_SANDBOX_NAME_BYTES;
  if (idFragment || !namePrefix) return { kind: 'page', query };
  return { kind: 'prefix', prefix: typed };
}
