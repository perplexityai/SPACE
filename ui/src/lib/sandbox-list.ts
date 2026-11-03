import type { Sandbox } from './types';

// The admin sandbox list answers in one of two shapes. With `cursor` (empty
// for the first page) a server that pages answers `{sandboxes, next_cursor}`
// and applies the filters below. A server from before paging ignores
// `cursor` and every filter except `org_id` and answers a bare array of the
// newest `limit` rows; the console then samples, as it did before paging.
export type ListMode = 'paged' | 'sampled';

export type SandboxListResponse =
  | { mode: 'paged'; sandboxes: Sandbox[]; nextCursor: string | null }
  | { mode: 'sampled'; sandboxes: Sandbox[] };

export function parseSandboxListResponse(body: unknown): SandboxListResponse {
  if (Array.isArray(body)) return { mode: 'sampled', sandboxes: body as Sandbox[] };
  if (body && typeof body === 'object') {
    const { sandboxes, next_cursor: nextCursor } = body as { sandboxes?: unknown; next_cursor?: unknown };
    if (Array.isArray(sandboxes) && (nextCursor == null || typeof nextCursor === 'string')) {
      // An empty cursor would ask for the first page again, not the next.
      return { mode: 'paged', sandboxes: sandboxes as Sandbox[], nextCursor: nextCursor || null };
    }
  }
  throw new Error('unrecognized sandbox list response');
}

export interface SandboxListFilters {
  org: string | null;
  // Stored lifecycles; see `STORED_LIFECYCLE_FILTER_STATES`.
  lifecycles: string[];
  // The row's own template; a row with none does not match, although the
  // Template column may show the one its node reports.
  template: string | null;
  userId: string | null;
  // RFC 3339, both exclusive.
  createdAfter: string | null;
  createdBefore: string | null;
  // A name prefix (case-sensitive) or ID prefix, as typed.
  prefix: string;
}

export const NO_FILTERS: SandboxListFilters = {
  org: null, lifecycles: [], template: null, userId: null, createdAfter: null, createdBefore: null, prefix: '',
};

// The query string of one list request. A sampled list sends exactly what
// the console sent before paging: the sample size and the org, the one
// filter such a server applies. A paged request sends the page position, the
// page size and every set filter; the server refuses an unknown parameter
// once `cursor` is present, so nothing else is added.
export function sandboxListQuery(
  mode: ListMode,
  filters: SandboxListFilters,
  { limit, cursor = '' }: { limit: number; cursor?: string },
): string {
  const params = new URLSearchParams({ limit: String(limit) });
  if (mode === 'paged') params.set('cursor', cursor);
  if (filters.org) params.set('org_id', filters.org);
  if (mode === 'sampled') return params.toString();
  if (filters.lifecycles.length) params.set('lifecycle', filters.lifecycles.join(','));
  if (filters.template) params.set('template', filters.template);
  if (filters.userId) params.set('user_id', filters.userId);
  if (filters.createdAfter) params.set('created_after', filters.createdAfter);
  if (filters.createdBefore) params.set('created_before', filters.createdBefore);
  // The server refuses a NUL, which no typed prefix needs.
  const prefix = filters.prefix.trim().replaceAll('\0', '');
  if (prefix) params.set('prefix', prefix);
  return params.toString();
}

// Which filters the console applies to the rows it loaded. A paged server
// applies every filter it was sent; it has no node filter yet, so that one
// always narrows the loaded page only. A sampled list applies only `org_id`.
export interface ClientFilters {
  text: boolean;
  template: boolean;
  status: boolean;
  user: boolean;
  created: boolean;
  node: boolean;
}

export function clientFilters(mode: ListMode): ClientFilters {
  const sampled = mode === 'sampled';
  return { text: sampled, template: sampled, status: sampled, user: sampled, created: sampled, node: true };
}

// What to do with a list answer. A bare array answering a paged request
// means the server does not page: the console switches to sampling, and
// reads the sample size it wanted unless this answer already holds it.
export function listAnswerPlan(
  sent: ListMode,
  response: SandboxListResponse,
  { pageSize, sampleLimit }: { pageSize: number; sampleLimit: number },
): { mode: ListMode; reloadSample: boolean } {
  if (response.mode === 'paged') return { mode: 'paged', reloadSample: false };
  return { mode: 'sampled', reloadSample: sent === 'paged' && pageSize !== sampleLimit };
}

export const PAGE_SIZE_OPTIONS = [25, 50, 100, 200, 500];
export const DEFAULT_PAGE_SIZE = 50;

export function parsePageSize(raw: string | null): number {
  const value = raw === null ? NaN : Number(raw);
  return PAGE_SIZE_OPTIONS.includes(value) ? value : DEFAULT_PAGE_SIZE;
}

// The cursors of the pages visited so far: `cursors[i]` reads page `i`, and
// `cursors[0]` is the empty first-page cursor. Going back rereads a visited
// page from its cursor; a refresh rereads the current one.
export interface PagePosition {
  cursors: string[];
  index: number;
}

export const FIRST_PAGE: PagePosition = { cursors: [''], index: 0 };

export function nextPage(position: PagePosition, nextCursor: string | null): PagePosition | null {
  if (!nextCursor) return null;
  return { cursors: [...position.cursors.slice(0, position.index + 1), nextCursor], index: position.index + 1 };
}

export function previousPage(position: PagePosition): PagePosition | null {
  return position.index > 0 ? { cursors: position.cursors, index: position.index - 1 } : null;
}

export function pageCursor(position: PagePosition): string {
  return position.cursors[position.index] ?? '';
}

// The creation-time bounds of "created within the last `withinMs`" and
// "created more than `olderThanMs` ago", fixed at `now` so that they stay the
// same from page to page. The server refuses bounds that leave no instant
// between them, which the console's own controls never offer; one taken from
// a URL keeps the window and drops the "older than" bound.
export function createdBounds(
  withinMs: number | null,
  olderThanMs: number | null,
  now: number,
): { createdAfter: string | null; createdBefore: string | null } {
  const createdAfter = withinMs === null ? null : new Date(now - withinMs).toISOString();
  const usable = olderThanMs !== null && (withinMs === null || olderThanMs < withinMs);
  return { createdAfter, createdBefore: usable ? new Date(now - olderThanMs).toISOString() : null };
}
