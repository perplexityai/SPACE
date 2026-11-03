import type { Sandbox } from './types';

// The admin sandbox list answers in one of two shapes. With `cursor` (empty
// for the first page) a server that pages answers `{sandboxes, next_cursor}`
// and applies the filters below. A server from before paging ignores
// `cursor` and every filter except `org_id` and answers a bare array of the
// newest `limit` rows; the console then samples, as it did before paging.
// A bare array is the older answer only for that request: during a rolling
// deploy the next request may reach a server that pages, so a sampled tab
// asks for its sample in the paged form and switches back on a page answer.
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

// A creation-time bound relative to now: "created within the last ..." or
// "created more than ... ago".
export const CREATED_AGES = {
  '1h': 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
  '30d': 30 * 24 * 60 * 60 * 1000,
  '90d': 90 * 24 * 60 * 60 * 1000,
  '1y': 365 * 24 * 60 * 60 * 1000,
} as const;
export type CreatedAge = keyof typeof CREATED_AGES;

export const createdAgeMs = (age: CreatedAge | null): number | null => (age === null ? null : CREATED_AGES[age]);

export interface SandboxListFilters {
  org: string | null;
  // Stored lifecycles; see `STORED_LIFECYCLE_FILTER_STATES`.
  lifecycles: string[];
  // The row's own template; a row with none does not match, although the
  // Template column may show the one its node reports.
  template: string | null;
  userId: string | null;
  // Relative bounds, kept as chosen; each request turns them into
  // timestamps when it is sent (see `createdBounds`).
  createdWithin: CreatedAge | null;
  olderThan: CreatedAge | null;
  // A name prefix (case-sensitive) or ID prefix, as typed.
  prefix: string;
}

export const NO_FILTERS: SandboxListFilters = {
  org: null, lifecycles: [], template: null, userId: null, createdWithin: null, olderThan: null, prefix: '',
};

// The query string of one list request. A sampled request is the first page
// of the unfiltered list of the org, the one filter a server without paging
// applies, at the sample size: such a server ignores `cursor` and answers
// its sample, and one that pages answers a page, which shows that it does.
// A paged request sends the page position, the page size and every set
// filter; the server refuses an unknown parameter once `cursor` is present,
// so nothing else is added. The creation-time bounds are computed from
// `now`, the time the request is sent.
export function sandboxListQuery(
  mode: ListMode,
  filters: SandboxListFilters,
  { limit, cursor = '', now }: { limit: number; cursor?: string; now: number },
): string {
  const params = new URLSearchParams({ limit: String(limit), cursor: mode === 'paged' ? cursor : '' });
  if (filters.org) params.set('org_id', filters.org);
  if (mode === 'sampled') return params.toString();
  if (filters.lifecycles.length) params.set('lifecycle', filters.lifecycles.join(','));
  if (filters.template) params.set('template', filters.template);
  if (filters.userId) params.set('user_id', filters.userId);
  const { createdAfter, createdBefore } = createdBounds(createdAgeMs(filters.createdWithin), createdAgeMs(filters.olderThan), now);
  if (createdAfter) params.set('created_after', createdAfter);
  if (createdBefore) params.set('created_before', createdBefore);
  // The server refuses a NUL, which no typed prefix needs.
  const prefix = filters.prefix.trim().replaceAll('\0', '');
  if (prefix) params.set('prefix', prefix);
  return params.toString();
}

// What one list request asks for: the list shape, the page size or sample
// size, the page position and the filters, with the creation-time bounds
// still relative. `key` names it: two scopes with the same key ask for the
// same rows, whenever they are sent, so a refresh of "Last 1h" an hour
// later is the same view, not a new one.
export interface ListScope {
  mode: ListMode;
  limit: number;
  cursor: string;
  filters: SandboxListFilters;
  key: string;
}

export function listScope(
  mode: ListMode,
  filters: SandboxListFilters,
  { limit, cursor }: { limit: number; cursor: string },
): ListScope {
  // A sampled request sends only the size and the org.
  const sent = mode === 'paged' ? filters : { ...NO_FILTERS, org: filters.org };
  const at = mode === 'paged' ? cursor : '';
  const key = new URLSearchParams({
    mode,
    limit: String(limit),
    cursor: at,
    org: sent.org ?? '',
    lifecycle: sent.lifecycles.join(','),
    template: sent.template ?? '',
    user: sent.userId ?? '',
    within: sent.createdWithin ?? '',
    older: sent.olderThan ?? '',
    prefix: sent.prefix.trim().replaceAll('\0', ''),
  }).toString();
  return { mode, limit, cursor: at, filters: sent, key };
}

export const sameScope = (a: ListScope | null, b: ListScope | null) => a !== null && b !== null && a.key === b.key;

// The query string of a request for `scope`, sent at `now`.
export function listRequestQuery(scope: ListScope, now: number): string {
  return sandboxListQuery(scope.mode, scope.filters, { limit: scope.limit, cursor: scope.cursor, now });
}

// The filters the console applies to the rows it loaded. A paged server
// applies every filter it was sent; it has no node filter yet, so that one
// always narrows the loaded rows only, and a search the server cannot answer
// (see `sandboxSearch`) is matched here too. A sampled list was sent only
// `org_id`, so every other filter applies here.
type LoadedRow = Pick<Sandbox, 'template' | 'status' | 'user_id' | 'active_node_id' | 'created_at'>;

export interface LoadedRowCriteria<T extends LoadedRow> {
  // The search as matched within the loaded rows, or null when the server
  // applied it or there is none.
  matchesSearch: ((row: T) => boolean) | null;
  template: string | null;
  statuses: string[];
  // The key a row's status filters under (`statusFilterKey`).
  statusKey: (status: string | null | undefined) => string;
  userId: string | null;
  node: 'any' | 'assigned' | 'unassigned';
  createdWithin: CreatedAge | null;
  olderThan: CreatedAge | null;
  now: number;
}

export function loadedRowFilter<T extends LoadedRow>(mode: ListMode, criteria: LoadedRowCriteria<T>): (row: T) => boolean {
  const sampled = mode === 'sampled';
  const { createdAfter, createdBefore } = createdBounds(
    createdAgeMs(criteria.createdWithin), createdAgeMs(criteria.olderThan), criteria.now);
  const after = createdAfter === null ? null : Date.parse(createdAfter);
  const before = createdBefore === null ? null : Date.parse(createdBefore);
  return (row) => {
    if (criteria.matchesSearch && !criteria.matchesSearch(row)) return false;
    if (criteria.node !== 'any' && (criteria.node === 'assigned') !== !!row.active_node_id) return false;
    if (!sampled) return true;
    if (criteria.template && row.template !== criteria.template) return false;
    if (criteria.statuses.length > 0 && !criteria.statuses.includes(criteria.statusKey(row.status))) return false;
    if (criteria.userId && row.user_id !== criteria.userId) return false;
    if (after !== null || before !== null) {
      const created = row.created_at ? Date.parse(row.created_at) : NaN;
      if (Number.isNaN(created)) return false;
      if (after !== null && created <= after) return false;
      if (before !== null && created >= before) return false;
    }
    return true;
  };
}

// What to do with a list answer: the list shape to use from now on, and
// whether to show the answer or read the selected scope in that shape. A
// bare array answering a paged request means the server does not page: the
// console switches to sampling and reads the sample. A page answering a
// sampled request means it pages now: the console switches back and reads
// the first page with the filters it did not send. Either way the answer
// was asked for in the other shape and is not shown.
export function listAnswerPlan(
  sent: ListMode,
  response: SandboxListResponse,
): { mode: ListMode; reread: boolean } {
  return { mode: response.mode, reread: response.mode !== sent };
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
// "created more than `olderThanMs` ago", at `now`. The server refuses
// bounds that leave no instant between them, which the console's own
// controls never offer; such a pair keeps the window and drops the "older
// than" bound.
export function createdBounds(
  withinMs: number | null,
  olderThanMs: number | null,
  now: number,
): { createdAfter: string | null; createdBefore: string | null } {
  const createdAfter = withinMs === null ? null : new Date(now - withinMs).toISOString();
  const usable = olderThanMs !== null && (withinMs === null || olderThanMs < withinMs);
  return { createdAfter, createdBefore: usable ? new Date(now - olderThanMs).toISOString() : null };
}

// The list requests in flight. Each takes a ticket naming its scope; any
// change of scope invalidates every outstanding ticket. An answer applies
// only when its ticket is the newest and its scope is still the selected
// one, so an answer for a filter that was chosen and then undone before it
// arrived is never shown under the filter now selected.
export class ListRequests {
  private latest = 0;

  begin(scope: ListScope): { id: number; scope: ListScope } {
    this.latest += 1;
    return { id: this.latest, scope };
  }

  invalidate(): void {
    this.latest += 1;
  }

  applies(ticket: { id: number; scope: ListScope }, selected: ListScope): boolean {
    return ticket.id === this.latest && sameScope(ticket.scope, selected);
  }
}

// After the selection changes: the rows on screen are kept when they are the
// selected scope's (a change undone before its answer arrived), and
// otherwise the selected scope is loaded.
export function scopeChangePlan(shown: ListScope | null, selected: ListScope): 'keep' | 'load' {
  return sameScope(shown, selected) ? 'keep' : 'load';
}

// The cursor of the page after the one shown, with the scope (filters,
// size and page position) of the request that returned it. Next uses it
// only while that scope is still selected; a failed load clears it.
export interface Continuation {
  scopeKey: string;
  cursor: string;
}

export function continuationFor(answered: ListScope, nextCursor: string | null): Continuation | null {
  return nextCursor ? { scopeKey: answered.key, cursor: nextCursor } : null;
}

export function nextCursorFor(continuation: Continuation | null, selected: ListScope): string | null {
  return continuation && continuation.scopeKey === selected.key ? continuation.cursor : null;
}
