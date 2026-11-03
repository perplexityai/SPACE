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

// A template name as the server parses one (`TemplateName` in
// crates/libs/common/core/src/types/template/identity.rs); it refuses any
// other `template` with a 400.
export function isTemplateName(value: string): boolean {
  return value.length <= 255 && value !== '.' && value !== '..' && /^[A-Za-z0-9._-]+$/.test(value);
}

export type AgeWindow = CreatedAge | 'all';
export type OlderThan = CreatedAge | 'any';
export type NodeFilter = 'any' | 'assigned' | 'unassigned';

const isCreatedAge = (value: string): value is CreatedAge => Object.prototype.hasOwnProperty.call(CREATED_AGES, value);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// The filters a list link sets, each one the list can apply, and the
// parameters left out because the list could not: a value no control offers,
// a template or user ID the server would refuse, or an "older than" bound
// that leaves no time inside the window. The status values are kept as
// given; which of them a list applies depends on its shape (see
// `splitStatusFilter`). `org` is parsed by `parseOrgParam`.
export interface ListLink {
  template: string | null;
  statuses: string[];
  user: string | null;
  node: NodeFilter;
  age: AgeWindow;
  older: OlderThan;
  pageSize: number;
  ignored: { param: string; value: string; reason: string }[];
}

export function parseListLink(params: { get(name: string): string | null }): ListLink {
  const ignored: ListLink['ignored'] = [];
  const raw = (param: string) => {
    const value = params.get(param)?.trim() ?? '';
    return value || null;
  };
  const ignore = (param: string, value: string, reason: string) => ignored.push({ param, value, reason });

  // One template, which is what the server filters on; an older link may
  // list several, and the first is kept.
  let template: string | null = null;
  const templates = raw('template')?.split(',').map((part) => part.trim()).filter(Boolean) ?? [];
  if (templates.length) {
    if (isTemplateName(templates[0])) template = templates[0];
    else ignore('template', templates[0], 'not a template name');
    if (templates.length > 1) ignore('template', templates.slice(1).join(','), 'the list filters on one template');
  }

  const statuses = [...new Set(raw('status')?.split(',').map((part) => part.trim().toLowerCase()).filter(Boolean) ?? [])];

  let user: string | null = null;
  const rawUser = raw('user');
  if (rawUser && UUID.test(rawUser)) user = rawUser.toLowerCase();
  else if (rawUser) ignore('user', rawUser, 'not a user ID');

  let node: NodeFilter = 'any';
  const rawNode = raw('node');
  if (rawNode === 'any' || rawNode === 'assigned' || rawNode === 'unassigned') node = rawNode;
  else if (rawNode) ignore('node', rawNode, 'not a node filter');

  let age: AgeWindow = 'all';
  const rawAge = raw('age');
  if (rawAge && (rawAge === 'all' || isCreatedAge(rawAge))) age = rawAge;
  else if (rawAge) ignore('age', rawAge, 'not a creation window');

  let older: OlderThan = 'any';
  const rawOlder = raw('older');
  if (rawOlder && isCreatedAge(rawOlder)) {
    if (age === 'all' || CREATED_AGES[rawOlder] < CREATED_AGES[age]) older = rawOlder;
    else ignore('older', rawOlder, `leaves no time inside the ${age} window`);
  } else if (rawOlder && rawOlder !== 'any') {
    ignore('older', rawOlder, 'not an age');
  }

  const rawPageSize = raw('page_size');
  const pageSize = parsePageSize(rawPageSize);
  if (rawPageSize && String(pageSize) !== rawPageSize) ignore('page_size', rawPageSize, 'not an offered page size');

  return { template, statuses, user, node, age, older, pageSize, ignored };
}

// Whether a paged request sends any filter. The server lists the rows nodes
// report with no database row (warm-pool slots, observed-local sandboxes),
// which belong to no org and carry none of the filtered fields, once: on the
// first page of the list with no filter (`is_unfiltered` on the admin list).
export function sendsListFilter(filters: SandboxListFilters): boolean {
  return listScope('paged', filters, { limit: 1, cursor: '' }).key
    !== listScope('paged', NO_FILTERS, { limit: 1, cursor: '' }).key;
}

// Where the node-reported rows of a paged list are: on this page, after its
// sandboxes; left out by a filter; or on the first page. A full ID in the
// search box shows the row it names on its own, of either kind, and the
// list's sections are not shown (`looked-up`).
export function nodeReportedPlacement(
  scope: ListScope,
  exactLookup: boolean,
): 'here' | 'filtered' | 'first-page' | 'looked-up' {
  if (exactLookup) return 'looked-up';
  if (sendsListFilter(scope.filters)) return 'filtered';
  return scope.cursor === '' ? 'here' : 'first-page';
}

export function splitNodeReported<T extends { resource_type?: string }>(rows: T[]): { sandboxes: T[]; nodeReported: T[] } {
  return {
    sandboxes: rows.filter((row) => row.resource_type === 'sandbox'),
    nodeReported: rows.filter((row) => row.resource_type !== 'sandbox'),
  };
}

// The rows of the main table and of the node-reported section. A page
// splits them; a sample, and the row a full ID found, whatever its kind,
// are all in the main table.
export function listSections<T extends { resource_type?: string }>(
  mode: ListMode,
  rows: T[],
  exactLookup: boolean,
): { sandboxes: T[]; nodeReported: T[] } {
  if (mode === 'sampled' || exactLookup) return { sandboxes: rows, nodeReported: [] };
  return splitNodeReported(rows);
}

// The template filter's options. `admin` is the admin template list, every
// template the server knows, or null when it could not be read; the options
// are then only the templates of the loaded rows and of the operator's own
// organization, and `partial` says so. Either way the loaded rows' templates
// are included, since a row may name a template since deleted, and so is the
// selected one, and a template name typed into the search can be chosen.
export function templateFilterOptions({
  admin,
  loaded,
  own,
  selected,
  typed,
}: {
  admin: string[] | null;
  loaded: (string | null | undefined)[];
  own: string[];
  selected: string | null;
  typed: string;
}): { names: string[]; typed: string | null; partial: boolean } {
  const names = new Set<string>([...(admin ?? own)]);
  for (const name of loaded) if (name) names.add(name);
  if (selected) names.add(selected);
  const search = typed.trim();
  return {
    names: [...names].sort(),
    typed: search && isTemplateName(search) && !names.has(search) ? search : null,
    partial: admin === null,
  };
}
