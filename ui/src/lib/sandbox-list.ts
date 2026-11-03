import type { Sandbox } from './types';

// The admin sandbox list answers in one of two shapes. With `cursor` (empty
// for the first page) a server that pages answers `{sandboxes, next_cursor}`
// and applies the filters below. A server from before paging ignores
// `cursor` and every filter except `org_id` and answers a bare array of the
// newest `limit` rows; the console then samples, as it did before paging.
// The first answer of a page view decides which (`ListSelection.load`).
// During a rolling deploy, or its rollback, replicas of both versions answer
// in turn. A list never joins rows of two shapes and keeps the rows on screen
// through a mismatch: a bare array answering a paged request is a failed
// refresh, and a sampled view switches to paging only once a page 1 read
// with every filter answers. Only when every answer for
// `STOP_AFTER_MISMATCH_MS` has been such a bare array, at least
// `STOP_AFTER_MISMATCHES` of them, does the list stop until the page is
// reloaded.
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
// always narrows the loaded rows only, and a search the list does not send
// (`page` and `fragment` in `sandboxSearch`) is matched here too. A sampled
// list was sent only `org_id`, so every other filter applies here.
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

// The request of a `fragment` search (`sandboxSearch`): the selected page's
// filters with the search as `prefix`, read from the first page at the page
// size. The list itself is read without it, so the search still matches ID
// suffixes, such as the ID column's label, within the page on screen. Null on
// a sampled list, which sends no prefix.
export function fragmentSearchScope(selected: ListScope, prefix: string): ListScope | null {
  if (selected.mode !== 'paged') return null;
  return listScope('paged', { ...selected.filters, prefix }, { limit: selected.limit, cursor: '' });
}

// After the selection changes: the rows on screen are kept when they are the
// selected scope's (a change undone before its answer arrived), and
// otherwise the selected scope is loaded.
export function scopeChangePlan(shown: ListScope | null, selected: ListScope): 'keep' | 'load' {
  return sameScope(shown, selected) ? 'keep' : 'load';
}

// The cursor of the page after the one shown, with the scope (filters,
// size and page position) of the request that returned it. Next uses it
// only while that scope is still selected, and loses it with the rows it was
// returned with: a failed refresh of the page on screen keeps both.
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

// A bare array answering a paged request: an older API version answered,
// as during a rolling deploy or its rollback. The load counts as failed.
export class OlderFormatAnswer extends Error {
  constructor() {
    super('the API answered in an older format; retrying');
  }
}

// How long bare arrays answering paged requests must go on, with no other
// answer in between and at least `STOP_AFTER_MISMATCHES` of them, to show
// that the API went back to a version without paging. A failed refresh is
// retried on the next one-second check, so a count alone would be reached
// within seconds while some replicas are old. A minute of nothing else, at
// that rate, means no replica that pages is answering.
export const STOP_AFTER_MISMATCH_MS = 60_000;
export const STOP_AFTER_MISMATCHES = 3;

// The outcome of `ListSelection.load`.
// - `shown`: the answer is the selected scope's, in the shape asked for.
// - `skipped`: the selection changed while it was in flight, or a newer load
//   started; nothing is shown and `loading` is left to that load.
// - `stopped`: only bare arrays answered paged requests for
//   `STOP_AFTER_MISMATCH_MS`, in this load or earlier; nothing more is read
//   until the page is reloaded. `kept` says whether the rows on screen are
//   the selected scope's; when they are not they must be cleared.
// - `failed`: the request failed, or a bare array answered a paged request
//   (`OlderFormatAnswer`). `scope` is the selected scope, which a sampled
//   view keeps when its switch to paging failed. `kept` says whether the
//   rows on screen are that scope's; when they are not they must be cleared
//   rather than shown under its label.
export type ListLoad =
  | { kind: 'shown'; scope: ListScope; response: SandboxListResponse }
  | { kind: 'skipped' }
  | { kind: 'stopped'; kept: boolean }
  | { kind: 'failed'; scope: ListScope; error: unknown; kept: boolean };

// What an answer in `answered` shape to a request in `asked` shape does to
// the list (`ListSelection.noteAnswer`). `undecided`: it came before the
// first answer of the list decided its shape, and says nothing.
export type AnswerNote =
  | { kind: 'counted' }
  | { kind: 'mismatched' }
  | { kind: 'stopped'; kept: boolean }
  | { kind: 'undecided' };

// The sandbox list's selection and its loads. The list shape, the filters,
// the page and sample sizes and the page position make up the selected
// scope; `shown` is the scope of the rows on screen, and the continuation
// is the cursor of the page after it. The list shape is the first answer's,
// until a page 1 read with every filter answers a sampled view.
// `mismatches` counts the bare arrays in a row that answered paged requests,
// the list's and the prefix search's, and `mismatchedSince` is when the
// first of them answered, by `now`; `stopped` is set when they have gone on
// long enough (`noteAnswer`).
export class ListSelection {
  mode: ListMode = 'paged';
  position: PagePosition = FIRST_PAGE;
  shown: ListScope | null = null;
  stopped = false;
  mismatches = 0;
  private mismatchedSince = 0;
  private decided = false;
  private filters: SandboxListFilters;
  private pageSize: number;
  private sampleSize: number;
  private continuation: Continuation | null = null;
  private readonly requests = new ListRequests();
  private readonly now: () => number;

  constructor({ filters, pageSize, sampleSize, now = Date.now }: {
    filters: SandboxListFilters;
    pageSize: number;
    sampleSize: number;
    now?: () => number;
  }) {
    this.filters = filters;
    this.pageSize = pageSize;
    this.sampleSize = sampleSize;
    this.now = now;
  }

  get selected(): ListScope {
    const limit = this.mode === 'paged' ? this.pageSize : this.sampleSize;
    return listScope(this.mode, this.filters, { limit, cursor: pageCursor(this.position) });
  }

  // A change of the filters or sizes. It restarts at the first page. When
  // the selected request is the same (a sampled list sends only the org and
  // the sample size) nothing else changes. Otherwise the loads in flight are
  // invalidated; the rows on screen are kept when they are the new scope's
  // (a change undone before its answer arrived), and otherwise it is loaded.
  select(next: { filters: SandboxListFilters; pageSize: number; sampleSize: number }): 'unchanged' | 'keep' | 'load' {
    const before = this.selected;
    this.filters = next.filters;
    this.pageSize = next.pageSize;
    this.sampleSize = next.sampleSize;
    this.position = FIRST_PAGE;
    if (sameScope(before, this.selected)) return 'unchanged';
    this.requests.invalidate();
    return scopeChangePlan(this.shown, this.selected);
  }

  // Previous or Next; the caller loads the page.
  goTo(position: PagePosition | null): boolean {
    if (!position) return false;
    this.position = position;
    this.requests.invalidate();
    return true;
  }

  // Whether the first answer decided the list shape. Until then the page
  // assumes paging only to make its first request, so a request made
  // alongside the list (the prefix search) waits for it.
  get shapeDecided(): boolean {
    return this.decided;
  }

  previousPosition(): PagePosition | null {
    return previousPage(this.position);
  }

  nextCursor(): string | null {
    return nextCursorFor(this.continuation, this.selected);
  }

  nextPosition(): PagePosition | null {
    return nextPage(this.position, this.nextCursor());
  }

  // Counts an answer, of the list or of a request made alongside it (the
  // prefix search), once the first answer decided the shape; an answer
  // before then neither counts nor starts over (`undecided`). A bare array
  // answering a paged request is a mismatch. The list stops when mismatches
  // alone have answered for `STOP_AFTER_MISMATCH_MS` since the first of
  // them, and there were at least `STOP_AFTER_MISMATCHES`. Any other answer
  // starts over: a page shows that a version that pages answered, and a
  // bare array is what a sampled request asks for. Failed requests neither
  // count nor start over.
  noteAnswer(asked: ListMode, answered: ListMode): AnswerNote {
    if (this.stopped) return this.stop();
    if (!this.decided) return { kind: 'undecided' };
    if (asked === 'sampled' || answered === 'paged') {
      this.mismatches = 0;
      return { kind: 'counted' };
    }
    const now = this.now();
    if (this.mismatches === 0) this.mismatchedSince = now;
    this.mismatches += 1;
    if (this.mismatches < STOP_AFTER_MISMATCHES || now - this.mismatchedSince < STOP_AFTER_MISMATCH_MS) {
      return { kind: 'mismatched' };
    }
    this.stopped = true;
    return this.stop();
  }

  // Load `scope` with `read`. A load whose scope is no longer selected when
  // it starts or answers is skipped. The first answer decides the list shape:
  // a page keeps paging, and a bare array, the answer of a server without
  // paging, switches to sampling and reads the sample. Later:
  // - A bare array answering a paged list fails the load (`noteAnswer`); the
  //   rows and the Next of the page on screen are kept.
  // - A page answering a sampled list reads the first page with every filter,
  //   since the sample request sent only the org. Only when that page answers,
  //   for the filters chosen by then, does the list switch to paging, and the
  //   page replaces the sample; until then the sample stays, and the load
  //   fails.
  async load(scope: ListScope, read: (scope: ListScope) => Promise<SandboxListResponse>): Promise<ListLoad> {
    if (this.stopped) return this.stop();
    if (!sameScope(this.selected, scope)) return { kind: 'skipped' };
    const ticket = this.requests.begin(scope);
    const answer = await this.read(ticket, scope, read);
    if (answer.kind !== 'answered') return answer;
    const { response } = answer;
    if (!this.decided) {
      this.decided = true;
      if (response.mode !== scope.mode) {
        // The first answer is a bare array, whose rows are the newest at the
        // page size, not the sample.
        this.mode = response.mode;
        this.position = FIRST_PAGE;
        return this.load(this.selected, read);
      }
    }
    const note = this.noteAnswer(scope.mode, response.mode);
    if (note.kind === 'stopped') return note;
    if (note.kind === 'mismatched') return this.fail(scope, new OlderFormatAnswer());
    if (response.mode === scope.mode) return this.show(scope, response);
    // A page answering a sampled list. The sample's ticket keeps the read
    // valid only while the sample is still selected. That ticket survives a
    // change of a filter the sample does not send, so a first page read for
    // filters changed meanwhile is read again for the filters now chosen.
    for (;;) {
      const firstPage = this.firstPage();
      const reread = await this.read(ticket, firstPage, read);
      if (reread.kind !== 'answered') return reread;
      const upgrade = this.noteAnswer('paged', reread.response.mode);
      if (upgrade.kind === 'stopped') return upgrade;
      if (upgrade.kind === 'mismatched') return this.fail(scope, new OlderFormatAnswer());
      if (!sameScope(firstPage, this.firstPage())) continue;
      this.mode = 'paged';
      this.position = FIRST_PAGE;
      return this.show(firstPage, reread.response);
    }
  }

  // The first page with every filter, at the page size.
  private firstPage(): ListScope {
    return listScope('paged', this.filters, { limit: this.pageSize, cursor: '' });
  }

  // One read for `ticket`: its answer, or a failed or skipped load. A failure
  // is the failure of the ticket's scope, the one selected.
  private async read(
    ticket: { id: number; scope: ListScope },
    scope: ListScope,
    read: (scope: ListScope) => Promise<SandboxListResponse>,
  ): Promise<{ kind: 'answered'; response: SandboxListResponse } | ListLoad> {
    let response: SandboxListResponse;
    try {
      response = await read(scope);
    } catch (error) {
      if (!this.requests.applies(ticket, this.selected)) return { kind: 'skipped' };
      return this.fail(ticket.scope, error);
    }
    if (!this.requests.applies(ticket, this.selected)) return { kind: 'skipped' };
    return { kind: 'answered', response };
  }

  private show(scope: ListScope, response: SandboxListResponse): ListLoad {
    this.shown = scope;
    this.continuation = continuationFor(scope, response.mode === 'paged' ? response.nextCursor : null);
    return { kind: 'shown', scope, response };
  }

  private fail(scope: ListScope, error: unknown): ListLoad {
    const kept = sameScope(this.shown, scope);
    if (!kept) {
      // The rows on screen are another scope's; they go, and their Next with them.
      this.shown = null;
      this.continuation = null;
    }
    return { kind: 'failed', scope, error, kept };
  }

  private stop(): { kind: 'stopped'; kept: boolean } {
    this.continuation = null;
    const kept = sameScope(this.shown, this.selected);
    if (!kept) this.shown = null;
    return { kind: 'stopped', kept };
  }
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
