// The Templates list: one row per template, searched, filtered, sorted and
// paged in the browser, with its state in the URL so a view can be shared and
// survives a reload. The admin API answers every template and every release
// in one read each (`GET /templates`, `GET /template-releases`), so the list
// works on those answers; only the latest build is read per template, for
// the rows a view needs (`buildsToRead`).
import type {
  NodeTemplateInventory,
  Template,
  TemplateBuildListEntry,
  TemplateRelease,
  WarmPoolPolicy,
} from './types';

// ------------------------------------------------------------------ tags

export function isDigestTag(tag: string): boolean {
  return tag.startsWith('sha256:') || /^[a-f0-9]{64}$/i.test(tag);
}

function normalizedDigest(value: string): string {
  return value.replace(/^sha256:/, '');
}

// The tags an operator names: not the digest tags a release carries for its
// own content hash.
export function visibleTemplateTags(tags: string[], contentHash: string | null): string[] {
  const contentDigest = contentHash ? normalizedDigest(contentHash) : null;
  return tags.filter((tag) => {
    if (isDigestTag(tag)) return false;
    return !contentDigest || normalizedDigest(tag) !== contentDigest;
  });
}

// ------------------------------------------------------------------ rows

export type BuildBucket = 'succeeded' | 'failed' | 'running';
export const BUILD_BUCKETS: readonly BuildBucket[] = ['succeeded', 'failed', 'running'];

// A template's newest build as `GET /templates/{name}/builds?limit=1` answers
// it: `none` when it has no recorded build, `unsupported` on an API release
// without the route, `error` when the read failed.
export type LatestBuild =
  | { kind: 'build'; build: TemplateBuildListEntry }
  | { kind: 'none' }
  | { kind: 'unsupported' }
  | { kind: 'error'; message: string };

// The state an operator filters by. A build whose builder cannot report it
// (`state` null) or reports `unspecified` is in no bucket.
export function buildBucket(latest: LatestBuild | undefined): BuildBucket | null {
  if (latest?.kind !== 'build') return null;
  switch (latest.build.state) {
    case 'succeeded':
      return 'succeeded';
    case 'failed':
    case 'publish_failed':
      return 'failed';
    case 'pending':
    case 'running':
      return 'running';
    default:
      return null;
  }
}

export interface TaggedRelease {
  release: TemplateRelease;
  tags: string[];
}

export interface PoolSummary {
  // `cluster`: the release-pinned warm pools of a Kubernetes cluster and its
  // rolling policies. `direct`: the template pool of a direct substrate.
  kind: 'cluster' | 'direct';
  // Pools that are enabled (cluster), or a pool with a target or observed
  // slots (direct).
  active: boolean;
  ready: number;
  target: number;
  // Cluster pools that exist but are disabled, and enabled rolling policies.
  disabled: number;
  policies: number;
}

export interface TemplateListRow {
  name: string;
  template: Template;
  orgId: string | null;
  // This template's releases, newest first.
  releases: TemplateRelease[];
  // Null until the releases have loaded: before that no template can be said
  // to have none.
  published: boolean | null;
  // Releases with an operator-named tag; the one holding `latest` first.
  tagged: TaggedRelease[];
  // The release new sandboxes get by name: the one holding `latest`. A
  // template without it has no current release, and a create by name fails.
  current: TemplateRelease | null;
  // The newest release, which may not be the current one.
  newest: TemplateRelease | null;
  pool: PoolSummary;
  // Reachable nodes holding the current release; null until the node
  // inventory has loaded, or when there is no current release.
  nodeCount: number | null;
}

export interface RowInputs {
  templates: Template[];
  releases: TemplateRelease[];
  releasesLoaded: boolean;
  inventory: NodeTemplateInventory[];
  inventoryLoaded: boolean;
  policies: WarmPoolPolicy[];
  clusterPools: boolean;
  latestTag: string;
}

export function templateListRows(inputs: RowInputs): TemplateListRow[] {
  const releasesByTemplate = new Map<string, TemplateRelease[]>();
  for (const release of inputs.releases) {
    const list = releasesByTemplate.get(release.template_name) ?? [];
    list.push(release);
    releasesByTemplate.set(release.template_name, list);
  }
  for (const list of releasesByTemplate.values()) {
    list.sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at));
  }
  // Which nodes hold each template@hash.
  const holders = new Map<string, Set<string>>();
  for (const entry of inputs.inventory) {
    if (!entry.reachable || entry.error) continue;
    for (const build of entry.builds) {
      const key = `${build.name}\u0000${build.content_hash ?? ''}`;
      const nodes = holders.get(key) ?? new Set<string>();
      nodes.add(entry.node.id);
      holders.set(key, nodes);
    }
  }
  const policiesByTemplate = new Map<string, number>();
  for (const policy of inputs.policies) {
    if (policy.enabled) policiesByTemplate.set(policy.template_name, (policiesByTemplate.get(policy.template_name) ?? 0) + 1);
  }

  return inputs.templates.map((template) => {
    const releases = releasesByTemplate.get(template.name) ?? [];
    const tagged: TaggedRelease[] = [];
    let latestHolder: TemplateRelease | null = null;
    for (const release of releases) {
      const tags = visibleTemplateTags(release.tags, release.content_hash);
      if (tags.length === 0) continue;
      if (tags.includes(inputs.latestTag) && !latestHolder) latestHolder = release;
      tagged.push({ release, tags });
    }
    tagged.sort((a, b) => Number(b.release === latestHolder) - Number(a.release === latestHolder));
    const current = latestHolder;
    const nodeCount = current && inputs.inventoryLoaded
      ? holders.get(`${template.name}\u0000${current.content_hash}`)?.size ?? 0
      : null;
    return {
      name: template.name,
      template,
      orgId: template.org_id,
      releases,
      published: inputs.releasesLoaded ? releases.length > 0 : null,
      tagged,
      current,
      newest: releases[0] ?? null,
      pool: inputs.clusterPools
        ? clusterPoolSummary(releases, policiesByTemplate.get(template.name) ?? 0)
        : directPoolSummary(template),
      nodeCount,
    };
  });
}

function clusterPoolSummary(releases: TemplateRelease[], policies: number): PoolSummary {
  const summary: PoolSummary = { kind: 'cluster', active: policies > 0, ready: 0, target: 0, disabled: 0, policies };
  for (const release of releases) {
    for (const pool of release.warm_pools) {
      if (!pool.enabled) {
        summary.disabled += 1;
        continue;
      }
      summary.active = true;
      summary.ready += pool.ready_slots;
      summary.target += pool.target_ready_slots;
    }
  }
  return summary;
}

function directPoolSummary(template: Template): PoolSummary {
  const current = template.pool_current ?? 0;
  const target = template.pool_target ?? 0;
  return { kind: 'direct', active: current > 0 || target > 0, ready: current, target, disabled: 0, policies: 0 };
}

// ---------------------------------------------------------------- search

export type ReleaseMatchReason = 'tag' | 'hash' | 'id';

export interface ReleaseMatch {
  release: TemplateRelease;
  reason: ReleaseMatchReason;
}

// The shortest hash or release ID prefix looked up: shorter ones match too
// many releases to be a lookup.
export const MIN_HASH_PREFIX = 4;
export const MIN_ID_PREFIX = 8;

function hexDigits(value: string): string {
  return value.replace(/^[a-z0-9]+:/, '');
}

// The releases a search names: by a tag (case-insensitive, whole), a content
// hash prefix (with or without its `<algorithm>:`), or a release ID prefix.
// A search that names none of these returns nothing.
export function releaseMatches(releases: TemplateRelease[], search: string): ReleaseMatch[] {
  const query = search.trim().toLowerCase();
  if (!query) return [];
  const hex = hexDigits(query);
  const hashLookup = hex.length >= MIN_HASH_PREFIX && /^[0-9a-f]+$/.test(hex);
  const idLookup = query.length >= MIN_ID_PREFIX && /^[0-9a-f-]+$/.test(query);
  const matches: ReleaseMatch[] = [];
  for (const release of releases) {
    if (idLookup && release.id.toLowerCase().startsWith(query)) {
      matches.push({ release, reason: 'id' });
    } else if (hashLookup && hexDigits(release.content_hash.toLowerCase()).startsWith(hex)) {
      matches.push({ release, reason: 'hash' });
    } else if (visibleTemplateTags(release.tags, release.content_hash).some((tag) => tag.toLowerCase() === query)) {
      matches.push({ release, reason: 'tag' });
    }
  }
  return matches;
}

// Whether one release (or node copy) matches the search of the All releases
// and Node availability tabs: a case-insensitive substring of its template
// name or of one of its tags, or a prefix of its content hash or release ID.
export function releaseRowMatches(
  row: { template_name: string; content_hash: string | null; tags: string[]; id?: string },
  search: string,
): boolean {
  const query = search.trim().toLowerCase();
  if (!query) return true;
  if (row.template_name.toLowerCase().includes(query)) return true;
  if (row.tags.some((tag) => !isDigestTag(tag) && tag.toLowerCase().includes(query))) return true;
  const hex = hexDigits(query);
  if (row.content_hash && /^[0-9a-f]+$/.test(hex) && hexDigits(row.content_hash.toLowerCase()).startsWith(hex)) return true;
  return Boolean(row.id && row.id.toLowerCase().startsWith(query));
}

// ------------------------------------------------------------ list state

export type SortKey = 'name' | 'org' | 'release' | 'build' | 'pool' | 'nodes';
export type SortOrder = 'ascend' | 'descend';
export const SORT_KEYS: readonly SortKey[] = ['name', 'org', 'release', 'build', 'pool', 'nodes'];
export const PAGE_SIZES = [10, 25, 50, 100] as const;
export const DEFAULT_PAGE_SIZE = 25;

// `org` is `system` (templates without an organization) or an organization ID.
export interface TemplateListState {
  q: string;
  org: string | null;
  release: 'published' | 'unpublished' | null;
  pool: 'with' | 'without' | null;
  build: BuildBucket | null;
  tag: string | null;
  sort: SortKey;
  order: SortOrder;
  page: number;
  size: number;
}

export const DEFAULT_LIST_STATE: TemplateListState = {
  q: '', org: null, release: null, pool: null, build: null, tag: null, sort: 'name', order: 'ascend', page: 1, size: DEFAULT_PAGE_SIZE,
};

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function oneOf<T extends string>(value: string | null, allowed: readonly T[]): T | null {
  return value !== null && (allowed as readonly string[]).includes(value) ? (value as T) : null;
}

// The list state a link names. A value the list cannot apply is dropped, so a
// hand-edited link shows the closest view rather than an empty one.
export function parseListState(params: { get(name: string): string | null }): TemplateListState {
  const raw = (name: string) => params.get(name)?.trim() || null;
  const org = raw('org');
  const page = Number(raw('page'));
  const size = Number(raw('size'));
  return {
    q: params.get('q') ?? '',
    org: org === 'system' ? 'system' : org && UUID.test(org) ? org.toLowerCase() : null,
    release: oneOf(raw('release'), ['published', 'unpublished'] as const),
    pool: oneOf(raw('pool'), ['with', 'without'] as const),
    build: oneOf(raw('build'), BUILD_BUCKETS),
    tag: raw('tag'),
    sort: oneOf(raw('sort'), SORT_KEYS) ?? DEFAULT_LIST_STATE.sort,
    order: oneOf(raw('order'), ['ascend', 'descend'] as const) ?? DEFAULT_LIST_STATE.order,
    page: Number.isInteger(page) && page >= 1 ? page : 1,
    size: (PAGE_SIZES as readonly number[]).includes(size) ? size : DEFAULT_PAGE_SIZE,
  };
}

// The query parameters of a list state, defaults left out.
export function listStateParams(state: TemplateListState): URLSearchParams {
  const params = new URLSearchParams();
  if (state.q) params.set('q', state.q);
  if (state.org) params.set('org', state.org);
  if (state.release) params.set('release', state.release);
  if (state.pool) params.set('pool', state.pool);
  if (state.build) params.set('build', state.build);
  if (state.tag) params.set('tag', state.tag);
  if (state.sort !== DEFAULT_LIST_STATE.sort || state.order !== DEFAULT_LIST_STATE.order) {
    params.set('sort', state.sort);
    params.set('order', state.order);
  }
  if (state.page !== 1) params.set('page', String(state.page));
  if (state.size !== DEFAULT_PAGE_SIZE) params.set('size', String(state.size));
  return params;
}

// A change to the search, a filter or the sort starts again on page 1.
export function withListChange(state: TemplateListState, change: Partial<TemplateListState>): TemplateListState {
  const keepsPage = Object.keys(change).every((key) => key === 'page' || key === 'size');
  return { ...state, ...change, page: keepsPage ? change.page ?? state.page : 1 };
}

// Whether the view needs the latest build of every template that passes the
// other filters, not only of the rows on the page.
export function needsEveryBuild(state: TemplateListState): boolean {
  return state.build !== null || state.sort === 'build';
}

// -------------------------------------------------------------- filtering

export interface FilteredRow extends TemplateListRow {
  // The releases the search named, when the name alone did not match.
  matched: ReleaseMatch[];
}

export interface FilterResult {
  rows: FilteredRow[];
  // Rows left out only because their latest build is not known yet.
  pendingBuilds: number;
  // Rows left out because their latest build could not be read (a failed
  // read, or an API without the builds route).
  unreadBuilds: number;
}

// The rows the search and filters keep, before sorting. A row matches the
// search by a case-insensitive substring of its name, or by one of its
// releases (`releaseMatches`).
export function filterTemplateRows(
  rows: TemplateListRow[],
  state: TemplateListState,
  latestBuilds: ReadonlyMap<string, LatestBuild>,
  matches: ReleaseMatch[] = [],
): FilterResult {
  const query = state.q.trim().toLowerCase();
  const matchedByTemplate = new Map<string, ReleaseMatch[]>();
  for (const match of matches) {
    const list = matchedByTemplate.get(match.release.template_name) ?? [];
    list.push(match);
    matchedByTemplate.set(match.release.template_name, list);
  }
  const kept: FilteredRow[] = [];
  let pendingBuilds = 0;
  let unreadBuilds = 0;
  for (const row of rows) {
    let matched: ReleaseMatch[] = [];
    if (query && !row.name.toLowerCase().includes(query)) {
      matched = matchedByTemplate.get(row.name) ?? [];
      if (matched.length === 0) continue;
    }
    if (state.org === 'system' && row.orgId !== null) continue;
    if (state.org && state.org !== 'system' && row.orgId !== state.org) continue;
    if (state.release === 'published' && row.published !== true) continue;
    if (state.release === 'unpublished' && row.published !== false) continue;
    if (state.pool === 'with' && !row.pool.active) continue;
    if (state.pool === 'without' && row.pool.active) continue;
    if (state.tag && !row.tagged.some((entry) => entry.tags.includes(state.tag!))) continue;
    if (state.build) {
      const latest = latestBuilds.get(row.name);
      if (latest === undefined) {
        pendingBuilds += 1;
        continue;
      }
      if (latest.kind === 'error' || latest.kind === 'unsupported') {
        unreadBuilds += 1;
        continue;
      }
      if (buildBucket(latest) !== state.build) continue;
    }
    kept.push({ ...row, matched });
  }
  return { rows: kept, pendingBuilds, unreadBuilds };
}

// ---------------------------------------------------------------- sorting

const BUCKET_ORDER: Record<BuildBucket, number> = { failed: 0, running: 1, succeeded: 2 };

function compareNullable(a: number | string | null, b: number | string | null, order: SortOrder): number {
  // Unknown values sort last in either order.
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  const sign = order === 'ascend' ? 1 : -1;
  if (typeof a === 'string' && typeof b === 'string') return sign * a.localeCompare(b);
  return sign * ((a as number) - (b as number));
}

function buildSortValue(latest: LatestBuild | undefined): number | null {
  const bucket = buildBucket(latest);
  if (bucket === null || latest?.kind !== 'build') return null;
  // By state, then newest first within a state.
  return BUCKET_ORDER[bucket] * 1e15 - Date.parse(latest.build.created_at);
}

export function sortTemplateRows<T extends TemplateListRow>(
  rows: T[],
  state: Pick<TemplateListState, 'sort' | 'order'>,
  latestBuilds: ReadonlyMap<string, LatestBuild>,
  orgLabel: (orgId: string | null) => string,
): T[] {
  const value = (row: T): number | string | null => {
    switch (state.sort) {
      case 'name':
        return row.name;
      case 'org':
        return orgLabel(row.orgId);
      case 'release':
        return row.current ? Date.parse(row.current.created_at) : null;
      case 'build':
        return buildSortValue(latestBuilds.get(row.name));
      case 'pool':
        return row.pool.active ? row.pool.ready : row.pool.disabled > 0 ? -1 : null;
      case 'nodes':
        return row.nodeCount;
    }
  };
  return [...rows].sort((a, b) => compareNullable(value(a), value(b), state.order) || a.name.localeCompare(b.name));
}

// The page of `total` rows a state shows: a page past the end shows the last.
export function clampPage(page: number, size: number, total: number): number {
  const last = Math.max(1, Math.ceil(total / size));
  return Math.min(Math.max(1, page), last);
}

// The templates whose latest build the view reads: the rows on the page, or
// every row that passes the other filters when the view filters or sorts by
// build.
export function buildsToRead(pageRows: TemplateListRow[], candidates: TemplateListRow[], state: TemplateListState): string[] {
  return (needsEveryBuild(state) ? candidates : pageRows).map((row) => row.name);
}

// The build filter's Retry re-reads the rows whose latest-build read failed
// (an API without the route would refuse again), each at most once a minute
// however often it is pressed. The reads go through the list's read queue.
export const BUILD_RETRY_MS = 60_000;

export function buildsToRetry(
  names: string[],
  latestBuilds: ReadonlyMap<string, LatestBuild>,
  retriedAt: ReadonlyMap<string, number>,
  now: number,
): string[] {
  return names.filter((name) => {
    if (latestBuilds.get(name)?.kind !== 'error') return false;
    const last = retriedAt.get(name);
    return last === undefined || now - last >= BUILD_RETRY_MS;
  });
}

// What an empty list says. It claims that no template matches only when
// every row's latest build was read.
export function listEmptyText(view: {
  readFailed: boolean;
  filtersSet: boolean;
  pendingBuilds: number;
  unreadBuilds: number;
}): string | undefined {
  if (view.readFailed) return 'Not read';
  if (view.unreadBuilds > 0) return `No match among the templates read; ${view.unreadBuilds} could not be read`;
  if (view.pendingBuilds > 0) return 'Reading the latest builds…';
  return view.filtersSet ? 'No template matches the search and filters' : undefined;
}
