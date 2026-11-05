import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  DEFAULT_LIST_STATE,
  ageText,
  buildBucket,
  buildsToRead,
  clampPage,
  filterTemplateRows,
  listStateParams,
  parseListState,
  releaseMatches,
  releaseRowMatches,
  sortTemplateRows,
  templateListRows,
  visibleTemplateTags,
  withListChange,
} from './template-list.ts';

const ORG = '11111111-2222-3333-4444-555555555555';
const HASH_A = `sha256:${'a1'.repeat(32)}`;
const HASH_B = `sha256:${'b2'.repeat(32)}`;
const HASH_C = `sha256:${'c3'.repeat(32)}`;

const template = (name, extra = {}) => ({ name, org_id: null, pool_current: null, pool_target: null, ...extra });
const release = (id, template_name, content_hash, tags, created_at, warm_pools = []) => ({
  id, template_name, runtime: 'fc', content_hash, tags, warm_pools, created_at,
});
const pool = (enabled, ready, target) => ({ id: `p-${ready}-${target}`, enabled, ready_slots: ready, target_ready_slots: target });
const build = (state, created_at) => ({ kind: 'build', build: { job_id: 'j', template_name: 't', created_at, state } });
const node = (id, builds, reachable = true) => ({ node: { id }, reachable, error: null, builds });

const TEMPLATES = [
  template('seed'),
  template('acme-web', { org_id: ORG }),
  template('acme-worker', { org_id: ORG }),
  template('empty'),
];
const RELEASES = [
  release('aaaaaaaa-0000-0000-0000-000000000001', 'seed', HASH_A, ['latest', HASH_A], '2026-11-01T00:00:00Z'),
  release('bbbbbbbb-0000-0000-0000-000000000002', 'seed', HASH_B, ['canary'], '2026-11-03T00:00:00Z'),
  release('cccccccc-0000-0000-0000-000000000003', 'acme-web', HASH_C, [], '2026-11-02T00:00:00Z', [pool(true, 2, 3)]),
  release('dddddddd-0000-0000-0000-000000000004', 'acme-worker', HASH_B, ['prod'], '2026-11-02T00:00:00Z', [pool(false, 0, 1)]),
];
const INVENTORY = [
  node('n1', [{ name: 'seed', content_hash: HASH_A }, { name: 'acme-web', content_hash: HASH_C }]),
  node('n2', [{ name: 'seed', content_hash: HASH_A }]),
  node('n3', [{ name: 'seed', content_hash: HASH_A }], false),
];

const rows = (overrides = {}) => templateListRows({
  templates: TEMPLATES,
  releases: RELEASES,
  releasesLoaded: true,
  inventory: INVENTORY,
  inventoryLoaded: true,
  policies: [],
  clusterPools: true,
  latestTag: 'latest',
  ...overrides,
});
const byName = (list) => Object.fromEntries(list.map((row) => [row.name, row]));
const names = (list) => list.map((row) => row.name);
const state = (change) => ({ ...DEFAULT_LIST_STATE, ...change });

test('visible tags leave out digest tags and the release digest', () => {
  assert.deepEqual(visibleTemplateTags(['latest', HASH_A, 'a1'.repeat(32), 'prod'], HASH_A), ['latest', 'prod']);
});

test('the current release is the latest holder, else the newest; nodes count only reachable holders of it', () => {
  const r = byName(rows());
  assert.equal(r.seed.current.id, RELEASES[0].id, 'latest wins over the newer canary release');
  assert.deepEqual(r.seed.tagged.map((entry) => entry.tags), [['latest'], ['canary']]);
  assert.equal(r.seed.nodeCount, 2, 'n3 is unreachable');
  assert.equal(r['acme-web'].current.id, RELEASES[2].id, 'an untagged template falls back to its newest release');
  assert.equal(r['acme-web'].nodeCount, 1);
  assert.equal(r.empty.current, null);
  assert.equal(r.empty.nodeCount, null);
  assert.equal(r.empty.published, false);
});

test('before the releases or the inventory load, nothing is claimed about them', () => {
  const r = byName(rows({ releases: [], releasesLoaded: false, inventoryLoaded: false }));
  assert.equal(r.seed.published, null);
  assert.equal(r.seed.nodeCount, null);
});

test('pool status counts enabled cluster pools and policies, or the direct template pool', () => {
  const r = byName(rows({ policies: [{ template_name: 'seed', enabled: true }, { template_name: 'empty', enabled: false }] }));
  assert.deepEqual([r['acme-web'].pool.active, r['acme-web'].pool.ready, r['acme-web'].pool.target], [true, 2, 3]);
  assert.deepEqual([r['acme-worker'].pool.active, r['acme-worker'].pool.disabled], [false, 1]);
  assert.equal(r.seed.pool.active, true, 'an enabled rolling policy is a pool');
  assert.equal(r.empty.pool.active, false, 'a disabled policy is not');
  const direct = byName(rows({ clusterPools: false, templates: [template('seed', { pool_current: 1, pool_target: 4 }), template('empty')] }));
  assert.deepEqual([direct.seed.pool.kind, direct.seed.pool.active, direct.seed.pool.ready, direct.seed.pool.target], ['direct', true, 1, 4]);
  assert.equal(direct.empty.pool.active, false);
});

test('a search names releases by tag, content hash prefix or release ID prefix', () => {
  const reasons = (q) => releaseMatches(RELEASES, q).map((match) => `${match.release.template_name}:${match.reason}`);
  assert.deepEqual(reasons('PROD'), ['acme-worker:tag'], 'tags match whole and case-insensitively');
  assert.deepEqual(reasons('pro'), [], 'not by a tag prefix');
  assert.deepEqual(reasons('b2b2'), ['seed:hash', 'acme-worker:hash']);
  assert.deepEqual(reasons('sha256:c3c3c3'), ['acme-web:hash'], 'with its algorithm prefix');
  assert.deepEqual(reasons('a1a'), [], 'a hash prefix shorter than four digits looks nothing up');
  assert.deepEqual(reasons('dddddddd-0000'), ['acme-worker:id']);
  assert.deepEqual(reasons('seed'), []);
});

test('the search keeps name substrings and the templates of matching releases', () => {
  const all = rows();
  const run = (q) => filterTemplateRows(all, state({ q }), new Map(), releaseMatches(RELEASES, q));
  assert.deepEqual(names(run('ACME').rows), ['acme-web', 'acme-worker']);
  const byTag = run('canary');
  assert.deepEqual(names(byTag.rows), ['seed']);
  assert.equal(byTag.rows[0].matched[0].reason, 'tag');
  assert.deepEqual(names(run('c3c3').rows), ['acme-web']);
  assert.deepEqual(byName(run('seed').rows).seed.matched, [], 'a name match lists no release match');
});

test('every filter narrows the rows', () => {
  const all = rows();
  const kept = (change, builds = new Map()) => names(filterTemplateRows(all, state(change), builds).rows);
  assert.deepEqual(kept({ org: 'system' }), ['seed', 'empty']);
  assert.deepEqual(kept({ org: ORG }), ['acme-web', 'acme-worker']);
  assert.deepEqual(kept({ release: 'published' }), ['seed', 'acme-web', 'acme-worker']);
  assert.deepEqual(kept({ release: 'unpublished' }), ['empty']);
  assert.deepEqual(kept({ pool: 'with' }), ['acme-web']);
  assert.deepEqual(kept({ pool: 'without' }), ['seed', 'acme-worker', 'empty']);
  assert.deepEqual(kept({ tag: 'prod' }), ['acme-worker']);
  const builds = new Map([
    ['seed', build('succeeded', '2026-11-04T00:00:00Z')],
    ['acme-web', build('publish_failed', '2026-11-04T00:00:00Z')],
    ['acme-worker', build('pending', '2026-11-04T00:00:00Z')],
  ]);
  assert.deepEqual(kept({ build: 'failed' }, builds), ['acme-web']);
  assert.deepEqual(kept({ build: 'running' }, builds), ['acme-worker']);
  const partial = filterTemplateRows(all, state({ build: 'succeeded' }), builds);
  assert.deepEqual([names(partial.rows), partial.pendingBuilds], [['seed'], 1], 'a template whose build is unread is pending, not dropped silently');
});

test('a build the builder cannot report is in no build bucket', () => {
  assert.equal(buildBucket(build(null, '2026-11-04T00:00:00Z')), null);
  assert.equal(buildBucket(build('unspecified', '2026-11-04T00:00:00Z')), null);
  assert.equal(buildBucket({ kind: 'none' }), null);
  assert.equal(buildBucket({ kind: 'unsupported' }), null);
});

test('sorting puts unknown values last in either order, then by name', () => {
  const all = rows();
  const orgLabel = (id) => (id ? 'Acme' : 'System');
  const sorted = (sort, order, builds = new Map()) => names(sortTemplateRows(all, { sort, order }, builds, orgLabel));
  assert.deepEqual(sorted('name', 'descend'), ['seed', 'empty', 'acme-worker', 'acme-web']);
  assert.deepEqual(sorted('org', 'ascend'), ['acme-web', 'acme-worker', 'empty', 'seed']);
  assert.deepEqual(sorted('nodes', 'descend'), ['seed', 'acme-web', 'acme-worker', 'empty']);
  assert.deepEqual(sorted('nodes', 'ascend'), ['acme-worker', 'acme-web', 'seed', 'empty']);
  assert.deepEqual(sorted('release', 'descend'), ['acme-web', 'acme-worker', 'seed', 'empty']);
  const builds = new Map([
    ['seed', build('succeeded', '2026-11-04T00:00:00Z')],
    ['acme-web', build('failed', '2026-11-03T00:00:00Z')],
    ['acme-worker', build('failed', '2026-11-04T00:00:00Z')],
  ]);
  assert.deepEqual(sorted('build', 'ascend', builds), ['acme-worker', 'acme-web', 'seed', 'empty'], 'failed first, newest first within a state');
});

test('the list state round-trips through the URL and drops what it cannot apply', () => {
  const shared = state({ q: 'acme', org: ORG, release: 'published', pool: 'with', build: 'failed', tag: 'prod', sort: 'nodes', order: 'descend', page: 3, size: 50 });
  assert.deepEqual(parseListState(listStateParams(shared)), shared);
  assert.equal(listStateParams(DEFAULT_LIST_STATE).toString(), '', 'the default view has a bare URL');
  const odd = parseListState(new URLSearchParams('org=acme&release=maybe&build=queued&sort=size&order=up&page=-2&size=7'));
  assert.deepEqual(odd, DEFAULT_LIST_STATE);
  assert.equal(parseListState(new URLSearchParams('org=system')).org, 'system');
});

test('a change to the search, a filter or the sort goes back to page 1; paging keeps it', () => {
  const on3 = state({ page: 3 });
  assert.equal(withListChange(on3, { q: 'x' }).page, 1);
  assert.equal(withListChange(on3, { sort: 'org' }).page, 1);
  assert.equal(withListChange(on3, { size: 50 }).page, 3);
  assert.equal(withListChange(on3, { page: 4 }).page, 4);
  assert.equal(clampPage(9, 25, 60), 3);
  assert.equal(clampPage(2, 25, 0), 1);
});

test('only the page rows read their build, unless the view filters or sorts by build', () => {
  const all = rows();
  const page = all.slice(0, 2);
  assert.deepEqual(buildsToRead(page, all, DEFAULT_LIST_STATE), ['seed', 'acme-web']);
  assert.deepEqual(buildsToRead(page, all, state({ build: 'failed' })), names(all));
  assert.deepEqual(buildsToRead(page, all, state({ sort: 'build' })), names(all));
});

test('ages are shown in their largest whole unit', () => {
  const now = Date.parse('2026-11-05T12:00:00Z');
  assert.equal(ageText('2026-11-05T11:59:30Z', now), 'just now');
  assert.equal(ageText('2026-11-05T11:15:00Z', now), '45m ago');
  assert.equal(ageText('2026-11-04T12:00:00Z', now), '24h ago');
  assert.equal(ageText('2026-11-01T12:00:00Z', now), '4d ago');
  assert.equal(ageText('not a time', now), '—');
});

test('the releases search matches a template substring, a tag substring, a hash prefix or an ID prefix', () => {
  const [latest, canary] = RELEASES;
  const matches = (row, q) => releaseRowMatches(row, q);
  assert.equal(matches(latest, ''), true);
  assert.equal(matches(latest, 'EE'), true, 'seed');
  assert.equal(matches(canary, 'anar'), true);
  assert.equal(matches(latest, 'a1a1'), true);
  assert.equal(matches(latest, 'sha256:a1a1'), true);
  assert.equal(matches(latest, '1a1a'), false, 'a hash matches by prefix only');
  assert.equal(matches(latest, 'sha256'), false, 'not by the digest tag the release carries');
  assert.equal(matches(latest, 'aaaaaaaa-0000'), true);
  assert.equal(matches({ template_name: 'seed', content_hash: null, tags: [] }, 'a1'), false, 'a node copy without a hash');
});
