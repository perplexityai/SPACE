import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  SPACED_SAMPLE_LIMIT,
  UNKNOWN_SPACED,
  loadSpacedCounts,
  spacedCounts,
  spacedCountsFromServer,
  spacedCoverage,
} from './spaced-counts.ts';

const row = (template, spaced_version, status = 'started', created_at = '2026-10-30T00:00:00Z', resource_type = 'sandbox') => (
  { template, spaced_version, status, lifecycle: status, created_at, resource_type }
);
const iso = (value) => value;

test('running sandboxes are counted per template and reported spaced version, deleted rows left out', () => {
  const counts = spacedCounts([
    row('seed', '0.8.3', 'started', '2026-10-31T00:00:00Z'),
    row('seed', '0.8.3'),
    row('seed', null),
    row('seed', '0.8.3', 'stopped', '2026-10-29T00:00:00Z'),
    row('seed', '0.8.3', 'deleted', '2026-01-01T00:00:00Z'),
    row('code-runtime', ' 0.8.2 '),
    row('code-runtime', '0.8.2', 'started', null, 'pool_slot'),
  ]);
  assert.deepEqual([...counts.byTemplate.get('seed')], [['0.8.3', 2], [UNKNOWN_SPACED, 1]]);
  assert.deepEqual([...counts.byTemplate.get('code-runtime')], [['0.8.2', 2]]);
  assert.equal(counts.running, 5);
  // Database rows only: the node-reported row is counted apart.
  assert.equal(counts.listed, 5);
  assert.equal(counts.nodeReported, 1);
  // The oldest listed row, not a deleted one.
  assert.equal(counts.oldestCreatedAt, '2026-10-29T00:00:00Z');
  assert.equal(counts.truncated, false);
  assert.equal(
    spacedCoverage(counts, iso).split(',')[0] + ',' + spacedCoverage(counts, iso).split(',')[1],
    'Running sandboxes among the newest 5 listed, created since 2026-10-29T00:00:00Z',
  );
  assert.match(spacedCoverage(counts, iso), /Also counted: 1 row nodes report without a database row/);
  assert.match(spacedCoverage(counts, iso), /A template with no running sandbox among them does not appear/);
  assert.doesNotMatch(spacedCoverage(counts, iso), /long-lived/);
});

test('the list is capped at 5000 database rows; node-reported rows do not count toward it', () => {
  const rows = (n) => Array.from({ length: n }, () => row('seed', '0.8.3'));
  const slots = Array.from({ length: 20 }, () => row('seed', '0.8.3', 'started', null, 'pool_slot'));
  assert.equal(SPACED_SAMPLE_LIMIT, 5000);
  assert.equal(spacedCounts([...rows(4999), ...slots]).truncated, false);
  const capped = spacedCounts([...rows(5000), ...slots]);
  assert.equal(capped.truncated, true);
  assert.match(spacedCoverage(capped, iso), /newest 5,000 listed/);
  assert.match(spacedCoverage(capped, iso), /Also counted: 20 rows nodes report/);
  assert.match(spacedCoverage(capped, iso), /long-lived sandboxes created earlier/);
});

test('node-reported rows do not move the cutoff', () => {
  const counts = spacedCounts([
    row('seed', '0.8.3', 'started', '2026-10-31T00:00:00Z'),
    row('seed', '0.8.3', 'started', '2020-01-01T00:00:00Z', 'pool_slot'),
  ]);
  assert.equal(counts.oldestCreatedAt, '2026-10-31T00:00:00Z');
});

test('the cap warning stays when no listed row is counted', () => {
  const deleted = Array.from({ length: 5000 }, () => row('seed', '0.8.3', 'deleted'));
  const counts = spacedCounts(deleted);
  assert.equal(counts.listed, 0);
  assert.equal(counts.truncated, true);
  assert.match(spacedCoverage(counts, iso), /returned no sandboxes that are not deleted/);
  assert.match(spacedCoverage(counts, iso), /long-lived sandboxes created earlier/);
});

test('an empty list says so', () => {
  assert.equal(spacedCoverage(spacedCounts([]), iso), 'The admin sandbox list returned no sandboxes that are not deleted.');
});

const group = (template, spaced_version, status, count, resource_type = 'sandbox') => (
  { template, spaced_version, status, resource_type, count }
);

test('the server count tallies the same population as the sampled list, with no cap', () => {
  const groups = [
    group('seed', '0.8.3', 'started', 2),
    group('seed', null, 'started', 1),
    group('seed', '0.8.3', 'stopped', 1),
    group('code-runtime', ' 0.8.2 ', 'started', 1),
    group('code-runtime', '0.8.2', 'started', 1, 'pool_slot'),
  ];
  // The rows the same sandboxes give in the list, plus a deleted one the
  // server leaves out.
  const rows = [
    row('seed', '0.8.3'), row('seed', '0.8.3'), row('seed', null), row('seed', '0.8.3', 'stopped'),
    row('code-runtime', ' 0.8.2 '), row('code-runtime', '0.8.2', 'started', null, 'pool_slot'),
    row('seed', '0.8.3', 'deleted'),
  ];
  const server = spacedCountsFromServer(groups);
  const sample = spacedCounts(rows);
  assert.deepEqual([...server.byTemplate], [...sample.byTemplate]);
  for (const key of ['running', 'listed', 'nodeReported']) assert.equal(server[key], sample[key], key);
  assert.equal(server.source, 'server');
  assert.equal(server.truncated, false);
  const text = spacedCoverage(server, iso);
  assert.match(text, /^Running sandboxes among all 5 that are not deleted/);
  assert.match(text, /Also counted: 1 row nodes report without a database row/);
  assert.doesNotMatch(text, /newest|long-lived|created since/);
});

test('a server group with status deleted is not counted', () => {
  assert.equal(spacedCountsFromServer([group('seed', '0.8.3', 'deleted', 4)]).listed, 0);
});

test('the server count is used when the route answers, and the sample only when it is missing', async () => {
  const sampleRows = Array.from({ length: 3 }, () => row('seed', '0.8.3'));
  const routeError = (status, body = null) => async () => { throw Object.assign(new Error(`status ${status}`), { status, body }); };
  let sampled = 0;
  const readSample = async () => { sampled += 1; return sampleRows; };

  const server = await loadSpacedCounts(async () => ({ counts: [group('seed', '0.8.3', 'started', 9)] }), readSample);
  assert.equal(server.source, 'server');
  assert.equal(server.running, 9);
  assert.equal(sampled, 0);

  // A server without the route reads `spaced-versions` as a sandbox ID (400,
  // or the sandbox handler's 404), has no such path (the fallback's 404), or
  // knows the path without GET (405).
  const missing = [
    routeError(400), routeError(404, { error: 'Sandbox not found' }), routeError(404, { code: 404, message: 'Endpoint not found' }),
    routeError(404), routeError(405),
  ];
  for (const readServer of missing) {
    const counts = await loadSpacedCounts(readServer, readSample, 1000);
    assert.equal(counts.source, 'sample');
    assert.equal(counts.running, 3);
    assert.equal(counts.sampleLimit, 1000);
  }
  // A body of another shape is not a count either.
  assert.equal((await loadSpacedCounts(async () => [], readSample)).source, 'sample');
  assert.equal((await loadSpacedCounts(async () => ({ counts: [{ template: 'seed' }] }), readSample)).source, 'sample');
  assert.equal(sampled, 7);

  // Any other failure is the server's answer, not a reason to sample.
  for (const status of [403, 500, 503]) {
    await assert.rejects(loadSpacedCounts(routeError(status), readSample), new RegExp(`status ${status}`));
  }
  assert.equal(sampled, 7);
});
