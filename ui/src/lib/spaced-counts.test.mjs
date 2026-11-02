import assert from 'node:assert/strict';
import { test } from 'node:test';
import { SPACED_SAMPLE_LIMIT, UNKNOWN_SPACED, spacedCounts, spacedCoverage } from './spaced-counts.ts';

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
