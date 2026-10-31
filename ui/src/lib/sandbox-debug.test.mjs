import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  formatAge,
  formatBytes,
  sectionData,
  sectionError,
  sortedTimeline,
  summarizeTimelineDetail,
} from './sandbox-debug.ts';

test('a failed debug section yields its error and no data', () => {
  const failed = { error: 'statement timeout', data: null };
  assert.equal(sectionData(failed), null);
  assert.equal(sectionError(failed), 'statement timeout');
});

test('a loaded debug section yields its data, including arrays and objects with an error field', () => {
  assert.deepEqual(sectionData([{ id: 'a' }]), [{ id: 'a' }]);
  const pod = { error: 'exit 1', state: 'failed' };
  assert.equal(sectionData(pod), pod);
  assert.equal(sectionError(pod), null);
  assert.equal(sectionData(undefined), null);
});

test('timeline details summarize the informative fields', () => {
  assert.equal(
    summarizeTimelineDetail({
      at: '2026-01-01T00:00:00Z',
      kind: 'snapshot_ready',
      detail: { id: 'x', state: 'ready', size_bytes: 1076591599, snapshot_type: 'full', failure_reason: null },
    }),
    'state: ready · snapshot type: full · size bytes: 1.1 GB',
  );
  assert.equal(summarizeTimelineDetail({ at: '', kind: 'k', detail: {} }), '');
});

test('timeline sorts newest first', () => {
  const events = [
    { at: '2026-01-01T00:00:01Z', kind: 'a', detail: {} },
    { at: '2026-01-01T00:00:03Z', kind: 'b', detail: {} },
    { at: '2026-01-01T00:00:02Z', kind: 'c', detail: {} },
  ];
  assert.deepEqual(sortedTimeline(events).map((e) => e.kind), ['b', 'c', 'a']);
});

test('ages and sizes format compactly', () => {
  const now = Date.parse('2026-01-03T00:00:00Z');
  assert.equal(formatAge('2026-01-02T23:59:30Z', now), '30s');
  assert.equal(formatAge('2026-01-02T23:00:00Z', now), '1h');
  assert.equal(formatAge('2025-12-31T00:00:00Z', now), '3d');
  assert.equal(formatAge(null, now), '—');
  assert.equal(formatBytes(2813952), '2.8 MB');
  assert.equal(formatBytes(null), '—');
});
