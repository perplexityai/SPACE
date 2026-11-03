import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  envNames,
  formatAge,
  formatBytes,
  formatMetadataValue,
  mergeOperationPages,
  quarantineStatus,
  sectionData,
  sectionError,
  snapshotKey,
  snapshotStorageById,
  snapshotStorageLabel,
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
  assert.equal(formatBytes(Number('not a number')), '—');
});

test('metadata values that are not strings render as compact JSON', () => {
  assert.equal(formatMetadataValue('ci'), 'ci');
  assert.equal(formatMetadataValue({ team: 'build', tier: 2 }), '{"team":"build","tier":2}');
  assert.equal(formatMetadataValue(['a', 1]), '["a",1]');
  assert.equal(formatMetadataValue(3), '3');
  assert.equal(formatMetadataValue(null), 'null');
});

test('timeline details use the keys each event kind carries', () => {
  const summary = (kind, detail) => summarizeTimelineDetail({ at: '2026-01-01T00:00:00Z', kind, detail });
  assert.equal(
    summary('port_exposure_failed', {
      protocol: 'http', guest_port: 8080, visibility: 'public', desired_state: 'exposed',
      apply_state: 'failed', last_error: 'bridge refused', last_bridge_status: 'error',
    }),
    'protocol: http · guest port: 8080 · visibility: public · desired state: exposed · apply state: failed · last error: bridge refused · last bridge status: error',
  );
  assert.equal(
    summary('observed_state', { node_id: 'n', status: 'running', disk_usage_bytes: 2_000_000, failure_reason: null, missing_publish_count: 0 }),
    'status: running · disk usage bytes: 2.0 MB',
  );
  assert.equal(
    summary('observed_state', { status: 'running', missing_publish_count: 3 }),
    'status: running · missing publish count: 3',
  );
  assert.equal(
    summary('pod_created', { generation: 2, pod_name: 'space-sbx-x-g2', k8s_node_name: 'worker', state: 'running', active: true }),
    'state: running · pod name: space-sbx-x-g2 · generation: 2 · k8s node name: worker',
  );
});

test('operation pages read under both IDs merge into one newest-first list', () => {
  const op = (id, at) => ({ id, created_at: at });
  const byPublicId = [op('b', '2026-01-02T00:00:00Z'), op('a', '2026-01-01T00:00:00Z')];
  const byUuid = [op('c', '2026-01-03T00:00:00Z'), op('b', '2026-01-02T00:00:00Z')];
  const merged = mergeOperationPages([byPublicId, byUuid], 50);
  assert.deepEqual(merged.operations.map((o) => o.id), ['c', 'b', 'a']);
  assert.equal(merged.more, false);
  const capped = mergeOperationPages([[op('x', '2026-01-02T00:00:00Z'), op('y', '2026-01-01T00:00:00Z')], []], 1);
  assert.deepEqual(capped.operations.map((o) => o.id), ['x']);
  assert.equal(capped.more, true);
});

test("a snapshot's storage reads as where its data is, with the node only for a local copy", () => {
  assert.equal(snapshotStorageLabel({ location: 'node_local', node_id: 'node-1' }), 'node-local (node node-1)');
  assert.equal(snapshotStorageLabel({ location: 'both', node_id: 'node-1' }), 'node-local and object store (node node-1)');
  assert.equal(snapshotStorageLabel({ location: 'object_store', node_id: null }), 'object store');
  assert.equal(snapshotStorageLabel({ location: 'none', node_id: null }), 'none');
  // Not reported (an older release, or a failed placement read): nothing.
  assert.equal(snapshotStorageLabel(undefined), null);
});

test("the debug route's bare snapshot UUID matches the snapshot API's snap_ ID", () => {
  const uuid = '0190aa3c-5f2e-7c4b-9d1e-0123456789ab';
  assert.equal(snapshotKey(uuid), snapshotKey(`snap_${uuid.replace(/-/g, '')}`));
  assert.equal(snapshotKey(uuid), snapshotKey(`snap_${uuid.toUpperCase()}`));
  const byId = snapshotStorageById([
    { id: uuid, storage: { location: 'object_store', node_id: null } },
    { id: '0190aa3c-5f2e-7c4b-9d1e-0123456789ac' },
  ]);
  assert.deepEqual(byId.get(snapshotKey(`snap_${uuid}`)), { location: 'object_store', node_id: null });
  assert.equal(byId.size, 1);
});

test('the environment shows names whether the release sends values or only names', () => {
  assert.deepEqual(envNames({ SYNTHETIC_TOKEN: null, EMPTY: null }), ['EMPTY', 'SYNTHETIC_TOKEN']);
  assert.deepEqual(envNames({ SYNTHETIC_TOKEN: 'synthetic-value' }), ['SYNTHETIC_TOKEN']);
  for (const env of [null, undefined, [], 'text', 3]) assert.deepEqual(envNames(env), []);
});

test('a quarantine read that failed is unknown, never "not quarantined"', () => {
  const clear = { quarantined: false, active: null, history: [] };
  assert.equal(quarantineStatus(clear), 'not-quarantined');
  assert.equal(quarantineStatus({ ...clear, quarantined: true }), 'quarantined');
  assert.equal(quarantineStatus({ quarantined: false, active: { quarantine_id: 'q-1' }, history: [] }), 'quarantined');
  assert.equal(quarantineStatus({ error: 'synthetic section failure', data: null }), 'unknown');
  // The whole debug read failed after an earlier one: what it showed is not current.
  assert.equal(quarantineStatus(clear, true), 'unknown');
  assert.equal(quarantineStatus(undefined), 'absent');
});
