import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  FULL_SNAPSHOT_RESTORE_NOTE,
  ROLLBACK_AWAITS_ADMIN_LIST,
  ROLLBACK_OTHER_ORG_UNSUPPORTED,
  ROLLBACK_SUSPENDED,
  isFullSnapshotName,
  isInternalSnapshotName,
  rollbackBody,
  rollbackRoute,
  rollbackTargets,
  snapshotTimeMs,
} from './rollback-targets.ts';

// A row of the debug view's `snapshots.inventory`, as the node publishes it.
const row = (snapshotId, overrides = {}) => ({
  snapshot_id: snapshotId,
  type: snapshotId.startsWith('f-') ? 'full' : 'disk',
  state: 'ready',
  timestamp: null,
  source_node_id: 'node-1',
  size_bytes: 4096,
  locations: [],
  observed_at: '2026-11-03T08:00:00Z',
  ...overrides,
});

test("the names sbx-mgr keeps for itself are never offered", () => {
  for (const name of [
    'd-stop-0190f1a2-0000-7000-8000-000000000001',
    'd-operation-req-1',
    '.gc-pending-0190f1a2-0000-7000-8000-000000000001',
    'd-1m-20261103T080000Z.gc-pending-0190f1a2000070008000000000000001',
    'f-10m-20261103T080000Z.gc-pending-{0190f1a2-0000-7000-8000-000000000001}',
  ]) {
    assert.equal(isInternalSnapshotName(name), true, name);
  }
  // Only a UUID after the marker is staging, as the node reads it.
  for (const name of ['run.gc-pending-1', 'd-1m-20261103T080000Z', 'f-10m-20261103T080000Z', 'checkpoint', 'd-stopped']) {
    assert.equal(isInternalSnapshotName(name), false, name);
  }
});

test("rollback is offered on the node's ready snapshots, newest first, and on no catalog ID", () => {
  const targets = rollbackTargets([
    row('d-1m-20261103T080000Z', { timestamp: '20261103T080000Z' }),
    row('d-stop-0190f1a2-0000-7000-8000-000000000001', { timestamp: '20261103T090000Z' }),
    row('f-10m-20261103T081000Z', { timestamp: '20261103T081000Z', state: 'unknown' }),
    row('d-5m-20261103T075500Z', { timestamp: '20261103T075500Z', state: 'incomplete' }),
    row('checkpoint', { timestamp: '2026-11-03T07:00:00Z' }),
  ], 'running');
  assert.deepEqual(targets.map((target) => target.snapshotName), [
    'f-10m-20261103T081000Z',
    'd-1m-20261103T080000Z',
    'd-5m-20261103T075500Z',
    'checkpoint',
  ]);
  const offered = Object.fromEntries(targets.map((target) => [target.snapshotName, target.unavailable]));
  assert.equal(offered['d-1m-20261103T080000Z'], null);
  // A full snapshot without a catalog publication is still rolled back to.
  assert.equal(offered['f-10m-20261103T081000Z'], null);
  assert.equal(offered.checkpoint, null);
  assert.match(offered['d-5m-20261103T075500Z'], /incomplete/);
});

test('every row is disabled while the sandbox is suspended, with the reason', () => {
  const targets = rollbackTargets([row('d-1m-20261103T080000Z'), row('f-10m-20261103T081000Z')], 'suspended');
  assert.deepEqual(targets.map((target) => target.unavailable), [ROLLBACK_SUSPENDED, ROLLBACK_SUSPENDED]);
  assert.match(ROLLBACK_SUSPENDED, /Restore it first/);
  for (const status of ['running', 'stopped', 'paused', null]) {
    assert.equal(rollbackTargets([row('d-1m-20261103T080000Z')], status)[0].unavailable, null, String(status));
  }
});

test("the rollback sends the node's directory name, never the catalog's snap_ ID", () => {
  const [target] = rollbackTargets([row('d-1m-20261103T080000Z')], 'running');
  assert.deepEqual(rollbackBody(target.snapshotName), { snapshot_name: 'd-1m-20261103T080000Z', auto_restart: true });
  assert.doesNotMatch(target.snapshotName, /^snap_/);
});

test('a full snapshot asks for restore support; a disk one does not', () => {
  assert.equal(isFullSnapshotName('f-1h-20261103T080000Z'), true);
  assert.equal(isFullSnapshotName('d-1m-20261103T080000Z'), false);
  assert.match(FULL_SNAPSHOT_RESTORE_NOTE, /snapshot restore/);
});

test("the own organization rolls back through :rollback; another only once the admin list answered", () => {
  for (const adminList of ['listed', 'none', 'unknown']) {
    assert.deepEqual(rollbackRoute(true, adminList), { via: 'public', reason: null });
  }
  assert.deepEqual(rollbackRoute(false, 'listed'), { via: 'admin', reason: null });
  assert.deepEqual(rollbackRoute(false, 'none'), { via: null, reason: ROLLBACK_OTHER_ORG_UNSUPPORTED });
  assert.deepEqual(rollbackRoute(false, 'unknown'), { via: null, reason: ROLLBACK_AWAITS_ADMIN_LIST });
});

test('node timestamps read compact or RFC 3339', () => {
  assert.equal(snapshotTimeMs('20261103T080000Z'), Date.parse('2026-11-03T08:00:00Z'));
  assert.equal(snapshotTimeMs('2026-11-03T08:00:00Z'), Date.parse('2026-11-03T08:00:00Z'));
  assert.equal(snapshotTimeMs('not a time'), null);
  assert.equal(snapshotTimeMs(null), null);
});
