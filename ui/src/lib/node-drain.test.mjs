import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  AUDIT_EVENT_MAX_LIMIT,
  NODE_ACTIONS,
  confirmationPhrase,
  drainElapsedMs,
  drainHistory,
  formatDuration,
  heartbeat,
  historyMayBeTruncated,
  kubeNodePresence,
  lifecycleCounts,
  terminateRefusal,
  typedConfirmationMatches,
} from './node-drain.ts';

const NODE = '0b4f5c1e-7d2a-4c9b-9f31-2a6e8d0c5b17';
const OTHER = '6c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f';

test('the phrase is the node name, or its ID without one', () => {
  assert.equal(confirmationPhrase({ id: NODE, name: 'worker-a' }), 'worker-a');
  assert.equal(confirmationPhrase({ id: NODE, name: null }), NODE);
  assert.equal(confirmationPhrase({ id: NODE, name: '  ' }), NODE);
});

test('only the exact phrase confirms', () => {
  assert.ok(typedConfirmationMatches('worker-a', 'worker-a'));
  assert.ok(typedConfirmationMatches('  worker-a ', 'worker-a'));
  for (const typed of ['', 'worker', 'Worker-a', 'worker-a2', 'worker-b']) {
    assert.ok(!typedConfirmationMatches(typed, 'worker-a'), typed);
  }
  assert.ok(!typedConfirmationMatches('', ''));
});

const refusal = (status, body) => Object.assign(new Error('refused'), { status, body });

test('an occupancy refusal lists each kind of sandbox it counted', () => {
  const both = terminateRefusal(refusal(409, { error: 'x', sandboxes_active: 2, sandboxes_drain_blockers: 1, sandboxes_local_snapshots: 1 }));
  assert.equal(both.title, 'Terminate refused: the node still holds sandboxes');
  assert.equal(both.reasons.length, 2);
  assert.match(both.reasons[0], /^2 sandboxes still occupy this node; terminating would destroy them\.$/);
  assert.match(both.reasons[1], /^1 sandbox is held for this node: /);

  const active = terminateRefusal(refusal(409, { sandboxes_active: 1, sandboxes_drain_blockers: 0 }));
  assert.deepEqual(active.reasons, ['1 sandbox still occupies this node; terminating would destroy it.']);

  const blockers = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_drain_blockers: 3 }));
  assert.equal(blockers.reasons.length, 1);
  assert.match(blockers.reasons[0], /^3 sandboxes are held for this node/);
});

test('an occupancy refusal from a release with only the alias still counts the blockers', () => {
  const alias = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_local_snapshots: 4 }));
  assert.match(alias.reasons[0], /^4 sandboxes are held/);
  // The real key wins over the alias when both are present.
  const both = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_drain_blockers: 2, sandboxes_local_snapshots: 9 }));
  assert.match(both.reasons[0], /^2 sandboxes are held/);
});

test('an occupancy refusal with zero counts still says something', () => {
  const zero = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_drain_blockers: 0 }));
  assert.equal(zero.reasons.length, 1);
});

test('a phase refusal names the phase and where terminate is accepted from', () => {
  assert.deepEqual(terminateRefusal(refusal(409, { error: 'drain-phase transition not allowed', current_drain_phase: 'draining', allowed_from: ['drained'] })), {
    title: 'Terminate refused: the node is not drained',
    reasons: ['The node is draining.', 'Terminate is accepted only from: drained.'],
  });
  assert.deepEqual(terminateRefusal(refusal(409, { current_drain_phase: null, allowed_from: ['drained'] })).reasons[0], 'The node is not draining.');
});

test('other answers are not read as a terminate refusal', () => {
  assert.equal(terminateRefusal(refusal(409, { error: 'something else' })), null);
  assert.equal(terminateRefusal(refusal(501, { sandboxes_active: 1 })), null);
  assert.equal(terminateRefusal(refusal(500, { current_drain_phase: 'draining' })), null);
  assert.equal(terminateRefusal(refusal(409, null)), null);
  assert.equal(terminateRefusal(null), null);
  assert.equal(terminateRefusal(new Error('network')), null);
});

const NOW = Date.parse('2026-01-01T12:00:00Z');
const status = (observed, staleAfter) => ({ observed_at: observed, stale_after: staleAfter, stale: false });

test('a heartbeat is fresh until stale_after and stale from it, whatever the snapshot flag said', () => {
  const window = status('2026-01-01T11:59:00Z', '2026-01-01T12:01:30Z');
  assert.deepEqual(heartbeat({ last_heartbeat: '2026-01-01T11:59:00Z', status: window }, NOW), { state: 'fresh', ageMs: 60_000, windowMs: 150_000 });
  const atEdge = status('2026-01-01T11:57:30Z', '2026-01-01T12:00:00Z');
  assert.equal(heartbeat({ last_heartbeat: '2026-01-01T11:57:30Z', status: atEdge }, NOW).state, 'stale');
  const flaggedStale = { ...status('2026-01-01T11:59:00Z', '2026-01-01T12:05:00Z'), stale: true };
  assert.equal(heartbeat({ last_heartbeat: '2026-01-01T11:59:00Z', status: flaggedStale }, NOW).state, 'fresh');
  const flaggedFresh = status('2026-01-01T11:50:00Z', '2026-01-01T11:55:00Z');
  assert.equal(heartbeat({ last_heartbeat: '2026-01-01T11:50:00Z', status: flaggedFresh }, NOW).state, 'stale');
});

test('a node without a status snapshot has no heartbeat evidence', () => {
  assert.deepEqual(heartbeat({ last_heartbeat: null, status: null }, NOW), { state: 'no-status', ageMs: null, windowMs: null });
  assert.deepEqual(heartbeat({ last_heartbeat: '2026-01-01T11:59:30Z', status: null }, NOW), { state: 'no-status', ageMs: 30_000, windowMs: null });
});

test('the heartbeat age runs from the newer of observed_at and last_heartbeat', () => {
  const recent = status('2026-01-01T11:59:40Z', '2026-01-01T12:02:10Z');
  const beat = heartbeat({ last_heartbeat: '2025-12-29T12:00:00Z', status: recent }, NOW);
  assert.equal(beat.ageMs, 20_000);
  assert.equal(beat.state, 'fresh');
});

test('durations read in the two largest units', () => {
  assert.equal(formatDuration(0), '0s');
  assert.equal(formatDuration(45_900), '45s');
  assert.equal(formatDuration(192_000), '3m 12s');
  assert.equal(formatDuration(7_500_000), '2h 5m');
  assert.equal(formatDuration(273_600_000), '3d 4h');
  assert.equal(formatDuration(-5), '0s');
});

test('drain elapsed time runs from drain_started_at', () => {
  assert.equal(drainElapsedMs({ drain_started_at: '2026-01-01T11:55:00Z' }, NOW), 300_000);
  assert.equal(drainElapsedMs({ drain_started_at: null }, NOW), null);
});

test('the Kubernetes Node is present only when the API reported its cordon state', () => {
  assert.equal(kubeNodePresence({ k8s_node_name: 'kind-worker', cordoned: false }), 'present');
  assert.equal(kubeNodePresence({ k8s_node_name: 'kind-worker', cordoned: true }), 'present');
  assert.equal(kubeNodePresence({ k8s_node_name: 'kind-worker' }), 'unknown');
  assert.equal(kubeNodePresence({ k8s_node_name: null, cordoned: false }), 'none');
});

const event = (path, at, extra = {}) => ({
  id: `${path}-${at}`, actor_email: 'operator@example.test', actor_user_id: 'u1', method: 'POST',
  path, status_code: 200, success: true, created_at: at, ...extra,
});

test("the history keeps this node's actions, newest first", () => {
  const events = [
    event(`/nodes/${NODE}:drain`, '2026-01-01T10:00:00Z'),
    event(`/admin/api/v1/nodes/${NODE}:undrain`, '2026-01-01T11:00:00Z', { actor_email: null }),
    event(`/nodes/${NODE}:terminate`, '2026-01-01T10:30:00Z', { status_code: 409, success: false }),
    event(`/nodes/${OTHER}:drain`, '2026-01-01T10:45:00Z'),
    event(`/nodes/${NODE}`, '2026-01-01T10:50:00Z', { method: 'PATCH' }),
    event(`/nodes/${NODE}/network-policy:reconcile`, '2026-01-01T10:55:00Z'),
    event(`/node-groups/${NODE}:drain`, '2026-01-01T10:56:00Z'),
    event(`/nodes/${NODE}:drain`, '2026-01-01T10:57:00Z', { method: 'GET' }),
    event(`/nodes/${NODE}:unknown`, '2026-01-01T10:58:00Z'),
  ];
  const history = drainHistory(events, NODE);
  assert.deepEqual(history.map((e) => [e.action, e.at, e.statusCode, e.success, e.actor]), [
    ['undrain', '2026-01-01T11:00:00Z', 200, true, 'u1'],
    ['terminate', '2026-01-01T10:30:00Z', 409, false, 'operator@example.test'],
    ['drain', '2026-01-01T10:00:00Z', 200, true, 'operator@example.test'],
  ]);
});

test('every node action is part of the history', () => {
  const actions = ['drain', 'undrain', 'terminate', 'cordon', 'uncordon', 'force-upload-local-snapshots'];
  assert.deepEqual([...NODE_ACTIONS].sort(), [...actions].sort());
  const events = actions.map((action, i) => event(`/nodes/${NODE}:${action}`, `2026-01-01T10:0${i}:00Z`));
  assert.deepEqual(drainHistory(events, NODE).map((e) => e.action), [...actions].reverse());
});

test('a full page of audit events may hide older history', () => {
  assert.ok(historyMayBeTruncated(AUDIT_EVENT_MAX_LIMIT, AUDIT_EVENT_MAX_LIMIT));
  assert.ok(!historyMayBeTruncated(AUDIT_EVENT_MAX_LIMIT - 1, AUDIT_EVENT_MAX_LIMIT));
  assert.ok(!historyMayBeTruncated(0, AUDIT_EVENT_MAX_LIMIT));
});

test('lifecycle counts put the most common first', () => {
  assert.deepEqual(lifecycleCounts([
    { lifecycle: 'suspended' }, { lifecycle: 'started' }, { lifecycle: 'started' }, { lifecycle: 'deleted' },
  ]), [['started', 2], ['deleted', 1], ['suspended', 1]]);
  assert.deepEqual(lifecycleCounts([]), []);
});
