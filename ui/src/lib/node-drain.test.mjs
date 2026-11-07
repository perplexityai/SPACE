import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  AUDIT_EVENT_MAX_LIMIT,
  NODE_ACTIONS,
  confirmationPhrase,
  HOLD_GROUPS,
  drainElapsedMs,
  drainHistory,
  drainStage,
  formatDuration,
  groupHolds,
  holdGatesLabel,
  holdGroup,
  holdSentence,
  heartbeat,
  historyMayBeTruncated,
  kubeNodePresence,
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
  assert.deepEqual(both.counts, { active: 2, blockers: 1 });

  const active = terminateRefusal(refusal(409, { sandboxes_active: 1, sandboxes_drain_blockers: 0 }));
  assert.deepEqual(active.reasons, ['1 sandbox still occupies this node; terminating would destroy it.']);
  assert.deepEqual(active.counts, { active: 1, blockers: 0 });

  const blockers = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_drain_blockers: 3 }));
  assert.equal(blockers.reasons.length, 1);
  assert.match(blockers.reasons[0], /^3 sandboxes are held for this node/);
  // A count the body leaves out is zero.
  assert.deepEqual(terminateRefusal(refusal(409, { sandboxes_drain_blockers: 3 })).counts, { active: 0, blockers: 3 });
});

test('an occupancy refusal from a release with only the alias still counts the blockers', () => {
  const alias = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_local_snapshots: 4 }));
  assert.match(alias.reasons[0], /^4 sandboxes are held/);
  assert.deepEqual(alias.counts, { active: 0, blockers: 4 });
  // The real key wins over the alias when both are present.
  const both = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_drain_blockers: 2, sandboxes_local_snapshots: 9 }));
  assert.match(both.reasons[0], /^2 sandboxes are held/);
  assert.deepEqual(both.counts, { active: 0, blockers: 2 });
});

test('an occupancy refusal with zero counts still says something', () => {
  const zero = terminateRefusal(refusal(409, { sandboxes_active: 0, sandboxes_drain_blockers: 0 }));
  assert.equal(zero.reasons.length, 1);
  assert.deepEqual(zero.counts, { active: 0, blockers: 0 });
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

test('an unreachable status does not count as a heartbeat', () => {
  const failed = { ...status('2026-01-01T11:59:55Z', '2026-01-01T12:02:10Z'), reachable: false };
  const beat = heartbeat({ last_heartbeat: '2026-01-01T11:50:00Z', status: failed }, NOW);
  assert.equal(beat.ageMs, 600_000);
  assert.equal(heartbeat({ last_heartbeat: null, status: failed }, NOW).ageMs, null);
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

// ------------------------------------------------------------- holds

const REASONS = ['census_absent', 'evacuating', 'upload_in_flight', 'stuck_transient', 'transient', 'local_only_snapshot', 'resident', 'termination_blocking'];
const hold = (reason, overrides = {}) => ({
  sandbox_id: `${reason}-id`,
  lifecycle: 'started',
  lifecycle_changed_at: '2026-01-01T11:45:00Z',
  placement: 'active',
  reason,
  counted_by: { terminate: true, drain: true, drain_with_termination_blocking_gate: true },
  terms: { resident: true, drain_blocker: false, active_resident: true },
  facts: {
    census_absent: false, durable_snapshot: false, upload_in_flight: false, upload_started_at: null,
    evacuation_restore_target: null, restore_attempt_count: null, next_restore_attempt_at: null, past_stuck_threshold: true,
  },
  ...overrides,
});

const STAGES = ['none', 'gentle', 'forced', 'parked'];
const NOT_COUNTED = { terminate: false, drain: false, drain_with_termination_blocking_gate: false };

// The drain acts on the node's sandboxes, as its mode says, only while the
// node is draining; a drained or terminating node is parked.
test('every reason has a group under every drain stage', () => {
  const N = 'needs_you', H = 'handled_automatically', W = 'waiting_on_users', B = 'blocks_terminate';
  const expected = {
    census_absent: [N, N, N, N],
    evacuating: [H, H, H, H],
    upload_in_flight: [H, H, H, H],
    stuck_transient: [N, N, H, N],
    transient: [H, H, H, H],
    local_only_snapshot: [H, H, H, N],
    resident: [W, W, H, N],
    termination_blocking: [B, B, B, B],
  };
  assert.deepEqual(Object.keys(expected).sort(), [...REASONS].sort());
  for (const reason of REASONS) {
    assert.deepEqual(STAGES.map((stage) => holdGroup(hold(reason), stage)), expected[reason], reason);
  }
  // An evacuation stopped part-way waits for a forced drain; one restoring
  // onto this node is the evacuation's own.
  assert.deepEqual(STAGES.map((stage) => holdGroup(hold('evacuating', { lifecycle: 'stopped' }), stage)), [B, B, H, B]);
  assert.deepEqual(STAGES.map((stage) => holdGroup(hold('evacuating', { lifecycle: 'restoring' }), stage)), [H, H, H, H]);
  // A row no gate counts is only listed, whatever its reason and stage.
  for (const reason of REASONS) {
    assert.deepEqual(STAGES.map((stage) => holdGroup(hold(reason, { counted_by: NOT_COUNTED }), stage)), Array(4).fill('not_counted'), reason);
  }
  // One flag is enough to count it: a census-absent row the drain counts
  // still needs someone.
  const drainOnly = { terminate: false, drain: true, drain_with_termination_blocking_gate: false };
  assert.deepEqual(STAGES.map((stage) => holdGroup(hold('census_absent', { counted_by: drainOnly }), stage)), [N, N, N, N]);
});

test('the drain stage reads a missing mode as gentle and a finished drain as parked', () => {
  assert.equal(drainStage({ drain_phase: null, drain_mode: 'forced' }), 'none');
  assert.equal(drainStage({ drain_phase: 'draining', drain_mode: null }), 'gentle');
  assert.equal(drainStage({ drain_phase: 'draining', drain_mode: 'forced' }), 'forced');
  assert.equal(drainStage({ drain_phase: 'drained', drain_mode: 'forced' }), 'parked');
  assert.equal(drainStage({ drain_phase: 'terminating', drain_mode: 'gentle' }), 'parked');
});

test('groups come in panel order, skip empty ones and keep the API row order', () => {
  const rows = [
    hold('resident', { sandbox_id: 'r1' }), hold('census_absent'), hold('termination_blocking'), hold('resident', { sandbox_id: 'r2' }),
    hold('census_absent', { sandbox_id: 'listed', counted_by: NOT_COUNTED }),
  ];
  const gentle = groupHolds(rows, 'gentle');
  assert.deepEqual(gentle.map((g) => g.key), ['needs_you', 'waiting_on_users', 'blocks_terminate', 'not_counted']);
  assert.deepEqual(gentle[1].rows.map((r) => r.sandbox_id), ['r1', 'r2']);
  assert.deepEqual(gentle[3].rows.map((r) => r.sandbox_id), ['listed']);
  assert.deepEqual(gentle.map((g) => g.title), ['Needs you', 'Waiting on users', 'Blocks terminate', 'Listed, not counted']);
  assert.deepEqual(groupHolds(rows, 'forced').map((g) => g.key), ['needs_you', 'handled_automatically', 'blocks_terminate', 'not_counted']);
  assert.deepEqual(HOLD_GROUPS.map((g) => g.key), ['needs_you', 'handled_automatically', 'waiting_on_users', 'blocks_terminate', 'not_counted']);
  assert.deepEqual(HOLD_GROUPS.map((g) => g.title), ['Needs you', 'Handled automatically', 'Waiting on users', 'Blocks terminate', 'Listed, not counted']);
  assert.deepEqual(groupHolds([], 'none'), []);
});

test('each reason reads as one sentence built from its facts', () => {
  const sentences = {
    census_absent: [hold('census_absent'), /^The database says this sandbox is started here, but the node's latest sandbox report does not list it/],
    evacuating: [
      hold('evacuating', { lifecycle: 'suspending', facts: { ...hold('x').facts, evacuation_restore_target: 'paused', restore_attempt_count: 2, next_restore_attempt_at: '2026-01-01T12:05:00Z' } }),
      /^An evacuation is moving this sandbox off this node, to come back paused elsewhere; it has been suspending for 15m 0s\. Its restore has failed 2 times; the next attempt is due /,
    ],
    upload_in_flight: [
      hold('upload_in_flight', { lifecycle: 'suspended', facts: { ...hold('x').facts, upload_in_flight: true, upload_started_at: '2026-01-01T11:58:30Z' } }),
      /^Its snapshot is uploading from this node, started 1m 30s ago; the node is held until the upload finishes\.$/,
    ],
    stuck_transient: [hold('stuck_transient', { lifecycle: 'starting' }), /^It has been starting for 15m 0s, longer than the drain's 10m 0s limit\. A gentle drain keeps waiting; Forced drain marks it failed\.$/],
    transient: [hold('transient', { lifecycle: 'restoring' }), /^It has been restoring for 15m 0s; a transition in progress that usually settles on its own\.$/],
    local_only_snapshot: [hold('local_only_snapshot', { lifecycle: 'suspended' }), /only snapshot is on this node's disk; the drain uploads it, or marks it failed when the snapshot is missing, before the node can go\. A snapshot a gentle drain cannot upload stays listed until Forced drain\.$/],
    resident: [hold('resident', { lifecycle: 'paused' }), /^It has been paused here for 15m 0s\. A gentle drain waits for its owner to suspend or delete it; Forced drain moves it to another node now\.$/],
    termination_blocking: [hold('termination_blocking', { lifecycle: 'stopped' }), /^It has been stopped here for 15m 0s\. The node cannot be terminated while it is stopped here; its owner can start it, which lets a forced drain move it, or delete it\.$/],
  };
  assert.deepEqual(Object.keys(sentences).sort(), [...REASONS].sort());
  for (const reason of REASONS) {
    const [row, pattern] = sentences[reason];
    for (const [stage, phase] of [['none', null], ['gentle', 'draining'], ['forced', 'draining'], ['parked', 'drained'], ['parked', 'terminating']]) {
      const sentence = holdSentence(row, NOW, 600, stage, phase);
      assert.ok(!/undefined|null|NaN/.test(sentence), `${reason} ${stage}: ${sentence}`);
      assert.match(sentence, /\.$/, `${reason} ${stage}`);
    }
    assert.match(holdSentence(row, NOW, 600, 'gentle', 'draining'), pattern, reason);
  }
  // Facts that are not set are left out rather than shown empty.
  assert.equal(
    holdSentence(hold('upload_in_flight'), NOW, 600, 'gentle', 'draining'),
    'Its snapshot is uploading from this node; the node is held until the upload finishes.',
  );
  assert.equal(
    holdSentence(hold('evacuating', { lifecycle: 'suspending', placement: 'active' }), NOW, 600, 'forced', 'draining'),
    'An evacuation is moving this sandbox off this node, to come back as it was elsewhere; it has been suspending for 15m 0s.',
  );
  assert.match(holdSentence(hold('termination_blocking', { lifecycle: 'quarantined' }), NOW, 600, 'gentle', 'draining'), /until the quarantine is released or the sandbox is deleted\.$/);
});

test('the sentences say what the drain does in each stage', () => {
  const at = (row, stage, phase = stage === 'parked' ? 'drained' : stage === 'none' ? null : 'draining') => holdSentence(row, NOW, 600, stage, phase);
  const resident = hold('resident', { lifecycle: 'started' });
  assert.equal(at(resident, 'none'), 'It has been started here for 15m 0s. A forced drain would move it to another node; a gentle drain would wait for its owner to suspend or delete it.');
  assert.equal(at(resident, 'gentle'), 'It has been started here for 15m 0s. A gentle drain waits for its owner to suspend or delete it; Forced drain moves it to another node now.');
  assert.equal(at(resident, 'forced'), 'It has been started here for 15m 0s. The forced drain moves it to another node.');
  assert.equal(at(resident, 'parked'), 'It has been started here for 15m 0s. The drain stopped moving sandboxes when the node became drained; its owner can still suspend or delete it, or Undrain and start a Forced drain to move it.');
  assert.equal(at(resident, 'parked', 'terminating'), 'It has been started here for 15m 0s. The drain stopped moving sandboxes when the node became terminating; its owner can still suspend or delete it.');

  const stuck = hold('stuck_transient', { lifecycle: 'stopping' });
  const limit = "It has been stopping for 15m 0s, longer than the drain's 10m 0s limit.";
  assert.equal(at(stuck, 'forced'), `${limit} The forced drain marks it failed.`);
  assert.equal(at(stuck, 'gentle'), `${limit} A gentle drain keeps waiting; Forced drain marks it failed.`);
  assert.equal(at(stuck, 'none'), `${limit} A forced drain would mark it failed.`);
  assert.equal(at(stuck, 'parked'), `${limit} The drain no longer acts on it now that the node is drained.`);

  const local = hold('local_only_snapshot', { lifecycle: 'suspended' });
  const uploads = "It is suspended and its only snapshot is on this node's disk; the drain uploads it, or marks it failed when the snapshot is missing, before the node can go.";
  assert.equal(at(local, 'forced'), uploads);
  assert.equal(at(local, 'gentle'), `${uploads} A snapshot a gentle drain cannot upload stays listed until Forced drain.`);
  assert.equal(at(local, 'none'), "It is suspended and its only snapshot is on this node's disk; a drain must upload it before the node can go.");
  assert.equal(at(local, 'parked'), "It is suspended and its only snapshot is on this node's disk: use Upload local snapshots before the node can go.");

  // Terminate-blocking rows: who can clear them depends on the stage.
  const blocking = (lifecycle) => hold('termination_blocking', { lifecycle });
  assert.equal(at(blocking('deleting'), 'forced'), 'It has been deleting here for 15m 0s; it releases the node when its delete completes.');
  assert.equal(at(blocking('created'), 'forced'), 'It has been created here for 15m 0s. Its owner cannot start it during a forced drain; Undrain lets its owner start it, or deleting it lets the node terminate.');
  const stoppedForced = 'It has been stopped here for 15m 0s. Its owner cannot start it during a forced drain. The drain moves it only if the cluster evacuates stopped sandboxes; otherwise Undrain lets its owner start it, or deleting it lets the node terminate.';
  assert.equal(at(blocking('stopped'), 'forced'), stoppedForced);
  const stoppedParked = 'It has been stopped here for 15m 0s. While the node is drained its owner cannot start it; Undrain lets its owner start it, or deleting it lets the node terminate.';
  assert.equal(at(blocking('stopped'), 'parked'), stoppedParked);
  // The API refuses Undrain on a terminating node, so only a delete clears it.
  assert.equal(
    at(blocking('stopped'), 'parked', 'terminating'),
    'It has been stopped here for 15m 0s. While the node is terminating, its owner cannot start it, so only deleting it lets the node terminate.',
  );
  const stoppedGentle = 'It has been stopped here for 15m 0s. The node cannot be terminated while it is stopped here; its owner can start it, which lets a forced drain move it, or delete it.';
  assert.equal(at(blocking('stopped'), 'gentle'), stoppedGentle);
  assert.equal(at(blocking('stopped'), 'none'), stoppedGentle);

  // Evacuations: onto this node, stopped part-way, or moving off.
  const restoring = hold('evacuating', { lifecycle: 'restoring', placement: 'both', facts: { ...hold('x').facts, evacuation_restore_target: 'started', restore_attempt_count: 1 } });
  assert.equal(at(restoring, 'forced'), 'An evacuation is restoring this sandbox onto this node, to come back started; it has been restoring for 15m 0s. Its restore has failed 1 time.');
  assert.match(at(hold('evacuating', { lifecycle: 'restoring' }), 'forced'), /^An evacuation is restoring this sandbox onto this node, to come back as it was; it has been restoring for 15m 0s\.$/);
  assert.match(at(hold('evacuating', { lifecycle: 'restoring', placement: 'target' }), 'forced'), /^An evacuation is moving this sandbox off this node/);
  const stoppedEvacuation = hold('evacuating', { lifecycle: 'stopped', facts: { ...hold('x').facts, evacuation_restore_target: 'stopped' } });
  const partWay = 'A forced drain began moving this stopped sandbox and did not finish; only a forced drain resumes it. ';
  assert.equal(at(stoppedEvacuation, 'none'), `${partWay}${stoppedGentle}`);
  assert.equal(at(stoppedEvacuation, 'parked'), `${partWay}${stoppedParked}`);
  assert.equal(at(stoppedEvacuation, 'forced'), 'An evacuation is moving this sandbox off this node, to come back stopped elsewhere; it has been stopped for 15m 0s.');

  // A row no gate counts says so after its reason.
  assert.equal(
    at(hold('census_absent', { counted_by: NOT_COUNTED }), 'forced'),
    "The database says this sandbox is started here, but the node's latest sandbox report does not list it: a row with nothing behind it on the node. Under node-census residency neither gate counts it.",
  );
  assert.ok(!/neither gate/.test(at(hold('census_absent'), 'forced')));
});

test('the gates column says which gate counts the row', () => {
  const counted = (terminate, drain, gated) => ({ counted_by: { terminate, drain, drain_with_termination_blocking_gate: gated } });
  assert.equal(holdGatesLabel(counted(true, true, true)), 'drain and terminate');
  assert.equal(holdGatesLabel(counted(false, true, false)), 'drain');
  assert.equal(holdGatesLabel(counted(true, false, true)), 'terminate; drain only with the termination-blocking gate on');
  assert.equal(holdGatesLabel(counted(true, false, false)), 'terminate');
  assert.equal(holdGatesLabel(counted(false, false, false)), 'neither');
});

