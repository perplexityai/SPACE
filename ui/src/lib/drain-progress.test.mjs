import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  HOLD_PRESETS,
  MAX_HOLD_SECONDS,
  STALL_BOUND_MS,
  compactDrainCell,
  controllerTerminateRefusal,
  compactDuration,
  drainCounts,
  drainProgress,
  evacuatedRows,
  holdDeadlineMs,
  isV2Drain,
  parseHoldSeconds,
  pendingKind,
  pendingRows,
  readDrain,
  shortenLengthens,
  unknownEngine,
  DRAIN_CHANGED,
  V2_HOLD_SENTENCE,
  closesOnDrainChange,
  drainChanged,
  drainSeen,
  holdWhy,
  isV2DrainBody,
  v2DrainBodyFor,
  v2DrainRefusal,
  v2HoldBody,
  v2HoldRefusal,
  v2StartBody,
} from './drain-progress.ts';
import { nodeActions } from './node-actions.ts';

const NOW = Date.parse('2026-11-19T12:00:00Z');
const iso = (offsetMs) => new Date(NOW + offsetMs).toISOString();
const MINUTE = 60_000;

const drain = (over = {}) => ({
  generation: 1_800_000_000_000_000,
  deadline: iso(8 * MINUTE + 3_000),
  cancelling: false,
  fenced: false,
  reported_at: iso(-10_000),
  evacuated_grew_at: iso(-MINUTE),
  evacuated_total: [],
  evacuated: [],
  pending: [],
  blocked: [],
  ...over,
});

const evacuated = (sandbox_id, over = {}) => ({
  sandbox_id, artifact_kind: 'full', was_running: false, recorded: true, ...over,
});
const pending = (sandbox_id, wait, over = {}) => ({
  sandbox_id, wait, running: wait === 'hold', pool_member: false, reason: null, since: null, ...over,
});

test('only a node whose drain_engine is v2 has a v2 drain', () => {
  assert.equal(isV2Drain({ drain_engine: 'v2' }), true);
  assert.equal(isV2Drain({ drain_engine: undefined }), false);
  assert.equal(isV2Drain({}), false);
});

// The evacuated count comes from `evacuated_total`, which a released sandbox
// stays in after it drops out of the node's report, plus the current
// report.
test('counts take each sandbox once, evacuated from evacuated_total and the report', () => {
  const counts = drainCounts(drain({
    evacuated_total: ['released', 'a'],
    evacuated: [evacuated('a'), evacuated('b', { recorded: false })],
    pending: [
      pending('held', 'hold'),
      pending('busy', 'hold'),
      pending('busy', 'moving'),
      pending('c', 'external'),
      pending('pool', 'hold', { pool_member: true }),
      // An entry for a sandbox already evacuated is not pending too.
      pending('released', 'moving'),
    ],
    blocked: [{ reason: 'a node-wide problem' }],
  }));
  assert.deepEqual(counts, {
    evacuated: 3, recording: 1, pending: 4, held: 2, poolMembers: 1, total: 7, blocked: 1,
  });
});

test('before the deadline the headline counts down; from it the node evacuates', () => {
  const holding = drainProgress(drain({ pending: [pending('a', 'hold')] }), NOW);
  assert.equal(holding.state, 'holding');
  assert.equal(holding.headline, 'deadline in 8m 3s');
  assert.equal(holding.deadlineInMs, 8 * MINUTE + 3_000);
  const past = drainProgress(drain({ deadline: iso(-MINUTE), pending: [pending('a', 'moving')] }), NOW);
  assert.equal(past.state, 'evacuating');
  assert.equal(past.deadlineInMs, 0);
  assert.equal(drainProgress(drain({ deadline: null, pending: [pending('a', 'moving')] }), NOW).state, 'evacuating', 'no deadline holds nothing');
});

test('cancelling outranks the rest, and a finished report reads finished', () => {
  assert.equal(drainProgress(drain({ cancelling: true }), NOW).state, 'cancelling');
  assert.equal(drainProgress(drain(), NOW).state, 'finished');
  assert.equal(drainProgress(drain({ reported_at: null }), NOW).state, 'holding', 'no report yet is not finished');
  assert.equal(drainProgress(drain({ evacuated: [evacuated('a', { recorded: false })] }), NOW).state, 'holding', 'an evacuation still recording');
  assert.equal(drainProgress(drain({ blocked: [{ reason: 'x' }] }), NOW).state, 'holding');
});

// The console derives the stall from the later of `evacuated_grew_at` and
// the deadline, plus the stall bound, past the deadline, while the drain
// hasn't finished, as the cluster controller does.
test('the stall comes from evacuated_grew_at or the deadline, plus the bound', () => {
  const grew = -20 * MINUTE;
  const base = { deadline: iso(-30 * MINUTE), evacuated_grew_at: iso(grew), pending: [pending('a', 'external')] };
  assert.equal(drainProgress(drain(base), NOW).stalledSinceMs, NOW + grew + STALL_BOUND_MS);
  assert.equal(drainProgress(drain({ ...base, evacuated_grew_at: iso(-10 * MINUTE) }), NOW).stalledSinceMs, null, 'under the bound');
  assert.equal(drainProgress(drain({ ...base, deadline: iso(MINUTE) }), NOW).stalledSinceMs, null, 'before the deadline');
  assert.equal(drainProgress(drain({ ...base, pending: [] }), NOW).stalledSinceMs, null, 'finished');
  assert.equal(drainProgress(drain({ ...base, cancelling: true }), NOW).stalledSinceMs, null, 'cancelling');
  assert.equal(drainProgress(drain({ ...base, evacuated_grew_at: null }), NOW).stalledSinceMs, null, 'no report yet');
  // Grown 30 minutes before the deadline: the hold isn't a stall, so the
  // clock starts at the deadline.
  const early = (deadline) => drain({ ...base, deadline: iso(deadline), evacuated_grew_at: iso(deadline - 30 * MINUTE) });
  assert.equal(drainProgress(early(-MINUTE), NOW).stalledSinceMs, null, 'a minute past the deadline');
  assert.equal(drainProgress(early(-15 * MINUTE), NOW).stalledSinceMs, NOW, 'the bound past the deadline');
  assert.equal(
    drainProgress(drain({ ...base, deadline: null }), NOW).stalledSinceMs,
    NOW + grew + STALL_BOUND_MS,
    'with no deadline, from the last growth',
  );
});

test('the nodes list cell reads evacuated of total, then the state', () => {
  assert.equal(compactDrainCell(drain({ evacuated_total: ['a'], pending: [pending('b', 'hold')] }), NOW), '1/2 evacuated · deadline 8m');
  assert.equal(compactDrainCell(drain({ deadline: iso(-MINUTE), pending: [pending('b', 'moving')] }), NOW), '0/1 evacuated · past the deadline');
  assert.equal(compactDrainCell(drain({ evacuated_total: ['a'] }), NOW), '1/1 evacuated · finished');
  assert.equal(compactDrainCell(drain({ cancelling: true }), NOW), 'cancelling on the node');
  assert.equal(
    compactDrainCell(drain({ deadline: iso(-40 * MINUTE), evacuated_grew_at: iso(-20 * MINUTE), pending: [pending('b', 'external')] }), NOW),
    '0/1 evacuated · past the deadline · stalled',
  );
});

test('compact durations keep the largest units', () => {
  assert.equal(compactDuration(45_000), '45s');
  assert.equal(compactDuration(8 * MINUTE + 59_000), '8m');
  assert.equal(compactDuration(3 * 60 * MINUTE), '3h');
  assert.equal(compactDuration(3 * 60 * MINUTE + 5 * MINUTE), '3h 5m');
  assert.equal(compactDuration(50 * 60 * MINUTE), '2d 2h');
  assert.equal(compactDuration(-1), '0s');
});

// A pending row carries its wait, whether it runs, and whether it is a
// warm-pool member, which the panel labels apart from a user's sandbox. The
// node's reason is displayed as given.
test('a pending row says its wait, how long it has been in its state, and a hold before the deadline that it runs until then', () => {
  const rows = pendingRows(drain({
    pending: [
      pending('held', 'hold'),
      pending('moving', 'moving', { since: iso(-(5 * MINUTE + 2_000)), reason: 'the node said so' }),
      pending('pool', 'hold', { pool_member: true }),
    ],
  }), NOW);
  assert.deepEqual(rows.map((row) => [row.sandbox_id, row.wait, row.running, row.note, row.inStateFor]), [
    ['held', 'hold', true, 'running until the deadline', null],
    ['moving', 'moving', false, null, '5m 2s'],
    ['pool', 'hold', true, 'running until the deadline', null],
  ]);
  assert.equal(rows[1].reason, 'the node said so');
  assert.deepEqual(rows.map(pendingKind), ['sandbox', 'sandbox', 'warm-pool member']);
  const past = pendingRows(drain({ deadline: iso(-MINUTE), pending: [pending('held', 'hold')] }), NOW);
  assert.equal(past[0].note, null, 'past the deadline nothing runs until it');
});

test('an evacuated row names its artifact, whether it ran, and whether it is recorded', () => {
  assert.deepEqual(
    evacuatedRows(drain({
      evacuated: [
        evacuated('ran', { was_running: true }),
        evacuated('stopped', { artifact_kind: 'disk', recorded: false }),
      ],
    })),
    [
      { sandbox_id: 'ran', kind: 'snapshot', wasRunning: true, status: 'recorded' },
      { sandbox_id: 'stopped', kind: 'disk', wasRunning: false, status: 'recording' },
    ],
  );
});

// A start names the engine and is start-only; a shorten carries the hold
// alone, so the API reads it as a repeat of the v2 drain in flight and never
// starts one. Neither carries a mode.
test('a v2 drain is started start-only, and shortened with its hold alone', () => {
  assert.deepEqual(HOLD_PRESETS.map((preset) => preset.seconds), [0, 600, 3600]);
  for (const seconds of [0, 600, 3600, 45]) {
    assert.deepEqual(v2StartBody(seconds), { engine: 'v2', hold_seconds: seconds, require_undrained: true });
    assert.deepEqual(v2HoldBody(seconds), { hold_seconds: seconds });
    assert.equal('mode' in v2StartBody(seconds) || 'mode' in v2HoldBody(seconds), false);
  }
  assert.equal(JSON.stringify(v2StartBody(600)), '{"engine":"v2","hold_seconds":600,"require_undrained":true}');
  assert.equal(JSON.stringify(v2HoldBody(0)), '{"hold_seconds":0}');
  assert.deepEqual(v2DrainBodyFor('start', 60), v2StartBody(60));
  assert.deepEqual(v2DrainBodyFor('shorten', 60), v2HoldBody(60));
  assert.equal(isV2DrainBody(v2StartBody(0)), true);
  assert.equal(isV2DrainBody(v2HoldBody(0)), true);
  assert.equal(isV2DrainBody({ mode: 'forced' }), false);
  assert.equal(isV2DrainBody(undefined), false);
});

// A confirm after the node's drain changed (its engine, phase or generation)
// closes the dialog instead of sending.
test("a dialog notices that the node's drain changed under it", () => {
  const draining = { drain_engine: 'v2', drain_phase: 'draining', drain: { generation: 7 } };
  const seen = drainSeen(draining);
  assert.deepEqual(seen, { engine: 'v2', phase: 'draining', generation: 7 });
  assert.equal(drainChanged(seen, drainSeen(draining)), false);
  for (const now of [
    { ...draining, drain_phase: 'drained' },
    { ...draining, drain: { generation: 8 } },
    { drain_engine: null, drain_phase: null, drain: null },
  ]) {
    assert.equal(drainChanged(seen, drainSeen(now)), true, JSON.stringify(now));
  }
  assert.match(DRAIN_CHANGED, /reopen/);
});

// Only a v2 action's confirm closes unsent on a changed drain: a v1 Forced
// drain or Terminate confirm sends as it always has.
test('a changed drain closes only a v2 confirm', () => {
  const action = (node, label) => {
    const item = nodeActions({ role: 'worker', drain_mode: null, ...node }, 'k8s').find((a) => a.label === label);
    assert.ok(item, `${label} on ${JSON.stringify(node)}`);
    return { item, seen: drainSeen(node) };
  };
  const undrained = { drain_engine: null, drain_phase: null, drain: null };
  for (const [case_, { item, seen }, closes] of [
    ['v1 Forced drain on a gentle drain', action({ drain_engine: null, drain_phase: 'draining', drain_mode: 'gentle', drain: null }, 'Forced drain'), false],
    ['v1 Forced drain on an idle node', action(undrained, 'Forced drain'), false],
    ['v1 Terminate', action({ drain_engine: null, drain_phase: 'drained', drain: null }, 'Terminate'), false],
    ['v2 drain', action(undrained, 'v2 drain'), true],
    ['Shorten hold', action({ drain_engine: 'v2', drain_phase: 'draining', drain: { generation: 7, fenced: false } }, 'Shorten hold'), true],
    ['End hold now', action({ drain_engine: 'v2', drain_phase: 'draining', drain: { generation: 7, fenced: false } }, 'End hold now'), true],
    ['v2 Terminate', action({ drain_engine: 'v2', drain_phase: 'drained', drain: { generation: 7, fenced: false } }, 'Terminate'), true],
  ]) {
    const changed = { ...seen, phase: seen.phase === 'drained' ? 'terminating' : 'drained', generation: (seen.generation ?? 0) + 1 };
    assert.equal(closesOnDrainChange(item, seen, changed), closes, `${case_}: a changed drain`);
    assert.equal(closesOnDrainChange(item, seen, seen), false, `${case_}: an unchanged drain`);
  }
});

// A shorten's deadline counts from the drain's start in whole seconds, as the
// API's FLOOR(EXTRACT(EPOCH FROM drain_started_at)) does, so an equal hold
// isn't read as later.
test("a shorten's deadline counts from the drain's start in whole seconds", () => {
  const started = '2026-11-20T10:00:00.750Z';
  const apiDeadline = Math.floor(Date.parse(started) / 1000) * 1000 + 600_000;
  assert.equal(holdDeadlineMs(600, started, true, 0), apiDeadline);
  assert.equal(shortenLengthens(holdDeadlineMs(600, started, true, 0), new Date(apiDeadline).toISOString()), null);
});

test('a custom hold is whole seconds from 0 to a year', () => {
  assert.equal(parseHoldSeconds('0'), 0);
  assert.equal(parseHoldSeconds(' 900 '), 900);
  assert.equal(parseHoldSeconds(String(MAX_HOLD_SECONDS)), MAX_HOLD_SECONDS);
  for (const bad of ['', ' ', '-1', '1.5', '10m', '1e3', String(MAX_HOLD_SECONDS + 1), '99999999999999999999']) {
    assert.equal(parseHoldSeconds(bad), null, JSON.stringify(bad));
  }
});

test("a start's deadline counts from now, a shorten's from the drain's start", () => {
  assert.equal(holdDeadlineMs(600, null, false, NOW), NOW + 600_000);
  assert.equal(holdDeadlineMs(600, iso(-30 * MINUTE), true, NOW), NOW - 20 * MINUTE, 'already passed');
  assert.equal(holdDeadlineMs(3600, iso(-30 * MINUTE), true, NOW), NOW + 30 * MINUTE);
  assert.equal(holdDeadlineMs(600, null, true, NOW), null);
});

// What `routes/admin/nodes/drain_v2.rs` and `nodes.rs` answer, as the
// console's ApiError carries it.
const apiError = (status, body, retryAfter = null) => ({
  status,
  body,
  message: typeof body?.error === 'string' ? body.error : 'Request failed',
  retryAfter,
});

test('the cap refusal shows its counts and Retry-After', () => {
  const refusal = v2DrainRefusal(apiError(409, {
    error: '4 nodes\' v2 drains hold every evacuation slot (SPACE_DRAIN_V2_MAX_EVACUATING_NODES is 4); retry once one reaches drained, or free a slot: shorten a drain in flight with hold_seconds 0, or undrain one',
    code: 'drain_v2_evacuation_cap',
    evacuating: 4,
    max_evacuating: 4,
  }, '60'));
  assert.equal(refusal.title, 'v2 drain refused: every evacuation slot is held');
  assert.match(refusal.reasons[0], /shorten a drain in flight with hold_seconds 0, or undrain one/);
  assert.deepEqual(refusal.reasons.slice(1), ['4 of 4 evacuation slots are held.', 'Retry after 60 s.']);
  // Without the header or the counts, it says what it has.
  assert.deepEqual(
    v2DrainRefusal(apiError(409, { error: 'every slot is held', code: 'drain_v2_evacuation_cap' })).reasons,
    ['every slot is held'],
  );
});

test('the stale heartbeat, the mode, the switch and a longer hold each read as their refusal', () => {
  const cases = [
    [apiError(409, { error: "the node's heartbeat is stale: a v2 drain starts only on a node that answers, since its node does the drain", code: 'drain_v2_node_heartbeat_stale' }),
      "v2 drain refused: the node's heartbeat is stale"],
    [apiError(400, { error: 'a v2 drain takes no mode: drain with hold_seconds 0 to suspend its running sandboxes at once' }),
      'v2 drain refused: a v2 drain takes no mode'],
    [apiError(400, { error: 'drain engine v2 is not enabled on this cluster' }),
      'v2 drains are switched off on this cluster'],
    [apiError(409, { error: "a v2 drain's deadline only moves earlier: hold_seconds counts from the drain's start, and this one would end it later; :undrain and drain again for a longer one", drain_started_at: iso(-MINUTE) }),
      'Shorten refused: the hold would end the drain later'],
  ];
  for (const [err, title] of cases) {
    const refusal = v2DrainRefusal(err);
    assert.equal(refusal?.title, title);
    assert.equal(refusal.reasons[0], err.body.error, 'the API\'s message is shown');
  }
  assert.match(v2DrainRefusal(cases[2][0]).reasons[1], /SPACE_DRAIN_V2_ENABLED/);
});

test('any other answer is left to the API\'s message', () => {
  for (const err of [
    apiError(409, { error: 'drain engine v2 drains Kubernetes nodes only' }),
    apiError(409, { error: 'a drain in flight keeps its engine' }),
    apiError(400, { error: 'hold_seconds must be at most 31536000, a year' }),
    apiError(500, null),
    new Error('network down'),
    null,
    undefined,
  ]) {
    assert.equal(v2DrainRefusal(err), null);
  }
});

test('the stale heartbeat refusal says when to retry, and a pending undrain reads as its own refusal', () => {
  const stale = v2DrainRefusal(apiError(409, { error: "the node's heartbeat is stale", code: 'drain_v2_node_heartbeat_stale' }, '60'));
  assert.deepEqual(stale.reasons, ["the node's heartbeat is stale", 'Retry after 60 s.']);
  const finishing = v2DrainRefusal(apiError(409, { error: "undrain still finishing: the previous v2 drain's cancel hasn't reached the node yet" }));
  assert.equal(finishing.title, "Drain refused: the last v2 drain's undrain is still finishing");
});

test('a drain is read defensively, so no answer can crash the panel', () => {
  const full = drain({
    evacuated_total: ['a'],
    evacuated: [evacuated('a')],
    pending: [pending('b', 'moving', { pool_member: true, reason: 'r', since: iso(0) })],
    blocked: [{ reason: 'x' }],
  });
  assert.deepEqual(readDrain(full), full);
  for (const raw of [null, undefined, 'v2', 7, {}, { generation: 'seven' }]) {
    assert.equal(readDrain(raw), null, JSON.stringify(raw));
  }
  assert.deepEqual(
    readDrain({
      generation: 3,
      evacuated_total: ['a', 4],
      evacuated: [{ artifact_kind: 'full' }, { sandbox_id: 'b' }],
      pending: [{ sandbox_id: 'c' }, 'junk'],
      blocked: [{}, null],
    }),
    {
      generation: 3, deadline: null, cancelling: false, fenced: false, reported_at: null, evacuated_grew_at: null,
      evacuated_total: ['a'],
      evacuated: [{ sandbox_id: 'b', artifact_kind: 'unknown', was_running: false, recorded: false }],
      pending: [{ sandbox_id: 'c', wait: 'unspecified', running: false, pool_member: false, reason: null, since: null }],
      blocked: [{ reason: 'unknown' }],
    },
  );
});

test('an evacuation whose recorded is missing or not true reads as recording, so the drain is not finished', () => {
  for (const recorded of [undefined, null, 'true', 1]) {
    const entry = { sandbox_id: 'a', artifact_kind: 'full', was_running: true };
    if (recorded !== undefined) entry.recorded = recorded;
    const read = readDrain(drain({ evacuated: [entry] }));
    assert.equal(read.evacuated[0].recorded, false, String(recorded));
    assert.notEqual(drainProgress(read, NOW).state, 'finished', String(recorded));
    assert.equal(evacuatedRows(read)[0].status, 'recording', String(recorded));
  }
  assert.equal(drainProgress(readDrain(drain({ evacuated: [evacuated('a')] })), NOW).state, 'finished');
});

test('an engine this console does not know is named, not read as v1', () => {
  assert.equal(unknownEngine({}), null);
  assert.equal(unknownEngine({ drain_engine: 'v1' }), null);
  assert.equal(unknownEngine({ drain_engine: 'v2' }), null);
  assert.equal(unknownEngine({ drain_engine: 'v3' }), 'v3');
});

// `:terminate` of a v2 node is refused only on its phase; the cluster
// controller's fresh check refuses the fence and returns the node to
// drained, with its refusal in drain_last_error.
test("a v2 terminate's refusal after the fresh check is the controller's", () => {
  const error = 'terminate refused: the node reports 1 pending, 0 blocked: sbx_a: the node said why';
  const refused = controllerTerminateRefusal({ drain_engine: 'v2', drain_phase: 'drained', drain_last_error: error });
  assert.equal(refused.title, 'The last terminate was refused: the node is back in drained');
  assert.equal(refused.reasons[0], error, 'the error is shown as written');
  for (const node of [
    { drain_engine: 'v2', drain_phase: 'drained', drain_last_error: 'the drain is stalled' },
    { drain_engine: 'v2', drain_phase: 'terminating', drain_last_error: error },
    { drain_engine: undefined, drain_phase: 'drained', drain_last_error: error },
    { drain_engine: 'v2', drain_phase: 'drained', drain_last_error: null },
  ]) {
    assert.equal(controllerTerminateRefusal(node), null, JSON.stringify(node));
  }
});

test('a shorten that would end the drain later is caught before it is sent', () => {
  const deadline = iso(10 * MINUTE);
  assert.equal(shortenLengthens(NOW + 5 * MINUTE, deadline), null);
  assert.equal(shortenLengthens(NOW + 10 * MINUTE, deadline), null, 'the same deadline is no later');
  assert.match(shortenLengthens(NOW + 11 * MINUTE, deadline), /a deadline only moves earlier/);
  assert.equal(shortenLengthens(null, deadline), null, 'no hold picked yet');
  assert.match(shortenLengthens(NOW, null), /already holds nothing/);
});

// The up-front stopped-sandbox 409 names the count, lists up to 20 sandboxes
// to link, and says how to clear it.
test("a v2 drain refused for the node's stopped sandboxes says which and what to do", () => {
  const ids = Array.from({ length: 25 }, (_, i) => `sbx_${i}`);
  const refused = v2DrainRefusal(apiError(409, {
    error: 'the node holds 25 stopped sandboxes, and a v2 drain doesn\'t move stopped sandboxes',
    code: 'drain_v2_stopped_sandboxes',
    stopped: 25,
    stopped_sandbox_ids: ids,
  }));
  assert.equal(refused.title, 'v2 drain refused: the node holds stopped sandboxes');
  assert.deepEqual(refused.sandboxIds, ids.slice(0, 20));
  assert.ok(refused.reasons.some((reason) => reason.startsWith('25 stopped sandboxes, the first 20 listed')), refused.reasons);
  assert.ok(refused.reasons.some((reason) => reason.includes('delete them, or start them, then drain again')), refused.reasons);
});

// A hold body refused on a node with no drain left: the v2 drain it was for
// has ended, whether drain v2 is on (409) or off (400).
test('a hold refused on a node with no drain says its drain has ended', () => {
  for (const err of [
    apiError(409, { error: 'v1 drains are refused while drain v2 is on', code: 'drain_v1_refused_while_v2_enabled' }),
    apiError(400, { error: 'hold_seconds is for a drain with engine v2 only' }),
  ]) {
    assert.equal(v2HoldRefusal(err).title, 'The v2 drain this was for has ended');
  }
  for (const err of [apiError(409, { error: 'something else' }), apiError(503, null), null]) {
    assert.equal(v2HoldRefusal(err), null);
  }
});

// A v2 drain's holds panel says no v1 sentence, which would tell the operator
// to force the drain or start a stopped sandbox.
test("a v2 node's holds say the v2 drain's sentence, a v1 node's its own", () => {
  assert.equal(holdWhy({ drain_engine: 'v2' }, () => 'v1 sentence'), V2_HOLD_SENTENCE);
  assert.doesNotMatch(V2_HOLD_SENTENCE, /[Ff]orced|start it/);
  assert.equal(holdWhy({}, () => 'v1 sentence'), 'v1 sentence');
});
