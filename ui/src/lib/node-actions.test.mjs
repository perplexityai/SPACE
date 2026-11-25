import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DRAIN_TERMINATES, FORCED_DRAIN_EVACUATES, TERMINATE_ENDS, V2_DRAIN_ENDS, V2_DRAIN_STARTS, V2_END_HOLD, V2_SHORTEN_HOLD, V2_TERMINATE_ENDS, V2_UNDRAIN_CANCELS, nodeActions, uploadLeftOut } from './node-actions.ts';

const names = (node, substrate = 'k8s') => nodeActions(node, substrate).map((a) => a.action);
const node = (drain_phase = null, drain_mode = null, role = 'worker') => ({ role, drain_phase, drain_mode });

const labels = (node, substrate = 'k8s') => nodeActions(node, substrate).map((a) => a.label);
const forced = (node, substrate = 'k8s') => nodeActions(node, substrate).find((a) => a.label === 'Forced drain');
const forcesDrain = (node, substrate = 'k8s') => nodeActions(node, substrate).some((a) => a.action === 'drain' && a.body?.mode === 'forced');

test('an idle worker offers cordon, uncordon, a gentle drain, a forced drain and a v2 drain', () => {
  assert.deepEqual(labels(node()), ['Cordon', 'Uncordon', 'Drain', 'Forced drain', 'v2 drain']);
  assert.deepEqual(names(node()), ['cordon', 'uncordon', 'drain', 'drain', 'drain']);
  assert.deepEqual(labels(node(null, null, 'builder')), ['Cordon', 'Uncordon', 'Drain', 'Forced drain'], 'a v2 drain is a worker\'s');
  const drain = nodeActions(node(), 'k8s').find((a) => a.label === 'Drain');
  assert.deepEqual(drain.body, { mode: 'gentle' });
  assert.ok(drain.confirm.includes(DRAIN_TERMINATES));
  assert.ok(drain.dangerous);
  assert.ok(!drain.typed);
});

test('a forced drain sends mode forced, says what it evacuates, and needs a typed confirmation', () => {
  const action = forced(node());
  assert.equal(action.action, 'drain');
  assert.deepEqual(action.body, { mode: 'forced' });
  assert.ok(action.dangerous && action.typed);
  assert.ok(action.confirm.includes(FORCED_DRAIN_EVACUATES));
  assert.ok(action.confirm.includes(DRAIN_TERMINATES));
});

test('forced drain escalates a gentle drain, including one with no recorded mode, but not a forced one', () => {
  for (const mode of [null, 'gentle']) {
    const action = forced(node('draining', mode));
    assert.ok(action, `escalation offered for mode ${mode}`);
    assert.match(action.confirm, /in place/);
  }
  assert.equal(forced(node('draining', 'forced')), undefined);
  assert.ok(!forcesDrain(node('draining', 'forced')), 'draining + forced');
  for (const role of ['worker', 'builder']) {
    for (const phase of ['drained', 'terminating']) {
      for (const mode of [null, 'gentle', 'forced']) {
        assert.equal(forced(node(phase, mode, role)), undefined, `${role}, phase ${phase}, mode ${mode}`);
        assert.ok(!forcesDrain(node(phase, mode, role)), `${role}, phase ${phase}, mode ${mode}: no forced drain action`);
      }
    }
  }
});

test('terminate is offered only on a drained node, behind a typed confirmation', () => {
  for (const role of ['worker', 'builder']) {
    for (const phase of [null, 'draining', 'drained', 'terminating']) {
      for (const mode of [null, 'gentle', 'forced']) {
        const terminate = nodeActions(node(phase, mode, role), 'k8s').find((a) => a.action === 'terminate');
        assert.equal(!!terminate, phase === 'drained', `${role}, phase ${phase}, mode ${mode}`);
        if (terminate) {
          assert.ok(terminate.dangerous && terminate.typed);
          assert.equal(terminate.confirm, TERMINATE_ENDS);
          assert.equal(terminate.body, undefined);
        }
      }
    }
  }
});

test('a draining v2 drain ends its hold instead of switching mode, typed, and nothing else escalates it', () => {
  const v2 = (drain_phase, drain_mode = 'gentle') => ({ ...node(drain_phase, drain_mode), drain_engine: 'v2' });
  const end = nodeActions(v2('draining'), 'k8s').find((a) => a.label === 'End hold now');
  assert.equal(end.action, 'drain');
  // The hold alone: a repeat of the v2 drain in flight, never a start.
  assert.deepEqual(end.body, { hold_seconds: 0 });
  assert.ok(end.dangerous && end.typed);
  assert.ok(end.confirm.includes(V2_END_HOLD));
  assert.ok(end.confirm.includes(V2_DRAIN_ENDS));
  assert.ok(!end.confirm.includes(DRAIN_TERMINATES), 'the drained-node terminator leaves v2 drains alone');
  const shorten = nodeActions(v2('draining'), 'k8s').find((a) => a.label === 'Shorten hold');
  assert.equal(shorten.action, 'drain');
  assert.equal(shorten.hold, 'shorten');
  assert.equal(shorten.body, undefined, 'the hold the operator picks makes the body');
  assert.ok(shorten.dangerous && shorten.typed);
  assert.ok(shorten.confirm.includes(V2_SHORTEN_HOLD));
  assert.deepEqual(labels(v2('draining')), ['Shorten hold', 'End hold now', 'Undrain']);
  for (const mode of [null, 'gentle', 'forced']) {
    assert.ok(!forcesDrain(v2('draining', mode)), `a v2 drain is never sent a mode (${mode})`);
  }
  assert.deepEqual(labels(v2('drained')), ['Undrain', 'Terminate']);
  assert.deepEqual(names(v2('terminating')), ['undrain'], 'unfenced');
  assert.equal(nodeActions(v2('draining'), 'space').find((a) => a.label === 'End hold now'), undefined, 'not on Direct');
  // A v1 drain still switches to forced.
  assert.ok(forced(node('draining', 'gentle')));
});

test('a v2 drain starts on an idle worker with a hold the operator picks, typed, beside the unchanged v1 choices', () => {
  const start = nodeActions(node(), 'k8s').find((a) => a.label === 'v2 drain');
  assert.equal(start.action, 'drain');
  assert.equal(start.hold, 'start');
  assert.equal(start.body, undefined);
  assert.ok(start.dangerous && start.typed);
  assert.ok(start.confirm.includes(V2_DRAIN_STARTS));
  assert.ok(start.confirm.includes(V2_DRAIN_ENDS));
  // The v1 choices, and v1 as the plain Drain, are as they were.
  const v1 = nodeActions(node(), 'k8s').filter((a) => a.action === 'drain' && !a.hold);
  assert.deepEqual(v1.map((a) => [a.label, a.body]), [['Drain', { mode: 'gentle' }], ['Forced drain', { mode: 'forced' }]]);
  // Not on Direct, not while any drain runs.
  assert.equal(nodeActions(node(), 'space').find((a) => a.hold), undefined);
  for (const phase of ['draining', 'drained', 'terminating']) {
    assert.equal(nodeActions(node(phase, 'gentle'), 'k8s').find((a) => a.hold === 'start'), undefined, phase);
  }
  // An undrained v2 drain still being cancelled has no phase: no drain is offered.
  assert.equal(nodeActions({ ...node(), drain_engine: 'v2' }, 'k8s').find((a) => a.hold === 'start'), undefined);
});

test('while a v2 cancel is pending, no drain starts and Uncordon is hidden: the API refuses both until it finishes', () => {
  const cancelling = { ...node(), drain_engine: 'v2' };
  assert.deepEqual(labels(cancelling), ['Cordon']);
  assert.equal(forcesDrain(cancelling), false);
  assert.deepEqual(labels(node()), ['Cordon', 'Uncordon', 'Drain', 'Forced drain', 'v2 drain'], 'an idle worker keeps every choice');
});

test('a v2 node in terminating can be undrained until its instance is fenced, and its terminate says what the controller checks', () => {
  const terminating = { ...node('terminating'), drain_engine: 'v2' };
  assert.ok(names({ ...terminating, drain: { fenced: false } }).includes('undrain'));
  assert.ok(!names({ ...terminating, drain: { fenced: true } }).includes('undrain'));
  const drained = { ...node('drained'), drain_engine: 'v2' };
  const terminate = nodeActions(drained, 'k8s').find((a) => a.action === 'terminate');
  assert.ok(terminate.confirm.includes('returns the node to drained'));
});

test('undrain stays available while draining or drained, and no other action needs typing', () => {
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    assert.equal(names(node(phase, 'gentle')).includes('undrain'), phase === 'draining' || phase === 'drained', `phase ${phase}`);
  }
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    for (const action of nodeActions(node(phase, 'gentle'), 'k8s')) {
      assert.equal(!!action.typed, ['Forced drain', 'End hold now', 'Shorten hold', 'v2 drain'].includes(action.label) || action.action === 'terminate', action.label);
    }
  }
});

test('upload is offered only where the API accepts it', () => {
  assert.ok(!names(node('draining', 'gentle')).includes('force-upload-local-snapshots'));
  assert.ok(names(node('draining', 'forced')).includes('force-upload-local-snapshots'));
  assert.ok(names(node('drained', 'gentle')).includes('force-upload-local-snapshots'));
  assert.ok(names(node('terminating', 'gentle')).includes('force-upload-local-snapshots'));
  assert.deepEqual(names(node('terminating', 'gentle')), ['force-upload-local-snapshots']);
});

// The API refuses it under a v2 drain in every phase and while its cancel is
// pending: the sealed node itself uploads each local snapshot of a suspended
// resident that it has not recorded as backed up.
test('upload is never offered on a v2 drain\'s node', () => {
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    for (const mode of [null, 'gentle', 'forced']) {
      for (const fenced of [false, true]) {
        const v2 = { ...node(phase, mode), drain_engine: 'v2', drain: { fenced } };
        assert.ok(!names(v2).includes('force-upload-local-snapshots'), `phase ${phase}, mode ${mode}, fenced ${fenced}`);
      }
    }
  }
});

test('the direct substrate keeps only undrain, and other roles get nothing', () => {
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    for (const mode of [null, 'gentle', 'forced']) {
      const expected = phase === 'draining' || phase === 'drained' ? ['undrain'] : [];
      for (const role of ['worker', 'builder']) {
        assert.deepEqual(names(node(phase, mode, role), 'space'), expected, `${role}, phase ${phase}, mode ${mode}`);
      }
      assert.deepEqual(names(node(phase, mode, 'cluster'), 'k8s'), [], `cluster node, phase ${phase}`);
      assert.deepEqual(names(node(phase, mode, 'control'), 'k8s'), [], `unknown role, phase ${phase}, mode ${mode}`);
    }
  }
  assert.ok(names(node('drained', 'gentle', 'builder'), 'k8s').includes('terminate'));
});

test("the upload confirmation explains the per-call limits without the server's numbers", () => {
  const upload = nodeActions(node('drained', 'gentle'), 'k8s').find((a) => a.action === 'force-upload-local-snapshots');
  assert.match(upload.confirm, /limited number of the pinned sandboxes/);
  assert.match(upload.confirm, /not attempted/);
  assert.doesNotMatch(upload.confirm, /\d/);
});

test('the upload result says what the call left out from its deferred and truncated fields', () => {
  assert.deepEqual(uploadLeftOut({ deferred: [], truncated: false }), []);
  assert.deepEqual(uploadLeftOut({}), []);
  assert.deepEqual(uploadLeftOut({ deferred: ['sbx_a'] }), ['1 listed sandbox not attempted in this call.']);
  assert.deepEqual(uploadLeftOut({ deferred: ['sbx_a', 'sbx_b', 'sbx_c'], truncated: true }), [
    '3 listed sandboxes not attempted in this call.',
    'More sandboxes are pinned to this node than this call listed.',
  ]);
});

// A v2 node's undrain says it stays cordoned until the controller cancels
// the drain; its start and terminate say what the drain doesn't move and what
// holds a terminate.
test("a v2 drain's texts: its undrain, its start's stopped sandboxes and refused stops, its held terminate", () => {
  const v2 = (drain_phase) => ({ ...node(drain_phase, null), drain_engine: 'v2' });
  const undrain = nodeActions(v2('draining'), 'k8s').find((a) => a.action === 'undrain');
  assert.equal(undrain.confirm, V2_UNDRAIN_CANCELS);
  assert.match(V2_UNDRAIN_CANCELS, /stays cordoned until the cluster controller/);
  assert.notEqual(nodeActions(node('draining', 'gentle'), 'k8s').find((a) => a.action === 'undrain').confirm, V2_UNDRAIN_CANCELS);
  assert.match(V2_DRAIN_STARTS, /but a running or stopped one/);
  assert.match(V2_DRAIN_STARTS, /:stop, :kill and rollbacks without restart/);
  assert.match(V2_TERMINATE_ENDS, /terminate held/);
  assert.match(V2_TERMINATE_ENDS, /A failed or non-empty check returns the node to drained/);
});
