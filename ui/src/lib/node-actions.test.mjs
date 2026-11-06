import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DRAIN_TERMINATES, FORCED_DRAIN_EVACUATES, TERMINATE_ENDS, nodeActions, uploadLeftOut } from './node-actions.ts';

const names = (node, substrate = 'k8s') => nodeActions(node, substrate).map((a) => a.action);
const node = (drain_phase = null, drain_mode = null, role = 'worker') => ({ role, drain_phase, drain_mode });

const labels = (node, substrate = 'k8s') => nodeActions(node, substrate).map((a) => a.label);
const forced = (node, substrate = 'k8s') => nodeActions(node, substrate).find((a) => a.label === 'Forced drain');

test('an idle worker offers cordon, uncordon, a gentle drain and a forced drain', () => {
  assert.deepEqual(labels(node()), ['Cordon', 'Uncordon', 'Drain', 'Forced drain']);
  assert.deepEqual(names(node()), ['cordon', 'uncordon', 'drain', 'drain']);
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
  for (const role of ['worker', 'builder']) {
    for (const phase of ['drained', 'terminating']) {
      for (const mode of [null, 'gentle', 'forced']) assert.equal(forced(node(phase, mode, role)), undefined, `${role}, phase ${phase}, mode ${mode}`);
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

test('undrain stays available while draining or drained, and no other action needs typing', () => {
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    assert.equal(names(node(phase, 'gentle')).includes('undrain'), phase === 'draining' || phase === 'drained', `phase ${phase}`);
  }
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    for (const action of nodeActions(node(phase, 'gentle'), 'k8s')) {
      assert.equal(!!action.typed, action.label === 'Forced drain' || action.action === 'terminate', action.label);
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

test('the direct substrate keeps only undrain, and other roles get nothing', () => {
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    for (const mode of [null, 'gentle', 'forced']) {
      const expected = phase === 'draining' || phase === 'drained' ? ['undrain'] : [];
      for (const role of ['worker', 'builder']) {
        assert.deepEqual(names(node(phase, mode, role), 'space'), expected, `${role}, phase ${phase}, mode ${mode}`);
      }
      assert.deepEqual(names(node(phase, mode, 'cluster'), 'k8s'), [], `cluster node, phase ${phase}`);
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
