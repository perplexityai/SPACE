import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DRAIN_TERMINATES, nodeActions } from './node-actions.ts';

const names = (node, substrate = 'kubernetes') => nodeActions(node, substrate).map((a) => a.action);
const node = (drain_phase = null, drain_mode = null, role = 'worker') => ({ role, drain_phase, drain_mode });

test('an idle worker offers cordon, uncordon and a gentle drain only', () => {
  assert.deepEqual(names(node()), ['cordon', 'uncordon', 'drain']);
  const drain = nodeActions(node(), 'kubernetes').find((a) => a.action === 'drain');
  assert.deepEqual(drain.body, { mode: 'gentle' });
  assert.ok(drain.confirm.includes(DRAIN_TERMINATES));
  assert.ok(drain.dangerous);
});

test('forced drain and terminate are never offered', () => {
  for (const phase of [null, 'draining', 'drained', 'terminating']) {
    for (const mode of [null, 'gentle', 'forced']) {
      const actions = nodeActions(node(phase, mode), 'kubernetes');
      assert.ok(!actions.some((a) => a.action === 'terminate'));
      assert.ok(!actions.some((a) => a.action === 'drain' && a.body?.mode === 'forced'));
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
  assert.deepEqual(names(node(), 'space'), []);
  assert.deepEqual(names(node('drained', 'gentle'), 'space'), ['undrain']);
  assert.deepEqual(names(node(null, null, 'control'), 'kubernetes'), []);
});
