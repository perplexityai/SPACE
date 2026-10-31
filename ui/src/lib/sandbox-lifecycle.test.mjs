import assert from 'node:assert/strict';
import { test } from 'node:test';
import { SANDBOX_LIFECYCLE_STATES, bringUpAction } from './sandbox-lifecycle.ts';

test('each resting state is brought up by the action that matches it', () => {
  assert.equal(bringUpAction('stopped')?.action, 'start');
  assert.equal(bringUpAction('created')?.action, 'start');
  assert.equal(bringUpAction('paused')?.action, 'resume');
  assert.equal(bringUpAction('suspended')?.action, 'restore');
});

test('running, transient and terminal states have no bring-up action', () => {
  for (const status of ['started', 'pausing', 'restoring', 'deleting', 'failed', 'quarantined', null]) {
    assert.equal(bringUpAction(status), null);
  }
});

test('the filter offers only states the admin API reports', () => {
  assert.ok(!SANDBOX_LIFECYCLE_STATES.includes('running'));
  assert.ok(!SANDBOX_LIFECYCLE_STATES.includes('deleted'));
  assert.ok(SANDBOX_LIFECYCLE_STATES.includes('quarantined'));
});
