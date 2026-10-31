import assert from 'node:assert/strict';
import { test } from 'node:test';
import { SANDBOX_LIFECYCLE_STATES, SANDBOX_STATUS_FILTER_STATES, bringUpAction } from './sandbox-lifecycle.ts';

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

test('the filter offers the states the admin list reports: node-reported ones and tombstones too', () => {
  assert.ok(!SANDBOX_STATUS_FILTER_STATES.includes('running'));
  assert.ok(SANDBOX_STATUS_FILTER_STATES.includes('quarantined'));
  assert.ok(SANDBOX_STATUS_FILTER_STATES.includes('cleanup_required'));
  assert.ok(SANDBOX_STATUS_FILTER_STATES.includes('cleanup_failed'));
  assert.ok(SANDBOX_STATUS_FILTER_STATES.includes('deleted'));
  for (const state of SANDBOX_LIFECYCLE_STATES) assert.ok(SANDBOX_STATUS_FILTER_STATES.includes(state));
});

test('a deleted tombstone has no bring-up action', () => {
  assert.equal(bringUpAction('deleted'), null);
});
