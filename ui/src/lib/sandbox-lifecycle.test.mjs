import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  SANDBOX_LIFECYCLE_STATES,
  SANDBOX_STATUS_FILTER_STATES,
  STORED_LIFECYCLE_FILTER_STATES,
  bringUpAction,
  statusFilterKey,
  statusFilterOptions,
  storedLifecycles,
} from './sandbox-lifecycle.ts';

const row = (status) => status;

test('each resting state is brought up by the action that matches it', () => {
  assert.equal(bringUpAction(row('stopped'))?.action, 'start');
  assert.equal(bringUpAction(row('created'))?.action, 'start');
  assert.equal(bringUpAction(row('paused'))?.action, 'resume');
  assert.equal(bringUpAction(row('suspended'))?.action, 'restore');
});

test('running, transient and terminal states have no bring-up action', () => {
  for (const status of ['started', 'pausing', 'restoring', 'deleting', 'failed', 'quarantined', null]) {
    assert.equal(bringUpAction(row(status)), null);
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
  assert.equal(bringUpAction(row('deleted')), null);
});

test('the filter offers unspecified, which nodes can report', () => {
  assert.ok(SANDBOX_STATUS_FILTER_STATES.includes('unspecified'));
});

test('the filter also offers every other status the loaded rows carry', () => {
  const options = statusFilterOptions(['started', 'running', 'zeta_state', null, 'running']);
  assert.deepEqual(options.slice(0, SANDBOX_STATUS_FILTER_STATES.length), [...SANDBOX_STATUS_FILTER_STATES]);
  assert.deepEqual(options.slice(SANDBOX_STATUS_FILTER_STATES.length), ['running', 'unknown', 'zeta_state']);
  assert.deepEqual(statusFilterOptions([]), [...SANDBOX_STATUS_FILTER_STATES]);
  // A row without a status filters under the option it is offered as.
  assert.equal(statusFilterKey(null), 'unknown');
  assert.equal(statusFilterKey('started'), 'started');
});

test('the paged list sends only stored lifecycles', () => {
  assert.deepEqual(STORED_LIFECYCLE_FILTER_STATES, [...SANDBOX_LIFECYCLE_STATES, 'deleted', 'cleanup_required']);
  // Node-only states and the no-status key are never stored.
  assert.deepEqual(storedLifecycles(['started', 'unknown', 'cleanup_failed', 'unspecified', 'deleted', 'cleanup_required']), [
    'started', 'deleted', 'cleanup_required',
  ]);
});
