import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  SANDBOX_LIFECYCLE_STATES,
  SANDBOX_STATUS_FILTER_STATES,
  bringUpAction,
  chooseAppliedStatuses,
  splitStatusFilter,
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
  // Every lifecycle and both tombstones; node-only states and the no-status
  // key are never stored.
  assert.deepEqual(storedLifecycles([...SANDBOX_LIFECYCLE_STATES]), [...SANDBOX_LIFECYCLE_STATES]);
  assert.deepEqual(storedLifecycles(['started', 'unknown', 'cleanup_failed', 'unspecified', 'deleted', 'cleanup_required']), [
    'started', 'deleted', 'cleanup_required',
  ]);
});

test('a status a paged list cannot send is shown as not applied, not as a filter', () => {
  // An older `?status=unknown` link, or statuses chosen while the list was
  // sampled.
  assert.deepEqual(splitStatusFilter(true, ['unknown']), { applied: [], notApplied: ['unknown'] });
  assert.deepEqual(splitStatusFilter(true, ['started', 'cleanup_failed', 'deleted']), {
    applied: ['started', 'deleted'], notApplied: ['cleanup_failed'],
  });
  // A sampled list matches each against its rows' statuses.
  assert.deepEqual(splitStatusFilter(false, ['unknown', 'started']), { applied: ['unknown', 'started'], notApplied: [] });
});

test('changing the lifecycle control keeps the statuses a paged list does not apply', () => {
  const current = ['started', 'unknown', 'cleanup_failed'];
  // Adding, removing and clearing lifecycles leaves the not-applied ones.
  assert.deepEqual(chooseAppliedStatuses(true, current, ['started', 'paused']), ['started', 'paused', 'unknown', 'cleanup_failed']);
  assert.deepEqual(chooseAppliedStatuses(true, current, []), ['unknown', 'cleanup_failed']);
  const kept = chooseAppliedStatuses(true, current, ['suspended']);
  assert.deepEqual(splitStatusFilter(true, kept), { applied: ['suspended'], notApplied: ['unknown', 'cleanup_failed'] });
  // A sampled list applies every status, so the control holds them all.
  assert.deepEqual(chooseAppliedStatuses(false, current, ['unknown']), ['unknown']);
});
