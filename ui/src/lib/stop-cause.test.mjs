import assert from 'node:assert/strict';
import { test } from 'node:test';
import { reportedStopCause, stopCauseText, stopReasonLabel } from './stop-cause.ts';

const recorded = { reason: 'pod_terminated', stopped_at: '2026-11-05T16:51:15Z' };

test('a reported cause explains a stopped sandbox', () => {
  assert.deepEqual(reportedStopCause({ status: 'stopped', stop_cause: recorded }), recorded);
  assert.equal(reportedStopCause({ status: 'stopped', stop_cause: { reason: 'vm_exited', stopped_at: recorded.stopped_at } })?.reason, 'vm_exited');
});

test('only a stopped sandbox is explained', () => {
  for (const status of ['started', 'stopping', 'restoring', 'failed', null]) {
    assert.equal(reportedStopCause({ status, stop_cause: recorded }), null, String(status));
  }
});

test('no reported cause reads as a plain stop', () => {
  // A requested stop answers null; an API without the field leaves it out.
  assert.equal(reportedStopCause({ status: 'stopped', stop_cause: null }), null);
  assert.equal(reportedStopCause({ status: 'stopped' }), null);
  // A cause in the round-one shape, without `stopped_at`, is not shown.
  assert.equal(reportedStopCause({ status: 'stopped', stop_cause: { reason: 'vm_exited', at: recorded.stopped_at } }), null);
});

test('the text names the cause and its time', () => {
  assert.match(stopCauseText(recorded), /^Stopped unexpectedly: pod terminated at /);
  assert.equal(stopReasonLabel('vm_exited'), 'VM exited');
  assert.equal(stopReasonLabel('node_lost'), 'node lost');
});
