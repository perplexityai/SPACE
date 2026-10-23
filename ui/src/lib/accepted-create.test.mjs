import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  OperationFailedError,
  OperationTimeoutError,
  settleAcceptedCreate,
} from './accepted-create.ts';
import { isHiddenOperationError } from './hidden-operations.ts';

const ok = async () => ({});
const fail = (err) => async () => {
  throw err;
};
const apiError = (status, message) => Object.assign(new Error(message), { status });

for (const [name, steps, expected] of [
  ['no snapshots requested', {}, { type: 'success', text: 'Sandbox created' }],
  [
    'snapshots enabled',
    { waitForCreate: ok, enableSnapshots: ok },
    { type: 'success', text: 'Sandbox created' },
  ],
  [
    'the create poll timed out',
    { waitForCreate: fail(new OperationTimeoutError('operation still running')), enableSnapshots: ok },
    { type: 'warning', text: 'Sandbox creating; check its status. Snapshots were not enabled' },
  ],
  [
    'polling the create failed',
    { waitForCreate: fail(new TypeError('Failed to fetch')), enableSnapshots: ok },
    { type: 'warning', text: 'Sandbox creating; check its status. Snapshots were not enabled' },
  ],
  [
    'the create operation failed',
    { waitForCreate: fail(new OperationFailedError('failed', 'template not found')), enableSnapshots: ok },
    { type: 'error', text: 'Sandbox create failed: template not found' },
  ],
  [
    'snapshots are hidden for the org',
    { waitForCreate: ok, enableSnapshots: fail(apiError(400, 'invalid sandbox action')) },
    { type: 'warning', text: 'Sandbox created; snapshots are not enabled for your organization' },
  ],
  [
    'enabling snapshots failed otherwise',
    { waitForCreate: ok, enableSnapshots: fail(apiError(503, 'unavailable')) },
    { type: 'warning', text: 'Sandbox created; enabling snapshots failed: unavailable' },
  ],
]) {
  test(`settleAcceptedCreate: ${name}`, async () => {
    // Never rejects: an accepted create must not reach the form's error path.
    const outcome = await settleAcceptedCreate({ ...steps, snapshotsOff: isHiddenOperationError });
    assert.deepEqual(outcome, expected);
  });
}

test('settleAcceptedCreate: a failed create does not try to enable snapshots', async () => {
  let enabled = false;
  await settleAcceptedCreate({
    waitForCreate: fail(new OperationFailedError('failed', 'boom')),
    enableSnapshots: async () => {
      enabled = true;
    },
    snapshotsOff: isHiddenOperationError,
  });
  assert.equal(enabled, false);
});
