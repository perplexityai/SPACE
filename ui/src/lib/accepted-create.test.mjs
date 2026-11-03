import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  OperationFailedError,
  OperationTimeoutError,
  settleAcceptedCreate,
  terminalErrorParts,
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
    'the template has no published release',
    {
      waitForCreate: fail(new OperationFailedError(
        'failed',
        'template has no published release',
        'template_has_no_release',
        "no release found for template 'synthetic' runtime 'fc' tag 'latest'",
      )),
    },
    {
      type: 'error',
      text: "Sandbox create failed: the template has no published release; build it, or publish a release, first (no release found for template 'synthetic' runtime 'fc' tag 'latest')",
    },
  ],
  [
    'no release matches the requested tag or digest',
    {
      waitForCreate: fail(new OperationFailedError(
        'failed',
        'no template release matches the requested tag or digest and runtime',
        'template_release_not_found',
        null,
      )),
    },
    { type: 'error', text: 'Sandbox create failed: no release of the template matches the requested tag or digest and runtime' },
  ],
  [
    'a reason this console does not know keeps the progress message',
    { waitForCreate: fail(new OperationFailedError('failed', 'sandbox create dependency was not found', 'something_new', 'x')) },
    { type: 'error', text: 'Sandbox create failed: sandbox create dependency was not found' },
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

test("a failed record's terminal error gives the reason and message, when it has them", () => {
  assert.deepEqual(
    terminalErrorParts({ operation_id: 'op', kind: 'create_sandbox', message: 'detail', reason: 'template_release_not_found' }),
    { reason: 'template_release_not_found', detail: 'detail' },
  );
  // Releases without reasons, and records without a terminal error.
  assert.deepEqual(terminalErrorParts({ message: 'detail' }), { reason: null, detail: 'detail' });
  assert.deepEqual(terminalErrorParts(null), { reason: null, detail: null });
  assert.deepEqual(terminalErrorParts('text'), { reason: null, detail: null });
});
