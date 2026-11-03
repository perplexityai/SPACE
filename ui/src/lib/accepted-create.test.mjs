import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  OperationFailedError,
  OperationFollowStoppedError,
  OperationTimeoutError,
  followRollback,
  pollOperation,
  rollbackNoticeKey,
  rollbackOutcome,
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

// A clock that advances only when the poll sleeps.
const fakeClock = () => {
  let at = 0;
  return { now: () => at, sleep: async (ms) => { at += ms; } };
};
const records = (...states) => {
  let i = 0;
  return async () => states[Math.min(i++, states.length - 1)];
};
const record = (state, extra = {}) => ({ state, progress_message: `synthetic ${state}`, terminal_error: null, ...extra });

test('a followed operation ends with its record once it succeeds', async () => {
  const done = record('succeeded');
  const result = await pollOperation(records(record('queued'), record('running'), done), { timeoutMs: 10_000, intervalMs: 800, ...fakeClock() });
  assert.equal(result, done);
});

test('a followed operation that fails reports its reason, not that it was queued', async () => {
  // As when an older API pod accepted `:force-rollback` and dropped its body.
  const failed = record('failed', { terminal_error: { reason: 'invalid_request', message: 'snapshot_name is required' } });
  await assert.rejects(
    pollOperation(records(record('queued'), failed), { timeoutMs: 10_000, intervalMs: 800, ...fakeClock() }),
    (err) => err instanceof OperationFailedError && err.state === 'failed' && err.reason === 'invalid_request' && err.detail === 'snapshot_name is required',
  );
});

test('a followed operation still running at the timeout is reported as running', async () => {
  let reads = 0;
  await assert.rejects(
    pollOperation(async () => { reads += 1; return record('running'); }, { timeoutMs: 4_000, intervalMs: 1_000, ...fakeClock() }),
    (err) => err instanceof OperationTimeoutError && err.message === 'synthetic running',
  );
  assert.equal(reads, 4);
});

test('a rollback outcome says done, failed with its reason, or still running', () => {
  assert.deepEqual(rollbackOutcome('checkpoint-1', null), { type: 'success', text: 'Rolled back to checkpoint-1' });
  const failed = rollbackOutcome('checkpoint-1', new OperationFailedError('failed', 'synthetic failed', 'invalid_request', 'snapshot_name is required'));
  assert.deepEqual(failed, { type: 'error', text: 'Rollback to checkpoint-1 failed: snapshot_name is required (invalid_request)' });
  assert.equal(rollbackOutcome('checkpoint-1', new OperationFailedError('canceled', 'operation canceled')).text, 'Rollback to checkpoint-1 canceled: operation canceled');
  const running = rollbackOutcome('checkpoint-1', new OperationTimeoutError('restoring'));
  assert.equal(running.type, 'warning');
  assert.match(running.text, /still running \(restoring\)/);
  const unread = rollbackOutcome('checkpoint-1', new TypeError('Failed to fetch'));
  assert.equal(unread.type, 'warning');
  assert.match(unread.text, /was queued, but its progress could not be read \(Failed to fetch\)/);
  for (const outcome of [failed, running, unread]) assert.doesNotMatch(outcome.text, /^Snapshot rollback queued$/);
});

test('a followed operation stops being read once its view stops following it', async () => {
  const follower = new AbortController();
  let reads = 0;
  const following = pollOperation(async () => {
    reads += 1;
    if (reads === 2) follower.abort();
    return record('running');
  }, { timeoutMs: 60_000, intervalMs: 1_000, ...fakeClock(), signal: follower.signal });
  await assert.rejects(following, (err) => err instanceof OperationFollowStoppedError);
  assert.equal(reads, 2);
  // Already stopped: nothing is read.
  reads = 0;
  await assert.rejects(
    pollOperation(async () => { reads += 1; return record('running'); }, { timeoutMs: 60_000, intervalMs: 1_000, ...fakeClock(), signal: follower.signal }),
    (err) => err instanceof OperationFollowStoppedError,
  );
  assert.equal(reads, 0);
});

// A rollback followed as `followRollback` follows it, with what it shows and
// whether it read the snapshots again.
const followed = (operationId, snapshotName, signal, follow) => {
  const shown = [];
  const result = { shown, settled: 0 };
  result.done = followRollback({
    snapshotName,
    operationId,
    follow,
    signal,
    show: (notice) => shown.push(notice),
    onSettled: () => { result.settled += 1; },
  });
  return result;
};

test("each rollback's notification is its operation's, so an earlier one cannot replace a later one's", async () => {
  let finishFirst;
  const first = followed('op-1', 'checkpoint-1', new AbortController().signal, () => new Promise((resolve) => { finishFirst = resolve; }));
  const second = followed('op-2', 'checkpoint-2', new AbortController().signal, async () => record('succeeded'));
  await second.done;
  finishFirst(record('succeeded'));
  await first.done;
  assert.deepEqual(first.shown.map((notice) => notice.key), [rollbackNoticeKey('op-1'), rollbackNoticeKey('op-1')]);
  assert.deepEqual(second.shown.map((notice) => notice.key), [rollbackNoticeKey('op-2'), rollbackNoticeKey('op-2')]);
  assert.notEqual(rollbackNoticeKey('op-1'), rollbackNoticeKey('op-2'));
  assert.deepEqual(first.shown.map((notice) => notice.type), ['loading', 'success']);
  assert.equal(first.settled, 1);
  assert.equal(second.settled, 1);
});

test('a rollback whose view closed says the console stopped following it, and does not read the snapshots again', async () => {
  const follower = new AbortController();
  let reads = 0;
  const closed = followed('op-3', 'checkpoint-3', follower.signal, (signal) => pollOperation(async () => {
    reads += 1;
    return record('running');
  }, { timeoutMs: 60_000, intervalMs: 1_000, ...fakeClock(), signal }));
  follower.abort();
  await closed.done;
  assert.equal(closed.settled, 0);
  // The read in flight when the view closed, and none after it.
  assert.equal(reads, 1);
  assert.deepEqual(closed.shown.map((notice) => [notice.key, notice.type]), [['rollback:op-3', 'loading'], ['rollback:op-3', 'info']]);
  assert.match(closed.shown[1].text, /was queued; the console stopped following it when its view closed/);
  // Closed after its operation ended, before the view heard: still no read.
  const late = new AbortController();
  const ended = followed('op-4', 'checkpoint-4', late.signal, async () => { late.abort(); return record('failed'); });
  await ended.done;
  assert.equal(ended.settled, 0);
  assert.equal(ended.shown.at(-1).type, 'info');
});
