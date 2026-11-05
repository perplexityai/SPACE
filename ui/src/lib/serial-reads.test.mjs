import assert from 'node:assert/strict';
import { test } from 'node:test';
import { serialReads, withTimeout } from './serial-reads.ts';

const deferred = () => {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { promise, resolve };
};

// The sandbox page polls every 4 s. A read slower than that still lands, and
// the ticks while it is pending start nothing.
test('a read slower than the poll interval is applied, and no tick overlaps it', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const reads = serialReads();
  const answers = [];
  const applied = [];
  const timer = setInterval(() => {
    void reads.poll(async () => {
      const answer = deferred();
      answers.push(answer);
      applied.push(await answer.promise);
    });
  }, 4000);
  t.mock.timers.tick(4000);
  assert.equal(answers.length, 1);
  // Three more ticks, 12 s, while the first read is pending.
  for (let i = 0; i < 3; i += 1) t.mock.timers.tick(4000);
  assert.equal(answers.length, 1);
  answers[0].resolve({ snapshots_enabled: true });
  await new Promise(setImmediate);
  assert.deepEqual(applied, [{ snapshots_enabled: true }]);
  // The skipped ticks left no reads queued behind it.
  assert.equal(answers.length, 1);
  // Once it has landed, the next tick reads again.
  t.mock.timers.tick(4000);
  assert.equal(answers.length, 2);
  clearInterval(timer);
});

test('an explicit read waits for the pending one, so answers land in order', async () => {
  const reads = serialReads();
  const applied = [];
  const older = deferred();
  const newer = deferred();
  let started = 0;
  const read = (answer) => async () => {
    started += 1;
    applied.push(await answer.promise);
  };
  const polled = reads.poll(read(older));
  const explicit = reads.next(read(newer));
  // The poll is pending, so a second poll skips and the explicit read waits.
  assert.equal(reads.poll(read(deferred())), null);
  await Promise.resolve();
  assert.equal(started, 1);
  newer.resolve('newer');
  older.resolve('older');
  await Promise.all([polled, explicit]);
  assert.equal(started, 2);
  assert.deepEqual(applied, ['older', 'newer']);
});

test('a failed read frees the next one', async () => {
  const reads = serialReads();
  await assert.rejects(reads.poll(async () => { throw new Error('unreachable'); }));
  assert.equal(await reads.poll(async () => 'read'), 'read');
});

// A request that never gets an answer, as fetch behaves: only its signal
// settles it, rejecting with the signal's reason.
const unanswered = (signal) => new Promise((_, reject) => {
  signal.addEventListener('abort', () => reject(signal.reason));
});

// A read that never settles would keep every later tick skipping, so the
// page would stay stale until a reload.
test('a read with no answer fails at its timeout, and the next tick reads again', async (t) => {
  t.mock.timers.enable({ apis: ['setInterval', 'setTimeout'] });
  // The sandbox page's bound; it spans several 4 s ticks.
  const timeoutMs = 30_000;
  const reads = serialReads();
  const failures = [];
  let started = 0;
  const timer = setInterval(() => {
    void reads.poll(() => withTimeout(timeoutMs, (signal) => {
      started += 1;
      return unanswered(signal);
    }))?.catch((err) => failures.push(err));
  }, 4000);
  t.mock.timers.tick(4000);
  assert.equal(started, 1);
  // The ticks before the timeout skip.
  for (let i = 0; i < 6; i += 1) t.mock.timers.tick(4000);
  assert.equal(started, 1);
  assert.equal(failures.length, 0);
  // 30 s after it started, at 34 s, the read fails like any failed read.
  t.mock.timers.tick(6000);
  await new Promise(setImmediate);
  assert.equal(failures.length, 1);
  assert.equal(failures[0].name, 'TimeoutError');
  assert.equal(failures[0].message, 'No answer from the API within 30 s');
  // It freed the queue, so the next tick starts a read.
  t.mock.timers.tick(2000);
  assert.equal(started, 2);
  clearInterval(timer);
});
