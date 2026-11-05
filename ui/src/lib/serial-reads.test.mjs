import assert from 'node:assert/strict';
import { test } from 'node:test';
import { serialReads } from './serial-reads.ts';

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
