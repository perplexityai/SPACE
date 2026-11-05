import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ReadTimeoutError, attemptGate, firstReadError, latestRead, refreshAll } from './template-reads.ts';

// A read the test answers when it chooses, and what the slot wrote.
function harness(timeoutMs) {
  const slot = latestRead(timeoutMs);
  const written = [];
  const sent = [];
  const write = { value: (value) => written.push(['value', value]), error: (err) => written.push(['error', err.message]) };
  const read = (label) => (signal) => {
    let answer;
    let fail;
    const promise = new Promise((resolve, reject) => { answer = resolve; fail = reject; });
    sent.push({ label, signal, answer: () => answer(label), fail: (message) => fail(new Error(message)) });
    return promise;
  };
  return { slot, written, sent, write, read };
}

test('a periodic read slower than the refresh interval is applied, with no second read beside it', async () => {
  const { slot, written, sent, write, read } = harness(10_000);
  // Three refresh ticks while the first read is still out.
  const ticks = ['tick-1', 'tick-2', 'tick-3'].map((label) => slot.poll(read(label), write));
  assert.equal(sent.length, 1, 'a tick joins the pending read, it does not send another');
  sent[0].answer();
  assert.deepEqual(await Promise.all(ticks), ['tick-1', 'tick-1', 'tick-1']);
  assert.deepEqual(written, [['value', 'tick-1']]);
  // Once it has answered, the next tick reads again.
  const next = slot.poll(read('tick-4'), write);
  assert.equal(sent.length, 2);
  sent[1].answer();
  assert.equal(await next, 'tick-4');
});

test('a read that never answers times out as a failed read and frees the slot', async () => {
  const { slot, written, sent, write, read } = harness(20);
  await assert.rejects(slot.poll(read('stalled'), write), ReadTimeoutError);
  assert.equal(sent[0].signal.aborted, true, 'the stalled request is cancelled');
  assert.deepEqual(written, [['error', 'no answer within 0.02 s']]);
  const retry = slot.poll(read('retry'), write);
  assert.equal(sent.length, 2, 'the next tick reads again');
  sent[1].answer();
  assert.equal(await retry, 'retry');
});

test('a read superseded by an explicit read or a reset never writes, and its caller gets the newer answer', async () => {
  for (const [name, supersede, settleOld] of [
    // A refresh after a delete: the read sent before it must not bring the template back.
    ['explicit read, older answers last', (h) => h.slot.next(h.read('newer'), h.write), (h) => h.sent[0].answer()],
    ['explicit read, older fails last', (h) => h.slot.next(h.read('newer'), h.write), (h) => h.sent[0].fail('older failed')],
    // The template page opened for another name: the old name's read is dropped.
    ['reset, then a read for the new name', (h) => { h.slot.reset(); return h.slot.next(h.read('newer'), h.write); }, (h) => h.sent[0].answer()],
  ]) {
    const h = harness(10_000);
    const older = h.slot.poll(h.read('older'), h.write);
    const newer = supersede(h);
    assert.equal(h.sent[0].signal.aborted, true, name);
    h.sent[1].answer();
    assert.equal(await newer, 'newer', name);
    settleOld(h);
    assert.equal(await older, 'newer', name);
    assert.deepEqual(h.written, [['value', 'newer']], name);
  }
});

test('a superseded read whose request ignores its signal still settles, and so does the refresh over it', { timeout: 2_000 }, async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  for (const [name, supersede, outcome] of [
    // The spaced-versions read once sent no signal: aborting it did not end it.
    ['explicit read', (h) => h.slot.next(h.read('newer'), h.write), 'newer'],
    // The page closed: no read follows, and the caller must not wait for one.
    ['reset with no next read', (h) => { h.slot.reset(); return null; }, 'ReadDroppedError'],
  ]) {
    const h = harness(20_000);
    const older = h.slot.poll(() => new Promise(() => {}), h.write);
    const calls = [];
    const refreshing = refreshAll([older], { settled: () => calls.push('settled'), succeeded: () => calls.push('succeeded') });
    const newer = supersede(h);
    if (newer) {
      h.sent[0].answer();
      assert.equal(await newer, 'newer', name);
      assert.equal(await older, 'newer', name);
      await refreshing;
      assert.deepEqual(calls, ['succeeded', 'settled'], name);
    } else {
      await assert.rejects(older, { name: outcome }, name);
      await assert.rejects(refreshing, { name: outcome }, name);
      assert.deepEqual(calls, ['settled'], name);
    }
    // The superseded read's timeout went with it.
    t.mock.timers.tick(20_000);
    assert.deepEqual(h.written, newer ? [['value', 'newer']] : [], name);
  }
});

test('a refresh counts as done whether its reads answer or fail, and is marked fresh only when all answer', async () => {
  for (const [name, reads, fails] of [
    ['all answer', [Promise.resolve('builds'), Promise.resolve('inventory')], null],
    // An unrelated read failing must not stop the build statuses refreshing.
    ['the inventory read fails', [Promise.resolve('builds'), Promise.reject(new Error('inventory failed'))], 'inventory failed'],
    ['a pool read fails', [Promise.reject(new Error('warm pools failed')), Promise.resolve('builds')], 'warm pools failed'],
  ]) {
    const calls = [];
    const run = refreshAll(reads, { settled: () => calls.push('settled'), succeeded: () => calls.push('succeeded') });
    if (fails) await assert.rejects(run, { message: fails }, name);
    else await run;
    assert.deepEqual(calls, fails ? ['settled'] : ['succeeded', 'settled'], name);
  }
});

test('a failed refresh is tried again at the refresh interval, not on every tick', () => {
  const gate = attemptGate(10_000);
  assert.equal(gate.due(0), true);
  gate.mark(0);
  assert.equal(gate.due(1_000), false, 'the next 1 s tick waits');
  assert.equal(gate.due(10_000), true);
});

test('a read is an error only while it has never answered', () => {
  assert.equal(firstReadError([{ loaded: false, error: null }]), null, 'still loading');
  assert.equal(firstReadError([{ loaded: true, error: 'later failure' }]), null, 'the last answer stays shown');
  assert.equal(
    firstReadError([{ loaded: true, error: 'x' }, { loaded: false, error: '503 Service Unavailable' }]),
    '503 Service Unavailable',
  );
});
