import assert from 'node:assert/strict';
import { test } from 'node:test';
import { exclusiveWith } from './exclusive.ts';

const env = (over = {}) => {
  const calls = { acquire: 0, release: [], wait: 0 };
  return {
    calls,
    env: {
      acquire: () => { calls.acquire += 1; return 'owner-1'; },
      release: (name, owner) => calls.release.push([name, owner]),
      wait: async () => { calls.wait += 1; },
      ...over,
    },
  };
};

test('uses Web Locks when the browser has them', async () => {
  const { env: e, calls } = env({ locks: { request: (name, cb) => Promise.resolve(cb()) } });
  assert.equal(await exclusiveWith(e, 'run:write', () => 7), 7);
  assert.equal(calls.acquire, 0);
});

test('the fallback holds the lock while work runs and releases it after', async () => {
  const { env: e, calls } = env();
  assert.equal(await exclusiveWith(e, 'run:write', () => { assert.equal(calls.release.length, 0); return 'done'; }), 'done');
  assert.deepEqual(calls.release, [['run:write', 'owner-1']]);
});

test('the fallback waits while another tab holds the lock, then takes it', async () => {
  let held = 2;
  const { env: e, calls } = env({ acquire: () => (held-- > 0 ? null : 'owner-2') });
  assert.equal(await exclusiveWith(e, 'run:write', () => 'done'), 'done');
  assert.equal(calls.wait, 2);
  assert.deepEqual(calls.release, [['run:write', 'owner-2']]);
});

test('storage that refuses the lock write runs work unlocked, once, without retrying', async () => {
  const quota = Object.assign(new Error('The quota has been exceeded.'), { name: 'QuotaExceededError' });
  const { env: e, calls } = env({ acquire: () => { throw quota; } });
  let ran = 0;
  assert.equal(await exclusiveWith(e, 'run:write', () => { ran += 1; return 'reported'; }), 'reported');
  assert.equal(ran, 1);
  assert.equal(calls.wait, 0);
  assert.deepEqual(calls.release, []);
});

test('a throw from work is not swallowed, and the lock is released', async () => {
  const { env: e, calls } = env();
  await assert.rejects(exclusiveWith(e, 'run:write', () => { throw new Error('boom'); }), /boom/);
  assert.equal(calls.release.length, 1);
});

test('work that throws after an unlocked fallback still propagates', async () => {
  const { env: e } = env({ acquire: () => { throw new Error('blocked'); } });
  await assert.rejects(exclusiveWith(e, 'run:write', () => { throw new Error('boom'); }), /boom/);
});
