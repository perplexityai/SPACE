import assert from 'node:assert/strict';
import { test } from 'node:test';
import { runMutation } from './mutation-refresh.ts';

test("a change's result is told before its refresh, and a failed refresh is not the change failing", async () => {
  for (const [name, mutate, refresh, told, tookEffect] of [
    // A tag moved, then the releases could not be read again.
    ['the change succeeds, its refresh fails', () => Promise.resolve('moved'), () => Promise.reject(new Error('503')),
      ['succeeded moved', 'refresh', 'refresh failed 503'], true],
    ['both succeed', () => Promise.resolve('moved'), () => Promise.resolve(),
      ['succeeded moved', 'refresh'], true],
    ['the change fails', () => Promise.reject(new Error('412')), () => Promise.resolve(),
      ['failed 412'], false],
  ]) {
    const calls = [];
    const ran = await runMutation({
      mutate,
      succeeded: (value) => calls.push(`succeeded ${value}`),
      failed: (err) => calls.push(`failed ${err.message}`),
      refresh: () => { calls.push('refresh'); return refresh(); },
      refreshFailed: (err) => calls.push(`refresh failed ${err.message}`),
    });
    assert.deepEqual(calls, told, name);
    assert.equal(ran, tookEffect, name);
  }
});
