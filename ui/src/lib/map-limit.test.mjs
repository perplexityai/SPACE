import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mapLimit } from './map-limit.ts';

test('mapLimit keeps at most `limit` calls in flight and the input order', async () => {
  let inFlight = 0;
  let peak = 0;
  const results = await mapLimit([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3, async (n) => {
    inFlight += 1;
    peak = Math.max(peak, inFlight);
    await new Promise((r) => setTimeout(r, 5));
    inFlight -= 1;
    if (n === 4) throw new Error('four');
    return n * 2;
  });
  assert.equal(peak, 3);
  assert.deepEqual(results.map((r) => (r.status === 'fulfilled' ? r.value : r.reason.message)), [2, 4, 6, 'four', 10, 12, 14, 16, 18, 20]);
});

test('mapLimit of nothing is nothing', async () => {
  assert.deepEqual(await mapLimit([], 6, async () => 1), []);
});
