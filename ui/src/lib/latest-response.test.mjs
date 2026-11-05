import assert from 'node:assert/strict';
import { test } from 'node:test';
import { latestResponse } from './latest-response.ts';

const deferred = () => {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { promise, resolve };
};

test('an older read that finishes last does not overwrite the newer one', async () => {
  const guard = latestResponse();
  const applied = [];
  const read = async (answer) => {
    const current = guard.begin();
    const value = await answer.promise;
    if (current()) applied.push(value);
  };
  const older = deferred();
  const newer = deferred();
  const first = read(older);
  const second = read(newer);
  newer.resolve({ snapshots_enabled: true });
  await second;
  older.resolve({ snapshots_enabled: false });
  await first;
  assert.deepEqual(applied, [{ snapshots_enabled: true }]);
});

test('a read stays current until a newer one starts', async () => {
  const guard = latestResponse();
  const first = guard.begin();
  assert.equal(first(), true);
  const second = guard.begin();
  assert.equal(second(), true);
  assert.equal(first(), false);
});
