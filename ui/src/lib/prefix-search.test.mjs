import assert from 'node:assert/strict';
import { test } from 'node:test';
import { PrefixRequests, supplementalMatches } from './prefix-search.ts';

// A request whose answer the test releases by hand, and which rejects when aborted.
function deferred(ticket) {
  let resolve;
  const promise = new Promise((res, rej) => {
    resolve = res;
    ticket.signal.addEventListener('abort', () => rej(new Error('aborted')));
  });
  return { promise, resolve };
}

// What the page does with one request: apply its answer if its ticket still applies.
function run(requests, key, current, shown) {
  const ticket = requests.begin(key);
  const d = deferred(ticket);
  d.promise.then(
    (value) => {
      if (requests.applies(ticket, current())) shown.push(value);
    },
    () => {},
  );
  return { ticket, d };
}

const tick = () => new Promise((resolve) => setImmediate(resolve));

test('an older request that completes after a newer one is dropped', async () => {
  const requests = new PrefixRequests();
  const shown = [];
  const first = run(requests, 'cafe', () => 'cafe', shown);
  const second = run(requests, 'cafe', () => 'cafe', shown);
  assert.equal(first.ticket.signal.aborted, true);
  second.d.resolve('new');
  await tick();
  first.d.resolve('old');
  await tick();
  assert.deepEqual(shown, ['new']);
});

test('searching A, B, A drops the first A even though its key matches again', async () => {
  const requests = new PrefixRequests();
  const shown = [];
  let current = 'a';
  const a1 = run(requests, 'a', () => current, shown);
  current = 'b';
  const b = run(requests, 'b', () => current, shown);
  current = 'a';
  const a2 = run(requests, 'a', () => current, shown);
  assert.equal(requests.applies(a1.ticket, current), false);
  a1.d.resolve('a-first');
  await tick();
  assert.deepEqual(shown, []);
  b.d.resolve('b');
  await tick();
  assert.deepEqual(shown, []);
  a2.d.resolve('a-second');
  await tick();
  assert.deepEqual(shown, ['a-second']);
});

test('only one request is in flight at a time', () => {
  const requests = new PrefixRequests();
  const tickets = [requests.begin('a'), requests.begin('a'), requests.begin('b')];
  assert.deepEqual(tickets.map((t) => t.signal.aborted), [true, true, false]);
});

test('cancel aborts the request in flight and voids its answer', () => {
  const requests = new PrefixRequests();
  const ticket = requests.begin('a');
  requests.cancel();
  assert.equal(ticket.signal.aborted, true);
  assert.equal(requests.applies(ticket, 'a'), false);
});

test('a lone request applies for its own search only', () => {
  const requests = new PrefixRequests();
  const ticket = requests.begin('a');
  assert.equal(requests.applies(ticket, 'a'), true);
  assert.equal(requests.applies(ticket, 'b'), false);
  assert.equal(requests.applies(ticket, null), false);
});

test('supplemental matches leave out the rows the main table shows', () => {
  const matches = [{ id: 'sbx_1' }, { id: 'sbx_2' }, { id: 'sbx_3' }];
  assert.deepEqual(supplementalMatches([matches], [{ id: 'sbx_2' }]).map((r) => r.id), ['sbx_3', 'sbx_1']);
  assert.deepEqual(supplementalMatches([matches], []).length, 3);
  assert.deepEqual(supplementalMatches([matches], matches), []);
});

test('the prefix and ID search matches merge once per sandbox, newest first, without the rows shown', () => {
  const row = (id, minute) => ({ id, created_at: `2026-11-06T04:${String(minute).padStart(2, '0')}:00.000000Z` });
  // A hex-looking name found by its prefix, an ID found by its end, and one
  // sandbox both found, which the page also shows in one case.
  const byPrefix = [row('sbx_e2e1-name', 30), row('sbx_both', 20)];
  const byId = [row('sbx_both', 20), row('sbx_label', 25), row('sbx_older', 5)];
  assert.deepEqual(
    supplementalMatches([byPrefix, byId], []).map((r) => r.id),
    ['sbx_e2e1-name', 'sbx_label', 'sbx_both', 'sbx_older'],
  );
  assert.deepEqual(
    supplementalMatches([byPrefix, byId], [row('sbx_both', 20)]).map((r) => r.id),
    ['sbx_e2e1-name', 'sbx_label', 'sbx_older'],
  );
  // Equal creation times order by ID, newest first, as the list does.
  assert.deepEqual(supplementalMatches([[row('sbx_a', 1)], [row('sbx_b', 1)]], []).map((r) => r.id), ['sbx_b', 'sbx_a']);
  // Either answer alone, or neither.
  assert.deepEqual(supplementalMatches([[], byId], []).map((r) => r.id), ['sbx_label', 'sbx_both', 'sbx_older']);
  assert.deepEqual(supplementalMatches([[], []], []), []);
});
