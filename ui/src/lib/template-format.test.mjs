import assert from 'node:assert/strict';
import { test } from 'node:test';
import { LATEST_TAG, buildsRead, currentTagHolder, retagStep, retagStepNow, shortHash, tagHolder, tagHolderChange, tagHolders } from './template-format.ts';
import { singleFlight } from './single-flight.ts';

const HEX = '0123456789abcdef'.repeat(4);
const holdersOf = (holders) => new Map(Object.entries(holders));

test('shortHash keeps 12 hex characters whatever the algorithm prefix', () => {
  assert.equal(shortHash(`blake3:${HEX}`), '0123456789ab');
  assert.equal(shortHash(`sha256:${HEX}`), '0123456789ab');
  assert.equal(shortHash(`sha3256:${HEX}`), '0123456789ab');
});

test('shortHash handles a digest with no prefix', () => {
  assert.equal(shortHash(HEX), '0123456789ab');
});

test('shortHash falls back when there is no digest', () => {
  assert.equal(shortHash(null), LATEST_TAG);
  assert.equal(shortHash(undefined, 'none'), 'none');
  assert.equal(shortHash('', ''), '');
});

test("the tag's current release is the one of that template holding it, across runtimes", () => {
  const releases = [
    { id: 'r1', template_name: 'seed', tags: ['latest', 'prod'] },
    { id: 'r2', template_name: 'seed', tags: [] },
    { id: 'r3', template_name: 'other', tags: ['canary'] },
  ];
  assert.equal(currentTagHolder(releases, 'seed', 'prod'), 'r1');
  // A tag only another template holds does not exist for this one.
  assert.equal(currentTagHolder(releases, 'seed', 'canary'), null);
  assert.equal(currentTagHolder(releases, 'other', 'canary'), 'r3');
});

test("a template's tag holders are read per template; a later read shows how a tag moved", () => {
  const opened = tagHolders([
    { id: 'r1', template_name: 'seed', tags: ['latest', 'prod'] },
    { id: 'r2', template_name: 'seed', tags: ['canary'] },
    { id: 'r3', template_name: 'other', tags: ['prod'] },
  ], 'seed');
  assert.deepEqual(opened, holdersOf({ latest: 'r1', prod: 'r1', canary: 'r2' }));
  // The precondition is what the operator saw, not what a refresh read later.
  assert.equal(tagHolder(opened, 'prod'), 'r1');
  assert.equal(tagHolder(opened, 'staging'), null);
  // Inherited names are not tags.
  assert.equal(tagHolder(opened, 'constructor'), null);
  const refreshed = tagHolders([
    { id: 'r1', template_name: 'seed', tags: ['latest'] },
    { id: 'r2', template_name: 'seed', tags: ['canary', 'prod'] },
    { id: 'r4', template_name: 'seed', tags: ['staging'] },
  ], 'seed');
  assert.deepEqual(tagHolderChange(opened, refreshed, 'prod'), { from: 'r1', to: 'r2' });
  assert.deepEqual(tagHolderChange(opened, refreshed, 'staging'), { from: null, to: 'r4' });
  assert.equal(tagHolderChange(opened, refreshed, 'latest'), null);
  assert.equal(tagHolderChange(opened, refreshed, 'unused'), null);
  // A tag deleted meanwhile moved to nowhere.
  assert.deepEqual(tagHolderChange(refreshed, new Map(), 'staging'), { from: 'r4', to: null });
});

test('a template that already has the tag __proto__ keeps its holder, and its move is seen', () => {
  const releases = [
    { id: 'r1', template_name: 'seed', tags: ['latest', '__proto__'] },
    { id: 'r2', template_name: 'seed', tags: ['__proto__'] },
  ];
  const opened = tagHolders(releases, 'seed');
  assert.equal(tagHolder(opened, '__proto__'), 'r1');
  assert.deepEqual([...opened.keys()], ['latest', '__proto__']);
  assert.equal(currentTagHolder(releases, 'seed', '__proto__'), 'r1');
  const moved = tagHolders([
    { id: 'r1', template_name: 'seed', tags: ['latest'] },
    { id: 'r2', template_name: 'seed', tags: ['__proto__'] },
  ], 'seed');
  assert.deepEqual(tagHolderChange(opened, moved, '__proto__'), { from: 'r1', to: 'r2' });
  // The dialog warns, advances the precondition to r2, and then moves from there.
  const warned = retagStep({ holders: opened, change: null }, moved, '__proto__');
  assert.equal(warned.kind, 'warn');
  assert.equal(tagHolder(warned.baseline.holders, '__proto__'), 'r2');
  assert.deepEqual(retagStep(warned.baseline, moved, '__proto__'), { kind: 'move', expected: 'r2' });
  // Deleted meanwhile: the precondition becomes "does not exist".
  const deleted = retagStep({ holders: opened, change: null }, new Map(), '__proto__');
  assert.equal(deleted.baseline.holders.has('__proto__'), false);
  assert.deepEqual(retagStep(deleted.baseline, new Map(), '__proto__'), { kind: 'move', expected: null });
});

const opened = (holders) => ({ holders: holdersOf(holders), change: null });

test('the retag dialog advances only the warned tag, so another tag that moved warns again instead of being overwritten', () => {
  // Opened with prod on A and canary on B; both moved while it was open.
  const atOpen = opened({ prod: 'rel-a', canary: 'rel-b' });
  const now = holdersOf({ prod: 'rel-c', canary: 'rel-d' });
  const warned = retagStep(atOpen, now, 'prod');
  assert.equal(warned.kind, 'warn');
  assert.deepEqual(warned.baseline.change, { tag: 'prod', from: 'rel-a', to: 'rel-c' });
  assert.deepEqual(warned.baseline.holders, holdersOf({ prod: 'rel-c', canary: 'rel-b' }));
  // Switched to canary: its move is shown first, not sent over.
  const canary = retagStep(warned.baseline, now, 'canary');
  assert.equal(canary.kind, 'warn');
  assert.deepEqual(canary.baseline.change, { tag: 'canary', from: 'rel-b', to: 'rel-d' });
  // OK again on canary moves it from where the dialog showed it, and prod's
  // reconfirmation still holds.
  assert.deepEqual(retagStep(canary.baseline, now, 'canary'), { kind: 'move', expected: 'rel-d' });
  assert.deepEqual(retagStep(canary.baseline, now, 'prod'), { kind: 'move', expected: 'rel-c' });
});

test('OK reads the releases again first, so a tag that moved since the page last refreshed warns instead of sending', async () => {
  // The page's list still shows prod on A (read under 10 s ago); the fresh
  // read says it moved to C.
  let reads = 0;
  const fresh = (releases) => async () => { reads += 1; return releases; };
  const moved = await retagStepNow(opened({ prod: 'rel-a' }), fresh([
    { id: 'rel-a', template_name: 'seed', tags: [] },
    { id: 'rel-c', template_name: 'seed', tags: ['prod'] },
    { id: 'rel-x', template_name: 'other', tags: ['prod'] },
  ]), 'seed', 'prod');
  assert.equal(moved.kind, 'warn');
  assert.deepEqual(moved.baseline.change, { tag: 'prod', from: 'rel-a', to: 'rel-c' });
  // Unmoved: sent with the holder the dialog opened with.
  assert.deepEqual(
    await retagStepNow(opened({ prod: 'rel-a' }), fresh([{ id: 'rel-a', template_name: 'seed', tags: ['prod'] }]), 'seed', 'prod'),
    { kind: 'move', expected: 'rel-a' },
  );
  assert.equal(reads, 2);
  // A failed read sends nothing: the caller gets the failure.
  const failure = new Error('failed to refresh template releases');
  await assert.rejects(retagStepNow(opened({ prod: 'rel-a' }), async () => { throw failure; }, 'seed', 'prod'), (thrown) => thrown === failure);
});

test('a reconfirmed tag moves only from the holder the dialog showed; a further move warns again', () => {
  const warned = retagStep(opened({ prod: 'rel-a' }), holdersOf({ prod: 'rel-c' }), 'prod');
  assert.deepEqual(retagStep(warned.baseline, holdersOf({ prod: 'rel-c' }), 'prod'), { kind: 'move', expected: 'rel-c' });
  const again = retagStep(warned.baseline, holdersOf({ prod: 'rel-e' }), 'prod');
  assert.equal(again.kind, 'warn');
  assert.deepEqual(again.baseline.change, { tag: 'prod', from: 'rel-c', to: 'rel-e' });
  // A tag deleted meanwhile is moved only while it still does not exist.
  const deleted = retagStep(opened({ prod: 'rel-a' }), new Map(), 'prod');
  assert.deepEqual(deleted.baseline.holders, new Map());
  assert.deepEqual(retagStep(deleted.baseline, new Map(), 'prod'), { kind: 'move', expected: null });
  // A tag that did not move is sent with the holder the dialog opened with.
  assert.deepEqual(retagStep(opened({ prod: 'rel-a' }), holdersOf({ prod: 'rel-a' }), 'prod'), { kind: 'move', expected: 'rel-a' });
});

test('a second OK while the tag move is in flight sends nothing', async () => {
  const flight = singleFlight();
  const sent = [];
  let finish;
  const move = () => flight.run(async () => {
    sent.push('prod');
    await new Promise((resolve) => { finish = resolve; });
    return 'moved';
  });
  const first = move();
  const second = move();
  assert.equal(flight.busy(), true);
  assert.deepEqual(await second, { ran: false });
  finish();
  assert.deepEqual(await first, { ran: true, value: 'moved' });
  assert.deepEqual(sent, ['prod']);
  // Once it answered, even with a failure, OK sends again.
  await assert.rejects(flight.run(async () => { throw new Error('synthetic 500'); }), /synthetic 500/);
  assert.equal(flight.busy(), false);
  assert.deepEqual(await flight.run(async () => 'again'), { ran: true, value: 'again' });
});

test("a template row's builds are read while it is expanded and on the page's refreshes, never while collapsed", () => {
  const view = (open, reloadKey, templateName = 'seed') => ({ templateName, open, reloadKey });
  assert.equal(buildsRead(null, view(true, 0)), 'load');
  assert.equal(buildsRead(view(true, 0), view(true, 0)), null);
  assert.equal(buildsRead(view(true, 0), view(true, 1)), 'background');
  // Collapsed, the table keeps the panel mounted: the page's refreshes read nothing.
  assert.equal(buildsRead(view(true, 1), view(false, 1)), null);
  assert.equal(buildsRead(view(false, 1), view(false, 2)), null);
  assert.equal(buildsRead(view(false, 2), view(false, 3)), null);
  // Expanded again, it reads once, keeping the rows it showed.
  assert.equal(buildsRead(view(false, 3), view(true, 3)), 'background');
  assert.equal(buildsRead(view(true, 3), view(true, 3, 'code-runtime')), 'load');
});
