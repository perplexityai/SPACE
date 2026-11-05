import assert from 'node:assert/strict';
import { test } from 'node:test';
import { rollbackTargets } from './rollback-targets.ts';
import { catalogEmptyText, currentInventory, inventoryCurrent, nodeEmptyText, nodeInventory, nodeListing, seePeriodic } from './snapshot-catalog.ts';

const UNAVAILABLE = "The node's report is unavailable, so what it reports keeping for this sandbox is unknown.";
const PERIODIC_OFF = 'The node does not report its snapshots while periodic snapshots are off, so this list is empty whatever it keeps, checkpoints and earlier periodic snapshots included.';
const REPORTS_NONE = 'The node reports no snapshots for this sandbox';
const NONE = 'No snapshots recorded for upload';
const points = (n) => `${NONE}. The node reports ${n}, listed under On the node below.`;
const row = { snapshot_id: 'f-manual-1', type: 'full', state: 'ready' };

// What the debug view's snapshots section can be, with the sandbox's
// observed periodic state, and what each list then says when it is empty.
test('each list says only what the node inventory shows', () => {
  for (const [name, section, periodic, node, catalog] of [
    ['available and empty', { snapshots: [], inventory: [] }, true, REPORTS_NONE, NONE],
    ['rows present', { snapshots: [], inventory: [row, row] }, true, null, points('2 snapshots')],
    ['one row', { snapshots: [], inventory: [row] }, true, null, points('1 snapshot')],
    // A row the node lists is evidence even when the observed state says off
    // or is not reported.
    ['rows present, periodic off', { snapshots: [], inventory: [row] }, false, null, points('1 snapshot')],
    ['rows present, periodic unknown', { snapshots: [], inventory: [row] }, null, null, points('1 snapshot')],
    ['periodic off', { snapshots: [], inventory: [] }, false, PERIODIC_OFF, NONE],
    ['periodic unknown', { snapshots: [], inventory: [] }, null, UNAVAILABLE, NONE],
    // A release whose snapshots section carries no inventory.
    ['an old API', { snapshots: [] }, true, UNAVAILABLE, NONE],
    ['an old API, periodic off', { snapshots: [] }, false, UNAVAILABLE, NONE],
    ['not loaded or failed', null, true, UNAVAILABLE, NONE],
  ]) {
    const inventory = nodeInventory(section);
    const listing = nodeListing(inventory ? inventory.length : null, periodic);
    if (node !== null) assert.equal(nodeEmptyText(false, listing), node, name);
    assert.equal(catalogEmptyText(false, listing), catalog, name);
  }
});

// The sandbox row (periodic state) and the debug view (inventory) are read
// separately: an inventory read requested before a periodic change was seen
// is unknown until a read requested after it completes.
test('an inventory read before a periodic change is not read against it', () => {
  const shown = (requestedAt, seen, count, periodic) => nodeListing(inventoryCurrent(requestedAt, seen) ? count : null, periodic);
  let seen = seePeriodic(null, false, 100);
  // First sight: nothing changed in this view, so the loaded read stands.
  assert.equal(nodeEmptyText(false, shown(50, seen, 0, false)), PERIODIC_OFF);
  // Periodic snapshots turn on, seen at 200; the empty read from 50, and one
  // requested at 190 that completes later, are both older.
  seen = seePeriodic(seen, true, 200);
  for (const requestedAt of [50, 190, null]) {
    const listing = shown(requestedAt, seen, 0, true);
    assert.equal(nodeEmptyText(false, listing), UNAVAILABLE, `requested at ${requestedAt}`);
    assert.equal(catalogEmptyText(false, listing), NONE);
  }
  // Rows from before the change point nowhere either.
  assert.equal(catalogEmptyText(false, shown(50, seen, 2, true)), NONE);
  // A read requested once the change was seen is the node's report.
  assert.equal(nodeEmptyText(false, shown(200, seen, 0, true)), REPORTS_NONE);
  assert.equal(catalogEmptyText(false, shown(210, seen, 1, true)), points('1 snapshot'));
  // The same state seen again keeps the time of the change.
  assert.equal(seePeriodic(seen, true, 300), seen);
  // Turning them off again makes the read from 210 old.
  seen = seePeriodic(seen, false, 400);
  assert.equal(nodeEmptyText(false, shown(210, seen, 0, false)), UNAVAILABLE);
  assert.equal(nodeEmptyText(false, shown(410, seen, 0, false)), PERIODIC_OFF);
});

// The rows On the node shows, and offers Rollback on, come from the same
// check as what it says: a read older than the periodic change shows none.
test('a read before a periodic change shows no rows to roll back to', () => {
  const section = { snapshots: [], inventory: [row, { ...row, snapshot_id: 'f-manual-2' }] };
  const seen = seePeriodic(seePeriodic(null, false, 100), true, 200);
  for (const requestedAt of [50, 190, null]) {
    assert.equal(currentInventory(section, requestedAt, seen), null, `requested at ${requestedAt}`);
  }
  const shown = currentInventory(section, 200, seen);
  assert.equal(rollbackTargets(shown, 'running').length, 2);
  assert.equal(nodeListing(shown.length, true).count, 2);
  // Nothing changed in this view: the loaded read stands.
  assert.equal(currentInventory(section, 50, seePeriodic(null, true, 100)), section.inventory);
  // Not known for the reasons `nodeInventory` gives, whenever it was read.
  assert.equal(currentInventory({ snapshots: [] }, 200, seen), null);
  assert.equal(currentInventory(null, 200, seen), null);
});

// A render React abandons never commits the periodic state it saw, so a later
// committed render reads the inventory against the history it committed.
test('a periodic change seen only by an abandoned render does not age a read', () => {
  const committed = seePeriodic(null, false, 100);
  // A render sees periodic snapshots on at 200 and is abandoned; the next
  // committed render still shows them off, from the read at 100.
  const abandoned = seePeriodic(committed, true, 200);
  const next = seePeriodic(committed, false, 100);
  assert.equal(next, committed);
  assert.equal(nodeEmptyText(false, nodeListing(currentInventory({ inventory: [] }, 50, next)?.length ?? null, false)), PERIODIC_OFF);
  // Had the abandoned render's history been kept, that read would be aged by
  // a change no committed render showed.
  assert.equal(currentInventory({ inventory: [] }, 50, seePeriodic(abandoned, false, 100)), null);
  // When the change commits, reads before it are old, as for any change.
  const changed = seePeriodic(committed, true, 200);
  for (const requestedAt of [50, 190]) assert.equal(currentInventory({ inventory: [row] }, requestedAt, changed), null);
  assert.deepEqual(currentInventory({ inventory: [row] }, 200, changed), [row]);
});

test('a failed read says so, and the sandbox list points nowhere', () => {
  const listed = nodeListing(2, true);
  assert.equal(nodeEmptyText(true, listed), "Could not load the node's snapshots");
  assert.equal(catalogEmptyText(true, listed), 'Could not load the snapshot catalog');
  // The fallback's rows are every record the debug view reports.
  assert.equal(catalogEmptyText(false, listed, 'records'), 'No snapshot records. The node reports 2 snapshots, listed under On the node below.');
  assert.equal(catalogEmptyText(true, listed, 'records'), "Could not load the control plane's snapshot records");
  assert.equal(catalogEmptyText(false, null), NONE);
  assert.equal(catalogEmptyText(true, null), 'Could not load the snapshot catalog');
});
