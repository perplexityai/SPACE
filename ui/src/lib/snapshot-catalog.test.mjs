import assert from 'node:assert/strict';
import { test } from 'node:test';
import { rollbackTargets } from './rollback-targets.ts';
import { catalogEmptyText, currentInventory, nodeEmptyText, nodeInventory, nodeListing, observePeriodic } from './snapshot-catalog.ts';

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

const section = { snapshots: [], inventory: [row, { ...row, snapshot_id: 'f-manual-2' }] };
const empty = { snapshots: [], inventory: [] };

// The sandbox row (periodic state) and the debug view (inventory) are read
// separately, so a debug read started before a periodic change may hold the
// node's report from before it. Not known, it shows no rows to roll back to.
test('a read from before a periodic change shows no rows and offers no Rollback', () => {
  const before = observePeriodic(null, false);
  const after = observePeriodic(before, true);
  assert.equal(after.generation, before.generation + 1);
  assert.equal(currentInventory(section, before.generation, after.generation), null);
  // An empty read from before the change is unknown, not empty.
  assert.equal(currentInventory(empty, before.generation, after.generation), null);
  // Before any debug read has landed.
  assert.equal(currentInventory(section, null, after.generation), null);
});

test('a read from the current generation shows its rows', () => {
  const before = observePeriodic(null, false);
  // First sight: nothing changed in this view, so the loaded read stands.
  assert.deepEqual(currentInventory(empty, before.generation, before.generation), []);
  const after = observePeriodic(before, true);
  const rows = currentInventory(section, after.generation, after.generation);
  assert.equal(rows, section.inventory);
  assert.deepEqual(rollbackTargets(rows, 'running').map((target) => target.snapshotName).sort(), ['f-manual-1', 'f-manual-2']);
  assert.deepEqual(nodeListing(rows.length, after.periodic), { state: 'listed', count: 2 });
  assert.deepEqual(currentInventory(empty, after.generation, after.generation), []);
  // Not known for the reasons `nodeInventory` gives, whatever the generation.
  assert.equal(currentInventory({ snapshots: [] }, after.generation, after.generation), null);
  assert.equal(currentInventory(null, after.generation, after.generation), null);
});

test('only a change of the periodic state bumps the generation', () => {
  const first = observePeriodic(null, true);
  assert.equal(first.generation, 0);
  assert.equal(observePeriodic(first, true), first);
  const off = observePeriodic(first, false);
  const on = observePeriodic(off, true);
  assert.equal(on.generation, 2);
  // Not reported counts as a state of its own.
  assert.equal(observePeriodic(on, null).generation, 3);
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
