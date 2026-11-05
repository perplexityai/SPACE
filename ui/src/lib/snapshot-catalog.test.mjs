import assert from 'node:assert/strict';
import { test } from 'node:test';
import { catalogEmptyText, nodeEmptyText, nodeInventory, nodeListing } from './snapshot-catalog.ts';

const UNAVAILABLE = "The node's snapshot list is unavailable, so whether the node keeps snapshots for this sandbox is unknown.";
const PERIODIC_OFF = 'The node does not list its snapshots while periodic snapshots are off. Turning them off hides every snapshot the node keeps for this sandbox, earlier periodic snapshots and checkpoints alike.';
const KEEPS_NONE = 'The node keeps no snapshots for this sandbox';
const row = { snapshot_id: 'f-manual-1', type: 'full', state: 'ready' };

// What the debug view's snapshots section can be, with the sandbox's
// observed periodic state, and what each list then says when it is empty.
test('each list says only what the node inventory shows', () => {
  for (const [name, section, periodic, node, catalog] of [
    ['available and empty', { snapshots: [], inventory: [] }, true, KEEPS_NONE, 'No backup records'],
    ['rows present', { snapshots: [], inventory: [row, row] }, true, null, 'No backup records. The node keeps 2 snapshots, listed under On the node below.'],
    ['one row', { snapshots: [], inventory: [row] }, true, null, 'No backup records. The node keeps 1 snapshot, listed under On the node below.'],
    // A row the node lists is evidence even when the observed state says off
    // or is not reported.
    ['rows present, periodic off', { snapshots: [], inventory: [row] }, false, null, 'No backup records. The node keeps 1 snapshot, listed under On the node below.'],
    ['rows present, periodic unknown', { snapshots: [], inventory: [row] }, null, null, 'No backup records. The node keeps 1 snapshot, listed under On the node below.'],
    ['periodic off', { snapshots: [], inventory: [] }, false, PERIODIC_OFF, 'No backup records'],
    ['periodic unknown', { snapshots: [], inventory: [] }, null, UNAVAILABLE, 'No backup records'],
    // A release whose snapshots section carries no inventory.
    ['an old API', { snapshots: [] }, true, UNAVAILABLE, 'No backup records'],
    ['an old API, periodic off', { snapshots: [] }, false, UNAVAILABLE, 'No backup records'],
    ['not loaded or failed', null, true, UNAVAILABLE, 'No backup records'],
  ]) {
    const inventory = nodeInventory(section);
    const listing = nodeListing(inventory ? inventory.length : null, periodic);
    if (node !== null) assert.equal(nodeEmptyText(false, listing), node, name);
    assert.equal(catalogEmptyText(false, listing), catalog, name);
  }
});

test('a failed read says so, and the sandbox list points nowhere', () => {
  const listed = nodeListing(2, true);
  assert.equal(nodeEmptyText(true, listed), "Could not load the node's snapshots");
  assert.equal(catalogEmptyText(true, listed), 'Could not load the snapshot catalog');
  assert.equal(catalogEmptyText(false, null), 'No backup records');
  assert.equal(catalogEmptyText(true, null), 'Could not load the snapshot catalog');
});
