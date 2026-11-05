import assert from 'node:assert/strict';
import { test } from 'node:test';
import { catalogEmptyText, nodeEmptyText } from './snapshot-catalog.ts';

test('an empty catalog points at the snapshots the node keeps', () => {
  for (const [failed, nodeKeeps, want] of [
    [false, 2, 'No stored snapshots. The node keeps 2 snapshots, listed under On the node below.'],
    [false, 1, 'No stored snapshots. The node keeps 1 snapshot, listed under On the node below.'],
    // Nothing to point at: the node list is empty, or has not loaded.
    [false, 0, 'No stored snapshots'],
    [false, null, 'No stored snapshots'],
    [true, 2, 'Could not load snapshots'],
  ]) {
    assert.equal(catalogEmptyText(failed, nodeKeeps), want, `failed=${failed} nodeKeeps=${nodeKeeps}`);
  }
});

test('an empty node list says the node lists nothing while periodic snapshots are off', () => {
  assert.match(nodeEmptyText(false, false), /only while periodic snapshots are on/);
  // On, or an API that does not report it: the node publishes its list, so
  // an empty one means it keeps none.
  for (const periodic of [true, null]) {
    assert.equal(nodeEmptyText(false, periodic), 'The node keeps no snapshots for this sandbox');
  }
  assert.equal(nodeEmptyText(true, false), "Could not load the node's snapshots");
});
