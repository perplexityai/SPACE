// What the Snapshots card's two lists hold, and what each says when it is
// empty.
//
// The catalog list ("Stored": the public and admin snapshot lists, and the
// debug view's `snapshots.snapshots`) has a row only for a snapshot a node
// uploads to object storage. The row is written when the upload begins, which
// a stop, a suspend, a node drain or a fork starts. A checkpoint is published
// on the sandbox's node alone, so the catalog does not list it.
//
// The node's own list ("On the node": the debug view's `snapshots.inventory`)
// holds checkpoints and periodic snapshots, but node-ctl publishes it only for
// a sandbox the node runs with periodic snapshots on. With them off, the node
// still keeps its checkpoints and the list is empty.

export const CATALOG_NOTE =
  'Snapshots stored in object storage, such as the ones a stop or suspend uploads. A checkpoint is kept on the node and listed under On the node, not here, unless it is stored.';

export const NODE_NOTE =
  "Checkpoints and periodic snapshots the sandbox's node keeps. Rollback restores one of them.";

// `nodeKeeps` is how many snapshots the On the node list shows, or null while
// the node's inventory has not loaded or could not.
export function catalogEmptyText(failed: boolean, nodeKeeps: number | null): string {
  if (failed) return 'Could not load snapshots';
  if (!nodeKeeps) return 'No stored snapshots';
  return `No stored snapshots. The node keeps ${nodeKeeps === 1 ? '1 snapshot' : `${nodeKeeps} snapshots`}, listed under On the node below.`;
}

// `periodic` is whether the node runs the sandbox with periodic snapshots on
// (the sandbox's observed `snapshots_enabled`), or null when the API does not
// say.
export function nodeEmptyText(failed: boolean, periodic: boolean | null): string {
  if (failed) return "Could not load the node's snapshots";
  if (periodic === false) {
    return "The node lists its snapshots, checkpoints included, only while periodic snapshots are on for this sandbox. It keeps any checkpoint taken while they are off without listing it.";
  }
  return 'The node keeps no snapshots for this sandbox';
}
