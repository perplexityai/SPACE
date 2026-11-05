// What the Snapshots card's two lists hold, and what each says when it is
// empty.
//
// The snapshot catalog (the public and admin snapshot lists, and the debug
// view's `snapshots.snapshots`) holds the control plane's backup records. A
// record is written when a node begins uploading a snapshot to object
// storage, or when a fork copies one, and it stays while the upload is
// `creating` and after it has `failed`: a record is not proof that the
// snapshot is stored. A checkpoint is published on the sandbox's node alone,
// so the catalog does not list it.
//
// The node's own list ("On the node": the debug view's `snapshots.inventory`)
// holds checkpoints and periodic snapshots, but node-ctl publishes it only for
// a sandbox the node runs with periodic snapshots on. With them off, the list
// is empty whatever the node keeps.

export const CATALOG_TITLE = 'Snapshot catalog';

export const CATALOG_NOTE =
  'Backup records, one for each snapshot upload to object storage that has begun. Entries include pending (creating) and failed uploads. A checkpoint stays on the node and is not listed here.';

export const NODE_NOTE =
  "Checkpoints and periodic snapshots the sandbox's node keeps, which it lists only while periodic snapshots are on. Rollback restores one of them.";

// What the On the node list can say about the snapshots the node keeps.
export type NodeListing =
  // Not known: the node's inventory has not loaded, could not, or is absent
  // from the API's answer, or it is empty while the API does not say whether
  // periodic snapshots are on.
  | { state: 'unavailable' }
  // Empty because the node lists nothing while periodic snapshots are off.
  | { state: 'periodic-off' }
  | { state: 'listed'; count: number };

// The node's inventory from the debug view's snapshots section, or null when
// it is not known: the section has not loaded or failed (`section` null), or
// comes from a release that does not report `inventory`.
export function nodeInventory<T>(section: { inventory?: T[] | null } | null | undefined): T[] | null {
  return section && Array.isArray(section.inventory) ? section.inventory : null;
}

// `shown` is how many snapshots the On the node list shows, or null when the
// inventory is not known (`nodeInventory`). `periodic` is whether the node
// runs the sandbox with periodic snapshots on (the sandbox's observed
// `snapshots_enabled`), or null when the API does not say. A listed snapshot
// is evidence on its own; an empty list is only with periodic snapshots on.
export function nodeListing(shown: number | null, periodic: boolean | null): NodeListing {
  if (shown === null) return { state: 'unavailable' };
  if (shown > 0) return { state: 'listed', count: shown };
  if (periodic === false) return { state: 'periodic-off' };
  if (periodic === null) return { state: 'unavailable' };
  return { state: 'listed', count: 0 };
}

// `listing` is null where the node's list is not shown (the sandbox list's
// Snapshots panel).
export function catalogEmptyText(failed: boolean, listing: NodeListing | null): string {
  if (failed) return 'Could not load the snapshot catalog';
  if (listing?.state !== 'listed' || listing.count === 0) return 'No backup records';
  const keeps = listing.count === 1 ? '1 snapshot' : `${listing.count} snapshots`;
  return `No backup records. The node keeps ${keeps}, listed under On the node below.`;
}

export function nodeEmptyText(failed: boolean, listing: NodeListing): string {
  if (failed) return "Could not load the node's snapshots";
  switch (listing.state) {
    case 'unavailable':
      return "The node's snapshot list is unavailable, so whether the node keeps snapshots for this sandbox is unknown.";
    case 'periodic-off':
      return 'The node does not list its snapshots while periodic snapshots are off. Turning them off hides every snapshot the node keeps for this sandbox, earlier periodic snapshots and checkpoints alike.';
    case 'listed':
      return 'The node keeps no snapshots for this sandbox';
  }
}
