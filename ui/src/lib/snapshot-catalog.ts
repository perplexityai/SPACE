// What the Snapshots card's two lists hold, and what each says when it is
// empty. Each is described by what it shows, not by what puts rows in it.
//
// The catalog: the public and admin snapshot lists, which show the
// sandbox's snapshots recorded for upload to object storage and leave out
// `deleting` rows and fork copies. A record is listed while its upload is
// pending and after it has failed, so a listed snapshot is not necessarily
// stored. The card's fallback lists the debug view's `snapshots.snapshots`
// instead: every record, whatever its state or origin. A checkpoint is
// neither.
//
// The node's list ("On the node": the debug view's `snapshots.inventory`) is
// what the node reports keeping for the sandbox, and node-ctl reports it only
// while periodic snapshots are on. With them off the list is empty whatever
// the node keeps; even with them on, the node keeps snapshots it does not
// report.

export const CATALOG_TITLE = 'Snapshot catalog';

export const CATALOG_NOTE =
  'Snapshots recorded for upload to object storage, including pending and failed uploads. A checkpoint stays on the node and is not listed here.';

// The fallback's rows are the debug view's, which the lists above filter.
export const RECORDS_NOTE =
  'Every snapshot record the control plane keeps for this sandbox, whatever its state or origin, including pending, failed and deleting uploads. A checkpoint stays on the node and is not listed here.';

export const NODE_NOTE =
  'What the node reports keeping for this sandbox, which it reports only while periodic snapshots are on. Rollback restores one of them.';

// What the On the node list can say.
export type NodeListing =
  // Not known: the node's inventory has not loaded, could not, is absent
  // from the API's answer, or was read in an older generation than the
  // periodic state shown (`currentInventory`), or it is empty while the API
  // does not say whether periodic snapshots are on.
  | { state: 'unavailable' }
  // Empty because the node reports nothing while periodic snapshots are off.
  | { state: 'periodic-off' }
  | { state: 'listed'; count: number };

// The node's inventory from the debug view's snapshots section, or null when
// it is not known: the section has not loaded or failed (`section` null), or
// comes from a release that does not report `inventory`.
export function nodeInventory<T>(section: { inventory?: T[] | null } | null | undefined): T[] | null {
  return section && Array.isArray(section.inventory) ? section.inventory : null;
}

// The periodic state the console last observed, and the inventory's
// generation: bumped each time that state changes. The sandbox row (periodic
// state) and the debug view (inventory) are separate reads, so each debug read
// is tagged with the generation it started in.
export interface InventoryGeneration {
  periodic: boolean | null;
  generation: number;
}

export function observePeriodic(prev: InventoryGeneration | null, periodic: boolean | null): InventoryGeneration {
  if (prev === null) return { periodic, generation: 0 };
  return prev.periodic === periodic ? prev : { periodic, generation: prev.generation + 1 };
}

// The inventory rows On the node shows, and offers Rollback on, or null when
// they are not known: the inventory is not (`nodeInventory`), or the read
// started in an older generation than `generation`, so it may show the
// node's report from before the periodic change.
export function currentInventory<T>(
  section: { inventory?: T[] | null } | null | undefined,
  readGeneration: number | null,
  generation: number,
): T[] | null {
  return readGeneration === generation ? nodeInventory(section) : null;
}

// `shown` is how many snapshots the On the node list shows, or null when the
// inventory is not known (`currentInventory`). `periodic` is
// whether the node runs the sandbox with periodic snapshots on (its observed
// `snapshots_enabled`), or null when the API does not say. A listed
// snapshot is evidence on its own; an empty list is only with periodic
// snapshots on.
export function nodeListing(shown: number | null, periodic: boolean | null): NodeListing {
  if (shown === null) return { state: 'unavailable' };
  if (shown > 0) return { state: 'listed', count: shown };
  if (periodic === false) return { state: 'periodic-off' };
  if (periodic === null) return { state: 'unavailable' };
  return { state: 'listed', count: 0 };
}

// `listing` is null where the node's list is not shown (the sandbox list's
// Snapshots panel). `rows` is which rows the table shows: a snapshot list's,
// or the fallback's debug records (`RECORDS_NOTE`).
export function catalogEmptyText(failed: boolean, listing: NodeListing | null, rows: 'catalog' | 'records' = 'catalog'): string {
  if (failed) return rows === 'catalog' ? 'Could not load the snapshot catalog' : "Could not load the control plane's snapshot records";
  const empty = rows === 'catalog' ? 'No snapshots recorded for upload' : 'No snapshot records';
  if (listing?.state !== 'listed' || listing.count === 0) return empty;
  const reports = listing.count === 1 ? '1 snapshot' : `${listing.count} snapshots`;
  return `${empty}. The node reports ${reports}, listed under On the node below.`;
}

export function nodeEmptyText(failed: boolean, listing: NodeListing): string {
  if (failed) return "Could not load the node's snapshots";
  switch (listing.state) {
    case 'unavailable':
      return "The node's report is unavailable, so what it reports keeping for this sandbox is unknown.";
    case 'periodic-off':
      return 'The node does not report its snapshots while periodic snapshots are off, so this list is empty whatever it keeps, checkpoints and earlier periodic snapshots included.';
    case 'listed':
      return 'The node reports no snapshots for this sandbox';
  }
}
