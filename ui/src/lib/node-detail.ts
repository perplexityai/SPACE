// What a failed node read does to the node page. A 403 or 404 replaces the
// page: the operator may not see this node, or it is gone. Any other failure
// replaces it only before the first successful load; after one, the page
// keeps the last loaded node and says the read failed.
export function nodeReadFailure(status: number | null, loaded: boolean): 'replace' | 'stale' {
  if (status === 403 || status === 404) return 'replace';
  return loaded ? 'stale' : 'replace';
}

// The counts `include_occupancy=true` adds. They are a separate read from the
// node itself, so a failed count leaves the rest of the page in place.
export interface NodeCounts {
  sandboxes_active: number | null;
  sandboxes_local_snapshots: number | null;
}

// One count as the page shows it. `failed` means the latest count read
// failed; a value from an earlier read is then marked stale.
export function countLabel(value: number | null | undefined, failed: boolean): string {
  if (value === null || value === undefined) return failed ? 'could not count' : 'not computed';
  return failed ? `${value} (stale: the last count failed)` : String(value);
}
