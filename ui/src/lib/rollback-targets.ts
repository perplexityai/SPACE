// What the console offers as a rollback target. The rollback routes pass
// `snapshot_name` to the sandbox's node unchanged, and the node rolls back
// only to a snapshot directory it keeps for that sandbox
// (`{cache}/{id}/snapshots/{name}`), not to a catalog snapshot ID
// (`snap_<uuid>`). The names the node keeps reach the console only through
// the debug view's `snapshots.inventory`, which the node publishes
// (`snapshot_id` is the directory name), so that inventory is the only list
// rollback is offered on.
import type { DebugSnapshotInventory } from './sandbox-debug';

// Directory names sbx-mgr keeps for itself and refuses to list or roll back
// to (`is_internal_snapshot_name` in
// `crates/l40_api/sbx/mgr/server/src/service/grpc_spaced_process_snapshots.rs`):
// its current-instance and per-operation disk artifacts, and GC staging,
// `.gc-pending-<uuid>` as the whole name or its end. The node already leaves
// them out of its inventory; an older node might not.
const INTERNAL_SNAPSHOT_PREFIXES = ['d-stop-', 'd-operation-'];
const GC_PENDING_MARKER = '.gc-pending-';
// The forms Rust's `Uuid::parse_str` reads.
const UUID_FORMS = /^(?:[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\}|urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i;

export function isInternalSnapshotName(name: string): boolean {
  if (INTERNAL_SNAPSHOT_PREFIXES.some((prefix) => name.startsWith(prefix))) return true;
  const marker = name.lastIndexOf(GC_PENDING_MARKER);
  return marker >= 0 && UUID_FORMS.test(name.slice(marker + GC_PENDING_MARKER.length));
}

// The node reports a snapshot `ready`; `unknown` is a full snapshot with its
// files but no catalog publication, which the node still rolls back to (older
// local snapshots predate the catalog). Anything else (`incomplete`, or a
// publication that is not ready) is refused by the node.
const ROLLBACK_STATES = new Set(['ready', 'unknown']);

export const ROLLBACK_SUSPENDED =
  'A suspended sandbox cannot be rolled back: its suspend snapshot describes the current disk. Restore it first.';

// A full (`f-*`) snapshot also restores memory, which the node refuses
// unless it runs with file-backed memory (no hugepages).
export const FULL_SNAPSHOT_RESTORE_NOTE =
  'A full snapshot also restores memory, so its node must support snapshot restore; a node that does not refuses it.';

export function isFullSnapshotName(name: string): boolean {
  return name.startsWith('f-');
}

export interface RollbackTarget {
  // Sent as the rollback's `snapshot_name`.
  snapshotName: string;
  row: DebugSnapshotInventory;
  // Why Rollback is disabled on this row, or null when it is offered.
  unavailable: string | null;
}

// Node inventory timestamps are compact (`20260331T140000Z`) or RFC 3339.
export function snapshotTimeMs(value: string | null | undefined): number | null {
  if (!value) return null;
  const compact = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(value);
  const ms = Date.parse(compact ? `${compact[1]}-${compact[2]}-${compact[3]}T${compact[4]}:${compact[5]}:${compact[6]}Z` : value);
  return Number.isFinite(ms) ? ms : null;
}

// The rows of the node's inventory the console shows, newest first, each
// with whether Rollback is offered. `sandboxStatus` is the sandbox's status
// now: the node refuses to roll back a suspended sandbox.
export function rollbackTargets(
  inventory: DebugSnapshotInventory[],
  sandboxStatus: string | null | undefined,
): RollbackTarget[] {
  return inventory
    .filter((row) => !isInternalSnapshotName(row.snapshot_id))
    .map((row) => ({
      snapshotName: row.snapshot_id,
      row,
      unavailable:
        sandboxStatus === 'suspended'
          ? ROLLBACK_SUSPENDED
          : ROLLBACK_STATES.has(row.state)
            ? null
            : `The node reports this snapshot ${row.state || 'in no state'}, and rolls back only to a ready one.`,
    }))
    .sort((a, b) =>
      (snapshotTimeMs(b.row.timestamp) ?? -Infinity) - (snapshotTimeMs(a.row.timestamp) ?? -Infinity)
      || b.snapshotName.localeCompare(a.snapshotName));
}

// The route a sandbox's rollback goes to. The operator's own organization
// uses the public `:rollback`. Another organization's uses the admin
// `:force-rollback`, but only once the admin snapshot list answered for the
// sandbox: a release without that list also has an older `:force-rollback`
// that accepts the request and drops the snapshot name, so its operation
// fails. `adminList` is what the admin list last answered: `listed`, `none`
// (the release has no such list), or `unknown` (not answered yet, or failed).
export type RollbackRoute = { via: 'public' | 'admin'; reason: null } | { via: null; reason: string };

export const ROLLBACK_OTHER_ORG_UNSUPPORTED =
  "This API release rolls back only your organization's sandboxes.";
export const ROLLBACK_AWAITS_ADMIN_LIST =
  "Rollback is offered once this sandbox's snapshot list has loaded, which shows that the API release rolls back other organizations' sandboxes.";

export function rollbackRoute(ownOrg: boolean, adminList: 'listed' | 'none' | 'unknown'): RollbackRoute {
  if (ownOrg) return { via: 'public', reason: null };
  if (adminList === 'listed') return { via: 'admin', reason: null };
  return { via: null, reason: adminList === 'none' ? ROLLBACK_OTHER_ORG_UNSUPPORTED : ROLLBACK_AWAITS_ADMIN_LIST };
}

// The body both rollback routes take: the node's directory name, and a
// restart once the disk is rolled back.
export function rollbackBody(snapshotName: string): { snapshot_name: string; auto_restart: true } {
  return { snapshot_name: snapshotName, auto_restart: true };
}
