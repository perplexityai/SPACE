// Snapshot listing, rollback and enabling snapshots are hidden native
// operations. Unless the deployment enables them for the signed-in user's
// organization (SPACE_NATIVE_HIDDEN_OPERATIONS_ORGS in docs/configuration.md),
// the API answers them exactly as a route or sandbox action that does not
// exist. The console reads that answer as "feature off", not as a failure.
export const SNAPSHOTS_NOT_ENABLED = 'Snapshots are not enabled for your organization.';
export const ROLLBACK_NOT_ENABLED = 'Snapshot rollback is not enabled for your organization.';

export function isHiddenOperationError(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false;
  const { status, message } = err as { status?: unknown; message?: unknown };
  return (
    (status === 404 && message === 'route not found') ||
    (status === 400 && message === 'invalid sandbox action')
  );
}

// An API release without the admin snapshot list serves snapshots only for
// the signed-in operator's own organization.
export const SNAPSHOTS_OTHER_ORG_UNSUPPORTED =
  "This API release lists snapshots only for your organization's sandboxes.";

// Why the API refused a rollback because rollback is not enabled for the
// sandbox's organization, or null for any other failure. The public route
// answers the gate as a hidden operation; the admin `:force-rollback` answers
// a 404 whose message says so, and no reason code.
export function rollbackGateRefusal(err: unknown, via: 'admin' | 'public'): string | null {
  if (via === 'public') return isHiddenOperationError(err) ? ROLLBACK_NOT_ENABLED : null;
  if (typeof err !== 'object' || err === null) return null;
  const { status, message } = err as { status?: unknown; message?: unknown };
  if (status !== 404 || typeof message !== 'string' || !message.includes('rollback is not enabled')) return null;
  return `Snapshot rollback is refused: ${message}.`;
}

// A gate refusal is remembered per organization for the browser session
// (the tab's `sessionStorage`, or memory where it is unavailable): Rollback
// is then disabled with the reason instead of offered to fail again, until
// the operator asks to try again (`forget`). Every panel showing a sandbox of
// that organization subscribes, so a refusal, or a "Try again", in one panel
// applies to all of them; a rollback checks `get` right before it is sent.
// What this tab remembers is held in memory and copied to storage, so it
// holds even when storage refuses the write.
type RefusalStorage = Pick<Storage, 'getItem' | 'setItem'>;

const ROLLBACK_REFUSALS_KEY = 'space:rollback-refusals:v1';

export interface RollbackRefusals {
  get(orgId: string | null): string | null;
  remember(orgId: string | null, reason: string): void;
  forget(orgId: string | null): void;
  // Calls `listener` whenever the refusal remembered for `orgId` changes.
  subscribe(orgId: string | null, listener: () => void): () => void;
}

function readRefusals(storage: RefusalStorage | null): Map<string, string> {
  try {
    const value: unknown = JSON.parse(storage?.getItem(ROLLBACK_REFUSALS_KEY) ?? '{}');
    if (!value || typeof value !== 'object' || Array.isArray(value)) return new Map();
    return new Map(Object.entries(value).filter((entry): entry is [string, string] => typeof entry[1] === 'string'));
  } catch {
    return new Map();
  }
}

export function createRollbackRefusals(storage: RefusalStorage | null): RollbackRefusals {
  const refusals = readRefusals(storage);
  const listeners = new Map<string, Set<() => void>>();
  const changed = (orgId: string) => {
    try {
      storage?.setItem(ROLLBACK_REFUSALS_KEY, JSON.stringify(Object.fromEntries(refusals)));
    } catch {
      // Storage refused the write: this tab keeps the refusal in memory.
    }
    for (const listener of [...(listeners.get(orgId) ?? [])]) listener();
  };
  return {
    get: (orgId) => (orgId ? refusals.get(orgId) ?? null : null),
    remember: (orgId, reason) => {
      if (!orgId || refusals.get(orgId) === reason) return;
      refusals.set(orgId, reason);
      changed(orgId);
    },
    forget: (orgId) => {
      if (!orgId || !refusals.delete(orgId)) return;
      changed(orgId);
    },
    subscribe: (orgId, listener) => {
      if (!orgId) return () => undefined;
      const forOrg = listeners.get(orgId) ?? new Set();
      listeners.set(orgId, forOrg);
      forOrg.add(listener);
      return () => {
        forOrg.delete(listener);
        if (!forOrg.size) listeners.delete(orgId);
      };
    },
  };
}

function sessionRefusalStorage(): RefusalStorage | null {
  try {
    if (typeof window !== 'undefined' && window.sessionStorage) return window.sessionStorage;
  } catch {
    // Storage disabled: keep refusals in memory.
  }
  return null;
}

let tabRefusals: RollbackRefusals | null = null;

// This tab's refusals, read from its session storage on first use.
export function rollbackRefusals(): RollbackRefusals {
  tabRefusals ??= createRollbackRefusals(sessionRefusalStorage());
  return tabRefusals;
}
