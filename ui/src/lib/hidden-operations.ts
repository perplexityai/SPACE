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
// the operator asks to try again (`forgetRollbackRefusal`).
type RefusalStorage = Pick<Storage, 'getItem' | 'setItem'>;

const ROLLBACK_REFUSALS_KEY = 'space:rollback-refusals:v1';
const memoryRefusals = new Map<string, string>();
const memoryStorage: RefusalStorage = {
  getItem: () => (memoryRefusals.size ? JSON.stringify(Object.fromEntries(memoryRefusals)) : null),
  setItem: (_key, value) => {
    memoryRefusals.clear();
    for (const [org, reason] of Object.entries(JSON.parse(value) as Record<string, string>)) memoryRefusals.set(org, reason);
  },
};

function sessionRefusalStorage(): RefusalStorage {
  try {
    if (typeof window !== 'undefined' && window.sessionStorage) return window.sessionStorage;
  } catch {
    // Storage disabled: keep refusals in memory.
  }
  return memoryStorage;
}

function readRefusals(storage: RefusalStorage): Record<string, string> {
  try {
    const value: unknown = JSON.parse(storage.getItem(ROLLBACK_REFUSALS_KEY) ?? '{}');
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return Object.fromEntries(Object.entries(value).filter(([, reason]) => typeof reason === 'string')) as Record<string, string>;
  } catch {
    return {};
  }
}

function writeRefusals(storage: RefusalStorage, refusals: Record<string, string>) {
  try {
    storage.setItem(ROLLBACK_REFUSALS_KEY, JSON.stringify(refusals));
  } catch {
    memoryStorage.setItem(ROLLBACK_REFUSALS_KEY, JSON.stringify(refusals));
  }
}

export function rememberedRollbackRefusal(orgId: string | null, storage = sessionRefusalStorage()): string | null {
  if (!orgId) return null;
  return readRefusals(storage)[orgId] ?? null;
}

export function rememberRollbackRefusal(orgId: string | null, reason: string, storage = sessionRefusalStorage()) {
  if (!orgId) return;
  writeRefusals(storage, { ...readRefusals(storage), [orgId]: reason });
}

export function forgetRollbackRefusal(orgId: string | null, storage = sessionRefusalStorage()) {
  if (!orgId) return;
  const { [orgId]: _forgotten, ...rest } = readRefusals(storage);
  writeRefusals(storage, rest);
}
