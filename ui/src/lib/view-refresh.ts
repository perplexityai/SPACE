// The interval only checks whether the view is stale, which is cheap; the
// staleness window is what sets how often a list is refetched. Every refetch
// of the sandbox, node or template list reads the whole admin list, so it
// runs every 10 s rather than every second. Pages refresh right after an
// operator's own action regardless.
export const VIEW_REFRESH_INTERVAL_MS = 1_000;
export const VIEW_REFRESH_STALE_MS = 10_000;
export const VIEW_REFRESH_LOCK_TTL_MS = 60_000;

const STORAGE_PREFIX = 'space:view-refresh:';
const OWNER = `${Date.now()}-${Math.random().toString(36).slice(2)}`;

interface LockRecord {
  owner: string;
  expiresAt: number;
}

interface BrowserLockManager {
  request<T>(
    name: string,
    options: { ifAvailable: true },
    callback: (lock: unknown | null) => T | Promise<T>,
  ): Promise<T>;
}

type NavigatorWithLocks = Navigator & { locks?: BrowserLockManager };

function hasStorage() {
  return typeof window !== 'undefined' && !!window.localStorage;
}

function readLock(storageKey: string): LockRecord | null {
  try {
    const raw = window.localStorage.getItem(storageKey);
    return raw ? JSON.parse(raw) as LockRecord : null;
  } catch {
    return null;
  }
}

export function shouldRefreshView(key: string, staleMs = VIEW_REFRESH_STALE_MS, now = Date.now()) {
  if (!hasStorage()) {
    return true;
  }
  const raw = window.localStorage.getItem(`${STORAGE_PREFIX}${key}:last`);
  const last = raw ? Number(raw) : 0;
  return !Number.isFinite(last) || last <= 0 || now - last >= staleMs;
}

export function markViewRefreshed(key: string, now = Date.now()) {
  if (!hasStorage()) {
    return;
  }
  window.localStorage.setItem(`${STORAGE_PREFIX}${key}:last`, String(now));
}

export function tryAcquireViewRefreshLock(key: string, ttlMs = VIEW_REFRESH_LOCK_TTL_MS, now = Date.now()) {
  const owner = `${OWNER}:${now}:${Math.random().toString(36).slice(2)}`;
  if (!hasStorage()) {
    return owner;
  }

  const storageKey = `${STORAGE_PREFIX}${key}:lock`;
  const current = readLock(storageKey);
  if (current && current.expiresAt > now) {
    return null;
  }

  const next: LockRecord = { owner, expiresAt: now + ttlMs };
  window.localStorage.setItem(storageKey, JSON.stringify(next));
  return readLock(storageKey)?.owner === owner ? owner : null;
}

export function releaseViewRefreshLock(key: string, owner: string) {
  if (!hasStorage()) {
    return;
  }
  const storageKey = `${STORAGE_PREFIX}${key}:lock`;
  const current = readLock(storageKey);
  if (current?.owner === owner) {
    window.localStorage.removeItem(storageKey);
  }
}

function renewViewRefreshLock(key: string, owner: string, ttlMs: number, now = Date.now()) {
  if (!hasStorage()) {
    return false;
  }
  const storageKey = `${STORAGE_PREFIX}${key}:lock`;
  const current = readLock(storageKey);
  if (current?.owner !== owner) {
    return false;
  }
  window.localStorage.setItem(storageKey, JSON.stringify({ owner, expiresAt: now + ttlMs }));
  return true;
}

function startViewRefreshLockHeartbeat(key: string, owner: string, ttlMs: number) {
  if (!hasStorage()) {
    return () => {};
  }
  const interval = window.setInterval(() => {
    if (!renewViewRefreshLock(key, owner, ttlMs)) {
      window.clearInterval(interval);
    }
  }, Math.max(1_000, Math.floor(ttlMs / 3)));
  return () => window.clearInterval(interval);
}

async function runWithBrowserLock(
  key: string,
  refresh: () => Promise<void>,
  force: boolean,
  staleMs: number,
): Promise<{ supported: true; ran: boolean } | { supported: false }> {
  if (typeof navigator === 'undefined') {
    return { supported: false };
  }
  const locks = (navigator as NavigatorWithLocks).locks;
  if (!locks) {
    return { supported: false };
  }

  const ran = await locks.request(
    `${STORAGE_PREFIX}${key}:lock`,
    { ifAvailable: true },
    async (lock) => {
      if (!lock) {
        return false;
      }
      if (!force && !shouldRefreshView(key, staleMs)) {
        return false;
      }
      await refresh();
      markViewRefreshed(key);
      return true;
    },
  );
  return { supported: true, ran };
}

export async function runLockedViewRefresh(
  key: string,
  refresh: () => Promise<void>,
  options: { force?: boolean; staleMs?: number; lockTtlMs?: number } = {},
) {
  const { force = false, staleMs = VIEW_REFRESH_STALE_MS, lockTtlMs = VIEW_REFRESH_LOCK_TTL_MS } = options;
  if (!force && !shouldRefreshView(key, staleMs)) {
    return false;
  }

  const browserLock = await runWithBrowserLock(key, refresh, force, staleMs);
  if (browserLock.supported) {
    return browserLock.ran;
  }

  const owner = tryAcquireViewRefreshLock(key, lockTtlMs);
  if (!owner) {
    return false;
  }
  const stopHeartbeat = startViewRefreshLockHeartbeat(key, owner, lockTtlMs);

  try {
    if (!force && !shouldRefreshView(key, staleMs)) {
      return false;
    }
    await refresh();
    markViewRefreshed(key);
    return true;
  } finally {
    stopHeartbeat();
    releaseViewRefreshLock(key, owner);
  }
}
