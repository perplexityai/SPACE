export interface BrowserLocks {
  request<T>(name: string, callback: () => T | Promise<T>): Promise<T>;
}

export interface ExclusiveEnv {
  locks?: BrowserLocks;
  acquire(name: string, ttlMs: number): string | null;
  release(name: string, owner: string): void;
  wait(ms: number): Promise<void>;
}

// The fallback lock is held only while `work` runs, which is synchronous, so a
// short TTL is enough; it frees the lock if a tab dies holding it.
export const FALLBACK_LOCK_TTL_MS = 2_000;
const FALLBACK_LOCK_RETRY_MS = 25;

// Whether storage refused the lock record: blocked (`SecurityError`) or full
// (`QuotaExceededError`).
function storageRefused(error: unknown): boolean {
  const name = typeof error === 'object' && error !== null ? (error as { name?: unknown }).name : undefined;
  return name === 'QuotaExceededError' || name === 'SecurityError';
}

// Runs `work` while holding a lock shared by this browser's tabs, so two
// tabs' read-modify-writes of the stored run do not interleave. The Web Locks
// API needs a secure context (https, or localhost); elsewhere a short-lived
// lock in localStorage stands in. Its read-then-write is not atomic across
// tabs, so it narrows the window rather than closing it.
export async function exclusiveWith<T>(env: ExclusiveEnv, name: string, work: () => T): Promise<T> {
  if (env.locks) return env.locks.request(name, () => work());
  for (;;) {
    let owner: string | null;
    try {
      owner = env.acquire(name, FALLBACK_LOCK_TTL_MS);
    } catch (error) {
      if (!storageRefused(error)) throw error;
      // Retrying cannot store the lock, so `work` goes ahead unlocked.
      // Blocked storage refuses `work`'s own writes too, and they report it.
      // Full storage still accepts removals and writes that do not grow the
      // stored value, so those land without the lock, where another tab's
      // write in the same moment can overwrite them.
      return work();
    }
    if (owner) {
      try {
        return work();
      } finally {
        env.release(name, owner);
      }
    }
    await env.wait(FALLBACK_LOCK_RETRY_MS);
  }
}
