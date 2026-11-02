export interface BrowserLocks {
  request<T>(name: string, callback: () => T | Promise<T>): Promise<T>;
}

export interface ExclusiveEnv {
  locks?: BrowserLocks;
  acquire(name: string): string | null;
  release(name: string, owner: string): void;
  wait(ms: number): Promise<void>;
}

const FALLBACK_LOCK_RETRY_MS = 25;

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
      owner = env.acquire(name);
    } catch {
      // Storage refuses writes (full or blocked), so the lock cannot be
      // taken and retrying cannot help. `work` goes ahead unlocked; its own
      // write fails the same way and reports it.
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
