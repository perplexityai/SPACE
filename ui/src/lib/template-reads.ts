// How the Templates pages read: one slot per kind of read (the template list,
// the releases, the node inventory, ...), so answers apply in order and a
// read that stalls cannot hold up the next.
//
// A periodic refresh joins the read of its kind already out rather than
// sending another: a plain "only the newest request writes" rule would let
// every tick supersede the last whenever a read takes longer than the refresh
// interval, and nothing would ever be applied. Only a read that is truly
// newer supersedes one out: an explicit read after a change (a delete, a tag
// move, the operator's Refresh), or the page opening another template.

// A whole-list read answers in well under a second at a few hundred
// templates. 20 s leaves room for a loaded API, frees a stalled read's slot
// long before the view's 60 s refresh lock would expire, and shows the
// operator the failure with Retry instead of loading indefinitely.
export const READ_TIMEOUT_MS = 20_000;

export class ReadTimeoutError extends Error {
  constructor(ms: number) {
    super(`no answer within ${ms / 1000} s`);
    this.name = 'ReadTimeoutError';
  }
}

// Why a reset's dropped read failed when no read followed it: the page that
// asked has closed, so there is nothing to report.
export class ReadDroppedError extends Error {
  constructor() {
    super('the read was dropped');
    this.name = 'ReadDroppedError';
  }
}

// What a read writes, called only while it is the slot's current read.
export interface ReadWrite<T> {
  value(value: T): void;
  error(err: unknown): void;
}

type Read<T> = (signal: AbortSignal) => Promise<T>;

export interface LatestRead<T> {
  // A periodic or automatic read: joins the pending read, or starts one.
  poll(read: Read<T>, write: ReadWrite<T>): Promise<T>;
  // An explicit read: cancels the pending read, whose answer then never
  // writes, and starts a new one.
  next(read: Read<T>, write: ReadWrite<T>): Promise<T>;
  // Drops the pending read without starting another. Its callers take the
  // answer of a read started in the same turn (the page now shows another
  // template); with none (the page closed), they fail with `ReadDroppedError`.
  reset(): void;
}

function withTimeout<T>(work: Promise<T>, ms: number, controller: AbortController): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const err = new ReadTimeoutError(ms);
      controller.abort(err);
      reject(err);
    }, ms);
    // A superseded read is cancelled: its timer goes with it, and it settles
    // now even if its request ignores the signal, so its caller is not left
    // waiting.
    controller.signal.addEventListener('abort', () => {
      clearTimeout(timer);
      reject(controller.signal.reason);
    }, { once: true });
  });
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

// Every read is bounded by `timeoutMs`: a timed-out read fails like any other,
// and frees the slot for the next.
export function latestRead<T>(timeoutMs = READ_TIMEOUT_MS): LatestRead<T> {
  let generation = 0;
  let pending: Promise<T> | null = null;
  // The newest read started since the last reset, answered or not.
  let latest: Promise<T> | null = null;
  let controller: AbortController | null = null;

  // A superseded read's caller gets the newer read's answer: an explicit
  // read never leaves its caller without one. A superseded read's answer is
  // handled after the turn that superseded it, so a read that turn started
  // is already `latest`; with none, the caller fails with the abort.
  const follow = (signal: AbortSignal): Promise<T> => latest ?? Promise.reject(signal.reason);

  const start = (read: Read<T>, write: ReadWrite<T>): Promise<T> => {
    controller?.abort();
    const mine = ++generation;
    const own = new AbortController();
    controller = own;
    const outcome = withTimeout(read(own.signal), timeoutMs, own).then(
      (value) => {
        if (mine !== generation) return follow(own.signal);
        pending = null;
        controller = null;
        write.value(value);
        return value;
      },
      (err: unknown) => {
        if (mine !== generation) return follow(own.signal);
        pending = null;
        controller = null;
        write.error(err);
        throw err;
      },
    );
    pending = outcome;
    latest = outcome;
    return outcome;
  };

  return {
    poll: (read, write) => pending ?? start(read, write),
    next: start,
    reset: () => {
      controller?.abort(new ReadDroppedError());
      controller = null;
      generation += 1;
      pending = null;
      latest = null;
    },
  };
}

// When a periodic refresh last started. A failed refresh leaves the view
// stale, so the 1 s staleness check would otherwise start it again on every
// tick; it is tried again at the refresh interval instead.
export function attemptGate(intervalMs: number) {
  let last: number | null = null;
  return {
    due: (now: number) => last === null || now - last >= intervalMs,
    mark: (now: number) => { last = now; },
  };
}

// Runs a refresh's reads together. `settled` runs whether they answer or
// fail, `succeeded` only when all answered; the first failure is rethrown.
export async function refreshAll(
  reads: Promise<unknown>[],
  hooks: { settled(): void; succeeded(): void },
): Promise<void> {
  const results = await Promise.allSettled(reads);
  try {
    const failed = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
    if (failed) throw failed.reason;
    hooks.succeeded();
  } finally {
    hooks.settled();
  }
}

// The error of the first read that has never answered. A read that answered
// once keeps its last answer shown when a later read fails.
export function firstReadError(reads: { loaded: boolean; error: string | null }[]): string | null {
  return reads.find((read) => !read.loaded && read.error)?.error ?? null;
}
