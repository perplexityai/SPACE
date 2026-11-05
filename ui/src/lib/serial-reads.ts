// Runs reads of one value one at a time, so an older read's answer never
// lands after a newer one's. A poll tick skips while a read is pending, however
// long it takes; an explicit read (Refresh, the read after an action) waits for
// the pending one and then reads, so it still sees what the action changed.
// A read that never settles would hold the queue for ever, so bound each read
// with `withTimeout`.
export interface SerialReads {
  // Starts `read` unless a read is pending; null when the tick skips.
  poll<T>(read: () => Promise<T>): Promise<T> | null;
  // Starts `read` once the pending reads, if any, have settled.
  next<T>(read: () => Promise<T>): Promise<T>;
}

export function serialReads(): SerialReads {
  // Settles when the last read started or queued has.
  let tail: Promise<void> | null = null;
  const next = <T>(read: () => Promise<T>): Promise<T> => {
    const result = tail ? tail.then(read) : read();
    const settled: Promise<void> = result.then(
      () => { if (tail === settled) tail = null; },
      () => { if (tail === settled) tail = null; },
    );
    tail = settled;
    return result;
  };
  return {
    poll: (read) => (tail ? null : next(read)),
    next,
  };
}

// Runs `read` with a signal that aborts after `ms`. A fetch given the signal
// then rejects with its reason, a TimeoutError, before or after the headers,
// so a request that gets no answer fails like any other read.
export async function withTimeout<T>(ms: number, read: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const request = new AbortController();
  const timer = setTimeout(
    () => request.abort(new DOMException(`No answer from the API within ${ms / 1000} s`, 'TimeoutError')),
    ms,
  );
  try {
    return await read(request.signal);
  } finally {
    clearTimeout(timer);
  }
}
