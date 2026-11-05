// Runs reads of one value one at a time, so an older read's answer never
// lands after a newer one's. A poll tick skips while a read is pending, however
// long it takes; an explicit read (Refresh, the read after an action) waits for
// the pending one and then reads, so it still sees what the action changed.
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
