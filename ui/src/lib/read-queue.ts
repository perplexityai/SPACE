// The Templates list's latest-build reads, sent a few at a time: each asks a
// builder. A name is queued once, while it waits and while its read is in
// flight, so it is never sent twice at once. Dropping the waiting reads (a
// view that no longer wants them, an unmount, or Strict Mode's cleanup before
// it runs the setup again) leaves in-flight names in place until `finish`.
export class ReadQueue {
  private waiting: string[] = [];
  private readonly inFlight = new Set<string>();
  private readonly concurrency: number;

  constructor(concurrency: number) {
    this.concurrency = concurrency;
  }

  has(name: string): boolean {
    return this.inFlight.has(name) || this.waiting.includes(name);
  }

  // Queues the names neither waiting nor in flight.
  add(names: string[]): void {
    for (const name of names) {
      if (!this.has(name)) this.waiting.push(name);
    }
  }

  // Drops the waiting names `keep` rejects (all of them by default).
  dropWaiting(keep: (name: string) => boolean = () => false): void {
    this.waiting = this.waiting.filter(keep);
  }

  // The names to send now, marked in flight.
  take(): string[] {
    const names: string[] = [];
    while (this.inFlight.size < this.concurrency && this.waiting.length > 0) {
      const name = this.waiting.shift()!;
      this.inFlight.add(name);
      names.push(name);
    }
    return names;
  }

  // A read ended, answered or not.
  finish(name: string): void {
    this.inFlight.delete(name);
  }
}
