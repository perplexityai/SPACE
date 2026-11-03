// The supplemental prefix search of the sandbox list: at most one request in
// flight, and an answer only for the newest request of the search now typed.

export interface PrefixTicket {
  id: number;
  key: string;
  signal: AbortSignal;
}

export class PrefixRequests {
  private latest = 0;
  private controller: AbortController | null = null;

  // Starting a request aborts the one before it, so none overlap.
  begin(key: string): PrefixTicket {
    this.controller?.abort();
    const controller = new AbortController();
    this.controller = controller;
    this.latest += 1;
    return { id: this.latest, key, signal: controller.signal };
  }

  // Aborts the request in flight and voids its ticket: for an effect's
  // cleanup, when the search or the list it refreshes with has changed.
  cancel(): void {
    this.controller?.abort();
    this.controller = null;
    this.latest += 1;
  }

  // A ticket applies only while it is the newest ever issued, not merely the
  // newest for its key: an answer to an earlier A must not land after A, B, A.
  applies(ticket: PrefixTicket, currentKey: string | null): boolean {
    return ticket.id === this.latest && ticket.key === currentKey && !ticket.signal.aborted;
  }
}

// The prefix matches the main table does not already show.
export function supplementalMatches<T extends { id: string }>(matches: readonly T[], shown: readonly { id: string }[]): T[] {
  const shownIds = new Set(shown.map((row) => row.id));
  return matches.filter((row) => !shownIds.has(row.id));
}
