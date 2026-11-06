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

// The server's matches the main table does not already show, from each of
// its answers (the fragment search's prefix request and the ID search's),
// once per sandbox ID, newest first as the list orders them: by creation
// time, then ID. A row without a creation time sorts last.
export function supplementalMatches<T extends { id: string; created_at?: string | null }>(
  answers: readonly (readonly T[])[],
  shown: readonly { id: string }[],
): T[] {
  const seen = new Set(shown.map((row) => row.id));
  const merged: T[] = [];
  for (const row of answers.flat()) {
    if (seen.has(row.id)) continue;
    seen.add(row.id);
    merged.push(row);
  }
  const created = (row: T) => Date.parse(row.created_at ?? '') || 0;
  return merged.sort((a, b) => created(b) - created(a) || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));
}
