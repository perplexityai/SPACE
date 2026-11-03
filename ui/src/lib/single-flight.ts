// Runs one piece of work at a time: while a run has not finished, another is
// dropped rather than started, as a second press of a dialog's OK while its
// request is in flight is. React state updates too late to stop that press.
export interface SingleFlight {
  busy(): boolean;
  // `{ ran: false }` when a run was already in flight.
  run<T>(work: () => Promise<T>): Promise<{ ran: true; value: T } | { ran: false }>;
}

export function singleFlight(): SingleFlight {
  let busy = false;
  return {
    busy: () => busy,
    async run<T>(work: () => Promise<T>): Promise<{ ran: true; value: T } | { ran: false }> {
      if (busy) return { ran: false };
      busy = true;
      try {
        return { ran: true, value: await work() };
      } finally {
        busy = false;
      }
    },
  };
}
