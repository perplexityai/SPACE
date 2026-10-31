// How a detail page reports that its object failed to load: only a 404
// means the object does not exist; anything else (403, 5xx, a network
// error) is a failure to load it, which may succeed on retry.
export interface LoadFailure {
  status: number | null;
  message: string;
}

export function loadFailureFrom(err: unknown, fallback: string): LoadFailure {
  const status = typeof err === 'object' && err !== null && typeof (err as { status?: unknown }).status === 'number'
    ? (err as { status: number }).status
    : null;
  const message = err instanceof Error && err.message ? err.message : fallback;
  return { status, message };
}

export function loadFailureView(failure: LoadFailure, noun: string): { title: string; retry: boolean } {
  if (failure.status === 404) {
    return { title: `${noun.charAt(0).toUpperCase()}${noun.slice(1)} not found`, retry: false };
  }
  return { title: `Could not load the ${noun}`, retry: true };
}
