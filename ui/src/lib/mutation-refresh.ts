// A change the console makes, then the reads of the views it changed. The
// change's result is told as soon as it is known. A refresh that fails after
// a change took effect is told as a view not read again, never as the change
// failing: that would have the operator retry a change already made.
export interface Mutation<T> {
  // The change itself; its failure is the change's failure.
  mutate(): Promise<T>;
  // Told of a change that took effect, before its refresh starts.
  succeeded(value: T): void;
  failed(err: unknown): void;
  // Reads the views the change touched.
  refresh(value: T): Promise<unknown>;
  // The refresh failed; the change still took effect.
  refreshFailed(err: unknown): void;
}

// Whether the change took effect, once its refresh has settled.
export async function runMutation<T>(mutation: Mutation<T>): Promise<boolean> {
  let value: T;
  try {
    value = await mutation.mutate();
  } catch (err) {
    mutation.failed(err);
    return false;
  }
  mutation.succeeded(value);
  try {
    await mutation.refresh(value);
  } catch (err) {
    mutation.refreshFailed(err);
  }
  return true;
}
