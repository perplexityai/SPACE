// Shared by the Templates page and the system-template rebuild.

// The tag new sandboxes resolve when they ask for a template by name.
export const LATEST_TAG = 'latest';

// A digest as the releases table shows it; `fallback` when there is none.
export function shortHash(hash: string | null | undefined, fallback: string = LATEST_TAG): string {
  if (!hash) return fallback;
  return hash.replace('sha256:', '').slice(0, 12);
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
