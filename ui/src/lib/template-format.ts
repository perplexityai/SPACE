// Shared by the Templates page and the system-template rebuild.

// The tag new sandboxes resolve when they ask for a template by name.
export const LATEST_TAG = 'latest';

// A digest as the releases table shows it: the first 12 hex characters, without
// its `<algorithm>:` prefix; `fallback` when there is none.
export function shortHash(hash: string | null | undefined, fallback: string = LATEST_TAG): string {
  if (!hash) return fallback;
  return hash.replace(/^[a-z0-9]+:/, '').slice(0, 12);
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// The release a template's tag points at now, for the tag route's
// `expected_current_release_id`: tags are per template, across runtimes;
// `null` when no release holds it, which asks that the tag not exist yet.
export function currentTagHolder(
  releases: { id: string; template_name: string; tags: string[] }[],
  templateName: string,
  tag: string,
): string | null {
  return releases.find((release) => release.template_name === templateName && release.tags.includes(tag))?.id ?? null;
}

export const TAG_PRECONDITION_UNCHECKED =
  'This API release does not check where the tag pointed before moving it, so a concurrent move may have been overwritten.';

// A 412 is the precondition refusing: the tag moved since the page read it.
export function tagMoveFailureMessage(err: unknown): string {
  const status = typeof err === 'object' && err !== null ? (err as { status?: unknown }).status : undefined;
  if (status === 412) return 'The tag moved since this page loaded its releases; nothing was changed. Refresh and try again.';
  return errorMessage(err);
}
