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

// Which release holds each of a template's tags, from one read of the
// releases: tags are per template, across runtimes. A tag dialog keeps this
// from when it opened, so a background refresh cannot change the precondition
// the operator confirmed (see `moveReleaseTag`).
export function tagHolders(
  releases: { id: string; template_name: string; tags: string[] }[],
  templateName: string,
): Record<string, string> {
  const holders: Record<string, string> = {};
  for (const release of releases) {
    if (release.template_name !== templateName) continue;
    for (const tag of release.tags) {
      if (!Object.hasOwn(holders, tag)) holders[tag] = release.id;
    }
  }
  return holders;
}

// The release `holders` names for `tag`; `null` when none held it, which, as
// the tag route's `expected_current_release_id`, asks that the tag not exist.
export function tagHolder(holders: Record<string, string>, tag: string): string | null {
  return Object.hasOwn(holders, tag) ? holders[tag] : null;
}

export function currentTagHolder(
  releases: { id: string; template_name: string; tags: string[] }[],
  templateName: string,
  tag: string,
): string | null {
  return tagHolder(tagHolders(releases, templateName), tag);
}

// How `tag` moved between two reads, or null when it did not.
export function tagHolderChange(
  before: Record<string, string>,
  after: Record<string, string>,
  tag: string,
): { from: string | null; to: string | null } | null {
  const from = tagHolder(before, tag);
  const to = tagHolder(after, tag);
  return from === to ? null : { from, to };
}
