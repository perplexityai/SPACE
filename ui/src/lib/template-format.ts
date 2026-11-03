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
// the operator confirmed (see `moveReleaseTag`). A `Map`, because any string
// is a valid tag: on a plain object, `__proto__` would set the prototype.
export type TagHolders = ReadonlyMap<string, string>;

export function tagHolders(
  releases: { id: string; template_name: string; tags: string[] }[],
  templateName: string,
): TagHolders {
  const holders = new Map<string, string>();
  for (const release of releases) {
    if (release.template_name !== templateName) continue;
    for (const tag of release.tags) {
      if (!holders.has(tag)) holders.set(tag, release.id);
    }
  }
  return holders;
}

// The release `holders` names for `tag`; `null` when none held it, which, as
// the tag route's `expected_current_release_id`, asks that the tag not exist.
export function tagHolder(holders: TagHolders, tag: string): string | null {
  return holders.get(tag) ?? null;
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
  before: TagHolders,
  after: TagHolders,
  tag: string,
): { from: string | null; to: string | null } | null {
  const from = tagHolder(before, tag);
  const to = tagHolder(after, tag);
  return from === to ? null : { from, to };
}

// The tag holders a retag dialog sends as preconditions: those it opened
// with, except a tag whose move it has shown (`change`).
export interface RetagBaseline {
  holders: TagHolders;
  change: { tag: string; from: string | null; to: string | null } | null;
}

// What OK does in the retag dialog for `tag`, given the page's releases now.
// When the tag moved since the dialog last showed it, nothing is sent: the
// dialog shows the move, and only that tag's precondition advances to where
// it is now, so OK again moves it from there, and only while it is still
// there. Every other tag keeps the holder the dialog opened with, so
// switching to another tag that also moved warns again rather than
// overwriting that move.
export function retagStep(
  baseline: RetagBaseline,
  holdersNow: TagHolders,
  tag: string,
): { kind: 'warn'; baseline: RetagBaseline } | { kind: 'move'; expected: string | null } {
  const change = tagHolderChange(baseline.holders, holdersNow, tag);
  if (!change) return { kind: 'move', expected: tagHolder(baseline.holders, tag) };
  const holders = new Map(baseline.holders);
  if (change.to === null) holders.delete(tag);
  else holders.set(tag, change.to);
  return { kind: 'warn', baseline: { holders, change: { tag, ...change } } };
}

// What OK does, from a fresh read of the releases (`readReleases`) rather
// than the page's list, which can be up to one refresh old: a tag that moved
// since the dialog showed it warns before anything is sent. A failed read
// throws, and nothing is sent.
export async function retagStepNow(
  baseline: RetagBaseline,
  readReleases: () => Promise<{ id: string; template_name: string; tags: string[] }[]>,
  templateName: string,
  tag: string,
): Promise<ReturnType<typeof retagStep>> {
  return retagStep(baseline, tagHolders(await readReleases(), templateName), tag);
}

// What a template row's builds panel shows: the template, whether the row is
// expanded, and the page refresh it last saw.
export interface BuildsView {
  templateName: string;
  open: boolean;
  reloadKey: number;
}

// Whether the builds panel reads its builds as its view changes: `load` (no
// rows to keep) when it first opens or shows another template, `background`
// (keeping the rows it shows) when it opens again or the page refreshes while
// it is open, and never while it is collapsed. The table keeps a collapsed
// row's panel mounted, so the page's refreshes still reach it.
export function buildsRead(previous: BuildsView | null, next: BuildsView): 'load' | 'background' | null {
  if (!next.open) return null;
  if (!previous || previous.templateName !== next.templateName) return 'load';
  if (!previous.open || previous.reloadKey !== next.reloadKey) return 'background';
  return null;
}
