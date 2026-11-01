import type { Sandbox } from './types';

// The `spaced` versions running sandboxes report, per template, sampled from
// the admin sandbox list: its newest rows first, at most SPACED_SAMPLE_LIMIT
// of them, with no lifecycle filter. A count over every sandbox would need a
// server-side query.
export const SPACED_SAMPLE_LIMIT = 5000;
export const UNKNOWN_SPACED = 'unknown';

type Row = Pick<Sandbox, 'template' | 'spaced_version' | 'status' | 'lifecycle' | 'resource_type' | 'created_at'>;

export interface SpacedCounts {
  // Per template, running sandboxes by reported spaced version.
  byTemplate: Map<string, Map<string, number>>;
  running: number;
  // Listed rows that are not deleted, and the oldest creation time among them.
  listed: number;
  oldestCreatedAt: string | null;
  // The list returned its limit of database rows: older sandboxes are missing.
  truncated: boolean;
}

export function spacedCounts(rows: Row[], limit: number = SPACED_SAMPLE_LIMIT): SpacedCounts {
  const byTemplate = new Map<string, Map<string, number>>();
  let running = 0;
  let listed = 0;
  let oldest: number | null = null;
  let oldestCreatedAt: string | null = null;
  for (const row of rows) {
    if ((row.status ?? row.lifecycle) === 'deleted') continue;
    listed += 1;
    const created = row.created_at ? Date.parse(row.created_at) : NaN;
    if (!Number.isNaN(created) && (oldest === null || created < oldest)) {
      oldest = created;
      oldestCreatedAt = row.created_at;
    }
    if (row.status !== 'started') continue;
    const template = row.template || UNKNOWN_SPACED;
    const version = row.spaced_version?.trim() || UNKNOWN_SPACED;
    const counts = byTemplate.get(template) ?? new Map<string, number>();
    counts.set(version, (counts.get(version) ?? 0) + 1);
    byTemplate.set(template, counts);
    running += 1;
  }
  // The limit counts the list's database rows, deleted ones included; rows
  // nodes report without one come on top of it.
  const databaseRows = rows.filter((row) => row.resource_type === 'sandbox').length;
  return { byTemplate, running, listed, oldestCreatedAt, truncated: databaseRows >= limit };
}

export function spacedCoverage(counts: SpacedCounts, formatTime: (iso: string) => string): string {
  if (counts.listed === 0) return 'The admin sandbox list returned no sandboxes.';
  const since = counts.oldestCreatedAt ? `, created since ${formatTime(counts.oldestCreatedAt)}` : '';
  const text = `Running sandboxes among the newest ${counts.listed.toLocaleString('en-US')} listed${since}, by the spaced ` +
    'version their guest reports; one that reports none counts as unknown. A template with no running sandbox ' +
    'among them does not appear.';
  return counts.truncated
    ? `${text} The list stops at its newest ${SPACED_SAMPLE_LIMIT.toLocaleString('en-US')} sandboxes, so long-lived ` +
      'sandboxes created earlier, the ones most likely to run an older spaced, are not counted.'
    : text;
}
