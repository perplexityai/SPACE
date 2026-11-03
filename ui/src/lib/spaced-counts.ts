import type { Sandbox } from './types';

// The `spaced` versions running sandboxes report, per template. A server
// that has `GET /sandboxes/spaced-versions` counts every listed sandbox that
// is not deleted; an older one does not, and the console then tallies a
// sample of the admin sandbox list: its newest rows first, at most
// SPACED_SAMPLE_LIMIT of them, with no lifecycle filter.
export const SPACED_SAMPLE_LIMIT = 5000;
export const UNKNOWN_SPACED = 'unknown';

type Row = Pick<Sandbox, 'template' | 'spaced_version' | 'status' | 'lifecycle' | 'resource_type' | 'created_at'>;

export interface SpacedCounts {
  // `server`: every listed sandbox, counted by the API. `sample`: the
  // console's tally of the newest `sampleLimit` list rows.
  source: 'server' | 'sample';
  sampleLimit: number;
  // Per template, running sandboxes by reported spaced version.
  byTemplate: Map<string, Map<string, number>>;
  running: number;
  // Listed database rows that are not deleted, and the oldest creation time
  // among them. The rows nodes report without a database row (such as
  // warm-pool slots), which the list appends beyond its limit, are counted
  // in `nodeReported` instead and do not move the cutoff.
  listed: number;
  oldestCreatedAt: string | null;
  nodeReported: number;
  // The list returned its limit of database rows: older sandboxes are missing.
  truncated: boolean;
}

function addRunning(byTemplate: SpacedCounts['byTemplate'], template: string | null, version: string | null, count: number) {
  const key = template || UNKNOWN_SPACED;
  const counts = byTemplate.get(key) ?? new Map<string, number>();
  const reported = version?.trim() || UNKNOWN_SPACED;
  counts.set(reported, (counts.get(reported) ?? 0) + count);
  byTemplate.set(key, counts);
}

export function spacedCounts(rows: Row[], limit: number = SPACED_SAMPLE_LIMIT): SpacedCounts {
  const byTemplate = new Map<string, Map<string, number>>();
  let running = 0;
  let listed = 0;
  let nodeReported = 0;
  let oldest: number | null = null;
  let oldestCreatedAt: string | null = null;
  for (const row of rows) {
    if ((row.status ?? row.lifecycle) === 'deleted') continue;
    if (row.resource_type === 'sandbox') {
      listed += 1;
      const created = row.created_at ? Date.parse(row.created_at) : NaN;
      if (!Number.isNaN(created) && (oldest === null || created < oldest)) {
        oldest = created;
        oldestCreatedAt = row.created_at;
      }
    } else {
      nodeReported += 1;
    }
    if (row.status !== 'started') continue;
    addRunning(byTemplate, row.template, row.spaced_version, 1);
    running += 1;
  }
  // The limit counts the list's database rows, deleted ones included; rows
  // nodes report without one come on top of it.
  const databaseRows = rows.filter((row) => row.resource_type === 'sandbox').length;
  return {
    source: 'sample',
    sampleLimit: limit,
    byTemplate,
    running,
    listed,
    oldestCreatedAt,
    nodeReported,
    truncated: databaseRows >= limit,
  };
}

// One group of `GET /sandboxes/spaced-versions`: the listed sandboxes whose
// status is not `deleted` that show this template, version, status and
// resource type in the list.
export interface ServerSpacedCount {
  template: string | null;
  spaced_version: string | null;
  status: string | null;
  resource_type: string;
  count: number;
}

// The route's `{counts}` body, or null for any other shape.
export function parseServerSpacedCounts(body: unknown): ServerSpacedCount[] | null {
  const counts = (body as { counts?: unknown } | null)?.counts;
  if (!Array.isArray(counts)) return null;
  const valid = counts.every((group) =>
    group && typeof group === 'object' && Number.isFinite((group as ServerSpacedCount).count));
  return valid ? counts as ServerSpacedCount[] : null;
}

// The same tally as `spacedCounts`, from the server's groups. The server
// counts every row, so nothing is truncated and there is no oldest row.
export function spacedCountsFromServer(groups: ServerSpacedCount[]): SpacedCounts {
  const byTemplate = new Map<string, Map<string, number>>();
  let running = 0;
  let listed = 0;
  let nodeReported = 0;
  for (const group of groups) {
    if (group.status === 'deleted') continue;
    if (group.resource_type === 'sandbox') listed += group.count;
    else nodeReported += group.count;
    if (group.status !== 'started') continue;
    addRunning(byTemplate, group.template, group.spaced_version, group.count);
    running += group.count;
  }
  return {
    source: 'server',
    sampleLimit: 0,
    byTemplate,
    running,
    listed,
    oldestCreatedAt: null,
    nodeReported,
    truncated: false,
  };
}

// A server without the count route answers 400 (it reads `spaced-versions`
// as a sandbox ID) or 404; any other failure is reported, not sampled.
export function spacedCountRouteMissing(err: unknown): boolean {
  const status = (err as { status?: unknown } | null)?.status;
  return status === 400 || status === 404;
}

// The server's count when it has the route, else the tally of a sample.
export async function loadSpacedCounts(
  readServer: () => Promise<unknown>,
  readSample: () => Promise<Row[]>,
  sampleLimit: number = SPACED_SAMPLE_LIMIT,
): Promise<SpacedCounts> {
  let body: unknown;
  try {
    body = await readServer();
  } catch (err) {
    if (!spacedCountRouteMissing(err)) throw err;
    return spacedCounts(await readSample(), sampleLimit);
  }
  const groups = parseServerSpacedCounts(body);
  return groups ? spacedCountsFromServer(groups) : spacedCounts(await readSample(), sampleLimit);
}

export function spacedCoverage(counts: SpacedCounts, formatTime: (iso: string) => string): string {
  const cap = counts.truncated
    ? ` The list stops at its newest ${counts.sampleLimit.toLocaleString('en-US')} sandboxes, so long-lived sandboxes ` +
      'created earlier, the ones most likely to run an older spaced, are not counted.'
    : '';
  const reported = counts.nodeReported
    ? ` Also counted: ${counts.nodeReported.toLocaleString('en-US')} row${counts.nodeReported === 1 ? '' : 's'} nodes report ` +
      'without a database row, such as warm-pool slots.'
    : '';
  if (counts.source === 'server') {
    return `Running sandboxes among all ${counts.listed.toLocaleString('en-US')} that are not deleted, by the spaced ` +
      'version their guest reports; one that reports none counts as unknown. A template with no running sandbox ' +
      `does not appear.${reported}`;
  }
  if (counts.listed === 0) return `The admin sandbox list returned no sandboxes that are not deleted.${reported}${cap}`;
  const since = counts.oldestCreatedAt ? `, created since ${formatTime(counts.oldestCreatedAt)}` : '';
  return `Running sandboxes among the newest ${counts.listed.toLocaleString('en-US')} listed${since}, by the spaced ` +
    'version their guest reports; one that reports none counts as unknown. A template with no running sandbox ' +
    `among them does not appear.${reported}${cap}`;
}
