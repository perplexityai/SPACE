import type { TemplateBuildState, TemplateBuildStatus, TemplateRelease } from './types';

// Rebuilding the system templates: seed, then code-runtime on that exact seed
// digest, as the seed README's release procedure does with the admin CLI.
// Every step that changes something (a build, a test sandbox, the
// promotion) is a click in the tab that shows it; the stored run only lets a
// reload, or another tab, show the run and keep polling its builds.

export type SystemTemplate = 'seed' | 'code-runtime';

const STORAGE_PREFIX = 'space:system-template-rebuild:v2';
// The builder keeps a job's status for four days; a run is kept for at most
// a day, which is shorter.
export const BUILDER_JOB_RETENTION_MS = 4 * 24 * 60 * 60_000;
export const RUN_MAX_AGE_MS = Math.min(BUILDER_JOB_RETENTION_MS, 24 * 60 * 60_000);

const BUILD_STATES: readonly TemplateBuildState[] = ['pending', 'running', 'succeeded', 'failed', 'publish_failed', 'unspecified'];

export interface BuildJob {
  job_id: string;
  state: TemplateBuildState;
  content_hash: string | null;
  error: string | null;
  // The status route answered 404: the builder no longer reports the job.
  lost: boolean;
}

// What a promotion moved, recorded from the click that confirmed it.
export interface Promotion {
  // `latest` of each template as read when the operator confirmed, written
  // before the first tag request and never read again: the rollback target.
  previous: Record<SystemTemplate, string | null>;
  targets: Record<SystemTemplate, string>;
  confirmedAt: string;
  // `latest` read back after the tag requests (the tag route can commit and
  // still answer an error), or null until then.
  readBack: Record<SystemTemplate, string | null> | null;
  error: string | null;
}

export interface TestSandbox {
  id: string;
  createdAt: string;
}

export interface RebuildRun {
  version: 2;
  runId: string;
  clusterId: string;
  userId: string;
  startedAt: string;
  // `latest` of each template when the run started.
  latestAtStart: Record<SystemTemplate, string | null>;
  seed: BuildJob | null;
  codeRuntime: BuildJob | null;
  // A build request whose outcome is unknown (it may have been accepted).
  submitUnknown: Partial<Record<SystemTemplate, string>>;
  testSandboxes: Partial<Record<SystemTemplate, TestSandbox>>;
  promotion: Promotion | null;
}

export function storageKey(clusterId: string, userId: string): string {
  return `${STORAGE_PREFIX}:${clusterId}:${userId}`;
}

export function newRun(
  ids: { runId: string; clusterId: string; userId: string },
  latestAtStart: Record<SystemTemplate, string | null>,
  now = new Date(),
): RebuildRun {
  return {
    version: 2,
    ...ids,
    startedAt: now.toISOString(),
    latestAtStart,
    seed: null,
    codeRuntime: null,
    submitUnknown: {},
    testSandboxes: {},
    promotion: null,
  };
}

export function runExpiresAt(run: Pick<RebuildRun, 'startedAt'>): number {
  return Date.parse(run.startedAt) + RUN_MAX_AGE_MS;
}

export function jobOf(run: RebuildRun, template: SystemTemplate): BuildJob | null {
  return template === 'seed' ? run.seed : run.codeRuntime;
}

export function withJob(run: RebuildRun, template: SystemTemplate, job: BuildJob): RebuildRun {
  return template === 'seed' ? { ...run, seed: job } : { ...run, codeRuntime: job };
}

export function isTerminal(job: BuildJob | null): boolean {
  return job !== null && (job.lost || job.state === 'succeeded' || job.state === 'failed' || job.state === 'publish_failed');
}

export function buildSucceeded(job: BuildJob | null): job is BuildJob & { content_hash: string } {
  return job !== null && !job.lost && job.state === 'succeeded' && Boolean(job.content_hash);
}

export function buildFailureText(template: SystemTemplate, job: BuildJob): string | null {
  if (job.lost) return `${template}: the builder no longer reports job ${job.job_id}; check Template Releases for its result`;
  if (job.state === 'failed') return `${template} failed${job.error ? `: ${job.error}` : ''}`;
  if (job.state === 'publish_failed') return `${template} built but did not publish${job.error ? `: ${job.error}` : ''}`;
  return null;
}

// The request body for one system build. Neither build carries publish tags,
// so each registers a release tagged only with its digest; `force` is not
// sent because builders ignore it.
export function systemBuildRequest(template: SystemTemplate, seedHash?: string) {
  if (template === 'code-runtime') {
    if (!seedHash) throw new Error('code-runtime is built on an exact seed digest');
    return { base: `seed@${seedHash}`, publish_tags: [] as string[] };
  }
  return { publish_tags: [] as string[] };
}

// The pinned reference a test sandbox is created from.
export function pinnedTemplate(template: SystemTemplate, contentHash: string): string {
  return `${template}@${contentHash}`;
}

// Which explicit step each build is at: whether its button is offered.
export function canBuild(run: RebuildRun | null, template: SystemTemplate): boolean {
  if (template === 'seed') return run !== null && run.seed === null;
  return run !== null && buildSucceeded(run.seed) && run.codeRuntime === null;
}

export function canPromote(run: RebuildRun | null): boolean {
  return run !== null && buildSucceeded(run.seed) && buildSucceeded(run.codeRuntime)
    && (run.promotion === null || run.promotion.error !== null);
}

// Apply a status answer only to the run and job that asked for it: a late
// answer for an earlier run, or an earlier job of this one, is dropped.
export function applyStatus(
  stored: RebuildRun | null,
  runId: string,
  template: SystemTemplate,
  status: TemplateBuildStatus,
): RebuildRun | null {
  if (!stored || stored.runId !== runId) return null;
  const job = jobOf(stored, template);
  if (!job || job.job_id !== status.job_id || isTerminal(job)) return null;
  const state = BUILD_STATES.includes(status.state) ? status.state : 'unspecified';
  return withJob(stored, template, {
    job_id: job.job_id,
    state,
    content_hash: status.content_hash ?? null,
    error: status.error ?? null,
    lost: false,
  });
}

export function applyLost(stored: RebuildRun | null, runId: string, template: SystemTemplate, jobId: string): RebuildRun | null {
  if (!stored || stored.runId !== runId) return null;
  const job = jobOf(stored, template);
  if (!job || job.job_id !== jobId || isTerminal(job)) return null;
  return withJob(stored, template, { ...job, lost: true });
}

// Poll every 5 s, backing off to 60 s while the status route keeps failing.
export function pollDelayMs(consecutiveFailures: number): number {
  return Math.min(60_000, 5_000 * 2 ** Math.max(0, consecutiveFailures));
}

// A build request that failed: refused before it reached the builder, or an
// outcome that is not known (a 5xx or a network error can arrive after the
// builder accepted the job).
export function submitFailureText(template: SystemTemplate, err: unknown): { known: boolean; text: string } {
  const status = typeof (err as { status?: unknown })?.status === 'number' ? (err as { status: number }).status : null;
  const message = err instanceof Error ? err.message : String(err);
  if (status !== null && status >= 400 && status < 500) {
    return { known: true, text: `The ${template} build was not started: ${message}` };
  }
  return {
    known: false,
    text: `The ${template} build request failed (${message}), and the builder may have accepted it. ` +
      'Refresh Template Releases, and look for a new release, before building again.',
  };
}

export function latestDigest(
  releases: Pick<TemplateRelease, 'template_name' | 'tags' | 'content_hash'>[],
  template: SystemTemplate,
  tag: string,
): string | null {
  return releases.find((release) => release.template_name === template && release.tags.includes(tag))?.content_hash ?? null;
}

export function findRelease<T extends Pick<TemplateRelease, 'template_name' | 'content_hash'>>(
  releases: T[],
  template: SystemTemplate,
  contentHash: string,
): T | null {
  return releases.find((release) => release.template_name === template && release.content_hash === contentHash) ?? null;
}

export interface PromotionPlanRow {
  template: SystemTemplate;
  from: string | null;
  to: string;
  // `latest` is no longer what it was when the run started.
  changedSinceStart: boolean;
}

// What a promotion would move, from releases read when the operator asks.
export function promotionPlan(
  run: RebuildRun,
  releases: Pick<TemplateRelease, 'template_name' | 'tags' | 'content_hash'>[],
  tag: string,
): PromotionPlanRow[] {
  const targets: [SystemTemplate, BuildJob | null][] = [['seed', run.seed], ['code-runtime', run.codeRuntime]];
  return targets.flatMap(([template, job]) => {
    if (!buildSucceeded(job)) return [];
    const from = latestDigest(releases, template, tag);
    // A template a first attempt already moved has not "changed" under the
    // operator.
    return [{ template, from, to: job.content_hash, changedSinceStart: from !== run.latestAtStart[template] && from !== job.content_hash }];
  });
}

// The promotion record written when the operator confirms, before any tag
// request. A retry keeps the record's `previous`.
export function confirmPromotion(run: RebuildRun, plan: PromotionPlanRow[], now = new Date()): RebuildRun {
  if (run.promotion) return { ...run, promotion: { ...run.promotion, error: null } };
  const previous = { seed: null, 'code-runtime': null } as Record<SystemTemplate, string | null>;
  const targets = {} as Record<SystemTemplate, string>;
  for (const row of plan) {
    previous[row.template] = row.from;
    targets[row.template] = row.to;
  }
  return { ...run, promotion: { previous, targets, confirmedAt: now.toISOString(), readBack: null, error: null } };
}

export function readBackPromotion(
  run: RebuildRun,
  releases: Pick<TemplateRelease, 'template_name' | 'tags' | 'content_hash'>[],
  tag: string,
  requestError: string | null,
): RebuildRun {
  if (!run.promotion) return run;
  const readBack = {
    seed: latestDigest(releases, 'seed', tag),
    'code-runtime': latestDigest(releases, 'code-runtime', tag),
  } as Record<SystemTemplate, string | null>;
  const unmoved = (Object.keys(run.promotion.targets) as SystemTemplate[]).filter((t) => readBack[t] !== run.promotion!.targets[t]);
  const error = unmoved.length
    ? `${unmoved.join(' and ')} ${unmoved.length > 1 ? 'do' : 'does'} not carry ${tag} on the new release${requestError ? ` (${requestError})` : ''}`
    : null;
  return { ...run, promotion: { ...run.promotion, readBack, error } };
}

// The templates a retry still has to move, from a fresh read.
export function promotionRemaining(
  run: RebuildRun,
  releases: Pick<TemplateRelease, 'template_name' | 'tags' | 'content_hash'>[],
  tag: string,
): SystemTemplate[] {
  if (!run.promotion) return [];
  return (Object.keys(run.promotion.targets) as SystemTemplate[])
    .filter((template) => latestDigest(releases, template, tag) !== run.promotion!.targets[template]);
}

// A stored run, if it is one this page wrote for this cluster and user and
// it has not expired; anything else is ignored.
export function parseStoredRun(raw: string | null, expect: { clusterId: string; userId: string }, now = Date.now()): RebuildRun | null {
  if (!raw) return null;
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRun(value)) return null;
  if (value.clusterId !== expect.clusterId || value.userId !== expect.userId) return null;
  if (!(now < runExpiresAt(value))) return null;
  return value;
}

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);
const isDigestOrNull = (v: unknown) => v === null || (typeof v === 'string' && v.length > 0);
const isDigestMap = (v: unknown, required: boolean) => isObject(v)
  && Object.entries(v).every(([k, d]) => (k === 'seed' || k === 'code-runtime') && isDigestOrNull(d))
  && (!required || ('seed' in v && 'code-runtime' in v));

function isJob(v: unknown): v is BuildJob {
  return isObject(v)
    && typeof v.job_id === 'string' && v.job_id.length > 0
    && typeof v.state === 'string' && (BUILD_STATES as readonly string[]).includes(v.state)
    && isDigestOrNull(v.content_hash)
    && (v.error === null || typeof v.error === 'string')
    && typeof v.lost === 'boolean';
}

function isPromotion(v: unknown): v is Promotion {
  return isObject(v)
    && isDigestMap(v.previous, true)
    && isObject(v.targets) && Object.keys(v.targets).length > 0
    && Object.entries(v.targets).every(([k, d]) => (k === 'seed' || k === 'code-runtime') && typeof d === 'string' && d.length > 0)
    && typeof v.confirmedAt === 'string' && !Number.isNaN(Date.parse(v.confirmedAt))
    && (v.readBack === null || isDigestMap(v.readBack, true))
    && (v.error === null || typeof v.error === 'string');
}

function isRun(v: unknown): v is RebuildRun {
  return isObject(v)
    && v.version === 2
    && typeof v.runId === 'string' && v.runId.length > 0
    && typeof v.clusterId === 'string' && v.clusterId.length > 0
    && typeof v.userId === 'string' && v.userId.length > 0
    && typeof v.startedAt === 'string' && !Number.isNaN(Date.parse(v.startedAt))
    && isDigestMap(v.latestAtStart, true)
    && (v.seed === null || isJob(v.seed))
    && (v.codeRuntime === null || isJob(v.codeRuntime))
    // A code-runtime job only ever follows a seed that succeeded.
    && (v.codeRuntime === null || buildSucceeded(v.seed as BuildJob | null))
    && isObject(v.submitUnknown) && Object.entries(v.submitUnknown).every(([k, t]) => (k === 'seed' || k === 'code-runtime') && typeof t === 'string')
    && isObject(v.testSandboxes) && Object.entries(v.testSandboxes).every(([k, t]) => (k === 'seed' || k === 'code-runtime')
      && isObject(t) && typeof t.id === 'string' && t.id.length > 0 && typeof t.createdAt === 'string')
    && (v.promotion === null || isPromotion(v.promotion));
}
