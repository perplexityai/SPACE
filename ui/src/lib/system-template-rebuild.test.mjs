import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  RUN_MAX_AGE_MS,
  applyLost,
  applyStatus,
  canBuild,
  canPromote,
  confirmPromotion,
  newRun,
  parseStoredRun,
  pinnedTemplate,
  pollDelayMs,
  promotionPlan,
  promotionRemaining,
  readBackPromotion,
  storageKey,
  submitFailureText,
  systemBuildRequest,
  withJob,
} from './system-template-rebuild.ts';

const SEED = 'blake3:' + 'a'.repeat(64);
const CODE = 'blake3:' + 'b'.repeat(64);
const OLD_SEED = 'sha256:' + '1'.repeat(64);
const OLD_CODE = 'sha256:' + '2'.repeat(64);
const ids = { runId: 'run-1', clusterId: 'cluster-a', userId: 'user-a' };
const job = (state, content_hash = null, job_id = 'job-1') => ({ job_id, state, content_hash, error: null, lost: false });
const release = (id, template_name, content_hash, tags = [content_hash]) => ({ id, template_name, content_hash, tags });
const start = () => newRun(ids, { seed: OLD_SEED, 'code-runtime': OLD_CODE }, new Date('2026-11-01T00:00:00Z'));
const built = () => withJob(withJob(start(), 'seed', job('succeeded', SEED, 'job-s')), 'code-runtime', job('succeeded', CODE, 'job-c'));

test('neither build carries publish tags, and code-runtime is built on the exact seed', () => {
  assert.deepEqual(systemBuildRequest('seed'), { publish_tags: [] });
  assert.deepEqual(systemBuildRequest('code-runtime', SEED), { base: `seed@${SEED}`, publish_tags: [] });
  assert.throws(() => systemBuildRequest('code-runtime'));
  assert.equal(pinnedTemplate('seed', SEED), `seed@${SEED}`);
});

test('each step is offered once: a build only while its step has no job', () => {
  const run = start();
  assert.equal(canBuild(null, 'seed'), false);
  assert.equal(canBuild(run, 'seed'), true);
  assert.equal(canBuild(run, 'code-runtime'), false);
  const seeding = withJob(run, 'seed', job('running'));
  assert.equal(canBuild(seeding, 'seed'), false);
  assert.equal(canBuild(seeding, 'code-runtime'), false);
  const seeded = withJob(run, 'seed', job('succeeded', SEED));
  assert.equal(canBuild(seeded, 'code-runtime'), true);
  assert.equal(canPromote(seeded), false);
  assert.equal(canPromote(built()), true);
});

test('a status answer applies only to the run and job that asked for it', () => {
  const run = withJob(start(), 'seed', job('running', null, 'job-s'));
  const answer = { job_id: 'job-s', template_name: 'seed', state: 'succeeded', content_hash: SEED, error: null };
  assert.equal(applyStatus(run, 'run-1', 'seed', answer).seed.content_hash, SEED);
  // A late answer for an earlier run, or another job, is dropped.
  assert.equal(applyStatus(run, 'run-0', 'seed', answer), null);
  assert.equal(applyStatus(run, 'run-1', 'seed', { ...answer, job_id: 'job-old' }), null);
  assert.equal(applyStatus(run, 'run-1', 'code-runtime', { ...answer, template_name: 'code-runtime' }), null);
  // A finished job does not change again.
  const done = applyStatus(run, 'run-1', 'seed', answer);
  assert.equal(applyStatus(done, 'run-1', 'seed', { ...answer, state: 'failed' }), null);
  assert.equal(applyLost(run, 'run-1', 'seed', 'job-s').seed.lost, true);
  assert.equal(applyLost(run, 'run-1', 'seed', 'job-other'), null);
});

test('polling backs off while the status route keeps failing', () => {
  assert.equal(pollDelayMs(0), 5_000);
  assert.equal(pollDelayMs(1), 10_000);
  assert.equal(pollDelayMs(2), 20_000);
  assert.equal(pollDelayMs(10), 60_000);
});

test('a refused build is known not to have started; a 5xx or a network error is not', () => {
  const refused = submitFailureText('seed', Object.assign(new Error('Template is deleted'), { status: 409 }));
  assert.equal(refused.known, true);
  assert.match(refused.text, /was not started/);
  for (const err of [Object.assign(new Error('Internal server error'), { status: 500 }), new TypeError('Failed to fetch')]) {
    const unknown = submitFailureText('seed', err);
    assert.equal(unknown.known, false);
    assert.doesNotMatch(unknown.text, /was not started/);
    assert.match(unknown.text, /may have accepted it/);
  }
});

test('promotion reads latest fresh, and flags a latest that moved since the run started', () => {
  const run = built();
  const unchanged = promotionPlan(run, [release('r1', 'seed', OLD_SEED, ['latest']), release('r2', 'code-runtime', OLD_CODE, ['latest'])], 'latest');
  assert.deepEqual(unchanged.map((r) => [r.template, r.from, r.to, r.changedSinceStart]), [
    ['seed', OLD_SEED, SEED, false],
    ['code-runtime', OLD_CODE, CODE, false],
  ]);
  const moved = promotionPlan(run, [release('r3', 'seed', 'sha256:' + '3'.repeat(64), ['latest'])], 'latest');
  assert.equal(moved[0].changedSinceStart, true);
  assert.equal(moved[1].from, null);
  assert.equal(moved[1].changedSinceStart, true);
});

test('the rollback target is captured once, at confirmation, and kept by a retry', () => {
  const run = built();
  const plan = promotionPlan(run, [release('r1', 'seed', OLD_SEED, ['latest']), release('r2', 'code-runtime', OLD_CODE, ['latest'])], 'latest');
  const confirmed = confirmPromotion(run, plan, new Date('2026-11-01T01:00:00Z'));
  assert.deepEqual(confirmed.promotion.previous, { seed: OLD_SEED, 'code-runtime': OLD_CODE });
  assert.deepEqual(confirmed.promotion.targets, { seed: SEED, 'code-runtime': CODE });
  // seed moved, code-runtime did not: read back says so.
  const partial = [release('n1', 'seed', SEED, ['latest']), release('r2', 'code-runtime', OLD_CODE, ['latest'])];
  const failed = readBackPromotion(confirmed, partial, 'latest', 'the code-runtime tag request answered: 500');
  assert.match(failed.promotion.error, /code-runtime does not carry latest/);
  assert.equal(canPromote(failed), true);
  // The retry plan reads latest fresh (seed already moved), but keeps the
  // previous digests the first attempt captured.
  const retryPlan = promotionPlan(failed, partial, 'latest');
  assert.equal(retryPlan.find((r) => r.template === 'seed').changedSinceStart, false);
  const retried = confirmPromotion(failed, retryPlan);
  assert.deepEqual(retried.promotion.previous, { seed: OLD_SEED, 'code-runtime': OLD_CODE });
  assert.deepEqual(promotionRemaining(retried, partial, 'latest'), ['code-runtime']);
  const done = readBackPromotion(retried, [release('n1', 'seed', SEED, ['latest']), release('n2', 'code-runtime', CODE, ['latest'])], 'latest', null);
  assert.equal(done.promotion.error, null);
  assert.deepEqual(done.promotion.readBack, { seed: SEED, 'code-runtime': CODE });
  assert.equal(canPromote(done), false);
});

test('a stored run is used only for its cluster and user, and only until it expires', () => {
  const run = built();
  const raw = JSON.stringify(run);
  const at = Date.parse(run.startedAt);
  assert.deepEqual(parseStoredRun(raw, ids, at + 1000), run);
  assert.equal(parseStoredRun(raw, { ...ids, clusterId: 'cluster-b' }, at), null);
  assert.equal(parseStoredRun(raw, { ...ids, userId: 'user-b' }, at), null);
  assert.equal(parseStoredRun(raw, ids, at + RUN_MAX_AGE_MS), null);
  assert.ok(RUN_MAX_AGE_MS <= 24 * 60 * 60_000);
  assert.notEqual(storageKey('cluster-a', 'user-a'), storageKey('cluster-b', 'user-a'));
});

test('a stored value in any shape this page does not write is ignored', () => {
  const run = built();
  const at = Date.parse(run.startedAt);
  const variants = [
    null, 'not json', '{}', '[]',
    JSON.stringify({ ...run, version: 1 }),
    JSON.stringify({ ...run, runId: '' }),
    JSON.stringify({ ...run, seed: { ...run.seed, state: 'exploded' } }),
    JSON.stringify({ ...run, seed: { ...run.seed, lost: 'no' } }),
    JSON.stringify({ ...run, latestAtStart: { seed: OLD_SEED } }),
    // code-runtime without a seed that succeeded
    JSON.stringify({ ...run, seed: job('running', null, 'job-s') }),
    JSON.stringify({ ...run, promotion: { previous: {}, targets: {}, confirmedAt: 'x', readBack: null, error: null } }),
    JSON.stringify({ ...run, testSandboxes: { seed: { id: '' } } }),
    JSON.stringify({ ...run, submitUnknown: { other: 'x' } }),
  ];
  for (const raw of variants) assert.equal(parseStoredRun(raw, ids, at), null, raw);
});
