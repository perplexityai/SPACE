import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  RUN_MAX_AGE_MS,
  acceptJob,
  acceptTestSandbox,
  applyLost,
  applyStatus,
  canBuild,
  canPromote,
  confirmPromotion,
  jobOutcomeNotice,
  newRun,
  parseStoredRun,
  pinnedTemplate,
  pollDelayMs,
  promotionPlan,
  promotionState,
  samePlan,
  sandboxOutcomeNotice,
  storageKey,
  submitFailureText,
  systemBuildRequest,
  testSandboxFailureText,
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

test('an answer is recorded only into the still-empty step it was sent for', () => {
  const run = start();
  const first = acceptJob(run, 'run-1', 'seed', job('pending', null, 'job-a'));
  assert.equal(first.kind, 'recorded');
  // Another tab's click got there first: its job is reported, not recorded.
  const second = acceptJob(first.run, 'run-1', 'seed', job('pending', null, 'job-b'));
  assert.equal(second.kind, 'extra');
  assert.equal(second.existing.job_id, 'job-a');
  assert.equal(acceptJob(null, 'run-1', 'seed', job('pending')).kind, 'gone');
  assert.equal(acceptJob(run, 'run-0', 'seed', job('pending')).kind, 'gone');
  const seeded = withJob(run, 'seed', job('succeeded', SEED));
  const sandbox = acceptTestSandbox(seeded, 'run-1', 'seed', { id: 'sbx_a', createdAt: '2026-11-01T00:00:00Z' });
  assert.equal(sandbox.kind, 'recorded');
  const extra = acceptTestSandbox(sandbox.run, 'run-1', 'seed', { id: 'sbx_b', createdAt: '2026-11-01T00:00:00Z' });
  assert.equal(extra.kind, 'extra');
  assert.equal(extra.existing.id, 'sbx_a');
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
    assert.match(unknown.text, /appears in Template Releases only when it finishes/);
    assert.match(unknown.text, /starts a second job/);
    const sandbox = testSandboxFailureText('rebuild-check-seed-x', err);
    assert.equal(sandbox.known, false);
    assert.match(sandbox.text, /may have been created: look for rebuild-check-seed-x in the Sandboxes list/);
  }
  assert.equal(testSandboxFailureText('x', Object.assign(new Error('bad template'), { status: 400 })).known, true);
});

test('a first promotion flags a latest that changed since the run started', () => {
  const run = built();
  const unchanged = promotionPlan(run, [release('r1', 'seed', OLD_SEED, ['latest']), release('r2', 'code-runtime', OLD_CODE, ['latest'])], 'latest');
  assert.deepEqual(unchanged.map((r) => [r.template, r.from, r.to, r.attention]), [
    ['seed', OLD_SEED, SEED, null],
    ['code-runtime', OLD_CODE, CODE, null],
  ]);
  const moved = promotionPlan(run, [release('r3', 'seed', 'sha256:' + '3'.repeat(64), ['latest'])], 'latest');
  assert.equal(moved[0].attention, 'changed-since-start');
  assert.equal(moved[1].from, null);
  assert.equal(moved[1].attention, 'changed-since-start');
  assert.equal(samePlan(unchanged, unchanged.map((r) => ({ ...r }))), true);
  assert.equal(samePlan(unchanged, moved), false);
});

test('a build or test sandbox the run recorded gets no notice; an extra or late one does', () => {
  const existing = job('pending', null, 'job-first');
  assert.equal(jobOutcomeNotice('seed', 'job-1', { kind: 'recorded', run: built() }), null);
  assert.match(jobOutcomeNotice('seed', 'job-2', { kind: 'extra', existing }), /Another click also started seed job job-2; this run follows job job-first/);
  assert.match(jobOutcomeNotice('seed', 'job-3', { kind: 'gone' }), /dismissed or expired meanwhile; seed job job-3 keeps running/);
  const sandbox = { id: 'sbx-first', createdAt: '2026-11-01T00:00:00Z' };
  assert.equal(sandboxOutcomeNotice('seed', 'sbx-1', { kind: 'recorded', run: built() }), null);
  assert.match(sandboxOutcomeNotice('seed', 'sbx-2', { kind: 'extra', existing: sandbox }), /this run keeps sbx-first\. Delete sbx-2\./);
  assert.match(sandboxOutcomeNotice('seed', 'sbx-3', { kind: 'gone' }), /test sandbox sbx-3 was created\. Delete it/);
});

test('a promotion stores only what it was asked to do; its state is read from the tags', () => {
  const run = built();
  const before = [release('r1', 'seed', OLD_SEED, ['latest']), release('r2', 'code-runtime', OLD_CODE, ['latest'])];
  const confirmed = confirmPromotion(run, before, 'latest', new Date('2026-11-01T01:00:00Z'));
  assert.deepEqual(Object.keys(confirmed.promotion).sort(), ['confirmedAt', 'previous', 'targets']);
  assert.deepEqual(confirmed.promotion.previous, { seed: OLD_SEED, 'code-runtime': OLD_CODE });
  assert.deepEqual(confirmed.promotion.targets, { seed: SEED, 'code-runtime': CODE });
  assert.equal(canPromote(confirmed), true);
  // Saved, then no tag moved: every latest is on the release it named
  // before, which reads the same as a rollback.
  assert.deepEqual(promotionState(confirmed.promotion, before, 'latest'), { kind: 'previous', back: ['seed', 'code-runtime'] });
  // seed moved, code-runtime did not.
  const partial = [release('n1', 'seed', SEED, ['latest']), release('r2', 'code-runtime', OLD_CODE, ['latest'])];
  assert.deepEqual(promotionState(confirmed.promotion, partial, 'latest'), { kind: 'incomplete', remaining: ['code-runtime'], diverged: [] });
  // A retry plans only what remains, and keeps the record (and so the rollback target).
  const retryPlan = promotionPlan(confirmed, partial, 'latest');
  assert.deepEqual(retryPlan.map((r) => [r.template, r.attention]), [['code-runtime', null]]);
  assert.equal(confirmPromotion(confirmed, partial, 'latest'), confirmed);
  // Someone moved code-runtime to a third release meanwhile: diverged, and the retry asks first.
  const THIRD = 'sha256:' + '4'.repeat(64);
  const diverged = [release('n1', 'seed', SEED, ['latest']), release('x', 'code-runtime', THIRD, ['latest'])];
  assert.deepEqual(promotionState(confirmed.promotion, diverged, 'latest'), { kind: 'incomplete', remaining: ['code-runtime'], diverged: [{ template: 'code-runtime', latest: THIRD }] });
  assert.equal(promotionPlan(confirmed, diverged, 'latest')[0].attention, 'diverged');
  // Complete only when every latest is its target.
  const done = [release('n1', 'seed', SEED, ['latest']), release('n2', 'code-runtime', CODE, ['latest'])];
  assert.deepEqual(promotionState(confirmed.promotion, done, 'latest'), { kind: 'complete', latest: { seed: SEED, 'code-runtime': CODE } });
  assert.deepEqual(promotionPlan(confirmed, done, 'latest'), []);
});

test('a template already at its target is recorded too, so a later change to it is seen', () => {
  const run = built();
  const before = [release('n1', 'seed', SEED, ['latest']), release('r2', 'code-runtime', OLD_CODE, ['latest'])];
  // Only code-runtime needs a tag request...
  assert.deepEqual(promotionPlan(run, before, 'latest').map((r) => r.template), ['code-runtime']);
  // ...but the record names both, with seed's latest as read.
  const confirmed = confirmPromotion(run, before, 'latest');
  assert.deepEqual(confirmed.promotion.targets, { seed: SEED, 'code-runtime': CODE });
  assert.deepEqual(confirmed.promotion.previous, { seed: SEED, 'code-runtime': OLD_CODE });
  const done = [release('n1', 'seed', SEED, ['latest']), release('n2', 'code-runtime', CODE, ['latest'])];
  assert.equal(promotionState(confirmed.promotion, done, 'latest').kind, 'complete');
  // Someone moves seed afterwards: not complete, and a retry asks before moving it back.
  const THIRD = 'sha256:' + '4'.repeat(64);
  const moved = [release('x', 'seed', THIRD, ['latest']), release('n2', 'code-runtime', CODE, ['latest'])];
  assert.deepEqual(promotionState(confirmed.promotion, moved, 'latest'), { kind: 'incomplete', remaining: ['seed'], diverged: [{ template: 'seed', latest: THIRD }] });
  assert.deepEqual(promotionPlan(confirmed, moved, 'latest').map((r) => [r.template, r.from, r.to, r.attention]), [['seed', THIRD, SEED, 'diverged']]);
});

test('latest moved back to the releases it named before reads as such, not as incomplete', () => {
  // code-runtime had no latest before, so a rollback leaves it on the new release.
  const run = withJob(withJob(newRun(ids, { seed: OLD_SEED, 'code-runtime': null }, new Date('2026-11-01T00:00:00Z')), 'seed', job('succeeded', SEED, 'job-s')), 'code-runtime', job('succeeded', CODE, 'job-c'));
  const confirmed = confirmPromotion(run, [release('r1', 'seed', OLD_SEED, ['latest'])], 'latest');
  assert.deepEqual(confirmed.promotion.previous, { seed: OLD_SEED, 'code-runtime': null });
  const rolledBack = [release('r1', 'seed', OLD_SEED, ['latest']), release('n2', 'code-runtime', CODE, ['latest'])];
  assert.deepEqual(promotionState(confirmed.promotion, rolledBack, 'latest'), { kind: 'previous', back: ['seed'] });
  // Promoting again asks nothing extra: seed is where the record says it was.
  assert.deepEqual(promotionPlan(confirmed, rolledBack, 'latest').map((r) => [r.template, r.attention]), [['seed', null]]);
  // Interrupted after seed moved: still incomplete.
  const interrupted = [release('n1', 'seed', SEED, ['latest'])];
  assert.deepEqual(promotionState(confirmed.promotion, interrupted, 'latest'), { kind: 'incomplete', remaining: ['code-runtime'], diverged: [] });
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
    JSON.stringify({ ...run, promotion: { previous: {}, targets: {}, confirmedAt: 'x' } }),
    // The earlier shape, with a stored read-back.
    JSON.stringify({ ...run, promotion: { previous: { seed: OLD_SEED, 'code-runtime': OLD_CODE }, targets: { seed: SEED }, confirmedAt: '2026-11-01T01:00:00Z', readBack: null, error: null } }),
    // A record naming one template only (before both were recorded).
    JSON.stringify({ ...run, promotion: { previous: { seed: OLD_SEED, 'code-runtime': OLD_CODE }, targets: { seed: SEED }, confirmedAt: '2026-11-01T01:00:00Z' } }),
    JSON.stringify({ ...run, testSandboxes: { seed: { id: '' } } }),
    JSON.stringify({ ...run, submitUnknown: { other: 'x' } }),
  ];
  for (const raw of variants) assert.equal(parseStoredRun(raw, ids, at), null, raw);
});
