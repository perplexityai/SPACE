import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  RUN_MAX_AGE_MS,
  acceptJob,
  acceptTestSandbox,
  applyLost,
  applySubmitFailure,
  buildKeyClosed,
  buildKeyGeneration,
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
  retiredBuilds,
  samePlan,
  sandboxOutcomeNotice,
  storageKey,
  submitFailureText,
  systemBuildIdempotencyKey,
  systemBuildRequest,
  testSandboxFailureText,
  withJob,
  withNewBuildKey,
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
  const first = acceptJob(run, 'run-1', 'seed', job('pending', null, 'job-a'), 0);
  assert.equal(first.kind, 'recorded');
  // Another tab's click got there first: its job is reported, not recorded.
  const second = acceptJob(first.run, 'run-1', 'seed', job('pending', null, 'job-b'), 0);
  assert.equal(second.kind, 'extra');
  assert.equal(second.existing.job_id, 'job-a');
  assert.match(jobOutcomeNotice('seed', 'job-b', second), /Another click also started seed job job-b/);
  // Another tab sent the same key and the API replayed it: the same job is
  // the build the run already follows, not an extra one.
  const replay = acceptJob(first.run, 'run-1', 'seed', job('pending', null, 'job-a'), 0);
  assert.deepEqual(replay, { kind: 'recorded', run: first.run });
  assert.equal(jobOutcomeNotice('seed', 'job-a', replay), null);
  assert.equal(acceptJob(null, 'run-1', 'seed', job('pending'), 0).kind, 'gone');
  assert.equal(acceptJob(run, 'run-0', 'seed', job('pending'), 0).kind, 'gone');
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

test("a build's Idempotency-Key is the same for the run and template, and differs otherwise", () => {
  assert.equal(systemBuildIdempotencyKey('run-1', 'seed'), systemBuildIdempotencyKey('run-1', 'seed'));
  assert.notEqual(systemBuildIdempotencyKey('run-1', 'seed'), systemBuildIdempotencyKey('run-1', 'code-runtime'));
  assert.notEqual(systemBuildIdempotencyKey('run-1', 'seed'), systemBuildIdempotencyKey('run-2', 'seed'));
});

const conflict = (message, retryable, details) => Object.assign(new Error(message), {
  status: 409,
  body: { code: 'conflict', message, request_id: null, retryable, details },
});

test('a 409 while the earlier request with the same key may still be being submitted asks to build again shortly', () => {
  const pending = conflict('a build with this Idempotency-Key may still be being submitted; retry shortly', true, [
    { key: 'reserved_at', value: '2026-11-03T05:00:00Z' },
  ]);
  const outcome = submitFailureText('seed', pending);
  assert.equal(outcome.known, false);
  assert.equal(outcome.closed, undefined);
  assert.match(outcome.text, /may still be being submitted; build again shortly/);
  // The same key with another body is a refusal.
  assert.equal(submitFailureText('seed', conflict('idempotency key was already used with a different request', false, [])).known, true);
});

test("a non-retryable 409 closes the key: it names the job when known and does not promise the first build", () => {
  const unknown = conflict('the build submitted with this Idempotency-Key at 2026-11-03T05:00:00Z has an unknown outcome', false, [
    { key: 'reserved_at', value: '2026-11-03T05:00:00Z' },
    { key: 'job_id', value: 'job-synthetic' },
  ]);
  const outcome = submitFailureText('code-runtime', unknown);
  assert.equal(outcome.known, false);
  assert.deepEqual(outcome.closed, { jobId: 'job-synthetic' });
  assert.match(outcome.text, /unknown outcome: it may have started and may still be running as job job-synthetic/);
  assert.match(outcome.text, /answers the same/);
  assert.doesNotMatch(outcome.text, /returns the first build|answers with the first build/);
  const noJob = submitFailureText('seed', conflict('x', false, [{ key: 'reserved_at', value: '2026-11-03T05:00:00Z' }]));
  assert.deepEqual(noJob.closed, { jobId: null });
  assert.doesNotMatch(noJob.text, / as job /);
});

test('the unknown-outcome text says what building again with the same key can answer', () => {
  const { text } = submitFailureText('seed', Object.assign(new Error('Internal server error'), { status: 500 }));
  assert.match(text, /resends the same Idempotency-Key/);
  assert.match(text, /could not tell whether the builder took it says so/);
  assert.doesNotMatch(text, /a retry returns the first build/);
});

test('"Start a new build" sends a new key for that template only, and keeps the attempt it replaced', () => {
  const closed = { ...start(), submitUnknown: { seed: 'unknown outcome' }, keyClosed: { seed: { jobId: 'job-x' } } };
  assert.equal(buildKeyGeneration(closed, 'seed'), 0);
  assert.deepEqual(buildKeyClosed(closed, 'seed'), { jobId: 'job-x' });
  const renewed = withNewBuildKey(closed, 'seed');
  assert.equal(buildKeyGeneration(renewed, 'seed'), 1);
  assert.equal(buildKeyGeneration(renewed, 'code-runtime'), 0);
  // Only the notice that blocked building is cleared.
  assert.equal(buildKeyClosed(renewed, 'seed'), null);
  assert.deepEqual(renewed.submitUnknown, {});
  // The attempt, and the job the 409 named, stay shown as unresolved.
  assert.deepEqual(retiredBuilds(renewed, 'seed'), [{ generation: 0, jobId: 'job-x', notes: [] }]);
  assert.deepEqual(retiredBuilds(renewed, 'code-runtime'), []);
  const first = systemBuildIdempotencyKey(ids.runId, 'seed', buildKeyGeneration(closed, 'seed'));
  const second = systemBuildIdempotencyKey(ids.runId, 'seed', buildKeyGeneration(renewed, 'seed'));
  assert.notEqual(first, second);
  // Generation 0 is the key runs stored before generations used.
  assert.equal(first, systemBuildIdempotencyKey(ids.runId, 'seed'));
  // A second new build keeps both earlier attempts, oldest first.
  const third = withNewBuildKey({ ...renewed, keyClosed: { seed: { jobId: null } } }, 'seed');
  assert.equal(buildKeyGeneration(third, 'seed'), 2);
  assert.deepEqual(retiredBuilds(third, 'seed').map((attempt) => [attempt.generation, attempt.jobId]), [[0, 'job-x'], [1, null]]);
  // The new generation's job is the run's build; the retired attempt stays.
  const recorded = acceptJob(renewed, ids.runId, 'seed', job('pending', null, 'job-y'), 1);
  assert.equal(recorded.kind, 'recorded');
  assert.equal(recorded.run.seed.job_id, 'job-y');
  assert.equal(retiredBuilds(recorded.run, 'seed').length, 1);
});

const serverError = (message) => Object.assign(new Error(message), { status: 500 });

test("two interleaved generations: an old request's answer never closes or fills the newer one", () => {
  // Generation 0's request is in flight when another tab starts a new build.
  const closedGen0 = { ...start(), submitUnknown: { seed: 'unknown outcome' }, keyClosed: { seed: { jobId: 'job-x' } } };
  const gen1 = withNewBuildKey(closedGen0, 'seed');
  // Generation 0 answers the closed-key 409 after generation 1 was sent: the
  // newer generation is not closed, and the answer is a note on the old attempt.
  const late409 = submitFailureText('seed', conflict('unknown outcome', false, [
    { key: 'reserved_at', value: '2026-11-03T05:00:00Z' },
    { key: 'job_id', value: 'job-x' },
  ]));
  const afterLate409 = applySubmitFailure(gen1, ids.runId, 'seed', 0, late409);
  assert.equal(afterLate409.current, false);
  assert.equal(buildKeyClosed(afterLate409.run, 'seed'), null);
  assert.deepEqual(afterLate409.run.submitUnknown, {});
  assert.equal(canBuild(afterLate409.run, 'seed'), true);
  assert.deepEqual(retiredBuilds(afterLate409.run, 'seed'), [{ generation: 0, jobId: 'job-x', notes: [late409.lateNote] }]);
  assert.match(late409.lateNote, /answered that its outcome is unknown \(job job-x\)/);
  // Generation 1 fails with an unknown outcome: that one is the run's.
  const gen1Unknown = applySubmitFailure(afterLate409.run, ids.runId, 'seed', 1, submitFailureText('seed', serverError('synthetic 502')));
  assert.equal(gen1Unknown.current, true);
  assert.match(gen1Unknown.run.submitUnknown.seed, /synthetic 502/);
  // Generation 0 then answers with a job: noted on the retired attempt, not
  // recorded as the run's build, and generation 1 stays open to retry.
  const late = acceptJob(gen1Unknown.run, ids.runId, 'seed', job('pending', null, 'job-old'), 0);
  assert.equal(late.kind, 'obsolete');
  assert.equal(late.run.seed, null);
  assert.match(late.run.submitUnknown.seed, /synthetic 502/);
  const [attempt] = retiredBuilds(late.run, 'seed');
  assert.equal(attempt.jobId, 'job-x');
  assert.match(attempt.notes.at(-1), /answered with job job-old/);
  assert.equal(jobOutcomeNotice('seed', 'job-old', late), null);
  // Generation 1's own job is the run's build.
  const current = acceptJob(late.run, ids.runId, 'seed', job('pending', null, 'job-new'), 1);
  assert.equal(current.kind, 'recorded');
  assert.equal(current.run.seed.job_id, 'job-new');
  assert.equal(current.run.submitUnknown.seed, undefined);
});

test('a retired attempt keeps its unknown outcome and job: later answers of its key are only noted', () => {
  // Generation 0's request may have started; another tab got the
  // non-retryable 409 naming its job and started generation 1.
  const gen1 = withNewBuildKey({ ...start(), keyClosed: { seed: { jobId: 'job-x' } } }, 'seed');
  const before = retiredBuilds(gen1, 'seed')[0];
  // A pending generation-0 retry is then refused after an authorization
  // change. The API checks that before it replays the key, so this says
  // nothing about the first request.
  const forbidden = submitFailureText('seed', Object.assign(new Error('synthetic forbidden'), { status: 403 }));
  const refused = applySubmitFailure(gen1, ids.runId, 'seed', 0, forbidden);
  assert.equal(refused.current, false);
  assert.deepEqual(retiredBuilds(refused.run, 'seed'), [{ ...before, notes: ['a later request of this attempt was refused (403): synthetic forbidden'] }]);
  // Further answers add notes, once each, and never change the job.
  const failed = submitFailureText('seed', new TypeError('synthetic network error'));
  const again = applySubmitFailure(applySubmitFailure(refused.run, ids.runId, 'seed', 0, failed).run, ids.runId, 'seed', 0, failed);
  const named = acceptJob(again.run, ids.runId, 'seed', job('pending', null, 'job-x'), 0);
  assert.deepEqual(retiredBuilds(named.run, 'seed'), [{
    generation: 0,
    jobId: 'job-x',
    notes: [
      'a later request of this attempt was refused (403): synthetic forbidden',
      'a later request of this attempt failed (no response): synthetic network error',
      'a later request of this attempt answered with job job-x',
    ],
  }]);
  // A generation that was never retired here takes nothing.
  assert.deepEqual(retiredBuilds(applySubmitFailure(named.run, ids.runId, 'seed', 5, forbidden).run, 'seed'), retiredBuilds(named.run, 'seed'));
  // The current attempt's refusal records nothing; the tab shows it.
  assert.deepEqual(applySubmitFailure(gen1, ids.runId, 'seed', 1, submitFailureText('seed', Object.assign(new Error('x'), { status: 400 }))), { run: null, current: true });
  // A dismissed run takes nothing.
  assert.deepEqual(applySubmitFailure(gen1, 'run-0', 'seed', 0, submitFailureText('seed', serverError('x'))), { run: null, current: true });
});

test('a stored run keeps its key generations and closed keys, and runs stored before them still load', () => {
  const run = {
    ...start(),
    keyGeneration: { seed: 2 },
    keyClosed: { 'code-runtime': { jobId: null } },
    retiredBuilds: { seed: [{ generation: 0, jobId: 'job-x', notes: [] }, { generation: 1, jobId: null, notes: ['synthetic note'] }] },
  };
  const at = Date.parse(run.startedAt);
  assert.deepEqual(parseStoredRun(JSON.stringify(run), ids, at + 1000), run);
  assert.deepEqual(parseStoredRun(JSON.stringify(start()), ids, at + 1000), start());
  for (const bad of [
    { ...start(), keyGeneration: { seed: -1 } },
    { ...start(), keyGeneration: { other: 1 } },
    { ...start(), keyClosed: { seed: { jobId: 7 } } },
    { ...start(), keyClosed: { seed: 'job-x' } },
    { ...start(), retiredBuilds: { seed: { generation: 0 } } },
    { ...start(), retiredBuilds: { seed: [{ generation: 0, jobId: '', notes: [] }] } },
    { ...start(), retiredBuilds: { seed: [{ generation: 0, jobId: null, notes: [7] }] } },
    // The shape before retired attempts were immutable.
    { ...start(), retiredBuilds: { seed: [{ generation: 0, jobId: null, outcome: 'not-started', detail: null }] } },
  ]) {
    assert.equal(parseStoredRun(JSON.stringify(bad), ids, at + 1000), null, JSON.stringify(bad));
  }
});
