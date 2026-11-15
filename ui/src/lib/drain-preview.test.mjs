import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  PREVIEW_ALL_CONCURRENCY,
  firstReasons,
  isWait,
  pendingCounts,
  previewFailure,
  previewPath,
  previewRow,
  previewVerdict,
  refusalsText,
  runBounded,
  sealText,
  verdictText,
} from './drain-preview.ts';

const NODE = '0b4f5c1e-7d2a-4c9b-9f31-2a6e8d0c5b17';

const preview = (over = {}) => ({
  node_id: NODE,
  sealed_epoch: null,
  done: false,
  evacuated: [],
  pending: [],
  blocked: [],
  ...over,
});

test('a preview path names the node', () => {
  assert.equal(previewPath(NODE), `/v1/nodes/${NODE}/drain-v2-preview`);
});

// Every pending action the planner emits (`DrainPending.action` in
// proto/sandbox/manager/v1/manager.proto): what clears as a drain runs (its
// own work, its window's pacing and an activation admitted before the seal),
// and the waits that can turn into blockers, `settle:` over every status the
// planner leaves to settle.
const CLEARS = ['suspend', 'publish_disk', 'upload', 'queued', 'activation'];
const WAITS = [
  'disk',
  'warm_pool_eviction',
  ...[
    'creating', 'created', 'starting', 'stopping', 'deleting', 'pausing', 'resuming',
    'checkpointing', 'suspending', 'restoring', 'cleanup_required',
  ].map((state) => `settle:${state}`),
];

test('only what can turn into a blocker is a wait', () => {
  for (const action of CLEARS) assert.equal(isWait(action), false, action);
  for (const action of WAITS) assert.equal(isWait(action), true, action);
  assert.equal(isWait('something_newer'), true, 'an action this console does not know waits');
});

test('one wait makes a drain wait, beside any amount of what clears', () => {
  for (const action of WAITS) {
    const waiting = preview({
      pending: [...CLEARS.map((work) => ({ sandbox_id: work, action: work })), { sandbox_id: 'w', action }],
    });
    assert.equal(previewVerdict(waiting), 'wait', action);
    assert.equal(verdictText(waiting), 'A drain would wait on one item that can turn into a blocker', action);
  }
  const two = preview({ pending: [{ sandbox_id: 'a', action: 'disk' }, { sandbox_id: 'b', action: 'settle:created' }] });
  assert.equal(verdictText(two), 'A drain would wait on 2 items that can turn into blockers');
  const blocked = preview({
    pending: [{ sandbox_id: 'a', action: 'disk' }],
    blocked: [{ sandbox_id: 'b', reason: 'quarantined' }],
  });
  assert.equal(previewVerdict(blocked), 'block', 'a blocker outranks a wait');
});

test('any blocker blocks; otherwise a drain would finish, at once only with nothing pending', () => {
  const blocked = preview({ blocked: [{ sandbox_id: null, reason: 'orphan inventory scan failed: x' }] });
  assert.equal(previewVerdict(blocked), 'block');
  assert.equal(verdictText(blocked), 'A drain would block: 1 blocker');
  const both = preview({
    pending: [{ sandbox_id: 'a', action: 'suspend' }],
    blocked: [{ sandbox_id: 'b', reason: 'quarantined' }, { sandbox_id: 'c', reason: 'quarantined' }],
  });
  assert.equal(previewVerdict(both), 'block');
  assert.equal(verdictText(both), 'A drain would block: 2 blockers');

  const pending = preview({ pending: CLEARS.map((action) => ({ sandbox_id: action, action })) });
  assert.equal(previewVerdict(pending), 'finish');
  assert.equal(verdictText(pending), 'A drain would finish once its own work and the activations in flight are done: 5 items');
  for (const action of ['queued', 'activation']) {
    const alone = preview({ pending: [{ sandbox_id: 'a', action }] });
    assert.equal(previewVerdict(alone), 'finish', action);
    assert.equal(verdictText(alone), 'A drain would finish once its own work and the activations in flight are done: one item', action);
  }
  assert.equal(previewVerdict(preview({ done: true })), 'finish');
  assert.equal(verdictText(preview({ done: true })), 'A drain would finish at once');
});

test('what a drain would do is counted by action, most common first', () => {
  assert.deepEqual(
    pendingCounts([
      { action: 'publish_disk' },
      { action: 'suspend' },
      { action: 'suspend' },
      { action: 'activation' },
    ]),
    [['suspend', 2], ['activation', 1], ['publish_disk', 1]],
  );
  assert.deepEqual(pendingCounts([]), []);
});

test('the first reasons name the sandbox when there is one', () => {
  const blocked = [
    { sandbox_id: null, reason: 'orphan inventory scan failed: CacheDirs: denied' },
    { sandbox_id: 'sbx-a', reason: 'quarantined' },
    { sandbox_id: 'sbx-b', reason: 'cleanup failed' },
  ];
  assert.deepEqual(firstReasons(blocked), ['orphan inventory scan failed: CacheDirs: denied', 'sbx-a: quarantined']);
  assert.equal(firstReasons(blocked, 3).length, 3);
});

test('the seal says whether a drain already holds the node', () => {
  assert.match(sealText(null), /^Not sealed/);
  assert.match(sealText(7), /^Sealed under epoch 7/);
});

const apiError = (status, body, message = 'failed') => Object.assign(new Error(message), { status, body });

test('a failed preview is told apart by the reason the API gives', () => {
  assert.equal(previewFailure(apiError(502, { reason: 'node_unreachable' }, 'node unreachable: refused')).kind, 'unreachable');
  const older = previewFailure(apiError(501, { reason: 'node_version_unsupported' }, 'too old'));
  assert.equal(older.kind, 'unsupported');
  assert.match(older.message, /^too old\. Upgrade the node's sandbox-manager and node-api/);
  assert.deepEqual(previewFailure(apiError(502, { reason: 'node_preview_failed' }, 'drain preview failed: x')), {
    kind: 'error',
    message: 'drain preview failed: x',
    refusals: [],
  });
  assert.deepEqual(previewFailure(apiError(503, { reason: 'database_timeout' }, 'the preview did not finish')), {
    kind: 'error',
    message: 'the preview did not finish',
    refusals: [],
  });
  assert.deepEqual(previewFailure(apiError(404, { error: 'Node not found' }, 'Node not found')), { kind: 'error', message: 'Node not found', refusals: [] });
  assert.deepEqual(previewFailure(new TypeError('fetch failed')), { kind: 'error', message: 'fetch failed', refusals: [] });
  assert.deepEqual(previewFailure(null), { kind: 'error', message: 'Preview failed', refusals: [] });
});

// The two refusals a Direct node gets (`drain_v2_node_refusals` in
// routes/admin/nodes.rs), where `:drain` answers with the first.
const REFUSALS = [
  'drain engine v2 drains Kubernetes nodes only',
  "drain engine v2 needs the node's EC2 instance recorded",
];

test("a failed preview keeps every refusal the API reports, whatever the node's answer", () => {
  for (const [status, reason, kind] of [
    [502, 'node_unreachable', 'unreachable'],
    [501, 'node_version_unsupported', 'unsupported'],
    [502, 'node_preview_failed', 'error'],
  ]) {
    const failure = previewFailure(apiError(status, { reason, refusals: REFUSALS }, 'x'));
    assert.equal(failure.kind, kind, reason);
    assert.deepEqual(failure.refusals, REFUSALS, reason);
  }
  assert.deepEqual(previewFailure(apiError(502, { reason: 'node_unreachable', refusals: 'not a list' })).refusals, []);
  assert.equal(refusalsText([]), null);
  assert.equal(
    refusalsText(REFUSALS),
    `Whatever the node answers, :drain would refuse a v2 drain of it: ${REFUSALS[0]}; ${REFUSALS[1]}`,
  );
});

test('a results row carries the verdict, the blocker count and the first reasons', () => {
  const blocked = previewRow(
    { id: NODE, name: 'worker-a' },
    {
      ok: true,
      value: preview({
        blocked: [
          { sandbox_id: 'sbx-a', reason: 'quarantined' },
          { sandbox_id: null, reason: 'orphan inventory scan failed: x' },
          { sandbox_id: 'sbx-c', reason: 'cleanup failed' },
        ],
        pending: [{ sandbox_id: 'b', action: 'suspend' }],
      }),
    },
  );
  assert.deepEqual(blocked, {
    nodeId: NODE,
    name: 'worker-a',
    verdict: 'block',
    blockers: 3,
    reasons: ['sbx-a: quarantined', 'orphan inventory scan failed: x'],
    pending: 1,
    failure: null,
  });
  const finish = previewRow({ id: NODE, name: null }, { ok: true, value: preview({ done: true }) });
  assert.equal(finish.name, NODE);
  assert.equal(finish.verdict, 'finish');
  const wait = previewRow(
    { id: NODE, name: 'worker-a' },
    { ok: true, value: preview({ pending: [{ sandbox_id: 'a', action: 'warm_pool_eviction' }] }) },
  );
  assert.equal(wait.verdict, 'wait');
  const failed = previewRow({ id: NODE, name: 'worker-a' }, { ok: false, error: apiError(502, { reason: 'node_unreachable' }, 'node unreachable: x') });
  assert.equal(failed.verdict, 'error');
  assert.equal(failed.failure.kind, 'unreachable');
  assert.deepEqual(failed.reasons, ['node unreachable: x']);
  const refused = previewRow(
    { id: NODE, name: 'worker-a' },
    { ok: false, error: apiError(502, { reason: 'node_unreachable', refusals: REFUSALS }, 'node unreachable: x') },
  );
  assert.deepEqual(refused.reasons, ['node unreachable: x', refusalsText(REFUSALS)]);
});

test('at most the bound run at once, every item runs, and results keep their order', async () => {
  let inFlight = 0;
  let most = 0;
  const progress = [];
  const items = Array.from({ length: 11 }, (_, i) => i);
  const results = await runBounded(
    items,
    PREVIEW_ALL_CONCURRENCY,
    async (i) => {
      inFlight += 1;
      most = Math.max(most, inFlight);
      await new Promise((resolve) => setTimeout(resolve, (i % 3) * 2));
      inFlight -= 1;
      if (i === 5) throw new Error('no answer');
      return i * 10;
    },
    (_index, _result, settled) => progress.push(settled),
  );
  assert.equal(PREVIEW_ALL_CONCURRENCY, 4);
  assert.equal(most, 4);
  assert.deepEqual(progress, items.map((i) => i + 1));
  assert.deepEqual(results.map((r) => (r.ok ? r.value : r.error.message)), [0, 10, 20, 30, 40, 'no answer', 60, 70, 80, 90, 100]);
});

test('once aborted, no further item starts', async () => {
  const controller = new AbortController();
  const started = [];
  const results = await runBounded(
    [0, 1, 2, 3, 4, 5],
    2,
    async (i) => {
      started.push(i);
      if (i === 1) controller.abort();
      return i;
    },
    undefined,
    controller.signal,
  );
  assert.deepEqual(started, [0, 1]);
  assert.deepEqual(results.map((r) => r.ok), [true, true, false, false, false, false]);
  assert.deepEqual(await runBounded([], 4, async () => 1), []);
});
