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
  verdictText,
} from './drain-preview.ts';

const NODE = '0b4f5c1e-7d2a-4c9b-9f31-2a6e8d0c5b17';

const preview = (over = {}) => ({
  node_id: NODE,
  evacuated: [],
  pending: [],
  blocked: [],
  ...over,
});

const pending = (sandbox_id, wait, running = false) => ({ sandbox_id, wait, running });

test('a preview path names the node', () => {
  assert.equal(previewPath(NODE), `/v1/nodes/${NODE}/drain-v2-preview`);
});

// Every `DrainWait` (proto/sandbox/manager/v1/manager.proto): a drain moves
// what it makes durable and what it holds; what it doesn't drive, and any
// wait this console doesn't know, can stall it.
test('only what a drain does not move is a wait', () => {
  for (const wait of ['moving', 'hold']) assert.equal(isWait(wait), false, wait);
  for (const wait of ['external', 'unspecified', 'something_newer']) assert.equal(isWait(wait), true, wait);
});

test('one wait makes a drain wait, beside any amount of what moves', () => {
  for (const wait of ['external', 'unspecified', 'something_newer']) {
    const waiting = preview({
      pending: [pending('a', 'moving', true), pending('b', 'hold', true), pending('w', wait)],
    });
    assert.equal(previewVerdict(waiting), 'wait', wait);
    assert.equal(verdictText(waiting), "A drain would wait on one sandbox it doesn't move itself", wait);
  }
  const two = preview({ pending: [pending('a', 'external'), pending('b', 'unspecified')] });
  assert.equal(verdictText(two), "A drain would wait on 2 sandboxes it doesn't move itself");
  const blocked = preview({ pending: [pending('a', 'external')], blocked: [{ reason: 'a node-wide problem' }] });
  assert.equal(previewVerdict(blocked), 'block', 'a blocker outranks a wait');
});

test('any blocker blocks; otherwise a drain would finish, at once only with nothing pending', () => {
  const blocked = preview({ blocked: [{ reason: 'one' }] });
  assert.equal(previewVerdict(blocked), 'block');
  assert.equal(verdictText(blocked), 'A drain would block: 1 blocker');
  const both = preview({ pending: [pending('a', 'moving')], blocked: [{ reason: 'one' }, { reason: 'two' }] });
  assert.equal(verdictText(both), 'A drain would block: 2 blockers');

  const moving = preview({ pending: [pending('a', 'moving', true), pending('b', 'moving'), pending('c', 'hold', true)] });
  assert.equal(previewVerdict(moving), 'finish');
  assert.equal(verdictText(moving), 'A drain would finish once it has made 3 sandboxes durable');
  assert.equal(verdictText(preview({ pending: [pending('a', 'moving')] })), 'A drain would finish once it has made one sandbox durable');
  assert.equal(previewVerdict(preview()), 'finish');
  assert.equal(verdictText(preview()), 'A drain would finish at once');
});

test('the pending sandboxes are counted by wait, most common first', () => {
  assert.deepEqual(
    pendingCounts([{ wait: 'moving' }, { wait: 'external' }, { wait: 'moving' }, { wait: 'hold' }]),
    [['moving', 2], ['external', 1], ['hold', 1]],
  );
  assert.deepEqual(pendingCounts([]), []);
});

test('the first reasons are the blockers the node gave', () => {
  const blocked = [{ reason: 'first' }, { reason: 'second' }, { reason: 'third' }];
  assert.deepEqual(firstReasons(blocked), ['first', 'second']);
  assert.equal(firstReasons(blocked, 3).length, 3);
});

const apiError = (status, body, message = 'failed') => Object.assign(new Error(message), { status, body });

test('a failed preview is told apart by the reason the API gives', () => {
  assert.equal(previewFailure(apiError(503, { reason: 'node_busy' }, 'busy')).kind, 'busy');
  assert.match(previewFailure(apiError(503, { reason: 'node_busy' }, 'busy')).message, /again in a few seconds/);
  assert.equal(previewFailure(apiError(409, { reason: 'drain_v2_in_progress' }, 'held')).kind, 'draining');
  assert.equal(previewFailure(apiError(409, { reason: 'orphaned_seal' }, 'sealed')).kind, 'orphaned_seal');
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
          { reason: 'first' },
          { reason: 'second' },
          { reason: 'third' },
        ],
        pending: [pending('b', 'moving', true)],
      }),
    },
  );
  assert.deepEqual(blocked, {
    nodeId: NODE,
    name: 'worker-a',
    verdict: 'block',
    blockers: 3,
    reasons: ['first', 'second'],
    pending: 1,
    failure: null,
  });
  const finish = previewRow({ id: NODE, name: null }, { ok: true, value: preview() });
  assert.equal(finish.name, NODE);
  assert.equal(finish.verdict, 'finish');
  const wait = previewRow(
    { id: NODE, name: 'worker-a' },
    { ok: true, value: preview({ pending: [pending('a', 'external')] }) },
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
