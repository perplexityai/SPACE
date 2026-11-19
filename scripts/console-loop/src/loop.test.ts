// Unit tests for the parts of the loop that need no browser or stack:
// `npm test`.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { ApiError, type Api } from './api.ts';
import { SECRET_PLACEHOLDER, carriesSecret, clickTarget, loopbackAddresses, withoutSecret } from './browser.ts';
import { loadConfig } from './config.ts';
import { isSelected, parseSelection } from './loop.ts';
import { normalizer, redact } from './recorder.ts';
import { ffmpegArgs, gifCuts, gifFile } from './gifs.ts';
import {
  clean, cleanSandbox, isSeededOrg, readSeedState, seedStatePath, upsertCreated, writeSeedState, type CreatedObject, type SeedState,
} from './seed.ts';
import {
  CONSOLE_PREVIEW_CONCURRENCY, CONSOLE_PREVIEW_TIMEOUT_MS, PREVIEW_WAIT_MARGIN_MS, everyPage, previewAllWaitMs, previewResultsMismatches,
  previewVerdictIn, previewWaitMs, resultVerdictIn, type ResultVerdict,
} from './nodes.ts';
import { sameStack } from './stack.ts';

const STEPS = [
  { name: 'login', tags: ['auth'] },
  { name: 'nodes', tags: ['nodes'] },
  { name: 'sandboxes-list', tags: ['sandboxes'] },
  { name: 'sandbox-pause', tags: ['sandboxes', 'flows', 'lifecycle'] },
];
const selected = (only?: string, skip?: string) => STEPS
  .filter((s) => isSelected(s, parseSelection(only, STEPS, 'only'), parseSelection(skip, STEPS, 'skip')))
  .map((s) => s.name);

test('--only and --skip select by exact step name or tag', () => {
  assert.deepEqual(selected(), ['login', 'nodes', 'sandboxes-list', 'sandbox-pause']);
  assert.deepEqual(selected('sandboxes,nodes'), ['nodes', 'sandboxes-list', 'sandbox-pause']);
  assert.deepEqual(selected('sandboxes', 'lifecycle'), ['sandboxes-list']);
  assert.deepEqual(selected(undefined, 'sandbox-pause, nodes'), ['login', 'sandboxes-list']);
});

test('--only and --skip refuse what is not a step name or tag', () => {
  assert.throws(() => parseSelection('sandboxes|nodes', STEPS, 'only'), /no step or tag is named sandboxes\|nodes/);
  assert.throws(() => parseSelection('sandbox', STEPS, 'only'), /no step or tag is named sandbox$/);
  assert.throws(() => parseSelection('.*', STEPS, 'skip'), /--skip: no step or tag/);
  assert.throws(() => parseSelection(',', STEPS, 'only'), /names no step or tag/);
});

test('previewVerdictIn reads the one verdict a preview names', () => {
  assert.equal(previewVerdictIn('would finish'), 'finish');
  assert.equal(previewVerdictIn('A drain would finish at once'), 'finish');
  assert.equal(previewVerdictIn("would wait A drain would wait on 2 items that can turn into blockers"), 'wait');
  assert.equal(previewVerdictIn('would block A drain would block: 1 blocker'), 'block');
  assert.equal(previewVerdictIn('worker-a would wait 0 none'), 'wait');
  assert.equal(previewVerdictIn('worker-a error unreachable'), null);
  assert.equal(previewVerdictIn('would finish would block'), null, 'two verdicts are none');
  assert.equal(previewVerdictIn('would waiting'), null);
});

test("the preview steps' budget is the console's", async () => {
  // Loaded at run time: `ui/` is not a module package, so the typecheck
  // can't follow a static import of it.
  const consoleModule = '../../../ui/src/lib/drain-preview.ts';
  const ui = await import(consoleModule);
  assert.equal(CONSOLE_PREVIEW_TIMEOUT_MS, ui.DRAIN_PREVIEW_TIMEOUT_MS);
  assert.equal(CONSOLE_PREVIEW_CONCURRENCY, ui.PREVIEW_ALL_CONCURRENCY);
});

test("the preview steps outwait the console's own wait for each round of previews", () => {
  assert.equal(previewWaitMs(), CONSOLE_PREVIEW_TIMEOUT_MS + PREVIEW_WAIT_MARGIN_MS);
  assert.ok(PREVIEW_WAIT_MARGIN_MS > 0);
  const rounds: [number, number][] = [
    [0, 0], [1, 1], [CONSOLE_PREVIEW_CONCURRENCY, 1], [CONSOLE_PREVIEW_CONCURRENCY + 1, 2], [3 * CONSOLE_PREVIEW_CONCURRENCY, 3],
  ];
  for (const [workers, n] of rounds) {
    assert.equal(previewAllWaitMs(workers), n * CONSOLE_PREVIEW_TIMEOUT_MS + PREVIEW_WAIT_MARGIN_MS, `${workers} workers`);
  }
});

// `rows` as a results table pages them, `pageSize` to a page: `read` gives
// the page shown, `next` turns to the following one.
function pagedTable(rows: { nodeId: string | null; verdict: string }[], pageSize: number) {
  let page = 0;
  const pages = Math.max(1, Math.ceil(rows.length / pageSize));
  return {
    read: async () => rows.slice(page * pageSize, (page + 1) * pageSize),
    next: async () => (page + 1 < pages ? (page += 1, true) : false),
  };
}

const VERDICT_CELL: Record<ResultVerdict, string> = {
  finish: 'would finish', wait: 'would wait', block: 'would block', error: 'error unreachable',
};
const workerIds = (n: number) => Array.from({ length: n }, (_, i) => `node-${String(i).padStart(3, '0')}`);
const verdictFor = (i: number): ResultVerdict => (['finish', 'wait', 'block', 'error'] as const)[i % 4];

test("the bulk preview check reads every page of the results, past the console's 50 a page", async () => {
  const ids = workerIds(120);
  const shown = ids.map((nodeId, i) => ({ nodeId, verdict: VERDICT_CELL[verdictFor(i)] }));
  const table = pagedTable(shown, 50);
  const { rows, pages, complete } = await everyPage(table.read, table.next, ids.length + 1);
  assert.equal(pages, 3);
  assert.ok(complete);
  assert.deepEqual(rows, shown);
  const expected = new Map(ids.map((id, i) => [id, verdictFor(i)] as const));
  assert.deepEqual(previewResultsMismatches(expected, rows), []);
  // The first page alone, which is all a single read sees, misses 70 workers.
  const firstPage = previewResultsMismatches(expected, shown.slice(0, 50));
  assert.equal(firstPage.length, 70);
  assert.ok(firstPage.includes('node-050 has no row on any page'), firstPage[0]);
});

test('everyPage stops at its bound and says the table is not complete', async () => {
  let turned = 0;
  const { pages, complete } = await everyPage(async () => [1], async () => (turned += 1, true), 4);
  assert.equal(pages, 4);
  assert.equal(complete, false);
});

test('a results row is checked against its own preview, and every worker has exactly one row', () => {
  const expected = new Map<string, ResultVerdict | null>([
    ['node-a', 'block'], ['node-b', 'finish'], ['node-c', 'error'], ['node-d', null],
  ]);
  assert.deepEqual(previewResultsMismatches(expected, [
    { nodeId: 'node-a', verdict: 'would block' },
    { nodeId: 'node-b', verdict: 'would finish' },
    { nodeId: 'node-c', verdict: 'error unreachable' },
  ]), ['node-d has no row on any page']);
  assert.deepEqual(previewResultsMismatches(expected, [
    { nodeId: 'node-a', verdict: 'would finish' },
    { nodeId: 'node-b', verdict: 'would finish' },
    { nodeId: 'node-b', verdict: 'would finish' },
    { nodeId: 'node-c', verdict: 'would wait' },
    { nodeId: 'node-d', verdict: 'would finish' },
    { nodeId: 'node-e', verdict: 'would finish' },
    { nodeId: null, verdict: 'would block' },
  ]), [
    'node-a shows finish, but its preview answered block',
    'node-c shows wait, but its preview answered error',
    'node-d has a row but the page asked no preview of it',
    'node-e has a row but is not a listed worker',
    'a row links to no node (would block)',
    'node-b has 2 rows',
  ]);
  assert.equal(resultVerdictIn('error unreachable'), 'error');
  assert.equal(resultVerdictIn('would wait'), 'wait');
  assert.equal(resultVerdictIn('no verdict'), null);
});

test('normalizer replaces seeded names, IDs and numbers', () => {
  const normalize = normalizer('loop');
  assert.equal(normalize('GET /orgs/loop-acme/users'), 'GET /orgs/<seeded>/users');
  assert.equal(normalize('sandbox xloop-run-k3 failed'), 'sandbox x<seeded> failed');
  assert.equal(normalize('loop- and loop stay'), 'loop- and loop stay');
  assert.equal(normalize('took 1500 ms on 0193ab12-3c4d-7e8f-9a0b-1c2d3e4f5a6b'), 'took <n> ms on <uuid>');
});

test('withoutSecret replaces the secret and redacts anything key-shaped', () => {
  const secret = 'ab'.repeat(32);
  const result = withoutSecret(JSON.stringify({ api_key: { id: 'k1', name: `copy ${'cd'.repeat(32)}` }, secret }));
  assert.equal(result.replaced, true);
  assert.equal(result.id, 'k1');
  assert.equal(JSON.parse(result.body).secret, SECRET_PLACEHOLDER);
  assert.doesNotMatch(result.body, /[0-9a-f]{64}/);
  const other = withoutSecret(`not json ${secret}`);
  assert.equal(other.replaced, false);
  assert.equal(other.body, 'not json <redacted>');
  assert.equal(withoutSecret('{"api_key":{"id":"k2"}}').replaced, false);
  const nested = withoutSecret(JSON.stringify({ data: [{ secret: 'live-value' }] }));
  assert.equal(nested.replaced, true);
  assert.deepEqual(JSON.parse(nested.body), { data: [{ secret: SECRET_PLACEHOLDER }] });
  assert.equal(redact(`x ${secret} y`), 'x <redacted> y');
});

test('the seed state is per prefix, and a state for another prefix is refused', () => {
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'console-loop-test-'));
  try {
    const loop = loadConfig(['--out', out, '--prefix', 'loop']);
    const other = loadConfig(['--out', out, '--prefix', 'other']);
    assert.equal(path.basename(seedStatePath(loop)), 'seed-loop.json');
    const state: SeedState = {
      prefix: 'loop', stack: { clusterId: 'c1', clusterCreatedAt: 't1' }, managingOrgId: 'm', userPassword: 'p',
      withSandboxes: false, orgs: {}, users: {}, apiKeys: {}, sandboxes: {}, created: [], notes: [],
    };
    writeSeedState(loop, state);
    assert.equal(readSeedState(other), null);
    assert.equal(readSeedState(loop)?.prefix, 'loop');
    fs.copyFileSync(seedStatePath(loop), seedStatePath(other));
    assert.throws(() => readSeedState(other), /records prefix "loop", not --prefix "other"/);
    const { stack: _stack, ...withoutStack } = state;
    fs.writeFileSync(seedStatePath(loop), JSON.stringify(withoutStack));
    assert.throws(() => readSeedState(loop), /does not record the stack/);
  } finally {
    fs.rmSync(out, { recursive: true, force: true });
  }
});

test('a prefix is limited to what is safe in names and file names', () => {
  assert.throws(() => loadConfig(['--prefix', '../x']), /--prefix must be/);
  assert.throws(() => loadConfig(['--prefix', 'Loop']), /--prefix must be/);
  assert.equal(loadConfig(['--prefix', 'pr-12']).prefix, 'pr-12');
});

test('the Datadog logs link is configured when the variable is set and not blank', () => {
  const saved = process.env.SPACE_UI_DATADOG_LOGS_URL;
  try {
    for (const [value, configured] of [[undefined, false], ['', false], ['  ', false], ['https://logs.example.test/logs', true]] as const) {
      if (value === undefined) delete process.env.SPACE_UI_DATADOG_LOGS_URL;
      else process.env.SPACE_UI_DATADOG_LOGS_URL = value;
      assert.equal(loadConfig([]).datadogLogsConfigured, configured, `value ${JSON.stringify(value)}`);
    }
  } finally {
    if (saved === undefined) delete process.env.SPACE_UI_DATADOG_LOGS_URL;
    else process.env.SPACE_UI_DATADOG_LOGS_URL = saved;
  }
});

test('upsertCreated keeps one entry per kind and name, the latest', () => {
  const state = { created: [] } as unknown as SeedState;
  upsertCreated(state, { kind: 'sandbox', name: 'loop-running', id: 'sbx_1' });
  upsertCreated(state, { kind: 'user', name: 'loop-running', id: 'u1' });
  upsertCreated(state, { kind: 'sandbox', name: 'loop-running', id: 'sbx_2' });
  assert.deepEqual(state.created, [
    { kind: 'sandbox', name: 'loop-running', id: 'sbx_2' },
    { kind: 'user', name: 'loop-running', id: 'u1' },
  ]);
});

test('two readings are one stack only when the cluster ID and creation time match', () => {
  const a = { clusterId: 'c1', clusterCreatedAt: 't1' };
  assert.equal(sameStack(a, { ...a }), true);
  assert.equal(sameStack(a, { ...a, clusterId: 'c2' }), false);
  assert.equal(sameStack(a, { ...a, clusterCreatedAt: 't2' }), false);
});

test('carriesSecret finds a secret string anywhere in a JSON body', () => {
  assert.equal(carriesSecret('{"secret":"x"}'), true);
  assert.equal(carriesSecret('{"items":[{"inner":{"secret":"x"}}]}'), true);
  assert.equal(carriesSecret(`{"secret":"${SECRET_PLACEHOLDER}"}`), false);
  assert.equal(carriesSecret('{"secret":null,"name":"secret"}'), false);
  assert.equal(carriesSecret('secret: not json'), false);
});

test('only the seeded org slugs are kept by default', () => {
  assert.equal(isSeededOrg('loop', 'loop-acme'), true);
  assert.equal(isSeededOrg('loop', 'loop-globex-research'), true);
  assert.equal(isSeededOrg('loop', 'loop-run-mvxd0bs8'), false);
  assert.equal(isSeededOrg('other', 'loop-acme'), false);
});

// An API whose sandbox and operation reads answer in turn from `sandbox` and
// `operation` (null is a 404; the last answer repeats), and that logs every
// call. A force-delete is accepted and its operation succeeds.
function fakeApi(script: { sandbox: ({ status: string } | null)[]; operation: (string | null)[] }) {
  const calls: string[] = [];
  const next = <T>(answers: T[]): T => (answers.length > 1 ? answers.shift()! : answers[0]);
  const surface = (name: string) => async (method: string, path: string) => {
    calls.push(`${name} ${method} ${path}`);
    if (method === 'GET' && path === '/orgs') return [];
    if (method === 'POST' && path.endsWith(':force-delete')) return { operation_id: 'op-delete' };
    const answer = method === 'GET' && path.startsWith('/sandboxes/') ? next(script.sandbox)
      : method === 'GET' && path.startsWith('/operations/') ? next(script.operation)
      : undefined;
    if (answer === undefined) throw new Error(`unexpected ${method} ${path}`);
    if (answer === null) throw new ApiError(404, method, path, '{}');
    return typeof answer === 'string' ? { id: path.split('/').pop(), state: answer } : { id: 'sbx_1', name: 'loop-running', ...answer };
  };
  const api = {
    admin: surface('admin'),
    public: surface('public'),
    waitForOperation: async (id: string) => ({ id, state: 'succeeded' }),
  } as unknown as Api;
  return { api, calls };
}

const pending: CreatedObject = { kind: 'sandbox', name: 'loop-running', id: 'sbx_1', createOperation: { id: 'op-create', admin: false } };

test('clean waits for an accepted create to make its sandbox, then deletes it', async () => {
  const { api, calls } = fakeApi({ sandbox: [null, { status: 'started' }, { status: 'deleted' }], operation: ['queued', 'running', 'succeeded'] });
  assert.equal(await cleanSandbox(api, pending, { pollMs: 0 }), 'deleted');
  assert.deepEqual(calls, [
    'admin GET /sandboxes/sbx_1',
    'public GET /operations/op-create',
    'public GET /operations/op-create',
    'public GET /operations/op-create',
    'admin GET /sandboxes/sbx_1',
    'admin POST /sandboxes/sbx_1:force-delete',
    'admin GET /sandboxes/sbx_1',
  ]);
});

test('a 404 is gone once the create operation is finished or gone, or when none is recorded', async () => {
  const failed = fakeApi({ sandbox: [null], operation: ['failed'] });
  assert.equal(await cleanSandbox(failed.api, pending, { pollMs: 0 }), 'already gone');
  assert.equal(failed.calls.filter((call) => call.includes('/sandboxes/')).length, 2);
  const collected = fakeApi({ sandbox: [null], operation: [null] });
  assert.equal(await cleanSandbox(collected.api, pending, { pollMs: 0 }), 'already gone');
  const unrecorded = fakeApi({ sandbox: [null], operation: ['queued'] });
  const { createOperation: _op, ...withoutOperation } = pending;
  assert.equal(await cleanSandbox(unrecorded.api, withoutOperation, { pollMs: 0 }), 'already gone');
  assert.deepEqual(unrecorded.calls, ['admin GET /sandboxes/sbx_1']);
});

test('clean keeps a sandbox whose create is still pending at the bound, fails, and deletes nothing', async () => {
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'console-loop-test-'));
  try {
    const config = loadConfig(['--out', out]);
    const state: SeedState = {
      prefix: 'loop', stack: { clusterId: 'c1', clusterCreatedAt: 't1' }, managingOrgId: 'm', userPassword: 'p',
      withSandboxes: true, orgs: {}, users: {}, apiKeys: {}, sandboxes: { running: 'sbx_1' }, created: [pending], notes: [],
    };
    writeSeedState(config, state);
    const { api, calls } = fakeApi({ sandbox: [null], operation: ['queued'] });
    const log = console.log;
    console.log = () => {};
    try {
      assert.equal(await clean(api, config, state, { deleteOrgs: false, wait: { pollMs: 0, timeoutMs: 0 } }), 1);
    } finally {
      console.log = log;
    }
    assert.equal(calls.some((call) => call.includes('force-delete')), false);
    assert.deepEqual(readSeedState(config)?.created, [pending]);
    assert.deepEqual(readSeedState(config)?.sandboxes, { running: 'sbx_1' });
  } finally {
    fs.rmSync(out, { recursive: true, force: true });
  }
});

test('loopbackAddresses keeps loopback and turns every other IPv4 address into one', () => {
  assert.equal(
    loopbackAddresses('{"address":"172.18.0.4","node_api_endpoint":"10.0.0.12:9800","api":"127.0.0.1:8080"}'),
    '{"address":"127.0.0.4","node_api_endpoint":"127.0.0.12:9800","api":"127.0.0.1:8080"}',
  );
  assert.equal(loopbackAddresses('fc 1.15.1 · linux/x86_64'), 'fc 1.15.1 · linux/x86_64');
});

// The node page's and the nodes list's buttons as the console renders them:
// an icon's label leads each icon button's accessible name.
const NODE_PAGE_BUTTONS = [
  { text: 'Cordon', name: 'Cordon' },
  { text: 'Forced drain', name: 'Forced drain' },
  { text: 'Preview v2 drain', name: 'eye Preview v2 drain' },
  { text: 'Refresh', name: 'reload Refresh' },
];
const NODES_LIST_BUTTONS = [
  { text: 'Preview v2 drain on all workers', name: 'eye Preview v2 drain on all workers' },
  { text: 'Refresh', name: 'reload Refresh' },
];

test('clickTarget finds a button by its visible text when an icon label leads its name', () => {
  assert.equal(clickTarget(NODE_PAGE_BUTTONS, 'Preview v2 drain'), 2);
  assert.equal(clickTarget(NODE_PAGE_BUTTONS, '  Preview  v2 drain '), 2);
  assert.equal(clickTarget(NODE_PAGE_BUTTONS, 'eye Preview v2 drain'), 2, 'the whole accessible name still works');
  assert.equal(clickTarget(NODES_LIST_BUTTONS, 'Preview v2 drain on all workers'), 0);
  assert.equal(clickTarget(NODES_LIST_BUTTONS, 'Preview v2 drain'), null, 'a label never picks a longer button');
  assert.equal(clickTarget(NODE_PAGE_BUTTONS, 'Undrain'), null);
});

test('clickTarget refuses a label more than one button shows', () => {
  assert.throws(
    () => clickTarget([...NODE_PAGE_BUTTONS, { text: 'Refresh', name: 'Refresh' }], 'Refresh'),
    /--click "Refresh" matches 2 buttons: "Refresh", "Refresh"/,
  );
});

test("a run's GIFs are cut from each step's place in its video", () => {
  const results = [
    { index: 1, name: 'login', video: { fromMs: 0, toMs: 2_000 } },
    { index: 7, name: 'node-drain-progress', video: { fromMs: 10_000, toMs: 410_000 } },
    { index: 8, name: 'no-video' },
  ];
  const cuts = gifCuts(results, null, 40);
  assert.deepEqual(cuts.map((cut) => [cut.file, cut.fromS, cut.durationS, cut.speed]), [
    ['001-login.gif', 0, 2, 1],
    ['007-node-drain-progress.gif', 10, 400, 10],
  ]);
  assert.deepEqual(gifCuts(results, new Set(['login']), 40).map((cut) => cut.step), ['login']);
  assert.equal(gifFile(12, 'Node V2: drain'), '012-node-v2-drain.gif');
  const args = ffmpegArgs('run.webm', cuts[1], 'out.gif', { width: 1280, fps: 5 });
  assert.deepEqual(args.slice(0, 9), ['-loglevel', 'error', '-y', '-ss', '10.00', '-t', '400.00', '-i', 'run.webm']);
  assert.match(args[10], /^setpts=PTS\/10,fps=5,scale=1280:-1:flags=lanczos,split\[a\]\[b\];\[a\]palettegen/);
});
