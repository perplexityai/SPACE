// Unit tests for the parts of the loop that need no browser or stack:
// `npm test`.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { ApiError, type Api } from './api.ts';
import { SECRET_PLACEHOLDER, carriesSecret, withoutSecret } from './browser.ts';
import { loadConfig } from './config.ts';
import { isSelected, parseSelection } from './loop.ts';
import { normalizer, redact } from './recorder.ts';
import {
  clean, cleanSandbox, isSeededOrg, readSeedState, seedStatePath, upsertCreated, writeSeedState, type CreatedObject, type SeedState,
} from './seed.ts';
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
