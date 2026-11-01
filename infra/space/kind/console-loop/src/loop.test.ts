// Unit tests for the parts of the loop that need no browser or stack:
// `npm test`.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import type { Operation } from './api.ts';
import { SECRET_PLACEHOLDER, carriesSecret, withoutSecret } from './browser.ts';
import { loadConfig } from './config.ts';
import { isSelected, parseSelection } from './loop.ts';
import { normalizer, redact } from './recorder.ts';
import {
  isSeededOrg, nextCreate, readSeedState, rejectedBeforeAdmission, seedStatePath, settleOperations, unfoundCreate, upsertCreated,
  writeSeedState,
  type CreatedObject, type OperationRef, type SeedState,
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

test('upsertCreated merges by kind and name and keeps every operation', () => {
  const state = { created: [] } as unknown as SeedState;
  upsertCreated(state, { kind: 'sandbox', name: 'loop-running', idempotencyKey: 'k' });
  upsertCreated(state, { kind: 'sandbox', name: 'loop-running', id: 'sbx_1', operations: [{ id: 'op1', admin: false, action: 'create' }] });
  upsertCreated(state, { kind: 'sandbox', name: 'loop-running', operations: [{ id: 'op2', admin: true, action: 'delete' }, { id: 'op1', admin: false, action: 'create' }] });
  upsertCreated(state, { kind: 'user', name: 'loop-running' });
  assert.equal(state.created.length, 2);
  assert.deepEqual(state.created[0], {
    kind: 'sandbox', name: 'loop-running', idempotencyKey: 'k', id: 'sbx_1',
    operations: [{ id: 'op1', admin: false, action: 'create' }, { id: 'op2', admin: true, action: 'delete' }],
  });
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

test('settleOperations records outcomes, and a vanished operation as gone instead of failing', async () => {
  const states: Record<string, (Operation | null)[]> = {
    // Deleted by the operations GC: the API answers 404.
    'op-gc': [null],
    'op-run': [{ id: 'op-run', state: 'running' }, { id: 'op-run', state: 'succeeded', target_id: 'sbx_1' }],
    'op-old': [{ id: 'op-old', state: 'failed' }],
  };
  const reads: string[] = [];
  const read = async (op: OperationRef) => {
    reads.push(op.id);
    return states[op.id].length > 1 ? states[op.id].shift()! : states[op.id][0];
  };
  const entry: CreatedObject = {
    kind: 'sandbox', name: 'loop-running',
    operations: [
      { id: 'op-gc', admin: false, action: 'create' },
      { id: 'op-run', admin: true, action: 'delete' },
      { id: 'op-old', admin: true, action: 'pause', outcome: 'failed' },
    ],
  };
  let saves = 0;
  await settleOperations(read, entry, () => { saves += 1; }, { pollMs: 0 });
  assert.deepEqual(entry.operations, [
    { id: 'op-gc', admin: false, action: 'create', outcome: 'gone' },
    { id: 'op-run', admin: true, action: 'delete', outcome: 'succeeded', targetId: 'sbx_1' },
    { id: 'op-old', admin: true, action: 'pause', outcome: 'failed' },
  ]);
  // An outcome already recorded is not asked for again, now or later.
  assert.deepEqual(reads, ['op-gc', 'op-run', 'op-run']);
  assert.equal(saves, 2);
  await settleOperations(read, entry, () => { saves += 1; }, { pollMs: 0 });
  assert.equal(reads.length, 3);
});

test('nextCreate replays an unanswered create unchanged and starts fresh only after one resolved', () => {
  const recorded = { admin: false, body: { template: 'old:1', runtime: 'fc', name: 'loop-running' } };
  const fresh = () => ({ idempotencyKey: 'new-key', request: { admin: false, body: { template: 'new:2', runtime: 'fc', name: 'loop-running' } } });
  const unanswered: CreatedObject = { kind: 'sandbox', name: 'loop-running', idempotencyKey: 'old-key', request: recorded };
  assert.deepEqual(nextCreate(unanswered, fresh), { idempotencyKey: 'old-key', request: recorded, replay: true });
  const resolved: CreatedObject = { ...unanswered, operations: [{ id: 'op1', admin: false, action: 'create', outcome: 'gone' }] };
  assert.deepEqual(nextCreate(resolved, fresh), { ...fresh(), replay: false });
  assert.deepEqual(nextCreate(undefined, fresh), { ...fresh(), replay: false });
});

test('only the seeded org slugs are kept by default', () => {
  assert.equal(isSeededOrg('loop', 'loop-acme'), true);
  assert.equal(isSeededOrg('loop', 'loop-globex-research'), true);
  assert.equal(isSeededOrg('loop', 'loop-run-mvxd0bs8'), false);
  assert.equal(isSeededOrg('other', 'loop-acme'), false);
});

test('a create rejected before admission resolves: a corrected seed starts fresh, and clean drops it', () => {
  for (const status of [400, 401, 403, 415, 422, 429]) assert.equal(rejectedBeforeAdmission(status), true, `${status}`);
  for (const status of [202, 409, 500, 503]) assert.equal(rejectedBeforeAdmission(status), false, `${status}`);
  const bad = { admin: false, body: { template: '', runtime: 'fc', name: 'loop-running' } };
  const good = { admin: false, body: { template: 'seed:latest', runtime: 'fc', name: 'loop-running' } };
  const corrected = () => ({ idempotencyKey: 'key-2', request: good });
  // Seeded with an empty template: the API answered 422.
  const rejected: CreatedObject = { kind: 'sandbox', name: 'loop-running', orgId: 'm', idempotencyKey: 'key-1', request: bad, rejected: 422 };
  // The corrected seed makes a new create with its options and a new key.
  assert.deepEqual(nextCreate(rejected, corrected), { idempotencyKey: 'key-2', request: good, replay: false });
  // Clean, finding no sandbox, drops the rejected intent instead of replaying it.
  assert.equal(unfoundCreate(rejected), 'rejected');
  // A 409 records nothing: the create stays unresolved, is replayed exactly, and keeps its key.
  const conflicted: CreatedObject = { ...rejected, rejected: undefined };
  assert.deepEqual(nextCreate(conflicted, corrected), { idempotencyKey: 'key-1', request: bad, replay: true });
  assert.equal(unfoundCreate(conflicted), 'replay');
  // The same for a browser create: rejected is dropped, unanswered is kept.
  assert.equal(unfoundCreate({ kind: 'sandbox', name: 'loop-run-x', submitted: true, rejected: 400 }), 'rejected');
  assert.equal(unfoundCreate({ kind: 'sandbox', name: 'loop-run-x', submitted: true }), 'unresolved');
  assert.equal(unfoundCreate({ kind: 'sandbox', name: 'loop-run-x' }), 'never submitted');
});
