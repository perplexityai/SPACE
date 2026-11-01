// Unit tests for the parts of the loop that need no browser or stack:
// `npm test`.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { SECRET_PLACEHOLDER, withoutSecret } from './browser.ts';
import { loadConfig } from './config.ts';
import { isSelected, parseSelection } from './loop.ts';
import { normalizer, redact } from './recorder.ts';
import { readSeedState, seedStatePath, upsertCreated, writeSeedState, type SeedState } from './seed.ts';
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
