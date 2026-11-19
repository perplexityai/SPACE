// Unit tests for the v2 drain steps' helpers that need no browser or stack:
// `npm test`.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  V2_CONTROLLER_KEYS, envAssignments, envLiterals, fakeInstanceOf, fakeProviderId, leaseFree, leaseName, withProviderId,
} from './drain-v2.ts';
import { bareSandboxId, drainStageOf, panelMismatches, rowMismatches, type ApiDrain, type PanelRead } from './nodes.ts';

test('a fake providerID names a fake instance, and only it does', () => {
  const { providerId, instanceId } = fakeProviderId();
  assert.match(instanceId, /^i-[0-9a-f]{17}$/);
  assert.equal(fakeInstanceOf(providerId), instanceId);
  assert.equal(fakeInstanceOf('kind://docker/space-local/space-local-worker2'), null);
  assert.equal(fakeInstanceOf('aws:///us-east-1a/i-0123456789abcdef0'), null, 'only the Kind fake zone');
});

test('the kubelet flags get the providerID exactly once', () => {
  const flags = 'KUBELET_KUBEADM_ARGS="--container-runtime-endpoint=unix:///run/containerd/containerd.sock --provider-id=kind://docker/space-local/space-local-worker2 --node-ip=172.18.0.7"\n';
  const { providerId } = fakeProviderId();
  assert.equal(withProviderId(flags, providerId), flags.replace('kind://docker/space-local/space-local-worker2', providerId));
  assert.throws(() => withProviderId('KUBELET_KUBEADM_ARGS=""', providerId), /exactly once/);
  assert.throws(() => withProviderId(`${flags} --provider-id=x`, providerId), /exactly once/);
  assert.throws(() => withProviderId(flags, 'aws:///us-east-1a/i-0123456789abcdef0'), /refusing providerID/);
});

test('the controller env is read as literals, and a valueFrom is refused', () => {
  assert.deepEqual(envLiterals([{ name: 'SPACE_NODE_DRAIN_TERMINATOR_KIND', value: 'kube-delete' }, { name: 'OTHER', value: 'x' }], V2_CONTROLLER_KEYS), {
    SPACE_DRAIN_V2_ENABLED: null,
    SPACE_NODE_DRAIN_TERMINATOR_KIND: 'kube-delete',
  });
  assert.deepEqual(envLiterals(undefined, V2_CONTROLLER_KEYS), { SPACE_DRAIN_V2_ENABLED: null, SPACE_NODE_DRAIN_TERMINATOR_KIND: null });
  assert.throws(() => envLiterals([{ name: 'SPACE_DRAIN_V2_ENABLED', valueFrom: { configMapKeyRef: {} } }], V2_CONTROLLER_KEYS), /valueFrom/);
  assert.deepEqual(envAssignments({ SPACE_DRAIN_V2_ENABLED: null, SPACE_NODE_DRAIN_TERMINATOR_KIND: 'aws' }), ['SPACE_DRAIN_V2_ENABLED-', 'SPACE_NODE_DRAIN_TERMINATOR_KIND=aws']);
});

test("a Lease is free without a holder, after its term, or when this host's loop that held it is gone", () => {
  const now = Date.parse('2026-11-19T12:00:00Z');
  const held = { holderIdentity: 'uid=1;pid=2;host=h;token=t', leaseDurationSeconds: 3600, renewTime: '2026-11-19T11:30:00.000000Z' };
  assert.equal(leaseFree({}, now), true);
  assert.equal(leaseFree({ ...held, holderIdentity: '' }, now), true);
  assert.equal(leaseFree(held, now), false, 'a smoke holding it');
  assert.equal(leaseFree({ ...held, renewTime: '2026-11-19T10:59:59.000000Z' }, now), true, 'its term ran out');
  assert.equal(leaseFree({ ...held, leaseDurationSeconds: 0 }, now), false, 'an unsafe duration is never taken');
  const loop = { ...held, holderIdentity: 'console-loop;uid=1000;pid=4242;host=box;token=t' };
  assert.equal(leaseFree(loop, now, 'box', () => false), true, 'an interrupted loop on this host');
  assert.equal(leaseFree(loop, now, 'box', () => true), false, 'a loop still running');
  assert.equal(leaseFree(loop, now, 'other', () => false), false, 'a loop on another host');
  assert.equal(leaseName('retention', 'kind-space-local'), leaseName('retention', 'kind-space-local'));
  assert.match(leaseName('cordon', 'kind-space-local'), /^space-smoke-cordon-[0-9a-f]{16}$/);
});

const NOW = Date.parse('2026-11-19T12:00:00Z');
const at = (ms: number) => new Date(NOW + ms).toISOString();
const A = '01a1f49c-ce87-7734-b2a3-c03bb5562cce';
const B = '01a1f49c-ce93-7167-ac1c-8929a3125540';
const C = '01a1f49c-cea9-71bb-adda-66d8e258999a';
const pub = (id: string) => `sbx_${id.replaceAll('-', '')}`;
const drain = (over: Partial<ApiDrain> = {}): ApiDrain => ({
  epoch: 1, deadline: at(120_000), sealed: true, cancelling: false, done: false, reported_at: at(-3_000), stalled_since: null,
  evacuated: [{ sandbox_id: C, artifact_kind: 'disk', was_running: false, recorded: true }],
  pending: [
    { sandbox_id: A, action: 'hold', reason: null, since: at(-60_000) },
    { sandbox_id: B, action: 'hold', reason: null, since: null },
  ],
  blocked: [],
  ...over,
});
const panel = (over: Partial<PanelRead> = {}): PanelRead => ({
  tags: ['deadline in 2m 0s', 'Node sealed'],
  text: 'Drain progress deadline in 2m 0s Node sealed 1/3 evacuated',
  pending: [
    { sandbox: pub(A), action: 'hold', state: 'running until the deadline in this state since 12:00 (1m 0s ago)' },
    { sandbox: pub(B), action: 'hold', state: 'running until the deadline' },
  ],
  evacuated: [{ sandbox: pub(C), artifact: 'disk', wasRunning: '—', controller: 'recorded' }],
  ...over,
});

test('the stage follows the design: cancelling, done, waiting for a slot, then the deadline', () => {
  assert.equal(drainStageOf(drain(), NOW), 'holding');
  assert.equal(drainStageOf(drain({ deadline: at(-1) }), NOW), 'past_deadline');
  assert.equal(drainStageOf(drain({ deadline: at(-1), sealed: false }), NOW), 'unacknowledged');
  assert.equal(drainStageOf(drain({ waiting_for_slot: true }), NOW), 'waiting_for_slot');
  assert.equal(drainStageOf(drain({ done: true, waiting_for_slot: true }), NOW), 'done');
  assert.equal(drainStageOf(drain({ cancelling: true, done: true }), NOW), 'cancelling');
  assert.equal(drainStageOf(drain({ deadline: null }), NOW), 'past_deadline');
});

test('a panel that matches the report has no mismatches; each difference is named', () => {
  assert.deepEqual(panelMismatches(panel(), drain(), NOW), []);
  assert.equal(bareSandboxId(pub(A)), bareSandboxId(A));
  const wrong = panelMismatches(panel({
    tags: ['past the deadline', 'Not yet acknowledged'],
    text: '0/0 evacuated',
    pending: [{ sandbox: pub(A), action: 'hold', state: '' }],
    evacuated: [{ sandbox: pub(C), artifact: 'snapshot', wasRunning: 'yes', controller: 'recording' }],
  }), drain(), NOW);
  for (const what of [/headline/, /seal tag/, /progress/, /pending table has 1 rows/, /no pending row for .*ce93/, /running until the deadline/, /since when/, /artifact "snapshot"/, /was running "yes"/, /"recording"/]) {
    assert.ok(wrong.some((m) => what.test(m)), `${what} in ${wrong.join(' | ')}`);
  }
  assert.ok(panelMismatches(panel(), drain({ stalled_since: at(-60_000) }), NOW).some((m) => /stall tag is missing/.test(m)));
});

test('near the deadline either side of it reads right', () => {
  assert.deepEqual(panelMismatches(panel({ tags: ['past the deadline', 'Node sealed'] }), drain({ deadline: at(2_000) }), NOW), []);
  assert.deepEqual(panelMismatches(panel({ tags: ['deadline in 1s', 'Node sealed'] }), drain({ deadline: at(-2_000) }), NOW).filter((m) => /headline/.test(m)), []);
});

test("the sandboxes' rows must agree with what the report says of them", () => {
  const statuses = new Map([[bareSandboxId(A), 'started'], [bareSandboxId(B), 'started'], [bareSandboxId(C), 'stopped']]);
  assert.deepEqual(rowMismatches(drain(), statuses), []);
  const off = rowMismatches(drain({ evacuated: [{ sandbox_id: A, artifact_kind: 'full', was_running: true, recorded: true }] }), statuses);
  assert.equal(off.length, 1);
  assert.match(off[0], /evacuated \(full\) but its row is started/);
  assert.match(rowMismatches(drain(), new Map([[bareSandboxId(A), 'suspended']]))[0], /held but its row is suspended/);
});
