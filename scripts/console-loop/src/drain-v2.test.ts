// Unit tests for the v2 drain steps' helpers that need no browser or stack:
// `npm test`.
import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  DRAIN_V2_SWITCH, V2_CONTROLLER_KEYS, envAssignments, envLiterals, fakeInstanceOf, fakeProviderId, leaseFree, leaseName, withProviderId,
} from './drain-v2.ts';
import {
  bareSandboxId, drainCountsOf, drainStageOf, panelMismatches, rowMismatches, stalledSince, type ApiDrain, type PanelRead,
} from './nodes.ts';

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
  // A run writes only the terminator; Kind's values render the switch.
  assert.deepEqual(V2_CONTROLLER_KEYS, ['SPACE_NODE_DRAIN_TERMINATOR_KIND']);
  assert.deepEqual(envLiterals([{ name: 'SPACE_NODE_DRAIN_TERMINATOR_KIND', value: 'kube-delete' }, { name: 'OTHER', value: 'x' }], [DRAIN_V2_SWITCH, ...V2_CONTROLLER_KEYS]), {
    SPACE_DRAIN_V2_ENABLED: null,
    SPACE_NODE_DRAIN_TERMINATOR_KIND: 'kube-delete',
  });
  assert.deepEqual(envLiterals(undefined, V2_CONTROLLER_KEYS), { SPACE_NODE_DRAIN_TERMINATOR_KIND: null });
  assert.throws(() => envLiterals([{ name: 'SPACE_DRAIN_V2_ENABLED', valueFrom: { configMapKeyRef: {} } }], [DRAIN_V2_SWITCH]), /valueFrom/);
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
const D = '01a1f49c-ceb1-7f00-8a00-000000000001';
const pub = (id: string) => `sbx_${id.replaceAll('-', '')}`;
const drain = (over: Partial<ApiDrain> = {}): ApiDrain => ({
  generation: 1_763_553_600_000_000, deadline: at(120_000), cancelling: false, fenced: false, reported_at: at(-3_000), evacuated_grew_at: at(-30_000),
  evacuated_total: [],
  evacuated: [{ sandbox_id: C, artifact_kind: 'disk', was_running: false, recorded: true }],
  pending: [
    { sandbox_id: A, wait: 'hold', running: true, pool_member: false, reason: null, since: at(-60_000) },
    { sandbox_id: B, wait: 'hold', running: true, pool_member: false, reason: null, since: null },
  ],
  blocked: [],
  ...over,
});
const panel = (over: Partial<PanelRead> = {}): PanelRead => ({
  tags: ['deadline in 2m 0s'],
  text: 'Drain progress deadline in 2m 0s 1/3 evacuated',
  pending: [
    { sandbox: pub(A), pool: false, wait: 'hold', running: true, state: 'running until the deadline in this state since 12:00 (1m 0s ago)' },
    { sandbox: pub(B), pool: false, wait: 'hold', running: true, state: 'running until the deadline' },
  ],
  evacuated: [{ sandbox: pub(C), artifact: 'disk', wasRunning: '—', controller: 'recorded' }],
  ...over,
});

test('the stage follows the design: cancelling, finished, then either side of the deadline', () => {
  assert.equal(drainStageOf(drain(), NOW), 'holding');
  assert.equal(drainStageOf(drain({ deadline: at(-1) }), NOW), 'evacuating');
  assert.equal(drainStageOf(drain({ deadline: null }), NOW), 'evacuating');
  assert.equal(drainStageOf(drain({ pending: [] }), NOW), 'finished', 'nothing pending or blocked is finished');
  assert.equal(drainStageOf(drain({ pending: [], blocked: [{ reason: 'x' }] }), NOW), 'holding', 'a blocker keeps it unfinished');
  assert.equal(drainStageOf(drain({ pending: [], reported_at: null }), NOW), 'holding', 'no report yet is not finished');
  assert.equal(drainStageOf(drain({ pending: [], evacuated: [{ sandbox_id: C, artifact_kind: 'disk', was_running: false, recorded: false }] }), NOW), 'holding', 'an evacuation still recording is not finished');
  assert.equal(drainStageOf(drain({ cancelling: true, pending: [] }), NOW), 'cancelling');
});

test('the counts are evacuated_total with the report, each sandbox once', () => {
  assert.deepEqual(drainCountsOf(drain()), { evacuated: 1, recording: 0, pending: 2, held: 2, poolMembers: 0, total: 3, blocked: 0 });
  // A released sandbox drops out of the report and stays in evacuated_total.
  const released = drainCountsOf(drain({ evacuated_total: [D, C], pending: [] }));
  assert.equal(released.evacuated, 2);
  assert.equal(released.total, 2);
  const pooled = drainCountsOf(drain({ pending: [{ sandbox_id: A, wait: 'moving', running: false, pool_member: true, reason: null, since: null }] }));
  assert.equal(pooled.poolMembers, 1);
  assert.equal(pooled.held, 0);
});

test('a drain is stalled only past its deadline, unfinished, the bound after its last growth', () => {
  assert.equal(stalledSince(drain({ deadline: at(-1), evacuated_grew_at: at(-16 * 60_000) }), NOW), NOW - 60_000);
  assert.equal(stalledSince(drain({ evacuated_grew_at: at(-16 * 60_000) }), NOW), null, 'before the deadline');
  assert.equal(stalledSince(drain({ deadline: at(-1), evacuated_grew_at: at(-14 * 60_000) }), NOW), null, 'inside the bound');
  assert.equal(stalledSince(drain({ deadline: at(-1), evacuated_grew_at: at(-16 * 60_000), pending: [] }), NOW), null, 'finished');
  assert.equal(stalledSince(drain({ deadline: at(-1), evacuated_grew_at: at(-16 * 60_000), cancelling: true }), NOW), null, 'cancelling');
});

test('a panel that matches the report has no mismatches; each difference is named', () => {
  assert.deepEqual(panelMismatches(panel(), drain(), NOW), []);
  assert.equal(bareSandboxId(pub(A)), bareSandboxId(A));
  const wrong = panelMismatches(panel({
    tags: ['past the deadline: the node suspends the sandboxes it held', 'instance fenced for termination'],
    text: '0/0 evacuated',
    pending: [{ sandbox: pub(A), pool: true, wait: 'hold', running: false, state: '' }],
    evacuated: [{ sandbox: pub(C), artifact: 'snapshot', wasRunning: 'yes', controller: 'recording' }],
  }), drain(), NOW);
  for (const what of [/headline/, /fence tag/, /progress/, /pending table has 1 rows/, /no pending row for .*ce93/, /not marked running/, /labelled a warm-pool member/, /running until the deadline/, /since when/, /artifact "snapshot"/, /was running "yes"/, /"recording"/]) {
    assert.ok(wrong.some((m) => what.test(m)), `${what} in ${wrong.join(' | ')}`);
  }
  const stalled = drain({ deadline: at(-60_000), evacuated_grew_at: at(-20 * 60_000) });
  assert.ok(panelMismatches(panel({ tags: ['past the deadline: the node suspends the sandboxes it held'] }), stalled, NOW).some((m) => /stall tag is missing/.test(m)));
  assert.ok(panelMismatches(panel({ tags: ['deadline in 2m 0s', 'stalled since 11:45'] }), drain(), NOW).some((m) => /stall tag is shown/.test(m)));
  const pooled = drain({ pending: [{ sandbox_id: A, wait: 'moving', running: false, pool_member: true, reason: 'its eviction from the pool', since: null }] });
  const pooledPanel = panel({ text: '1/2 evacuated Pending 1 (1 warm-pool)', pending: [{ sandbox: pub(A), pool: true, wait: 'moving', running: false, state: 'its eviction from the pool' }] });
  assert.deepEqual(panelMismatches(pooledPanel, pooled, NOW), []);
  assert.ok(panelMismatches({ ...pooledPanel, text: '1/2 evacuated' }, pooled, NOW).some((m) => /warm-pool members/.test(m)));
  assert.ok(panelMismatches({ ...pooledPanel, pending: [{ ...pooledPanel.pending[0], state: '' }] }, pooled, NOW).some((m) => /does not show the node's reason/.test(m)));
});

test('the finished and cancelling headlines', () => {
  const finished = drain({ pending: [] });
  assert.deepEqual(panelMismatches(panel({ tags: ['Finished: everything on the node is durable elsewhere'], text: '1/1 evacuated', pending: [] }), finished, NOW), []);
  assert.ok(panelMismatches(panel({ tags: ['deadline in 2m 0s'], text: '1/1 evacuated', pending: [] }), finished, NOW).some((m) => /headline/.test(m)));
  assert.deepEqual(panelMismatches(panel({ tags: ['Undrained: cancelling the drain on the node'] }), drain({ cancelling: true }), NOW).filter((m) => /headline/.test(m)), []);
});

test('near the deadline either side of it reads right', () => {
  const past = ['past the deadline: the node suspends the sandboxes it held'];
  assert.deepEqual(panelMismatches(panel({ tags: past }), drain({ deadline: at(2_000) }), NOW), []);
  assert.deepEqual(panelMismatches(panel({ tags: ['deadline in 1s'] }), drain({ deadline: at(-2_000) }), NOW).filter((m) => /headline/.test(m)), []);
});

test("the sandboxes' rows must agree with what the report says of them", () => {
  const statuses = new Map([[bareSandboxId(A), 'started'], [bareSandboxId(B), 'started'], [bareSandboxId(C), 'stopped']]);
  assert.deepEqual(rowMismatches(drain(), statuses), []);
  const off = rowMismatches(drain({ evacuated: [{ sandbox_id: A, artifact_kind: 'full', was_running: true, recorded: true }] }), statuses);
  assert.equal(off.length, 1);
  assert.match(off[0], /evacuated \(full\) but its row is started/);
  assert.match(rowMismatches(drain(), new Map([[bareSandboxId(A), 'suspended']]))[0], /held but its row is suspended/);
  assert.deepEqual(rowMismatches(drain({ pending: [{ sandbox_id: A, wait: 'moving', running: true, pool_member: false, reason: null, since: null }] }), new Map([[bareSandboxId(A), 'started']])), [], 'only a hold must still be started');
});
