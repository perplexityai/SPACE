import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  networkPolicyFromDetailed,
  networkPolicyFromMode,
  networkPoliciesEqual,
  tlsInterceptionMode,
} from './network-policy.ts';

const LEGACY_POLICY = {
  version: 'v1',
  egress: { default_action: 'deny' },
};

test('TLS interception defaults legacy policy documents to off', () => {
  assert.equal(tlsInterceptionMode(LEGACY_POLICY), 'off');
});

test('TLS interception uses the policy document mode', () => {
  assert.equal(tlsInterceptionMode({ ...LEGACY_POLICY, tls_interception: 'on' }), 'on');
});

test('policy presets preserve loaded TLS interception mode', () => {
  const loadedPolicy = { ...LEGACY_POLICY, tls_interception: 'on' };

  for (const [mode, defaultAction] of [['open', 'allow'], ['closed', 'deny']]) {
    assert.deepEqual(networkPolicyFromMode(mode, loadedPolicy), {
      version: 'v1',
      egress: { default_action: defaultAction, allow: [], deny: [] },
      tls_interception: 'on',
    });
  }
});

test('detailed policy edits preserve loaded TLS interception mode', () => {
  const loadedPolicy = { ...LEGACY_POLICY, tls_interception: 'on' };
  const editedRules = [
    { matchKind: 'domain', pattern: 'api.example.com', decision: 'allow' },
    { matchKind: 'cidr', pattern: '10.20.0.0/16', decision: 'deny' },
  ];

  assert.deepEqual(networkPolicyFromDetailed('deny', editedRules, loadedPolicy), {
    version: 'v1',
    egress: {
      default_action: 'deny',
      allow: [{ domain: 'api.example.com' }],
      deny: [{ cidr: '10.20.0.0/16' }],
    },
    tls_interception: 'on',
  });
});

test('generated policies explicitly default missing TLS interception mode to off', () => {
  assert.equal(networkPolicyFromMode('open', LEGACY_POLICY).tls_interception, 'off');
  assert.equal(networkPolicyFromDetailed('deny', [], LEGACY_POLICY).tls_interception, 'off');
});

test('policy equality includes effective TLS interception mode', () => {
  const explicitOffPolicy = { ...LEGACY_POLICY, tls_interception: 'off' };
  const onPolicy = { ...LEGACY_POLICY, tls_interception: 'on' };

  assert.equal(networkPoliciesEqual(LEGACY_POLICY, explicitOffPolicy), true);
  assert.equal(networkPoliciesEqual(explicitOffPolicy, onPolicy), false);
});

test('policy edits preserve TLS exemptions and equality detects their removal', () => {
  const loaded = {
    ...LEGACY_POLICY,
    tls_interception: 'on',
    tls_interception_exemptions: [{ domain: 'sensor.example.test' }, { domain: 'push.example.test', port: 5228 }],
  };
  for (const edited of [networkPolicyFromMode('open', loaded), networkPolicyFromDetailed('deny', [], loaded)]) {
    assert.deepEqual(edited.tls_interception_exemptions, loaded.tls_interception_exemptions);
  }
  assert.equal(networkPoliciesEqual(loaded, { ...loaded, tls_interception_exemptions: [] }), false);
  assert.equal(
    networkPoliciesEqual(loaded, {
      ...loaded,
      tls_interception_exemptions: [{ domain: 'sensor.example.test' }, { domain: 'push.example.test', port: 443 }],
    }),
    false,
  );
  assert.equal(networkPoliciesEqual(LEGACY_POLICY, { ...LEGACY_POLICY, tls_interception_exemptions: [] }), true);
});
