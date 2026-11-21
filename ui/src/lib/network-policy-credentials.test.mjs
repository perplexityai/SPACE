import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  networkPoliciesEqual,
  networkPolicyFromDetailed,
  networkPolicyFromMode,
} from './network-policy.ts';

const credentialRule = {
  rule_id: 'example-service',
  destination: { exact_host: 'api.example.com', port: 443, path_prefix: '/v1/' },
  credential_header_bindings: [{
    header_name: 'Authorization',
    credential_reference_id: '00000000-0000-7000-8000-000000000001',
  }],
  supported_http_methods: ['GET', 'POST'],
};

const policy = {
  version: 'v1',
  egress: { default_action: 'deny', allow: [], deny: [] },
  tls_interception: 'on',
  network_credential_rules: [credentialRule],
};

test('egress edits preserve credential injection rules in every designer mode', () => {
  for (const edited of [
    networkPolicyFromMode('open', policy),
    networkPolicyFromMode('closed', policy),
    networkPolicyFromDetailed('deny', [
      { matchKind: 'domain', pattern: 'api.example.com', decision: 'allow' },
    ], policy),
  ]) {
    assert.deepEqual(edited.network_credential_rules, [credentialRule]);
  }
});

test('policy equality detects credential changes and normalizes absent rules', () => {
  assert.equal(networkPoliciesEqual(policy, { ...policy, network_credential_rules: [] }), false);
  assert.equal(networkPoliciesEqual(policy, {
    ...policy,
    network_credential_rules: [{ ...credentialRule, supported_http_methods: ['GET'] }],
  }), false);
  const { network_credential_rules, ...withoutRules } = policy;
  assert.equal(networkPoliciesEqual(withoutRules, {
    ...withoutRules, network_credential_rules: [],
  }), true);
});
