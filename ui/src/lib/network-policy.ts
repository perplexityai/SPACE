import type {
  NetworkPolicyAction,
  NetworkPolicyDestinationV1,
  NetworkPolicyTlsInterception,
  NetworkPolicyV1,
} from './types';

export type NetworkPolicyMode = 'open' | 'closed';

export interface NetworkPolicyDraftRule {
  matchKind: 'domain' | 'wildcard_domain' | 'ip' | 'cidr';
  pattern: string;
  decision: NetworkPolicyAction;
}

export function tlsInterceptionMode(
  policy?: NetworkPolicyV1 | null,
): NetworkPolicyTlsInterception {
  return policy?.tls_interception ?? 'off';
}

export function networkPolicyFromMode(
  mode: NetworkPolicyMode,
  loadedPolicy?: NetworkPolicyV1 | null,
): NetworkPolicyV1 {
  return {
    version: 'v1',
    egress: {
      default_action: mode === 'open' ? 'allow' : 'deny',
      allow: [],
      deny: [],
    },
    tls_interception: tlsInterceptionMode(loadedPolicy),
    ...(loadedPolicy?.network_credential_rules?.length
      ? { network_credential_rules: loadedPolicy.network_credential_rules }
      : {}),
    ...(loadedPolicy?.tls_interception_exemptions?.length
      ? { tls_interception_exemptions: loadedPolicy.tls_interception_exemptions }
      : {}),
  };
}

export function networkPolicyFromDetailed(
  defaultAction: NetworkPolicyAction,
  rules: NetworkPolicyDraftRule[],
  loadedPolicy?: NetworkPolicyV1 | null,
): NetworkPolicyV1 {
  return {
    version: 'v1',
    egress: {
      default_action: defaultAction,
      allow: rules.filter((rule) => rule.decision === 'allow').map(destinationFromRule),
      deny: rules.filter((rule) => rule.decision === 'deny').map(destinationFromRule),
    },
    tls_interception: tlsInterceptionMode(loadedPolicy),
    ...(loadedPolicy?.network_credential_rules?.length
      ? { network_credential_rules: loadedPolicy.network_credential_rules }
      : {}),
    ...(loadedPolicy?.tls_interception_exemptions?.length
      ? { tls_interception_exemptions: loadedPolicy.tls_interception_exemptions }
      : {}),
  };
}

export function networkPoliciesEqual(
  left?: NetworkPolicyV1 | null,
  right?: NetworkPolicyV1 | null,
): boolean {
  return JSON.stringify(normalizedNetworkPolicy(left)) === JSON.stringify(normalizedNetworkPolicy(right));
}

function normalizedNetworkPolicy(policy?: NetworkPolicyV1 | null): NetworkPolicyV1 | null {
  if (!policy) return null;
  return {
    version: 'v1',
    egress: {
      default_action: policy.egress.default_action,
      allow: policy.egress.allow ?? [],
      deny: policy.egress.deny ?? [],
    },
    tls_interception: tlsInterceptionMode(policy),
    network_credential_rules: policy.network_credential_rules ?? [],
    tls_interception_exemptions: policy.tls_interception_exemptions ?? [],
  };
}

function destinationFromRule(rule: NetworkPolicyDraftRule): NetworkPolicyDestinationV1 {
  const pattern = rule.pattern.trim();
  switch (rule.matchKind) {
    case 'ip':
      return { ip: pattern };
    case 'cidr':
      return { cidr: pattern };
    case 'wildcard_domain':
      return { wildcard_domain: pattern };
    case 'domain':
      return pattern.startsWith('*.') ? { wildcard_domain: pattern } : { domain: pattern };
  }
}
