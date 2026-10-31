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

// Text for the compact network-policy tag in sandbox and template tables:
// what the policy does, and whether the node has applied its latest version.
export function networkPolicyTagText(
  policyLabel: string | null | undefined,
  desired: number,
  applied: number | null | undefined,
): { label: string; meaning: string; sync: string } {
  const meaning =
    policyLabel === 'closed'
      ? 'Egress: deny all.'
      : policyLabel === 'open'
        ? 'Egress: allow all.'
        : policyLabel === 'custom'
          ? 'Egress: custom allow and deny rules.'
          : 'Egress policy unknown.';
  const sync =
    applied === null || applied === undefined || applied === 0
      ? `Version ${desired} is not yet reported as applied by the node.`
      : applied < desired
        ? `The node applies version ${applied}; version ${desired} is pending.`
        : applied > desired
          ? `The node reports version ${applied}, newer than the desired version ${desired}.`
          : `Version ${desired}, applied by the node.`;
  const label = policyLabel ? `${policyLabel} · v${desired}` : `v${desired}`;
  return { label, meaning, sync };
}
