'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import type { ColumnsType } from 'antd/es/table';
import {
  Alert,
  App,
  Button,
  Divider,
  Input,
  Modal,
  Select,
  Segmented,
  Space,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import {
  ApiOutlined,
  BranchesOutlined,
  CodeOutlined,
  DeleteOutlined,
  ExperimentOutlined,
  LockOutlined,
  PlusOutlined,
  QuestionCircleOutlined,
  SafetyCertificateOutlined,
  UnlockOutlined,
} from '@ant-design/icons';
import type {
  NetworkPolicyAction,
  NetworkPolicyDestinationV1,
  NetworkPolicyV1,
} from '@/lib/types';
import {
  networkPolicyFromDetailed,
  networkPolicyFromMode,
  tlsInterceptionMode,
} from '@/lib/network-policy';
import type { NetworkPolicyMode } from '@/lib/network-policy';
export { networkPoliciesEqual } from '@/lib/network-policy';

const { Text } = Typography;
const { TextArea } = Input;

type DesignerMode = 'inherit' | NetworkPolicyMode | 'detailed';
type DraftDecision = 'allow' | 'deny';
type DraftMatchKind = 'domain' | 'wildcard_domain' | 'ip' | 'cidr';

interface DraftRule {
  id: string;
  matchKind: DraftMatchKind;
  pattern: string;
  decision: DraftDecision;
  script: string;
  notes: string;
}

interface RuleValidationError {
  ruleId: string;
  message: string;
}

interface NetworkPolicyDesignerProps {
  open: boolean;
  targetKind: 'sandbox' | 'template';
  targetName: string;
  templateName?: string | null;
  policy?: NetworkPolicyV1 | null;
  templatePolicy?: NetworkPolicyV1 | null;
  onClose: () => void;
  onSave: (policy: NetworkPolicyV1) => Promise<void>;
  onClear?: () => Promise<void>;
}

const DEFAULT_PREVIEW_CODE = `export async function decide(ctx) {
  if (ctx.destination.domain?.endsWith(".bitcoin.com")) {
    const response = await askUser({
      title: "Allow crypto-related network access?",
      body: \`\${ctx.process.command} wants to reach \${ctx.destination.host}\`,
      choices: ["allow once", "deny", "allow for sandbox"]
    });

    return response.choice === "allow once" ? "allow" : "deny";
  }

  return "default";
}`;

const INITIAL_DRAFT_RULES: DraftRule[] = [];

const PLATFORM_GUARDRAILS = [
  { label: 'EC2 IMDS', cidr: '169.254.169.254/32' },
  { label: 'Link-local infrastructure', cidr: '169.254.0.0/16 except 169.254.0.20/30' },
  { label: 'SPACE slots', cidr: '10.11.0.0/16' },
  { label: 'SPACE slots', cidr: '10.12.0.0/16' },
  { label: 'Guest loopback', cidr: '127.0.0.0/8' },
  { label: 'IPv6 link-local', cidr: 'fe80::/10' },
  { label: 'IPv6 loopback', cidr: '::1/128' },
];

function hasDetailedRules(policy?: NetworkPolicyV1 | null): boolean {
  return Boolean(policy?.egress?.allow?.length || policy?.egress?.deny?.length);
}

export function networkPolicyMode(policy?: NetworkPolicyV1 | null): NetworkPolicyMode {
  return policy?.egress?.default_action === 'allow' ? 'open' : 'closed';
}

export function networkPolicyColor(policy?: NetworkPolicyV1 | null): string {
  return networkPolicyMode(policy) === 'open' ? 'green' : 'default';
}

export function summarizeNetworkPolicy(policy?: NetworkPolicyV1 | null): string {
  if (!policy) return 'unknown';
  const mode = networkPolicyMode(policy);
  if (!hasDetailedRules(policy)) return mode;
  const allowCount = policy?.egress.allow?.length ?? 0;
  const denyCount = policy?.egress.deny?.length ?? 0;
  return `${mode}, ${allowCount} allow / ${denyCount} deny`;
}

export function compactNetworkPolicyLabel(policy?: NetworkPolicyV1 | null): string {
  if (!policy) return 'unknown';
  if (hasDetailedRules(policy)) return 'custom';
  return networkPolicyMode(policy);
}

function decisionLabel(decision: DraftDecision): string {
  switch (decision) {
    case 'allow':
      return 'Allow';
    case 'deny':
      return 'Deny';
  }
}

function decisionColor(decision: DraftDecision): string {
  switch (decision) {
    case 'allow':
      return 'green';
    case 'deny':
      return 'red';
  }
}

function modeIcon(mode: DesignerMode) {
  switch (mode) {
    case 'inherit':
      return <BranchesOutlined />;
    case 'open':
      return <UnlockOutlined />;
    case 'closed':
      return <LockOutlined />;
    case 'detailed':
      return <SafetyCertificateOutlined />;
  }
}

function actionFromPolicy(policy?: NetworkPolicyV1 | null): NetworkPolicyAction {
  return policy?.egress?.default_action === 'allow' ? 'allow' : 'deny';
}

function draftRulesFromPolicy(policy?: NetworkPolicyV1 | null): DraftRule[] {
  const rules: DraftRule[] = [];
  const addRules = (decision: DraftDecision, entries: NetworkPolicyDestinationV1[] | undefined) => {
    for (const [index, entry] of (entries ?? []).entries()) {
      const matchKind = destinationKind(entry);
      rules.push({
        id: `${decision}-${index}-${matchKind}`,
        matchKind,
        pattern: destinationPattern(entry),
        decision,
        script: '',
        notes: '',
      });
    }
  };
  addRules('allow', policy?.egress.allow);
  addRules('deny', policy?.egress.deny);
  return rules;
}

function destinationKind(entry: NetworkPolicyDestinationV1): DraftMatchKind {
  if ('ip' in entry) return 'ip';
  if ('cidr' in entry) return 'cidr';
  if ('wildcard_domain' in entry) return 'wildcard_domain';
  return 'domain';
}

function destinationPattern(entry: NetworkPolicyDestinationV1): string {
  if ('ip' in entry) return entry.ip;
  if ('cidr' in entry) return entry.cidr;
  if ('wildcard_domain' in entry) return entry.wildcard_domain;
  return entry.domain;
}

function isDomainMatchKind(matchKind: DraftMatchKind): boolean {
  return matchKind === 'domain' || matchKind === 'wildcard_domain';
}

function decisionOptions(matchKind: DraftMatchKind) {
  return [
    { value: 'allow', label: 'Allow' },
    {
      value: 'deny',
      label: isDomainMatchKind(matchKind) ? 'Deny (IP/CIDR only)' : 'Deny',
      disabled: isDomainMatchKind(matchKind),
    },
  ];
}

function isValidIpv4(value: string): boolean {
  const parts = value.split('.');
  return parts.length === 4 && parts.every((part) => {
    if (!/^\d{1,3}$/.test(part)) return false;
    const octet = Number(part);
    return Number.isInteger(octet) && octet >= 0 && octet <= 255;
  });
}

function isValidIpv6(value: string): boolean {
  if (!value.includes(':') || !/^[0-9a-f:.]+$/i.test(value) || value.includes(':::')) {
    return false;
  }
  const doubleColonCount = (value.match(/::/g) ?? []).length;
  if (doubleColonCount > 1) return false;

  const segments = value.split(':');
  const last = segments[segments.length - 1];
  const hasEmbeddedIpv4 = last.includes('.');
  if (hasEmbeddedIpv4 && !isValidIpv4(last)) return false;

  const hexSegments = hasEmbeddedIpv4 ? segments.slice(0, -1) : segments;
  if (hexSegments.some((segment) => segment.length > 4 || (segment && !/^[0-9a-f]+$/i.test(segment)))) {
    return false;
  }

  const nonEmptySegments = hexSegments.filter(Boolean).length + (hasEmbeddedIpv4 ? 2 : 0);
  return doubleColonCount === 1 ? nonEmptySegments < 8 : nonEmptySegments === 8;
}

function isValidIp(value: string): boolean {
  return isValidIpv4(value) || isValidIpv6(value);
}

function validDomainLabel(label: string): boolean {
  return (
    label.length > 0 &&
    label.length <= 63 &&
    /^[a-z0-9]/.test(label) &&
    /[a-z0-9]$/.test(label) &&
    /^[a-z0-9-]+$/.test(label)
  );
}

function normalizeDomainPattern(value: string): string {
  return value.trim().replace(/\.$/, '').toLowerCase();
}

function isValidDomain(value: string): boolean {
  const domain = normalizeDomainPattern(value);
  if (!domain || domain.length > 253 || domain.includes('*')) return false;
  const labels = domain.split('.');
  return labels.length >= 2 && labels.every(validDomainLabel);
}

function isValidWildcardDomain(value: string): boolean {
  const domain = normalizeDomainPattern(value);
  return domain.startsWith('*.') && isValidDomain(domain.slice(2));
}

function isValidCidr(value: string): boolean {
  const [ip, prefix, extra] = value.trim().split('/');
  if (!ip || prefix === undefined || extra !== undefined || !/^\d+$/.test(prefix)) return false;
  if (!isValidIp(ip)) return false;
  const prefixLength = Number(prefix);
  const maxPrefix = isValidIpv4(ip) ? 32 : 128;
  return Number.isInteger(prefixLength) && prefixLength >= 0 && prefixLength <= maxPrefix;
}

function validateRule(rule: DraftRule, index: number): RuleValidationError[] {
  const pattern = rule.pattern.trim();
  const label = `Rule ${index + 1}`;
  if (!pattern) {
    return [{ ruleId: rule.id, message: `${label}: match pattern is required` }];
  }
  if (isDomainMatchKind(rule.matchKind) && rule.decision === 'deny') {
    return [{ ruleId: rule.id, message: `${label}: domain deny rules are not supported in v1` }];
  }

  const invalid = (expected: string) => [{ ruleId: rule.id, message: `${label}: expected ${expected}` }];
  switch (rule.matchKind) {
    case 'ip':
      return isValidIp(pattern) ? [] : invalid('a valid IPv4 or IPv6 address');
    case 'cidr':
      return isValidCidr(pattern) ? [] : invalid('a valid IPv4 or IPv6 CIDR');
    case 'wildcard_domain':
      return isValidWildcardDomain(pattern) ? [] : invalid('a wildcard domain like *.example.com');
    case 'domain':
      if (pattern.startsWith('*.')) {
        return isValidWildcardDomain(pattern) ? [] : invalid('a wildcard domain like *.example.com');
      }
      return isValidDomain(pattern) ? [] : invalid('a domain like example.com');
  }
}

function validateRules(rules: DraftRule[]): RuleValidationError[] {
  return rules.flatMap(validateRule);
}

function draftRulesEqual(left: DraftRule[], right: DraftRule[]): boolean {
  return left.length === right.length && left.every((rule, index) => {
    const other = right[index];
    return (
      Boolean(other) &&
      rule.matchKind === other.matchKind &&
      rule.pattern === other.pattern &&
      rule.decision === other.decision &&
      rule.script === other.script &&
      rule.notes === other.notes
    );
  });
}

function destinationDecisionEntries(
  policy: NetworkPolicyV1,
  decision: DraftDecision,
): NetworkPolicyDestinationV1[] {
  return decision === 'allow' ? (policy.egress.allow ?? []) : (policy.egress.deny ?? []);
}

function destinationTagLabel(entry: NetworkPolicyDestinationV1): string {
  if ('ip' in entry) return `ip:${entry.ip}`;
  if ('cidr' in entry) return `cidr:${entry.cidr}`;
  if ('wildcard_domain' in entry) return `domain:${entry.wildcard_domain}`;
  return `domain:${entry.domain}`;
}

export function NetworkPolicyDetails({ policy }: { policy?: NetworkPolicyV1 | null }) {
  if (!policy) {
    return <Text type="secondary">unknown</Text>;
  }

  return (
    <Space direction="vertical" size={4} style={{ width: '100%' }}>
      <Space size={4} wrap>
        <Text type="secondary">default</Text>
        <Tag color={decisionColor(policy.egress.default_action)}>
          {decisionLabel(policy.egress.default_action)}
        </Tag>
      </Space>
      <Space size={4} wrap>
        <Text type="secondary">TLS interception</Text>
        <Tag>{tlsInterceptionMode(policy) === 'on' ? 'On' : 'Off'}</Tag>
      </Space>
      {(['allow', 'deny'] as DraftDecision[]).map((decision) => {
        const entries = destinationDecisionEntries(policy, decision);
        if (entries.length === 0) return null;

        return (
          <Space key={decision} size={4} wrap>
            <Text type="secondary">{decision}</Text>
            {entries.map((entry, index) => (
              <Tag
                key={`${decision}-${index}-${destinationTagLabel(entry)}`}
                color={decisionColor(decision)}
              >
                {destinationTagLabel(entry)}
              </Tag>
            ))}
          </Space>
        );
      })}
    </Space>
  );
}

function ruleMatches(rule: DraftRule, probe: string): boolean {
  const value = probe.trim().toLowerCase();
  if (!value) return false;
  const pattern = rule.pattern.trim().toLowerCase();
  if (pattern === 'any') return true;
  if (rule.matchKind === 'domain') {
    return value === pattern || value.includes(`://${pattern}`);
  }
  if (rule.matchKind === 'wildcard_domain') {
    const suffix = pattern.startsWith('*.') ? pattern.slice(1) : `.${pattern}`;
    return value.endsWith(suffix) && value !== suffix.slice(1);
  }
  return value.includes(pattern);
}

export default function NetworkPolicyDesigner({
  open,
  targetKind,
  targetName,
  templateName,
  policy,
  templatePolicy,
  onClose,
  onSave,
  onClear,
}: NetworkPolicyDesignerProps) {
  const { modal } = App.useApp();
  const inherited = targetKind === 'sandbox' && !policy;
  const fallbackPolicy = useMemo(() => networkPolicyFromMode('closed'), []);
  const effectivePolicy = useMemo(
    () => policy ?? templatePolicy ?? fallbackPolicy,
    [fallbackPolicy, policy, templatePolicy],
  );
  const [mode, setMode] = useState<DesignerMode>('closed');
  const [saving, setSaving] = useState(false);
  const [dnsAllowed, setDnsAllowed] = useState(true);
  const [defaultAction, setDefaultAction] = useState<NetworkPolicyAction>('deny');
  const [rules, setRules] = useState<DraftRule[]>(INITIAL_DRAFT_RULES);
  const [selectedRuleId, setSelectedRuleId] = useState('');
  const [probe, setProbe] = useState('https://wallet.bitcoin.com');
  const previousOpenRef = useRef(false);
  const previousTargetRef = useRef<string | null>(null);
  const initialPolicyRef = useRef<NetworkPolicyV1 | null>(null);

  useEffect(() => {
    const targetKey = `${targetKind}:${targetName}`;
    const wasOpen = previousOpenRef.current;
    const targetChanged = previousTargetRef.current !== targetKey;
    previousOpenRef.current = open;

    if (!open) {
      previousTargetRef.current = null;
      initialPolicyRef.current = null;
      return;
    }

    if (wasOpen && !targetChanged) return;

    previousTargetRef.current = targetKey;
    initialPolicyRef.current = effectivePolicy;
    setMode(inherited ? 'inherit' : hasDetailedRules(policy) ? 'detailed' : networkPolicyMode(policy));
    setDefaultAction(actionFromPolicy(effectivePolicy));
    setDnsAllowed(true);
    const nextRules = draftRulesFromPolicy(effectivePolicy);
    setRules(nextRules);
    setSelectedRuleId(nextRules[0]?.id ?? '');
    setProbe('https://api.github.com');
  }, [effectivePolicy, inherited, open, policy, targetKind, targetName]);

  const selectedRule = rules.find((rule) => rule.id === selectedRuleId) ?? rules[0];
  const validationErrors = useMemo(
    () => mode === 'detailed' ? validateRules(rules) : [],
    [mode, rules],
  );
  const selectedRuleErrors = selectedRule
    ? validationErrors.filter((error) => error.ruleId === selectedRule.id)
    : [];
  const matchedRule = useMemo(
    () => rules.find((rule) => ruleMatches(rule, probe)),
    [probe, rules],
  );
  const initialPolicy = initialPolicyRef.current ?? effectivePolicy;
  const detailedDraftChanged = useMemo(
    () => (
      defaultAction !== actionFromPolicy(initialPolicy) ||
      !draftRulesEqual(rules, draftRulesFromPolicy(initialPolicy))
    ),
    [defaultAction, initialPolicy, rules],
  );

  const options = targetKind === 'sandbox'
    ? [
        { label: 'Inherit template', value: 'inherit', icon: modeIcon('inherit') },
        { label: 'Open', value: 'open', icon: modeIcon('open') },
        { label: 'Closed', value: 'closed', icon: modeIcon('closed') },
        { label: 'Detailed', value: 'detailed', icon: modeIcon('detailed') },
      ]
    : [
        { label: 'Open', value: 'open', icon: modeIcon('open') },
        { label: 'Closed', value: 'closed', icon: modeIcon('closed') },
        { label: 'Detailed', value: 'detailed', icon: modeIcon('detailed') },
      ];

  const handleModeChange = (nextMode: DesignerMode) => {
    if (mode === 'detailed' && nextMode !== 'detailed' && detailedDraftChanged) {
      modal.confirm({
        title: 'Discard detailed rules?',
        content: 'Switching modes saves only the selected preset and drops detailed allow and deny rules.',
        okText: 'Discard',
        okType: 'danger',
        cancelText: 'Keep editing',
        onOk: () => setMode(nextMode),
      });
      return;
    }
    setMode(nextMode);
  };

  const updateRule = (patch: Partial<DraftRule>) => {
    if (!selectedRule) return;
    setRules((current) =>
      current.map((rule) => rule.id === selectedRule.id ? { ...rule, ...patch } : rule),
    );
  };

  const updateRuleMatchKind = (matchKind: DraftMatchKind) => {
    const patch: Partial<DraftRule> = { matchKind };
    if (isDomainMatchKind(matchKind) && selectedRule?.decision === 'deny') {
      patch.decision = 'allow';
    }
    updateRule(patch);
  };

  const addRule = () => {
    const id = `draft-${Date.now()}`;
    const next: DraftRule = {
      id,
      matchKind: 'domain',
      pattern: 'example.com',
      decision: 'allow',
      script: '',
      notes: '',
    };
    setRules((current) => [...current, next]);
    setSelectedRuleId(id);
  };

  const deleteRule = () => {
    if (!selectedRule) return;
    const remaining = rules.filter((rule) => rule.id !== selectedRule.id);
    setRules(remaining);
    setSelectedRuleId(remaining[0]?.id ?? '');
  };

  const handleSave = async () => {
    if (validationErrors.length > 0) return;
    setSaving(true);
    try {
      if (mode === 'inherit') {
        await onClear?.();
      } else if (mode === 'detailed') {
        await onSave(networkPolicyFromDetailed(defaultAction, rules, initialPolicy));
      } else {
        await onSave(networkPolicyFromMode(mode, initialPolicy));
      }
    } finally {
      setSaving(false);
    }
  };

  const columns: ColumnsType<DraftRule> = [
    {
      title: 'Match',
      width: 220,
      render: (_, rule) => (
        <Space direction="vertical" size={0}>
          <Text>{rule.pattern}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>{rule.matchKind}</Text>
        </Space>
      ),
    },
    {
      title: 'Decision',
      width: 120,
      render: (_, rule) => <Tag color={decisionColor(rule.decision)}>{decisionLabel(rule.decision)}</Tag>,
    },
    {
      title: 'Script',
      width: 130,
      render: (_, rule) => rule.script ? <code>{rule.script}</code> : <Text type="secondary">none</Text>,
    },
    { title: 'Notes', dataIndex: 'notes', ellipsis: true },
  ];

  const source = inherited
    ? `template(${templateName || 'default'})`
    : targetKind === 'sandbox'
      ? 'own'
      : 'template';

  return (
    <Modal
      title={`Network Policy: ${targetName}`}
      open={open}
      onCancel={onClose}
      width={1040}
      destroyOnClose
      footer={[
        targetKind === 'sandbox' && !inherited ? (
          <Button key="clear" danger onClick={async () => {
            setSaving(true);
            try {
              await onClear?.();
            } finally {
              setSaving(false);
            }
          }}>
            Clear override
          </Button>
        ) : null,
        <Button key="cancel" onClick={onClose}>Cancel</Button>,
        <Button
          key="save"
          type="primary"
          loading={saving}
          disabled={validationErrors.length > 0}
          onClick={handleSave}
        >
          {mode === 'inherit' ? 'Clear override' : 'Save'}
        </Button>,
      ]}
    >
      <Space direction="vertical" size={16} style={{ width: '100%' }}>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
          <Tag icon={<BranchesOutlined />}>source: {source}</Tag>
          <Tag color={networkPolicyColor(effectivePolicy)}>
            effective: {summarizeNetworkPolicy(effectivePolicy)}
          </Tag>
          <Tag>mode: {inherited ? 'inherited' : targetKind}</Tag>
        </div>

        <div
          style={{
            border: '1px solid var(--ant-color-border)',
            borderRadius: 8,
            padding: 12,
            background: 'var(--ant-color-fill-tertiary)',
          }}
        >
          <Space direction="vertical" size={6} style={{ width: '100%' }}>
            <Text strong>
              <SafetyCertificateOutlined /> Platform guardrails
            </Text>
            <Text type="secondary">Always blocked by SPACE before this policy is evaluated.</Text>
            <Space wrap>
              {PLATFORM_GUARDRAILS.map((guardrail) => (
                <Tag key={guardrail.cidr}>
                  {guardrail.label}: {guardrail.cidr}
                </Tag>
              ))}
            </Space>
          </Space>
        </div>

        <Segmented
          block
          value={mode}
          options={options}
          onChange={(value) => handleModeChange(value as DesignerMode)}
        />

        {mode !== 'detailed' ? (
          <div style={{ border: '1px solid var(--ant-color-border)', borderRadius: 8, padding: 16 }}>
            <Space align="start">
              {mode === 'open' ? <UnlockOutlined style={{ fontSize: 22 }} /> : mode === 'closed' ? <LockOutlined style={{ fontSize: 22 }} /> : <BranchesOutlined style={{ fontSize: 22 }} />}
              <Space direction="vertical" size={4}>
                <Text strong>
                  {mode === 'inherit'
                    ? `Follow ${templateName || 'template'} network policy`
                    : mode === 'open'
                      ? 'Allow outbound network by default'
                      : 'Deny outbound network by default'}
                </Text>
                <Text type="secondary">
                  {mode === 'inherit'
                    ? 'The sandbox keeps no override; template updates continue to apply.'
                    : 'This saves a v1 policy with only the egress default action.'}
                </Text>
              </Space>
            </Space>
          </div>
        ) : (
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Alert
              type="info"
              showIcon
              message="Detailed policy"
              description="Allow and deny rules are enforced by SPACE. Ask-user decisions and scripts remain preview-only and are not saved in v1."
            />
            {validationErrors.length > 0 ? (
              <Alert
                type="error"
                showIcon
                message="Invalid detailed policy"
                description={(
                  <ul style={{ margin: 0, paddingLeft: 18 }}>
                    {validationErrors.map((error) => (
                      <li key={`${error.ruleId}:${error.message}`}>{error.message}</li>
                    ))}
                  </ul>
                )}
              />
            ) : null}

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              <div style={{ border: '1px solid var(--ant-color-border)', borderRadius: 8, padding: 12 }}>
                <Space direction="vertical" size={8} style={{ width: '100%' }}>
                  <Text strong><SafetyCertificateOutlined /> Default decision</Text>
                  <Select
                    value={defaultAction}
                    onChange={(value) => setDefaultAction(value)}
                    style={{ width: '100%' }}
                    aria-label="Default network policy decision"
                    options={[
                      { value: 'allow', label: 'Allow' },
                      { value: 'deny', label: 'Deny' },
                    ]}
                  />
                  <Text type="secondary">Used when no rule makes a final decision.</Text>
                </Space>
              </div>

              <div style={{ border: '1px solid var(--ant-color-border)', borderRadius: 8, padding: 12 }}>
                <Space direction="vertical" size={8} style={{ width: '100%' }}>
                  <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                    <Text strong><ApiOutlined /> DNS resolver</Text>
                    <Switch
                      checked={dnsAllowed}
                      onChange={setDnsAllowed}
                      disabled
                      aria-label="DNS resolver access"
                    />
                  </Space>
                  <Text type="secondary">System resolver stays reachable so domain rules can evaluate.</Text>
                </Space>
              </div>
            </div>

            <div style={{ border: '1px solid var(--ant-color-border)', borderRadius: 8, padding: 12 }}>
              <Space direction="vertical" size={8} style={{ width: '100%' }}>
                <Text strong><ExperimentOutlined /> Policy test</Text>
                <Input
                  value={probe}
                  onChange={(event) => setProbe(event.target.value)}
                  placeholder="https://github.com, 16.12.4.2, api.gmail.com"
                  aria-label="Policy test destination"
                />
                <Space wrap>
                  <Text type="secondary">Result:</Text>
                  {matchedRule ? (
                    <>
                      <Tag>{matchedRule.pattern}</Tag>
                      <Tag color={decisionColor(matchedRule.decision)}>{decisionLabel(matchedRule.decision)}</Tag>
                    </>
                  ) : (
                    <Tag color={decisionColor(defaultAction)}>{decisionLabel(defaultAction)}</Tag>
                  )}
                </Space>
              </Space>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 320px', gap: 12 }}>
              <div style={{ minWidth: 0 }}>
                <Space style={{ justifyContent: 'space-between', width: '100%', marginBottom: 8 }}>
                  <Text strong>Ordered rules</Text>
                  <Button size="small" icon={<PlusOutlined />} onClick={addRule}>Add rule</Button>
                </Space>
                <Table
                  columns={columns}
                  dataSource={rules}
                  rowKey="id"
                  size="small"
                  pagination={false}
                  rowSelection={{
                    type: 'radio',
                    selectedRowKeys: selectedRule ? [selectedRule.id] : [],
                    onChange: (keys) => setSelectedRuleId(String(keys[0])),
                  }}
                  locale={{
                    emptyText: (
                      <Space direction="vertical" size={8}>
                        <Text type="secondary">No custom rules</Text>
                        <Button size="small" icon={<PlusOutlined />} onClick={addRule}>
                          Add rule
                        </Button>
                      </Space>
                    ),
                  }}
                />
              </div>

              <div style={{ border: '1px solid var(--ant-color-border)', borderRadius: 8, padding: 12 }}>
                {selectedRule ? (
                  <Space direction="vertical" size={10} style={{ width: '100%' }}>
                    <Space style={{ justifyContent: 'space-between', width: '100%' }}>
                      <Text strong>Rule editor</Text>
                      <Tooltip title="Delete rule">
                        <Button type="text" size="small" danger icon={<DeleteOutlined />} onClick={deleteRule} />
                      </Tooltip>
                    </Space>
                    <Select
                      value={selectedRule.matchKind}
                      onChange={(value) => updateRuleMatchKind(value)}
                      style={{ width: '100%' }}
                      aria-label="Rule match kind"
                      options={[
                        { value: 'domain', label: 'Domain' },
                        { value: 'wildcard_domain', label: 'Wildcard domain' },
                        { value: 'ip', label: 'IP address' },
                        { value: 'cidr', label: 'CIDR range' },
                      ]}
                    />
                    <Input
                      value={selectedRule.pattern}
                      status={selectedRuleErrors.length > 0 ? 'error' : undefined}
                      onChange={(event) => updateRule({ pattern: event.target.value })}
                      aria-label="Rule pattern"
                    />
                    {selectedRuleErrors.map((error) => (
                      <Text key={error.message} type="danger" style={{ fontSize: 12 }}>
                        {error.message}
                      </Text>
                    ))}
                    <Select
                      value={selectedRule.decision}
                      onChange={(value) => updateRule({ decision: value })}
                      style={{ width: '100%' }}
                      aria-label="Rule decision"
                      options={decisionOptions(selectedRule.matchKind)}
                    />
                    <Input
                      value={selectedRule.script}
                      onChange={(event) => updateRule({ script: event.target.value })}
                      placeholder="script name"
                      prefix={<CodeOutlined />}
                      disabled
                      aria-label="Rule script name"
                    />
                    <Input
                      value={selectedRule.notes}
                      onChange={(event) => updateRule({ notes: event.target.value })}
                      placeholder="notes"
                      aria-label="Rule notes"
                    />
                  </Space>
                ) : (
                  <Space direction="vertical" size={8} style={{ width: '100%' }}>
                    <Text type="secondary">Add a rule to edit match criteria.</Text>
                    <Button icon={<PlusOutlined />} onClick={addRule}>Add rule</Button>
                  </Space>
                )}
              </div>
            </div>

            <Divider style={{ margin: 0 }} />

            <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) 320px', gap: 12 }}>
              <div style={{ border: '1px solid var(--ant-color-border)', borderRadius: 8, padding: 12 }}>
                <Space direction="vertical" size={8} style={{ width: '100%' }}>
                  <Text strong><CodeOutlined /> WASM decision script</Text>
                  <TextArea
                    value={DEFAULT_PREVIEW_CODE}
                    readOnly
                    rows={13}
                    style={{ fontFamily: 'monospace' }}
                    aria-label="WASM decision script preview"
                  />
                </Space>
              </div>

              <div style={{ border: '1px solid var(--ant-color-border)', borderRadius: 8, padding: 12 }}>
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  <Text strong><QuestionCircleOutlined /> Ask-user prompt</Text>
                  <Text>75588b18 wants to connect to wallet.bitcoin.com:443</Text>
                  <Tag>rule: *.bitcoin.com</Tag>
                  <Tag>process: python /app/scraper.py</Tag>
                  <Tag>template: {templateName || targetName}</Tag>
                  <Space wrap>
                    <Button size="small" disabled>Allow once</Button>
                    <Button size="small" danger disabled>Deny</Button>
                    <Button size="small" disabled>Always allow</Button>
                  </Space>
                  <Text type="secondary">Future runtime flow: pause request, collect user decision, then resume script.</Text>
                </Space>
              </div>
            </div>
          </Space>
        )}
      </Space>
    </Modal>
  );
}
