'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import {
  Alert,
  Card,
  Col,
  Empty,
  Input,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
  App,
  theme,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { KeyOutlined, SafetyCertificateOutlined, SearchOutlined } from '@ant-design/icons';

import { adminApi } from '@/lib/api';
import {
  credentialHostDecision,
  formatPolicyDestination,
  type CredentialHostDecision,
} from '@/lib/credential-rules';
import type {
  CredentialHeaderBindingV1,
  NetworkCredentialRuleV1,
  NetworkPolicyV1,
  Organization,
  Sandbox,
} from '@/lib/types';

const { Paragraph, Text, Title } = Typography;

type PolicySource = 'sandbox override' | 'template';

interface CredentialRuleRow {
  key: string;
  sandbox: Sandbox;
  policy: NetworkPolicyV1;
  policySource: PolicySource;
  rule: NetworkCredentialRuleV1;
  order: number;
  hostDecision: CredentialHostDecision;
}

function effectivePolicy(sandbox: Sandbox): NetworkPolicyV1 | null {
  return (
    sandbox.effective_desired_network_policy ??
    sandbox.network_policy_override ??
    sandbox.template_network_policy ??
    sandbox.network_policy
  );
}

function policySource(sandbox: Sandbox): PolicySource {
  return sandbox.network_policy_override ? 'sandbox override' : 'template';
}

function referenceLabel(binding: CredentialHeaderBindingV1) {
  return (
    <Space key={`${binding.header_name}:${binding.credential_reference_id}`} size={4} wrap>
      <Text code>{binding.header_name}</Text>
      <Text type="secondary">←</Text>
      <Tooltip title={binding.credential_reference_id}>
        <Text copyable={{ text: binding.credential_reference_id }}>
          {binding.credential_reference_id.slice(0, 8)}…
        </Text>
      </Tooltip>
    </Space>
  );
}

function decisionDescription(decision: CredentialHostDecision): string {
  switch (decision.reason) {
    case 'explicit_deny':
      return `deny rule ${formatPolicyDestination(decision.matched_destination!)}`;
    case 'explicit_allow':
      return `allow rule ${formatPolicyDestination(decision.matched_destination!)}`;
    case 'default_allow':
      return 'default allow';
    case 'default_deny':
      return 'default deny';
  }
}

function flattenRules(sandboxes: Sandbox[]): CredentialRuleRow[] {
  return sandboxes.flatMap((sandbox) => {
    const policy = effectivePolicy(sandbox);
    if (!policy) return [];
    return (policy.network_credential_rules ?? []).map((rule, index) => ({
      key: `${sandbox.id}:${rule.rule_id}`,
      sandbox,
      policy,
      policySource: policySource(sandbox),
      rule,
      order: index + 1,
      hostDecision: credentialHostDecision(policy, rule.destination.exact_host),
    }));
  });
}

export default function CredentialRulesPage() {
  const { message } = App.useApp();
  const { token } = theme.useToken();
  const [sandboxes, setSandboxes] = useState<Sandbox[]>([]);
  const [organizations, setOrganizations] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState('');
  const [decisionFilter, setDecisionFilter] = useState<'all' | 'allow' | 'deny'>('all');

  useEffect(() => {
    let active = true;
    Promise.all([
      adminApi.get<Sandbox[]>('/v1/sandboxes?limit=5000'),
      adminApi.get<Organization[]>('/v1/orgs'),
    ])
      .then(([loadedSandboxes, loadedOrganizations]) => {
        if (active) {
          setSandboxes(loadedSandboxes);
          setOrganizations(loadedOrganizations);
        }
      })
      .catch((error: unknown) => {
        if (active) {
          message.error(error instanceof Error ? error.message : 'Failed to load credential rules');
        }
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  const rows = useMemo(() => flattenRules(sandboxes), [sandboxes]);
  const organizationNames = useMemo(
    () => new Map(organizations.map((organization) => [organization.id, organization.name])),
    [organizations],
  );
  const filteredRows = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return rows.filter((row) => {
      if (decisionFilter !== 'all' && row.hostDecision.action !== decisionFilter) return false;
      if (!needle) return true;
      return [
        row.sandbox.name,
        row.sandbox.id,
        row.sandbox.org_id ? organizationNames.get(row.sandbox.org_id) : null,
        row.sandbox.org_id,
        row.rule.rule_id,
        row.rule.destination.exact_host,
        ...row.rule.credential_header_bindings.flatMap((binding) => [
          binding.header_name,
          binding.credential_reference_id,
        ]),
      ].some((value) => value?.toLowerCase().includes(needle));
    });
  }, [decisionFilter, organizationNames, query, rows]);

  const sandboxCount = new Set(rows.map((row) => row.sandbox.id)).size;
  const blockedCount = rows.filter((row) => row.hostDecision.action === 'deny').length;

  const columns: ColumnsType<CredentialRuleRow> = [
    {
      title: 'Sandbox',
      key: 'sandbox',
      fixed: 'left',
      width: 210,
      render: (_, row) => (
        <Space orientation="vertical" size={0}>
          <Link href={`/dashboard/sandboxes/${row.sandbox.id}`}>
            {row.sandbox.name || row.sandbox.id.slice(0, 8)}
          </Link>
          <Text type="secondary" style={{ fontSize: 12 }} copyable={{ text: row.sandbox.id }}>
            {row.sandbox.id.slice(0, 8)}…
          </Text>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {row.sandbox.org_id
              ? organizationNames.get(row.sandbox.org_id) ?? row.sandbox.org_id
              : 'no organization'}
          </Text>
        </Space>
      ),
    },
    {
      title: 'Order',
      dataIndex: 'order',
      width: 125,
      render: (order: number, row) => (
        <Space orientation="vertical" size={2}>
          <Tooltip title="For overlapping rules, the first matching rule wins for each header name">
            <Tag>#{order}</Tag>
          </Tooltip>
          <Text type="secondary" ellipsis={{ tooltip: row.rule.rule_id }} style={{ maxWidth: 105 }}>
            {row.rule.rule_id}
          </Text>
        </Space>
      ),
    },
    {
      title: 'Credential match',
      key: 'destination',
      width: 260,
      render: (_, row) => (
        <Space orientation="vertical" size={4}>
          <Text strong code>
            {row.rule.destination.exact_host}:{row.rule.destination.port ?? 443}
          </Text>
          <Text type="secondary">
            {row.rule.destination.path_prefix ?? 'all safe paths'}
          </Text>
          <Space size={4} wrap>
            {row.rule.supported_http_methods.map((method) => (
              <Tag key={method} color="blue">{method}</Tag>
            ))}
          </Space>
        </Space>
      ),
    },
    {
      title: 'Injected headers',
      key: 'credentials',
      width: 300,
      render: (_, row) => (
        <Space orientation="vertical" size={4}>
          {row.rule.credential_header_bindings.map(referenceLabel)}
        </Space>
      ),
    },
    {
      title: 'Network policy',
      key: 'decision',
      width: 210,
      render: (_, row) => (
        <Space orientation="vertical" size={2}>
          <Tag color={row.hostDecision.action === 'allow' ? 'success' : 'error'}>
            {row.hostDecision.action.toUpperCase()}
          </Tag>
          <Text>{decisionDescription(row.hostDecision)}</Text>
          <Text type="secondary" style={{ fontSize: 12 }}>
            TLS interception {row.policy.tls_interception === 'on' ? 'on' : 'off'}
          </Text>
        </Space>
      ),
    },
    {
      title: 'Policy state',
      key: 'state',
      width: 190,
      render: (_, row) => (
        <Space orientation="vertical" size={2}>
          <Tag color={row.policySource === 'sandbox override' ? 'purple' : 'default'}>
            {row.policySource}
          </Tag>
          <Text type="secondary" style={{ fontSize: 12 }}>
            desired {row.sandbox.network_policy_generation ?? '—'} · observed{' '}
            {row.sandbox.network_policy_status?.observed_generation ?? '—'}
          </Text>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Space align="start" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
        <div>
          <Title level={2} style={{ marginBottom: 4 }}>Credential Rules</Title>
          <Paragraph type="secondary">
            Read-only effective credential injection and network policy for managed sandboxes.
          </Paragraph>
        </div>
        <Tag icon={<KeyOutlined />} color="geekblue">reference metadata only</Tag>
      </Space>

      <Alert
        type="info"
        showIcon
        icon={<SafetyCertificateOutlined />}
        style={{ marginBottom: 16 }}
        message="Evaluation order"
        description={
          <Space orientation="vertical" size={2}>
            <Text>1. Platform and explicit network denies are evaluated before allows.</Text>
            <Text>2. Exact and wildcard domain rules are matched; wildcards cover subdomains, not the apex.</Text>
            <Text>3. The policy default applies when neither list matches.</Text>
            <Text>4. For an allowed HTTPS request, credential rules match exact host, port, path, and method. Earlier rules win per header name.</Text>
            <Text type="secondary">Resolved IP/CIDR rules and platform guardrails can still deny a host shown as allowed here.</Text>
          </Space>
        }
      />

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} sm={8}><Card size="small"><Statistic title="Credential rules" value={rows.length} /></Card></Col>
        <Col xs={24} sm={8}><Card size="small"><Statistic title="Sandboxes" value={sandboxCount} /></Card></Col>
        <Col xs={24} sm={8}><Card size="small"><Statistic title="Host matches denied" value={blockedCount} valueStyle={blockedCount ? { color: token.colorError } : undefined} /></Card></Col>
      </Row>

      <Card size="small">
        <Space style={{ marginBottom: 16 }} wrap>
          <Input
            allowClear
            prefix={<SearchOutlined />}
            placeholder="Search sandbox, host, rule, header, or reference"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            style={{ width: 390, maxWidth: '100%' }}
          />
          <Select
            value={decisionFilter}
            onChange={setDecisionFilter}
            style={{ width: 160 }}
            options={[
              { value: 'all', label: 'All decisions' },
              { value: 'allow', label: 'Allowed hosts' },
              { value: 'deny', label: 'Denied hosts' },
            ]}
          />
        </Space>
        <Table
          columns={columns}
          dataSource={filteredRows}
          loading={loading}
          pagination={{ pageSize: 25, showSizeChanger: true }}
          scroll={{ x: 1295 }}
          size="small"
          locale={{
            emptyText: loading ? null : (
              <Empty description={rows.length ? 'No rules match these filters' : 'No credential rules found'} />
            ),
          }}
        />
      </Card>
    </div>
  );
}
