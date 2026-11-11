'use client';

import { useMemo } from 'react';
import Link from 'next/link';
import { Button, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { WarmPoolPolicy } from '@/lib/types';
import type { TemplateActions } from './template-actions';
import type { TemplatesData } from './use-templates-data';
import { templateHref } from './links';

const { Title, Text } = Typography;

interface Props {
  data: TemplatesData;
  actions: TemplateActions;
  // The template page: its own policies, and New Policy creates one for it.
  templateName?: string;
}

// The rolling warm pool policies of a Kubernetes cluster.
export default function WarmPoolPoliciesTable({ data, actions, templateName }: Props) {
  const { warmPoolPolicies, warmPoolPolicyCapabilities, canAdminTemplates, loadingReleases } = data;
  const { openWarmPoolPolicyModal } = actions;
  const shown = useMemo(
    () => (templateName ? warmPoolPolicies.filter((policy) => policy.template_name === templateName) : warmPoolPolicies),
    [warmPoolPolicies, templateName],
  );

  const columns: ColumnsType<WarmPoolPolicy> = [
    ...(templateName ? [] : [{
      title: 'Template',
      dataIndex: 'template_name',
      width: 180,
      render: (value: string, record: WarmPoolPolicy) => (
        <Space size={4}>
          <Link href={templateHref(value, 'pool')} style={{ fontWeight: 600 }}>{value}</Link>
          <Text type="secondary" style={{ fontSize: 12 }}>({record.runtime})</Text>
        </Space>
      ),
    }]),
    {
      title: 'Shape',
      width: 135,
      render: (_, record) => (
        <Space orientation="vertical" size={0}>
          <span>{record.vcpu}c / {record.memory_mib} MiB</span>
          {warmPoolPolicyCapabilities.disk_capacity ? (
            <Text type="secondary">
              {record.disk_mib ? `${record.disk_mib} MiB disk` : 'Template disk'}
            </Text>
          ) : null}
        </Space>
      ),
    },
    ...(warmPoolPolicyCapabilities.disk_io ? [{
      title: 'Disk I/O',
      width: 190,
      render: (_: unknown, record: WarmPoolPolicy) => {
        const diskIo = warmPoolPolicyCapabilities.disk_io!;
        if (record.disk_io_max_operations_per_sec !== undefined
          && record.disk_io_max_bandwidth_mib_per_sec !== undefined) {
          return `${record.disk_io_max_operations_per_sec.toLocaleString()} IOPS / ${record.disk_io_max_bandwidth_mib_per_sec} MiB/s`;
        }
        return (
          <Text type="secondary">
            Default ({diskIo.default_operations_per_sec.toLocaleString()} IOPS / {diskIo.default_bandwidth_mib_per_sec} MiB/s)
          </Text>
        );
      },
    }] : []),
    {
      title: 'Ready / release',
      width: 135,
      render: (_, record) => `${record.min_ready_slots} - ${record.max_ready_slots}`,
    },
    {
      title: 'Refill / tick',
      dataIndex: 'refill_per_tick',
      width: 105,
    },
    {
      title: 'Warming slots',
      dataIndex: 'max_warming_slots',
      width: 120,
    },
    {
      title: 'Retained releases',
      dataIndex: 'retain_releases',
      width: 135,
    },
    {
      title: 'Baseline slots',
      width: 125,
      render: (_, record) => record.min_ready_slots * record.retain_releases,
    },
    {
      title: 'Status',
      dataIndex: 'enabled',
      width: 105,
      render: (enabled: boolean) => (
        <Tag color={enabled ? 'green' : undefined}>{enabled ? 'Enabled' : 'Disabled'}</Tag>
      ),
    },
    {
      title: 'Actions',
      width: 70,
      render: (_, record) => (
        canAdminTemplates ? (
          <Button type="text" size="small" onClick={() => openWarmPoolPolicyModal(record)}>
            Edit
          </Button>
        ) : <Text type="secondary">—</Text>
      ),
    },
  ];

  return (
    <>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <div>
          <Title level={5} style={{ marginBottom: 0 }}>Rolling Warm Pool Policies</Title>
          <Text type="secondary">
            Deployment-like policies create release-specific pools; the baseline is minimum ready slots multiplied by retained releases.
          </Text>
        </div>
        {canAdminTemplates ? (
          <Button type="primary" onClick={() => openWarmPoolPolicyModal(null, templateName ?? null)}>
            New Policy
          </Button>
        ) : null}
      </Space>
      <Table
        columns={columns}
        dataSource={shown}
        rowKey={(policy) => `${policy.template_name}:${policy.runtime}:${policy.vcpu}:${policy.memory_mib}`}
        loading={loadingReleases}
        size="small"
        pagination={false}
        style={{ marginTop: 12, marginBottom: 24 }}
        scroll={{ x: 1080 }}
        locale={templateName ? { emptyText: 'No rolling policy for this template' } : undefined}
      />
    </>
  );
}
