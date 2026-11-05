'use client';

import { useMemo } from 'react';
import Link from 'next/link';
import { Button, Input, Popconfirm, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { DeleteOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { shortHash } from '@/lib/template-format';
import { releaseRowMatches, visibleTemplateTags } from '@/lib/template-list';
import { tablePagination } from '@/lib/table-pagination';
import type { TemplateRelease, TemplateReleaseWarmPool } from '@/lib/types';
import {
  formatShortDateTime, matchesWarmPoolFilter, tableFilterOptions, warmPoolLabel, warmPoolTooltip,
} from './template-cells';
import type { TemplateActions } from './template-actions';
import type { TemplatesData } from './use-templates-data';
import { templateHref } from './links';

const { Text } = Typography;

interface Props {
  data: TemplatesData;
  actions: TemplateActions;
  // The template page shows only its own releases, without the Template column.
  templateName?: string;
  search: string;
  onSearch: (search: string) => void;
  // A release to mark, such as one a search on the list named.
  highlight?: string | null;
}

export default function ReleasesTable({ data, actions, templateName, search, onSearch, highlight }: Props) {
  const { templateReleases, loadingReleases, supportsClusterPools, canAdminTemplates, warmPoolPolicyCapabilities } = data;
  const { canManageTemplateReleases, templateOperationRunning, renderReleaseTagControls, openClusterPoolModal, handleDeleteRelease } = actions;

  const scoped = useMemo(
    () => (templateName ? templateReleases.filter((release) => release.template_name === templateName) : templateReleases),
    [templateReleases, templateName],
  );
  const shown = useMemo(() => scoped.filter((release) => releaseRowMatches(release, search)), [scoped, search]);

  const releaseTemplateFilters = useMemo(
    () => tableFilterOptions(scoped.map((release) => release.template_name)),
    [scoped],
  );
  const releaseTagFilters = useMemo(
    () => tableFilterOptions(scoped.flatMap((release) => visibleTemplateTags(release.tags, release.content_hash))),
    [scoped],
  );

  const columns: ColumnsType<TemplateRelease> = [
    ...(templateName ? [] : [{
      title: 'Template',
      dataIndex: 'template_name',
      width: 180,
      defaultSortOrder: 'ascend' as const,
      filters: releaseTemplateFilters,
      filterSearch: true,
      onFilter: (value: unknown, record: TemplateRelease) => record.template_name === String(value),
      sorter: (a: TemplateRelease, b: TemplateRelease) => a.template_name.localeCompare(b.template_name),
      render: (value: string, record: TemplateRelease) => (
        <Space size={4} wrap={false}>
          <Link href={templateHref(value, 'releases')} style={{ whiteSpace: 'nowrap', fontWeight: 600 }}>{value}</Link>
          <Text type="secondary" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
            ({record.runtime || '—'})
          </Text>
        </Space>
      ),
    }]),
    {
      title: 'Tags',
      dataIndex: 'tags',
      width: 320,
      filters: releaseTagFilters,
      filterSearch: true,
      onFilter: (value, record) =>
        visibleTemplateTags(record.tags, record.content_hash).includes(String(value)),
      render: (_tags: string[], record) => renderReleaseTagControls(record),
    },
    {
      title: 'Hash',
      dataIndex: 'content_hash',
      width: 130,
      render: (value: string, record) => (
        <Space size={4}>
          <Tooltip title={value}>
            <code>{shortHash(value)}</code>
          </Tooltip>
          {record.id === highlight ? <Tag color="gold" style={{ marginInlineEnd: 0 }}>searched</Tag> : null}
        </Space>
      ),
    },
    ...(templateName ? [{
      title: 'Runtime',
      dataIndex: 'runtime',
      width: 80,
      render: (value: string) => value || '—',
    }] : []),
    {
      title: 'Created',
      dataIndex: 'created_at',
      width: 125,
      sorter: (a, b) => Date.parse(a.created_at) - Date.parse(b.created_at),
      ...(templateName ? { defaultSortOrder: 'descend' as const } : {}),
      render: (value: string) => formatShortDateTime(value),
    },
    {
      title: 'Warm Pool',
      dataIndex: 'warm_pools',
      width: 230,
      filters: supportsClusterPools ? [
        { text: 'Enabled', value: 'enabled' },
        { text: 'Disabled', value: 'disabled' },
        { text: 'No pool', value: 'none' },
      ] : undefined,
      onFilter: supportsClusterPools
        ? (value, record) => matchesWarmPoolFilter(record.warm_pools, String(value))
        : undefined,
      render: (pools: TemplateReleaseWarmPool[], record) => {
        if (!supportsClusterPools) return <Text type="secondary">Kubernetes only</Text>;
        if (pools.length === 0) return <Text type="secondary">No warm pool</Text>;
        return (
          <Space wrap size={4}>
            {pools.map((pool) => {
              const tag = (
                <Tag
                  color={!pool.enabled ? undefined : pool.managed_by === 'autoscaler' ? 'purple' : 'gold'}
                  style={{ marginRight: 0 }}
                >
                  {warmPoolLabel(pool)} {pool.managed_by}
                </Tag>
              );
              return (
                <Tooltip
                  key={pool.id}
                  title={warmPoolTooltip(pool, Boolean(warmPoolPolicyCapabilities.disk_capacity))}
                >
                  {canAdminTemplates ? (
                    <Button
                      type="text"
                      size="small"
                      aria-label={`Manage ${pool.managed_by} pool`}
                      onClick={() => openClusterPoolModal(record, pool.id)}
                      style={{ height: 'auto', padding: 0 }}
                    >
                      {tag}
                    </Button>
                  ) : tag}
                </Tooltip>
              );
            })}
          </Space>
        );
      },
    },
    {
      title: 'Actions',
      fixed: 'right',
      width: 136,
      render: (_, record) => {
        const deleteReleaseRunning = templateOperationRunning(`delete-release:${record.id}`);
        return (
          <Space size={4}>
            {supportsClusterPools ? (
              record.warm_pools.length === 0 ? (
                <Button type="text" size="small" onClick={() => openClusterPoolModal(record)}>
                  Create Pool
                </Button>
              ) : <Text type="secondary">Select pool</Text>
            ) : (
              <Text type="secondary">—</Text>
            )}
            {canManageTemplateReleases ? (
              <Popconfirm
                title={`Unpublish ${record.template_name}@${shortHash(record.content_hash)}?`}
                description="Node-local cached copies are not deleted."
                okText="Unpublish"
                okType="danger"
                disabled={record.warm_pools.length > 0 || deleteReleaseRunning}
                onConfirm={() => handleDeleteRelease(record)}
              >
                <Tooltip title={record.warm_pools.length > 0 ? 'Delete pool policy first' : 'Unpublish release'}>
                  <span style={{ display: 'inline-block' }}>
                    <Button
                      type="text"
                      size="small"
                      danger
                      disabled={record.warm_pools.length > 0 || deleteReleaseRunning}
                      icon={<DeleteOutlined />}
                      loading={deleteReleaseRunning}
                      aria-label="Unpublish release"
                    />
                  </span>
                </Tooltip>
              </Popconfirm>
            ) : null}
          </Space>
        );
      },
    },
  ];

  return (
    <>
      <Input.Search
        allowClear
        value={search}
        onChange={(event) => onSearch(event.target.value)}
        placeholder={templateName ? 'Search tag, hash or release ID' : 'Search template, tag, hash or release ID'}
        aria-label="Search releases"
        style={{ maxWidth: 420, marginBottom: 8 }}
      />
      <Table
        columns={columns}
        dataSource={shown}
        rowKey="id"
        loading={loadingReleases}
        size="small"
        pagination={tablePagination('template releases')}
        scroll={{ x: templateName ? 1020 : 1120 }}
        rowClassName={(record) => (record.id === highlight ? 'template-release-highlight' : '')}
      />
    </>
  );
}
