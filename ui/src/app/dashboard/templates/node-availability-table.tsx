'use client';

import { useMemo } from 'react';
import Link from 'next/link';
import { Button, Input, Popconfirm, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { DeleteOutlined, DownloadOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { shortHash } from '@/lib/template-format';
import { releaseRowMatches } from '@/lib/template-list';
import { tablePagination } from '@/lib/table-pagination';
import {
  NodeSnapshotIcon, formatShortDateTime, renderDiskCell, roleColor, shortNodeIdentity, tableFilterOptions,
} from './template-cells';
import type { TemplateActions } from './template-actions';
import type { TemplateAvailabilityRow, TemplatesData } from './use-templates-data';
import { templateHref } from './links';

const { Text } = Typography;

interface Props {
  data: TemplatesData;
  actions: TemplateActions;
  // The template page: its own copies only, without the Template column.
  templateName?: string;
  search: string;
  onSearch: (search: string) => void;
}

// Which reachable nodes hold each template@hash, with pull and delete on a
// node.
export default function NodeAvailabilityTable({ data, actions, templateName, search, onSearch }: Props) {
  const { availabilityRows, loadingInventory, templateCapableNodes } = data;
  const { renderReleaseTagControls, templateOperationRunning, handleDeleteTemplateFromNode, openPullModal } = actions;

  const scoped = useMemo(
    () => (templateName ? availabilityRows.filter((row) => row.name === templateName) : availabilityRows),
    [availabilityRows, templateName],
  );
  const shown = useMemo(() => {
    const query = search.trim().toLowerCase();
    return scoped.filter((row) => releaseRowMatches({ template_name: row.name, content_hash: row.content_hash, tags: row.tags }, search)
      || row.nodes.some((availability) => shortNodeIdentity(availability.node).toLowerCase().includes(query)));
  }, [scoped, search]);

  const inventoryTemplateFilters = useMemo(() => tableFilterOptions(scoped.map((row) => row.name)), [scoped]);
  const inventoryTagFilters = useMemo(() => tableFilterOptions(scoped.flatMap((row) => row.tags)), [scoped]);

  const columns: ColumnsType<TemplateAvailabilityRow> = [
    ...(templateName ? [] : [{
      title: 'Template',
      dataIndex: 'name',
      width: 130,
      filters: inventoryTemplateFilters,
      filterSearch: true,
      onFilter: (value: unknown, record: TemplateAvailabilityRow) => record.name === String(value),
      render: (value: string) => <Link href={templateHref(value, 'nodes')} style={{ fontWeight: 600 }}>{value}</Link>,
    }]),
    {
      title: 'Tags',
      dataIndex: 'tags',
      width: 170,
      filters: inventoryTagFilters,
      filterSearch: true,
      onFilter: (value, record) => record.tags.includes(String(value)),
      render: (_tags: string[], record) =>
        record.release ? renderReleaseTagControls(record.release) : <Text type="secondary">unpublished</Text>,
    },
    {
      title: 'Hash',
      dataIndex: 'content_hash',
      width: 110,
      render: (value: string | null) => <code>{shortHash(value)}</code>,
    },
    {
      title: 'Runtime',
      dataIndex: 'runtime',
      width: 90,
      render: (value: string | null) => value || '—',
    },
    {
      title: 'Built',
      dataIndex: 'build_date',
      width: 115,
      render: (value: string | null) => value ? formatShortDateTime(value) : '—',
    },
    {
      title: 'Disk',
      dataIndex: 'disk_size_mb',
      width: 90,
      render: (_, record) => renderDiskCell(record),
    },
    {
      title: 'Nodes',
      render: (_, record) => {
        if (record.nodes.length === 0) return <Text type="secondary">No nodes</Text>;
        const sortedItems = [...record.nodes].sort((a, b) =>
          shortNodeIdentity(a.node).localeCompare(shortNodeIdentity(b.node)));
        return (
          <Space wrap size={4}>
            {sortedItems.map((availability) => {
              const node = availability.node;
              const nodeText = shortNodeIdentity(node);
              return (
                <Tag
                  key={node.id}
                  color={roleColor(node.role)}
                  style={{ marginRight: 0, paddingRight: node.managed_by === 'space' ? 2 : undefined }}
                >
                  <Space size={4}>
                    <NodeSnapshotIcon availability={availability} />
                    <code style={{ fontSize: 12 }}>{nodeText}</code>
                    {node.managed_by !== 'space' ? (
                      <span style={{ opacity: 0.7, fontSize: 11 }}>{node.managed_by}</span>
                    ) : (
                      <Popconfirm
                        title={`Delete ${record.name}@${shortHash(record.content_hash)} from ${node.name || node.node_api_endpoint || node.id}?`}
                        okText="Delete"
                        okType="danger"
                        onConfirm={() => handleDeleteTemplateFromNode(record, node)}
                      >
                        <Tooltip title="Delete from node">
                          <Button
                            type="text"
                            size="small"
                            icon={<DeleteOutlined />}
                            loading={templateOperationRunning(`delete:${record.key}:${node.id}`)}
                            aria-label="Delete from node"
                            style={{ width: 18, height: 18, minWidth: 18, padding: 0, color: 'inherit' }}
                          />
                        </Tooltip>
                      </Popconfirm>
                    )}
                  </Space>
                </Tag>
              );
            })}
          </Space>
        );
      },
    },
    {
      title: 'Actions',
      width: 52,
      render: (_, record) => {
        const existingNodeIds = new Set(record.nodes.map((availability) => availability.node.id));
        const hasTarget = templateCapableNodes.some((node) => !existingNodeIds.has(node.id));
        return (
          <Tooltip title="Pull to node">
            <Button
              type="text"
              size="small"
              className="icon-action-btn"
              icon={<DownloadOutlined />}
              disabled={!hasTarget}
              loading={templateOperationRunning(`pull:${record.key}`)}
              aria-label="Pull to node"
              onClick={() => openPullModal(record)}
            />
          </Tooltip>
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
        placeholder={templateName ? 'Search tag, hash or node' : 'Search template, tag, hash or node'}
        aria-label="Search node copies"
        style={{ maxWidth: 420, marginBottom: 8 }}
      />
      <Table
        columns={columns}
        dataSource={shown}
        rowKey="key"
        loading={loadingInventory}
        size="small"
        pagination={tablePagination('template builds')}
        scroll={{ x: 1000 }}
        locale={{ emptyText: templateName ? 'No reachable node reports a copy of this template' : undefined }}
      />
    </>
  );
}
