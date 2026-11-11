'use client';

import { useEffect } from 'react';
import Link from 'next/link';
import { Alert, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { spacedCoverage } from '@/lib/spaced-counts';
import type { TemplatesData } from './use-templates-data';
import { templateHref } from './links';

const { Text } = Typography;

interface SpacedRow {
  key: string;
  template: string;
  version: string;
  count: number;
}

// The spaced versions running sandboxes report, per template. Mounting it
// asks the data to read the counts (once, then on Refresh).
export default function SpacedVersions({ data, templateName }: { data: TemplatesData; templateName?: string }) {
  const { spacedCounts, spacedCountsError, showSpacedCounts } = data;
  useEffect(() => showSpacedCounts(), []);

  // One row per template and reported spaced version.
  const spacedRows: SpacedRow[] = spacedCounts
    ? [...spacedCounts.byTemplate.entries()]
      .filter(([template]) => !templateName || template === templateName)
      .sort(([a], [b]) => a.localeCompare(b))
      .flatMap(([template, versions]) => [...versions.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([version, count]) => ({ key: `${template}:${version}`, template, version, count })))
    : [];
  const spacedColumns: ColumnsType<SpacedRow> = [
    ...(templateName ? [] : [{
      title: 'Template',
      dataIndex: 'template',
      render: (value: string) => <Link href={templateHref(value)} style={{ fontWeight: 600 }}>{value}</Link>,
    }]),
    { title: 'spaced version', dataIndex: 'version', render: (value: string) => <code>{value}</code> },
    { title: 'Running sandboxes', dataIndex: 'count', align: 'right' },
  ];

  return (
    <>
      <Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
        {spacedCounts ? spacedCoverage(spacedCounts, (iso) => new Date(iso).toLocaleString()) : 'Counted from the admin sandbox list.'}
      </Text>
      {spacedCountsError ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 8, maxWidth: 720 }}
          title={`Could not refresh the sandbox counts: ${spacedCountsError}${spacedCounts ? '. The counts below are from the previous read.' : ''}`}
        />
      ) : null}
      <Table
        columns={spacedColumns}
        dataSource={spacedRows}
        rowKey="key"
        size="small"
        pagination={false}
        loading={!spacedCounts && !spacedCountsError}
        locale={{ emptyText: spacedCounts ? (spacedCounts.source === 'server' ? 'No running sandboxes' : 'No running sandboxes among the listed ones') : 'Not loaded' }}
        style={{ maxWidth: 720 }}
      />
    </>
  );
}
