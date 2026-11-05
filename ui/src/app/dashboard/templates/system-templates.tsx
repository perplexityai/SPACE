'use client';

import Link from 'next/link';
import { Space, Table, Tag, Tooltip, Typography } from 'antd';
import { LATEST_TAG, shortHash } from '@/lib/template-format';
import SystemTemplateRebuild from './system-template-rebuild';
import type { TemplatesData } from './use-templates-data';
import { templateHref } from './links';

const { Text, Title } = Typography;
const SYSTEM_TEMPLATES = ['seed', 'code-runtime'] as const;

// The system templates and their rebuild. The rebuild builds from the recipe
// bundled with the API, so it is offered here whether or not the templates
// exist yet: a new cluster's first build starts from this tab.
export default function SystemTemplates({ data }: { data: TemplatesData }) {
  const rows = SYSTEM_TEMPLATES.map((name) => {
    const template = data.templates.find((candidate) => candidate.name === name) ?? null;
    const latest = data.templateReleases.find((release) => release.template_name === name && release.tags.includes(LATEST_TAG)) ?? null;
    return { name, template, latest };
  });

  return (
    <Space orientation="vertical" size={8} style={{ width: '100%' }}>
      <Table
        size="small"
        rowKey="name"
        pagination={false}
        dataSource={rows}
        style={{ maxWidth: 720 }}
        columns={[
          {
            title: 'Template',
            dataIndex: 'name',
            render: (name: string, row) => (row.template
              ? <Link href={templateHref(name)} style={{ fontWeight: 600 }}>{name}</Link>
              : <Text strong>{name}</Text>),
          },
          {
            title: LATEST_TAG,
            render: (_, row) => {
              if (!data.templatesLoaded || !data.releasesLoaded) return <Text type="secondary">…</Text>;
              if (!row.template) return <Tag style={{ marginInlineEnd: 0 }}>not built yet</Tag>;
              if (!row.latest) return <Tag color="warning" style={{ marginInlineEnd: 0 }}>no {LATEST_TAG}</Tag>;
              return <Tooltip title={row.latest.content_hash}><code>{shortHash(row.latest.content_hash)}</code></Tooltip>;
            },
          },
          {
            title: 'Builds',
            render: (_, row) => (row.template
              ? <Link href={templateHref(row.name, 'builds')}>Builds</Link>
              : <Text type="secondary">—</Text>),
          },
        ]}
      />
      <SystemTemplateRebuild
        releases={data.templateReleases}
        releasesLoaded={data.releasesLoaded}
        refreshReleases={() => data.refreshTemplateReleases({ showLoading: false })}
        title={<Title level={5} style={{ margin: 0 }}>System template rebuild</Title>}
      />
    </Space>
  );
}
