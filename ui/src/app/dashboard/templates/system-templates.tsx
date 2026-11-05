'use client';

import Link from 'next/link';
import { Space, Table, Tag, Tooltip, Typography } from 'antd';
import { LATEST_TAG, shortHash } from '@/lib/template-format';
import { linkableTemplates } from '@/lib/system-template-rebuild';
import { firstReadError } from '@/lib/template-reads';
import { ReadErrorAlert } from './template-cells';
import SystemTemplateRebuild from './system-template-rebuild';
import SeedBuilder from './seed-builder';
import { useAuth } from '@/lib/auth';
import type { TemplateActions } from './template-actions';
import type { TemplatesData } from './use-templates-data';
import { templateHref } from './links';

const { Text, Title } = Typography;
const SYSTEM_TEMPLATES = ['seed', 'code-runtime'] as const;

// The system templates and their rebuild. The rebuild builds from the recipe
// bundled with the API, so it is offered here whether or not the templates
// exist yet: a new cluster's first build starts from this tab.
export default function SystemTemplates({ data, actions }: { data: TemplatesData; actions: TemplateActions }) {
  const { cluster, user } = useAuth();
  const hasCodeRuntime = data.templates.some((template) => template.name === 'code-runtime');
  const rows = SYSTEM_TEMPLATES.filter((name) => name === 'seed' || hasCodeRuntime).map((name) => {
    const template = data.templates.find((candidate) => candidate.name === name) ?? null;
    const latest = data.templateReleases.find((release) => release.template_name === name && release.tags.includes(LATEST_TAG)) ?? null;
    return { name, template, latest };
  });

  const readError = firstReadError([
    { loaded: data.templatesLoaded, error: data.templatesError },
    { loaded: data.releasesLoaded, error: data.releasesError },
  ]);

  return (
    <Space orientation="vertical" size={8} style={{ width: '100%' }}>
      {readError ? (
        <ReadErrorAlert what="the system templates" error={readError} data={data} style={{ maxWidth: 720 }} />
      ) : null}
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
              if (readError) return <Text type="warning">not read</Text>;
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
      <SeedBuilder key={`${cluster?.id}:${user?.id}`} data={data} actions={actions} />
      {hasCodeRuntime ? <SystemTemplateRebuild
        releases={data.templateReleases}
        releasesLoaded={data.releasesLoaded}
        refreshReleases={() => data.refreshTemplateReleases({ showLoading: false })}
        title={<Title level={5} style={{ margin: 0 }}>System template rebuild</Title>}
        listedTemplates={linkableTemplates({ releases: data.templateReleases, listed: data.templates })}
      /> : null}
    </Space>
  );
}
