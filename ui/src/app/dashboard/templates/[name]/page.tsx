'use client';

import { Suspense, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useParams, usePathname, useRouter, useSearchParams } from 'next/navigation';
import { Button, Popconfirm, Result, Space, Spin, Tabs, Tag, Tooltip, Typography } from 'antd';
import { ArrowLeftOutlined, DeleteOutlined, ReloadOutlined } from '@ant-design/icons';
import { adminApi } from '@/lib/api';
import { LATEST_TAG, shortHash } from '@/lib/template-format';
import { templateListRows } from '@/lib/template-list';
import type { Organization } from '@/lib/types';
import { useTemplatesData } from '../use-templates-data';
import { useTemplateActions } from '../template-actions';
import { COMPACT_TABLE_STYLE } from '../template-cells';
import { TemplateBuildsPanel } from '../template-builds';
import TemplateOverview from '../template-overview';
import TemplatePool from '../template-pool';
import ReleasesTable from '../releases-table';
import NodeAvailabilityTable from '../node-availability-table';
import SpacedVersions from '../spaced-versions';
import SystemTemplateRebuild from '../system-template-rebuild';
import { TEMPLATE_SECTIONS, templateSandboxesHref, type TemplateSection } from '../links';
import { useUrlView } from '../use-url-view';

const { Title, Text } = Typography;
const SYSTEM_TEMPLATES = new Set(['seed', 'code-runtime']);

interface View {
  tab: TemplateSection;
  // The release a search named, marked in the Releases tab.
  release: string | null;
  rq: string;
  nq: string;
}

function parseView(params: URLSearchParams): View {
  const raw = params.get('tab');
  return {
    tab: (TEMPLATE_SECTIONS as readonly string[]).includes(raw ?? '') ? (raw as TemplateSection) : 'overview',
    release: params.get('release') || null,
    rq: params.get('rq') ?? '',
    nq: params.get('nq') ?? '',
  };
}

function viewParams(view: View): URLSearchParams {
  const params = new URLSearchParams();
  if (view.tab !== 'overview') params.set('tab', view.tab);
  if (view.release) params.set('release', view.release);
  if (view.rq) params.set('rq', view.rq);
  if (view.nq) params.set('nq', view.nq);
  return params;
}

export default function TemplateDetailRoute() {
  return (
    <Suspense fallback={null}>
      <TemplateDetailPage />
    </Suspense>
  );
}

// One template: its settings, releases, builds, warm pool, node copies and
// sandboxes, as tabs whose choice is in the URL.
function TemplateDetailPage() {
  // Template names are URL-safe (`[A-Za-z0-9._-]`), so the segment is the name.
  const { name } = useParams<{ name: string }>();
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const data = useTemplatesData({ viewKey: `dashboard:templates:${name}`, templateName: name });
  // A delete waits for its operation; by then the operator may have left.
  const actions = useTemplateActions(data, {
    onTemplateDeleted: () => {
      if (window.location.pathname === pathname) router.push('/dashboard/templates');
    },
  });
  const [view, update] = useUrlView(searchParams, pathname, parseView, viewParams);
  const { tab } = view;
  const [orgName, setOrgName] = useState<string | null>(null);
  const template = data.templates.find((candidate) => candidate.name === name) ?? null;

  const orgId = template?.org_id ?? null;
  useEffect(() => {
    if (!orgId) return;
    adminApi.get<Organization>(`/v1/orgs/${orgId}`).then((org) => setOrgName(org.name)).catch(() => setOrgName(null));
  }, [orgId]);

  const row = useMemo(() => (template ? templateListRows({
    templates: [template],
    releases: data.templateReleases,
    releasesLoaded: data.releasesLoaded,
    inventory: data.inventory,
    inventoryLoaded: data.inventoryLoaded,
    policies: data.warmPoolPolicies,
    clusterPools: data.supportsClusterPools,
    latestTag: LATEST_TAG,
  })[0] : null), [template, data.templateReleases, data.releasesLoaded, data.inventory, data.inventoryLoaded, data.warmPoolPolicies, data.supportsClusterPools]);

  const back = (
    <Link href="/dashboard/templates"><ArrowLeftOutlined /> Templates</Link>
  );

  if (data.templateMissing) {
    return (
      <Result
        status="404"
        title={`Template ${name} not found`}
        subTitle="It may have been deleted, or the link names another cluster's template."
        extra={back}
      />
    );
  }
  if (!template) {
    if (data.templatesError) {
      return (
        <Result
          status="error"
          title={`Template ${name} could not be loaded`}
          subTitle={data.templatesError}
          extra={(
            <Space>
              {back}
              <Button
                type="primary"
                onClick={() => void data.refresh({ includeBaseSnapshotPolicies: true }).catch(() => undefined)}
              >
                Retry
              </Button>
            </Space>
          )}
        />
      );
    }
    return <div style={{ padding: 48, textAlign: 'center' }}><Spin /></div>;
  }

  const orgLabel = template.org_id === null ? 'System' : orgName ?? template.org_id;
  const releaseCount = row?.releases.length ?? 0;
  const isSystem = SYSTEM_TEMPLATES.has(template.name);

  const items = [
    {
      key: 'overview',
      label: 'Overview',
      children: <TemplateOverview template={template} current={row?.current ?? null} newest={row?.newest ?? null} releaseCount={releaseCount} orgLabel={orgLabel} data={data} actions={actions} />,
    },
    {
      key: 'releases',
      label: `Releases${data.releasesLoaded ? ` (${releaseCount})` : ''}`,
      children: (
        <ReleasesTable
          data={data}
          actions={actions}
          templateName={template.name}
          search={view.rq}
          onSearch={(rq) => update({ rq })}
          highlight={view.release}
          onClearHighlight={() => update({ release: null })}
        />
      ),
    },
    {
      key: 'builds',
      label: 'Builds',
      children: (
        <Space orientation="vertical" size={12} style={{ width: '100%' }}>
          {isSystem && data.canAdminTemplates ? (
            <SystemTemplateRebuild
              releases={data.templateReleases}
              releasesLoaded={data.releasesLoaded}
              refreshReleases={() => data.refreshTemplateReleases({ showLoading: false })}
              title={<Title level={5} style={{ margin: 0 }}>System template rebuild</Title>}
            />
          ) : null}
          <TemplateBuildsPanel templateName={template.name} open={tab === 'builds'} reloadKey={data.refreshGeneration} />
        </Space>
      ),
    },
    {
      key: 'pool',
      label: 'Warm pool',
      children: <TemplatePool template={template} current={row?.current ?? null} data={data} actions={actions} />,
    },
    {
      key: 'nodes',
      label: 'Nodes',
      children: (
        <NodeAvailabilityTable
          data={data}
          actions={actions}
          templateName={template.name}
          search={view.nq}
          onSearch={(nq) => update({ nq })}
        />
      ),
    },
    {
      key: 'sandboxes',
      label: 'Sandboxes',
      children: (
        <Space orientation="vertical" size={12} style={{ width: '100%' }}>
          <Link href={templateSandboxesHref(template.name)}>
            <Button type="primary">Open the sandboxes of {template.name}</Button>
          </Link>
          {data.canAdminTemplates ? <SpacedVersions data={data} templateName={template.name} /> : null}
        </Space>
      ),
    },
  ];

  return (
    <div className="compact-table">
      <style>{COMPACT_TABLE_STYLE}</style>
      <div style={{ marginBottom: 4 }}>{back}</div>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between', marginBottom: 8 }}>
        <Space align="center" size={8} wrap>
          <Title level={3} style={{ margin: 0 }}>{template.name}</Title>
          <Tag style={{ marginInlineEnd: 0 }}>{orgLabel}</Tag>
          {template.desired_state !== 'present' ? <Tag color="orange" style={{ marginInlineEnd: 0 }}>{template.desired_state}</Tag> : null}
          {row?.current ? (
            <Tooltip title={row.current.content_hash}>
              <Text type="secondary">current <code>{shortHash(row.current.content_hash)}</code></Text>
            </Tooltip>
          ) : row?.newest ? (
            <Tooltip title={`No release holds ${LATEST_TAG}, so creating a sandbox by the template's name fails.`}>
              <Tag color="warning" style={{ marginInlineEnd: 0 }}>no {LATEST_TAG}</Tag>
            </Tooltip>
          ) : data.releasesLoaded ? <Tag style={{ marginInlineEnd: 0 }}>no release</Tag> : null}
        </Space>
        <Space>
          <Button
            icon={<ReloadOutlined />}
            loading={data.manualRefreshing}
            disabled={data.refreshCooldown}
            onClick={data.handleManualRefresh}
          >
            Refresh
          </Button>
          <Popconfirm
            title={`Delete template ${template.name}?`}
            okText="Delete"
            okType="danger"
            onConfirm={() => actions.handleDelete(template.name)}
          >
            <Button
              danger
              icon={<DeleteOutlined />}
              loading={actions.templateOperationRunning(`delete-template:${template.name}`)}
            >
              Delete
            </Button>
          </Popconfirm>
        </Space>
      </Space>
      <Tabs activeKey={tab} onChange={(key) => update({ tab: key as TemplateSection }, 'push')} items={items} />
      {actions.modals}
    </div>
  );
}
