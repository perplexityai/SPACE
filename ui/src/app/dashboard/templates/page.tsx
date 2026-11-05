'use client';

import { Suspense, useEffect, useState } from 'react';
import { usePathname, useSearchParams } from 'next/navigation';
import { Button, Space, Tabs, Typography } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { adminApi } from '@/lib/api';
import { listStateParams, parseListState, type TemplateListState } from '@/lib/template-list';
import type { Organization } from '@/lib/types';
import { useTemplatesData } from './use-templates-data';
import { useTemplateActions } from './template-actions';
import TemplatesList from './templates-list';
import ReleasesTable from './releases-table';
import NodeAvailabilityTable from './node-availability-table';
import WarmPoolPoliciesTable from './warm-pool-policies-table';
import SpacedVersions from './spaced-versions';
import { COMPACT_TABLE_STYLE } from './template-cells';

const { Title } = Typography;
const TEMPLATES_VIEW_REFRESH_KEY = 'dashboard:templates';

type Tab = 'templates' | 'releases' | 'policies' | 'nodes' | 'spaced';
const TABS: readonly Tab[] = ['templates', 'releases', 'policies', 'nodes', 'spaced'];

// `useSearchParams` needs a Suspense boundary for `next build`; the state is
// read from the URL synchronously on mount, so the fallback never shows.
export default function TemplatesPageRoute() {
  return (
    <Suspense fallback={null}>
      <TemplatesPage />
    </Suspense>
  );
}

// The Templates page: the searchable template list, and the views across
// templates (every release, warm pool policies, node copies, spaced
// versions) as tabs. The search, filters, sort, page and tab are in the URL,
// written with `replaceState` so a view can be shared and survives a reload
// without adding history entries.
function TemplatesPage() {
  const searchParams = useSearchParams();
  const pathname = usePathname();
  const data = useTemplatesData({ viewKey: TEMPLATES_VIEW_REFRESH_KEY });
  const actions = useTemplateActions(data);
  const [tab, setTab] = useState<Tab>(() => {
    const raw = searchParams.get('tab');
    return (TABS as readonly string[]).includes(raw ?? '') ? (raw as Tab) : 'templates';
  });
  const [listState, setListState] = useState<TemplateListState>(() => parseListState(searchParams));
  const [releaseSearch, setReleaseSearch] = useState(() => searchParams.get('rq') ?? '');
  const [nodeSearch, setNodeSearch] = useState(() => searchParams.get('nq') ?? '');
  const [orgs, setOrgs] = useState<Organization[] | null>(null);

  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then(setOrgs).catch(() => setOrgs(null));
  }, []);

  useEffect(() => {
    const params = listStateParams(listState);
    if (tab !== 'templates') params.set('tab', tab);
    if (releaseSearch) params.set('rq', releaseSearch);
    if (nodeSearch) params.set('nq', nodeSearch);
    const query = params.toString();
    window.history.replaceState(null, '', query ? `${pathname}?${query}` : pathname);
  }, [listState, tab, releaseSearch, nodeSearch, pathname]);

  const items = [
    {
      key: 'templates',
      label: `Templates${data.templatesLoaded ? ` (${data.templates.length})` : ''}`,
      children: <TemplatesList data={data} actions={actions} state={listState} onChange={setListState} orgs={orgs} />,
    },
    {
      key: 'releases',
      label: `All releases${data.releasesLoaded ? ` (${data.templateReleases.length})` : ''}`,
      children: <ReleasesTable data={data} actions={actions} search={releaseSearch} onSearch={setReleaseSearch} />,
    },
    ...(data.supportsClusterPools ? [{
      key: 'policies',
      label: 'Warm pool policies',
      children: <WarmPoolPoliciesTable data={data} actions={actions} />,
    }] : []),
    {
      key: 'nodes',
      label: 'Node availability',
      children: <NodeAvailabilityTable data={data} actions={actions} search={nodeSearch} onSearch={setNodeSearch} />,
    },
    ...(data.canAdminTemplates ? [{
      key: 'spaced',
      label: 'spaced versions',
      children: <SpacedVersions data={data} />,
    }] : []),
  ];

  return (
    <div className="compact-table">
      <style>{COMPACT_TABLE_STYLE}</style>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between', marginBottom: 8 }}>
        <Title level={3} style={{ margin: 0 }}>Templates</Title>
        <Button
          icon={<ReloadOutlined />}
          loading={data.manualRefreshing}
          disabled={data.refreshCooldown}
          onClick={data.handleManualRefresh}
        >
          Refresh
        </Button>
      </Space>
      <Tabs
        activeKey={items.some((item) => item.key === tab) ? tab : 'templates'}
        onChange={(key) => setTab(key as Tab)}
        items={items}
      />
      {actions.modals}
    </div>
  );
}
