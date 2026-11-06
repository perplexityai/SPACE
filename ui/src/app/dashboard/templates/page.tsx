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
import SystemTemplates from './system-templates';
import { COMPACT_TABLE_STYLE } from './template-cells';
import { useUrlView } from './use-url-view';

const { Title } = Typography;
const TEMPLATES_VIEW_REFRESH_KEY = 'dashboard:templates';

type Tab = 'templates' | 'releases' | 'policies' | 'nodes' | 'spaced' | 'system';
const TABS: readonly Tab[] = ['templates', 'releases', 'policies', 'nodes', 'spaced', 'system'];

interface View {
  tab: Tab;
  list: TemplateListState;
  // The All releases and Node availability searches.
  rq: string;
  nq: string;
}

function parseView(params: URLSearchParams): View {
  const raw = params.get('tab');
  return {
    tab: (TABS as readonly string[]).includes(raw ?? '') ? (raw as Tab) : 'templates',
    list: parseListState(params),
    rq: params.get('rq') ?? '',
    nq: params.get('nq') ?? '',
  };
}

function viewParams(view: View): URLSearchParams {
  const params = listStateParams(view.list);
  if (view.tab !== 'templates') params.set('tab', view.tab);
  if (view.rq) params.set('rq', view.rq);
  if (view.nq) params.set('nq', view.nq);
  return params;
}

// `useSearchParams` needs a Suspense boundary for `next build`; the state is
// read from the URL synchronously on mount, so the fallback never shows.
export default function TemplatesPageRoute() {
  return (
    <Suspense fallback={null}>
      <TemplatesPage />
    </Suspense>
  );
}

// The Templates page: the searchable template list, the views across
// templates (every release, warm pool policies, node copies, spaced
// versions) and the system-template rebuild, as tabs. The URL holds the tab,
// search, filters, sort and page (`useUrlView`), so a view can be shared,
// survives a reload, and follows links and Back.
function TemplatesPage() {
  const searchParams = useSearchParams();
  const pathname = usePathname();
  const data = useTemplatesData({ viewKey: TEMPLATES_VIEW_REFRESH_KEY });
  const actions = useTemplateActions(data);
  const [view, update] = useUrlView(searchParams, pathname, parseView, viewParams);
  const [orgs, setOrgs] = useState<Organization[] | null>(null);

  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then(setOrgs).catch(() => setOrgs(null));
  }, []);

  const items = [
    {
      key: 'templates',
      label: `Templates${data.templatesLoaded ? ` (${data.templates.length})` : ''}`,
      children: (
        <TemplatesList
          data={data}
          actions={actions}
          state={view.list}
          onChange={(list) => update({ list })}
          orgs={orgs}
          onRebuild={() => update({ tab: 'system' }, 'push')}
        />
      ),
    },
    {
      key: 'releases',
      label: `All releases${data.releasesLoaded ? ` (${data.templateReleases.length})` : ''}`,
      children: <ReleasesTable data={data} actions={actions} search={view.rq} onSearch={(rq) => update({ rq })} />,
    },
    ...(data.supportsClusterPools ? [{
      key: 'policies',
      label: 'Warm pool policies',
      children: <WarmPoolPoliciesTable data={data} actions={actions} />,
    }] : []),
    {
      key: 'nodes',
      label: 'Node availability',
      children: <NodeAvailabilityTable data={data} actions={actions} search={view.nq} onSearch={(nq) => update({ nq })} />,
    },
    ...(data.canAdminTemplates ? [{
      key: 'spaced',
      label: 'spaced versions',
      children: <SpacedVersions data={data} />,
    }, {
      key: 'system',
      label: 'System templates',
      children: <SystemTemplates data={data} actions={actions} />,
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
        activeKey={items.some((item) => item.key === view.tab) ? view.tab : 'templates'}
        onChange={(key) => update({ tab: key as Tab }, 'push')}
        items={items}
      />
      {actions.modals}
    </div>
  );
}
