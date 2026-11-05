'use client';

import { useMemo } from 'react';
import { Button, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { LATEST_TAG, shortHash } from '@/lib/template-format';
import { visibleTemplateTags } from '@/lib/template-list';
import type { Template, TemplateRelease, TemplateReleaseWarmPool } from '@/lib/types';
import { hasObservedPoolSandboxes, warmPoolLabel, warmPoolTooltip } from './template-cells';
import { PoolSandboxesPanel } from './template-builds';
import WarmPoolPoliciesTable from './warm-pool-policies-table';
import type { TemplateActions } from './template-actions';
import type { TemplatesData } from './use-templates-data';

const { Title, Text } = Typography;

interface PoolRow {
  key: string;
  release: TemplateRelease;
  pool: TemplateReleaseWarmPool;
}

interface Props {
  template: Template;
  // The release new sandboxes get by name (the `latest` holder), where a
  // new pool goes.
  current: TemplateRelease | null;
  data: TemplatesData;
  actions: TemplateActions;
}

// A template's warm pools: on Kubernetes, its release pools and rolling
// policies; on a direct substrate, its template pool and the sandboxes in it.
export default function TemplatePool({ template, current, data, actions }: Props) {
  const { supportsClusterPools, isDirectSubstrate, canAdminTemplates, templateReleases, warmPoolPolicyCapabilities } = data;
  const rows = useMemo<PoolRow[]>(
    () => templateReleases
      .filter((release) => release.template_name === template.name)
      .flatMap((release) => release.warm_pools.map((pool) => ({ key: pool.id, release, pool }))),
    [templateReleases, template.name],
  );

  if (isDirectSubstrate) {
    const hasPool = hasObservedPoolSandboxes(template);
    const poolCurrent = template.pool_current ?? 0;
    const target = template.pool_target;
    return (
      <Space orientation="vertical" size={12} style={{ width: '100%' }}>
        <Space size={8}>
          {hasPool ? (
            <Tag color="purple" style={{ marginRight: 0 }}>
              {target !== null && target !== undefined ? `current ${poolCurrent} / target ${target}` : `current ${poolCurrent}`}
            </Tag>
          ) : <Text type="secondary">No pool</Text>}
          <Button size="small" onClick={() => actions.openPoolModal(template)}>
            {hasPool ? 'Manage pool' : 'Create pool'}
          </Button>
        </Space>
        {hasPool ? <PoolSandboxesPanel templateName={template.name} /> : null}
      </Space>
    );
  }

  if (!supportsClusterPools) return <Text type="secondary">This substrate has no warm pools.</Text>;

  return (
    <Space orientation="vertical" size={16} style={{ width: '100%' }}>
      <div>
        <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
          <Title level={5} style={{ margin: 0 }}>Release pools</Title>
          {/* As in the Releases table, Create is offered only on a release
              with no pool: the API's create updates a pool of the same
              release and shape and makes it manual, which would reset its
              target and detach it from its rolling policy without asking. */}
          {canAdminTemplates && current && current.warm_pools.length === 0 ? (
            <Button onClick={() => actions.openClusterPoolModal(current)}>
              Create pool on {shortHash(current.content_hash)} ({LATEST_TAG})
            </Button>
          ) : null}
        </Space>
        <Table<PoolRow>
          size="small"
          rowKey="key"
          dataSource={rows}
          pagination={false}
          style={{ marginTop: 8 }}
          locale={{ emptyText: 'No warm pool on any of this template\'s releases' }}
          columns={[
            {
              title: 'Release',
              render: (_, row) => (
                <Space size={4}>
                  <Tooltip title={row.release.content_hash}><code>{shortHash(row.release.content_hash)}</code></Tooltip>
                  {visibleTemplateTags(row.release.tags, row.release.content_hash).map((tag) => <Tag key={tag} style={{ marginInlineEnd: 0 }}>{tag}</Tag>)}
                </Space>
              ),
            },
            {
              title: 'Pool',
              render: (_, row) => (
                <Tooltip title={warmPoolTooltip(row.pool, Boolean(warmPoolPolicyCapabilities.disk_capacity))}>
                  <Tag
                    color={!row.pool.enabled ? undefined : row.pool.managed_by === 'autoscaler' ? 'purple' : 'gold'}
                    style={{ marginRight: 0 }}
                  >
                    {warmPoolLabel(row.pool)} {row.pool.managed_by}
                  </Tag>
                </Tooltip>
              ),
            },
            { title: 'Shape', render: (_, row) => `${row.pool.vcpu}c / ${row.pool.memory_mib} MiB` },
            { title: 'Warming', render: (_, row) => row.pool.warming_slots },
            { title: 'Claimed', render: (_, row) => row.pool.claimed_slots },
            { title: 'Namespace', render: (_, row) => row.pool.namespace },
            {
              title: 'Actions',
              width: 90,
              render: (_, row) => (canAdminTemplates ? (
                <Button type="text" size="small" onClick={() => actions.openClusterPoolModal(row.release, row.pool.id)}>
                  Manage
                </Button>
              ) : <Text type="secondary">—</Text>),
            },
          ]}
        />
      </div>
      <div>
        <WarmPoolPoliciesTable data={data} actions={actions} templateName={template.name} />
      </div>
    </Space>
  );
}
