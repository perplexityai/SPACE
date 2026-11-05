'use client';

import { Button, Descriptions, Space, Switch, Tag, Tooltip, Typography, theme } from 'antd';
import { CheckCircleFilled, ClockCircleOutlined } from '@ant-design/icons';
import { compactNetworkPolicyLabel } from '@/components/network-policy-designer';
import NetworkPolicyGenerationTag from '@/components/network-policy-generation-tag';
import { disabledAutoLifecycle, summarizeAutoLifecycle } from '@/components/auto-lifecycle';
import { LATEST_TAG, shortHash } from '@/lib/template-format';
import Link from 'next/link';
import { visibleTemplateTags } from '@/lib/template-list';
import type { Template, TemplateRelease } from '@/lib/types';
import { templateHref } from './links';
import { formatShortDateTime, renderDiskCell } from './template-cells';
import type { TemplateActions } from './template-actions';
import type { TemplatesData } from './use-templates-data';

const { Text } = Typography;

function hasAutoLifecyclePolicy(policy: Template['default_auto_lifecycle']) {
  return JSON.stringify(policy ?? disabledAutoLifecycle()) !== JSON.stringify(disabledAutoLifecycle());
}

function baseSnapshotTooltip(template: Template, desired: boolean) {
  return (
    <Space orientation="vertical" size={0}>
      <span>Desired: {desired ? 'enabled' : 'disabled'}</span>
      <span>Observed: {template.has_base_snapshot ? 'ready' : 'not ready'}</span>
    </Space>
  );
}

interface Props {
  template: Template;
  // The release new sandboxes get by name, and how many releases it has.
  current: TemplateRelease | null;
  releaseCount: number;
  orgLabel: string;
  data: TemplatesData;
  actions: TemplateActions;
}

// A template's settings: what its builder reports, its default auto
// lifecycle, network policy and base snapshot policy.
export default function TemplateOverview({ template, current, releaseCount, orgLabel, data, actions }: Props) {
  const { token } = theme.useToken();
  const policy = data.baseSnapshotPolicies[template.name];
  const desired = policy?.ensure_base_snapshot ?? false;
  const customLifecycle = hasAutoLifecyclePolicy(template.default_auto_lifecycle);
  const networkLabel = compactNetworkPolicyLabel(template.network_policy);
  const published = releaseCount > 0;

  return (
    <Descriptions size="small" bordered column={{ xs: 1, sm: 1, md: 2, lg: 2, xl: 2, xxl: 2 }} styles={{ label: { width: 180 } }}>
      <Descriptions.Item label="Organization">{orgLabel}</Descriptions.Item>
      <Descriptions.Item label="State">{template.desired_state}</Descriptions.Item>
      <Descriptions.Item label="Description" span={2}>
        {template.description || <Text type="secondary">—</Text>}
      </Descriptions.Item>
      <Descriptions.Item label="Current release" span={2}>
        {current ? (
          <Space size={4} wrap>
            <Tooltip title={current.content_hash}><code>{shortHash(current.content_hash)}</code></Tooltip>
            {visibleTemplateTags(current.tags, current.content_hash).map((tag) => (
              <Tag key={tag} color={tag === LATEST_TAG ? 'green' : 'blue'} style={{ marginInlineEnd: 0 }}>{tag}</Tag>
            ))}
            <Text type="secondary">created {formatShortDateTime(current.created_at)};</Text>
            <Link href={templateHref(template.name, 'releases')}>{releaseCount} release{releaseCount === 1 ? '' : 's'}</Link>
          </Space>
        ) : <Text type="secondary">{data.releasesLoaded ? 'No release has been published' : '…'}</Text>}
      </Descriptions.Item>
      <Descriptions.Item label="Builder hash">
        {template.content_hash ? (
          <Tooltip title={template.content_hash}><code>{shortHash(template.content_hash)}</code></Tooltip>
        ) : (
          <Tooltip title={published ? 'Published; no builder node reports a build hash for it.' : 'No release has been published for this template.'}>
            <Text type="secondary">{published ? '—' : 'no release'}</Text>
          </Tooltip>
        )}
      </Descriptions.Item>
      <Descriptions.Item label="Built">{template.built_at ? formatShortDateTime(template.built_at) : '—'}</Descriptions.Item>
      <Descriptions.Item label="Runtime">{template.runtime || '—'}{template.arch ? ` / ${template.arch}` : ''}</Descriptions.Item>
      <Descriptions.Item label="Shape">
        {template.vcpu ? `${template.vcpu}c` : '—'} / {template.memory_mib ? `${template.memory_mib} MiB` : '—'}
      </Descriptions.Item>
      <Descriptions.Item label="Disk">{renderDiskCell(template)}</Descriptions.Item>
      <Descriptions.Item label="Created">{template.created_at ? formatShortDateTime(template.created_at) : '—'}</Descriptions.Item>
      <Descriptions.Item label="Default auto lifecycle" span={2}>
        <Space size={4}>
          <Tooltip title={summarizeAutoLifecycle(template.default_auto_lifecycle)}>
            <Tag color={customLifecycle ? 'blue' : undefined} style={{ marginRight: 0 }}>
              {customLifecycle ? 'custom' : 'disabled'}
            </Tag>
          </Tooltip>
          <Text type="secondary">{summarizeAutoLifecycle(template.default_auto_lifecycle)}</Text>
          <Button type="text" size="small" onClick={() => actions.openTemplateLifecycleModal(template)}>
            Edit
          </Button>
        </Space>
      </Descriptions.Item>
      <Descriptions.Item label="Network" span={2}>
        <Space size={4} wrap>
          <Tag color={networkLabel === 'open' ? 'green' : undefined} style={{ marginRight: 0 }}>{networkLabel}</Tag>
          <NetworkPolicyGenerationTag
            desired={template.network_policy_generation}
            applied={template.network_policy_status?.observed_generation}
            policyLabel={networkLabel}
            desiredSource="template"
            observedSource={template.network_policy_status?.observed_source}
          />
          <Button type="text" size="small" onClick={() => actions.editTemplateNetwork(template)}>
            Edit
          </Button>
        </Space>
      </Descriptions.Item>
      <Descriptions.Item label="Base snapshot" span={2}>
        <Space size={8}>
          <Tooltip title={baseSnapshotTooltip(template, desired)}>
            <Switch
              size="small"
              checked={desired}
              loading={actions.buildingSnapshot === template.name}
              onChange={(checked) => actions.handleToggleBaseSnapshot(template, checked)}
              aria-label="Ensure a base snapshot"
            />
          </Tooltip>
          <Text type="secondary">Ensure a base snapshot</Text>
          {template.has_base_snapshot ? (
            <Space size={4}><CheckCircleFilled style={{ color: token.colorSuccess }} /> observed</Space>
          ) : (
            <Space size={4}><ClockCircleOutlined style={{ color: token.colorTextTertiary }} /> not observed</Space>
          )}
        </Space>
      </Descriptions.Item>
    </Descriptions>
  );
}
