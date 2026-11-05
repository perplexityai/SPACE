'use client';

import { useMemo } from 'react';
import Link from 'next/link';
import { Alert, Button, Input, Popconfirm, Select, Space, Table, Tag, Tooltip, Typography, theme } from 'antd';
import { CheckCircleFilled, ClockCircleOutlined, DeleteOutlined, SyncOutlined } from '@ant-design/icons';
import { compactNetworkPolicyLabel } from '@/components/network-policy-designer';
import NetworkPolicyGenerationTag from '@/components/network-policy-generation-tag';
import type { ColumnsType, SorterResult } from 'antd/es/table/interface';
import { LATEST_TAG, shortHash } from '@/lib/template-format';
import {
  BUILD_BUCKETS,
  DEFAULT_LIST_STATE,
  PAGE_SIZES,
  buildsToRead,
  clampPage,
  filterTemplateRows,
  needsEveryBuild,
  releaseMatches,
  sortTemplateRows,
  templateListRows,
  withListChange,
  type FilteredRow,
  type LatestBuild,
  type SortKey,
  type TemplateListState,
} from '@/lib/template-list';
import { orgOptionLabels } from '@/lib/org-filter';
import type { Organization } from '@/lib/types';
import { LatestBuildCell } from './template-cells';
import type { TemplateActions } from './template-actions';
import type { TemplatesData } from './use-templates-data';
import { useLatestBuilds } from './use-latest-builds';
import { templateHref } from './links';

const { Text } = Typography;
const NO_BUILDS: ReadonlyMap<string, LatestBuild> = new Map();
const SYSTEM_TEMPLATES = new Set(['seed', 'code-runtime']);
const RELEASE_MATCHES_SHOWN = 5;

interface Props {
  // Opens the system-template rebuild.
  onRebuild: () => void;
  data: TemplatesData;
  actions: TemplateActions;
  state: TemplateListState;
  onChange: (state: TemplateListState) => void;
  // Null while loading or when the organizations could not be read.
  orgs: Organization[] | null;
}

// Every template, one compact row each, searched, filtered, sorted and paged
// in the browser. The row links to the template's page.
export default function TemplatesList({ data, actions, state, onChange, orgs, onRebuild }: Props) {
  const {
    templates, templateReleases, releasesLoaded, inventory, inventoryLoaded, warmPoolPolicies, supportsClusterPools,
    loadingTemplates, refreshGeneration,
  } = data;
  const { handleDelete, templateOperationRunning } = actions;
  const { token } = theme.useToken();
  const change = (next: Partial<TemplateListState>) => onChange(withListChange(state, next));

  // Organization names need not be unique: a repeated one carries its slug.
  const orgNames = useMemo(() => new Map(orgOptionLabels(orgs ?? []).map((option) => [option.value, option.label])), [orgs]);
  const orgLabel = (orgId: string | null) => (orgId === null ? 'System' : orgNames.get(orgId) ?? orgId.slice(0, 8));

  const rows = useMemo(() => templateListRows({
    templates,
    releases: templateReleases,
    releasesLoaded,
    inventory,
    inventoryLoaded,
    policies: warmPoolPolicies,
    clusterPools: supportsClusterPools,
    latestTag: LATEST_TAG,
  }), [templates, templateReleases, releasesLoaded, inventory, inventoryLoaded, warmPoolPolicies, supportsClusterPools]);

  const matches = useMemo(() => releaseMatches(templateReleases, state.q), [templateReleases, state.q]);
  // Every filter but the build state, which needs each row's latest build.
  const candidates = useMemo(
    () => filterTemplateRows(rows, { ...state, build: null }, NO_BUILDS, matches).rows,
    [rows, state, matches],
  );
  const everyBuild = needsEveryBuild(state);
  const sortedCandidates = useMemo(
    () => (everyBuild ? candidates : sortTemplateRows(candidates, state, NO_BUILDS, orgLabel)),
    [candidates, everyBuild, state.sort, state.order, orgNames],
  );
  const pageOf = (list: FilteredRow[]) => {
    const page = clampPage(state.page, state.size, list.length);
    return list.slice((page - 1) * state.size, page * state.size);
  };
  const wanted = buildsToRead(pageOf(sortedCandidates), candidates, state);
  const { builds, unsupported: buildsUnsupported } = useLatestBuilds(wanted, refreshGeneration);

  const filtered = useMemo(
    () => (everyBuild ? filterTemplateRows(rows, state, builds, matches) : { rows: sortedCandidates, pendingBuilds: 0, unreadBuilds: 0 }),
    [everyBuild, rows, state, builds, matches, sortedCandidates],
  );
  const shown = useMemo(
    () => (everyBuild ? sortTemplateRows(filtered.rows, state, builds, orgLabel) : filtered.rows),
    [everyBuild, filtered, state.sort, state.order, builds, orgNames],
  );
  const page = clampPage(state.page, state.size, shown.length);
  const buildsPending = everyBuild ? candidates.filter((row) => !builds.has(row.name)).length : 0;

  const orgOptions = useMemo(() => {
    const present = new Set(templates.map((template) => template.org_id).filter((id): id is string => id !== null));
    return [
      { value: 'system', label: 'System (no organization)' },
      ...[...present].map((id) => ({ value: id, label: orgLabel(id) })).sort((a, b) => a.label.localeCompare(b.label)),
    ];
  }, [templates, orgNames]);
  const tagOptions = useMemo(
    () => [...new Set(rows.flatMap((row) => row.tagged.flatMap((entry) => entry.tags)))]
      .sort((a, b) => a.localeCompare(b))
      .map((tag) => ({ value: tag, label: tag })),
    [rows],
  );
  const filtersSet = state.q || state.org || state.release || state.pool || state.build || state.tag;

  const sortable = (key: SortKey) => ({
    key,
    sorter: true,
    sortOrder: state.sort === key ? state.order : null,
    sortDirections: ['ascend', 'descend'] as ('ascend' | 'descend')[],
  });

  const columns: ColumnsType<FilteredRow> = [
    {
      title: 'Name',
      ...sortable('name'),
      width: 190,
      fixed: 'left',
      render: (_, row) => (
        <Space orientation="vertical" size={0}>
          <Link href={templateHref(row.name)} style={{ fontWeight: 600 }}>{row.name}</Link>
          {row.matched.slice(0, 2).map((match) => (
            <Link key={match.release.id} href={templateHref(row.name, 'releases', match.release.id)} style={{ fontSize: 12 }}>
              {match.reason === 'tag' ? 'tag' : match.reason === 'id' ? 'release' : 'hash'} match: {shortHash(match.release.content_hash)}
            </Link>
          ))}
          {row.template.desired_state !== 'present' ? <Tag color="orange" style={{ marginInlineEnd: 0 }}>{row.template.desired_state}</Tag> : null}
        </Space>
      ),
    },
    {
      title: 'Organization',
      ...sortable('org'),
      width: 120,
      render: (_, row) => (row.orgId === null ? <Text type="secondary">System</Text> : (
        <Tooltip title={row.orgId}><span>{orgLabel(row.orgId)}</span></Tooltip>
      )),
    },
    {
      title: 'Current release',
      ...sortable('release'),
      width: 262,
      render: (_, row) => {
        if (row.published === null) return <Text type="secondary">…</Text>;
        if (!row.newest) {
          return (
            <Tooltip title="No release has been published for this template: it may never have been built, every build may have failed, or its releases were unpublished.">
              <Tag style={{ marginInlineEnd: 0 }}>no release</Tag>
            </Tooltip>
          );
        }
        if (!row.current) {
          return (
            <Space size={4} wrap={false}>
              <Tooltip title={`No release holds ${LATEST_TAG}, so creating a sandbox by the template's name fails.`}>
                <Tag color="warning" style={{ marginInlineEnd: 0 }}>no {LATEST_TAG}</Tag>
              </Tooltip>
              <Tooltip title={row.newest.content_hash}>
                <Text type="secondary" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
                  newest <code>{shortHash(row.newest.content_hash)}</code>
                </Text>
              </Tooltip>
              {row.tagged.length > 0 ? (
                <Link href={templateHref(row.name, 'releases')} style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
                  {row.tagged.length} tagged
                </Link>
              ) : null}
            </Space>
          );
        }
        // One line: the current release with its tags, and how many other
        // releases carry a tag.
        const currentTags = row.tagged.find((entry) => entry.release.id === row.current!.id)?.tags ?? [];
        const others = row.tagged.filter((entry) => entry.release.id !== row.current!.id);
        return (
          <Space size={4} wrap={false}>
            {currentTags.slice(0, 3).map((tag) => (
              <Tag key={tag} color={tag === LATEST_TAG ? 'green' : 'blue'} style={{ marginInlineEnd: 0 }}>{tag}</Tag>
            ))}
            {currentTags.length > 3 ? <Text type="secondary" style={{ fontSize: 12 }}>+{currentTags.length - 3}</Text> : null}
            <Tooltip title={row.current.content_hash}><code style={{ fontSize: 12 }}>{shortHash(row.current.content_hash)}</code></Tooltip>
            {others.length > 0 ? (
              <Tooltip
                title={(
                  <Space orientation="vertical" size={0}>
                    {others.map((entry) => <span key={entry.release.id}>{entry.tags.join(', ')}: {shortHash(entry.release.content_hash)}</span>)}
                  </Space>
                )}
              >
                <Link href={templateHref(row.name, 'releases')} style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
                  +{others.length} tagged
                </Link>
              </Tooltip>
            ) : null}
          </Space>
        );
      },
    },
    {
      title: 'Latest build',
      ...sortable('build'),
      width: 140,
      render: (_, row) => <LatestBuildCell latest={builds.get(row.name)} now={Date.now()} />,
    },
    {
      title: 'Pool',
      ...sortable('pool'),
      width: 100,
      render: (_, row) => {
        const { pool } = row;
        if (!pool.active) {
          return pool.disabled > 0
            ? <Text type="secondary">{pool.disabled} disabled</Text>
            : <Text type="secondary">No pool</Text>;
        }
        const label = pool.kind === 'direct' && pool.target === 0 ? `${pool.ready}` : `${pool.ready}/${pool.target}`;
        return (
          <Space size={4} wrap={false}>
            <Tooltip title={pool.kind === 'direct' ? `current ${pool.ready} / target ${pool.target}` : `ready ${pool.ready} / target ${pool.target} across enabled release pools`}>
              <Tag color="purple" style={{ marginInlineEnd: 0 }}>{label}</Tag>
            </Tooltip>
            {pool.policies > 0 ? (
              <Tooltip title={`${pool.policies} enabled rolling polic${pool.policies === 1 ? 'y' : 'ies'}`}>
                <SyncOutlined aria-label="Rolling policy" />
              </Tooltip>
            ) : null}
          </Space>
        );
      },
    },
    {
      // The policy and whether nodes applied its newest generation.
      title: 'Network',
      width: 124,
      render: (_, row) => {
        const label = compactNetworkPolicyLabel(row.template.network_policy);
        return (
          <NetworkPolicyGenerationTag
            desired={row.template.network_policy_generation}
            applied={row.template.network_policy_status?.observed_generation}
            policyLabel={label}
            desiredSource="template"
            observedSource={row.template.network_policy_status?.observed_source}
          />
        );
      },
    },
    {
      title: 'Snap',
      width: 50,
      align: 'center',
      render: (_, row) => (
        <Tooltip title={row.template.has_base_snapshot ? 'Base snapshot observed' : 'No base snapshot observed'}>
          {row.template.has_base_snapshot ? (
            <CheckCircleFilled style={{ color: token.colorSuccess, fontSize: 13 }} aria-label="Base snapshot observed" />
          ) : (
            <ClockCircleOutlined style={{ color: token.colorTextTertiary, fontSize: 13 }} aria-label="No base snapshot observed" />
          )}
        </Tooltip>
      ),
    },
    {
      title: 'Nodes',
      ...sortable('nodes'),
      width: 76,
      align: 'right',
      render: (_, row) => {
        if (row.nodeCount === null) {
          return (
            <Tooltip title={row.current ? 'The node inventory has not loaded' : `No release holds ${LATEST_TAG}`}>
              <Text type="secondary">—</Text>
            </Tooltip>
          );
        }
        return (
          <Tooltip title={`Reachable nodes holding ${row.name}@${shortHash(row.current?.content_hash)}`}>
            <Link href={templateHref(row.name, 'nodes')}>{row.nodeCount}</Link>
          </Tooltip>
        );
      },
    },
    {
      title: 'Actions',
      fixed: 'right',
      width: 100,
      render: (_, row) => (
        <Space size={2}>
          {SYSTEM_TEMPLATES.has(row.name) && data.canAdminTemplates ? (
            <Tooltip title="Rebuild seed and code-runtime from this API release's recipe">
              <Button type="text" size="small" onClick={onRebuild}>
                Rebuild
              </Button>
            </Tooltip>
          ) : null}
          <Popconfirm
            title={`Delete template ${row.name}?`}
            okText="Delete"
            okType="danger"
            onConfirm={() => handleDelete(row.name)}
          >
            <Button
              type="text"
              size="small"
              danger
              icon={<DeleteOutlined />}
              loading={templateOperationRunning(`delete-template:${row.name}`)}
              aria-label={`Delete template ${row.name}`}
            />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Space orientation="vertical" size={8} style={{ width: '100%' }}>
      <Space wrap size={8}>
        <Input.Search
          allowClear
          value={state.q}
          onChange={(event) => change({ q: event.target.value })}
          placeholder="Search name, or a hash prefix, release ID or tag"
          aria-label="Search templates"
          style={{ width: 300 }}
        />
        <Select
          allowClear
          showSearch
          optionFilterProp="label"
          placeholder="Organization"
          aria-label="Organization filter"
          value={state.org ?? undefined}
          onChange={(value) => change({ org: value ?? null })}
          options={orgOptions}
          style={{ width: 170 }}
        />
        <Select
          allowClear
          placeholder="Release"
          aria-label="Release filter"
          value={state.release ?? undefined}
          onChange={(value) => change({ release: value ?? null })}
          options={[{ value: 'published', label: 'Has a release' }, { value: 'unpublished', label: 'No release' }]}
          style={{ width: 130 }}
        />
        <Select
          allowClear
          placeholder="Warm pool"
          aria-label="Warm pool filter"
          value={state.pool ?? undefined}
          onChange={(value) => change({ pool: value ?? null })}
          options={[{ value: 'with', label: 'Has a warm pool' }, { value: 'without', label: 'No warm pool' }]}
          style={{ width: 140 }}
        />
        <Tooltip title={buildsUnsupported ? "This API release does not list a template's builds." : undefined}>
          <Select
            allowClear
            placeholder="Latest build"
            aria-label="Latest build filter"
            disabled={buildsUnsupported && !state.build}
            value={state.build ?? undefined}
            onChange={(value) => change({ build: value ?? null })}
            options={BUILD_BUCKETS.map((bucket) => ({ value: bucket, label: bucket === 'running' ? 'running or queued' : bucket }))}
            style={{ width: 150 }}
          />
        </Tooltip>
        <Select
          allowClear
          showSearch
          placeholder="Tag"
          aria-label="Tag filter"
          value={state.tag ?? undefined}
          onChange={(value) => change({ tag: value ?? null })}
          options={tagOptions}
          style={{ width: 120 }}
        />
        {filtersSet ? (
          <Button onClick={() => onChange({ ...DEFAULT_LIST_STATE, sort: state.sort, order: state.order, size: state.size })}>
            Clear
          </Button>
        ) : null}
      </Space>
      {matches.length > 0 ? (
        <Alert
          type="info"
          showIcon
          title={`${matches.length} release${matches.length === 1 ? '' : 's'} match${matches.length === 1 ? 'es' : ''} “${state.q.trim()}”`}
          description={(
            <Space orientation="vertical" size={0}>
              {matches.slice(0, RELEASE_MATCHES_SHOWN).map(({ release, reason }) => (
                <Link key={release.id} href={templateHref(release.template_name, 'releases', release.id)}>
                  {release.template_name}@{shortHash(release.content_hash)}
                  <Text type="secondary">{' '}({reason === 'tag' ? 'tag' : reason === 'id' ? 'release ID' : 'content hash'})</Text>
                </Link>
              ))}
              {matches.length > RELEASE_MATCHES_SHOWN ? (
                <Text type="secondary">and {matches.length - RELEASE_MATCHES_SHOWN} more; their templates are in the table below.</Text>
              ) : null}
            </Space>
          )}
        />
      ) : null}
      {filtered.unreadBuilds > 0 ? (
        <Text type="warning">
          {filtered.unreadBuilds} template{filtered.unreadBuilds === 1 ? '' : 's'} left out: the latest build could not be read.
        </Text>
      ) : null}
      {buildsPending > 0 ? (
        <Text type="secondary">
          Reading the latest build of {buildsPending} more template{buildsPending === 1 ? '' : 's'}…
        </Text>
      ) : null}
      <Table<FilteredRow>
        columns={columns}
        dataSource={shown}
        rowKey="name"
        loading={loadingTemplates}
        size="small"
        scroll={{ x: 1160 }}
        pagination={{
          current: page,
          pageSize: state.size,
          total: shown.length,
          showSizeChanger: true,
          pageSizeOptions: PAGE_SIZES.map(String),
          showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} templates${total !== templates.length ? ` (${templates.length} in all)` : ''}`,
        }}
        onChange={(pagination, _filters, sorter, extra) => {
          if (extra.action === 'sort') {
            const sort = sorter as SorterResult<FilteredRow>;
            const key = (sort.columnKey as SortKey | undefined) ?? DEFAULT_LIST_STATE.sort;
            change({ sort: sort.order ? key : DEFAULT_LIST_STATE.sort, order: sort.order ?? DEFAULT_LIST_STATE.order });
            return;
          }
          change({ page: pagination.current ?? 1, size: pagination.pageSize ?? state.size });
        }}
        locale={{ emptyText: filtersSet ? 'No template matches the search and filters' : undefined }}
      />
    </Space>
  );
}
