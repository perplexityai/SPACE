'use client';

import { useEffect, useRef, useState } from 'react';
import { Alert, App, Button, Popconfirm, Space, Table, Tooltip, Typography, theme } from 'antd';
import { DeleteOutlined } from '@ant-design/icons';
import { adminApi } from '@/lib/api';
import { idempotencyKey } from '@/lib/public-api';
import { publicSandboxIdFromAny, sandboxLabelOrId } from '@/lib/sandbox-id';
import { buildsRead, errorMessage, shortHash, type BuildsView } from '@/lib/template-format';
import { isMissingRoute, pageRows } from '@/lib/admin-fallback';
import { latestRead } from '@/lib/template-reads';
import type { PoolInfo, TemplateBuildListEntry } from '@/lib/types';
import { BuildStateTag } from './template-cells';
import { encodedPathSegment, type TemplatePoolStatusResponse } from './use-templates-data';

const { Text } = Typography;

const TEMPLATE_BUILDS_LIMIT = 20;

// The template's newest builds, from `GET /templates/{name}/builds`. Releases
// that predate the route answer 405 (the path is POST-only there). While
// `open`, the panel reads them again on each of the page's refreshes
// (`reloadKey`), keeping the rows it shows until the new answer arrives;
// closed, it reads nothing (`buildsRead`). Each answer decides afresh whether
// the route exists.
export function TemplateBuildsPanel({ templateName, open, reloadKey }: { templateName: string; open: boolean; reloadKey: number }) {
  const { token } = theme.useToken();
  const [builds, setBuilds] = useState<TemplateBuildListEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [unsupported, setUnsupported] = useState(false);
  // A page refresh joins a read already out; opening the panel, another
  // template or Retry supersedes it.
  const [reads] = useState(() => latestRead<TemplateBuildListEntry[]>());

  const load = (background = false) => {
    if (!background) {
      setBuilds(null);
      setError(null);
    }
    const read = (signal: AbortSignal) => adminApi
      .get<unknown>(`/v1/templates/${encodedPathSegment(templateName)}/builds?limit=${TEMPLATE_BUILDS_LIMIT}`, { signal })
      .then((body) => pageRows<TemplateBuildListEntry>(body).rows);
    const write = {
      value: (rows: TemplateBuildListEntry[]) => {
        setBuilds(rows);
        setError(null);
        setUnsupported(false);
      },
      error: (err: unknown) => {
        const missing = isMissingRoute(err);
        setUnsupported(missing);
        setError(missing ? null : errorMessage(err));
        setBuilds((shown) => (background && shown ? shown : []));
      },
    };
    void (background ? reads.poll(read, write) : reads.next(read, write)).catch(() => undefined);
  };

  const shown = useRef<BuildsView | null>(null);
  useEffect(() => {
    const view = { templateName, open, reloadKey };
    const read = buildsRead(shown.current, view);
    shown.current = view;
    if (read) load(read === 'background');
  }, [templateName, open, reloadKey]);

  if (unsupported) {
    return <Text type="secondary">This API release does not list a template&apos;s builds.</Text>;
  }

  return (
    <div>
      {error ? (
        <Alert
          type="error"
          showIcon
          title={`Could not load builds: ${error}`}
          action={<Button size="small" onClick={() => load()}>Retry</Button>}
          style={{ margin: '8px 0' }}
        />
      ) : null}
      <Table<TemplateBuildListEntry>
        size="small"
        rowKey="job_id"
        loading={builds === null}
        dataSource={builds ?? []}
        pagination={false}
        locale={{ emptyText: error ? 'Not loaded' : 'No builds recorded' }}
        columns={[
          {
            title: 'Created',
            dataIndex: 'created_at',
            width: 190,
            render: (value: string) => new Date(value).toLocaleString(),
          },
          {
            title: 'State',
            dataIndex: 'state',
            width: 130,
            render: (state: TemplateBuildListEntry['state'], row) => <BuildStateTag state={state} statusError={row.status_error} />,
          },
          {
            title: 'Content hash',
            dataIndex: 'content_hash',
            width: 140,
            render: (hash: string | null) => hash
              ? <Tooltip title={hash}><Text code>{shortHash(hash)}</Text></Tooltip>
              : <Text type="secondary">—</Text>,
          },
          {
            title: 'Error',
            render: (_, row) => (
              <Space orientation="vertical" size={0}>
                {row.error ? <Text type="danger">{row.error}</Text> : null}
                {row.status_error ? (
                  <Text style={{ color: token.colorTextTertiary }}>Status not read: {row.status_error}</Text>
                ) : null}
                {!row.error && !row.status_error ? <Text type="secondary">—</Text> : null}
              </Space>
            ),
          },
          {
            title: 'Job',
            dataIndex: 'job_id',
            width: 140,
            render: (jobId: string) => <Text code copyable={{ text: jobId }}>{jobId.slice(0, 8)}</Text>,
          },
        ]}
      />
    </div>
  );
}

export function PoolSandboxesPanel({ templateName }: { templateName: string }) {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(true);
  const [poolInfo, setPoolInfo] = useState<PoolInfo | null>(null);
  const [deletingSandbox, setDeletingSandbox] = useState<string | null>(null);

  const refreshPool = async () => {
    setLoading(true);
    try {
      const status = await adminApi.get<TemplatePoolStatusResponse>(
        `/v1/templates/${encodedPathSegment(templateName)}/pool`,
      );
      const target = status.policy?.target_count ?? status.observed?.pool_target ?? 0;
      setPoolInfo(status.policy || status.observed ? {
        template: status.template,
        target_count: target,
        current_count: status.observed?.pool_current ?? 0,
        pending_count: status.observed?.pool_pending ?? 0,
        sandbox_ids: status.observed?.sandbox_ids ?? [],
      } : null);
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refreshPool();
  }, [templateName]);

  const deleteSandbox = async (sandboxId: string) => {
    setDeletingSandbox(sandboxId);
    try {
      await adminApi.post(
        `/v1/sandboxes/${publicSandboxIdFromAny(sandboxId)}:force-delete`,
        undefined,
        { 'Idempotency-Key': idempotencyKey() },
      );
      await refreshPool();
      message.success('Pool sandbox delete queued');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      setDeletingSandbox(null);
    }
  };

  const sandboxIds = poolInfo?.sandbox_ids ?? [];

  return (
    <Table
      size="small"
      loading={loading}
      dataSource={sandboxIds.map((id) => ({ id }))}
      rowKey="id"
      pagination={false}
      locale={{ emptyText: 'No observed pool sandboxes' }}
      columns={[
        {
          title: 'Sandbox',
          dataIndex: 'id',
          render: (id: string) => <code>{sandboxLabelOrId(id)}</code>,
        },
        {
          title: 'Actions',
          width: 80,
          render: (_, record: { id: string }) => (
            <Popconfirm
              title={`Delete pool sandbox ${sandboxLabelOrId(record.id)}?`}
              okText="Delete"
              okType="danger"
              onConfirm={() => deleteSandbox(record.id)}
            >
              <Button
                type="text"
                size="small"
                danger
                icon={<DeleteOutlined />}
                loading={deletingSandbox === record.id}
                aria-label="Delete pool sandbox"
              />
            </Popconfirm>
          ),
        },
      ]}
    />
  );
}
