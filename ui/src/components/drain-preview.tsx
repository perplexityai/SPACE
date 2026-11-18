'use client';

import Link from 'next/link';
import { Alert, Button, Card, Descriptions, Progress, Space, Table, Tag, Typography } from 'antd';

import {
  PREVIEW_CHANGES_NOTHING,
  PREVIEW_SKIPS_ORPHAN_INVENTORY,
  failureLabel,
  isWait,
  pendingCounts,
  previewVerdict,
  refusalsText,
  verdictText,
  type DrainPreview,
  type PreviewFailure,
  type PreviewRow,
} from '@/lib/drain-preview';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';

const { Text, Title } = Typography;

// A sandbox the node names, linked to its page.
function SandboxRef({ id }: { id: string }) {
  const publicId = publicSandboxIdFromAny(id);
  return <Link href={`/dashboard/sandboxes/${publicId}`}><code>{publicId}</code></Link>;
}

function verdictTag(verdict: PreviewRow['verdict']) {
  switch (verdict) {
    case 'block':
      return <Tag color="red">would block</Tag>;
    case 'wait':
      return <Tag color="gold">would wait</Tag>;
    case 'finish':
      return <Tag color="green">would finish</Tag>;
    default:
      return <Tag color="orange">error</Tag>;
  }
}

// Rows keyed by their position: the node may report the same item twice,
// such as two uploads of one sandbox.
function byPosition<T extends object>(rows: T[]): (T & { key: number })[] {
  return rows.map((row, key) => ({ ...row, key }));
}

// What a pending sandbox waits on, colored by whether a drain moves it.
function waitTag(wait: string) {
  return <Tag color={isWait(wait) ? 'gold' : undefined}>{wait}</Tag>;
}

// The node page's panel: the last preview's verdict, every blocker, and what
// a drain would do or wait for.
export function DrainPreviewPanel({
  preview,
  failure,
  loading,
  previewedAt,
}: {
  preview: DrainPreview | null;
  failure: PreviewFailure | null;
  loading: boolean;
  previewedAt: number | null;
}) {
  const counts = preview ? pendingCounts(preview.pending) : [];
  return (
    <Card
      size="small"
      title="v2 drain preview"
      id="drain-preview"
      loading={loading && !preview && !failure}
      extra={previewedAt ? <Text type="secondary">previewed {new Date(previewedAt).toLocaleTimeString()}</Text> : null}
    >
      <Alert
        type="info"
        showIcon
        title={PREVIEW_CHANGES_NOTHING}
        description={PREVIEW_SKIPS_ORPHAN_INVENTORY}
        style={{ marginBottom: 12 }}
      />
      {failure ? (
        <Alert
          type={failure.kind === 'error' ? 'error' : 'warning'}
          showIcon
          title={`Could not preview this node (${failureLabel(failure.kind)})`}
          description={
            <>
              <div>{failure.message}</div>
              {refusalsText(failure.refusals) ? (
                <div data-testid="drain-preview-refusals">{refusalsText(failure.refusals)}</div>
              ) : null}
            </>
          }
          style={{ marginBottom: 12 }}
        />
      ) : null}
      {preview ? (
        <>
          <Space style={{ marginBottom: 8 }} wrap>
            <span data-testid="drain-preview-verdict-tag">{verdictTag(previewVerdict(preview))}</span>
            <Text strong data-testid="drain-preview-verdict">{verdictText(preview)}</Text>
          </Space>
          <Descriptions size="small" column={2}>
            <Descriptions.Item label="Already evacuated">{preview.evacuated.length}</Descriptions.Item>
            <Descriptions.Item label="Running">{preview.pending.filter((p) => p.running).length}</Descriptions.Item>
            <Descriptions.Item label="Pending, by wait" span="filled">
              {counts.length ? (
                <Space size={4} wrap>
                  {counts.map(([wait, n]) => <span key={wait}>{waitTag(wait)} {n}</span>)}
                </Space>
              ) : <Text type="secondary">nothing</Text>}
            </Descriptions.Item>
          </Descriptions>

          <Title level={5} style={{ marginTop: 12 }}>Blockers ({preview.blocked.length})</Title>
          <Table
            size="small"
            dataSource={byPosition(preview.blocked)}
            pagination={false}
            locale={{ emptyText: 'Nothing would block a drain' }}
            columns={[{ title: 'Reason', dataIndex: 'reason' }]}
          />

          <Title level={5} style={{ marginTop: 12 }}>What a drain would do ({preview.pending.length})</Title>
          <Table
            size="small"
            dataSource={byPosition(preview.pending)}
            pagination={preview.pending.length > 20 ? { pageSize: 20 } : false}
            locale={{ emptyText: 'Nothing to do or wait for' }}
            columns={[
              { title: 'Sandbox', dataIndex: 'sandbox_id', width: 340, render: (v: string) => <SandboxRef id={v} /> },
              { title: 'Wait', dataIndex: 'wait', width: 120, render: (v: string) => waitTag(v) },
              { title: 'Running', dataIndex: 'running', width: 90, render: (v: boolean) => (v ? 'yes' : 'no') },
              { title: 'Reason', dataIndex: 'reason', render: (v: string | undefined) => v ?? <Text type="secondary">—</Text> },
            ]}
          />
        </>
      ) : !failure && !loading ? (
        <Text type="secondary">Not previewed yet.</Text>
      ) : null}
    </Card>
  );
}

// The nodes list's results: progress while the previews run, then one row a
// node.
export function DrainPreviewAll({
  total,
  rows,
  running,
  onCancel,
  onClose,
}: {
  total: number;
  rows: PreviewRow[];
  running: boolean;
  onCancel: () => void;
  onClose: () => void;
}) {
  const blocking = rows.filter((row) => row.verdict === 'block').length;
  const waiting = rows.filter((row) => row.verdict === 'wait').length;
  const failed = rows.filter((row) => row.verdict === 'error').length;
  return (
    <Card
      size="small"
      title={`v2 drain preview of ${total} worker ${total === 1 ? 'node' : 'nodes'}`}
      id="drain-preview-all"
      style={{ marginBottom: 16 }}
      extra={running ? <Button size="small" onClick={onCancel}>Stop</Button> : <Button size="small" onClick={onClose}>Close</Button>}
    >
      <Alert type="info" showIcon title={PREVIEW_CHANGES_NOTHING} style={{ marginBottom: 12 }} />
      <Space style={{ marginBottom: 8 }} wrap>
        <Progress
          percent={total ? Math.round((rows.length / total) * 100) : 100}
          size="small"
          status={running ? 'active' : 'normal'}
          style={{ width: 240 }}
        />
        <Text data-testid="drain-preview-progress">{rows.length} of {total} previewed</Text>
        {rows.length ? <Text type="secondary">{blocking} would block, {waiting} would wait, {failed} failed</Text> : null}
      </Space>
      <Table
        size="small"
        rowKey="nodeId"
        dataSource={rows}
        pagination={rows.length > 50 ? { pageSize: 50 } : false}
        locale={{ emptyText: running ? 'Previewing…' : 'No worker nodes listed' }}
        columns={[
          {
            title: 'Node', dataIndex: 'name',
            render: (name: string, row) => <Link href={`/dashboard/nodes/${row.nodeId}`}>{name}</Link>,
          },
          {
            title: 'Verdict', dataIndex: 'verdict', width: 170,
            render: (verdict: PreviewRow['verdict'], row) => (
              <Space size={4}>
                {verdictTag(verdict)}
                {row.failure ? <Text type="secondary">{failureLabel(row.failure.kind)}</Text> : null}
              </Space>
            ),
          },
          { title: 'Blockers', dataIndex: 'blockers', width: 90, render: (n: number, row) => (row.failure ? '—' : n) },
          {
            title: 'First reasons', dataIndex: 'reasons',
            render: (reasons: string[], row) => (
              <Space orientation="vertical" size={0}>
                {reasons.length ? reasons.map((reason) => (
                  <Text key={reason} type={row.failure ? 'danger' : undefined}>{reason}</Text>
                )) : <Text type="secondary">{row.pending ? `${row.pending} to do or wait for` : 'none'}</Text>}
              </Space>
            ),
          },
        ]}
      />
    </Card>
  );
}
