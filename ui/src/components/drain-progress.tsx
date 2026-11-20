'use client';

import Link from 'next/link';
import { Alert, Card, Descriptions, Input, Progress, Radio, Space, Table, Tag, Tooltip, Typography } from 'antd';

import {
  HOLD_PRESETS,
  compactDuration,
  drainProgress,
  evacuatedRows,
  holdDeadlineMs,
  parseHoldSeconds,
  pendingKind,
  pendingRows,
  shortenLengthens,
  type DrainProgressState,
  type EvacuatedRow,
  type PendingRow,
} from '@/lib/drain-progress';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
import type { AdminNodeDrain } from '@/lib/types';

const { Text } = Typography;

function SandboxRef({ id }: { id: string }) {
  const publicId = publicSandboxIdFromAny(id);
  return <Link href={`/dashboard/sandboxes/${publicId}`}><code>{publicId}</code></Link>;
}

function stateColor(state: DrainProgressState): string {
  switch (state) {
    case 'finished':
      return 'green';
    case 'holding':
      return 'blue';
    case 'evacuating':
      return 'orange';
    default:
      return 'default';
  }
}

function time(iso: string | null): string {
  return iso ? new Date(iso).toLocaleString() : '—';
}

// A v2 drain's progress on the node page: the headline, the counts, and the
// node's last report, as the cluster controller stored it (`drain` on the
// admin node response). The evacuated count is every sandbox the controller
// recorded as evacuated in this drain (`evacuated_total`), which a released
// sandbox stays in, with the ones the last report lists.
export function DrainProgressPanel({ drain, nowMs }: { drain: AdminNodeDrain; nowMs: number }) {
  const progress = drainProgress(drain, nowMs);
  const { counts } = progress;
  const pending = pendingRows(drain, nowMs);
  const evacuated = evacuatedRows(drain);
  return (
    <Card size="small" title="Drain progress" id="drain-progress">
      <Space orientation="vertical" size={12} style={{ width: '100%' }}>
        <Space wrap>
          <Tag color={stateColor(progress.state)}>{progress.headline}</Tag>
          {drain.fenced ? <Tag color="red">instance fenced for termination</Tag> : null}
          {progress.stalledSinceMs !== null ? (
            <Tooltip title="Past its deadline, the node has reported no further sandbox evacuated for 15 minutes. The drain goes on; Last error says what it still waits for.">
              <Tag color="red">stalled since {new Date(progress.stalledSinceMs).toLocaleString()}</Tag>
            </Tooltip>
          ) : null}
        </Space>
        {progress.state === 'cancelling' ? (
          <Text type="secondary">
            Undrained. The node stays cordoned until the cluster controller has cancelled the drain on it; a sandbox the drain suspended stays suspended until a call wakes it.
          </Text>
        ) : null}
        <Progress
          percent={counts.total ? Math.floor((counts.evacuated / counts.total) * 100) : 100}
          format={() => `${counts.evacuated}/${counts.total} evacuated`}
          size="small"
        />
        <Descriptions size="small" column={3}>
          <Descriptions.Item label="Deadline">{time(drain.deadline)}</Descriptions.Item>
          <Descriptions.Item label="Evacuated">
            {counts.evacuated}{counts.recording ? ` (${counts.recording} recording)` : ''}
          </Descriptions.Item>
          <Descriptions.Item label="Pending">
            {counts.pending}{counts.held ? ` (${counts.held} held)` : ''}
            {counts.poolMembers ? ` (${counts.poolMembers} warm-pool)` : ''}
          </Descriptions.Item>
          <Descriptions.Item label="Blocked">
            {counts.blocked ? <Text type="danger">{counts.blocked}</Text> : 0}
          </Descriptions.Item>
          <Descriptions.Item label="Reported">{time(drain.reported_at)}</Descriptions.Item>
          <Descriptions.Item label="Generation"><Text code>{drain.generation}</Text></Descriptions.Item>
        </Descriptions>
        {drain.blocked.length ? (
          <Space orientation="vertical" size={0}>
            <Text strong>Blocked</Text>
            {drain.blocked.map((entry, index) => <Text key={`${entry.reason}:${index}`} type="danger">{entry.reason}</Text>)}
          </Space>
        ) : null}
        {pending.length ? (
          <Table<PendingRow>
            size="small"
            rowKey="key"
            pagination={pending.length > 20 ? { pageSize: 20 } : false}
            dataSource={pending}
            title={() => <Text strong>Pending</Text>}
            columns={[
              {
                title: 'Sandbox',
                render: (_, row) => (
                  <Space size={4}>
                    <SandboxRef id={row.sandbox_id} />
                    {row.poolMember ? <Tag color="purple">{pendingKind(row)}</Tag> : null}
                  </Space>
                ),
              },
              {
                title: 'Wait',
                render: (_, row) => (
                  <Space size={4}>
                    <Tag>{row.wait}</Tag>
                    {row.running ? <Text type="secondary">running</Text> : null}
                  </Space>
                ),
              },
              {
                title: 'State',
                render: (_, row) => (
                  <Space orientation="vertical" size={0}>
                    {row.note ? <Text>{row.note}</Text> : null}
                    {row.reason ? <Text type="secondary">{row.reason}</Text> : null}
                    {row.sinceMs !== null ? (
                      <Text type="secondary">
                        in this state since {new Date(row.sinceMs).toLocaleString()} ({row.inStateFor} ago)
                      </Text>
                    ) : null}
                  </Space>
                ),
              },
            ]}
          />
        ) : null}
        {evacuated.length ? (
          <Table<EvacuatedRow>
            size="small"
            rowKey="sandbox_id"
            pagination={evacuated.length > 20 ? { pageSize: 20 } : false}
            dataSource={evacuated}
            title={() => <Text strong>Evacuated</Text>}
            columns={[
              { title: 'Sandbox', render: (_, row) => <SandboxRef id={row.sandbox_id} /> },
              { title: 'Artifact', render: (_, row) => row.kind },
              { title: 'Was running', render: (_, row) => (row.wasRunning ? 'yes' : '—') },
              {
                title: 'Controller',
                render: (_, row) => <Tag color={row.status === 'recorded' ? 'green' : 'gold'}>{row.status}</Tag>,
              },
            ]}
          />
        ) : null}
      </Space>
    </Card>
  );
}

// A v2 drain whose `drain` the API left out: it does so when the stored
// state doesn't read, so the page says that rather than showing nothing.
export function DrainProgressUnreadable() {
  return (
    <Card size="small" title="Drain progress" id="drain-progress">
      <Alert
        type="warning"
        showIcon
        title="This v2 drain's progress can't be shown"
        description="The API reported a v2 drain on this node but no readable progress: it leaves the drain out when the state the cluster controller stored doesn't read. The phase, Last error and the actions above still apply."
      />
    </Card>
  );
}

// The hold a v2 drain's start or shorten sends: a preset or a custom number
// of seconds. `choice` is a preset's seconds, or `custom` with `custom`'s
// text. A start's deadline counts from now; a shorten's from the drain's
// start, `startedAt`, which the page says so the operator sees one that has
// already passed, and the drain's current deadline, `currentDeadline`, which
// a shorten may only move earlier.
export function HoldPicker({
  shorten,
  startedAt,
  currentDeadline,
  nowMs,
  choice,
  custom,
  onChange,
}: {
  shorten: boolean;
  startedAt: string | null;
  currentDeadline?: string | null;
  nowMs: number;
  choice: number | 'custom';
  custom: string;
  onChange: (choice: number | 'custom', custom: string) => void;
}) {
  const seconds = choice === 'custom' ? parseHoldSeconds(custom) : choice;
  const deadline = seconds === null ? null : holdDeadlineMs(seconds, startedAt, shorten, nowMs);
  // `currentDeadline` undefined: the drain's deadline isn't known.
  const lengthens = shorten && currentDeadline !== undefined ? shortenLengthens(deadline, currentDeadline) : null;
  return (
    <Space orientation="vertical" size={4} style={{ width: '100%' }}>
      <Text strong>{shorten ? 'Hold, counted from the drain\'s start' : 'Hold running sandboxes for'}</Text>
      {shorten && currentDeadline !== undefined ? (
        <Text type="secondary">
          Current deadline: {currentDeadline ? new Date(currentDeadline).toLocaleString() : 'none'}
        </Text>
      ) : null}
      <Radio.Group
        value={choice}
        onChange={(e) => onChange(e.target.value as number | 'custom', custom)}
        options={[
          ...HOLD_PRESETS.map((preset) => ({ label: preset.label, value: preset.seconds })),
          { label: 'Custom', value: 'custom' },
        ]}
      />
      {choice === 'custom' ? (
        <Input
          aria-label="Hold in seconds"
          value={custom}
          onChange={(e) => onChange('custom', e.target.value)}
          suffix="seconds"
          status={seconds === null ? 'error' : undefined}
          style={{ maxWidth: 240 }}
        />
      ) : null}
      {seconds === null ? (
        <Text type="danger">A whole number of seconds, at most a year.</Text>
      ) : lengthens ? (
        <Text type="danger">{lengthens}</Text>
      ) : deadline === null ? (
        <Text type="secondary">The drain&apos;s start isn&apos;t known; the API counts the hold from it.</Text>
      ) : (
        <Text type="secondary">
          Deadline {new Date(deadline).toLocaleString()}
          {deadline <= nowMs ? ': already passed, so the hold ends at once' : ` (in ${compactDuration(deadline - nowMs)})`}
        </Text>
      )}
    </Space>
  );
}
