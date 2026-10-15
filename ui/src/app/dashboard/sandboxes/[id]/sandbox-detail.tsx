'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { useParams, useRouter } from 'next/navigation';
import {
  Button,
  Card,
  Descriptions,
  Form,
  Input,
  App,
  Popconfirm,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
  theme,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  ArrowLeftOutlined,
  CameraOutlined,
  ClearOutlined,
  CodeOutlined,
  CopyOutlined,
  DeleteOutlined,
  DesktopOutlined,
  DisconnectOutlined,
  LockOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  RightOutlined,
  RollbackOutlined,
  SaveOutlined,
  SendOutlined,
  StopOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { sandboxLabelOrId, sandboxUuidPart } from '@/lib/sandbox-id';
import { runSandboxCommand, type ProcessRunResult } from '@/lib/data-plane-process';
import {
  openSandboxTerminal,
  type TerminalConnection,
  type TerminalSessionEvent,
} from '@/lib/data-plane-terminal';
import {
  forceConsoleSandboxAction,
  forceDeleteConsoleSandbox,
  getConsoleSandbox,
  getPublicPage,
  publicSnapshotToConsoleSnapshot,
  publicTemplateToConsoleTemplate,
  postPublicOperation,
  updateConsoleSandbox,
  waitForAdminOperation,
} from '@/lib/public-api';
import type {
  NetworkPolicyV1,
  PublicSnapshot,
  PublicTemplate,
  Sandbox,
  SnapshotInfo,
  Template,
} from '@/lib/types';
import { tablePagination } from '@/lib/table-pagination';
import NetworkPolicyDesigner, {
  compactNetworkPolicyLabel,
  NetworkPolicyDetails,
  networkPoliciesEqual,
  summarizeNetworkPolicy,
} from '@/components/network-policy-designer';
import NetworkPolicyGenerationTag from '@/components/network-policy-generation-tag';

const { Title, Text, Paragraph } = Typography;

const STATE_ACTIONS: Record<string, Record<string, boolean>> = {
  creating:  { start: false, wake: false, stop: false, kill: true,  pause: false, resume: false, suspend: false, restore: false },
  created:   { start: true,  wake: true,  stop: false, kill: false, pause: false, resume: false, suspend: false, restore: false },
  started:   { start: false, wake: false, stop: true,  kill: true,  pause: true,  resume: false, suspend: true,  restore: false },
  running:   { start: false, wake: false, stop: true,  kill: true,  pause: true,  resume: false, suspend: true,  restore: false },
  stopped:   { start: true,  wake: true,  stop: false, kill: false, pause: false, resume: false, suspend: false, restore: false },
  paused:    { start: false, wake: true,  stop: false, kill: false, pause: false, resume: true,  suspend: false, restore: false },
  suspended: { start: false, wake: true,  stop: false, kill: false, pause: false, resume: false, suspend: false, restore: true },
  deleting:  { start: false, wake: false, stop: false, kill: false, pause: false, resume: false, suspend: false, restore: false },
  deleted:   { start: false, wake: false, stop: false, kill: false, pause: false, resume: false, suspend: false, restore: false },
  failed:    { start: false, wake: false, stop: false, kill: false, pause: false, resume: false, suspend: false, restore: false },
};

const TRANSIENT_ACTIONS = {
  start: false, wake: false, stop: false, kill: false,
  pause: false, resume: false, suspend: false, restore: false,
};

const TERMINAL_MIN_SPACED_VERSION_LABEL = '0.1.2';
const TERMINAL_MIN_SPACED_VERSION: SemVer = { major: 0, minor: 1, patch: 2 };

function sandboxIdWithoutDashes(sandboxId: string): string {
  return sandboxUuidPart(sandboxId).replaceAll('-', '');
}

type DatadogTimeWindowParams = {
  from_ts: string;
  to_ts: string;
  live: 'true';
};

function datadogTimeWindowParams(createdAt: string | null): DatadogTimeWindowParams {
  const now = Date.now();
  const createdAtMs = createdAt ? Date.parse(createdAt) : Number.NaN;
  const fromTs = Number.isFinite(createdAtMs) ? Math.min(createdAtMs, now) : now;

  return {
    from_ts: String(fromTs),
    to_ts: String(now),
    live: 'true',
  };
}

function datadogLogsUrl(sandbox: Sandbox, timeWindowParams: DatadogTimeWindowParams): string {
  const sandboxUuid = sandboxUuidPart(sandbox.id);
  const sandboxPodName = `space-sbx-${sandboxIdWithoutDashes(sandbox.id)}-*`;
  const params = new URLSearchParams({
    query: `(*:${sandbox.id} OR ${sandbox.id} OR *:${sandboxUuid} OR ${sandboxUuid} OR ${sandboxPodName} OR *:${sandboxPodName})`,
    agg_m: 'count',
    agg_m_source: 'base',
    agg_t: 'count',
    cols: 'host,service',
    fromUser: 'true',
    messageDisplay: 'expanded-md',
    refresh_mode: 'paused',
    storage: 'hot',
    stream_sort: 'time,asc',
    viz: 'stream',
    ...timeWindowParams,
  });

  return `https://app.datadoghq.com/logs?${params.toString()}`;
}

function datadogDebugDashboardUrl(
  sandbox: Sandbox,
  timeWindowParams: DatadogTimeWindowParams,
  dashboardUrl: string,
): string {
  const sandboxUuid = sandboxUuidPart(sandbox.id);
  const sandboxPodName = `space-sbx-${sandboxIdWithoutDashes(sandbox.id)}*`;
  const params = new URLSearchParams({
    fromUser: 'false',
    page: '0',
    refresh_mode: 'sliding',
    'tpl_var_new-variable[0]': '*',
    'tpl_var_sandbox_id[0]': sandboxUuid,
    'tpl_var_sandbox_pod_name[0]': sandboxPodName,
    ...timeWindowParams,
  });

  const url = new URL(dashboardUrl);
  params.forEach((value, key) => url.searchParams.set(key, value));
  return url.toString();
}

function statusTag(status: string | null, failureReason?: string | null) {
  if (!status) return <Tag>unknown</Tag>;
  const colorMap: Record<string, string> = {
    started: 'green', running: 'green', created: 'gold', stopped: 'default',
    paused: 'orange', suspended: 'purple', failed: 'red',
  };
  const tag = (
    <Tag color={status.endsWith('ing') ? 'processing' : (colorMap[status] || 'default')}>
      {status}
    </Tag>
  );
  if (status === 'failed' && failureReason) {
    return <Tooltip title={failureReason}>{tag}</Tooltip>;
  }
  return tag;
}

function formatBytes(n: number | null | undefined) {
  if (n === null || n === undefined) return '—';
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)} KB`;
  return `${n} B`;
}

type SemVer = {
  major: number;
  minor: number;
  patch: number;
};

function parseSemVer(value: string): SemVer | null {
  const match = /^v?(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$/.exec(value.trim());
  if (!match) return null;
  return {
    major: Number.parseInt(match[1], 10),
    minor: Number.parseInt(match[2], 10),
    patch: Number.parseInt(match[3], 10),
  };
}

function compareSemVer(left: SemVer, right: SemVer): number {
  if (left.major !== right.major) return left.major > right.major ? 1 : -1;
  if (left.minor !== right.minor) return left.minor > right.minor ? 1 : -1;
  if (left.patch !== right.patch) return left.patch > right.patch ? 1 : -1;
  return 0;
}

type TerminalVersionStatus =
  | { supported: true; message: null }
  | { supported: false; message: string };

function terminalVersionStatus(sandbox: Sandbox): TerminalVersionStatus {
  const version = sandbox.spaced_version?.trim() || null;
  if (!version) {
    return {
      supported: false,
      message: `Terminal is available for sandboxes running spaced ${TERMINAL_MIN_SPACED_VERSION_LABEL} or later. This sandbox has not reported a spaced version yet.`,
    };
  }
  const parsed = parseSemVer(version);
  if (!parsed) {
    return {
      supported: false,
      message: `Terminal is available for sandboxes running spaced ${TERMINAL_MIN_SPACED_VERSION_LABEL} or later. This sandbox reported an invalid spaced version: ${version}.`,
    };
  }
  if (compareSemVer(parsed, TERMINAL_MIN_SPACED_VERSION) < 0) {
    return {
      supported: false,
      message: `Terminal is available for sandboxes running spaced ${TERMINAL_MIN_SPACED_VERSION_LABEL} or later. This sandbox reports spaced ${version}.`,
    };
  }
  return { supported: true, message: null };
}

function spacedVersionDisplay(sandbox: Sandbox) {
  const version = sandbox.spaced_version?.trim();
  if (!version || !parseSemVer(version)) return <Text type="secondary">-</Text>;
  return version;
}

function nodeRoleColor(role: string | null | undefined) {
  switch (role) {
    case 'worker': return 'blue';
    case 'builder': return 'green';
    case 'cluster': return 'gold';
    default: return 'default';
  }
}

function nodeDisplay(sandbox: Sandbox) {
  const active = sandbox.active_node;
  const node = active;
  if (!node) return <Text type="secondary">unassigned</Text>;
  const label = node.name || node.address || node.id.slice(0, 8);

  const tag = (
    <Tag color={nodeRoleColor(node.role)}>
      {label}
    </Tag>
  );

  return (
    <Tooltip
      title={(
        <div>
          <div><strong>ID:</strong> {node.id}</div>
          <div><strong>Role:</strong> {node.role || 'node'}</div>
          {node.address ? <div><strong>Address:</strong> {node.address}</div> : null}
          {node.managed_by ? <div><strong>Managed by:</strong> {node.managed_by}</div> : null}
        </div>
      )}
    >
      {tag}
    </Tooltip>
  );
}

function sandboxTemplateHash(sandbox: Sandbox) {
  return sandbox.content_hash || sandbox.image_hash;
}

function isDigestTag(tag: string) {
  return tag.startsWith('sha256:') || /^[a-f0-9]{64}$/i.test(tag);
}

function normalizedDigest(value: string) {
  return value.replace(/^sha256:/, '');
}

function visibleReleaseTags(tags: string[] | null | undefined, contentHash: string | null | undefined) {
  const contentDigest = contentHash ? normalizedDigest(contentHash) : null;
  return (tags || []).filter((tag) => {
    if (isDigestTag(tag)) return false;
    return !contentDigest || normalizedDigest(tag) !== contentDigest;
  });
}

function releaseTagsDisplay(sandbox: Sandbox) {
  const tags = visibleReleaseTags(sandbox.release_tags, sandboxTemplateHash(sandbox));
  if (tags.length === 0) return '—';

  return (
    <Space wrap size={4}>
      {tags.map((tag) => (
        <Tag key={tag} color={tag === 'latest' ? 'green' : 'blue'} style={{ marginRight: 0 }}>
          {tag}
        </Tag>
      ))}
    </Space>
  );
}

function templateDisplay(sandbox: Sandbox) {
  if (!sandbox.template) return '—';
  const contentHash = sandboxTemplateHash(sandbox);
  return (
    <Space direction="vertical" size={0} style={{ maxWidth: '100%' }}>
      <span>{sandbox.template}</span>
      {contentHash ? (
        <Text
          code
          copyable={{ text: contentHash }}
          style={{ fontSize: 12, whiteSpace: 'normal', wordBreak: 'break-all' }}
        >
          {contentHash}
        </Text>
      ) : null}
    </Space>
  );
}

function parseCommand(input: string): string[] {
  const command = input.trim();
  return command ? ['/bin/sh', '-c', command] : [];
}

function CollapsibleCard({
  title,
  extra,
  children,
  defaultOpen = true,
}: {
  title: React.ReactNode;
  extra?: React.ReactNode;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Card
      size="small"
      title={(
        <Button
          type="text"
          size="small"
          icon={<RightOutlined rotate={open ? 90 : 0} />}
          onClick={() => setOpen((current) => !current)}
          style={{ padding: 0, height: 'auto', fontWeight: 600 }}
        >
          {title}
        </Button>
      )}
      extra={extra}
    >
      {open ? children : null}
    </Card>
  );
}

// Map the storage `type` value to the wire codec name the snapshot uses for
// its rootfs artifact. Mirrors what `scripts/space-snapshot-inspect.py sizes`
// shows in its `codec` column so the two views agree.
function snapshotCodec(type: string): { label: string; color: string } {
  if (type === 'full') return { label: 'full', color: 'green' };
  if (type === 'disk') return { label: 'btrfs-send', color: 'blue' };
  return { label: type || '—', color: 'default' };
}

function SnapshotsCard({ sandboxId }: { sandboxId: string }) {
  const { message } = App.useApp();
  const { token } = theme.useToken();
  const [snapshots, setSnapshots] = useState<SnapshotInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = () => {
    setLoading(true);
    getPublicPage<PublicSnapshot>(`/sandboxes/${sandboxId}/snapshots`)
      .then((items) => setSnapshots(items.map(publicSnapshotToConsoleSnapshot)))
      .catch((err: any) => message.error(err.message || 'failed to load snapshots'))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, [sandboxId]);

  const handleRollback = async (snapshotName: string) => {
    try {
      await postPublicOperation(`/sandboxes/${sandboxId}:rollback`, {
        snapshot_name: snapshotName,
        auto_restart: true,
      });
      message.success('Snapshot rollback queued');
    } catch (err: any) {
      message.error(err.message || 'failed to queue rollback');
    }
  };

  const totalBytes = snapshots.reduce((acc, s) => acc + (s.size_bytes || 0), 0);

  const columns: ColumnsType<SnapshotInfo> = [
    { title: 'Name', dataIndex: 'id', ellipsis: true },
    {
      title: 'Codec', dataIndex: 'type', width: 110,
      render: (v: string) => {
        const codec = snapshotCodec(v);
        return <Tag color={codec.color}>{codec.label}</Tag>;
      },
    },
    { title: 'Timestamp', dataIndex: 'timestamp', width: 200, render: (v: string | null) => v ?? '—' },
    { title: 'Size', dataIndex: 'size_bytes', width: 100, render: (v: number) => formatBytes(v) },
    {
      title: 'Action', width: 130,
      render: (_, record) => (
        <Tooltip title="Roll back sandbox to this snapshot">
          <Popconfirm
            title={`Roll back to ${record.id}?`}
            onConfirm={() => handleRollback(record.id)}
          >
            <Button type="link" size="small" icon={<RollbackOutlined />}>Rollback</Button>
          </Popconfirm>
        </Tooltip>
      ),
    },
  ];

  return (
    <Card
      size="small"
      title={<Space><CameraOutlined /> Snapshots</Space>}
      extra={<Button size="small" icon={<ReloadOutlined />} onClick={refresh}>Refresh</Button>}
    >
      <Table
        columns={columns}
        dataSource={snapshots}
        rowKey="id"
        loading={loading}
        size="small"
        pagination={tablePagination('snapshots', 10)}
        locale={{ emptyText: 'No snapshots' }}
        footer={() =>
          snapshots.length === 0 ? null : (
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: token.colorTextSecondary }}>
              <span>{snapshots.length} snapshot{snapshots.length === 1 ? '' : 's'}</span>
              <span>total {formatBytes(totalBytes)}</span>
            </div>
          )
        }
      />
    </Card>
  );
}

function ExecCard({ sandbox }: { sandbox: Sandbox }) {
  const { message } = App.useApp();
  const { token } = theme.useToken();
  const [command, setCommand] = useState('');
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<ProcessRunResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const enabled = sandbox.status === 'started' || sandbox.status === 'running';

  const run = async () => {
    const argv = parseCommand(command);
    if (argv.length === 0) {
      message.warning('Enter a command');
      return;
    }
    setRunning(true);
    setError(null);
    setResult(null);
    try {
      await runSandboxCommand(sandbox.id, argv, setResult);
    } catch (err: any) {
      setError(err.message || 'Process failed');
    } finally {
      setRunning(false);
    }
  };

  return (
    <CollapsibleCard
      title={<Space><CodeOutlined /> Command</Space>}
      extra={(
        <Text type="secondary" style={{ fontSize: 12 }}>
          {enabled ? 'Sandbox started' : `Disabled (status: ${sandbox.status || 'unknown'})`}
        </Text>
      )}
    >
      <Space direction="vertical" style={{ width: '100%' }} size={8}>
        <Input.TextArea
          value={command}
          onChange={(e) => setCommand(e.target.value)}
          placeholder='e.g. ls -la /workspace   or   bash -c "echo hi && date"'
          autoSize={{ minRows: 2, maxRows: 6 }}
          disabled={!enabled || running}
          onPressEnter={(e) => {
            if (!e.shiftKey) {
              e.preventDefault();
              void run();
            }
          }}
        />
        <Space>
          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            loading={running}
            disabled={!enabled || !command.trim()}
            onClick={run}
          >
            Run
          </Button>
        </Space>
        {error ? <Text type="danger">{error}</Text> : null}
        {result ? (
          <div style={{ borderTop: `1px solid ${token.colorBorderSecondary}`, paddingTop: 8 }}>
            <Descriptions size="small" column={3}>
              <Descriptions.Item label="Outcome">
                {result.outcome?.type === 'status'
                  ? result.outcome.status
                  : (result.outcome?.type ?? (running ? 'running' : 'unknown'))}
              </Descriptions.Item>
              <Descriptions.Item label="Exit code">
                {result.outcome?.type === 'exited' || result.outcome?.type === 'status'
                  ? result.outcome.exitCode
                  : '—'}
              </Descriptions.Item>
              <Descriptions.Item label="Session">
                {result.sessionId ?? '—'}
              </Descriptions.Item>
              <Descriptions.Item label="Bytes">
                {result.stdoutBytes} / {result.stderrBytes}
              </Descriptions.Item>
            </Descriptions>
            {result.stdout ? (
              <>
                <Text strong style={{ fontSize: 12 }}>stdout</Text>
                <pre style={{
                  background: token.colorFillQuaternary, padding: 8, borderRadius: token.borderRadiusSM,
                  fontSize: 12, maxHeight: 240, overflow: 'auto',
                }}>{result.stdout}</pre>
              </>
            ) : null}
            {result.stderr ? (
              <>
                <Text strong style={{ fontSize: 12 }}>stderr</Text>
                <pre style={{
                  background: token.colorFillQuaternary, padding: 8, borderRadius: token.borderRadiusSM,
                  fontSize: 12, maxHeight: 240, overflow: 'auto',
                }}>{result.stderr}</pre>
              </>
            ) : null}
          </div>
        ) : null}
      </Space>
    </CollapsibleCard>
  );
}

type TerminalStatus = 'disconnected' | 'connecting' | 'attached' | 'closed' | 'error';

type TerminalLine = {
  id: number;
  stream: 'stdout' | 'stderr' | 'system';
  text: string;
};

function TerminalCard({ sandbox }: { sandbox: Sandbox }) {
  const { token } = theme.useToken();
  const [status, setStatus] = useState<TerminalStatus>('disconnected');
  const [input, setInput] = useState('');
  const [lines, setLines] = useState<TerminalLine[]>([]);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const connectionRef = useRef<TerminalConnection | null>(null);
  const lineIdRef = useRef(0);
  const outputRef = useRef<HTMLPreElement | null>(null);
  const enabled = sandbox.status === 'started' || sandbox.status === 'running';
  const versionStatus = terminalVersionStatus(sandbox);
  const terminalEnabled = enabled && versionStatus.supported;
  const connecting = status === 'connecting';
  const attached = status === 'attached';
  const active = connecting || attached;

  const appendLine = (stream: TerminalLine['stream'], text: string) => {
    if (!text) return;
    lineIdRef.current += 1;
    const line = { id: lineIdRef.current, stream, text };
    setLines((current) => [...current.slice(-400), line]);
  };

  const handleTerminalEvent = (event: TerminalSessionEvent) => {
    switch (event.type) {
      case 'status':
        setStatus(event.status === 'closed' ? 'closed' : event.status);
        if (event.message) appendLine('system', `\n[${event.message}]\n`);
        break;
      case 'started':
        appendLine('system', `[process ${event.processId}${event.pid ? ` pid ${event.pid}` : ''}]\n`);
        break;
      case 'output':
        appendLine(event.stream, event.text);
        break;
      case 'accepted':
        break;
      case 'exited':
        appendLine('system', `\n[process exited: ${event.status} ${event.exitCode}]\n`);
        setStatus('closed');
        connectionRef.current = null;
        break;
      case 'reconnecting':
        appendLine('system', `\n[${event.message}]\n`);
        break;
      case 'error':
        appendLine('system', `\n[error: ${event.message}]\n`);
        setStatus('error');
        break;
    }
  };

  useEffect(() => {
    outputRef.current?.scrollTo({ top: outputRef.current.scrollHeight });
  }, [lines]);

  useEffect(() => {
    if (!terminalEnabled && connectionRef.current) {
      connectionRef.current.close();
      connectionRef.current = null;
      setStatus('closed');
    }
  }, [terminalEnabled]);

  useEffect(() => () => {
    connectionRef.current?.close();
    connectionRef.current = null;
  }, []);

  const connect = async () => {
    if (!terminalEnabled || active) return;
    setStatus('connecting');
    setLines([]);
    setSessionId(null);
    try {
      const connection = await openSandboxTerminal(sandbox.id, handleTerminalEvent, {
        cols: 100,
        rows: 30,
      });
      connectionRef.current = connection;
      setSessionId(connection.sessionId);
    } catch (err: any) {
      setStatus('error');
      appendLine('system', `[error: ${err.message || 'failed to open terminal'}]\n`);
    }
  };

  const disconnect = () => {
    connectionRef.current?.close();
    connectionRef.current = null;
    setStatus('closed');
  };

  const sendInput = () => {
    const connection = connectionRef.current;
    if (!connection || !input) return;
    connection.sendInput(`${input}\n`);
    setInput('');
  };

  const shortSessionId = sessionId && sessionId.length > 24
    ? `${sessionId.slice(0, 24)}…`
    : sessionId;
  const statusText = enabled
    ? (versionStatus.supported
      ? (shortSessionId ? `${status} · ${shortSessionId}` : status)
      : 'Disabled by spaced version')
    : `Disabled (status: ${sandbox.status || 'unknown'})`;

  return (
    <CollapsibleCard
      title={<Space><DesktopOutlined /> Terminal</Space>}
      extra={(
        <Space>
          <Text type="secondary" style={{ fontSize: 12 }}>{statusText}</Text>
          <Button
            size="small"
            icon={<ClearOutlined />}
            onClick={() => setLines([])}
            disabled={lines.length === 0}
          >
            Clear
          </Button>
          {active ? (
            <Button
              size="small"
              icon={<DisconnectOutlined />}
              onClick={disconnect}
              loading={connecting}
            >
              Disconnect
            </Button>
          ) : (
            <Button
              size="small"
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={connect}
              disabled={!terminalEnabled}
            >
              Connect
            </Button>
          )}
        </Space>
      )}
    >
      <div style={{ position: 'relative' }}>
        <Space direction="vertical" style={{ width: '100%' }} size={8}>
          <pre
            ref={outputRef}
            style={{
              background: '#101418',
              color: '#e6edf3',
              border: '1px solid rgba(255,255,255,0.08)',
              borderRadius: 4,
              fontSize: 12,
              lineHeight: 1.5,
              minHeight: 260,
              maxHeight: 420,
              overflow: 'auto',
              padding: 12,
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
            }}
          >
            {lines.length === 0 ? 'Connect to open an interactive shell.\n' : lines.map((line) => (
              <span
                key={line.id}
                style={{
                  color: line.stream === 'stderr'
                    ? '#ffb4a8'
                    : line.stream === 'system'
                      ? '#8ab4f8'
                      : '#e6edf3',
                }}
              >
                {line.text}
              </span>
            ))}
          </pre>
          <Input
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onPressEnter={(event) => {
              event.preventDefault();
              sendInput();
            }}
            disabled={!attached || !terminalEnabled}
            placeholder="Type shell input and press Enter"
            addonBefore="$"
            suffix={(
              <Button
                type="text"
                size="small"
                icon={<SendOutlined />}
                disabled={!attached || !terminalEnabled || !input}
                onClick={sendInput}
                aria-label="Send terminal input"
              />
            )}
          />
        </Space>
        {!versionStatus.supported ? (
          <div
            style={{
              position: 'absolute',
              inset: 0,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: 24,
              background: token.colorBgContainer,
              borderRadius: 4,
              textAlign: 'center',
            }}
          >
            <Text type="secondary">{versionStatus.message}</Text>
          </div>
        ) : null}
      </div>
    </CollapsibleCard>
  );
}

function ControlButtons({ sandbox, onAction, busy }: {
  sandbox: Sandbox;
  onAction: (action: string) => void;
  busy: boolean;
}) {
  const s = sandbox.status || '';
  const transient = s.endsWith('ing');
  const actions = transient ? TRANSIENT_ACTIONS : (STATE_ACTIONS[s] || TRANSIENT_ACTIONS);
  const observedOnly = sandbox.resource_type === 'observed_local';
  const poolSlot = sandbox.resource_type === 'pool_slot';

  const btn = (
    action: string,
    enabled: boolean,
    icon: React.ReactNode,
    label: string,
    danger = false,
    primary = false,
  ) => {
    const allowed = enabled && !observedOnly && !poolSlot;
    return (
      <Tooltip
        key={action}
        title={
          observedOnly
            ? 'Observed-local sandboxes are inventory only'
            : poolSlot
              ? 'Pool slots only support delete'
              : (!enabled ? `Cannot ${label.toLowerCase()} in ${s || 'unknown'} state` : label)
        }
      >
        <Button
          size="small"
          danger={danger}
          type={primary ? 'primary' : 'default'}
          icon={icon}
          disabled={!allowed || busy}
          onClick={() => onAction(action)}
        >
          {label}
        </Button>
      </Tooltip>
    );
  };

  return (
    <Space wrap>
      {btn('start',   actions.start,   <PlayCircleOutlined />, 'Start',   false, true)}
      {btn('wake',    actions.wake,    <ThunderboltOutlined />, 'Wake')}
      {btn('pause',   actions.pause,   <PauseCircleOutlined />, 'Pause')}
      {btn('resume',  actions.resume,  <PlayCircleOutlined />, 'Resume')}
      {btn('suspend', actions.suspend, <SaveOutlined />, 'Suspend')}
      {btn('restore', actions.restore, <RollbackOutlined />, 'Restore')}
      {btn('stop',    actions.stop,    <StopOutlined />, 'Stop', true)}
      {btn('kill',    actions.kill,    <StopOutlined />, 'Kill', true)}
      <Popconfirm
        title={
          observedOnly
            ? 'Delete this observed local sandbox?'
            : poolSlot
              ? `Delete this ${sandbox.pool_template || 'template'} pool slot?`
              : 'Delete this sandbox?'
        }
        description={
          observedOnly
            ? 'This deletes node-local runtime state that is not tracked by a control-plane sandbox row.'
            : poolSlot
              ? 'The pool controller may recreate it to maintain the configured target size.'
              : undefined
        }
        onConfirm={() => onAction('delete')}
        disabled={busy}
      >
        <Button
          size="small"
          danger
          icon={<DeleteOutlined />}
          disabled={busy}
        >
          Delete
        </Button>
      </Popconfirm>
    </Space>
  );
}

export default function SandboxDetailPage({ dashboardUrl }: { dashboardUrl: string | null }) {
  const { message } = App.useApp();
  const params = useParams<{ id: string }>();
  const router = useRouter();
  const id = params.id;
  const [sandbox, setSandbox] = useState<Sandbox | null>(null);
  const [templates, setTemplates] = useState<Template[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingNetwork, setEditingNetwork] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const copyToClipboard = (value: string) => {
    if (typeof navigator !== 'undefined' && navigator.clipboard) {
      void navigator.clipboard.writeText(value);
      message.success('Copied');
    }
  };

  const refresh = async (showSpinner = false) => {
    if (showSpinner) setLoading(true);
    try {
      setSandbox(await getConsoleSandbox(id));
      setError(null);
    } catch (err: any) {
      setError(err.message || 'Failed to load sandbox');
    } finally {
      if (showSpinner) setLoading(false);
    }
  };

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    void refresh(true);
    getPublicPage<PublicTemplate>('/templates?limit=1000')
      .then((items) => setTemplates(items.map(publicTemplateToConsoleTemplate)))
      .catch(() => {});
    pollRef.current = setInterval(() => {
      if (typeof document !== 'undefined' && document.hidden) return;
      void refresh(false);
    }, 4000);
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [id]);

  const templatePolicy = useMemo(() => {
    if (!sandbox) return null;
    if (sandbox.template_network_policy) return sandbox.template_network_policy;
    if (!sandbox.template) return null;
    return templates.find((t) => t.name === sandbox.template)?.network_policy ?? null;
  }, [sandbox, templates]);

  const effectivePolicy = useMemo(() => {
    if (!sandbox) return null;
    return (
      sandbox.effective_desired_network_policy ??
      sandbox.network_policy_override ??
      sandbox.network_policy ??
      templatePolicy
    );
  }, [sandbox, templatePolicy]);

  const handleAction = async (action: string) => {
    if (!sandbox) return;
    setBusy(true);
    try {
      if (action === 'delete') {
        const operation = await forceDeleteConsoleSandbox(sandbox.id);
        message.success('Sandbox delete queued');
        await waitForAdminOperation(operation.operation_id);
        message.success('Sandbox deleted');
        // The sandbox we're viewing is gone — bounce to the list. Refreshing
        // in place would 404 and then the 4 s poll loop would keep hammering
        // 404 until the user navigated away by hand.
        router.push('/dashboard/sandboxes');
        return;
      }
      const operation = await forceConsoleSandboxAction(sandbox.id, action);
      message.success(`${action} queued`);
      await waitForAdminOperation(operation.operation_id);
      message.success(`Sandbox ${action} completed`);
      await refresh(false);
    } catch (err: any) {
      message.error(err.message || `${action} failed`);
    } finally {
      setBusy(false);
    }
  };

  const handleNetworkSave = async (policy: NetworkPolicyV1) => {
    if (!sandbox) return;
    try {
      await updateConsoleSandbox(sandbox.id, {
        network_policy: { mode: 'set', policy },
      });
      setEditingNetwork(false);
      await refresh(false);
      message.success('Sandbox network policy update queued');
    } catch (err: any) {
      message.error(err.message || 'failed to update network policy');
    }
  };

  const handleNetworkClear = async () => {
    if (!sandbox) return;
    try {
      await updateConsoleSandbox(sandbox.id, {
        network_policy: { mode: 'inherit_template_default' },
      });
      setEditingNetwork(false);
      await refresh(false);
      message.success('Sandbox network policy reset queued');
    } catch (err: any) {
      message.error(err.message || 'failed to reset network policy');
    }
  };

  if (!id) return null;

  if (loading && !sandbox) {
    return (
      <div>
        <Link href="/dashboard/sandboxes"><ArrowLeftOutlined /> Sandboxes</Link>
        <Title level={3} style={{ marginTop: 8 }}>Loading…</Title>
      </div>
    );
  }

  if (error && !sandbox) {
    return (
      <div>
        <Link href="/dashboard/sandboxes"><ArrowLeftOutlined /> Sandboxes</Link>
        <Title level={3} style={{ marginTop: 8 }}>Sandbox not found</Title>
        <Paragraph type="danger">{error}</Paragraph>
        <Paragraph><code>{sandboxLabelOrId(id)}</code></Paragraph>
      </div>
    );
  }

  if (!sandbox) return null;

  const datadogTimeWindow = datadogTimeWindowParams(sandbox.created_at);
  const datadogLogsHref = datadogLogsUrl(sandbox, datadogTimeWindow);
  const datadogDashboardHref = dashboardUrl
    ? datadogDebugDashboardUrl(sandbox, datadogTimeWindow, dashboardUrl)
    : null;
  // Show the snapshots card for every real sandbox, not just ones enrolled
  // in the scheduler. Auto-suspend snapshots from the auto_lifecycle policy
  // accumulate even when `snapshots_enabled` is false, and operators need
  // to see those rows in the DB without dropping to the CLI.
  const showSnapshots = sandbox.resource_type !== 'pool_slot';

  return (
    <div>
      <Space direction="vertical" size={16} style={{ width: '100%' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <Link href="/dashboard/sandboxes"><ArrowLeftOutlined /> Sandboxes</Link>
          <Title level={3} style={{ margin: 0 }}>
            <code>{sandboxLabelOrId(sandbox.id)}</code>
          </Title>
          {statusTag(sandbox.status, sandbox.failure_reason)}
          {sandbox.resource_type === 'pool_slot' ? <Tag color="cyan">Pool slot</Tag> : null}
          {sandbox.resource_type === 'observed_local' ? <Tag>Observed local</Tag> : null}
          {sandbox.volume_encryption ? (
            <Tooltip title="BYOK: this sandbox runs on encrypted volumes keyed to its org's master key">
              <Tag color="geekblue" icon={<LockOutlined />}>encrypted</Tag>
            </Tooltip>
          ) : null}
          <div style={{ marginLeft: 'auto' }}>
            <Space>
              <Button size="small" href={datadogLogsHref} target="_blank" rel="noreferrer">
                Datadog logs
              </Button>
              {datadogDashboardHref && (
                <Button size="small" href={datadogDashboardHref} target="_blank" rel="noreferrer">
                  Debug Dashboard
                </Button>
              )}
              <Button
                size="small"
                icon={<ReloadOutlined />}
                onClick={() => refresh(true)}
                loading={loading}
              >
                Refresh
              </Button>
            </Space>
          </div>
        </div>

        <CollapsibleCard title="Details">
          <Descriptions size="small" column={2}>
            <Descriptions.Item label="ID" contentStyle={{ fontFamily: 'monospace', fontSize: 12 }}>
              <Space size={4}>
                <code>{sandboxLabelOrId(sandbox.id)}</code>
                <Button
                  type="text"
                  size="small"
                  icon={<CopyOutlined />}
                  onClick={() => copyToClipboard(sandbox.id)}
                  aria-label="Copy full ID"
                />
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="Name">{sandbox.name || '—'}</Descriptions.Item>
            <Descriptions.Item label="Template">{templateDisplay(sandbox)}</Descriptions.Item>
            <Descriptions.Item label="Release tags">{releaseTagsDisplay(sandbox)}</Descriptions.Item>
            <Descriptions.Item label="Runtime">{sandbox.runtime || 'fc'}</Descriptions.Item>
            <Descriptions.Item label="spaced">{spacedVersionDisplay(sandbox)}</Descriptions.Item>
            <Descriptions.Item label="Node">{nodeDisplay(sandbox)}</Descriptions.Item>
            <Descriptions.Item label="Image hash" contentStyle={{ fontFamily: 'monospace', fontSize: 12 }}>
              {sandbox.image_hash || '—'}
            </Descriptions.Item>
            <Descriptions.Item label="PID">{sandbox.pid ?? '—'}</Descriptions.Item>
            <Descriptions.Item label="Disk usage">{formatBytes(sandbox.disk_usage_bytes)}</Descriptions.Item>
            <Descriptions.Item label="Age">{sandbox.age || '—'}</Descriptions.Item>
            <Descriptions.Item label="Uptime">{sandbox.uptime || '—'}</Descriptions.Item>
            <Descriptions.Item label="User">{sandbox.user || '—'}</Descriptions.Item>
            <Descriptions.Item label="Created at">{sandbox.created_at || '—'}</Descriptions.Item>
            <Descriptions.Item label="Org">
              {sandbox.org_id ? <code>{sandbox.org_id}</code> : '—'}
            </Descriptions.Item>
            <Descriptions.Item label="Owner">
              {sandbox.user_id ? <code>{sandbox.user_id}</code> : '—'}
            </Descriptions.Item>
            {sandbox.pool_template ? (
              <Descriptions.Item label="Pool template">{sandbox.pool_template}</Descriptions.Item>
            ) : null}
            {sandbox.failure_reason ? (
              <Descriptions.Item label="Failure reason" span={2}>
                <Text type="danger">{sandbox.failure_reason}</Text>
              </Descriptions.Item>
            ) : null}
          </Descriptions>
        </CollapsibleCard>

        <Card size="small" title="Controls">
          <ControlButtons sandbox={sandbox} onAction={handleAction} busy={busy} />
        </Card>

        <CollapsibleCard
          title="Network policy"
          extra={(
            <Space>
              <NetworkPolicyGenerationTag
                desired={sandbox.network_policy_generation}
                applied={sandbox.network_policy_status?.observed_generation}
                policyLabel={compactNetworkPolicyLabel(effectivePolicy)}
                desiredSource={sandbox.network_policy_override ? 'override' : 'template'}
                observedSource={sandbox.network_policy_status?.observed_source}
              />
              <Button size="small" onClick={() => setEditingNetwork(true)}>Manage</Button>
            </Space>
          )}
        >
          <Descriptions size="small" column={1}>
            <Descriptions.Item label="Effective">
              {summarizeNetworkPolicy(effectivePolicy)}
            </Descriptions.Item>
            <Descriptions.Item label="Source">
              {sandbox.network_policy_override
                ? 'override (sandbox)'
                : (sandbox.template ? `template (${sandbox.template})` : '—')}
            </Descriptions.Item>
            <Descriptions.Item label="Generation">
              desired {sandbox.network_policy_generation ?? '—'}
              {' / '}
              observed {sandbox.network_policy_status?.observed_generation ?? '—'}
            </Descriptions.Item>
            <Descriptions.Item label="Desired (Postgres)">
              <NetworkPolicyDetails policy={effectivePolicy} />
            </Descriptions.Item>
            <Descriptions.Item label="Observed">
              <NetworkPolicyDetails policy={sandbox.network_policy_status?.observed_network_policy} />
            </Descriptions.Item>
            <Descriptions.Item label="Observed source">
              {sandbox.network_policy_status?.observed_source ?? '—'}
              {sandbox.network_policy_status?.observed_authority
                ? ` / ${sandbox.network_policy_status.observed_authority}`
                : ''}
            </Descriptions.Item>
            <Descriptions.Item label="Observed at">
              {sandbox.network_policy_status?.observed_at ?? '—'}
            </Descriptions.Item>
            {sandbox.network_policy_status?.conditions?.length ? (
              <Descriptions.Item label="Conditions">
                <Space wrap size={4}>
                  {sandbox.network_policy_status.conditions.map((c, idx) => (
                    <Tooltip
                      key={`${c.type}-${idx}`}
                      title={c.message || c.reason || c.type}
                    >
                      <Tag color={c.status === 'True' ? 'green' : c.status === 'False' ? 'red' : 'default'}>
                        {c.type}: {c.status}
                      </Tag>
                    </Tooltip>
                  ))}
                </Space>
              </Descriptions.Item>
            ) : null}
          </Descriptions>
        </CollapsibleCard>

        <ExecCard sandbox={sandbox} />

        <TerminalCard sandbox={sandbox} />

        {showSnapshots ? <SnapshotsCard sandboxId={sandbox.id} /> : null}
      </Space>

      {editingNetwork ? (
        <NetworkPolicyDesigner
          open={editingNetwork}
          targetKind="sandbox"
          targetName={sandboxLabelOrId(sandbox.id)}
          templateName={sandbox.template}
          policy={sandbox.network_policy_override}
          templatePolicy={templatePolicy}
          onClose={() => setEditingNetwork(false)}
          onSave={handleNetworkSave}
          onClear={handleNetworkClear}
        />
      ) : null}
    </div>
  );
}
