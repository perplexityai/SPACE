'use client';

import { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import { Alert, App, Button, Card, Collapse, Descriptions, Result, Select, Space, Switch, Table, Tag, Tooltip, Typography, type TableColumnsType } from 'antd';
import { ArrowLeftOutlined, ReloadOutlined } from '@ant-design/icons';

import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import {
  UPLOAD_ACCEPTED_MEANS,
  UPLOAD_RETRY_ORDER,
  uploadLeftOut,
  nodeActions,
  type ForceUploadResponse,
  type NodeAction,
} from '@/lib/node-actions';
import { countLabel, nodeReadFailure, type NodeCounts } from '@/lib/node-detail';
import {
  AUDIT_EVENT_MAX_LIMIT,
  confirmationPhrase,
  drainElapsedMs,
  drainHistory,
  formatDuration,
  drainStage,
  groupHolds,
  heartbeat,
  historyMayBeTruncated,
  holdGatesLabel,
  holdSentence,
  kubeNodePresence,
  residencyLabel,
  terminateRefusal,
  type ActionRefusal,
  type AuditEventRow,
  type NodeHold,
  type NodeHoldsResponse,
} from '@/lib/node-drain';
import { TypedConfirmModal } from '@/components/typed-confirm-modal';
import { serialReads, withTimeout, type SerialReads } from '@/lib/serial-reads';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
import { STORED_LIFECYCLE_FILTER_STATES } from '@/lib/sandbox-lifecycle';
import type { AdminNodeResponse, NodeHealthStatus } from '@/lib/types';
import { cordonedText, runtimeInstallationLines } from '@/lib/node-state';

const { Title, Text } = Typography;

// `GET /v1/nodes/{id}/sandboxes?order=lifecycle_changed_desc`: every row that
// names the node, newest lifecycle change first. An API without these
// parameters ignores them and answers its first page by id, with
// `next_after` and no `next_cursor` or change times.
interface NodeSandboxPage {
  sandboxes: {
    id: string;
    lifecycle: string;
    active_node_id: string | null;
    target_node_id: string | null;
    lifecycle_changed_at?: string;
  }[];
  next_cursor?: string | null;
}

function formatUnix(secs: number) {
  return secs ? new Date(secs * 1000).toLocaleString() : '—';
}

// The node is re-read every NODE_POLL_MS while the tab is visible, so a drain
// can be followed without Refresh. What holds the node, its sandboxes and the
// audit log are larger reads and follow every HISTORY_POLL_MS; the sandbox
// counts run a counting query and follow every COUNTS_POLL_MS, like the nodes
// list's occupancy.
//
// Every read gives up after READ_TIMEOUT_MS, the sandbox page's bound
// (`SANDBOX_READ_TIMEOUT_MS`), and then fails like any other: before the
// first load the page shows the error with Retry, and after it the page keeps
// the last node and says the refresh failed. Without the bound, a read that
// gets no answer would hold that kind's later reads, and the reload an action
// waits on, for ever.
const NODE_POLL_MS = 5_000;
const HISTORY_POLL_MS = 15_000;
const COUNTS_POLL_MS = 30_000;
const READ_TIMEOUT_MS = 30_000;

type ReadMode = 'poll' | 'next';

function drainPhaseColor(phase: string | null) {
  switch (phase) {
    case 'draining':
      return 'orange';
    case 'drained':
      return 'blue';
    case 'terminating':
      return 'red';
    default:
      return 'default';
  }
}

export default function NodeDetailPage() {
  const { message, modal } = App.useApp();
  const { id } = useParams<{ id: string }>();
  const { substrate } = useAuth();
  const [node, setNode] = useState<AdminNodeResponse | null>(null);
  const [page, setPage] = useState<NodeSandboxPage | null>(null);
  const [pageError, setPageError] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | undefined>();
  // Each kind of read runs one at a time (`serialReads`): a poll tick skips
  // while that kind's read is pending, and Refresh, Retry and the reload after
  // an action wait for it and then read. The counters drop answers that a
  // later event supersedes: an `id` change for every kind, a cursor change for
  // the sandbox page, and an action starting for the node and its counts, so a
  // read that began before the action cannot land after it.
  const nodeReads = useRef(serialReads()).current;
  const countsReads = useRef(serialReads()).current;
  const pageReads = useRef(serialReads()).current;
  const eventsReads = useRef(serialReads()).current;
  const holdsReads = useRef(serialReads()).current;
  const nodeRequest = useRef(0);
  const countsRequest = useRef(0);
  const pageRequest = useRef(0);
  const eventsRequest = useRef(0);
  const holdsRequest = useRef(0);
  const nodeLoaded = useRef(false);
  // Set once a node read fails with 403 or 404: the page then shows "Node not
  // found" or the access error, and the interval stops reading.
  const nodeGone = useRef(false);
  // Set once the holds read answers 403 or 404 (an API without the route
  // answers 404): the panel says so and its poll stops.
  const holdsGone = useRef(false);
  const [loadedAtMs, setLoadedAtMs] = useState<number | null>(null);
  // `error` replaces the page; `refreshError` keeps the last loaded node.
  const [error, setError] = useState<{ status: number | null; message: string } | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [counts, setCounts] = useState<NodeCounts | null>(null);
  const [countsFailed, setCountsFailed] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [nowMs, setNowMs] = useState(() => Date.now());
  // The action waiting on a typed confirmation, and the last refused one.
  const [typedAction, setTypedAction] = useState<NodeAction | null>(null);
  const [refusal, setRefusal] = useState<ActionRefusal | null>(null);
  const [events, setEvents] = useState<AuditEventRow[] | null>(null);
  const [eventsError, setEventsError] = useState<string | null>(null);
  const [holds, setHolds] = useState<NodeHoldsResponse | null>(null);
  const [holdsError, setHoldsError] = useState<{ status: number | null; message: string } | null>(null);
  // The history's filters: lifecycles, and failed since the drain started.
  const [historyLifecycles, setHistoryLifecycles] = useState<string[]>([]);
  const [failedDuringDrain, setFailedDuringDrain] = useState(false);
  const busyRef = useRef(false);

  // A poll tick skips while a read of the same kind is pending; any other
  // read waits for it.
  const read = (reads: SerialReads, mode: ReadMode, run: () => Promise<void>) =>
    mode === 'poll' ? reads.poll(run) : reads.next(run);
  const get = <T,>(path: string) => withTimeout(READ_TIMEOUT_MS, (signal) => adminApi.get<T>(path, { signal }));

  // Health comes from the node response's own status snapshot, so it is
  // always shown with that snapshot's time, never with a newer read's. The
  // node read is metadata-only; sandbox counts run a counting query and are
  // a separate best-effort read (`loadCounts`).
  const loadNode = (mode: ReadMode = 'next') => {
    const request = nodeRequest.current;
    return read(nodeReads, mode, () => get<AdminNodeResponse>(`/v1/nodes/${id}?include_occupancy=false`)
      .then((loaded) => {
        if (request !== nodeRequest.current) return;
        nodeLoaded.current = true;
        nodeGone.current = false;
        setNode(loaded);
        setLoadedAtMs(Date.now());
        setError(null);
        setRefreshError(null);
      })
      .catch((err: any) => {
        if (request !== nodeRequest.current) return;
        const status = typeof err?.status === 'number' ? err.status : null;
        const reason = err?.message || 'Failed to load node';
        if (status === 403 || status === 404) nodeGone.current = true;
        if (nodeReadFailure(status, nodeLoaded.current) === 'replace') {
          setError({ status, message: reason });
        } else {
          setRefreshError(reason);
        }
      }));
  };

  const loadCounts = (mode: ReadMode = 'next') => {
    const request = countsRequest.current;
    return read(countsReads, mode, () => get<AdminNodeResponse>(`/v1/nodes/${id}?include_occupancy=true`)
      .then((loaded) => {
        if (request !== countsRequest.current) return;
        setCounts({ sandboxes_active: loaded.sandboxes_active, sandboxes_local_snapshots: loaded.sandboxes_local_snapshots });
        setCountsFailed(false);
      })
      .catch(() => {
        if (request === countsRequest.current) setCountsFailed(true);
      }));
  };

  // The history's query: every row that names the node, newest lifecycle
  // change first, narrowed by its filters. "Failed during this drain" is the
  // failed rows whose lifecycle changed since the drain started; it replaces
  // the lifecycle filter while it is on.
  const drainStartedAt = node?.drain_started_at ?? null;
  const failedOnly = failedDuringDrain && !!drainStartedAt;
  const since = failedOnly ? drainStartedAt : null;
  const historyKey = JSON.stringify([historyLifecycles, since]);
  const historyParams = (after: string | undefined) => {
    const params = new URLSearchParams({ order: 'lifecycle_changed_desc' });
    for (const lifecycle of since ? ['failed'] : historyLifecycles) params.append('lifecycle', lifecycle);
    if (since) params.set('changed_since', since);
    if (after) params.set('cursor', after);
    return params;
  };

  // A poll keeps the rows on screen until its answer lands.
  const loadSandboxes = (after: string | undefined = cursor, mode: ReadMode = 'next') => {
    const request = pageRequest.current;
    const params = historyParams(after);
    if (mode === 'next') {
      setPage(null);
      setPageError(null);
    }
    return read(pageReads, mode, () => get<NodeSandboxPage>(`/v1/nodes/${id}/sandboxes?${params}`)
      .then((loaded) => {
        if (request !== pageRequest.current) return;
        setPage(loaded);
        setPageError(null);
      })
      .catch((err: any) => {
        if (request === pageRequest.current) setPageError(err?.message || 'Failed to load sandboxes');
      }));
  };

  // The audit log has no node filter: read its newest page and keep this
  // node's actions. A failed read leaves the rest of the page in place.
  const loadEvents = (mode: ReadMode = 'next') => {
    const request = eventsRequest.current;
    return read(eventsReads, mode, () => get<{ events: AuditEventRow[] }>(`/v1/audit-events?limit=${AUDIT_EVENT_MAX_LIMIT}`)
      .then((body) => {
        if (request !== eventsRequest.current) return;
        setEvents(body.events);
        setEventsError(null);
      })
      .catch((err: any) => {
        if (request === eventsRequest.current) setEventsError(err?.message || 'Failed to load the audit log');
      }));
  };

  // What the gates count against the node. A 403 or 404 stops the poll: the
  // operator may not see the node, it is gone, or the API predates the route.
  const loadHolds = (mode: ReadMode = 'next') => {
    const request = holdsRequest.current;
    return read(holdsReads, mode, () => get<NodeHoldsResponse>(`/v1/nodes/${id}/holds`)
      .then((loaded) => {
        if (request !== holdsRequest.current) return;
        holdsGone.current = false;
        setHolds(loaded);
        setHoldsError(null);
      })
      .catch((err: any) => {
        if (request !== holdsRequest.current) return;
        const status = typeof err?.status === 'number' ? err.status : null;
        if (status === 403 || status === 404) holdsGone.current = true;
        setHoldsError({ status, message: err?.message || 'Failed to load what holds this node' });
      }));
  };

  const refreshAll = () => {
    void loadNode();
    void loadCounts();
    if (holdsShown.current) void loadHolds();
    void loadSandboxes();
    void loadEvents();
  };

  // The poll, and the reload after an action, read the page of sandboxes the
  // operator is on, with the filters on screen. Set from an effect, since a
  // ref must not be written during render.
  const pollSandboxesRef = useRef(() => {});
  const reloadSandboxesRef = useRef(() => {});
  useEffect(() => {
    pollSandboxesRef.current = () => void loadSandboxes(cursor, 'poll');
    reloadSandboxesRef.current = () => void loadSandboxes(cursor);
  }, [id, cursor, historyKey]);

  // Ending or restarting the drain clears "Failed during this drain" and the
  // History's paging.
  useEffect(() => {
    setFailedDuringDrain(false);
    setCursor(undefined);
  }, [drainStartedAt]);

  // Only worker and builder nodes show the holds panel, so only theirs are
  // read: once the node's role is known, and on every poll after.
  const holdsFor = node && (node.role === 'worker' || node.role === 'builder') ? node.id : null;
  const holdsShown = useRef(false);
  useEffect(() => {
    holdsShown.current = holdsFor !== null;
    if (holdsFor !== null) void loadHolds();
  }, [holdsFor]);

  useEffect(() => {
    nodeRequest.current += 1;
    countsRequest.current += 1;
    eventsRequest.current += 1;
    holdsRequest.current += 1;
    nodeLoaded.current = false;
    nodeGone.current = false;
    holdsGone.current = false;
    setNode(null);
    setLoadedAtMs(null);
    setError(null);
    setRefreshError(null);
    setCounts(null);
    setCountsFailed(false);
    setEvents(null);
    setEventsError(null);
    setHolds(null);
    setHoldsError(null);
    setRefusal(null);
    void loadNode();
    void loadCounts();
    void loadEvents();
    // One tick a second keeps the ages current; the reads run on their own
    // periods, skip a hidden tab, stop once the node is gone or not ours to
    // see, and wait while an action is in flight (it reloads everything when
    // it ends).
    const polling = () => !document.hidden && !busyRef.current && !nodeGone.current;
    let ticks = 0;
    const timer = setInterval(() => {
      setNowMs(Date.now());
      if (!polling()) return;
      ticks += 1;
      const ms = ticks * 1000;
      if (ms % NODE_POLL_MS === 0) void loadNode('poll');
      if (ms % HISTORY_POLL_MS === 0) {
        void loadEvents('poll');
        if (holdsShown.current && !holdsGone.current) void loadHolds('poll');
        pollSandboxesRef.current();
      }
      if (ms % COUNTS_POLL_MS === 0) void loadCounts('poll');
    }, 1000);
    // A tab shown again reads the node at once rather than at the next period.
    const onVisible = () => {
      if (!polling()) return;
      setNowMs(Date.now());
      void loadNode('poll');
    };
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      clearInterval(timer);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [id]);
  useEffect(() => {
    pageRequest.current += 1;
    void loadSandboxes(cursor);
  }, [id, cursor, historyKey]);

  const showUploadResult = (result: ForceUploadResponse) => {
    const accepted = result.reuploaded.length;
    const failed = result.failed ?? [];
    const leftOut = uploadLeftOut(result);
    modal.info({
      title: `Upload requests accepted for ${accepted} sandbox${accepted === 1 ? '' : 'es'}${failed.length ? `; ${failed.length} not accepted` : ''}`,
      width: 640,
      content: (
        <Space orientation="vertical" size={4} style={{ width: '100%' }}>
          <Text>{UPLOAD_ACCEPTED_MEANS}</Text>
          {failed.length ? <Text strong>Requests that failed:</Text> : null}
          {failed.map((f) => (
            <Text key={f.sandbox_id} type="danger">{publicSandboxIdFromAny(f.sandbox_id)}: {f.error}</Text>
          ))}
          {leftOut.map((note) => <Text key={note}>{note}</Text>)}
          {leftOut.length || failed.length ? <Text type="secondary">{UPLOAD_RETRY_ORDER}</Text> : null}
        </Space>
      ),
    });
  };

  // Sends the action and reloads. Resolves once the reload settles, so a
  // typed confirmation stays open (and its OK busy) until then.
  const send = async (item: NodeAction) => {
    setBusy(item.label);
    busyRef.current = true;
    setRefusal(null);
    // Earlier reads describe the node before this action: drop them.
    nodeRequest.current += 1;
    countsRequest.current += 1;
    holdsRequest.current += 1;
    try {
      const result = await adminApi.post<ForceUploadResponse | unknown>(`/v1/nodes/${id}:${item.action}`, item.body);
      if (item.action === 'force-upload-local-snapshots' && result && typeof result === 'object' && 'reuploaded' in result) {
        showUploadResult(result as ForceUploadResponse);
      } else {
        message.success(`${item.label} requested`);
      }
    } catch (err: any) {
      const refused = item.action === 'terminate' ? terminateRefusal(err) : null;
      if (refused) {
        setRefusal(refused);
        message.error(refused.title);
      } else {
        message.error(err?.message || `${item.label} failed`);
      }
    } finally {
      // Some actions commit and then report an error: always reload, and
      // keep the actions disabled until that read settles. The History reads
      // the page and filters on screen now, not the ones at the click.
      reloadSandboxesRef.current();
      void loadCounts();
      if (holdsShown.current) void loadHolds();
      void loadEvents();
      await loadNode();
      busyRef.current = false;
      setBusy(null);
    }
  };

  const runAction = (item: NodeAction) => {
    if (item.typed) {
      setTypedAction(item);
      return;
    }
    modal.confirm({
      title: `${item.label} ${node?.name ?? id}?`,
      content: item.confirm,
      okText: item.label,
      okButtonProps: { danger: item.dangerous },
      focusable: { autoFocusButton: item.dangerous ? 'cancel' : 'ok' },
      onOk: () => send(item),
    });
  };

  if (error) {
    return (
      <Result
        status="warning"
        title={error.status === 404 ? 'Node not found' : 'Could not load the node'}
        subTitle={<>{error.message} <code>{id}</code></>}
        extra={
          <Space>
            {error.status !== 404 ? <Button type="primary" icon={<ReloadOutlined />} onClick={refreshAll}>Retry</Button> : null}
            <Link href="/dashboard/nodes"><ArrowLeftOutlined /> Nodes</Link>
          </Space>
        }
      />
    );
  }
  if (!node) return null;

  const slots = node.network_slots;
  const status = node.status;
  const health: NodeHealthStatus | null = status?.health ?? null;
  const hasHealth = node.role === 'worker' || node.role === 'builder';
  const cordoned = cordonedText(node.cordoned);
  const runtimeLines = runtimeInstallationLines(node.runtime_installations);
  const beat = heartbeat(node, nowMs);
  const elapsed = drainElapsedMs(node, nowMs);
  const kubeNode = kubeNodePresence(node);
  const history = events ? drainHistory(events, node.id) : [];
  const actions = nodeActions(node, substrate);
  const uploadAction = actions.find((item) => item.action === 'force-upload-local-snapshots');
  const stage = drainStage(node);
  const holdGroups = holds ? groupHolds(holds.holds, stage) : [];
  const residency = holds ? residencyLabel(holds.residency_evidence) : null;
  const terminateCounted = holds?.holds.filter((h) => h.counted_by.terminate).length ?? 0;
  // Whether any term counts a row: the rows listed but not counted do not
  // hold the node.
  const heldByTerms = holds ? holds.term_counts.resident + holds.term_counts.drain_blocker + holds.term_counts.active_resident > 0 : false;
  const nothingHolds = 'Nothing holds this node: no sandbox the drain or terminate counts.';
  const age = (at: string) => formatDuration(nowMs - new Date(at).getTime());
  // An API without the History's parameters answers no `next_cursor`.
  const historyIgnored = page !== null && !('next_cursor' in page);
  const holdColumns: TableColumnsType<NodeHold> = [
    {
      title: 'Sandbox', dataIndex: 'sandbox_id', width: 300,
      render: (sid: string) => <Link href={`/dashboard/sandboxes/${publicSandboxIdFromAny(sid)}`}><code>{publicSandboxIdFromAny(sid)}</code></Link>,
    },
    {
      title: 'Lifecycle', width: 190,
      render: (_, h) => (
        <Space size={4}>
          <Tag>{h.lifecycle}</Tag>
          <Tooltip title={`changed ${new Date(h.lifecycle_changed_at).toLocaleString()}`}><Text type="secondary">{age(h.lifecycle_changed_at)}</Text></Tooltip>
        </Space>
      ),
    },
    { title: 'Why', render: (_, h) => holdSentence(h, nowMs, holds?.stuck_transient_after_secs ?? 600, stage, node.drain_phase) },
    {
      title: 'Counted by', width: 250,
      render: (_, h) => {
        const terms = Object.entries(h.terms).filter(([, on]) => on).map(([term]) => term);
        return <Tooltip title={`Gate terms: ${terms.join(', ') || 'none'}. Placement on this node: ${h.placement.replace('_', ' ')}.`}>{holdGatesLabel(h)}</Tooltip>;
      },
    },
  ];

  return (
    <Space orientation="vertical" size={16} style={{ width: '100%' }}>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
        <Space align="center">
          <Link href="/dashboard/nodes"><ArrowLeftOutlined /> Nodes</Link>
          <Title level={3} style={{ margin: 0 }}>{node.name || node.address || node.id}</Title>
          <Tag>{node.role}</Tag>
          {node.drain_phase ? <Tag color="orange">{node.drain_phase} · {node.drain_mode ?? 'gentle'}</Tag> : null}
          {node.cordoned ? <Tag color="orange">cordoned</Tag> : null}
          <Tag color={node.scheduling_status === 'ready' ? 'green' : 'default'}>{node.scheduling_status ?? 'unknown'}</Tag>
        </Space>
        <Space wrap>
          {actions.map((item) => (
            <Button key={item.label} danger={item.dangerous} loading={busy === item.label} disabled={!!busy} onClick={() => runAction(item)}>
              {item.label}
            </Button>
          ))}
          <Button icon={<ReloadOutlined />} onClick={refreshAll} disabled={!!busy}>Refresh</Button>
        </Space>
      </Space>

      {refreshError ? (
        <Alert
          type="warning"
          showIcon
          title={`Could not refresh the node: ${refreshError}`}
          description={`Showing the node as loaded ${loadedAtMs ? `at ${new Date(loadedAtMs).toLocaleString()}` : 'earlier'}; it may be stale.`}
          action={<Button size="small" onClick={refreshAll} disabled={!!busy}>Retry</Button>}
        />
      ) : null}

      {hasHealth ? (
        <Card
          size="small"
          title="Drain"
          id="drain"
          extra={loadedAtMs ? <Text type="secondary">updated {new Date(loadedAtMs).toLocaleTimeString()}, every {NODE_POLL_MS / 1000} s</Text> : null}
        >
          {refusal ? (
            <Alert
              type="error"
              showIcon
              closable={{ onClose: () => setRefusal(null) }}
              title={refusal.title}
              description={(
                <>
                  <ul style={{ margin: 0, paddingInlineStart: 20 }}>
                    {refusal.reasons.map((reason) => <li key={reason}>{reason}</li>)}
                  </ul>
                  {refusal.counts ? (
                    <div style={{ marginTop: 8 }}>
                      <a href="#node-holds">See what holds this node</a>
                      {holds ? (
                        <Text>
                          {' '}— the panel counts {holds.term_counts.active_resident} occupying and {holds.term_counts.drain_blocker} held for the node
                          {holds.truncated ? '' : `, ${terminateCounted} sandbox${terminateCounted === 1 ? '' : 'es'} in all`}
                          {holds.term_counts.active_resident === refusal.counts.active && holds.term_counts.drain_blocker === refusal.counts.blockers
                            ? ', as the refusal did.'
                            : `; the refusal counted ${refusal.counts.active} and ${refusal.counts.blockers}. They were read at different moments: Refresh to compare again.`}
                        </Text>
                      ) : null}
                    </div>
                  ) : null}
                </>
              )}
              style={{ marginBottom: 12 }}
            />
          ) : null}
          <Descriptions size="small" column={3}>
            <Descriptions.Item label="Phase">
              {node.drain_phase ? <Tag color={drainPhaseColor(node.drain_phase)}>{node.drain_phase}</Tag> : <Text type="secondary">not draining</Text>}
            </Descriptions.Item>
            {/* A missing mode on a drain is gentle, as the API resolves it. */}
            <Descriptions.Item label="Mode">{node.drain_phase ? node.drain_mode ?? 'gentle' : '—'}</Descriptions.Item>
            <Descriptions.Item label="Started">
              {node.drain_started_at
                ? `${new Date(node.drain_started_at).toLocaleString()}${elapsed !== null ? ` (${formatDuration(elapsed)} ago)` : ''}`
                : '—'}
            </Descriptions.Item>
            <Descriptions.Item label="Last error">
              {node.drain_last_error ? <Text type="danger">{node.drain_last_error}</Text> : '—'}
            </Descriptions.Item>
            <Descriptions.Item label="Heartbeat">
              <Space size={4} wrap>
                <span>{beat.ageMs !== null ? `${formatDuration(beat.ageMs)} ago` : 'never'}</span>
                {beat.state === 'stale' ? (
                  <Tooltip title={`No status publication within the node's window${beat.windowMs !== null ? ` of ${formatDuration(beat.windowMs)}` : ''}; the API reports this node's status as stale.`}>
                    <Tag color="red">stale</Tag>
                  </Tooltip>
                ) : null}
                {beat.state === 'no-status' && hasHealth ? <Tag>no status received</Tag> : null}
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="Kubernetes Node">
              {kubeNode === 'present' ? (
                <span>present{node.cordoned ? ', cordoned' : ''}</span>
              ) : kubeNode === 'unknown' ? (
                <Tooltip title="The API reports a Kubernetes Node only while its node cache holds it, so a missing Node and a cache that has not listed yet look the same.">
                  <Text type="secondary">not reported</Text>
                </Tooltip>
              ) : (
                <Text type="secondary">—</Text>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="Holding the node" span="filled">
              {!holds ? (
                <Text type="secondary">{holdsError ? 'not loaded' : 'loading'}</Text>
              ) : heldByTerms ? (
                <Space size={4} wrap>
                  {holdGroups.filter((group) => group.key !== 'not_counted').map((group) => <Tag key={group.key}>{group.title} {group.rows.length}</Tag>)}
                  <a href="#node-holds">what holds it</a>
                </Space>
              ) : (
                <Text type="secondary">{nothingHolds}</Text>
              )}
            </Descriptions.Item>
          </Descriptions>

          <Title level={5} style={{ marginTop: 12 }}>History</Title>
          {eventsError ? (
            <Alert
              type="warning"
              showIcon
              title={`Could not load the audit log: ${eventsError}`}
              description={events ? 'Showing the history as last loaded.' : undefined}
              action={<Button size="small" onClick={() => void loadEvents()}>Retry</Button>}
              style={{ marginBottom: 8 }}
            />
          ) : null}
          <Table
            size="small"
            rowKey="id"
            loading={events === null && !eventsError}
            dataSource={history}
            pagination={false}
            locale={{ emptyText: events === null ? 'Not loaded' : 'No actions on this node among the audit events read' }}
            columns={[
              { title: 'When', dataIndex: 'at', width: 200, render: (v: string) => <Tooltip title={v}>{new Date(v).toLocaleString()}</Tooltip> },
              { title: 'Action', dataIndex: 'action', width: 220, render: (v: string) => <Tag>{v}</Tag> },
              {
                title: 'Operator', dataIndex: 'actor',
                render: (v: string, e) => (e.actorUserId ? <Link href={`/dashboard/users/${e.actorUserId}`}>{v}</Link> : v),
              },
              {
                title: 'Result', dataIndex: 'statusCode', width: 110,
                render: (v: number, e) => <Tag color={e.success ? 'green' : 'red'}>{v}</Tag>,
              },
            ]}
          />
          <Space orientation="vertical" size={0} style={{ marginTop: 8 }}>
            {events && historyMayBeTruncated(events.length, AUDIT_EVENT_MAX_LIMIT) ? (
              <Text type="warning">
                Only the newest {AUDIT_EVENT_MAX_LIMIT} recorded admin API requests, from every client (the console, the CLI and scripts), were searched; older actions on this node may be missing. The Audit log page reads the same log.
              </Text>
            ) : null}
            <Text type="secondary">
              From the admin audit log, which records admin API writes and their HTTP result, but not their body (a gentle and a forced drain look the same), and keeps them for a limited time. Phase changes the drain reconciler and the drained-node terminator make on their own are not recorded there.
            </Text>
          </Space>
        </Card>
      ) : null}

      {hasHealth ? (
        <Card
          size="small"
          title="What holds this node"
          id="node-holds"
          extra={holds && residency ? (
            <Space size={8} wrap>
              <Tooltip title={residency.tooltip}><Tag>residency: {residency.label}</Tag></Tooltip>
              <Text type="secondary">every {HISTORY_POLL_MS / 1000} s</Text>
              <Button size="small" icon={<ReloadOutlined />} onClick={() => void loadHolds()}>Reload</Button>
            </Space>
          ) : null}
        >
          {holdsError ? (
            <Alert
              type={holds ? 'warning' : 'error'}
              showIcon
              title={holdsError.status === 404 && !holds ? 'This API does not list what holds a node' : `Could not load what holds this node: ${holdsError.message}`}
              description={holds ? 'Showing them as last loaded.' : undefined}
              action={<Button size="small" onClick={() => void loadHolds()}>Retry</Button>}
              style={{ marginBottom: 8 }}
            />
          ) : null}
          {!holds && !holdsError ? <Text type="secondary">Loading</Text> : null}
          {holds && !heldByTerms ? <Text type="secondary">{nothingHolds}</Text> : null}
          {holds?.truncated ? (
            <Text type="warning">Listing the first {holds.holds.length} of {holds.total}; the group counts in the Drain section cover only those.</Text>
          ) : null}
          {holdGroups.map((group) => (
            <div key={group.key} data-hold-group={group.key} style={{ marginTop: 12 }}>
              <Space align="center" style={{ marginBottom: 4 }} wrap>
                <Title level={5} style={{ margin: 0 }}>{group.title}</Title>
                <Tag>{group.rows.length}</Tag>
                {uploadAction && group.rows.some((h) => h.reason === 'local_only_snapshot') ? (
                  <Button size="small" danger loading={busy === uploadAction.label} disabled={!!busy} onClick={() => runAction(uploadAction)}>
                    {uploadAction.label}
                  </Button>
                ) : null}
              </Space>
              <Table size="small" rowKey="sandbox_id" pagination={false} dataSource={group.rows} columns={holdColumns} />
            </div>
          ))}
          <Text type="secondary" style={{ display: 'block', marginTop: 12 }}>
            The sandboxes the drain and terminate gates count against this node, as the API reads them now. The drain also waits for the evacuations it has in flight, which only the cluster-controller knows; a sandbox an evacuation is moving still shows here, as evacuating. Whether the drain also waits on a row only terminate counts depends on the cluster-controller&apos;s termination-blocking gate, which the API does not report.
          </Text>
        </Card>
      ) : null}

      <Card size="small" title="Node">
        <Descriptions size="small" column={3}>
          <Descriptions.Item label="ID"><Text code copyable>{node.id}</Text></Descriptions.Item>
          <Descriptions.Item label="Address">{node.address || '—'}</Descriptions.Item>
          <Descriptions.Item label="Kubernetes node">{node.k8s_node_name || '—'}</Descriptions.Item>
          <Descriptions.Item label="Managed by">{node.managed_by}</Descriptions.Item>
          <Descriptions.Item label="State">{node.state || 'registered'}</Descriptions.Item>
          <Descriptions.Item label="Scheduling">{node.scheduling_reason || node.scheduling_status || '—'}</Descriptions.Item>
          <Descriptions.Item label="Last heartbeat">{node.last_heartbeat ? new Date(node.last_heartbeat).toLocaleString() : '—'}</Descriptions.Item>
          <Descriptions.Item label="Status observed">
            {status ? `${new Date(status.observed_at).toLocaleString()}${status.stale ? ' (stale)' : ''}` : '—'}
          </Descriptions.Item>
          {cordoned !== null ? <Descriptions.Item label="Cordoned">{cordoned}</Descriptions.Item> : null}
          {node.node_group_id ? (
            <Descriptions.Item label="Node group"><Text code copyable>{node.node_group_id}</Text></Descriptions.Item>
          ) : null}
          {runtimeLines !== null ? (
            <Descriptions.Item label="Runtime installations" span="filled">
              {runtimeLines.length ? (
                <Space orientation="vertical" size={0}>
                  {runtimeLines.map((line) => <Text key={line}>{line}</Text>)}
                </Space>
              ) : <Text type="secondary">none reported</Text>}
            </Descriptions.Item>
          ) : null}
        </Descriptions>
      </Card>

      <Card size="small" title="Occupancy">
        <Descriptions size="small" column={3}>
          <Descriptions.Item label="Active sandboxes">{countLabel(counts?.sandboxes_active, countsFailed)}</Descriptions.Item>
          <Descriptions.Item label="Node-local snapshots">{countLabel(counts?.sandboxes_local_snapshots, countsFailed)}</Descriptions.Item>
          <Descriptions.Item label="Network slots leased">
            {slots ? `${slots.leased ?? '—'} of ${slots.total ?? '—'} (max ${slots.max_slots ?? '—'})` : '—'}
          </Descriptions.Item>
          <Descriptions.Item label="Slots ready / min">{slots ? `${slots.ready ?? '—'} / ${slots.min_ready ?? '—'}` : '—'}</Descriptions.Item>
          <Descriptions.Item label="Slots degraded">{slots?.degraded ?? '—'}</Descriptions.Item>
          <Descriptions.Item label="Slots quarantined">{slots?.quarantined ?? '—'}</Descriptions.Item>
        </Descriptions>
      </Card>

      {hasHealth ? (
        <Card size="small" title="Health">
          {!health ? <Text type="secondary">No status snapshot has been received from this node.</Text> : null}
          {health ? (
            <>
              <Space style={{ marginBottom: 8 }} wrap>
                {!health.reachable ? (
                  <Tag color="red">unreachable</Tag>
                ) : (
                  <Tag color={health.healthy ? 'green' : 'red'}>{health.healthy ? 'healthy' : 'unhealthy'}</Tag>
                )}
                {status?.observed_at ? (
                  <Text type="secondary">
                    as of {new Date(status.observed_at).toLocaleString()}{status.stale ? ' (stale)' : ''}
                  </Text>
                ) : null}
                {health.error ? <Text type="danger">{health.error}</Text> : null}
                {health.node_ctl && !health.node_ctl.reachable ? (
                  <Text type="danger">node-ctl unreachable{health.node_ctl.error ? `: ${health.node_ctl.error}` : ''}</Text>
                ) : null}
              </Space>
              {health.services.length ? (
                <Space wrap style={{ marginBottom: 8 }}>
                  {health.services.map((service) => (
                    <Tooltip key={service.name} title={service.error || 'reachable'}>
                      <Tag color={service.reachable ? 'green' : 'red'}>{service.name}</Tag>
                    </Tooltip>
                  ))}
                </Space>
              ) : null}
              <Table
                size="small"
                rowKey="name"
                pagination={false}
                dataSource={health.node_ctl?.reconciliations ?? []}
                locale={{ emptyText: health.node_ctl ? 'No reconcilers reported' : 'node-ctl not reachable' }}
                columns={[
                  { title: 'Reconciler', dataIndex: 'name' },
                  { title: 'Last success', dataIndex: 'last_success_at_unix_secs', render: formatUnix },
                  { title: 'Next due', dataIndex: 'next_due_at_unix_secs', render: formatUnix },
                  { title: 'Seen (templates / sandboxes)', render: (_, r) => `${r.templates_seen} / ${r.sandboxes_seen}` },
                  { title: 'Last error', dataIndex: 'last_error', render: (v: string) => (v ? <Text type="danger">{v}</Text> : '—') },
                ]}
              />
            </>
          ) : null}
        </Card>
      ) : null}

      <div id="node-sandboxes">
        <Collapse
          items={[{
            key: 'history',
            label: 'History: every sandbox that names this node, newest change first',
            children: (
              <>
                <Space wrap style={{ marginBottom: 8, width: '100%', justifyContent: 'space-between' }}>
                  <Space wrap>
                    <Select
                      mode="multiple"
                      allowClear
                      aria-label="Lifecycle filter"
                      placeholder="Any lifecycle"
                      style={{ minWidth: 260 }}
                      disabled={failedOnly}
                      value={historyLifecycles}
                      options={STORED_LIFECYCLE_FILTER_STATES.map((value) => ({ value, label: value }))}
                      onChange={(values: string[]) => {
                        setHistoryLifecycles(values);
                        setCursor(undefined);
                      }}
                    />
                    <Tooltip title={drainStartedAt ? `Failed sandboxes whose lifecycle changed since ${new Date(drainStartedAt).toLocaleString()}, when the drain started.` : 'The node is not draining.'}>
                      <Space size={4}>
                        <Switch
                          size="small"
                          aria-label="Failed during this drain"
                          checked={failedOnly}
                          disabled={!drainStartedAt}
                          onChange={(on) => {
                            setFailedDuringDrain(on);
                            setCursor(undefined);
                          }}
                        />
                        <Text>Failed during this drain</Text>
                      </Space>
                    </Tooltip>
                  </Space>
                  <Space>
                    <Button size="small" disabled={!cursor} onClick={() => setCursor(undefined)}>First page</Button>
                    <Button size="small" icon={<ReloadOutlined />} onClick={() => void loadSandboxes()}>Reload</Button>
                    <Button size="small" disabled={!page?.next_cursor} onClick={() => page?.next_cursor && setCursor(page.next_cursor)}>Next 100</Button>
                  </Space>
                </Space>
                {historyIgnored ? (
                  <Alert
                    type="warning"
                    showIcon
                    title="This API ignores the History's order, filters and paging: the list shows the node's first 100 sandboxes by id, unfiltered."
                    style={{ marginBottom: 8 }}
                  />
                ) : null}
                {pageError ? (
                  <Alert
                    type="error"
                    showIcon
                    title={`Could not load this node's sandboxes: ${pageError}`}
                    action={<Button size="small" onClick={() => void loadSandboxes()}>Retry</Button>}
                    style={{ marginBottom: 8 }}
                  />
                ) : null}
                <Table
                  size="small"
                  rowKey="id"
                  loading={page === null && !pageError}
                  dataSource={page?.sandboxes ?? []}
                  pagination={false}
                  locale={{ emptyText: pageError ? 'Not loaded' : historyLifecycles.length || failedOnly ? 'No sandboxes match the filter' : 'No sandboxes placed on this node' }}
                  columns={[
                    {
                      title: 'Sandbox', dataIndex: 'id',
                      render: (sid: string) => <Link href={`/dashboard/sandboxes/${publicSandboxIdFromAny(sid)}`}><code>{publicSandboxIdFromAny(sid)}</code></Link>,
                    },
                    { title: 'Lifecycle', dataIndex: 'lifecycle', width: 140, render: (v: string) => <Tag>{v}</Tag> },
                    {
                      title: 'Changed', dataIndex: 'lifecycle_changed_at', width: 160,
                      render: (v: string | undefined) => (v !== undefined && Number.isFinite(Date.parse(v))
                        ? <Tooltip title={new Date(v).toLocaleString()}>{age(v)} ago</Tooltip>
                        : '—'),
                    },
                    {
                      title: 'Placement', width: 160,
                      render: (_, row) => [row.active_node_id === node.id ? 'active' : '', row.target_node_id === node.id ? 'target' : ''].filter(Boolean).join(', '),
                    },
                  ]}
                />
              </>
            ),
          }]}
        />
      </div>

      <TypedConfirmModal
        open={typedAction !== null}
        title={typedAction ? `${typedAction.label} ${node.name ?? node.id}?` : ''}
        explanation={typedAction?.confirm ?? ''}
        phrase={confirmationPhrase(node)}
        okText={typedAction?.label ?? 'OK'}
        onCancel={() => setTypedAction(null)}
        onConfirm={async () => {
          if (!typedAction) return;
          await send(typedAction);
          setTypedAction(null);
        }}
      />
    </Space>
  );
}
