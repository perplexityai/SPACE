'use client';

import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { App, Button, Popconfirm, Space, Tooltip, Typography } from 'antd';
import { RollbackOutlined } from '@ant-design/icons';

import { followRollback } from '@/lib/accepted-create';
import { rollbackGateRefusal, rollbackRefusals } from '@/lib/hidden-operations';
import { rollbackConsoleSandbox } from '@/lib/public-api';
import { FULL_SNAPSHOT_RESTORE_NOTE, isFullSnapshotName } from '@/lib/rollback-targets';

const { Text } = Typography;

export interface SnapshotRollback {
  // Why rollback is refused for the sandbox's organization, once the API has
  // said so in this session; Rollback is disabled while it is set.
  refusal: string | null;
  // The snapshot being rolled back to, while its operation is followed.
  rollingBack: string | null;
  rollback: (snapshotName: string) => Promise<void>;
  tryAgain: () => void;
}

// Roll a sandbox back and report how its operation ended, not only that it
// was queued. `via` is the route the rollback goes to (`rollbackRoute`; null
// while none is offered); `onSettled` runs once the outcome is known. A refusal is
// shared with every panel of the organization (`rollbackRefusals`). The
// rollback is followed only while this view shows the sandbox: unmounting,
// or showing another sandbox, stops following it, so its outcome cannot
// reload a view that is no longer shown.
export function useSnapshotRollback({
  sandboxId,
  orgId,
  via,
  onSettled,
}: {
  sandboxId: string;
  orgId: string | null;
  via: 'admin' | 'public' | null;
  onSettled?: () => void;
}): SnapshotRollback {
  const { message } = App.useApp();
  const refusals = rollbackRefusals();
  const subscribe = useCallback((listener: () => void) => refusals.subscribe(orgId, listener), [refusals, orgId]);
  const refusal = useSyncExternalStore(subscribe, () => refusals.get(orgId), () => null);
  const [rollingBack, setRollingBack] = useState<string | null>(null);
  // The rollbacks this view follows, each stopped when the view stops
  // showing its sandbox.
  const following = useRef(new Set<AbortController>());
  useEffect(() => {
    const followers = following.current;
    return () => {
      for (const follower of followers) follower.abort();
      followers.clear();
      setRollingBack(null);
    };
  }, [sandboxId]);

  const rollback = async (snapshotName: string) => {
    // Checked right before sending: another panel may have been refused since
    // this one rendered.
    if (!via || rollingBack || refusals.get(orgId)) return;
    const follower = new AbortController();
    following.current.add(follower);
    setRollingBack(snapshotName);
    try {
      let accepted: Awaited<ReturnType<typeof rollbackConsoleSandbox>>;
      try {
        accepted = await rollbackConsoleSandbox(sandboxId, snapshotName, via);
      } catch (err: any) {
        const reason = rollbackGateRefusal(err, via);
        if (reason) {
          refusals.remember(orgId, reason);
          message.info(reason);
          return;
        }
        message.error(err?.message || 'failed to queue rollback');
        return;
      }
      await followRollback({
        snapshotName,
        operationId: accepted.operationId,
        follow: accepted.follow,
        signal: follower.signal,
        show: ({ key, type, text }) => message.open({
          key,
          type,
          content: text,
          duration: type === 'loading' ? 0 : type === 'success' ? 4 : type === 'info' ? 8 : 12,
        }),
        onSettled,
      });
    } finally {
      following.current.delete(follower);
      if (!follower.signal.aborted) setRollingBack(null);
    }
  };

  const tryAgain = () => refusals.forget(orgId);

  return { refusal, rollingBack, rollback, tryAgain };
}

// Rollback for one of the node's snapshots (`rollbackTargets`). `unavailable`
// is why it is not offered for this row (`RollbackTarget.unavailable`, or the
// route's reason, `rollbackRoute`); a remembered gate refusal applies to every
// row.
export function RollbackButton({
  snapshotName,
  unavailable,
  state,
}: {
  snapshotName: string;
  unavailable: string | null;
  state: SnapshotRollback;
}) {
  const reason = state.refusal ?? unavailable;
  if (reason) {
    return (
      <Tooltip title={reason}>
        <Button type="link" size="small" icon={<RollbackOutlined />} disabled>Rollback</Button>
      </Tooltip>
    );
  }
  return (
    <Tooltip title="Roll back sandbox to this snapshot">
      <Popconfirm
        title={`Roll back to ${snapshotName}?`}
        description={isFullSnapshotName(snapshotName) ? FULL_SNAPSHOT_RESTORE_NOTE : undefined}
        onConfirm={() => state.rollback(snapshotName)}
      >
        <Button
          type="link"
          size="small"
          icon={<RollbackOutlined />}
          loading={state.rollingBack === snapshotName}
          disabled={state.rollingBack !== null && state.rollingBack !== snapshotName}
        >
          Rollback
        </Button>
      </Popconfirm>
    </Tooltip>
  );
}

// The remembered refusal, with a way to ask the API again (after the gate
// was opened for the organization, say).
export function RollbackRefusalNote({ state }: { state: SnapshotRollback }) {
  if (!state.refusal) return null;
  return (
    <Space size={4} wrap>
      <Text type="secondary" style={{ fontSize: 12 }}>{state.refusal}</Text>
      <Button type="link" size="small" style={{ padding: 0, fontSize: 12 }} onClick={state.tryAgain}>Try again</Button>
    </Space>
  );
}
