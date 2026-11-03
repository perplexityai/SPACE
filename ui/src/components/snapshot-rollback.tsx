'use client';

import { useEffect, useState } from 'react';
import { App, Button, Popconfirm, Space, Tooltip, Typography } from 'antd';
import { RollbackOutlined } from '@ant-design/icons';

import { rollbackOutcome } from '@/lib/accepted-create';
import {
  forgetRollbackRefusal,
  rememberRollbackRefusal,
  rememberedRollbackRefusal,
  rollbackGateRefusal,
} from '@/lib/hidden-operations';
import { rollbackConsoleSandbox } from '@/lib/public-api';

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
// was queued. `via` is the route its snapshots were listed from (null while
// none is known); `onSettled` runs once the outcome is known.
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
  const [refusal, setRefusal] = useState<string | null>(() => rememberedRollbackRefusal(orgId));
  const [rollingBack, setRollingBack] = useState<string | null>(null);
  useEffect(() => setRefusal(rememberedRollbackRefusal(orgId)), [orgId]);

  const rollback = async (snapshotName: string) => {
    if (!via || rollingBack || refusal) return;
    const key = `rollback:${sandboxId}`;
    setRollingBack(snapshotName);
    try {
      let follow: () => Promise<unknown>;
      try {
        ({ follow } = await rollbackConsoleSandbox(sandboxId, snapshotName, via));
      } catch (err: any) {
        const reason = rollbackGateRefusal(err, via);
        if (reason) {
          rememberRollbackRefusal(orgId, reason);
          setRefusal(reason);
          message.info(reason);
          return;
        }
        message.error(err?.message || 'failed to queue rollback');
        return;
      }
      message.open({ key, type: 'loading', content: `Rollback to ${snapshotName} queued; following it`, duration: 0 });
      let failure: unknown | null = null;
      try {
        await follow();
      } catch (err) {
        failure = err;
      }
      const outcome = rollbackOutcome(snapshotName, failure);
      message.open({ key, type: outcome.type, content: outcome.text, duration: outcome.type === 'success' ? 4 : 12 });
      onSettled?.();
    } finally {
      setRollingBack(null);
    }
  };

  const tryAgain = () => {
    forgetRollbackRefusal(orgId);
    setRefusal(null);
  };

  return { refusal, rollingBack, rollback, tryAgain };
}

export function RollbackButton({ snapshotName, state }: { snapshotName: string; state: SnapshotRollback }) {
  if (state.refusal) {
    return (
      <Tooltip title={state.refusal}>
        <Button type="link" size="small" icon={<RollbackOutlined />} disabled>Rollback</Button>
      </Tooltip>
    );
  }
  return (
    <Tooltip title="Roll back sandbox to this snapshot">
      <Popconfirm title={`Roll back to ${snapshotName}?`} onConfirm={() => state.rollback(snapshotName)}>
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
