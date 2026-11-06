'use client';

import { useEffect, useState } from 'react';
import { Input, Modal, Space, Typography } from 'antd';

import { typedConfirmationMatches } from '@/lib/node-drain';

const { Text } = Typography;

// A confirmation for an action that cannot be taken back: OK, and Enter in
// the box, do nothing until the operator types `phrase` exactly, so neither a
// misclick nor a reflexive Enter sends the action.
export function TypedConfirmModal({
  open,
  title,
  explanation,
  phrase,
  okText,
  onConfirm,
  onCancel,
}: {
  open: boolean;
  title: string;
  explanation: string;
  phrase: string;
  okText: string;
  onConfirm: () => Promise<void>;
  onCancel: () => void;
}) {
  const [typed, setTyped] = useState('');
  const [sending, setSending] = useState(false);
  useEffect(() => {
    if (open) setTyped('');
  }, [open, phrase]);
  const matches = typedConfirmationMatches(typed, phrase);

  const confirm = async () => {
    if (!matches || sending) return;
    setSending(true);
    try {
      await onConfirm();
    } finally {
      setSending(false);
    }
  };

  return (
    <Modal
      open={open}
      title={title}
      okText={okText}
      okButtonProps={{ danger: true, disabled: !matches, loading: sending }}
      cancelButtonProps={{ disabled: sending }}
      onOk={confirm}
      onCancel={() => { if (!sending) onCancel(); }}
      maskClosable={false}
      destroyOnHidden
    >
      <Space orientation="vertical" size={8} style={{ width: '100%' }}>
        <Text>{explanation}</Text>
        <Text>
          Type <Text code>{phrase}</Text> to confirm.
        </Text>
        <Input
          aria-label="Type the node name to confirm"
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
          onPressEnter={confirm}
          autoFocus
          autoComplete="off"
          spellCheck={false}
          disabled={sending}
        />
      </Space>
    </Modal>
  );
}
