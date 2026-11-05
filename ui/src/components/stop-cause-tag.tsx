'use client';

import { Tag, Tooltip } from 'antd';
import { WarningOutlined } from '@ant-design/icons';
import { stopCauseText, stopReasonLabel, type SandboxStopCause } from '@/lib/stop-cause';

// Next to a stopped sandbox's state: what stopped it, with the full sentence
// on hover or keyboard focus. `compact` shows only the icon, for the list's
// narrow State column; it is then an image named by that sentence.
export function StopCauseTag({ cause, compact = false }: { cause: SandboxStopCause | null; compact?: boolean }) {
  if (!cause) return null;
  const text = stopCauseText(cause);
  return (
    <Tooltip title={text} trigger={['hover', 'focus']}>
      <Tag
        color="orange"
        icon={<WarningOutlined />}
        style={{ marginInlineEnd: 0 }}
        data-testid="stop-cause-tag"
        tabIndex={0}
        {...(compact ? { role: 'img', 'aria-label': text } : {})}
      >
        {compact ? null : stopReasonLabel(cause.reason)}
      </Tag>
    </Tooltip>
  );
}
