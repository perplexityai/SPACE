'use client';

import { WarningOutlined } from '@ant-design/icons';
import { Tag, Tooltip } from 'antd';
import { networkPolicyTagText } from '@/lib/network-policy';

interface NetworkPolicyGenerationTagProps {
  desired: number | null | undefined;
  applied: number | null | undefined;
  policyLabel?: string | null;
  desiredSource?: string | null;
  observedSource?: string | null;
}

function sourceLabel(source: string | null | undefined) {
  switch (source) {
    case 'template':
      return 'template';
    case 'override':
      return 'override';
    default:
      return source || 'policy';
  }
}

function generationLabel(
  source: string | null | undefined,
  generation: number | null | undefined,
) {
  if (generation === null || generation === undefined || generation === 0) {
    return 'not observed';
  }
  const prefix = source ? `${sourceLabel(source)}-` : '';
  return `${prefix}gen${generation}`;
}

export default function NetworkPolicyGenerationTag({
  desired,
  applied,
  policyLabel,
  desiredSource,
  observedSource,
}: NetworkPolicyGenerationTagProps) {
  if (!desired) return null;
  const text = networkPolicyTagText(policyLabel, desired, applied);
  const label = text.label;
  const sourceMatches = !desiredSource || !observedSource || desiredSource === observedSource;
  const tooltipTitle = () => {
    const current = generationLabel(observedSource ?? desiredSource, applied);
    const summary = `${text.meaning}\n${text.sync}`;
    if (applied === desired && sourceMatches) {
      return <span style={{ whiteSpace: 'pre-line' }}>{`${summary}\ncurrent: ${current}`}</span>;
    }
    return (
      <span style={{ whiteSpace: 'pre-line' }}>
        {`${summary}\ndesired: ${generationLabel(desiredSource, desired)}\ncurrent: ${current}`}
      </span>
    );
  };

  if (applied === null || applied === undefined) {
    return (
      <Tooltip title={tooltipTitle()}>
        <Tag color="default" style={{ marginRight: 0 }}>
          {label}
        </Tag>
      </Tooltip>
    );
  }

  if (applied === 0) {
    return (
      <Tooltip title={tooltipTitle()}>
        <Tag color="orange" style={{ marginRight: 0 }}>
          {label}
        </Tag>
      </Tooltip>
    );
  }

  if (!sourceMatches) {
    return (
      <Tooltip title={tooltipTitle()}>
        <Tag color="orange" style={{ marginRight: 0 }}>{label}</Tag>
      </Tooltip>
    );
  }

  if (applied < desired) {
    const lag = desired - applied;
    const urgent = lag > 2;
    return (
      <Tooltip title={tooltipTitle()}>
        <Tag color={urgent ? 'red' : 'orange'} icon={urgent ? <WarningOutlined /> : undefined} style={{ marginRight: 0 }}>
          {label}
        </Tag>
      </Tooltip>
    );
  }

  if (applied > desired) {
    return (
      <Tooltip title={tooltipTitle()}>
        <Tag color="gold" icon={<WarningOutlined />} style={{ marginRight: 0 }}>
          {label}
        </Tag>
      </Tooltip>
    );
  }

  return (
    <Tooltip title={tooltipTitle()}>
      <Tag style={{ marginRight: 0 }}>{label}</Tag>
    </Tooltip>
  );
}
