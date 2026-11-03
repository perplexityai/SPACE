'use client';

import Link from 'next/link';
import { Button, Result, Space } from 'antd';
import { ArrowLeftOutlined, ReloadOutlined } from '@ant-design/icons';

import { loadFailureView, type LoadFailure } from '@/lib/load-failure';

export default function LoadFailureResult({
  failure,
  noun,
  id,
  backHref,
  backLabel,
  onRetry,
}: {
  failure: LoadFailure;
  noun: string;
  id: string;
  backHref: string;
  backLabel: string;
  onRetry: () => void;
}) {
  const view = loadFailureView(failure, noun);
  return (
    <Result
      status="warning"
      title={view.title}
      subTitle={<>{failure.message} <code>{id}</code></>}
      extra={
        <Space>
          {view.retry ? (
            <Button type="primary" icon={<ReloadOutlined />} onClick={onRetry}>
              Retry
            </Button>
          ) : null}
          <Link href={backHref}><ArrowLeftOutlined /> {backLabel}</Link>
        </Space>
      }
    />
  );
}
