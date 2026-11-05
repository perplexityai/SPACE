'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { Alert, App, Button, Modal, Select, Space, Typography } from 'antd';
import { adminApi } from '@/lib/api';
import { idempotencyKey, postPublicOperation } from '@/lib/public-api';
import { errorMessage, shortHash } from '@/lib/template-format';
import { READ_TIMEOUT_MS } from '@/lib/template-reads';
import type { TemplateBuildStart, TemplateBuildStatus } from '@/lib/types';
import type { TemplateActions } from './template-actions';
import type { TemplatesData } from './use-templates-data';
import { templateHref } from './links';

const { Text, Paragraph, Title } = Typography;

interface BuildEnvironment {
  environment: string;
  architecture: string;
  runtime: string;
  runtime_version: string;
  kernel_version: string;
}

const pairLabel = (pair: BuildEnvironment) =>
  `${pair.runtime === 'firecracker' ? 'Firecracker' : pair.runtime === 'cloud-hypervisor' ? 'Cloud Hypervisor' : pair.runtime} ${pair.runtime_version} · Linux ${pair.kernel_version} · ${pair.architecture}`;

export default function SeedBuilder({ data, actions }: { data: TemplatesData; actions: TemplateActions }) {
  const { message } = App.useApp();
  const [open, setOpen] = useState(false);
  const [pairs, setPairs] = useState<BuildEnvironment[]>([]);
  const [pairsLoading, setPairsLoading] = useState(true);
  const [pairsError, setPairsError] = useState<string | null>(null);
  const [environment, setEnvironment] = useState('');
  const [attempt, setAttempt] = useState<{ key: string; environment: string } | null>(null);
  const [job, setJob] = useState<TemplateBuildStatus | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sandboxId, setSandboxId] = useState<string | null>(null);
  const jobId = job?.job_id;
  const refreshReleases = data.refreshTemplateReleases;
  const terminal = job && ['succeeded', 'failed', 'publish_failed'].includes(job.state);
  const release = data.templateReleases.find((entry) => entry.template_name === 'seed' && entry.content_hash === job?.content_hash);
  const pair = pairs.find((entry) => entry.environment === attempt?.environment);

  useEffect(() => {
    let cancelled = false;
    adminApi.get<{ environments: BuildEnvironment[] }>('/v1/system-template-environments', { signal: AbortSignal.timeout(READ_TIMEOUT_MS) })
      .then((result) => { if (!cancelled) setPairs(result.environments); })
      .catch((err) => { if (!cancelled) setPairsError(errorMessage(err)); })
      .finally(() => { if (!cancelled) setPairsLoading(false); });
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (!jobId || terminal) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      try {
        const status = await adminApi.get<TemplateBuildStatus>(`/v1/templates/seed/builds/${encodeURIComponent(jobId)}`, { signal: AbortSignal.timeout(READ_TIMEOUT_MS) });
        if (cancelled) return;
        setJob(status);
        setError(null);
        if (['succeeded', 'failed', 'publish_failed'].includes(status.state)) {
          await refreshReleases({ showLoading: false });
          return;
        }
      } catch (err) {
        if (cancelled) return;
        setError(errorMessage(err));
      }
      if (!cancelled) timer = setTimeout(poll, 3000);
    };
    void poll();
    return () => { cancelled = true; if (timer) clearTimeout(timer); };
  }, [jobId, terminal, refreshReleases]);

  const build = async () => {
    const selected = attempt ?? { key: idempotencyKey(), environment };
    setAttempt(selected);
    setBusy(true);
    setError(null);
    try {
      const started = await adminApi.post<TemplateBuildStart>('/v1/system-templates/seed/builds', {
        publish_tags: [],
        ...(selected.environment ? { environment: selected.environment } : {}),
      }, { 'Idempotency-Key': selected.key });
      setJob({ ...started, state: 'pending', content_hash: null, error: null });
      setOpen(false);
    } catch (err) {
      setError(`${errorMessage(err)}. Retry uses the same request key; the build may already have started.`);
    } finally {
      setBusy(false);
    }
  };

  const test = async () => {
    if (!job?.content_hash) return;
    setBusy(true);
    try {
      const accepted = await postPublicOperation('/sandboxes', {
        name: `seed-check-${job.job_id}`,
        template: `seed@${job.content_hash}`,
        runtime: 'fc',
        env: {},
        metadata: {},
        resources: null,
      });
      setSandboxId(accepted.sandbox_id);
    } catch (err) {
      message.error(`${errorMessage(err)}. Check the sandbox list before retrying.`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Space orientation="vertical" size={12} style={{ width: '100%', marginTop: 24 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between' }}>
        <Title level={5} style={{ margin: 0 }}>Seed builder</Title>
        <Button type="primary" disabled={Boolean(job) || busy} onClick={() => setOpen(true)}>{attempt && !job ? 'Retry build request' : 'Build seed'}</Button>
      </Space>
      {job ? (
        <Alert type={job.state === 'failed' || job.state === 'publish_failed' ? 'error' : 'info'} title={job.state === 'succeeded' ? `Built seed@${shortHash(job.content_hash)}` : `Seed build: ${job.state}`} description={(
          <Space orientation="vertical" size={8}>
            <Text>Runtime/kernel pair: {pair ? pairLabel(pair) : attempt?.environment || 'Cluster default (bundled recipe)'}</Text>
            <Text>Job: <Text code copyable>{job.job_id}</Text>. The build continues if you leave this page.</Text>
            {job.error ? <Text type="danger">{job.error}</Text> : null}
            <Space wrap>
              <Link href={templateHref('seed', 'builds')}>Seed builds</Link>
              {release ? <Link href={templateHref('seed', 'releases', release.content_hash)}>Seed release</Link> : null}
              {job.state === 'succeeded' && !sandboxId ? <Button onClick={() => void test()} loading={busy}>Create a test sandbox</Button> : null}
              {sandboxId ? <Text>Test sandbox: <Link href={`/dashboard/sandboxes/${encodeURIComponent(sandboxId)}`}>{sandboxId}</Link>. Delete it when you are done.</Text> : null}
              {release && !release.tags.includes('latest') && actions.canManageTemplateReleases ? <Button onClick={() => actions.openRetagModal(release)}>Promote seed…</Button> : null}
              {release?.tags.includes('latest') ? <Text>seed:latest points to this release.</Text> : null}
              {terminal ? <Button onClick={() => { setAttempt(null); setJob(null); setSandboxId(null); setError(null); }}>Dismiss</Button> : null}
            </Space>
          </Space>
        )} />
      ) : null}
      {error && !open ? <Alert type="warning" title={error} /> : null}
      <Modal title="Build seed" open={open} okText={attempt ? 'Retry build request' : 'Build seed'} onOk={() => void build()} onCancel={() => setOpen(false)} confirmLoading={busy} closable={!busy} maskClosable={!busy} cancelButtonProps={{ disabled: busy }} okButtonProps={{ disabled: pairsLoading || Boolean(environment && !pairs.some((entry) => entry.environment === environment)) }}>
        <Space orientation="vertical" size={16} style={{ width: '100%' }}>
          <Paragraph style={{ margin: 0 }}>Build a seed from the recipe bundled with this cluster. Nothing else is built or promoted.</Paragraph>
          <div>
            <label htmlFor="seed-runtime-kernel-pair">Runtime/kernel pair</label>
            <Select id="seed-runtime-kernel-pair" aria-label="Runtime/kernel pair" style={{ width: '100%', marginTop: 8 }} value={environment} onChange={setEnvironment} disabled={Boolean(attempt) || busy || pairsLoading} loading={pairsLoading} showSearch={{ optionFilterProp: 'label' }} options={[{ value: '', label: 'Cluster default (bundled recipe)' }, ...pairs.map((entry) => ({ value: entry.environment, label: pairLabel(entry) }))]} />
          </div>
          <Text type="secondary">Qualified pairs come from the server catalog. The builder checks installation and execution policy; this does not change the cluster default.</Text>
          {environment ? <Text code copyable>{environment}</Text> : null}
          {pairsError ? <Alert type="warning" title="Could not load qualified pairs" description={`${pairsError}. The bundled recipe is still available.`} /> : null}
          {error ? <Alert type="error" title={error} /> : null}
          <Alert type="info" title="Build only, no promotion" description="The seed release is registered by digest. seed:latest moves only when you explicitly promote that seed." />
          <Text type="secondary">After a reload, find the job in <Link href={templateHref('seed', 'builds')}>seed builds</Link>.</Text>
        </Space>
      </Modal>
    </Space>
  );
}
