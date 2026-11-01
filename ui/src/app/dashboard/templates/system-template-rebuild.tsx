'use client';

import { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import Link from 'next/link';
import { Alert, App, Button, Checkbox, Modal, Space, Table, Typography } from 'antd';
import { ApiError, adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { idempotencyKey, postPublicOperation } from '@/lib/public-api';
import { LATEST_TAG, errorMessage, shortHash } from '@/lib/template-format';
import type { TemplateBuildStart, TemplateBuildStatus, TemplateRelease } from '@/lib/types';
import {
  BuildJob,
  PromotionPlanRow,
  RebuildRun,
  SystemTemplate,
  applyLost,
  applyStatus,
  buildFailureText,
  buildSucceeded,
  canBuild,
  canPromote,
  confirmPromotion,
  findRelease,
  isTerminal,
  jobOf,
  latestDigest,
  newRun,
  parseStoredRun,
  pinnedTemplate,
  pollDelayMs,
  promotionPlan,
  promotionRemaining,
  readBackPromotion,
  runExpiresAt,
  storageKey,
  submitFailureText,
  systemBuildRequest,
  withJob,
} from '@/lib/system-template-rebuild';

const { Text, Paragraph } = Typography;
const TEMPLATES: SystemTemplate[] = ['seed', 'code-runtime'];
const formatTime = (iso: string | number) => new Date(iso).toLocaleString();
const digest = (hash: string | null | undefined) => <code>{shortHash(hash, 'none')}</code>;

interface PollTrouble {
  failures: number;
  text: string;
}

interface PromoteDialog {
  // A retry of a promotion that did not complete; decided when the dialog opens.
  retry: boolean;
  loading: boolean;
  plan: PromotionPlanRow[];
  error: string | null;
  acknowledged: boolean;
  running: boolean;
}

interface Props {
  // Reads the releases fresh, and refreshes the page's table with them.
  refreshReleases: () => Promise<TemplateRelease[]>;
  releases: TemplateRelease[];
  // The Template Releases title, which this component's header row carries.
  title: ReactNode;
}

// Rebuilding seed and code-runtime, one explicit step per click. The run is
// stored per cluster and user, for at most a day, so a reload or another tab
// can show it and keep polling its builds; nothing is sent from storage.
export default function SystemTemplateRebuild({ refreshReleases, releases, title }: Props) {
  const { message } = App.useApp();
  const { user, cluster } = useAuth();
  const key = user && cluster ? storageKey(cluster.id, user.id) : null;
  const ids = user && cluster ? { clusterId: cluster.id, userId: user.id } : null;
  const [run, setRun] = useState<RebuildRun | null>(null);
  const [busy, setBusy] = useState<Set<string>>(() => new Set());
  const [trouble, setTrouble] = useState<Partial<Record<SystemTemplate, PollTrouble>>>({});
  const [promote, setPromote] = useState<PromoteDialog | null>(null);
  // Bumped after every poll, so the next one is scheduled even when the
  // answer changed nothing.
  const [pollTick, setPollTick] = useState(0);
  const runRef = useRef<RebuildRun | null>(null);
  runRef.current = run;

  const readStored = (): RebuildRun | null => {
    if (!key || !ids) return null;
    try {
      return parseStoredRun(window.localStorage.getItem(key), ids);
    } catch {
      return null;
    }
  };

  const store = (next: RebuildRun | null) => {
    if (!key) return;
    try {
      if (next) window.localStorage.setItem(key, JSON.stringify(next));
      else window.localStorage.removeItem(key);
    } catch {
      // Without storage the run still shows in this tab.
    }
    setRun(next);
  };

  // Change the stored run, only if it is still the run `runId` names.
  const update = (runId: string, change: (current: RebuildRun) => RebuildRun | null): RebuildRun | null => {
    const current = readStored() ?? runRef.current;
    if (!current || current.runId !== runId) return null;
    const next = change(current);
    if (next) store(next);
    return next;
  };

  useEffect(() => {
    if (!key || !ids) return;
    const stored = readStored();
    // An expired run, or one in a shape this page does not write, is dropped.
    if (!stored && window.localStorage.getItem(key) !== null) window.localStorage.removeItem(key);
    setRun(stored);
    const onStorage = (event: StorageEvent) => {
      if (event.key === key) setRun(parseStoredRun(event.newValue, ids));
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, [key]);

  const withBusy = async (action: string, work: () => Promise<void>) => {
    if (busy.has(action)) return;
    setBusy((current) => new Set(current).add(action));
    try {
      await work();
    } finally {
      setBusy((current) => {
        const next = new Set(current);
        next.delete(action);
        return next;
      });
    }
  };

  // Polls each build that has a job and has not ended. Only reads: the
  // answer is applied to the run and job that asked for it.
  const pollKey = run
    ? `${run.runId}:${pollTick}:` + TEMPLATES.map((t) => { const job = jobOf(run, t); return `${t}:${job?.job_id ?? '-'}:${job?.state ?? '-'}:${isTerminal(job)}:${trouble[t]?.failures ?? 0}`; }).join('|')
    : '';
  useEffect(() => {
    if (!run) return;
    const runId = run.runId;
    const timers: ReturnType<typeof setTimeout>[] = [];
    for (const template of TEMPLATES) {
      const job = jobOf(run, template);
      if (!job || isTerminal(job)) continue;
      const failures = trouble[template]?.failures ?? 0;
      timers.push(setTimeout(async () => {
        try {
          const status = await adminApi.get<TemplateBuildStatus>(`/v1/templates/${template}/builds/${encodeURIComponent(job.job_id)}`);
          const next = update(runId, (current) => applyStatus(current, runId, template, status));
          if (status.state === 'unspecified') {
            setTrouble((t) => ({ ...t, [template]: { failures: failures + 1, text: `The builder reports an unknown state for ${template}.` } }));
          } else {
            setTrouble((t) => ({ ...t, [template]: undefined }));
          }
          if (next && isTerminal(jobOf(next, template))) void refreshReleases().catch(() => undefined);
        } catch (err) {
          if (err instanceof ApiError && err.status === 404) {
            update(runId, (current) => applyLost(current, runId, template, job.job_id));
            setTrouble((t) => ({ ...t, [template]: undefined }));
            return;
          }
          setTrouble((t) => ({ ...t, [template]: { failures: failures + 1, text: `Could not read the ${template} build status: ${errorMessage(err)}` } }));
        } finally {
          setPollTick((n) => n + 1);
        }
      }, failures === 0 && job.state === 'pending' ? 1_000 : pollDelayMs(failures)));
    }
    return () => timers.forEach(clearTimeout);
  }, [pollKey]);

  const start = () => {
    if (!ids || readStored()) return;
    const latestAtStart = {
      seed: latestDigest(releases, 'seed', LATEST_TAG),
      'code-runtime': latestDigest(releases, 'code-runtime', LATEST_TAG),
    };
    store(newRun({ runId: idempotencyKey(), ...ids }, latestAtStart));
  };

  const build = (template: SystemTemplate) => withBusy(`build:${template}`, async () => {
    const current = readStored() ?? runRef.current;
    // Another tab may have started this build since this one rendered.
    if (!current || !canBuild(current, template)) return;
    const seedHash = template === 'code-runtime' && buildSucceeded(current.seed) ? current.seed.content_hash : undefined;
    try {
      const accepted = await adminApi.post<TemplateBuildStart>(`/v1/system-templates/${template}/builds`, systemBuildRequest(template, seedHash));
      const job: BuildJob = { job_id: accepted.job_id, state: 'pending', content_hash: null, error: null, lost: false };
      update(current.runId, (latest) => {
        const { [template]: _dropped, ...submitUnknown } = latest.submitUnknown;
        return withJob({ ...latest, submitUnknown }, template, job);
      });
    } catch (err) {
      const failure = submitFailureText(template, err);
      if (failure.known) message.error(failure.text);
      else update(current.runId, (latest) => ({ ...latest, submitUnknown: { ...latest.submitUnknown, [template]: failure.text } }));
    }
  });

  const createTestSandbox = (template: SystemTemplate) => withBusy(`test:${template}`, async () => {
    const current = readStored() ?? runRef.current;
    const job = current ? jobOf(current, template) : null;
    if (!current || !buildSucceeded(job) || current.testSandboxes[template]) return;
    try {
      const accepted = await postPublicOperation('/sandboxes', {
        name: `rebuild-check-${template}-${current.runId.slice(0, 8)}`,
        template: pinnedTemplate(template, job.content_hash),
        runtime: 'fc',
        env: {},
        metadata: {},
        resources: null,
      });
      update(current.runId, (latest) => ({
        ...latest,
        testSandboxes: { ...latest.testSandboxes, [template]: { id: accepted.sandbox_id, createdAt: new Date().toISOString() } },
      }));
      message.success(`Test sandbox for ${template} requested`);
    } catch (err) {
      message.error(`The test sandbox was not created: ${errorMessage(err)}`);
    }
  });

  const openPromote = async () => {
    if (!run) return;
    const retry = Boolean(run.promotion);
    setPromote({ retry, loading: true, plan: [], error: null, acknowledged: false, running: false });
    try {
      const fresh = await refreshReleases();
      setPromote({ retry, loading: false, plan: promotionPlan(run, fresh, LATEST_TAG), error: null, acknowledged: false, running: false });
    } catch (err) {
      setPromote({ retry, loading: false, plan: [], error: `Could not read the current releases: ${errorMessage(err)}`, acknowledged: false, running: false });
    }
  };

  const confirm = (plan: PromotionPlanRow[]) => withBusy('promote', async () => {
    const current = readStored() ?? runRef.current;
    if (!current || !canPromote(current)) return;
    setPromote((p) => p && { ...p, running: true });
    // The rollback target is written before the first tag request, and a
    // retry keeps the one its first attempt wrote.
    const confirmed = update(current.runId, (latest) => confirmPromotion(latest, plan));
    if (!confirmed?.promotion) return;
    let requestError: string | null = null;
    try {
      // A retry first reads which templates still need to move.
      const fresh = await refreshReleases();
      for (const template of promotionRemaining(confirmed, fresh, LATEST_TAG)) {
        const release = findRelease(fresh, template, confirmed.promotion.targets[template]);
        if (!release) {
          requestError = `the new ${template} release is not listed`;
          break;
        }
        try {
          await adminApi.put(`/v1/template-releases/${release.id}/tag`, { tag: LATEST_TAG });
        } catch (err) {
          requestError = `the ${template} tag request answered: ${errorMessage(err)}`;
          break;
        }
      }
    } catch (err) {
      requestError = errorMessage(err);
    }
    // What actually moved is read back, not taken from the answers.
    try {
      const after = await refreshReleases();
      update(confirmed.runId, (latest) => readBackPromotion(latest, after, LATEST_TAG, requestError));
    } catch (err) {
      update(confirmed.runId, (latest) => latest.promotion
        ? { ...latest, promotion: { ...latest.promotion, error: `could not read the tags back: ${errorMessage(err)}` } }
        : latest);
    }
    setPromote(null);
  });

  const dismiss = () => {
    store(null);
    setTrouble({});
  };

  const header = (
    <Space align="center" style={{ width: '100%', justifyContent: 'space-between', marginTop: 24 }}>
      {title}
      <Button onClick={start} disabled={Boolean(run) || !ids} title={run ? 'Dismiss the current rebuild first' : undefined}>
        Rebuild system templates
      </Button>
    </Space>
  );

  if (!run) return header;

  const failures = TEMPLATES.flatMap((t) => { const job = jobOf(run, t); const text = job ? buildFailureText(t, job) : null; return text ? [text] : []; });
  const promotion = run.promotion;

  const buildRow = (template: SystemTemplate) => {
    const job = jobOf(run, template);
    const unknown = run.submitUnknown[template];
    const test = run.testSandboxes[template];
    let state: ReactNode;
    if (!job) {
      state = template === 'code-runtime' && !buildSucceeded(run.seed) ? <Text type="secondary">After seed succeeds</Text> : <Text type="secondary">Not started</Text>;
    } else if (buildSucceeded(job)) {
      state = <>Built {digest(job.content_hash)}</>;
    } else if (isTerminal(job)) {
      state = <Text type="danger">{job.lost ? 'Job no longer reported' : job.state === 'publish_failed' ? 'Built, not published' : 'Failed'}</Text>;
    } else {
      state = <>{job.state === 'pending' ? 'Queued' : 'Building'} <Text type="secondary">(job {job.job_id})</Text></>;
    }
    const actions: ReactNode[] = [];
    if (canBuild(run, template)) {
      actions.push(
        <Button key="build" type="primary" size="small" loading={busy.has(`build:${template}`)} onClick={() => void build(template)}>
          {template === 'seed' ? 'Build seed' : `Build code-runtime on seed@${shortHash(run.seed?.content_hash, '')}`}
          {unknown ? ' again' : ''}
        </Button>,
      );
    }
    if (buildSucceeded(job) && !test) {
      actions.push(
        <Button key="test" size="small" loading={busy.has(`test:${template}`)} onClick={() => void createTestSandbox(template)}>
          Create a test sandbox
        </Button>,
      );
    }
    return {
      key: template,
      template: <strong>{template}</strong>,
      state: (
        <Space orientation="vertical" size={2}>
          {state}
          {unknown ? <Text type="warning">{unknown}</Text> : null}
          {trouble[template] ? <Text type="warning">{trouble[template]!.text.replace(/\.$/, '')}. Retrying in {Math.round(pollDelayMs(trouble[template]!.failures) / 1000)} s.</Text> : null}
          {test ? (
            <Text>
              Test sandbox: <Link href={`/dashboard/sandboxes/${test.id}`}>{test.id}</Link>; its page shows the spaced version
              its guest reports. Delete it when you are done.
            </Text>
          ) : null}
        </Space>
      ),
      actions: actions.length ? <Space wrap>{actions}</Space> : null,
    };
  };

  const promotionRow = (() => {
    if (!buildSucceeded(run.seed) || !buildSucceeded(run.codeRuntime)) return null;
    if (promotion && !promotion.error && promotion.readBack) {
      return (
        <Alert
          type="success"
          showIcon
          title={`Promoted ${formatTime(promotion.confirmedAt)}: latest read back as seed ${shortHash(promotion.readBack.seed, 'none')} and code-runtime ${shortHash(promotion.readBack['code-runtime'], 'none')}.`}
          description={rollbackText(promotion.previous)}
        />
      );
    }
    return (
      <Space orientation="vertical" size={4}>
        {promotion?.error ? <Alert type="error" showIcon title={`Promotion incomplete: ${promotion.error}`} description={rollbackText(promotion.previous)} /> : null}
        <Text type="secondary">
          Nothing is promoted until you do it. Before promoting, a test sandbox from each new release shows the spaced
          version it carries, and that it boots on this cluster.
        </Text>
        <Space>
          <Button type="primary" loading={busy.has('promote')} onClick={() => void openPromote()}>
            {promotion?.error ? 'Retry promotion…' : 'Promote to latest…'}
          </Button>
        </Space>
      </Space>
    );
  })();

  return (
    <>
      {header}
      <Alert
        type={failures.length ? 'error' : 'info'}
        style={{ margin: '8px 0 12px' }}
        title={`System template rebuild, started ${formatTime(run.startedAt)}`}
        description={(
          <Space orientation="vertical" style={{ width: '100%' }}>
            <Text type="secondary">
              Builds seed from the recipe bundled with this API release, then code-runtime on that exact seed digest.
              Each build publishes a release named only by its digest; nothing moves latest until you promote. Workload
              templates built on top of code-runtime come from their own build workflows, which may run outside SPACE;
              this does not rebuild them. This browser keeps the run until {formatTime(runExpiresAt(run))}, or until you dismiss it.
            </Text>
            <Table
              size="small"
              pagination={false}
              showHeader={false}
              dataSource={TEMPLATES.map(buildRow)}
              columns={[
                { dataIndex: 'template', width: 130 },
                { dataIndex: 'state' },
                { dataIndex: 'actions', width: 360, align: 'right' },
              ]}
            />
            {failures.map((text) => <Text key={text} type="danger">{text}</Text>)}
            {promotionRow}
            <Space>
              <Button onClick={dismiss}>Dismiss</Button>
              <Text type="secondary">Dismissing forgets the run here; builds already sent keep running on the builder.</Text>
            </Space>
          </Space>
        )}
      />
      <Modal
        open={promote !== null}
        title={promote?.retry ? 'Retry the promotion' : 'Promote to latest'}
        okText={promote?.retry ? 'Retry' : 'Promote'}
        onCancel={() => setPromote(null)}
        confirmLoading={promote?.running}
        okButtonProps={{
          disabled: !promote || promote.loading || Boolean(promote.error) || promote.plan.length === 0
            || (promote.plan.some((row) => row.changedSinceStart) && !promote.acknowledged),
        }}
        onOk={() => promote && void confirm(promote.plan)}
        destroyOnHidden
      >
        {promote?.loading ? <Paragraph>Reading the current releases…</Paragraph> : null}
        {promote?.error ? <Alert type="error" showIcon title={promote.error} /> : null}
        {promote && !promote.loading && !promote.error ? (
          <Space orientation="vertical" style={{ width: '100%' }}>
            <Paragraph style={{ marginBottom: 0 }}>
              New sandboxes that ask for latest will use the new releases. Existing sandboxes and pinned warm pools keep theirs.
            </Paragraph>
            <Table
              size="small"
              pagination={false}
              rowKey="template"
              dataSource={promote.plan}
              columns={[
                { title: 'Template', dataIndex: 'template' },
                { title: 'latest now', dataIndex: 'from', render: (v: string | null) => digest(v) },
                { title: 'moves to', dataIndex: 'to', render: (v: string) => digest(v) },
              ]}
            />
            {promote.plan.some((row) => row.changedSinceStart) ? (
              <Alert
                type="warning"
                showIcon
                title="latest changed since this run started"
                description={(
                  <Space orientation="vertical">
                    {promote.plan.filter((row) => row.changedSinceStart).map((row) => (
                      <Text key={row.template}>
                        {row.template}: it was {digest(run.latestAtStart[row.template])} then, and is {digest(row.from)} now.
                        Someone may have moved it on purpose.
                      </Text>
                    ))}
                    <Checkbox checked={promote.acknowledged} onChange={(e) => setPromote((p) => p && { ...p, acknowledged: e.target.checked })}>
                      Promote anyway
                    </Checkbox>
                  </Space>
                )}
              />
            ) : null}
            {promote.retry && promotion ? <Text type="secondary">{rollbackText(promotion.previous)}</Text> : null}
          </Space>
        ) : null}
      </Modal>
    </>
  );
}

// Where latest pointed when the promotion was confirmed, to move it back with
// the tag dialog. A template that had no latest keeps the new release: the
// API does not delete latest.
function rollbackText(previous: Record<SystemTemplate, string | null>): string {
  const parts = TEMPLATES.map((t) => (previous[t] ? `${t} ${shortHash(previous[t])}` : `${t}: none (it keeps the new release)`));
  return `To roll back, assign latest with the tag button in Template Releases to the releases it named before: ${parts.join('; ')}.`;
}
