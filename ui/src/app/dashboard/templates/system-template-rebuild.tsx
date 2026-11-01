'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
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
  Promotion,
  PromotionPlanRow,
  RebuildRun,
  SystemTemplate,
  acceptJob,
  acceptTestSandbox,
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
  promotionState,
  runExpiresAt,
  samePlan,
  storageKey,
  submitFailureText,
  systemBuildRequest,
  testSandboxFailureText,
  testSandboxName,
} from '@/lib/system-template-rebuild';

const { Text, Paragraph } = Typography;
const TEMPLATES: SystemTemplate[] = ['seed', 'code-runtime'];
const formatTime = (iso: string | number) => new Date(iso).toLocaleString();

interface BrowserLocks {
  request<T>(name: string, callback: () => T | Promise<T>): Promise<T>;
}

// Runs `work` while holding a lock shared by this browser's tabs, so two
// tabs' read-modify-writes of the stored run do not interleave. Without the
// Web Locks API it runs unlocked.
async function exclusive<T>(name: string, work: () => T): Promise<T> {
  const locks = typeof navigator !== 'undefined' ? (navigator as Navigator & { locks?: BrowserLocks }).locks : undefined;
  return locks ? locks.request(name, () => work()) : work();
}
const digest = (hash: string | null | undefined) => <code>{shortHash(hash, 'none')}</code>;
// The digests a promotion of this run would move latest to.
const targetsKey = (run: RebuildRun | null) => (run ? `${run.runId}:${run.seed?.content_hash ?? ''}:${run.codeRuntime?.content_hash ?? ''}` : '');

interface PollTrouble {
  failures: number;
  text: string;
}

interface PromoteDialog {
  // The run and build digests the dialog was opened for.
  runKey: string;
  runId: string;
  // A retry of a recorded promotion; decided when the dialog opens.
  retry: boolean;
  loading: boolean;
  plan: PromotionPlanRow[];
  error: string | null;
  notice: string | null;
  acknowledged: boolean;
  running: boolean;
}

interface Props {
  // Reads the releases fresh, and refreshes the page's table with them.
  refreshReleases: () => Promise<TemplateRelease[]>;
  releases: TemplateRelease[];
  // The releases have been read at least once (until then, a promotion's
  // state is not known).
  releasesLoaded: boolean;
  // The Template Releases title, which this component's header row carries.
  title: ReactNode;
}

// Rebuilding seed and code-runtime, one explicit step per click. The run is
// stored per cluster and user, for at most a day, so a reload or another tab
// can show it and keep polling its builds; nothing is sent from storage.
// Whether a promotion is done is read from the tags, not stored.
export default function SystemTemplateRebuild({ refreshReleases, releases, releasesLoaded, title }: Props) {
  const { message } = App.useApp();
  const { user, cluster } = useAuth();
  const key = user && cluster ? storageKey(cluster.id, user.id) : null;
  const ids = user && cluster ? { clusterId: cluster.id, userId: user.id } : null;
  const [run, setRun] = useState<RebuildRun | null>(null);
  const [busy, setBusy] = useState<Set<string>>(() => new Set());
  const [trouble, setTrouble] = useState<Partial<Record<SystemTemplate, PollTrouble>>>({});
  // What this tab saw that the run does not record: an extra job or sandbox
  // from a click that lost the race, or a save that failed.
  const [notices, setNotices] = useState<string[]>([]);
  const [promote, setPromote] = useState<PromoteDialog | null>(null);
  // How this tab's last promotion attempt ended, when it stopped short.
  // Cleared when another attempt starts; not shown once the tags read as
  // complete.
  const [attemptNote, setAttemptNote] = useState<string | null>(null);
  // Bumped after every poll, so the next one is scheduled even when the
  // answer changed nothing.
  const [pollTick, setPollTick] = useState(0);
  const runRef = useRef<RebuildRun | null>(null);
  runRef.current = run;

  const notice = (text: string) => setNotices((current) => (current.includes(text) ? current : [...current, text]));

  // The stored run; `available` is false when this browser's storage cannot
  // be read, and the run then lives in this tab only.
  const readStored = (): { available: boolean; run: RebuildRun | null } => {
    if (!key || !ids) return { available: false, run: null };
    try {
      return { available: true, run: parseStoredRun(window.localStorage.getItem(key), ids) };
    } catch {
      return { available: false, run: null };
    }
  };

  // Saves the run (or forgets it); false when the browser refused.
  const store = (next: RebuildRun | null): boolean => {
    let saved = true;
    if (key) {
      try {
        if (next) window.localStorage.setItem(key, JSON.stringify(next));
        else window.localStorage.removeItem(key);
      } catch {
        saved = false;
      }
    }
    setRun(next);
    return saved;
  };

  // The run `runId` names, if it still exists: not dismissed in another tab
  // and not expired. Checked before every request that changes something and
  // before every write of an answer.
  const live = (runId: string): RebuildRun | null => {
    const stored = readStored();
    const current = stored.available ? stored.run : runRef.current;
    if (!current || current.runId !== runId || Date.now() >= runExpiresAt(current)) {
      if (runRef.current?.runId === runId) setRun(stored.available ? stored.run : null);
      return null;
    }
    return current;
  };

  const lockName = `${key ?? 'none'}:write`;
  const unsaved = () => notice('This browser could not save the rebuild; a reload or another tab will not see the latest step.');

  // Every change to the stored run is a read-modify-write under the lock.
  const update = (runId: string, change: (current: RebuildRun) => RebuildRun | null): Promise<RebuildRun | null> => exclusive(lockName, () => {
    const current = live(runId);
    if (!current) return null;
    const next = change(current);
    if (next && !store(next)) unsaved();
    return next;
  });

  useEffect(() => {
    if (!key || !ids) return;
    const stored = readStored();
    // An expired run, or one in a shape this page does not write, is dropped.
    try {
      if (stored.available && !stored.run && window.localStorage.getItem(key) !== null) window.localStorage.removeItem(key);
    } catch {
      // Nothing to drop.
    }
    setRun(stored.run);
    const onStorage = (event: StorageEvent) => {
      if (event.key === key) setRun(parseStoredRun(event.newValue, ids));
    };
    window.addEventListener('storage', onStorage);
    return () => window.removeEventListener('storage', onStorage);
  }, [key]);

  // The panel clears when the run expires, in every tab.
  useEffect(() => {
    if (!run) return;
    const timer = setTimeout(() => live(run.runId), Math.max(0, runExpiresAt(run) - Date.now()) + 50);
    return () => clearTimeout(timer);
  }, [run?.runId, run?.startedAt]);

  // The promotion dialog belongs to the run and build digests it opened
  // for; it closes when another tab dismisses the run or starts another.
  useEffect(() => {
    if (promote && promote.runKey !== targetsKey(run)) {
      setPromote(null);
      message.info('The rebuild changed in another tab, so the promotion dialog closed.');
    }
  }, [targetsKey(run)]);

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
        if (!live(runId)) return;
        try {
          const status = await adminApi.get<TemplateBuildStatus>(`/v1/templates/${template}/builds/${encodeURIComponent(job.job_id)}`);
          const next = await update(runId, (current) => applyStatus(current, runId, template, status));
          if (status.state === 'unspecified') {
            setTrouble((t) => ({ ...t, [template]: { failures: failures + 1, text: `The builder reports an unknown state for ${template}.` } }));
          } else {
            setTrouble((t) => ({ ...t, [template]: undefined }));
          }
          if (next && isTerminal(jobOf(next, template))) void refreshReleases().catch(() => undefined);
        } catch (err) {
          if (err instanceof ApiError && err.status === 404) {
            await update(runId, (current) => applyLost(current, runId, template, job.job_id));
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

  // `latest` when the run starts comes from a fresh read, not the page's
  // cached list, which may not have loaded.
  const start = () => withBusy('start', async () => {
    if (!ids || readStored().run) return;
    let fresh: TemplateRelease[];
    try {
      fresh = await refreshReleases();
    } catch (err) {
      message.error(`Could not read the current releases, so no rebuild was started: ${errorMessage(err)}`);
      return;
    }
    const latestAtStart = {
      seed: latestDigest(fresh, 'seed', LATEST_TAG),
      'code-runtime': latestDigest(fresh, 'code-runtime', LATEST_TAG),
    };
    await exclusive(lockName, () => {
      // Another tab may have started one meanwhile; show that one instead.
      const existing = readStored().run;
      if (existing) {
        setRun(existing);
        return;
      }
      setNotices([]);
      if (!store(newRun({ runId: idempotencyKey(), ...ids }, latestAtStart))) {
        notice('This browser could not save the rebuild; it shows in this tab only, and a reload loses it.');
      }
    });
  });

  const build = (template: SystemTemplate) => withBusy(`build:${template}`, async () => {
    const runId = runRef.current?.runId;
    const current = runId ? live(runId) : null;
    // Another tab may have started this build since this one rendered.
    if (!current || !canBuild(current, template)) return;
    const seedHash = template === 'code-runtime' && buildSucceeded(current.seed) ? current.seed.content_hash : undefined;
    try {
      const accepted = await adminApi.post<TemplateBuildStart>(`/v1/system-templates/${template}/builds`, systemBuildRequest(template, seedHash));
      const job: BuildJob = { job_id: accepted.job_id, state: 'pending', content_hash: null, error: null, lost: false };
      const outcome = await exclusive(lockName, () => {
        const accepted = acceptJob(live(current.runId), current.runId, template, job);
        if (accepted.kind === 'recorded' && !store(accepted.run)) unsaved();
        return accepted;
      });
      if (outcome.kind === 'extra') {
        notice(`Another click also started ${template} job ${accepted.job_id}; this run follows job ${outcome.existing.job_id}. The extra job publishes a release of its own.`);
      } else {
        notice(`The rebuild was dismissed or expired meanwhile; ${template} job ${accepted.job_id} keeps running on the builder.`);
      }
    } catch (err) {
      const failure = submitFailureText(template, err);
      if (failure.known) message.error(failure.text);
      else await update(current.runId, (latest) => (jobOf(latest, template) ? null : { ...latest, submitUnknown: { ...latest.submitUnknown, [template]: failure.text } }));
    }
  });

  const createTestSandbox = (template: SystemTemplate) => withBusy(`test:${template}`, async () => {
    const runId = runRef.current?.runId;
    const current = runId ? live(runId) : null;
    const job = current ? jobOf(current, template) : null;
    if (!current || !buildSucceeded(job) || current.testSandboxes[template]) return;
    const name = testSandboxName(template, current.runId);
    try {
      const accepted = await postPublicOperation('/sandboxes', {
        name,
        template: pinnedTemplate(template, job.content_hash),
        runtime: 'fc',
        env: {},
        metadata: {},
        resources: null,
      });
      const outcome = await exclusive(lockName, () => {
        const recorded = acceptTestSandbox(live(current.runId), current.runId, template, { id: accepted.sandbox_id, createdAt: new Date().toISOString() });
        if (recorded.kind === 'recorded' && !store(recorded.run)) unsaved();
        return recorded;
      });
      if (outcome.kind === 'recorded') {
        message.success(`Test sandbox for ${template} requested`);
      } else if (outcome.kind === 'extra') {
        notice(`Another click also created test sandbox ${accepted.sandbox_id} for ${template}; this run keeps ${outcome.existing.id}. Delete ${accepted.sandbox_id}.`);
      } else {
        notice(`The rebuild was dismissed or expired meanwhile; test sandbox ${accepted.sandbox_id} was created. Delete it when you are done.`);
      }
    } catch (err) {
      const failure = testSandboxFailureText(name, err);
      if (failure.known) message.error(failure.text);
      else notice(failure.text);
    }
  });

  const openPromote = async () => {
    if (!run || !canPromote(run)) return;
    const base = { runKey: targetsKey(run), runId: run.runId, retry: Boolean(run.promotion), notice: null, acknowledged: false, running: false };
    setPromote({ ...base, loading: true, plan: [], error: null });
    try {
      const fresh = await refreshReleases();
      setPromote((p) => p && p.runKey === base.runKey ? { ...p, loading: false, plan: promotionPlan(run, fresh, LATEST_TAG) } : p);
    } catch (err) {
      setPromote((p) => p && { ...p, loading: false, error: `Could not read the current releases: ${errorMessage(err)}` });
    }
  };

  const confirm = (dialog: PromoteDialog) => withBusy('promote', async () => {
    const current = live(dialog.runId);
    if (!current || targetsKey(current) !== dialog.runKey || !canPromote(current)) {
      setPromote(null);
      message.error('This promotion belongs to a rebuild that is no longer current; nothing was tagged.');
      return;
    }
    setPromote((p) => p && { ...p, running: true });
    setAttemptNote(null);
    // The tags again, now: if anything moved since the dialog showed the
    // plan, show the new plan and ask again.
    let fresh: TemplateRelease[];
    try {
      fresh = await refreshReleases();
    } catch (err) {
      setPromote((p) => p && { ...p, running: false, error: `Could not read the current releases, so nothing was tagged: ${errorMessage(err)}` });
      return;
    }
    const plan = promotionPlan(current, fresh, LATEST_TAG);
    if (!samePlan(plan, dialog.plan)) {
      setPromote((p) => p && { ...p, running: false, plan, acknowledged: false, notice: 'latest changed while this dialog was open. Check the moves again.' });
      return;
    }
    // The rollback target is saved before the first tag request; without
    // it, nothing is tagged.
    const saved = await exclusive(lockName, () => {
      const latest = live(dialog.runId);
      if (!latest || targetsKey(latest) !== dialog.runKey) return { ok: false as const, reason: 'gone' };
      const next = confirmPromotion(latest, plan);
      if (next !== latest && !store(next)) {
        setRun(latest);
        return { ok: false as const, reason: 'unsaved' };
      }
      return { ok: true as const, run: next };
    });
    if (!saved.ok) {
      setPromote((p) => p && { ...p, running: false, error: saved.reason === 'unsaved'
        ? 'This browser could not save the promotion record (the rollback target), so nothing was tagged.'
        : 'This rebuild was dismissed or changed in another tab; nothing was tagged.' });
      return;
    }
    const confirmed = saved.run;
    let requestError: string | null = null;
    for (const row of plan) {
      if (!live(confirmed.runId)) {
        requestError = 'the rebuild was dismissed in another tab';
        break;
      }
      const release = findRelease(fresh, row.template, row.to);
      if (!release) {
        requestError = `the new ${row.template} release is not listed`;
        break;
      }
      try {
        await adminApi.put(`/v1/template-releases/${release.id}/tag`, { tag: LATEST_TAG });
      } catch (err) {
        // The tag route can commit and still answer an error: the read below decides.
        requestError = `the ${row.template} tag request answered: ${errorMessage(err)}`;
        break;
      }
    }
    setPromote(null);
    // What moved is read back, not taken from the answers; the panel shows
    // it from the releases this refresh reads.
    try {
      await refreshReleases();
    } catch (err) {
      requestError = `${requestError ? `${requestError}; then ` : ''}could not read the tags back (${errorMessage(err)}). Refresh to see what moved`;
    }
    if (requestError) setAttemptNote(`The last promotion attempt stopped: ${requestError}.`);
  });

  const dismiss = () => {
    store(null);
    setTrouble({});
    setNotices([]);
    setAttemptNote(null);
    setPromote(null);
  };

  // A recorded promotion's state, from the tags as last read.
  const promotionView = useMemo(
    () => (run?.promotion && releasesLoaded ? promotionState(run.promotion, releases, LATEST_TAG) : null),
    [run?.promotion, releases, releasesLoaded],
  );

  const header = (
    <Space align="center" style={{ width: '100%', justifyContent: 'space-between', marginTop: 24 }}>
      {title}
      <Button onClick={() => void start()} loading={busy.has('start')} disabled={Boolean(run) || !ids} title={run ? 'Dismiss the current rebuild first' : undefined}>
        Rebuild system templates
      </Button>
    </Space>
  );

  if (!run) return header;

  const failures = TEMPLATES.flatMap((t) => { const job = jobOf(run, t); const text = job ? buildFailureText(t, job) : null; return text ? [text] : []; });

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
    if (!canPromote(run)) return null;
    const promotion = run.promotion;
    if (!promotion) {
      return (
        <Space orientation="vertical" size={4}>
          <Text type="secondary">
            Nothing is promoted until you do it. Before promoting, a test sandbox from each new release shows the spaced
            version it carries, and that it boots on this cluster.
          </Text>
          <Space>
            <Button type="primary" loading={busy.has('promote')} onClick={() => void openPromote()}>Promote to latest…</Button>
          </Space>
        </Space>
      );
    }
    if (!promotionView) return <Text type="secondary">Promoted {formatTime(promotion.confirmedAt)}; reading latest…</Text>;
    if (promotionView.kind === 'complete') {
      return (
        <Alert
          type="success"
          showIcon
          title={`Promoted ${formatTime(promotion.confirmedAt)}. As read just now, latest names ${(Object.keys(promotionView.latest) as SystemTemplate[]).map((t) => `${t} ${shortHash(promotionView.latest[t])}`).join(' and ')}.`}
          description={rollbackText(promotion)}
        />
      );
    }
    return (
      <Space orientation="vertical" size={4} style={{ width: '100%' }}>
        <Alert
          type="warning"
          showIcon
          title={`Promotion incomplete: ${promotionView.remaining.join(' and ')} still ${promotionView.remaining.length > 1 ? 'have' : 'has'} to move to the new release.`}
          description={(
            <Space orientation="vertical" size={2}>
              {promotionView.diverged.map(({ template, latest }) => (
                <Text key={template}>
                  {template}: latest is {digest(latest)}, neither the new release nor the one it named before. Someone moved it;
                  a retry asks before overwriting it.
                </Text>
              ))}
              <Text>{rollbackText(promotion)}</Text>
            </Space>
          )}
        />
        <Space>
          <Button type="primary" loading={busy.has('promote')} onClick={() => void openPromote()}>Retry promotion…</Button>
        </Space>
      </Space>
    );
  })();

  const needsAck = promote?.plan.some((row) => row.attention) ?? false;

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
            {notices.map((text) => <Text key={text} type="warning">{text}</Text>)}
            {attemptNote && promotionView?.kind !== 'complete' ? <Text type="warning">{attemptNote}</Text> : null}
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
        okButtonProps={{ disabled: !promote || promote.loading || Boolean(promote.error) || promote.plan.length === 0 || (needsAck && !promote.acknowledged) }}
        onOk={() => promote && void confirm(promote)}
        destroyOnHidden
      >
        {promote?.loading ? <Paragraph>Reading the current releases…</Paragraph> : null}
        {promote?.error ? <Alert type="error" showIcon title={promote.error} /> : null}
        {promote && !promote.loading && !promote.error ? (
          <Space orientation="vertical" style={{ width: '100%' }}>
            {promote.notice ? <Alert type="warning" showIcon title={promote.notice} /> : null}
            {promote.plan.length === 0 ? (
              <Paragraph>latest already names the new releases; there is nothing to move.</Paragraph>
            ) : (
              <>
                <Paragraph style={{ marginBottom: 0 }}>
                  New sandboxes that ask for latest will use the new releases. Existing sandboxes and pinned warm pools
                  keep theirs. The tag route cannot refuse a change made after this dialog&apos;s final read, just before
                  each request.
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
              </>
            )}
            {needsAck ? (
              <Alert
                type="warning"
                showIcon
                title={promote.retry ? 'latest moved since the promotion was confirmed' : 'latest changed since this run started'}
                description={(
                  <Space orientation="vertical">
                    {promote.plan.filter((row) => row.attention).map((row) => (
                      <Text key={row.template}>
                        {row.template}: it was {digest(row.attention === 'diverged' ? run.promotion?.previous[row.template] : run.latestAtStart[row.template])}
                        {' '}then, and is {digest(row.from)} now. Someone may have moved it on purpose.
                      </Text>
                    ))}
                    <Checkbox checked={promote.acknowledged} onChange={(e) => setPromote((p) => p && { ...p, acknowledged: e.target.checked })}>
                      {promote.retry ? 'Overwrite it' : 'Promote anyway'}
                    </Checkbox>
                  </Space>
                )}
              />
            ) : null}
            {promote.retry && run.promotion ? <Text type="secondary">{rollbackText(run.promotion)}</Text> : null}
          </Space>
        ) : null}
      </Modal>
    </>
  );
}

// Where latest pointed when the promotion was confirmed, to move it back with
// the tag dialog. A template that had no latest keeps the new release: the
// API does not delete latest.
function rollbackText(promotion: Promotion): string {
  const parts = (Object.keys(promotion.targets) as SystemTemplate[]).map((t) => (
    promotion.previous[t] ? `${t} ${shortHash(promotion.previous[t])}` : `${t}: none (it keeps the new release)`
  ));
  return `To roll back, assign latest with the tag button in Template Releases to the releases it named before: ${parts.join('; ')}.`;
}
