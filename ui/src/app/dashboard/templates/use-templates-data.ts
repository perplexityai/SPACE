'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { App } from 'antd';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { isMissingRoute } from '@/lib/admin-fallback';
import { isManagingOrgAdmin } from '@/lib/userRoles';
import { getSpacedVersionCounts } from '@/lib/public-api';
import { SPACED_SAMPLE_LIMIT, SpacedCounts, loadSpacedCounts } from '@/lib/spaced-counts';
import { LATEST_TAG, errorMessage, shortHash } from '@/lib/template-format';
import { visibleTemplateTags } from '@/lib/template-list';
import { attemptGate, latestRead, refreshAll, type LatestRead, type ReadWrite } from '@/lib/template-reads';
import {
  markViewRefreshed,
  runLockedViewRefresh,
  VIEW_REFRESH_INTERVAL_MS,
  VIEW_REFRESH_STALE_MS,
} from '@/lib/view-refresh';
import type {
  ClusterPoolConfig,
  NodeTemplateInventory,
  Sandbox,
  SpaceNode,
  Template,
  TemplateRelease,
  WarmPoolPolicy,
  WarmPoolPolicyCapabilities,
} from '@/lib/types';

const REFRESH_CLICK_COOLDOWN_MS = 3000;

export interface BaseSnapshotPolicy {
  template: string;
  ensure_base_snapshot: boolean;
}

export interface TemplatePoolStatusResponse {
  template: string;
  policy: {
    target_count: number;
    boot_mode: string;
  } | null;
  observed: {
    pool_target: number | null;
    pool_current: number;
    pool_pending: number;
    sandbox_ids: string[];
  } | null;
}

export interface TemplateNodeAvailability {
  node: SpaceNode;
  snapshots: NonNullable<NodeTemplateInventory['builds'][number]['snapshots']>;
}

// One template@hash and the reachable nodes that hold it.
export interface TemplateAvailabilityRow {
  key: string;
  name: string;
  content_hash: string | null;
  tags: string[];
  build_date: string | null;
  runtime: string | null;
  arch: string | null;
  artifact_size_bytes: number | null;
  cache_size_bytes: number | null;
  disk_size_mb: number | null;
  vcpu: number | null;
  memory_mib: number | null;
  release: TemplateRelease | null;
  nodes: TemplateNodeAvailability[];
}

// `poll`: a periodic or automatic read, which joins a read of its kind
// already out. `next`: an explicit read (after a change, or the operator's
// Refresh), which supersedes it. See `@/lib/template-reads`.
export type ReadMode = 'poll' | 'next';

export interface RefreshOptions {
  showLoading?: boolean;
  includeBaseSnapshotPolicies?: boolean;
  mode?: ReadMode;
}

function runRead<T>(slot: LatestRead<T>, mode: ReadMode, read: (signal: AbortSignal) => Promise<T>, write: ReadWrite<T>) {
  return mode === 'poll' ? slot.poll(read, write) : slot.next(read, write);
}

interface TemplatesAnswer {
  items: Template[];
  // The template page's template is missing (404).
  missing: boolean;
  // Read only when asked for.
  policies?: Record<string, BaseSnapshotPolicy>;
}

export function encodedPathSegment(value: string) {
  return encodeURIComponent(value);
}

export function nodeLabel(node: SpaceNode) {
  const identity = node.name || node.id.slice(0, 8);
  const where = node.node_api_endpoint ?? `${node.managed_by} substrate`;
  return `${identity} - ${node.role} - ${where}`;
}

function releasePoolKey(release: TemplateRelease) {
  return `${release.template_name}:${release.content_hash}`;
}

interface Options {
  // The view's refresh key: windows showing the same view share one refresh.
  viewKey: string;
  // Only this template (the template page), read with `GET /templates/{name}`.
  templateName?: string;
}

// What the Templates views read, and their refreshes. The list reads every
// template; the template page reads its own, and `template` is null once
// the API answers that it does not exist. Every view reads the releases, the
// node inventory and the cluster's warm pools: the API has no per-template
// form of those reads.
export function useTemplatesData({ viewKey, templateName }: Options) {
  const { message } = App.useApp();
  const { user, cluster, substrate } = useAuth();
  const [templateReleases, setTemplateReleases] = useState<TemplateRelease[]>([]);
  const [templates, setTemplates] = useState<Template[]>([]);
  // The template page's template is missing (404).
  const [templateMissing, setTemplateMissing] = useState(false);
  const [templatesLoaded, setTemplatesLoaded] = useState(false);
  // The last template and release reads' failures, cleared by a read that
  // answers; a view shows them instead of loading forever.
  const [templatesError, setTemplatesError] = useState<string | null>(null);
  const [releasesError, setReleasesError] = useState<string | null>(null);
  const [baseSnapshotPolicies, setBaseSnapshotPolicies] = useState<Record<string, BaseSnapshotPolicy>>({});
  const [inventory, setInventory] = useState<NodeTemplateInventory[]>([]);
  const [inventoryLoaded, setInventoryLoaded] = useState(false);
  const [clusterPools, setClusterPools] = useState<ClusterPoolConfig[]>([]);
  const [warmPoolPolicies, setWarmPoolPolicies] = useState<WarmPoolPolicy[]>([]);
  const [warmPoolPolicyCapabilities, setWarmPoolPolicyCapabilities] = useState<WarmPoolPolicyCapabilities>({});
  const [loadingReleases, setLoadingReleases] = useState(true);
  // A promotion's state is read from the releases; until they first load it
  // is unknown. Before they do, no template can be said to have no release.
  const [releasesLoaded, setReleasesLoaded] = useState(false);
  // One slot per kind of read: an older answer never replaces a newer one (a
  // promotion's state is read from the releases), and a stalled read times
  // out rather than holding up the next.
  const [reads] = useState(() => ({
    releases: latestRead<TemplateRelease[]>(),
    templates: latestRead<TemplatesAnswer>(),
    inventory: latestRead<NodeTemplateInventory[]>(),
    clusterPools: latestRead<ClusterPoolConfig[]>(),
    policies: latestRead<WarmPoolPolicy[]>(),
    capabilities: latestRead<WarmPoolPolicyCapabilities>(),
    spaced: latestRead<SpacedCounts>(),
  }));
  const [periodicAttempts] = useState(() => attemptGate(VIEW_REFRESH_STALE_MS));
  const [loadingTemplates, setLoadingTemplates] = useState(true);
  const [loadingInventory, setLoadingInventory] = useState(true);
  const [spacedCounts, setSpacedCounts] = useState<SpacedCounts | null>(null);
  const [spacedCountsError, setSpacedCountsError] = useState<string | null>(null);
  // The spaced counts are read once a view shows them, then on Refresh.
  const spacedWanted = useRef(false);
  const loadingRef = useRef(true);
  const [manualRefreshing, setManualRefreshing] = useState(false);
  // Counts the view's completed refreshes; open builds panels read again on each.
  const [refreshGeneration, setRefreshGeneration] = useState(0);
  const [refreshCooldown, setRefreshCooldown] = useState(false);
  const refreshCooldownRef = useRef(false);
  const refreshCooldownTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isDirectSubstrate = substrate === 'space';
  const supportsClusterPools = substrate === 'k8s';
  const canAdminTemplates = isManagingOrgAdmin(user, cluster);

  const startRefreshCooldown = () => {
    refreshCooldownRef.current = true;
    setRefreshCooldown(true);
    if (refreshCooldownTimerRef.current) {
      clearTimeout(refreshCooldownTimerRef.current);
    }
    refreshCooldownTimerRef.current = setTimeout(() => {
      refreshCooldownRef.current = false;
      setRefreshCooldown(false);
      refreshCooldownTimerRef.current = null;
    }, REFRESH_CLICK_COOLDOWN_MS);
  };

  const refreshTemplateReleases = async ({ showLoading = true, mode = 'next' }: RefreshOptions = {}) => {
    if (showLoading) setLoadingReleases(true);
    try {
      return await runRead(
        reads.releases,
        mode,
        (signal) => adminApi.get<TemplateRelease[]>('/v1/template-releases', { signal }),
        {
          value: (items) => {
            setTemplateReleases(items);
            setReleasesLoaded(true);
            setReleasesError(null);
          },
          error: (err) => {
            console.warn(errorMessage(err));
            setReleasesError(errorMessage(err));
          },
        },
      );
    } catch {
      throw new Error('failed to refresh template releases');
    } finally {
      if (showLoading) setLoadingReleases(false);
    }
  };

  // The spaced versions of running sandboxes, per template, counted by the
  // server, or from the admin list's newest rows on a server without the
  // count. Read when a view first shows them and on Refresh, not on the
  // periodic refresh: the sample is large. A failed read keeps the last
  // counts, with the error shown.
  const refreshSpacedCounts = async (mode: ReadMode = 'next') => {
    if (!canAdminTemplates || !spacedWanted.current) return;
    await runRead(
      reads.spaced,
      mode,
      (signal) => loadSpacedCounts(
        getSpacedVersionCounts,
        () => adminApi.get<Sandbox[]>(`/v1/sandboxes?limit=${SPACED_SAMPLE_LIMIT}`, { signal }),
      ),
      {
        value: (counts) => {
          setSpacedCounts(counts);
          setSpacedCountsError(null);
        },
        error: (err) => setSpacedCountsError(errorMessage(err)),
      },
    ).catch(() => undefined);
  };

  const showSpacedCounts = () => {
    if (spacedWanted.current) return;
    spacedWanted.current = true;
    void refreshSpacedCounts('poll');
  };

  const readBaseSnapshotPolicies = async (items: Template[], signal: AbortSignal) => {
    const entries = await Promise.all(
      items.map(async (template) => {
        try {
          const policy = await adminApi.get<BaseSnapshotPolicy>(
            `/v1/templates/${encodedPathSegment(template.name)}/base-snapshot-policy`,
            { signal },
          );
          return [template.name, policy] as const;
        } catch {
          return null;
        }
      }),
    );
    return Object.fromEntries(entries.filter(Boolean) as [string, BaseSnapshotPolicy][]);
  };

  const hydrateDirectTemplatePools = async (items: Template[], signal: AbortSignal) => {
    if (!isDirectSubstrate || items.length === 0) {
      return items;
    }

    const statuses = await Promise.all(
      items.map(async (template) => {
        const status = await adminApi.get<TemplatePoolStatusResponse>(
          `/v1/templates/${encodedPathSegment(template.name)}/pool`,
          { signal },
        );
        return [template.name, status] as const;
      }),
    );
    const statusByTemplate = new Map(statuses);
    return items.map((template) => {
      const status = statusByTemplate.get(template.name);
      if (!status) return template;
      const observed = status.observed;
      const target = status.policy?.target_count ?? observed?.pool_target ?? null;
      return {
        ...template,
        pool_target: target,
        pool_current: observed?.pool_current ?? null,
      };
    });
  };

  const readTemplates = async (signal: AbortSignal): Promise<{ items: Template[]; missing: boolean }> => {
    if (!templateName) return { items: await adminApi.get<Template[]>('/v1/templates', { signal }), missing: false };
    try {
      const template = await adminApi.get<Template>(`/v1/templates/${encodedPathSegment(templateName)}`, { signal });
      return { items: [template], missing: false };
    } catch (err: unknown) {
      // The handler's own 404 ("Template not found"), not a missing route.
      if ((err as { status?: unknown })?.status === 404 && !isMissingRoute(err)) {
        return { items: [], missing: true };
      }
      throw err;
    }
  };

  const refreshTemplates = async ({
    showLoading = true,
    includeBaseSnapshotPolicies = false,
    mode = 'next',
  }: RefreshOptions = {}) => {
    if (showLoading) setLoadingTemplates(true);
    try {
      const answer = await runRead(
        reads.templates,
        mode,
        async (signal): Promise<TemplatesAnswer> => {
          const { items, missing } = await readTemplates(signal);
          const hydrated = await hydrateDirectTemplatePools(items, signal);
          const policies = includeBaseSnapshotPolicies ? await readBaseSnapshotPolicies(hydrated, signal) : undefined;
          return { items: hydrated, missing, policies };
        },
        {
          value: ({ items, missing, policies }) => {
            setTemplateMissing(missing);
            setTemplates(items);
            setTemplatesLoaded(true);
            setTemplatesError(null);
            if (policies) setBaseSnapshotPolicies(policies);
          },
          error: (err) => {
            console.warn(errorMessage(err));
            setTemplatesError(errorMessage(err));
          },
        },
      );
      return answer.items;
    } catch {
      throw new Error('failed to refresh templates');
    } finally {
      if (showLoading) setLoadingTemplates(false);
    }
  };

  const refreshInventory = async ({ showLoading = true, mode = 'next' }: RefreshOptions = {}) => {
    if (showLoading) setLoadingInventory(true);
    try {
      return await runRead(
        reads.inventory,
        mode,
        (signal) => adminApi.get<NodeTemplateInventory[]>('/v1/nodes/templates', { signal }),
        {
          value: (items) => {
            setInventory(items);
            setInventoryLoaded(true);
          },
          // Keep the last rendered view when node-api inventory is briefly unavailable.
          error: (err) => console.warn(errorMessage(err)),
        },
      );
    } catch {
      throw new Error('failed to refresh template inventory');
    } finally {
      if (showLoading) setLoadingInventory(false);
    }
  };

  const refreshClusterPools = async (mode: ReadMode = 'next') => {
    if (!supportsClusterPools) {
      setClusterPools([]);
      return [];
    }
    try {
      return await runRead(
        reads.clusterPools,
        mode,
        (signal) => adminApi.get<ClusterPoolConfig[]>('/v1/warm-pools', { signal }),
        { value: setClusterPools, error: () => undefined },
      );
    } catch {
      throw new Error('failed to refresh cluster pools');
    }
  };

  const refreshWarmPoolPolicies = async (mode: ReadMode = 'next') => {
    if (!supportsClusterPools) {
      setWarmPoolPolicies([]);
      return [];
    }
    try {
      return await runRead(
        reads.policies,
        mode,
        (signal) => adminApi.get<WarmPoolPolicy[]>('/v1/warm-pool-policies', { signal }),
        { value: setWarmPoolPolicies, error: () => undefined },
      );
    } catch {
      throw new Error('failed to refresh warm pool policies');
    }
  };

  const refreshWarmPoolPolicyCapabilities = async (mode: ReadMode = 'next') => {
    if (!supportsClusterPools) {
      setWarmPoolPolicyCapabilities({});
      return {};
    }
    return runRead(
      reads.capabilities,
      mode,
      (signal) => adminApi.get<WarmPoolPolicyCapabilities>('/v1/warm-pool-policy-capabilities', { signal }),
      {
        value: setWarmPoolPolicyCapabilities,
        error: (err) => {
          // During a rolling deploy, an older API replica may not have this optional endpoint yet.
          console.warn('failed to refresh warm pool policy capabilities', err);
          setWarmPoolPolicyCapabilities({});
        },
      },
    ).catch(() => ({}));
  };

  // Open builds panels and the list's latest-build cells read again on each
  // refresh, answered or failed: a failed refresh must not leave their
  // unread rows unread. Only a refresh whose reads all answered makes the
  // view fresh.
  const refresh = async ({
    showLoading = true,
    includeBaseSnapshotPolicies = false,
    spaced = false,
    mode = 'next',
  }: RefreshOptions & { spaced?: boolean } = {}) => {
    await refreshAll([
      refreshTemplateReleases({ showLoading, mode }),
      spaced ? refreshSpacedCounts(mode) : Promise.resolve(),
      refreshTemplates({ showLoading, includeBaseSnapshotPolicies, mode }),
      refreshInventory({ showLoading, mode }),
      refreshClusterPools(mode),
      refreshWarmPoolPolicies(mode),
      refreshWarmPoolPolicyCapabilities(mode),
    ], {
      succeeded: () => markViewRefreshed(viewKey),
      settled: () => setRefreshGeneration((generation) => generation + 1),
    });
  };

  // The template page reads its template's base snapshot policy; the list
  // shows none, so it does not read one per template.
  const withBaseSnapshotPolicy = Boolean(templateName);

  const maybeRefresh = () => {
    if (loadingRef.current) {
      return;
    }
    if (document.hidden) {
      return;
    }
    if (!periodicAttempts.due(Date.now())) {
      return;
    }
    void runLockedViewRefresh(
      viewKey,
      () => {
        periodicAttempts.mark(Date.now());
        return refresh({ showLoading: false, mode: 'poll' });
      },
      { staleMs: VIEW_REFRESH_STALE_MS },
    ).catch((err: unknown) => console.warn(errorMessage(err)));
  };

  useEffect(() => {
    loadingRef.current = loadingReleases || loadingTemplates || loadingInventory;
  }, [loadingReleases, loadingTemplates, loadingInventory]);

  useEffect(() => {
    // What the previous template's reads left is not this template's.
    setTemplateMissing(false);
    setTemplatesLoaded(false);
    setTemplatesError(null);
    void refresh({ includeBaseSnapshotPolicies: withBaseSnapshotPolicy }).catch((err: unknown) => message.error(errorMessage(err)));
    const interval = setInterval(maybeRefresh, VIEW_REFRESH_INTERVAL_MS);
    return () => {
      clearInterval(interval);
      // A read started for the previous template or substrate never writes.
      for (const slot of Object.values(reads)) slot.reset();
    };
  }, [substrate, templateName]);

  useEffect(() => () => {
    if (refreshCooldownTimerRef.current) {
      clearTimeout(refreshCooldownTimerRef.current);
    }
  }, []);

  const handleManualRefresh = () => {
    if (refreshCooldownRef.current || manualRefreshing) return;
    startRefreshCooldown();
    setManualRefreshing(true);
    void runLockedViewRefresh(
      viewKey,
      () => refresh({
        showLoading: false,
        includeBaseSnapshotPolicies: withBaseSnapshotPolicy,
        spaced: true,
      }),
      { force: true },
    ).then((ran) => {
      if (!ran) {
        message.info('Refresh already in progress');
        return;
      }
      message.success('Templates refreshed');
    }).catch((err: unknown) => {
      message.error(errorMessage(err));
    }).finally(() => {
      setManualRefreshing(false);
    });
  };

  const clusterPoolById = useMemo(() => {
    const pools = new Map<string, ClusterPoolConfig>();
    for (const pool of clusterPools) {
      pools.set(pool.id, pool);
    }
    return pools;
  }, [clusterPools]);

  const releaseByTemplateHash = useMemo(() => {
    const releases = new Map<string, TemplateRelease>();
    for (const release of templateReleases) {
      releases.set(releasePoolKey(release), release);
    }
    return releases;
  }, [templateReleases]);

  // Every reachable node's copies, one row per template@hash. The template
  // page passes only its own template's rows on.
  const availabilityRows = useMemo<TemplateAvailabilityRow[]>(() => {
    const rows = new Map<string, TemplateAvailabilityRow>();
    const templateByName = new Map(templates.map((template) => [template.name, template]));

    for (const entry of inventory) {
      if (!entry.reachable || entry.error) continue;
      for (const build of entry.builds) {
        if (templateName && build.name !== templateName) continue;
        const key = `${build.name}:${build.content_hash || LATEST_TAG}`;
        const release = releaseByTemplateHash.get(key) ?? null;
        const template = templateByName.get(build.name);
        const row = rows.get(key) || {
          key,
          name: build.name,
          content_hash: build.content_hash,
          tags: release ? visibleTemplateTags(release.tags, release.content_hash) : [],
          build_date: build.build_date,
          runtime: build.runtime,
          arch: build.arch,
          artifact_size_bytes: build.artifact_size_bytes,
          cache_size_bytes: build.cache_size_bytes,
          disk_size_mb: build.disk_size_mb,
          vcpu: template?.vcpu ?? null,
          memory_mib: template?.memory_mib ?? null,
          release,
          nodes: [],
        };
        if (!row.nodes.some((availability) => availability.node.id === entry.node.id)) {
          row.nodes.push({
            node: entry.node,
            snapshots: build.snapshots,
          });
        }
        rows.set(key, row);
      }
    }

    return Array.from(rows.values()).sort((a, b) => {
      const byName = a.name.localeCompare(b.name);
      if (byName !== 0) return byName;
      return shortHash(a.content_hash).localeCompare(shortHash(b.content_hash));
    });
  }, [inventory, releaseByTemplateHash, templates, templateName]);

  const templateCapableNodes = useMemo<SpaceNode[]>(() => {
    const nodes = new Map<string, SpaceNode>();
    for (const entry of inventory) {
      if (!entry.reachable || entry.error) continue;
      if (entry.node.role !== 'worker' && entry.node.role !== 'builder') continue;
      // The pull mutation hits POST /nodes/:id/template-pulls, which is
      // direct-substrate-only on the backend. Exclude k8s rows so the picker
      // can't offer a target that would 501 on submit.
      if (entry.node.managed_by !== 'space') continue;
      nodes.set(entry.node.id, entry.node);
    }
    return Array.from(nodes.values()).sort((a, b) => nodeLabel(a).localeCompare(nodeLabel(b)));
  }, [inventory]);

  return {
    templates,
    templatesLoaded,
    templatesError,
    templateMissing,
    templateReleases,
    releasesLoaded,
    releasesError,
    loadingReleases,
    loadingTemplates,
    loadingInventory,
    inventory,
    inventoryLoaded,
    availabilityRows,
    templateCapableNodes,
    baseSnapshotPolicies,
    clusterPools,
    clusterPoolById,
    warmPoolPolicies,
    warmPoolPolicyCapabilities,
    spacedCounts,
    spacedCountsError,
    showSpacedCounts,
    refreshGeneration,
    manualRefreshing,
    refreshCooldown,
    handleManualRefresh,
    isDirectSubstrate,
    supportsClusterPools,
    canAdminTemplates,
    refresh,
    refreshTemplates,
    refreshTemplateReleases,
    refreshInventory,
    refreshClusterPools,
    refreshWarmPoolPolicies,
  };
}

export type TemplatesData = ReturnType<typeof useTemplatesData>;
