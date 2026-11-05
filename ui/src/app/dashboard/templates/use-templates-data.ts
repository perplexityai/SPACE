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

export interface RefreshOptions {
  showLoading?: boolean;
  includeBaseSnapshotPolicies?: boolean;
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
  // Numbers each release read, so a slower, older answer cannot replace a
  // newer one (a promotion's state is read from this list).
  const releasesRequestRef = useRef(0);
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

  const refreshTemplateReleases = async ({ showLoading = true }: RefreshOptions = {}) => {
    if (showLoading) setLoadingReleases(true);
    try {
      const request = ++releasesRequestRef.current;
      const items = await adminApi.get<TemplateRelease[]>('/v1/template-releases');
      if (request === releasesRequestRef.current) {
        setTemplateReleases(items);
        setReleasesLoaded(true);
        setReleasesError(null);
      }
      return items;
    } catch (err: unknown) {
      console.warn(errorMessage(err));
      setReleasesError(errorMessage(err));
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
  const refreshSpacedCounts = async () => {
    if (!canAdminTemplates || !spacedWanted.current) return;
    try {
      setSpacedCounts(await loadSpacedCounts(
        getSpacedVersionCounts,
        () => adminApi.get<Sandbox[]>(`/v1/sandboxes?limit=${SPACED_SAMPLE_LIMIT}`),
      ));
      setSpacedCountsError(null);
    } catch (err: unknown) {
      setSpacedCountsError(errorMessage(err));
    }
  };

  const showSpacedCounts = () => {
    if (spacedWanted.current) return;
    spacedWanted.current = true;
    void refreshSpacedCounts();
  };

  const refreshBaseSnapshotPolicies = async (items: Template[]) => {
    const entries = await Promise.all(
      items.map(async (template) => {
        try {
          const policy = await adminApi.get<BaseSnapshotPolicy>(
            `/v1/templates/${encodedPathSegment(template.name)}/base-snapshot-policy`,
          );
          return [template.name, policy] as const;
        } catch {
          return null;
        }
      }),
    );
    setBaseSnapshotPolicies(Object.fromEntries(entries.filter(Boolean) as [string, BaseSnapshotPolicy][]));
  };

  const hydrateDirectTemplatePools = async (items: Template[]) => {
    if (!isDirectSubstrate || items.length === 0) {
      return items;
    }

    const statuses = await Promise.all(
      items.map(async (template) => {
        const status = await adminApi.get<TemplatePoolStatusResponse>(
          `/v1/templates/${encodedPathSegment(template.name)}/pool`,
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

  const readTemplates = async (): Promise<Template[]> => {
    if (!templateName) return adminApi.get<Template[]>('/v1/templates');
    try {
      const template = await adminApi.get<Template>(`/v1/templates/${encodedPathSegment(templateName)}`);
      setTemplateMissing(false);
      return [template];
    } catch (err: unknown) {
      // The handler's own 404 ("Template not found"), not a missing route.
      if ((err as { status?: unknown })?.status === 404 && !isMissingRoute(err)) {
        setTemplateMissing(true);
        return [];
      }
      throw err;
    }
  };

  const refreshTemplates = async ({
    showLoading = true,
    includeBaseSnapshotPolicies = false,
  }: RefreshOptions = {}) => {
    if (showLoading) setLoadingTemplates(true);
    try {
      const items = await hydrateDirectTemplatePools(await readTemplates());
      setTemplates(items);
      setTemplatesLoaded(true);
      setTemplatesError(null);
      if (includeBaseSnapshotPolicies) {
        await refreshBaseSnapshotPolicies(items);
      }
      return items;
    } catch (err: unknown) {
      console.warn(errorMessage(err));
      setTemplatesError(errorMessage(err));
      throw new Error('failed to refresh templates');
    } finally {
      if (showLoading) setLoadingTemplates(false);
    }
  };

  const refreshInventory = async ({ showLoading = true }: RefreshOptions = {}) => {
    if (showLoading) setLoadingInventory(true);
    try {
      const items = await adminApi.get<NodeTemplateInventory[]>('/v1/nodes/templates');
      setInventory(items);
      setInventoryLoaded(true);
      return items;
    } catch (err: unknown) {
      console.warn(errorMessage(err));
      // Keep the last rendered view when node-api inventory is briefly unavailable.
      throw new Error('failed to refresh template inventory');
    } finally {
      if (showLoading) setLoadingInventory(false);
    }
  };

  const refreshClusterPools = async () => {
    if (!supportsClusterPools) {
      setClusterPools([]);
      return [];
    }
    try {
      const items = await adminApi.get<ClusterPoolConfig[]>('/v1/warm-pools');
      setClusterPools(items);
      return items;
    } catch {
      throw new Error('failed to refresh cluster pools');
    }
  };

  const refreshWarmPoolPolicies = async () => {
    if (!supportsClusterPools) {
      setWarmPoolPolicies([]);
      return [];
    }
    try {
      const items = await adminApi.get<WarmPoolPolicy[]>('/v1/warm-pool-policies');
      setWarmPoolPolicies(items);
      return items;
    } catch {
      throw new Error('failed to refresh warm pool policies');
    }
  };

  const refreshWarmPoolPolicyCapabilities = async () => {
    if (!supportsClusterPools) {
      setWarmPoolPolicyCapabilities({});
      return {};
    }
    try {
      const capabilities = await adminApi.get<WarmPoolPolicyCapabilities>(
        '/v1/warm-pool-policy-capabilities',
      );
      setWarmPoolPolicyCapabilities(capabilities);
      return capabilities;
    } catch (err: unknown) {
      // During a rolling deploy, an older API replica may not have this optional endpoint yet.
      console.warn('failed to refresh warm pool policy capabilities', err);
      setWarmPoolPolicyCapabilities({});
      return {};
    }
  };

  const refresh = async ({
    showLoading = true,
    includeBaseSnapshotPolicies = false,
    spaced = false,
  }: RefreshOptions & { spaced?: boolean } = {}) => {
    await Promise.all([
      refreshTemplateReleases({ showLoading }),
      spaced ? refreshSpacedCounts() : Promise.resolve(),
      refreshTemplates({ showLoading, includeBaseSnapshotPolicies }),
      refreshInventory({ showLoading }),
      refreshClusterPools(),
      refreshWarmPoolPolicies(),
      refreshWarmPoolPolicyCapabilities(),
    ]);
    markViewRefreshed(viewKey);
    setRefreshGeneration((generation) => generation + 1);
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
    void runLockedViewRefresh(
      viewKey,
      () => refresh({ showLoading: false }),
      { staleMs: VIEW_REFRESH_STALE_MS },
    ).catch((err: unknown) => console.warn(errorMessage(err)));
  };

  useEffect(() => {
    loadingRef.current = loadingReleases || loadingTemplates || loadingInventory;
  }, [loadingReleases, loadingTemplates, loadingInventory]);

  useEffect(() => {
    void refresh({ includeBaseSnapshotPolicies: withBaseSnapshotPolicy }).catch((err: unknown) => message.error(errorMessage(err)));
    const interval = setInterval(maybeRefresh, VIEW_REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
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
