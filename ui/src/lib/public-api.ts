import { OperationFailedError, OperationTimeoutError, terminalErrorParts } from './accepted-create';
import { adminApi, api, ApiError } from './api';
import { listSnapshotsWithFallback, pageRows, type SnapshotListing } from './admin-fallback';
import { DEFAULT_LIST_LIMIT, parseSandboxRef, publicSandboxIdFromAny } from './sandbox-id';
import { mergeOperationPages, type SandboxDebug } from './sandbox-debug';
import { parseSandboxListResponse, type SandboxListResponse } from './sandbox-list';
import {
  PublicOperationRecord,
  PublicPage,
  PublicSnapshot,
  PublicTemplate,
  Sandbox,
  SandboxOperationResponse,
  SnapshotInfo,
  Template,
} from './types';

const OPERATION_POLL_INTERVAL_MS = 800;
const OPERATION_POLL_TIMEOUT_MS = 30_000;

export function idempotencyKey(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export async function getPublicPage<T>(path: string): Promise<T[]> {
  const page = await api.get<PublicPage<T>>(path);
  return page.data;
}

// The newest `limit` sandbox rows, across every org or for `orgId` only. The
// unscoped list then appends the rows nodes report without a database row
// (rowless pool slots, observed-local sandboxes; see `sandboxRowCount`); those
// belong to no org, so an org-scoped list leaves them out. The admin list has
// no cursor.
export async function getConsoleSandboxes(
  limit: number = DEFAULT_LIST_LIMIT,
  orgId: string | null = null,
): Promise<Sandbox[]> {
  const params = new URLSearchParams({ limit: String(limit) });
  if (orgId) params.set('org_id', orgId);
  const sandboxes = await adminApi.get<Sandbox[]>(`/v1/sandboxes?${params}`);
  return sandboxes.map(adminSandboxToConsoleSandbox);
}

// One admin list request, built by `sandboxListQuery`: a page when the
// server pages, or the bare array of a server that does not.
export async function getConsoleSandboxList(query: string, signal?: AbortSignal): Promise<SandboxListResponse> {
  const response = parseSandboxListResponse(await adminApi.get<unknown>(`/v1/sandboxes?${query}`, { signal }));
  return { ...response, sandboxes: response.sandboxes.map(adminSandboxToConsoleSandbox) };
}

// `GET /sandboxes/spaced-versions`; see `loadSpacedCounts` for what a server
// without it answers.
export function getSpacedVersionCounts(): Promise<unknown> {
  return adminApi.get<unknown>('/v1/sandboxes/spaced-versions');
}

// `id` may be any form `parseSandboxRef` accepts; the admin route takes only
// the `sbx_` form.
export async function getConsoleSandbox(id: string): Promise<Sandbox> {
  const ref = parseSandboxRef(id);
  if (!ref) {
    throw new ApiError(400, 'Not a sandbox ID, UUID or sandbox pod name');
  }
  return adminSandboxToConsoleSandbox(await adminApi.get<Sandbox>(`/v1/sandboxes/${ref.publicId}`));
}

export interface AdminOperationRecord extends PublicOperationRecord {
  org_id: string | null;
  actor_user_id: string | null;
  // Absent on releases that do not report it.
  actor_email?: string | null;
  api_product: string | null;
  target_type: string | null;
  target_id: string | null;
  attempts: number;
  requested_at: string | null;
}

// Everything the control plane records about one sandbox, from the admin
// debug route, which takes the bare UUID.
export async function getSandboxDebug(id: string): Promise<SandboxDebug> {
  const ref = parseSandboxRef(id);
  if (!ref) throw new ApiError(400, 'Not a sandbox ID, UUID or sandbox pod name');
  return adminApi.get<SandboxDebug>(`/v1/debug/sandbox/${ref.uuid}`);
}

// Lifecycle operations recorded against a sandbox, newest first. They outlive
// the sandbox row, so they also explain a sandbox that was deleted. Most
// operations name the sandbox by its `sbx_` ID; a fork names its source by
// the bare UUID, so both are read and merged. `more` is true when either read
// held more than `limit` rows.
export async function getSandboxOperations(
  id: string,
  { limit = 50, orgId = null }: { limit?: number; orgId?: string | null } = {},
): Promise<{ operations: AdminOperationRecord[]; more: boolean }> {
  const ref = parseSandboxRef(id);
  if (!ref) throw new ApiError(400, 'Not a sandbox ID, UUID or sandbox pod name');
  const read = (targetId: string) => {
    const params = new URLSearchParams({ target_type: 'sandbox', target_id: targetId, limit: String(limit + 1) });
    if (orgId) params.set('org_id', orgId);
    return adminApi.get<AdminOperationRecord[]>(`/v1/operations?${params}`);
  };
  const [byPublicId, byUuid] = await Promise.all([read(ref.publicId), read(ref.uuid)]);
  return mergeOperationPages([byPublicId, byUuid], limit);
}

export async function postPublicOperation(
  path: string,
  body?: unknown,
): Promise<SandboxOperationResponse> {
  return api.post<SandboxOperationResponse>(path, body, {
    'Idempotency-Key': idempotencyKey(),
  });
}

export async function waitForPublicOperation(
  operationId: string,
  timeoutMs: number = OPERATION_POLL_TIMEOUT_MS,
): Promise<PublicOperationRecord> {
  const deadline = Date.now() + timeoutMs;
  let last: PublicOperationRecord | null = null;

  while (Date.now() < deadline) {
    last = await api.get<PublicOperationRecord>(`/operations/${operationId}`);
    if (['succeeded', 'failed', 'canceled', 'expired'].includes(last.state)) {
      if (last.state === 'succeeded') {
        return last;
      }
      const { reason, detail } = terminalErrorParts(last.terminal_error);
      throw new OperationFailedError(last.state, last.progress_message || `operation ${last.state}`, reason, detail);
    }
    await new Promise((resolve) => setTimeout(resolve, OPERATION_POLL_INTERVAL_MS));
  }

  throw new OperationTimeoutError(last?.progress_message || 'operation still running');
}

export async function waitForAdminOperation(
  operationId: string,
  timeoutMs: number = OPERATION_POLL_TIMEOUT_MS,
): Promise<PublicOperationRecord> {
  const deadline = Date.now() + timeoutMs;
  let last: PublicOperationRecord | null = null;

  while (Date.now() < deadline) {
    last = await adminApi.get<PublicOperationRecord>(`/v1/operations/${operationId}`);
    if (['succeeded', 'failed', 'canceled', 'expired'].includes(last.state)) {
      if (last.state === 'succeeded') {
        return last;
      }
      const { reason, detail } = terminalErrorParts(last.terminal_error);
      throw new OperationFailedError(last.state, last.progress_message || `operation ${last.state}`, reason, detail);
    }
    await new Promise((resolve) => setTimeout(resolve, OPERATION_POLL_INTERVAL_MS));
  }

  throw new OperationTimeoutError(last?.progress_message || 'operation still running');
}

// Cross-org admin lifecycle action. The console lists sandboxes across every
// org via the admin API, but the org-scoped public action endpoint
// (`POST /api/v1/sandboxes/{id}:{action}`) rejects any sandbox the signed-in
// admin does not personally own with "sandbox not found". Route lifecycle
// actions through the admin force-action surface (resolved without an org
// filter, run under the sandbox's owning org) — the same surface the list and
// the data-plane command path already use — so wake/restore/start/etc. work on
// every sandbox the console can see.
export async function forceConsoleSandboxAction(
  sandboxId: string,
  action: string,
): Promise<SandboxOperationResponse> {
  return adminApi.post<SandboxOperationResponse>(
    `/v1/sandboxes/${publicSandboxIdFromAny(sandboxId)}:force-${action}`,
    undefined,
    { 'Idempotency-Key': idempotencyKey() },
  );
}

export async function forceDeleteConsoleSandbox(
  sandboxId: string,
): Promise<SandboxOperationResponse> {
  return forceConsoleSandboxAction(sandboxId, 'delete');
}

// Cross-org admin sandbox update (network policy, auto-lifecycle, metadata).
// Same reason as forceConsoleSandboxAction: the org-scoped public
// `PATCH /api/v1/sandboxes/{id}` rejects any sandbox the signed-in admin's
// org does not own with "sandbox not found", so console edits go through the
// admin surface, which resolves the sandbox without an org filter and runs
// the update under its owning org.
export async function updateConsoleSandbox(
  sandboxId: string,
  body: unknown,
): Promise<SandboxOperationResponse> {
  return adminApi.patch<SandboxOperationResponse>(
    `/v1/sandboxes/${publicSandboxIdFromAny(sandboxId)}`,
    body,
    { 'Idempotency-Key': idempotencyKey() },
  );
}

// Create a sandbox under an arbitrary org from the operator surface. The
// public create path is bound to the signed-in admin's own org; this admin
// route enqueues the same create operation under the target org (owner
// defaults to the org's oldest user server-side).
export async function createConsoleSandboxForOrg(
  orgId: string,
  sandbox: unknown,
): Promise<SandboxOperationResponse> {
  return adminApi.post<SandboxOperationResponse>(
    '/v1/sandboxes',
    { org_id: orgId, sandbox },
    { 'Idempotency-Key': idempotencyKey() },
  );
}

function adminSandboxToConsoleSandbox(sandbox: Sandbox): Sandbox {
  return {
    ...sandbox,
    id: publicSandboxIdFromAny(sandbox.id),
  };
}

export function publicTemplateToConsoleTemplate(template: PublicTemplate): Template {
  const variant = template.variants[0] ?? null;
  return {
    name: template.name,
    description: template.description,
    org_id: null,
    desired_state: 'present',
    created_at: template.created_at,
    built_at: variant?.built_at ?? null,
    content_hash: variant?.content_hash ?? null,
    runtime: variant?.runtime ?? null,
    arch: variant?.arch ?? null,
    has_base_snapshot: variant?.has_base_snapshot ?? null,
    pool_target: null,
    pool_current: null,
    artifact_size_bytes: variant?.artifact_size_bytes ?? null,
    cache_size_bytes: null,
    base_snapshot_size_bytes: variant?.base_snapshot_size_bytes ?? null,
    snapshots: variant?.snapshots ?? [],
    vcpu: variant?.resources.vcpu ?? null,
    memory_mib: variant?.resources.memory_mib ?? null,
    disk_size_mb: variant?.resources.disk_mib ?? null,
    default_auto_lifecycle: null,
    network_policy: null,
    network_policy_generation: null,
    network_policy_status: null,
  };
}

export function publicSnapshotToConsoleSnapshot(snapshot: PublicSnapshot): SnapshotInfo {
  return {
    id: snapshot.id,
    type: snapshot.snapshot_type,
    state: snapshot.state,
    timestamp: snapshot.created_at,
    label: snapshot.label,
    retention_tier: snapshot.retention_tier,
    size_bytes: snapshot.size_bytes ?? 0,
  };
}


const ADMIN_SNAPSHOT_PAGE_LIMIT = 1000;

// Any sandbox's snapshots through the admin route, newest page first, up to
// one full page.
async function getAdminSnapshots(sandboxId: string): Promise<SnapshotInfo[]> {
  const body = await adminApi.get<unknown>(
    `/v1/sandboxes/${publicSandboxIdFromAny(sandboxId)}/snapshots?limit=${ADMIN_SNAPSHOT_PAGE_LIMIT}`,
  );
  return pageRows<PublicSnapshot>(body).rows.map(publicSnapshotToConsoleSnapshot);
}

// A sandbox's snapshots, and the route its rollback must use; see
// `listSnapshotsWithFallback`. `ownOrg` says whether the public route may
// serve this sandbox on releases without the admin list.
export async function getConsoleSnapshots(
  sandboxId: string,
  { ownOrg }: { ownOrg: boolean },
): Promise<SnapshotListing<SnapshotInfo>> {
  return listSnapshotsWithFallback(
    () => getAdminSnapshots(sandboxId),
    ownOrg
      ? async () => (await getPublicPage<PublicSnapshot>(`/sandboxes/${sandboxId}/snapshots`)).map(publicSnapshotToConsoleSnapshot)
      : null,
  );
}

// Roll back through the route the snapshots were listed from. The admin route
// refuses unless snapshot rollback is enabled for the sandbox's organization,
// with a 404 that says so.
export async function rollbackConsoleSandbox(
  sandboxId: string,
  snapshotName: string,
  via: 'admin' | 'public',
): Promise<SandboxOperationResponse> {
  const body = { snapshot_name: snapshotName, auto_restart: true };
  if (via === 'public') return postPublicOperation(`/sandboxes/${sandboxId}:rollback`, body);
  return adminApi.post<SandboxOperationResponse>(
    `/v1/sandboxes/${publicSandboxIdFromAny(sandboxId)}:force-rollback`,
    body,
    { 'Idempotency-Key': idempotencyKey() },
  );
}
