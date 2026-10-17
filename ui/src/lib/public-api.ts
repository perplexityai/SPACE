import { adminApi, api } from './api';
import { publicSandboxIdFromAny } from './sandbox-id';
import type {
  PublicOperationRecord,
  PublicPage,
  PublicSandbox,
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
  const [pathname, query] = path.split('?', 2);
  const params = new URLSearchParams(query);
  const items: T[] = [];
  let nextPath = path;

  while (true) {
    const page = await api.get<PublicPage<T>>(nextPath);
    items.push(...page.data);
    if (!page.next_cursor) return items;
    params.set('cursor', page.next_cursor);
    nextPath = `${pathname}?${params}`;
  }
}

export async function getPublicSandbox(id: string): Promise<Sandbox> {
  return publicSandboxToConsoleSandbox(await api.get<PublicSandbox>(`/sandboxes/${id}`));
}

export async function getConsoleSandboxes(): Promise<Sandbox[]> {
  const sandboxes = await adminApi.get<Sandbox[]>('/v1/sandboxes?limit=1000');
  return sandboxes.map(adminSandboxToConsoleSandbox);
}

export async function getConsoleSandbox(id: string): Promise<Sandbox> {
  return adminSandboxToConsoleSandbox(await adminApi.get<Sandbox>(`/v1/sandboxes/${id}`));
}

export async function postPublicOperation(
  path: string,
  body?: unknown,
): Promise<SandboxOperationResponse> {
  return api.post<SandboxOperationResponse>(path, body, {
    'Idempotency-Key': idempotencyKey(),
  });
}

export async function deletePublicOperation(path: string): Promise<SandboxOperationResponse> {
  return api.delete<SandboxOperationResponse>(path, {
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
      throw new Error(last.progress_message || `operation ${last.state}`);
    }
    await new Promise((resolve) => setTimeout(resolve, OPERATION_POLL_INTERVAL_MS));
  }

  throw new Error(last?.progress_message || 'operation still running');
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
      throw new Error(last.progress_message || `operation ${last.state}`);
    }
    await new Promise((resolve) => setTimeout(resolve, OPERATION_POLL_INTERVAL_MS));
  }

  throw new Error(last?.progress_message || 'operation still running');
}

export async function runPublicSandboxOperation(
  path: string,
  body?: unknown,
): Promise<SandboxOperationResponse> {
  const response = await postPublicOperation(path, body);
  void waitForPublicOperation(response.operation_id).catch(() => {});
  return response;
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

export function publicSandboxToConsoleSandbox(sandbox: PublicSandbox): Sandbox {
  return {
    id: sandbox.id,
    resource_type: 'sandbox',
    name: sandbox.name,
    user_id: null,
    org_id: null,
    managed_by: null,
    lifecycle: sandbox.state,
    placement: null,
    active_node_id: null,
    active_node: null,
    created_at: sandbox.created_at,
    updated_at: sandbox.updated_at,
    last_used_at: null,
    last_access_at: null,
    last_command_at: null,
    status: sandbox.state,
    template: sandbox.template,
    content_hash: sandbox.template_digest,
    release_tags: [],
    runtime: null,
    pid: null,
    namespace: null,
    uptime: null,
    user: null,
    image_hash: null,
    spaced_version: null,
    snapshots_enabled_desired: false,
    snapshots_enabled: null,
    pool_template: null,
    disk_usage_bytes: null,
    failure_reason: failedConditionMessage(sandbox),
    age: null,
    volume_encryption: false,
    auto_lifecycle: null,
    auto_lifecycle_status: null,
    network_policy_override: null,
    template_network_policy: null,
    effective_desired_network_policy: null,
    network_policy: null,
    network_policy_generation: null,
    network_policy_status: null,
  };
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

function failedConditionMessage(sandbox: PublicSandbox): string | null {
  return (
    sandbox.conditions.find((condition) => condition.status === 'false' && condition.message)
      ?.message ?? null
  );
}
