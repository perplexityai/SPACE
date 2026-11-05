'use client';

import { useRef, useState } from 'react';
import type { ReactNode } from 'react';
import {
  Alert, App, Button, Form, Input, InputNumber, Modal, Popconfirm, Select, Space, Switch, Tag, Tooltip, Typography,
} from 'antd';
import { DeleteOutlined, TagOutlined } from '@ant-design/icons';
import { adminApi } from '@/lib/api';
import { idempotencyKey, waitForPublicOperation } from '@/lib/public-api';
import {
  LATEST_TAG as DEFAULT_TEMPLATE_TAG, errorMessage, retagStepNow, shortHash, tagHolders, type RetagBaseline,
} from '@/lib/template-format';
import { isDigestTag, visibleTemplateTags } from '@/lib/template-list';
import { singleFlight } from '@/lib/single-flight';
import { TAG_PRECONDITION_UNCHECKED, moveReleaseTag, tagMoveFailureMessage } from '@/lib/admin-fallback';
import { runMutation } from '@/lib/mutation-refresh';
import NetworkPolicyDesigner, { networkPoliciesEqual } from '@/components/network-policy-designer';
import {
  AutoLifecycleEditor,
  disabledAutoLifecycle,
  formValueToPolicy,
  policyToFormValue,
} from '@/components/auto-lifecycle';
import type {
  ClusterPoolConfig, NetworkPolicyV1, SpaceNode, Template, TemplateOperationResponse, TemplateRelease, WarmPoolPolicy,
} from '@/lib/types';
import { hasObservedPoolSandboxes } from './template-cells';
import { encodedPathSegment, nodeLabel, reportableFailure, type TemplateAvailabilityRow, type TemplatesData } from './use-templates-data';

const { Text } = Typography;

function formNumberIsBlank(value: number | null | undefined) {
  return value === null || value === undefined;
}

export function canDeleteVisibleTemplateTag(tag: string) {
  return tag !== DEFAULT_TEMPLATE_TAG && !isDigestTag(tag);
}

interface WarmPoolPolicyValues {
  template_name: string;
  vcpu: number;
  memory_mib: number;
  disk_mib?: number | null;
  min_ready_slots: number;
  max_ready_slots: number;
  refill_per_tick: number;
  max_warming_slots: number;
  retain_releases: number;
  disk_io_max_operations_per_sec?: number | null;
  disk_io_max_bandwidth_mib_per_sec?: number | null;
  enabled?: boolean;
}

interface ClusterPoolValues {
  target_ready_slots: number;
  refill_per_tick: number;
  max_warming_slots: number;
  vcpu: number;
  memory_mib: number;
  disk_mib?: number | null;
  namespace?: string;
  enabled?: boolean;
}

interface Options {
  // After a template delete has finished; the template page leaves.
  onTemplateDeleted?: (name: string) => void;
}

// Every change the Templates views make, with its dialog, confirmation and
// refresh. The list, its tabs and the template page share them, so each
// action behaves the same wherever it is offered. Render `modals` once.
export function useTemplateActions(data: TemplatesData, { onTemplateDeleted }: Options = {}) {
  const { message } = App.useApp();
  const {
    canAdminTemplates,
    templateReleases,
    templateCapableNodes,
    clusterPoolById,
    warmPoolPolicyCapabilities,
    refresh,
    refreshTemplates,
    refreshTemplateReleases,
    refreshInventory,
    refreshClusterPools,
    refreshWarmPoolPolicies,
  } = data;
  const canManageTemplateReleases = canAdminTemplates;
  const [poolForm] = Form.useForm();
  const [clusterPoolForm] = Form.useForm();
  const [warmPoolPolicyForm] = Form.useForm();
  const [warmPoolPolicyModal, setWarmPoolPolicyModal] = useState<{
    open: boolean;
    policy: WarmPoolPolicy | null;
    // A new policy for this template only (the template page).
    template: string | null;
  }>({ open: false, policy: null, template: null });
  const [clusterPoolModal, setClusterPoolModal] = useState<{
    open: boolean;
    release: TemplateRelease | null;
    config: ClusterPoolConfig | null;
  }>({
    open: false,
    release: null,
    config: null,
  });
  const [poolModal, setPoolModal] = useState<{ open: boolean; name: string; mode: 'create' | 'manage' }>({
    open: false,
    name: '',
    mode: 'create',
  });
  const [pullForm] = Form.useForm();
  const [pullModal, setPullModal] = useState<{ open: boolean; template: TemplateAvailabilityRow | null }>({
    open: false,
    template: null,
  });
  const [retagForm] = Form.useForm();
  // `holders` is which release held each of the template's tags when the
  // dialog opened, except a tag whose move `change` showed (`retagStep`):
  // the precondition the move sends. A refresh while the dialog is open does
  // not change it.
  const [retagModal, setRetagModal] = useState<{ open: boolean; release: TemplateRelease | null } & RetagBaseline>({
    open: false,
    release: null,
    holders: new Map(),
    change: null,
  });
  // A second OK while the move is in flight would send the same precondition
  // again and read as a 412 "tag moved" (the first press moved it).
  const retagFlight = useRef(singleFlight()).current;
  const [retagSubmitting, setRetagSubmitting] = useState(false);
  // Bumped when the dialog opens or closes, so a submit whose re-read
  // outlived its dialog sends nothing. The tag input and OK are disabled
  // while a submit is in flight, so its answer is for the tag still shown.
  const retagSession = useRef(0);
  const retagTagInput = Form.useWatch('tag', retagForm) as string | undefined;
  const [templateOperations, setTemplateOperations] = useState<Set<string>>(() => new Set());
  const [buildingSnapshot, setBuildingSnapshot] = useState<string | null>(null);
  const [templateLifecycleForm] = Form.useForm();
  const [editingTemplateLifecycle, setEditingTemplateLifecycle] = useState<Template | null>(null);
  const [editingTemplateNetwork, setEditingTemplateNetwork] = useState<Template | null>(null);

  const startTemplateOperation = (operation: string) => {
    setTemplateOperations((current) => new Set(current).add(operation));
  };

  const finishTemplateOperation = (operation: string) => {
    setTemplateOperations((current) => {
      const next = new Set(current);
      next.delete(operation);
      return next;
    });
  };

  const templateOperationRunning = (operation: string) => templateOperations.has(operation);

  // A change's result is told before its refresh starts; a refresh that
  // fails after it is not the change failing (`runMutation`).
  const change = <T,>({ mutate, succeeded, refresh, failed = (err) => message.error(errorMessage(err)) }: {
    mutate: () => Promise<T>;
    succeeded: (value: T) => void;
    refresh: (value: T) => Promise<unknown>;
    failed?: (err: unknown) => void;
  }) => runMutation({
    mutate,
    succeeded,
    failed,
    refresh,
    refreshFailed: (err) => {
      if (reportableFailure(err)) {
        message.warning(`The change was made, but the page could not read it back (${errorMessage(err)}). Refresh to see it.`);
      }
    },
  });

  const openRetagModal = (release: TemplateRelease) => {
    if (!canManageTemplateReleases) return;
    retagSession.current += 1;
    const visibleTags = visibleTemplateTags(release.tags, release.content_hash);
    retagForm.setFieldsValue({ tag: visibleTags[0] ?? DEFAULT_TEMPLATE_TAG });
    setRetagModal({ open: true, release, holders: tagHolders(templateReleases, release.template_name), change: null });
  };

  const closeRetagModal = () => {
    retagSession.current += 1;
    setRetagModal({ open: false, release: null, holders: new Map(), change: null });
    retagForm.resetFields();
  };

  const releaseLabel = (releaseId: string | null) => {
    if (releaseId === null) return 'no release (the tag did not exist)';
    const holder = templateReleases.find((candidate) => candidate.id === releaseId);
    return holder ? `${holder.template_name}@${shortHash(holder.content_hash)}` : `release ${releaseId}`;
  };

  const handleRetagRelease = (values: { tag: string }) => retagFlight.run(async () => {
    const release = retagModal.release;
    if (!release || !canManageTemplateReleases) return;

    const tag = values.tag.trim();
    const session = retagSession.current;
    setRetagSubmitting(true);
    try {
      // The releases are read again first: if the tag moved since the dialog
      // showed it, show where it is now and ask again before moving it.
      let step: Awaited<ReturnType<typeof retagStepNow>>;
      try {
        step = await retagStepNow(
          retagModal,
          () => refreshTemplateReleases({ showLoading: false }),
          release.template_name,
          tag,
          // Closed, or opened for another release, while the releases were
          // read; or showing another tag, whose warning would not be seen.
          (shown) => session === retagSession.current && String(retagForm.getFieldValue('tag') ?? '').trim() === shown,
        );
      } catch {
        message.error("Could not read the template's tags again before moving the tag; nothing was changed. Try again.");
        return;
      }
      if (step.kind === 'stale') return;
      if (step.kind === 'warn') {
        setRetagModal((current) => ({ ...current, ...step.baseline }));
        return;
      }
      const expected = step.expected;
      await change({
        // Moved only while the tag still points where the operator saw it.
        mutate: () => moveReleaseTag((path, body) => adminApi.put(path, body), release.id, tag, expected),
        succeeded: ({ checked }) => {
          closeRetagModal();
          if (checked) message.success('Template release tag updated');
          else message.warning(`Template release tag updated. ${TAG_PRECONDITION_UNCHECKED}`);
        },
        failed: (err) => message.error(tagMoveFailureMessage(err)),
        refresh: () => refreshTemplateReleases({ showLoading: false }),
      });
    } finally {
      setRetagSubmitting(false);
    }
  });

  const handleDeleteReleaseTag = async (release: TemplateRelease, tag: string) => {
    if (!canManageTemplateReleases || !canDeleteVisibleTemplateTag(tag)) return;
    const operation = `delete-tag:${release.id}:${tag}`;
    startTemplateOperation(operation);
    try {
      await change({
        mutate: () => adminApi.delete(`/v1/template-releases/${release.id}/tags/${encodedPathSegment(tag)}`),
        succeeded: () => message.success('Template release tag deleted'),
        refresh: () => refreshTemplateReleases({ showLoading: false }),
      });
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const handleDeleteRelease = async (release: TemplateRelease) => {
    if (!canManageTemplateReleases) return;
    const operation = `delete-release:${release.id}`;
    startTemplateOperation(operation);
    try {
      await change({
        mutate: () => adminApi.delete(`/v1/template-releases/${release.id}`),
        succeeded: () => message.success('Template release deleted'),
        refresh: () => Promise.all([
          refreshTemplateReleases({ showLoading: false }),
          refreshTemplates({ showLoading: false }),
        ]),
      });
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const openClusterPoolModal = (release: TemplateRelease, configId?: string) => {
    const existing = configId ? clusterPoolById.get(configId) : undefined;
    clusterPoolForm.setFieldsValue({
      target_ready_slots: existing?.target_ready_slots ?? 3,
      refill_per_tick: existing?.refill_per_tick ?? 1,
      max_warming_slots: existing?.max_warming_slots ?? 2,
      vcpu: existing?.vcpu ?? 2,
      memory_mib: existing?.memory_mib ?? 1024,
      disk_mib: existing?.disk_mib,
      namespace: existing?.namespace,
      enabled: existing?.enabled ?? true,
    });
    setClusterPoolModal({ open: true, release, config: existing ?? null });
  };

  const closeClusterPoolModal = () => {
    setClusterPoolModal({ open: false, release: null, config: null });
    clusterPoolForm.resetFields();
  };

  const handleClusterPoolSubmit = async (values: ClusterPoolValues) => {
    const release = clusterPoolModal.release;
    if (!release) return;

    await change({
      mutate: () => adminApi.put('/v1/warm-pools', {
        template_name: release.template_name,
        content_hash: release.content_hash,
        vcpu: values.vcpu,
        memory_mib: values.memory_mib,
        ...(warmPoolPolicyCapabilities.disk_capacity && !formNumberIsBlank(values.disk_mib)
          ? { disk_mib: values.disk_mib }
          : {}),
        ...(warmPoolPolicyCapabilities.disk_capacity &&
        clusterPoolModal.config?.disk_mib !== undefined &&
        formNumberIsBlank(values.disk_mib)
          ? { reset_disk_to_template_default: true }
          : {}),
        target_ready_slots: values.target_ready_slots,
        refill_per_tick: values.refill_per_tick,
        max_warming_slots: values.max_warming_slots,
        namespace: values.namespace?.trim() || undefined,
        enabled: values.enabled ?? true,
      }),
      succeeded: () => {
        closeClusterPoolModal();
        message.success('Cluster pool config saved');
      },
      refresh: () => Promise.all([
        refreshClusterPools(),
        refreshTemplateReleases({ showLoading: false }),
      ]),
    });
  };

  const handleDisableClusterPool = async () => {
    const existing = clusterPoolModal.config;
    if (!existing) return;

    await change({
      mutate: () => existing.managed_by === 'autoscaler'
        ? adminApi.put('/v1/warm-pools', {
          template_name: existing.template_name,
          content_hash: existing.content_hash,
          vcpu: existing.vcpu,
          memory_mib: existing.memory_mib,
          ...(existing.disk_mib !== undefined ? { disk_mib: existing.disk_mib } : {}),
          target_ready_slots: 0,
          refill_per_tick: existing.refill_per_tick,
          max_warming_slots: existing.max_warming_slots,
          namespace: existing.namespace,
          enabled: false,
        })
        : adminApi.delete(`/v1/warm-pools/${existing.id}`),
      succeeded: () => {
        closeClusterPoolModal();
        message.success('Cluster pool disabled');
      },
      refresh: () => Promise.all([
        refreshClusterPools(),
        refreshTemplateReleases({ showLoading: false }),
      ]),
    });
  };

  const openWarmPoolPolicyModal = (policy: WarmPoolPolicy | null = null, template: string | null = null) => {
    warmPoolPolicyForm.setFieldsValue({
      template_name: policy?.template_name ?? template ?? undefined,
      vcpu: policy?.vcpu ?? 2,
      memory_mib: policy?.memory_mib ?? 8192,
      disk_mib: policy?.disk_mib,
      min_ready_slots: policy?.min_ready_slots ?? 5,
      max_ready_slots: policy?.max_ready_slots ?? 30,
      refill_per_tick: policy?.refill_per_tick ?? 5,
      max_warming_slots: policy?.max_warming_slots ?? 10,
      retain_releases: policy?.retain_releases ?? 2,
      disk_io_max_operations_per_sec: policy?.disk_io_max_operations_per_sec,
      disk_io_max_bandwidth_mib_per_sec: policy?.disk_io_max_bandwidth_mib_per_sec,
      enabled: policy?.enabled ?? true,
    });
    setWarmPoolPolicyModal({ open: true, policy, template });
  };

  const closeWarmPoolPolicyModal = () => {
    setWarmPoolPolicyModal({ open: false, policy: null, template: null });
    warmPoolPolicyForm.resetFields();
  };

  const saveWarmPoolPolicy = async (
    values: WarmPoolPolicyValues,
    resetDiskIoToDefault = false,
    resetDiskToTemplateDefault = false,
  ) => {
    const hasDiskIoLimits = !formNumberIsBlank(values.disk_io_max_operations_per_sec)
      && !formNumberIsBlank(values.disk_io_max_bandwidth_mib_per_sec);
    await change({
      mutate: () => adminApi.put('/v1/warm-pool-policies', {
        template_name: values.template_name,
        vcpu: values.vcpu,
        memory_mib: values.memory_mib,
        ...(warmPoolPolicyCapabilities.disk_capacity && !formNumberIsBlank(values.disk_mib)
          ? { disk_mib: values.disk_mib }
          : {}),
        ...(resetDiskToTemplateDefault ? { reset_disk_to_template_default: true } : {}),
        min_ready_slots: values.min_ready_slots,
        max_ready_slots: values.max_ready_slots,
        refill_per_tick: values.refill_per_tick,
        max_warming_slots: values.max_warming_slots,
        retain_releases: values.retain_releases,
        ...(hasDiskIoLimits ? {
          disk_io_max_operations_per_sec: values.disk_io_max_operations_per_sec,
          disk_io_max_bandwidth_mib_per_sec: values.disk_io_max_bandwidth_mib_per_sec,
        } : {}),
        ...(resetDiskIoToDefault ? { reset_disk_io_to_default: true } : {}),
        enabled: values.enabled ?? true,
      }),
      succeeded: () => {
        closeWarmPoolPolicyModal();
        message.success('Warm pool policy saved');
      },
      refresh: () => Promise.all([
        refreshWarmPoolPolicies(),
        refreshTemplateReleases({ showLoading: false }),
      ]),
    });
  };

  const handleWarmPoolPolicySubmit = async (values: WarmPoolPolicyValues) => {
    try {
      const policy = warmPoolPolicyModal.policy;
      const hadExplicitDiskIo = policy?.disk_io_max_operations_per_sec !== undefined
        && policy.disk_io_max_bandwidth_mib_per_sec !== undefined;
      const clearedDiskIo = formNumberIsBlank(values.disk_io_max_operations_per_sec)
        && formNumberIsBlank(values.disk_io_max_bandwidth_mib_per_sec);
      const diskIoControlsAvailable = warmPoolPolicyCapabilities.disk_io !== undefined;
      const hadExplicitDisk = policy?.disk_mib !== undefined;
      const clearedDisk = formNumberIsBlank(values.disk_mib);
      await saveWarmPoolPolicy(
        values,
        Boolean(diskIoControlsAvailable && hadExplicitDiskIo && clearedDiskIo),
        Boolean(warmPoolPolicyCapabilities.disk_capacity && hadExplicitDisk && clearedDisk),
      );
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleDisableWarmPoolPolicy = async () => {
    const policy = warmPoolPolicyModal.policy;
    if (!policy) return;
    try {
      await saveWarmPoolPolicy({
        template_name: policy.template_name,
        vcpu: policy.vcpu,
        memory_mib: policy.memory_mib,
        disk_mib: policy.disk_mib,
        min_ready_slots: policy.min_ready_slots,
        max_ready_slots: policy.max_ready_slots,
        refill_per_tick: policy.refill_per_tick,
        max_warming_slots: policy.max_warming_slots,
        retain_releases: policy.retain_releases,
        enabled: false,
      });
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const openTemplateLifecycleModal = (template: Template) => {
    templateLifecycleForm.setFieldsValue({
      auto_lifecycle: policyToFormValue(template.default_auto_lifecycle),
    });
    setEditingTemplateLifecycle(template);
  };

  const closeTemplateLifecycleModal = () => {
    setEditingTemplateLifecycle(null);
    templateLifecycleForm.resetFields();
  };

  const handleTemplateLifecycleUpdate = async (values: { auto_lifecycle?: any }) => {
    if (!editingTemplateLifecycle) return;
    const templateName = editingTemplateLifecycle.name;
    await change({
      mutate: () => adminApi.patch(`/v1/templates/${encodedPathSegment(templateName)}`, {
        default_auto_lifecycle: formValueToPolicy(values.auto_lifecycle),
      }),
      succeeded: () => {
        closeTemplateLifecycleModal();
        message.success('Template lifecycle policy updated');
      },
      refresh: () => refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true }),
    });
  };

  const handleTemplateLifecycleClear = async () => {
    if (!editingTemplateLifecycle) return;
    const templateName = editingTemplateLifecycle.name;
    await change({
      mutate: () => adminApi.patch(`/v1/templates/${encodedPathSegment(templateName)}`, {
        default_auto_lifecycle: disabledAutoLifecycle(),
      }),
      succeeded: () => {
        closeTemplateLifecycleModal();
        message.success('Template lifecycle policy cleared');
      },
      refresh: () => refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true }),
    });
  };

  const handleTemplateNetworkSave = async (policy: NetworkPolicyV1) => {
    if (!editingTemplateNetwork) return;
    if (networkPoliciesEqual(editingTemplateNetwork.network_policy, policy)) {
      setEditingTemplateNetwork(null);
      return;
    }

    const templateName = editingTemplateNetwork.name;
    await change({
      mutate: () => adminApi.patch(`/v1/templates/${encodedPathSegment(templateName)}`, {
        network_policy: policy,
      }),
      succeeded: () => {
        setEditingTemplateNetwork(null);
        message.success('Template network policy updated');
      },
      refresh: () => refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true }),
    });
  };

  const handleDelete = async (name: string) => {
    const operation = `delete-template:${name}`;
    startTemplateOperation(operation);
    try {
      await change({
        mutate: async () => {
          const response = await adminApi.delete<TemplateOperationResponse>(
            `/v1/templates/${encodedPathSegment(name)}`,
            { 'Idempotency-Key': idempotencyKey() },
          );
          await waitForPublicOperation(response.operation_id, 120_000);
        },
        succeeded: () => {
          message.success(`Template ${name} deleted`);
          onTemplateDeleted?.(name);
        },
        // The template page leaves instead; the list it opens reads afresh.
        refresh: async () => {
          if (!onTemplateDeleted) await refresh({ showLoading: false });
        },
      });
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const openPoolModal = (template: Template) => {
    poolForm.setFieldsValue({
      target_count: template.pool_target ?? template.pool_current ?? 3,
      boot_mode: template.has_base_snapshot ? 'snapshot' : 'cold',
    });
    setPoolModal({
      open: true,
      name: template.name,
      mode: hasObservedPoolSandboxes(template) ? 'manage' : 'create',
    });
  };

  const closePoolModal = () => {
    setPoolModal({ open: false, name: '', mode: 'create' });
    poolForm.resetFields();
  };

  const handlePoolSubmit = async (values: { target_count: number; boot_mode: 'cold' | 'snapshot' }) => {
    if (!poolModal.name) return;
    const name = poolModal.name;
    await change({
      mutate: () => adminApi.post(`/v1/templates/${encodedPathSegment(name)}/pool`, values),
      succeeded: () => {
        closePoolModal();
        message.success('Template pool policy saved');
      },
      refresh: () => refreshTemplates({ showLoading: false }),
    });
  };

  const handleDeletePool = async (name: string) => {
    await change({
      mutate: () => adminApi.delete(`/v1/templates/${encodedPathSegment(name)}/pool`),
      succeeded: () => {
        closePoolModal();
        message.success('Template pool policy deleted');
      },
      refresh: () => refreshTemplates({ showLoading: false }),
    });
  };

  const handleToggleBaseSnapshot = async (template: Template, enabled: boolean) => {
    setBuildingSnapshot(template.name);
    try {
      await change({
        mutate: () => adminApi.patch(`/v1/templates/${encodedPathSegment(template.name)}/base-snapshot-policy`, {
          ensure_base_snapshot: enabled,
        }),
        succeeded: () => message.success(enabled ? 'Base snapshot policy enabled' : 'Base snapshot policy disabled'),
        refresh: () => refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true }),
      });
    } finally {
      setBuildingSnapshot(null);
    }
  };

  const openPullModal = (template: TemplateAvailabilityRow) => {
    const existingNodeIds = new Set(template.nodes.map((availability) => availability.node.id));
    const targetNode = templateCapableNodes.find((node) => !existingNodeIds.has(node.id));
    pullForm.setFieldsValue({
      source_node_id: template.nodes[0]?.node.id,
      target_node_id: targetNode?.id,
    });
    setPullModal({ open: true, template });
  };

  const handlePullTemplate = async (values: { source_node_id: string; target_node_id: string }) => {
    const template = pullModal.template;
    if (!template) return;

    const operation = `pull:${template.key}`;
    startTemplateOperation(operation);
    try {
      setPullModal({ open: false, template: null });
      pullForm.resetFields();
      const source = template.nodes.find((availability) => availability.node.id === values.source_node_id);
      const snapshot = source?.snapshots[0] ?? template.nodes.flatMap((availability) => availability.snapshots)[0];
      await change({
        mutate: () => adminApi.post(`/v1/nodes/${values.target_node_id}/template-pulls`, {
          template_id: template.name,
          content_hash: template.content_hash || undefined,
          source_node_id: values.source_node_id,
          include_snapshot: Boolean(snapshot),
          snapshot_vcpu: snapshot?.vcpu,
          snapshot_memory_mib: snapshot?.memory_mib,
          skip_if_exists: true,
          block_on_snapshot_policy_achieved: true,
        }),
        succeeded: () => message.success('Template pull queued'),
        refresh: () => Promise.all([
          refreshInventory({ showLoading: false }),
          refreshTemplateReleases({ showLoading: false }),
        ]),
      });
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const handleDeleteTemplateFromNode = async (template: TemplateAvailabilityRow, node: SpaceNode) => {
    const operation = `delete:${template.key}:${node.id}`;
    startTemplateOperation(operation);
    try {
      const query = template.content_hash
        ? `?content_hash=${encodeURIComponent(template.content_hash)}`
        : '';
      await change({
        mutate: () => adminApi.delete(`/v1/nodes/${node.id}/templates/${encodedPathSegment(template.name)}${query}`),
        succeeded: () => message.success('Template deleted from node'),
        refresh: () => refreshInventory({ showLoading: false }),
      });
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const renderReleaseTagControls = (release: TemplateRelease, allowActions = true): ReactNode => {
    const actionsAllowed = allowActions && canManageTemplateReleases;
    const visibleTags = visibleTemplateTags(release.tags, release.content_hash);
    return (
      <Space wrap size={4}>
        {visibleTags.length > 0 ? visibleTags.map((tag) => {
          const deletable = actionsAllowed && canDeleteVisibleTemplateTag(tag);
          const deleteTagOperation = `delete-tag:${release.id}:${tag}`;
          const deleteTagRunning = templateOperationRunning(deleteTagOperation);
          return (
            <Tag key={tag} color={tag === DEFAULT_TEMPLATE_TAG ? 'green' : 'blue'} style={{ marginRight: 0 }}>
              <Space size={2}>
                <span>{tag}</span>
                {deletable ? (
                  <Popconfirm
                    title={`Delete tag ${tag}?`}
                    okText="Delete"
                    okType="danger"
                    onConfirm={() => handleDeleteReleaseTag(release, tag)}
                  >
                    <Button
                      type="text"
                      size="small"
                      danger
                      icon={<DeleteOutlined />}
                      loading={deleteTagRunning}
                      disabled={deleteTagRunning}
                      aria-label={`Delete tag ${tag}`}
                      style={{ width: 16, height: 16, minWidth: 16, padding: 0 }}
                    />
                  </Popconfirm>
                ) : null}
              </Space>
            </Tag>
          );
        }) : <Text type="secondary">untagged</Text>}
        {actionsAllowed ? (
          <Tooltip title="Assign or move tag">
            <Button
              type="text"
              size="small"
              icon={<TagOutlined />}
              aria-label="Assign or move release tag"
              onClick={() => openRetagModal(release)}
            />
          </Tooltip>
        ) : null}
      </Space>
    );
  };

  const pullTemplate = pullModal.template;
  const pullSourceNodes = pullTemplate?.nodes ?? [];
  const pullTargetNodes = pullTemplate
    ? templateCapableNodes.filter((node) => !pullTemplate.nodes.some((source) => source.node.id === node.id))
    : [];
  const policyTemplateFixed = Boolean(warmPoolPolicyModal.policy || warmPoolPolicyModal.template);

  const modals = (
    <>
      <Modal
        title={warmPoolPolicyModal.policy
          ? `Edit rolling policy for "${warmPoolPolicyModal.policy.template_name}"`
          : 'Create rolling warm pool policy'}
        open={warmPoolPolicyModal.open}
        onCancel={closeWarmPoolPolicyModal}
        footer={[
          warmPoolPolicyModal.policy?.enabled ? (
            <Popconfirm
              key="disable"
              title="Disable this policy and drain its autoscaled release pools?"
              okText="Disable"
              okType="danger"
              onConfirm={handleDisableWarmPoolPolicy}
            >
              <Button danger>Disable Policy</Button>
            </Popconfirm>
          ) : null,
          <Button key="cancel" onClick={closeWarmPoolPolicyModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => warmPoolPolicyForm.submit()}>
            Save
          </Button>,
        ]}
        destroyOnHidden
      >
        <Form form={warmPoolPolicyForm} layout="vertical" onFinish={handleWarmPoolPolicySubmit}>
          <Form.Item name="template_name" label="Template" rules={[{ required: true }]}>
            <Select
              showSearch
              disabled={policyTemplateFixed}
              options={data.templates.map((template) => ({ value: template.name, label: template.name }))}
              placeholder="Select a template"
            />
          </Form.Item>
          <Space.Compact block>
            <Form.Item name="vcpu" label="vCPU" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={1} max={64} disabled={Boolean(warmPoolPolicyModal.policy)} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="memory_mib" label="Memory (MiB)" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={128} max={262144} disabled={Boolean(warmPoolPolicyModal.policy)} style={{ width: '100%' }} />
            </Form.Item>
          </Space.Compact>
          {warmPoolPolicyCapabilities.disk_capacity ? (
            <Form.Item
              name="disk_mib"
              label="Disk capacity (MiB)"
              extra="Leave blank for the template-baked size. Changing this drains and refills existing warm slots."
            >
              <InputNumber min={1} max={2147483647} placeholder="Template default" style={{ width: '100%' }} />
            </Form.Item>
          ) : null}
          <Space.Compact block>
            <Form.Item name="min_ready_slots" label="Minimum Ready / Release" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="max_ready_slots" label="Maximum Ready / Release" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
          </Space.Compact>
          <Space.Compact block>
            <Form.Item name="refill_per_tick" label="Refill Per Tick" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="max_warming_slots" label="Warming Slots" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
          </Space.Compact>
          <Form.Item name="retain_releases" label="Retained Releases" rules={[{ required: true }]}>
            <InputNumber min={1} max={50} style={{ width: '100%' }} />
          </Form.Item>
          {warmPoolPolicyCapabilities.disk_io ? (
            <>
              <Space.Compact block>
                <Form.Item
                  name="disk_io_max_operations_per_sec"
                  label="Disk IOPS"
                  dependencies={['disk_io_max_bandwidth_mib_per_sec']}
                  rules={[{
                    validator: (_, value) => {
                      const bandwidth = warmPoolPolicyForm.getFieldValue('disk_io_max_bandwidth_mib_per_sec');
                      return formNumberIsBlank(value) === formNumberIsBlank(bandwidth)
                        ? Promise.resolve()
                        : Promise.reject(new Error('Set both disk I/O limits or leave both blank'));
                    },
                  }]}
                  style={{ width: '50%' }}
                >
                  <InputNumber
                    min={1}
                    max={warmPoolPolicyCapabilities.disk_io.max_operations_per_sec}
                    placeholder={String(warmPoolPolicyCapabilities.disk_io.default_operations_per_sec)}
                    style={{ width: '100%' }}
                  />
                </Form.Item>
                <Form.Item
                  name="disk_io_max_bandwidth_mib_per_sec"
                  label="Disk bandwidth (MiB/s)"
                  dependencies={['disk_io_max_operations_per_sec']}
                  rules={[{
                    validator: (_, value) => {
                      const operations = warmPoolPolicyForm.getFieldValue('disk_io_max_operations_per_sec');
                      return formNumberIsBlank(value) === formNumberIsBlank(operations)
                        ? Promise.resolve()
                        : Promise.reject(new Error('Set both disk I/O limits or leave both blank'));
                    },
                  }]}
                  style={{ width: '50%' }}
                >
                  <InputNumber
                    min={1}
                    max={warmPoolPolicyCapabilities.disk_io.max_bandwidth_mib_per_sec}
                    placeholder={String(warmPoolPolicyCapabilities.disk_io.default_bandwidth_mib_per_sec)}
                    style={{ width: '100%' }}
                  />
                </Form.Item>
              </Space.Compact>
              <Text type="secondary">
                Leave both blank for the platform default. Cluster maximum: {' '}
                {warmPoolPolicyCapabilities.disk_io.max_operations_per_sec.toLocaleString()} IOPS / {' '}
                {warmPoolPolicyCapabilities.disk_io.max_bandwidth_mib_per_sec} MiB/s.
              </Text>
            </>
          ) : null}
          <Form.Item name="enabled" label="Enabled" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Space orientation="vertical" size={4}>
            <Text type="secondary">
              Each retained release receives the configured minimum ready slots before demand.
            </Text>
            {warmPoolPolicyModal.policy ? (
              <Text type="secondary">
                Template and VM shape identify this policy. Disable it and create a replacement policy to change shape.
              </Text>
            ) : null}
          </Space>
        </Form>
      </Modal>

      <Modal
        title={retagModal.release ? `Assign tag to ${retagModal.release.template_name}@${shortHash(retagModal.release.content_hash)}` : 'Assign tag'}
        open={retagModal.open}
        onCancel={closeRetagModal}
        onOk={() => retagForm.submit()}
        confirmLoading={retagSubmitting}
        okButtonProps={{ disabled: retagSubmitting }}
        destroyOnHidden
      >
        {retagModal.change && retagTagInput?.trim() === retagModal.change.tag ? (
          <Alert
            type="warning"
            showIcon
            style={{ marginBottom: 12 }}
            title={`While this dialog was open, ${retagModal.change.tag} moved from ${releaseLabel(retagModal.change.from)} to ${releaseLabel(retagModal.change.to)}. Nothing was changed; press OK again to move it from where it is now.`}
          />
        ) : null}
        <Form form={retagForm} layout="vertical" onFinish={handleRetagRelease}>
          <Form.Item
            name="tag"
            label="Tag"
            rules={[{ required: true, message: 'Tag is required' }]}
          >
            <Input placeholder={DEFAULT_TEMPLATE_TAG} disabled={retagSubmitting} />
          </Form.Item>
          <Space orientation="vertical" size={4}>
            <Text type="secondary">
              If this tag already exists for the template, it will move to this release.
            </Text>
            <Text type="secondary">
              The latest tag can be moved but not deleted. Existing warm pools are pinned to release IDs.
            </Text>
          </Space>
        </Form>
      </Modal>

      <Modal
        title={poolModal.mode === 'manage' ? `Manage Pool for "${poolModal.name}"` : `Create Pool for "${poolModal.name}"`}
        open={poolModal.open}
        onCancel={closePoolModal}
        onOk={() => poolForm.submit()}
        footer={[
          poolModal.mode === 'manage' ? (
            <Popconfirm
              key="delete"
              title="Delete this pool policy and let workers drain existing slots?"
              okText="Delete"
              okType="danger"
              onConfirm={() => handleDeletePool(poolModal.name)}
            >
              <Button danger>Delete Pool</Button>
            </Popconfirm>
          ) : null,
          <Button key="cancel" onClick={closePoolModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => poolForm.submit()}>
            Save
          </Button>,
        ]}
        destroyOnHidden
      >
        <Form form={poolForm} layout="vertical" onFinish={handlePoolSubmit}>
          <Form.Item name="target_count" label="Target Warm Sandboxes" rules={[{ required: true }]}>
            <InputNumber min={1} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="boot_mode" label="Boot Mode" rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'cold', label: 'Cold boot' },
                { value: 'snapshot', label: 'Base snapshot' },
              ]}
            />
          </Form.Item>
          <Text type="secondary">
            Direct worker pools are reconciled by node controllers from the template policy.
          </Text>
        </Form>
      </Modal>

      <Modal
        title={clusterPoolModal.release ? `Cluster Pool for "${clusterPoolModal.release.template_name}"` : 'Cluster Pool'}
        open={clusterPoolModal.open}
        onCancel={closeClusterPoolModal}
        onOk={() => clusterPoolForm.submit()}
        footer={[
          clusterPoolModal.config ? (
            <Popconfirm
              key="disable"
              title={clusterPoolModal.config.managed_by === 'autoscaler'
                ? 'Detach this pool from its rolling policy and disable it?'
                : 'Disable this cluster pool and remove unclaimed warm slots?'}
              okText="Disable"
              okType="danger"
              onConfirm={handleDisableClusterPool}
            >
              <Button danger>
                {clusterPoolModal.config.managed_by === 'autoscaler' ? 'Detach and Disable' : 'Disable Pool'}
              </Button>
            </Popconfirm>
          ) : null,
          <Button key="cancel" onClick={closeClusterPoolModal}>Cancel</Button>,
          clusterPoolModal.config?.managed_by === 'autoscaler' ? (
            <Popconfirm
              key="save"
              title="Detach this pool from its rolling policy and save it as manually managed?"
              okText="Detach and Save"
              onConfirm={() => clusterPoolForm.submit()}
            >
              <Button type="primary">Detach and Save</Button>
            </Popconfirm>
          ) : (
            <Button key="save" type="primary" onClick={() => clusterPoolForm.submit()}>
              Save
            </Button>
          ),
        ]}
        destroyOnHidden
      >
        <Form form={clusterPoolForm} layout="vertical" onFinish={handleClusterPoolSubmit}>
          <Form.Item name="enabled" label="Enabled" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Form.Item name="target_ready_slots" label="Target Ready Slots" rules={[{ required: true }]}>
            <InputNumber min={0} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="refill_per_tick" label="Refill Per Tick" rules={[{ required: true }]}>
            <InputNumber min={0} max={100} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="max_warming_slots" label="Max Warming Slots" rules={[{ required: true }]}>
            <InputNumber min={0} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="vcpu" label="vCPU" rules={[{ required: true }]}>
            <InputNumber min={1} max={64} disabled={Boolean(clusterPoolModal.config)} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="memory_mib" label="Memory (MiB)" rules={[{ required: true }]}>
            <InputNumber min={128} max={262144} disabled={Boolean(clusterPoolModal.config)} style={{ width: '100%' }} />
          </Form.Item>
          {warmPoolPolicyCapabilities.disk_capacity ? (
            <Form.Item
              name="disk_mib"
              label="Disk capacity (MiB)"
              extra="Leave blank for the template-baked size. Changing this drains and refills existing warm slots."
            >
              <InputNumber min={1} max={2147483647} placeholder="Template default" style={{ width: '100%' }} />
            </Form.Item>
          ) : null}
          <Form.Item name="namespace" label="Namespace">
            <Input placeholder="Cluster default" />
          </Form.Item>
          <Text type="secondary">
            Applies to {clusterPoolModal.release ? shortHash(clusterPoolModal.release.content_hash) : 'the selected release'}.
            The controller reconciles target changes on its next tick.
          </Text>
          {clusterPoolModal.config?.managed_by === 'autoscaler' ? (
            <Text type="warning">
              Detaching stops policy updates and preserves this release-specific pool under manual control.
            </Text>
          ) : clusterPoolModal.config ? (
            <Text type="secondary">Release and VM shape identify this manually managed pool.</Text>
          ) : null}
        </Form>
      </Modal>

      <Modal
        title={pullTemplate ? `Pull ${pullTemplate.name}@${shortHash(pullTemplate.content_hash)} to node` : 'Pull template to node'}
        open={pullModal.open}
        onCancel={() => {
          setPullModal({ open: false, template: null });
          pullForm.resetFields();
        }}
        onOk={() => pullForm.submit()}
        confirmLoading={pullTemplate ? templateOperationRunning(`pull:${pullTemplate.key}`) : false}
        destroyOnHidden
      >
        <Form form={pullForm} layout="vertical" onFinish={handlePullTemplate}>
          <Form.Item name="source_node_id" label="Source Node" rules={[{ required: true }]}>
            <Select
              options={pullSourceNodes.map((availability) => ({
                value: availability.node.id,
                label: nodeLabel(availability.node),
              }))}
            />
          </Form.Item>
          <Form.Item name="target_node_id" label="Target Node" rules={[{ required: true }]}>
            <Select
              placeholder="Select node"
              options={pullTargetNodes.map((node) => ({
                value: node.id,
                label: nodeLabel(node),
              }))}
            />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={editingTemplateLifecycle ? `Default Auto Lifecycle for "${editingTemplateLifecycle.name}"` : 'Default Auto Lifecycle'}
        open={Boolean(editingTemplateLifecycle)}
        onCancel={closeTemplateLifecycleModal}
        onOk={() => templateLifecycleForm.submit()}
        footer={[
          <Button key="clear" onClick={handleTemplateLifecycleClear}>
            Clear
          </Button>,
          <Button key="cancel" onClick={closeTemplateLifecycleModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => templateLifecycleForm.submit()}>
            Save
          </Button>,
        ]}
        width={760}
        destroyOnHidden
      >
        <Form form={templateLifecycleForm} layout="vertical" onFinish={handleTemplateLifecycleUpdate}>
          <AutoLifecycleEditor prefix={['auto_lifecycle']} />
        </Form>
      </Modal>

      <NetworkPolicyDesigner
        open={Boolean(editingTemplateNetwork)}
        targetKind="template"
        targetName={editingTemplateNetwork?.name ?? ''}
        policy={editingTemplateNetwork?.network_policy ?? null}
        onClose={() => setEditingTemplateNetwork(null)}
        onSave={handleTemplateNetworkSave}
      />
    </>
  );

  return {
    modals,
    canManageTemplateReleases,
    templateOperationRunning,
    buildingSnapshot,
    renderReleaseTagControls,
    openRetagModal,
    handleDeleteRelease,
    openClusterPoolModal,
    openWarmPoolPolicyModal,
    openPoolModal,
    openPullModal,
    handleDeleteTemplateFromNode,
    handleDelete,
    handleToggleBaseSnapshot,
    openTemplateLifecycleModal,
    editTemplateNetwork: setEditingTemplateNetwork,
  };
}

export type TemplateActions = ReturnType<typeof useTemplateActions>;
