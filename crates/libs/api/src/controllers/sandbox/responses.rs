use pplx_space_common_core::types::SandboxId;

use super::*;
use pplx_space_common_core::sql;

use crate::controllers::placement_fence::{Writer, check_placement_fence, fence_refusal_message};
use pplx_space_net_common::NetworkPolicy;

fn proto_spaced_version(value: Option<&str>) -> Option<SemVer> {
    let value = value?;
    match SemVer::parse(value) {
        Ok(version) => Some(version),
        Err(error) => {
            tracing::warn!(error = %error, "live sandbox info contained invalid spaced version");
            None
        }
    }
}

const SANDBOX_COLUMNS: &str = "
    s.id,
    s.name,
    s.user_id,
    s.org_id,
    owner_user.owner_source AS owner_source,
    owner_user.upstream_issuer AS upstream_issuer,
    owner_org.upstream_org_id AS upstream_org_id,
    owner_user.upstream_subject_id AS upstream_subject_id,
    s.managed_by,
    s.lifecycle,
    s.placement,
    COALESCE(direct_route.current_node_id, s.active_node_id) AS active_node_id,
    COALESCE(active_direct.name, active_k8s.k8s_node_name) AS active_node_name,
    active_node.role AS active_node_role,
    host(active_node.address) AS active_node_address,
    COALESCE(direct_route.substrate_kind, active_node.managed_by) AS active_node_managed_by,
    COALESCE(active_execution.generation, direct_route.execution_generation)
        AS active_execution_generation,
    COALESCE(active_execution.pod_uid, direct_route.pod_uid) AS active_pod_uid,
    COALESCE(active_execution.route_activation_id, direct_route.activation_attempt_id)
        AS active_activation_attempt_id,
    COALESCE(ecw_credential.epoch, 1) AS ecw_credential_epoch,
    s.target_node_id,
    s.placement_changed_at,
    s.template_id,
    s.content_hash,
    s.vcpu,
    s.memory_mib,
    s.disk_mib,
    s.disk_io_max_operations_per_sec,
    s.disk_io_max_bandwidth_mib_per_sec,
    s.env,
    s.metadata,
    s.auto_lifecycle_json AS auto_lifecycle,
    s.auto_lifecycle_state_json AS auto_lifecycle_state,
    s.network_policy_override_json AS network_policy,
    s.network_policy_generation,
    COALESCE(s.volume_encryption, FALSE) AS volume_encryption,
    t.network_policy_json AS template_network_policy,
    t.network_policy_generation AS template_network_policy_generation,
    s.next_auto_event_at,
    s.snapshots_enabled_desired,
    s.command_ready_pending,
    s.last_used_at,
    s.last_access_at,
    s.last_command_at,
    s.created_at,
    s.updated_at,
    COALESCE(release_tags.tags, ARRAY[]::TEXT[]) AS release_tags
";

const SANDBOX_JOIN_TAIL: &str = "
    JOIN users owner_user ON owner_user.id = s.user_id
    JOIN organizations owner_org ON owner_org.id = s.org_id
    LEFT JOIN templates t ON t.name = s.template_id
    LEFT JOIN sandbox_direct_route_identities direct_route
        ON direct_route.sandbox_id = s.id
    LEFT JOIN nodes active_node
        ON active_node.id = COALESCE(direct_route.current_node_id, s.active_node_id)
    LEFT JOIN node_direct active_direct ON active_direct.node_id = active_node.id
    LEFT JOIN node_k8s active_k8s ON active_k8s.node_id = active_node.id
    LEFT JOIN sandbox_ecw_credential_epochs ecw_credential
        ON ecw_credential.sandbox_id = s.id
    LEFT JOIN LATERAL (
        SELECT e.generation,
               NULLIF(BTRIM(e.pod_uid), '') AS pod_uid,
               e.route_activation_id
        FROM sandbox_k8s_executions e
        WHERE e.sandbox_id = s.id
          AND e.active
        ORDER BY e.generation DESC
        LIMIT 1
    ) active_execution ON TRUE
    LEFT JOIN LATERAL (
        SELECT ARRAY_AGG(trt.tag ORDER BY trt.tag) AS tags
        FROM template_releases tr
        JOIN template_release_tags trt ON trt.template_release_id = tr.id
        WHERE tr.template_name = s.template_id
          AND tr.hash = s.content_hash
    ) release_tags ON TRUE
";

const SANDBOX_FROM: &str = "
    sandboxes s
    {sandbox_join_tail}
";

fn format_age(epoch_str: &str) -> Option<String> {
    let epoch: u64 = epoch_str.parse().ok()?;
    Some(format_age_from_epoch(epoch))
}

fn format_age_from_epoch(epoch: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(epoch);
    let secs = now.saturating_sub(epoch);

    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

fn format_age_from_time(value: time::OffsetDateTime) -> Option<String> {
    let epoch = u64::try_from(value.unix_timestamp()).ok()?;
    Some(format_age_from_epoch(epoch))
}

fn format_time(value: time::OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn epoch_time(epoch_str: &str) -> Option<time::OffsetDateTime> {
    let epoch = epoch_str.parse().ok()?;
    time::OffsetDateTime::from_unix_timestamp(epoch).ok()
}

fn observed_created_at(observed: &SandboxObservedState) -> Option<time::OffsetDateTime> {
    observed
        .observed_created_at
        .as_deref()
        .and_then(epoch_time)
        .or(Some(observed.observed_at))
}

fn active_node_summary(sandbox: &Sandbox) -> Option<SandboxNodeSummary> {
    sandbox.active_node_id.map(|id| SandboxNodeSummary {
        id,
        name: sandbox.active_node_name.clone(),
        role: sandbox.active_node_role,
        address: sandbox.active_node_address,
        managed_by: sandbox.active_node_managed_by,
    })
}

fn network_policy_source_name(source: i32) -> Option<String> {
    match NetworkPolicySource::try_from(source).unwrap_or(NetworkPolicySource::Unspecified) {
        NetworkPolicySource::Template => Some("template".to_string()),
        NetworkPolicySource::Override => Some("override".to_string()),
        NetworkPolicySource::Unspecified => None,
    }
}

fn sandbox_policy_override(sandbox: &Sandbox) -> Option<NetworkPolicy> {
    sandbox
        .network_policy
        .as_ref()
        .map(|policy| policy.0.clone())
}

fn sandbox_template_policy(sandbox: &Sandbox) -> Option<NetworkPolicyV1> {
    sandbox
        .template_network_policy
        .as_ref()
        .map(|policy| policy.0.clone())
}

fn sandbox_effective_desired_policy(sandbox: &Sandbox) -> Option<NetworkPolicy> {
    sandbox_policy_override(sandbox).or_else(|| sandbox_template_policy(sandbox))
}

fn i64_to_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

fn sandbox_effective_network_policy_generation(sandbox: &Sandbox) -> Option<u64> {
    if sandbox.network_policy.is_some() {
        i64_to_u64(sandbox.network_policy_generation)
    } else {
        sandbox
            .template_network_policy_generation
            .and_then(i64_to_u64)
            .or_else(|| i64_to_u64(sandbox.network_policy_generation))
    }
}

fn sandbox_effective_network_policy_source(sandbox: &Sandbox) -> Option<&'static str> {
    if sandbox.network_policy.is_some() {
        Some("override")
    } else if sandbox_effective_network_policy_generation(sandbox).is_some() {
        Some("template")
    } else {
        None
    }
}

pub(super) fn network_policy_status_for(
    sandbox: Option<&Sandbox>,
    observed: Option<&SandboxObservedState>,
    live: Option<&SandboxInfo>,
) -> Option<NetworkPolicyStatus> {
    let desired_generation = sandbox.and_then(sandbox_effective_network_policy_generation);
    let desired_source = sandbox.and_then(sandbox_effective_network_policy_source);
    let observed_generation = observed
        .and_then(|state| state.observed_network_policy_generation)
        .and_then(i64_to_u64)
        .or_else(|| {
            live.map(|info| info.applied_network_policy_generation)
                .filter(|generation| *generation != 0)
        });
    let observed_source = observed
        .and_then(|state| state.observed_network_policy_source.clone())
        .or_else(|| {
            live.and_then(|info| network_policy_source_name(info.applied_network_policy_source))
        });
    let observed_authority = observed
        .and_then(|state| state.observed_network_policy_authority.clone())
        .or_else(|| {
            live.and_then(|info| {
                PolicyAuthority::from_optional_proto(info.network_policy_authority)
                    .ok()
                    .flatten()
                    .map(|authority| authority.to_string())
            })
        });
    let inherited_from_template = sandbox
        .map(|sandbox| sandbox.network_policy.is_none())
        .or_else(|| {
            observed
                .and_then(|state| state.observed_network_policy_source.as_deref())
                .map(|source| source == "template")
        });

    if observed.is_none() && live.is_none() && desired_generation.is_none() {
        return None;
    }

    let computed_condition = match (
        desired_source,
        desired_generation,
        observed_source.as_deref(),
        observed_generation,
    ) {
        (
            Some(desired_source),
            Some(desired_generation),
            Some(observed_source),
            Some(observed_generation),
        ) if observed_source == desired_source && observed_generation == desired_generation => {
            NetworkPolicyCondition {
                condition_type: "Reconciled".to_string(),
                status: "True".to_string(),
                reason: Some("ObservedSourceAndGenerationMatch".to_string()),
                ..NetworkPolicyCondition::default()
            }
        }
        (Some(_), Some(_), Some(_), Some(_)) => NetworkPolicyCondition {
            condition_type: "Stale".to_string(),
            status: "True".to_string(),
            reason: Some("ObservedSourceOrGenerationDiffers".to_string()),
            ..NetworkPolicyCondition::default()
        },
        (Some(_), Some(_), None, Some(_)) => NetworkPolicyCondition {
            condition_type: "Unknown".to_string(),
            status: "True".to_string(),
            reason: Some("NoObservedSource".to_string()),
            ..NetworkPolicyCondition::default()
        },
        (Some(_), Some(_), _, None) => NetworkPolicyCondition {
            condition_type: "Unknown".to_string(),
            status: "True".to_string(),
            reason: Some("NoObservedGeneration".to_string()),
            ..NetworkPolicyCondition::default()
        },
        _ => NetworkPolicyCondition {
            condition_type: "Unknown".to_string(),
            status: "True".to_string(),
            reason: Some("NoDesiredGeneration".to_string()),
            ..NetworkPolicyCondition::default()
        },
    };
    let mut conditions = vec![computed_condition];
    if let Some(observed) = observed {
        for condition in &observed.network_policy_conditions.0 {
            if !conditions
                .iter()
                .any(|existing| existing.condition_type == condition.condition_type)
            {
                conditions.push(condition.clone());
            }
        }
    }
    conditions.sort_by(|left, right| left.condition_type.cmp(&right.condition_type));

    Some(NetworkPolicyStatus {
        observed_network_policy: observed
            .and_then(|state| state.observed_network_policy.as_ref())
            .map(|policy| policy.0.clone()),
        observed_generation,
        observed_source,
        observed_authority,
        observed_at: observed
            .and_then(|state| state.observed_network_policy_at)
            .map(format_time),
        inherited_from_template,
        source_template: sandbox.and_then(|sandbox| sandbox.template_id.clone()),
        conditions,
    })
}

/// Best-effort `sandboxes.lifecycle` write that refuses to overwrite a
/// terminal state. Called from the lifecycle action dispatcher after a
/// substrate operation succeeds (e.g. `pause`, `stop`, `checkpoint`) to
/// stamp the user-facing result lifecycle onto the row.
///
/// The terminal-state exclusion is the safety contract with the
/// transient-lifecycle sweeper: if the sweeper landed on `pausing`/
/// `stopping`/etc. while the substrate call was in flight and flipped
/// the row to `failed` (with the backing execution torn down), a
/// post-substrate "stamp `paused` / `stopped`" write would resurrect
/// the row as live while the pod is gone. CAS-guarding on `lifecycle
/// NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')` keeps the swept
/// terminal state.
///
/// The `maybe_` prefix is preserved: the helper has always been
/// best-effort (the original shape silently swallowed DB errors); a
/// race-loss now also silently leaves the swept state in place rather
/// than 5xx-ing. Long-running paths that *should* surface a CAS-loss
/// (Restore, Wake) carry their own dedicated CAS upstream of this
/// helper.
pub(super) async fn maybe_update_sandbox_lifecycle(pool: &PgPool, id: SandboxId, lifecycle: &str) {
    if let Err(err) = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $1
          WHERE id = $2
            AND lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')"
    ))
    .bind(lifecycle)
    .bind(id)
    .execute(pool)
    .await
    {
        tracing::warn!(
            sandbox_id = %id,
            lifecycle,
            error = %err,
            "best-effort sandbox lifecycle update failed"
        );
    }
}

/// Best-effort `(lifecycle, placement)` write that refuses to overwrite
/// a terminal state. Same terminal-state guard as
/// [`maybe_update_sandbox_lifecycle`]; see that function's doc for the
/// transient-lifecycle-sweeper safety contract.
pub(super) async fn maybe_update_sandbox_state(
    pool: &PgPool,
    id: SandboxId,
    lifecycle: &str,
    placement: &str,
) {
    if let Err(err) = sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $1, placement = $2
          WHERE id = $3
            AND lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')"
    ))
    .bind(lifecycle)
    .bind(placement)
    .bind(id)
    .execute(pool)
    .await
    {
        tracing::warn!(
            sandbox_id = %id,
            lifecycle,
            placement,
            error = %err,
            "best-effort sandbox state update failed"
        );
    }
}

pub(super) async fn fetch_sandbox_observed_states(
    pool: &PgPool,
    ids: &[SandboxId],
) -> Result<std::collections::HashMap<SandboxId, SandboxObservedState>, ControllerError> {
    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let rows = sqlx::query_as::<_, SandboxObservedState>(
        sql!("SELECT sos.sandbox_id,
                sos.node_id,
                COALESCE(observed_direct.name, observed_k8s.k8s_node_name) AS node_name,
                observed_node.role AS node_role,
                observed_node.managed_by AS node_managed_by,
                observed_direct.node_api_endpoint AS node_api_endpoint,
                observed_k8s.k8s_node_name AS substrate_node_id,
                COALESCE(sos.template, s.template_id, observed_template.template_name) AS template,
                sos.status,
                sos.spaced_version,
                sos.pid,
                sos.namespace,
                sos.uptime,
                sos.sandbox_owner,
                sos.image_hash,
                sos.snapshots_enabled,
                sos.disk_usage_bytes,
                sos.pool_template,
                sos.failure_reason,
                sos.observed_created_at,
                sos.observed_at,
                COALESCE(observed_release_tags.tags, ARRAY[]::TEXT[]) AS release_tags,
                nfos.observed_payload AS observed_network_policy,
                nfos.observed_generation AS observed_network_policy_generation,
                nfos.applied_source AS observed_network_policy_source,
                nfos.applied_authority AS observed_network_policy_authority,
                nfos.observed_at AS observed_network_policy_at,
                nfc.network_policy_conditions
         FROM sandbox_observed_states sos
         LEFT JOIN sandboxes s ON s.id = sos.sandbox_id
         LEFT JOIN nodes observed_node ON observed_node.id = sos.node_id
         LEFT JOIN node_direct observed_direct ON observed_direct.node_id = observed_node.id
         LEFT JOIN node_k8s observed_k8s ON observed_k8s.node_id = observed_node.id
         LEFT JOIN LATERAL (
             SELECT nti.template_name
             FROM node_template_inventory nti
             WHERE nti.node_id = sos.node_id
               AND sos.image_hash IS NOT NULL
               AND nti.content_hash = sos.image_hash
             ORDER BY nti.observed_at DESC, nti.template_name ASC
             LIMIT 1
         ) observed_template ON TRUE
         LEFT JOIN LATERAL (
             SELECT ARRAY_AGG(trt.tag ORDER BY trt.tag) AS tags
             FROM template_releases tr
             JOIN template_release_tags trt ON trt.template_release_id = tr.id
             WHERE tr.template_name = COALESCE(sos.template, s.template_id, observed_template.template_name)
               AND sos.image_hash IS NOT NULL
               AND tr.hash = sos.image_hash
         ) observed_release_tags ON TRUE
         LEFT JOIN node_facet_observed_status nfos
           ON nfos.node_id = sos.node_id
          AND nfos.facet = $2
          AND nfos.element = 'sandbox:' || sos.sandbox_id::text
         LEFT JOIN LATERAL (
             SELECT COALESCE(
                 jsonb_agg(
                     jsonb_build_object(
                         'type', condition_type,
                         'status', condition_status,
                         'reason', reason,
                         'message', message,
                         'last_transition_at', last_transition_at
                     )
                     ORDER BY condition_type ASC
                 ),
                 '[]'::jsonb
             ) AS network_policy_conditions
             FROM node_facet_conditions
             WHERE node_id = sos.node_id
               AND facet = $2
               AND element = 'sandbox:' || sos.sandbox_id::text
         ) nfc ON TRUE
         WHERE sos.sandbox_id = ANY($1)"),
    )
    .bind(ids)
    .bind(ReconcileFacet::NetworkPolicy.as_str())
    .fetch_all(pool)
    .await
    .map_err(|err| db_contention_error("query sandbox observed state", &err))?;
    Ok(rows.into_iter().map(|row| (row.sandbox_id, row)).collect())
}

pub(super) async fn fetch_observed_only_sandboxes(
    pool: &PgPool,
    known_ids: &[SandboxId],
) -> Result<Vec<SandboxObservedState>, ControllerError> {
    sqlx::query_as::<_, SandboxObservedState>(
        sql!("SELECT sos.sandbox_id,
                sos.node_id,
                COALESCE(observed_direct.name, observed_k8s.k8s_node_name) AS node_name,
                observed_node.role AS node_role,
                observed_node.managed_by AS node_managed_by,
                observed_direct.node_api_endpoint AS node_api_endpoint,
                observed_k8s.k8s_node_name AS substrate_node_id,
                COALESCE(sos.template, s.template_id, observed_template.template_name) AS template,
                sos.status,
                sos.spaced_version,
                sos.pid,
                sos.namespace,
                sos.uptime,
                sos.sandbox_owner,
                sos.image_hash,
                sos.snapshots_enabled,
                sos.disk_usage_bytes,
                sos.pool_template,
                sos.failure_reason,
                sos.observed_created_at,
                sos.observed_at,
                COALESCE(observed_release_tags.tags, ARRAY[]::TEXT[]) AS release_tags,
                nfos.observed_payload AS observed_network_policy,
                nfos.observed_generation AS observed_network_policy_generation,
                nfos.applied_source AS observed_network_policy_source,
                nfos.applied_authority AS observed_network_policy_authority,
                nfos.observed_at AS observed_network_policy_at,
                nfc.network_policy_conditions
         FROM sandbox_observed_states sos
         LEFT JOIN sandboxes s ON s.id = sos.sandbox_id
         LEFT JOIN nodes observed_node ON observed_node.id = sos.node_id
         LEFT JOIN node_direct observed_direct ON observed_direct.node_id = observed_node.id
         LEFT JOIN node_k8s observed_k8s ON observed_k8s.node_id = observed_node.id
         LEFT JOIN LATERAL (
             SELECT nti.template_name
             FROM node_template_inventory nti
             WHERE nti.node_id = sos.node_id
               AND sos.image_hash IS NOT NULL
               AND nti.content_hash = sos.image_hash
             ORDER BY nti.observed_at DESC, nti.template_name ASC
             LIMIT 1
         ) observed_template ON TRUE
         LEFT JOIN LATERAL (
             SELECT ARRAY_AGG(trt.tag ORDER BY trt.tag) AS tags
             FROM template_releases tr
             JOIN template_release_tags trt ON trt.template_release_id = tr.id
             WHERE tr.template_name = COALESCE(sos.template, s.template_id, observed_template.template_name)
               AND sos.image_hash IS NOT NULL
               AND tr.hash = sos.image_hash
         ) observed_release_tags ON TRUE
         LEFT JOIN node_facet_observed_status nfos
           ON nfos.node_id = sos.node_id
          AND nfos.facet = $2
          AND nfos.element = 'sandbox:' || sos.sandbox_id::text
         LEFT JOIN LATERAL (
             SELECT COALESCE(
                 jsonb_agg(
                     jsonb_build_object(
                         'type', condition_type,
                         'status', condition_status,
                         'reason', reason,
                         'message', message,
                         'last_transition_at', last_transition_at
                     )
                     ORDER BY condition_type ASC
                 ),
                 '[]'::jsonb
             ) AS network_policy_conditions
             FROM node_facet_conditions
             WHERE node_id = sos.node_id
               AND facet = $2
               AND element = 'sandbox:' || sos.sandbox_id::text
         ) nfc ON TRUE
         WHERE s.id IS NULL
           AND NOT (sos.sandbox_id = ANY($1))
         ORDER BY sos.observed_at DESC"),
    )
    .bind(known_ids)
    .bind(ReconcileFacet::NetworkPolicy.as_str())
    .fetch_all(pool)
    .await
    .map_err(|err| db_contention_error("query observed-only sandboxes", &err))
}

pub(super) fn sandbox_to_cached_response(
    sandbox: &Sandbox,
    observed: Option<&SandboxObservedState>,
) -> SandboxResponse {
    let mut response = sandbox_to_response(sandbox, None);
    if let Some(observed) = observed {
        response.lifecycle = observed.status.clone().or(response.lifecycle);
        response.status = observed.status.clone().or(response.status);
        response.spaced_version = observed.spaced_version.clone();
        response.pid = observed.pid.and_then(|value| u32::try_from(value).ok());
        response.namespace = observed.namespace.clone();
        response.uptime = observed.uptime.clone();
        response.image_hash = observed.image_hash.clone();
        if response.content_hash.is_none() {
            response.content_hash = observed.image_hash.clone();
        }
        if response.release_tags.is_empty() {
            response.release_tags.clone_from(&observed.release_tags);
        }
        if response.template.is_none() {
            response.template = observed_template(observed);
        }
        response.snapshots_enabled = Some(observed.snapshots_enabled);
        response.pool_template = observed.pool_template.clone();
        response.disk_usage_bytes = observed_disk_usage_bytes(observed);
        response.failure_reason = observed.failure_reason.clone();
        response.age = observed.observed_created_at.as_deref().and_then(format_age);
        response.network_policy_status =
            network_policy_status_for(Some(sandbox), Some(observed), None);
    }
    response
}

fn observed_template(observed: &SandboxObservedState) -> Option<String> {
    observed
        .template
        .clone()
        .or_else(|| observed.pool_template.clone())
}

pub(super) fn observed_only_sandbox_response(observed: &SandboxObservedState) -> SandboxResponse {
    let mut response = SandboxResponse {
        id: observed.sandbox_id,
        resource_type: if observed.pool_template.is_some() {
            SandboxResourceType::PoolSlot
        } else {
            SandboxResourceType::ObservedLocal
        },
        lifecycle: observed.status.clone(),
        placement: Some("observed".to_string()),
        active_node_id: None,
        active_node: None,
        status: observed.status.clone(),
        runtime: None,
        spaced_version: observed.spaced_version.clone(),
        pid: observed.pid.and_then(|value| u32::try_from(value).ok()),
        namespace: observed.namespace.clone(),
        uptime: observed.uptime.clone(),
        user: observed.sandbox_owner.clone(),
        content_hash: observed.image_hash.clone(),
        release_tags: observed.release_tags.clone(),
        image_hash: observed.image_hash.clone(),
        snapshots_enabled: Some(observed.snapshots_enabled),
        pool_template: observed.pool_template.clone(),
        disk_usage_bytes: observed_disk_usage_bytes(observed),
        failure_reason: observed.failure_reason.clone(),
        created_at: observed_created_at(observed),
        updated_at: Some(observed.observed_at),
        last_used_at: None,
        last_access_at: None,
        last_command_at: None,
        age: observed.observed_created_at.as_deref().and_then(format_age),
        network_policy: None,
        network_policy_override: None,
        template_network_policy: None,
        effective_desired_network_policy: None,
        network_policy_generation: None,
        network_policy_status: network_policy_status_for(None, Some(observed), None),
        ..SandboxResponse::default()
    };
    response.template = observed_template(observed);
    response
}

pub(super) fn observed_disk_usage_bytes(observed: &SandboxObservedState) -> Option<u64> {
    observed
        .disk_usage_bytes
        .and_then(|value| u64::try_from(value).ok())
}

pub fn sandbox_to_response(sandbox: &Sandbox, live: Option<&SandboxInfo>) -> SandboxResponse {
    let live_policy = live
        .and_then(|info| info.auto_lifecycle.as_ref())
        .and_then(|policy| policy_from_proto(policy).ok());
    let live_status = live
        .and_then(|info| info.auto_lifecycle_status.as_ref())
        .and_then(|status| status_from_proto(status).ok());

    SandboxResponse {
        id: sandbox.id,
        resource_type: SandboxResourceType::Sandbox,
        name: sandbox.name.clone(),
        user_id: Some(sandbox.user_id),
        org_id: Some(sandbox.org_id),
        lifecycle: live
            .map(|info| {
                info.observed_lifecycle()
                    .map(|state| state.label().to_string())
                    .unwrap_or_else(|_| "unknown".into())
            })
            .or_else(|| Some(sandbox.lifecycle.clone())),
        placement: Some(if live.is_some() {
            "on_node".to_string()
        } else {
            sandbox.placement.clone()
        }),
        active_node_id: sandbox.active_node_id,
        active_node: active_node_summary(sandbox),
        metadata: sandbox.metadata.0.clone(),
        created_at: Some(sandbox.created_at),
        updated_at: Some(sandbox.updated_at),
        last_used_at: sandbox.last_used_at,
        last_access_at: sandbox.last_access_at,
        last_command_at: sandbox.last_command_at,
        auto_lifecycle: Some(live_policy.unwrap_or_else(|| sandbox.auto_lifecycle.0.clone())),
        auto_lifecycle_status: Some(
            live_status.unwrap_or_else(|| sandbox.auto_lifecycle_state.0.public_status()),
        ),
        network_policy: sandbox_policy_override(sandbox),
        network_policy_override: sandbox_policy_override(sandbox),
        template_network_policy: sandbox_template_policy(sandbox),
        effective_desired_network_policy: sandbox_effective_desired_policy(sandbox),
        network_policy_generation: sandbox_effective_network_policy_generation(sandbox),
        network_policy_status: network_policy_status_for(Some(sandbox), None, live),
        status: live
            .map(|info| {
                info.observed_lifecycle()
                    .map(|state| state.label().to_string())
                    .unwrap_or_else(|_| "unknown".into())
            })
            .or_else(|| Some(sandbox.lifecycle.clone())),
        template: live
            .and_then(|info| info.template.clone())
            .or_else(|| sandbox.template_id.clone()),
        content_hash: sandbox
            .content_hash
            .clone()
            .or_else(|| live.and_then(|info| info.image_hash.clone())),
        release_tags: sandbox.release_tags.clone(),
        runtime: live.map(|info| info.runtime.clone().unwrap_or_else(|| "fc".to_string())),
        spaced_version: live.and_then(|info| proto_spaced_version(info.spaced_version.as_deref())),
        pid: live.and_then(|info| info.pid),
        namespace: live.and_then(|info| info.namespace.clone()),
        uptime: live.and_then(|info| info.uptime.clone()),
        // Ownership is an immutable birth fact: the label derives from the
        // cluster's sandbox row, never from what a node reports.
        user: Some(
            pplx_space_common_core::types::SandboxOwner::platform_user(sandbox.user_id).to_string(),
        ),
        image_hash: live.and_then(|info| info.image_hash.clone()),
        snapshots_enabled: live.map(|info| info.snapshots_enabled),
        snapshots_enabled_desired: sandbox.snapshots_enabled_desired,
        pool_template: live.and_then(|info| info.pool_template.clone()),
        disk_usage_bytes: None,
        failure_reason: live.and_then(|info| info.failure_reason.clone()),
        age: live
            .and_then(|info| info.created_at.as_deref().and_then(format_age))
            .or_else(|| format_age_from_time(sandbox.created_at)),
        volume_encryption: sandbox.volume_encryption,
    }
}

pub fn sandbox_query(sql: &str) -> String {
    sql.replace("{sandbox_columns}", SANDBOX_COLUMNS)
        .replace("{sandbox_from}", SANDBOX_FROM)
        .replace("{sandbox_join_tail}", SANDBOX_JOIN_TAIL)
}

pub async fn fetch_sandbox(pool: &PgPool, id: SandboxId) -> Result<Option<Sandbox>, sqlx::Error> {
    sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(sql!(sandbox_query(
        "SELECT {sandbox_columns} FROM {sandbox_from} WHERE s.id = $1",
    ))))
    .bind(id)
    .fetch_optional(pool)
    .await
}

/// Marks a pooled sandbox as started/on-node after its wake completes and
/// returns the freshly-joined response row in the same round-trip. The
/// lifecycle guard matches the non-pooled wake/restore completion writers:
/// if a supervisor moved the row into an inert lifecycle while wake was in
/// flight, this helper must surface the CAS miss rather than resurrecting it
/// as started.
///
/// Runs in a transaction so the termination fence's `nodes` row lock is still
/// held when the `UPDATE` commits. Two independent refusals, and they are
/// deliberately different errors: losing the lifecycle CAS means somebody else
/// owns this row, while the fence refusing means the *node* is leaving and the
/// caller should retry to be placed elsewhere.
pub async fn update_and_fetch_post_wake_sandbox(
    pool: &PgPool,
    id: SandboxId,
    active_node_id: Option<NodeId>,
) -> Result<Sandbox, ControllerError> {
    match post_wake_update(pool, id, active_node_id).await? {
        PostWakeUpdate::Updated(sandbox) => Ok(*sandbox),
        PostWakeUpdate::LifecycleCasLost => Err(post_wake_cas_lost()),
    }
}

/// What a pooled post-wake update found.
pub(crate) enum PostWakeUpdate {
    Updated(Box<Sandbox>),
    /// Another supervisor moved the row to an inert lifecycle while the wake was
    /// in flight, so the row is theirs now: a caller must not roll it back.
    LifecycleCasLost,
}

pub(crate) fn post_wake_cas_lost() -> ControllerError {
    ControllerError::internal(
        "pooled sandbox wake lost the lifecycle CAS — another supervisor flipped the row \
         to an inert lifecycle while pooled wake was in flight",
    )
}

/// [`update_and_fetch_post_wake_sandbox`], with a lost lifecycle CAS reported as
/// an outcome rather than an error, so a caller can tell "someone else owns this
/// row" apart from a failure it should roll back.
pub(crate) async fn post_wake_update(
    pool: &PgPool,
    id: SandboxId,
    active_node_id: Option<NodeId>,
) -> Result<PostWakeUpdate, ControllerError> {
    let sql = sandbox_query(
        "WITH updated AS (
             UPDATE sandboxes
             SET lifecycle = 'started',
                 placement = 'on_node',
                 active_node_id = $1,
                 placement_changed_at = NOW()
             WHERE id = $2
               AND lifecycle NOT IN ('failed', 'quarantined', 'deleted', 'cleanup_required')
             RETURNING *
         )
         SELECT {sandbox_columns}
         FROM updated s
         {sandbox_join_tail}",
    );
    // Opened unconditionally, so a wake that attaches no node pays a BEGIN and a
    // COMMIT to lock nothing. Kept deliberately for the same reason as
    // `finalize_created_sandbox`: the alternative extracts this CTE into its own
    // executor-generic function and separates the write from the fence that
    // guards it, and the adjacency is the property worth more than two round
    // trips.
    let mut tx = pool.begin().await.map_err(|err| {
        ControllerError::internal(format!("failed to start pooled post-wake update: {err}"))
    })?;
    // A wake that attaches no node has nothing to order against the terminate
    // path; the lifecycle flip below still has to happen.
    if let Some(node_id) = active_node_id {
        let fence = check_placement_fence(&mut tx, node_id, id, Writer::PooledPostWake)
            .await
            .map_err(|err| {
                ControllerError::internal(format!(
                    "failed to check the pooled post-wake placement fence: {err}"
                ))
            })?;
        if fence.refuses() {
            // The transaction is dropped unwritten, so the row keeps whatever
            // lifecycle it had rather than being published as `started` on a
            // node that is about to stop existing. The woken guest is lost
            // either way — it is on that node — and the caller's create/wake
            // failure path is what tears it down.
            return Err(ControllerError::service_unavailable(fence_refusal_message(
                node_id, fence,
            )));
        }
    }
    let updated = sqlx::query_as::<_, Sandbox>(sqlx::AssertSqlSafe(sql!(sql.as_str())))
        .bind(active_node_id)
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        // Not classified, deliberately: by the time this runs the warm slot
        // is claimed, bound, and prepared and the pod is running. Both callers
        // roll the claim back on any error from here, but `create` mints a
        // fresh `SandboxId` per request with no idempotency key, so a retryable
        // 503 would invite a retry that claims a second slot even if that
        // rollback itself failed. See `db_contention_error`'s exception.
        .map_err(|err| {
            crate::controllers::internal_error("pooled sandbox post-wake update failed", err)
        })?;
    let Some(updated) = updated else {
        return Ok(PostWakeUpdate::LifecycleCasLost);
    };
    tx.commit().await.map_err(|err| {
        ControllerError::internal(format!("failed to commit pooled post-wake update: {err}"))
    })?;
    Ok(PostWakeUpdate::Updated(Box::new(updated)))
}
