use pplx_space_sandbox_model::SandboxId;

use super::*;
use pplx_space_net_common::NetworkPolicy;
use pplx_space_sandbox_store as sandbox_store;

use super::lifecycle::SandboxAction;

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

/// Which start stamp this substrate needs. On a substrate whose start of a
/// `suspended` row claims no `restoring` (Direct), a start of a `suspended`
/// row cold-boots it on its node and leaves the row `suspended` until the
/// stamp, and a Direct suspend writes no `suspending`, so every start there
/// may write over `suspended` (see the `action_stamp` module docs). On
/// Kubernetes a start of a `suspended` row delegates to the wake, which
/// claims `restoring` and normally commits `started` itself, so a
/// `suspended` the stamp finds is a newer suspend's.
pub(super) fn start_substrate(
    capabilities: crate::controllers::substrate::SubstrateCapabilities,
) -> sandbox_store::action_stamp::StartSubstrate {
    use sandbox_store::action_stamp::StartSubstrate;
    if capabilities.start_of_suspended_claims_restoring {
        StartSubstrate::ClaimsRestoring
    } else {
        StartSubstrate::LeavesSuspended
    }
}

/// The stamp a successful run of `action` writes after its substrate call.
/// `None` for the actions that write no result lifecycle there: start's arm
/// already attempted its full stamp (`maybe_mark_started_after_start`), wake
/// and restore run their own completion CAS, a suspend on a substrate whose
/// suspend commits the lifecycle itself writes none (see
/// `SubstrateCapabilities::suspend_commits_lifecycle`), and the rest leave
/// the lifecycle alone.
pub(super) fn post_action_stamp(
    action: SandboxAction,
    capabilities: crate::controllers::substrate::SubstrateCapabilities,
) -> Option<sandbox_store::action_stamp::ActionStamp> {
    use sandbox_store::action_stamp::ActionStamp;
    match action {
        SandboxAction::Resume => Some(ActionStamp::Resume),
        SandboxAction::Pause => Some(ActionStamp::Pause),
        SandboxAction::Stop | SandboxAction::Kill => Some(ActionStamp::Stop),
        SandboxAction::Suspend => {
            (!capabilities.suspend_commits_lifecycle).then_some(ActionStamp::Suspend)
        }
        SandboxAction::Start
        | SandboxAction::Wake
        | SandboxAction::ImplicitWake(_)
        | SandboxAction::Restore
        | SandboxAction::Checkpoint
        | SandboxAction::EnableSnapshots
        | SandboxAction::DisableSnapshots
        | SandboxAction::Stats
        | SandboxAction::OpenPort
        | SandboxAction::Rollback
        | SandboxAction::Spaced
        | SandboxAction::Update => None,
    }
}

/// Best-effort `sandboxes.lifecycle` write of an action's result. Called from
/// the lifecycle action dispatcher after a substrate operation succeeds
/// (e.g. `pause`, `stop`) to stamp the user-facing result lifecycle onto the
/// row.
///
/// The stamp writes only while the row is still in one of its action's
/// in-flight lifecycles (`ActionStamp::admits` in
/// `pplx-space-sandbox-store`, whose `action_stamp` module docs say who owns
/// the row when it refuses). That keeps the transient-lifecycle sweeper's
/// `failed` (with the backing execution torn down, a stamp would resurrect
/// the row as live while the pod is gone) and another intent's state: a
/// delete, stop or suspend that began or finished while the substrate call
/// was in flight, or another owner's restore claim. The `action_stamp`
/// module docs list which stamp keeps which.
///
/// The `maybe_` prefix is preserved: the helper has always been
/// best-effort (the original shape silently swallowed DB errors). A refusal
/// is another writer winning, so it is logged and the action still succeeds
/// rather than 5xx-ing. Long-running paths that *should* surface a CAS-loss
/// (Restore, Wake) carry their own dedicated CAS upstream of this helper.
pub(super) async fn maybe_update_sandbox_lifecycle(
    pool: &PgPool,
    id: SandboxId,
    stamp: sandbox_store::action_stamp::ActionStamp,
) {
    match sandbox_store::action_stamp::stamp_action_lifecycle(pool, id, stamp).await {
        Ok(true) => {}
        Ok(false) => tracing::info!(
            sandbox_id = %id,
            lifecycle = %stamp.lifecycle(),
            "sandbox lifecycle stamp refused: another writer owns the row"
        ),
        Err(err) => tracing::warn!(
            sandbox_id = %id,
            lifecycle = %stamp.lifecycle(),
            error = %err,
            "best-effort sandbox lifecycle update failed"
        ),
    }
}

/// Best-effort write of a successful start: `started`, `on_node`, under the
/// same in-flight guard as [`maybe_update_sandbox_lifecycle`] with
/// `ActionStamp::Start(substrate)`; see that function's doc.
///
/// It also drops a `disk` artifact pin. A disk pin names the bytes of a
/// stopped rootfs, and a started guest can write past them, so once the start
/// lands the pin describes nothing current; the node drops its own pin when a
/// cold start completes (sandbox-manager's `mark_start_succeeded`). Left set,
/// the cluster pin would outlive the next stop and make a later forced drain
/// refuse the sandbox as an unattributable artifact on every tick. The pins
/// this clears are the node drain's (`record_stopped_evacuation_artifact`),
/// which a stopped re-placement's finalize keeps in place. A `full` pin is
/// kept: this write also completes a start that restored a suspended sandbox,
/// and the node keeps that pin across a restore (`mark_restore_succeeded`).
pub(super) async fn maybe_mark_started_after_start(
    pool: &PgPool,
    id: SandboxId,
    substrate: sandbox_store::action_stamp::StartSubstrate,
) {
    match sandbox_store::action_stamp::stamp_started_after_start(pool, id, substrate).await {
        Ok(true) => {}
        Ok(false) => tracing::info!(
            sandbox_id = %id,
            "post-start sandbox state stamp refused: another writer owns the row"
        ),
        Err(err) => tracing::warn!(
            sandbox_id = %id,
            error = %err,
            "best-effort post-start sandbox state update failed"
        ),
    }
}

pub(super) async fn fetch_sandbox_observed_states(
    pool: &PgPool,
    ids: &[SandboxId],
) -> Result<std::collections::HashMap<SandboxId, SandboxObservedState>, ControllerError> {
    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let rows = pplx_space_sandbox_teardown::direct::observed_reads::observed_states(
        pool,
        ids,
        ReconcileFacet::NetworkPolicy.as_str(),
    )
    .await
    .map_err(|err| db_contention_error("query sandbox observed state", &err))?;
    Ok(rows
        .into_iter()
        .map(|row| (row.sandbox_id, SandboxObservedState::from(row)))
        .collect())
}

pub(super) async fn fetch_observed_only_sandboxes(
    pool: &PgPool,
    known_ids: &[SandboxId],
) -> Result<Vec<SandboxObservedState>, ControllerError> {
    pplx_space_sandbox_teardown::direct::observed_reads::observed_only(
        pool,
        known_ids,
        ReconcileFacet::NetworkPolicy.as_str(),
    )
    .await
    .map(|rows| rows.into_iter().map(SandboxObservedState::from).collect())
    .map_err(|err| db_contention_error("query observed-only sandboxes", &err))
}

pub(super) fn sandbox_to_cached_response(
    sandbox: &Sandbox,
    observed: Option<&SandboxObservedState>,
) -> SandboxResponse {
    let mut response = sandbox_to_response(sandbox, None);
    if let Some(observed) = observed {
        response.set_state(SandboxStateInput::observed_on(
            sandbox.lifecycle.clone(),
            sandbox.active_node_id,
            observed.status.clone(),
            observed.node_id,
        ));
        response.spaced_version = observed.version.spaced_version.clone();
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

/// A sandbox a node reports with no sandbox row. It is a pool slot when the
/// node says so (`pool_template`, which Direct nodes report) or when a
/// warm-pool slot reserves its ID (`pool_id`, Kubernetes, whose nodes report
/// no pool membership).
pub(super) fn observed_only_sandbox_response(
    observed: &SandboxObservedState,
    pool_id: Option<uuid::Uuid>,
) -> SandboxResponse {
    let mut response = SandboxResponse {
        id: observed.sandbox_id,
        resource_type: if observed.pool_template.is_some() || pool_id.is_some() {
            SandboxResourceType::PoolSlot
        } else {
            SandboxResourceType::ObservedLocal
        },
        pool_id,
        placement: Some("observed".to_string()),
        active_node_id: None,
        active_node: None,
        runtime: None,
        spaced_version: observed.version.spaced_version.clone(),
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
    response.set_state(SandboxStateInput::new(None, observed.status.clone()));
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
    let live_lifecycle = live.map(|info| {
        info.observed_status()
            .map(|state| state.label().to_string())
            .unwrap_or_else(|_| "unknown".into())
    });
    let state_input = SandboxStateInput::new(Some(sandbox.lifecycle.clone()), live_lifecycle);

    SandboxResponse {
        id: sandbox.id,
        resource_type: SandboxResourceType::Sandbox,
        name: sandbox.name.clone(),
        user_id: Some(sandbox.user_id),
        user_email: match sandbox.owner_source {
            pplx_space_accounts::SandboxOwnerSource::Local => sandbox.user_email.clone(),
            pplx_space_accounts::SandboxOwnerSource::FederatedIdentity => None,
        },
        org_id: Some(sandbox.org_id),
        lifecycle: state_input.reported_lifecycle(),
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
        status: state_input.reported_lifecycle(),
        state_input,
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
        user: Some(pplx_space_accounts::SandboxOwner::platform_user(sandbox.user_id).to_string()),
        image_hash: live.and_then(|info| info.image_hash.clone()),
        snapshots_enabled: live.map(|info| info.snapshots_enabled),
        snapshots_enabled_desired: sandbox.snapshots_enabled_desired,
        pool_template: live.and_then(|info| info.pool_template.clone()),
        pool_id: None,
        disk_usage_bytes: None,
        failure_reason: live.and_then(|info| info.failure_reason.clone()),
        age: live
            .and_then(|info| info.created_at.as_deref().and_then(format_age))
            .or_else(|| format_age_from_time(sandbox.created_at)),
        volume_encryption: sandbox.volume_encryption,
    }
}

/// The sandbox's response row; `None` when there is no row.
pub async fn fetch_sandbox(
    executor: impl sqlx::PgExecutor<'_>,
    id: SandboxId,
) -> Result<Option<Sandbox>, sqlx::Error> {
    sandbox_store::direct::sandbox_listing::by_id(executor, id).await
}

/// Marks a pooled sandbox as started/on-node after its wake completes and
/// returns the freshly-joined response row. The store verb's lifecycle guard
/// ([`sandbox_store::action_stamp::stamp_pooled_post_wake`]) refuses a row
/// another writer moved while the wake was in flight: this helper must
/// surface the CAS miss rather than resurrecting a failed row as started or
/// overwriting a delete or suspend that began. The read shares the write's
/// transaction, so the row lock keeps the response showing this write.
pub async fn update_and_fetch_post_wake_sandbox(
    pool: &PgPool,
    id: SandboxId,
    active_node_id: Option<NodeId>,
) -> Result<Sandbox, ControllerError> {
    // Not classified, deliberately, and for the same reason as the
    // `fetch_sandbox` arm this shares an `if` with in
    // `queries::try_create_from_pool`: by the time this runs the warm slot
    // is claimed, bound, and prepared and the pod is running, with no
    // rollback on this arm. `create` mints a fresh `SandboxId` per request
    // with no idempotency key, so a retryable 503 here claims a second
    // slot and starts a second sandbox while the first stays `started`
    // under an id the caller never sees. See `db_contention_error`'s
    // exception. This is the `wait_until_ready` arm — the one ECW create
    // takes — so it is the more common of the two, not the edge case.
    let failed =
        |err| crate::controllers::internal_error("pooled sandbox post-wake update failed", err);
    let lost_cas = || {
        ControllerError::internal(
            "pooled sandbox wake lost the lifecycle CAS — another writer moved the row to a \
             terminal, deleting, stopped, restoring or suspend lifecycle while pooled wake was \
             in flight",
        )
    };
    let mut tx = pool.begin().await.map_err(failed)?;
    if !sandbox_store::action_stamp::stamp_pooled_post_wake(&mut *tx, id, active_node_id)
        .await
        .map_err(failed)?
    {
        return Err(lost_cas());
    }
    let sandbox = fetch_sandbox(&mut *tx, id)
        .await
        .map_err(failed)?
        .ok_or_else(lost_cas)?;
    tx.commit().await.map_err(failed)?;
    Ok(sandbox)
}
