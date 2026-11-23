use pplx_space_accounts::NodeId;
use pplx_space_byte_size::MemorySize;
use pplx_space_cluster_pool::{
    ClaimedPoolSlot, ManualPoolConfigSpec, PoolAssignment, PoolConfigWithCounts, PoolSlotState,
    ReservedPoolSlot, WarmPoolStore,
};
use pplx_space_execution_spec::VcpuCount;
use pplx_space_k8s_ids::PodUid;
use pplx_space_node_registration_store::lookup as node_lookup;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use pplx_space_template_release_original::digest_original;
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::LazyLock;
use uuid::Uuid;

use crate::controllers::{
    ControllerError, db_contention_error, pool_store_error,
    substrate::{
        AdmissionPoolClaim, AdmittedPoolClaim, AdmittedPoolClaimLookup, PooledExecutionClaim,
        PooledExecutionClaimAttempt, PooledExecutionClaimRequest, PooledExecutionPoolShape,
    },
};
use crate::models::{ClusterPoolConfigRequest, ClusterPoolConfigResponse};

use super::K8sStore;

pub(super) fn pooled_route_activation_id(
    claim: &PooledExecutionClaim,
) -> Result<Uuid, ControllerError> {
    claim.pool_slot_id.ok_or_else(|| {
        ControllerError::internal("Kubernetes pooled claim is missing its pool slot id")
    })
}

fn u32_to_i32(value: u32, field: &'static str) -> Result<i32, ControllerError> {
    i32::try_from(value)
        .map_err(|err| ControllerError::bad_request(format!("{field} is too large: {err}")))
}

fn i32_to_u32(value: i32, field: &'static str) -> Result<u32, ControllerError> {
    u32::try_from(value).map_err(|err| {
        ControllerError::internal(format!("cluster pool config has invalid {field}: {err}"))
    })
}

fn i64_count_to_u32(value: i64, field: &'static str) -> Result<u32, ControllerError> {
    u32::try_from(value).map_err(|err| {
        ControllerError::internal(format!("cluster pool count has invalid {field}: {err}"))
    })
}

pub(super) async fn list_pool_configs(
    store: &K8sStore,
) -> Result<Vec<ClusterPoolConfigResponse>, ControllerError> {
    let rows = WarmPoolStore::new(store.pool().clone())
        .list_configs_with_counts()
        .await
        .map_err(|err| pool_store_error("failed to load cluster pool configs", &err))?;

    rows.into_iter().map(TryInto::try_into).collect()
}

/// `boots_template_twins`: whether this substrate's creates boot the twin
/// they resolve to (the environment placement gate is on), so the twin's
/// config takes the change too.
pub(super) async fn upsert_pool_config(
    store: &K8sStore,
    request: ClusterPoolConfigRequest,
    boots_template_twins: bool,
) -> Result<ClusterPoolConfigResponse, ControllerError> {
    if let Some(disk_mib) = request.disk_mib {
        pplx_space_sandbox_model::sandbox_storage::validate_rootfs_mib(disk_mib)
            .map_err(|error| ControllerError::bad_request(format!("disk_mib: {error}")))?;
    }
    if request.reset_disk_to_template_default && request.disk_mib.is_some() {
        return Err(ControllerError::bad_request(
            "reset_disk_to_template_default cannot be combined with disk_mib",
        ));
    }
    let spec = ManualPoolConfigSpec {
        vcpu: u32_to_i32(request.vcpu, "vcpu")?,
        memory_mib: u32_to_i32(request.memory_mib, "memory_mib")?,
        disk_mib: request
            .disk_mib
            .map(|value| u32_to_i32(value, "disk_mib"))
            .transpose()?,
        reset_disk_to_template_default: request.reset_disk_to_template_default,
        target_ready_slots: u32_to_i32(request.target_ready_slots, "target_ready_slots")?,
        refill_per_tick: u32_to_i32(request.refill_per_tick, "refill_per_tick")?,
        max_warming_slots: u32_to_i32(request.max_warming_slots, "max_warming_slots")?,
        namespace: store.namespace(),
        enabled: request.enabled,
        encryptable: request.encryptable,
    };
    let mut tx = store
        .pool()
        .begin()
        .await
        .map_err(|err| db_contention_error("failed to start transaction", &err))?;
    // An edit of an existing pool names its release, so the change lands on
    // that config even after the template's creates move to another twin.
    let release = match request.template_release_id {
        Some(release_id) => {
            named_pool_template_release(
                &mut tx,
                release_id,
                &request.template_name,
                request.content_hash.as_deref(),
            )
            .await?
        }
        None => {
            resolve_pool_template_release(
                &mut tx,
                &request.template_name,
                request.content_hash.as_deref(),
            )
            .await?
        }
    }
    .ok_or_else(|| match request.template_release_id {
        Some(release_id) => ControllerError::not_found(format!(
            "template release {release_id} isn't a release of the request's template and hash"
        )),
        None => ControllerError::not_found("template release not found"),
    })?;
    // After the release lookup, which reads no config row, and before the
    // config write: a template whose first release registered after a lock
    // taken earlier found no row would otherwise go unlocked.
    if boots_template_twins {
        store
            .lock_template_against_twin_inserts(&mut tx, &request.template_name)
            .await?;
    }
    let upsert_error = |err: pplx_space_cluster_pool::PoolError| match err {
        pplx_space_cluster_pool::PoolError::EncryptableConfigFenced => {
            ControllerError::bad_request(err.to_string())
        }
        err => pool_store_error("failed to upsert cluster pool config", &err),
    };
    let id = WarmPoolStore::upsert_manual_config(&mut tx, release.id, &spec)
        .await
        .map_err(upsert_error)?;
    // Creates of the release may resolve to one of its twins
    // (`resolved_release_sql`), and claim only that twin's slots, so the
    // twin's config takes the same change. The original's stays the one
    // reported: it's the release the request names, and the pool a rollback
    // resolves to.
    let resolved = crate::controllers::resolved_create_release(
        &mut *tx,
        &request.template_name,
        &release.content_hash,
        boots_template_twins,
    )
    .await?;
    // Only a release in its build's original environment passes the change
    // on, as only its disable does ([`disable_pool_config`]): a twin's own
    // upsert, like its disable, changes only its config.
    let release_is_twin = pplx_space_template_release_store::twin_original(
        &mut *tx,
        release.id,
        pplx_space_execution_catalog::catalog::initial_environment_reference(),
    )
    .await
    .map_err(|err| db_contention_error("failed to read the release's original", &err))?
    .is_some();
    if let Some(twin) = resolved.filter(|resolved| !release_is_twin && *resolved != release.id) {
        // The twin takes the original's effective disk size, not the
        // request's: a request without `disk_mib` keeps the original's
        // override, which a twin config made by this upsert would lack. It
        // takes the original's disk I/O limits too: a config this upsert
        // makes would get the platform defaults, and once it's
        // operator-managed the autoscaler no longer sets them from policy.
        let disk_mib = WarmPoolStore::config_disk_mib(&mut tx, id)
            .await
            .map_err(|err| pool_store_error("failed to read cluster pool config", &err))?;
        let twin_spec = ManualPoolConfigSpec {
            disk_mib,
            reset_disk_to_template_default: disk_mib.is_none(),
            ..spec
        };
        let twin_config = WarmPoolStore::upsert_manual_config(&mut tx, twin, &twin_spec)
            .await
            .map_err(upsert_error)?;
        WarmPoolStore::copy_config_disk_io_limits(&mut tx, id, twin_config)
            .await
            .map_err(|err| pool_store_error("failed to copy cluster pool config", &err))?;
    }
    tx.commit()
        .await
        .map_err(|err| db_contention_error("failed to commit cluster pool config", &err))?;

    fetch_pool_config(store, id)
        .await?
        .ok_or_else(|| ControllerError::internal("cluster pool config missing after upsert"))
}

/// Disable pool config `id`. An original's disable also disables the config
/// for the same shape on every one of its twins, in the same transaction.
/// Creates of the build may resolve to any twin, and claim only that twin's
/// slots ([`upsert_pool_config`] writes the twin they resolve to when it
/// runs), and the twin they resolve to can change after an upsert, when the
/// default moves or the gate leaves `on`. A disable that reached only the
/// twin resolving now would leave an earlier twin's config warming slots
/// that nothing disables. Where the build has no twins, it disables only
/// `id`, as before twins.
pub(super) async fn disable_pool_config(
    store: &K8sStore,
    id: Uuid,
) -> Result<ClusterPoolConfigResponse, ControllerError> {
    let disable_error = |err: pplx_space_cluster_pool::PoolError| {
        pool_store_error("failed to disable cluster pool config", &err)
    };
    let mut tx = store
        .pool()
        .begin()
        .await
        .map_err(|err| db_contention_error("failed to start transaction", &err))?;
    // Before the config's row, as the upsert takes it: a twin insert holds
    // the template's row and then locks the twin's config rows, and a twin
    // it hasn't committed yet is one this disable's read of the twins below
    // can't see.
    if let Some(template_name) = store.pool_config_template(&mut tx, id).await? {
        store
            .lock_template_against_twin_inserts(&mut tx, &template_name)
            .await?;
    }
    let disabled = WarmPoolStore::disable_config(&mut tx, id)
        .await
        .map_err(disable_error)?
        .ok_or_else(|| ControllerError::not_found("cluster pool config not found"))?;
    // Only the disable of a release in its build's original environment
    // reaches the twins, as only such a release's upsert passes its change
    // on ([`upsert_pool_config`]): a superseded
    // twin's config, disabled to stop warming slots nobody claims, leaves the
    // twin creates use and the original alone. The original is the one
    // resolution picks, across both spellings of the hash.
    let initial_environment =
        pplx_space_execution_catalog::catalog::initial_environment_reference();
    let twin_of = pplx_space_template_release_store::twin_original(
        &mut *tx,
        disabled.template_release_id,
        initial_environment,
    )
    .await
    .map_err(|err| db_contention_error("failed to read the release's original", &err))?;
    if twin_of.is_none() {
        let twins = pplx_space_template_release_store::twin_releases(
            &mut *tx,
            disabled.template_release_id,
            initial_environment,
        )
        .await
        .map_err(|err| db_contention_error("failed to read the release's twins", &err))?;
        for twin in twins {
            WarmPoolStore::disable_shape_config(&mut tx, twin, &disabled)
                .await
                .map_err(disable_error)?;
        }
    }
    tx.commit()
        .await
        .map_err(|err| db_contention_error("failed to commit cluster pool config", &err))?;

    fetch_pool_config(store, id)
        .await?
        .ok_or_else(|| ControllerError::not_found("cluster pool config not found"))
}

#[tracing::instrument(
    name = "substrate.k8s.pool.claim_pooled_execution",
    skip_all,
    fields(
        template = request.template,
        content_hash = request.content_hash.unwrap_or(""),
        vcpu = ?request.vcpu,
        memory_mib = ?request.memory_mib,
        result = tracing::field::Empty,
        claim_id = tracing::field::Empty,
        sandbox_id = tracing::field::Empty,
    )
)]
/// `boots_template_twins`: whether this substrate's creates boot the twin
/// they resolve to (the environment placement gate is on); only then does a
/// claim without a resolved release take the twin's slots.
pub(super) async fn claim_pooled_execution(
    pool: &PgPool,
    request: &PooledExecutionClaimRequest<'_>,
    namespace: &str,
    boots_template_twins: bool,
) -> Result<PooledExecutionClaimAttempt, ControllerError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| db_contention_error("failed to start pool claim transaction", &err))?;
    let claim =
        claim_pooled_execution_tx(&mut tx, request, namespace, boots_template_twins).await?;
    tx.commit()
        .await
        .map_err(|err| db_contention_error("failed to commit pool claim transaction", &err))?;
    Ok(claim)
}

async fn claim_pooled_execution_tx(
    tx: &mut Transaction<'_, Postgres>,
    request: &PooledExecutionClaimRequest<'_>,
    namespace: &str,
    boots_template_twins: bool,
) -> Result<PooledExecutionClaimAttempt, ControllerError> {
    let span = tracing::Span::current();
    let (release_id, content_hash) = match (request.template_release_id, request.content_hash) {
        // Fast path: caller already resolved the template release. Skip the
        // `template_releases` lookup the API controller just performed.
        (Some(release_id), Some(content_hash)) => (release_id, content_hash.to_string()),
        _ => {
            let Some(release) =
                resolve_pool_template_release(tx, request.template, request.content_hash).await?
            else {
                span.record("result", "template_miss");
                return Ok(PooledExecutionClaimAttempt::missed(None));
            };
            // A create claims the slots of the release it resolves to
            // (`resolved_release_sql`), which may be a twin.
            let release_id = crate::controllers::resolved_create_release(
                &mut **tx,
                request.template,
                &release.content_hash,
                boots_template_twins,
            )
            .await?
            .unwrap_or(release.id);
            (release_id, release.content_hash)
        }
    };

    let claim_id = Uuid::now_v7();
    span.record("claim_id", tracing::field::display(claim_id));
    let (slot, miss_shape) =
        claim_ready_pool_slot(tx, request, release_id, namespace, claim_id).await?;
    let Some(slot) = slot else {
        span.record("result", "slot_miss");
        return Ok(PooledExecutionClaimAttempt::missed(miss_shape));
    };
    span.record(
        "sandbox_id",
        tracing::field::display(slot.reserved_sandbox_id),
    );
    let (claim, pool_shape) = pooled_claim_from_slot(tx, slot, claim_id, release_id, content_hash)
        .await
        .inspect_err(|_| {
            span.record("result", "error");
        })?;
    span.record("result", "claimed");

    Ok(PooledExecutionClaimAttempt::claimed(claim, pool_shape))
}

/// Build the execution claim for a slot the caller holds under `claim_id`.
/// Shared by the any-slot claim and the admitted-slot lookup, so both hand
/// the adoption path the same pod identity, shape, and generation.
/// `template_release_id` is the release of the slot's pool.
async fn pooled_claim_from_slot(
    tx: &mut Transaction<'_, Postgres>,
    slot: ClaimedPoolSlot,
    claim_id: Uuid,
    template_release_id: Uuid,
    content_hash: String,
) -> Result<(PooledExecutionClaim, PooledExecutionPoolShape), ControllerError> {
    let pod_ip = slot
        .pod_ip
        .ok_or_else(|| ControllerError::internal("claimed pool slot is missing pod IP"))?;

    let vcpu = slot.vcpu.get().get();
    let memory_mib = u32::try_from(slot.memory.as_mib_floor()).map_err(|err| {
        ControllerError::internal(format!(
            "claimed pool slot memory_mib does not fit request model: {err}"
        ))
    })?;
    let pool_shape = PooledExecutionPoolShape::with_disk_mib(
        vcpu,
        memory_mib,
        slot.disk_mib,
        slot.disk_io_limits,
    );

    let active_node_id = resolve_pool_slot_node_id(tx, slot.k8s_node_name.as_deref())
        .await
        .map_err(|err| db_contention_error("failed to resolve pool node", &err))?;
    let pod_uid = slot
        .pod_uid
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PodUid::parse)
        .transpose()
        .map_err(|err| ControllerError::internal(format!("claimed pool slot pod_uid: {err}")))?;

    Ok((
        PooledExecutionClaim {
            claim_id,
            pool_slot_id: Some(slot.id),
            sandbox_id: slot.reserved_sandbox_id,
            content_hash,
            template_release_id: Some(template_release_id),
            pod_name: slot.pod_name,
            pod_uid,
            execution_generation: 1,
            pod_ip,
            vcpu: Some(vcpu),
            memory_mib: Some(memory_mib),
            disk_mib: slot.disk_mib,
            disk_io_limits: slot.disk_io_limits,
            active_node_id,
            route_identity: None,
        },
        pool_shape,
    ))
}

/// Read the slot a create was admitted against, under a row lock, and decide
/// from its row alone whether the create may adopt it.
///
/// Adoptable only when the slot reserving `sandbox_id` is the admitted slot,
/// is still `claimed` under the admitted claim id, lives in this namespace,
/// was built from the release the create resolved, and has the requested
/// shape. Pod liveness and node placement are the caller's to check.
///
/// Every other answer is about whether the pool still owns a pod under this
/// ID. A missing slot (collected by the stale-claimed sweep) or a `bound`
/// receipt (a bind committed and the sandbox was later retired) owns none.
/// Anything else does, and is taken out of the claimable inventory here so no
/// other create can bind this ID: a `ready` slot is fenced to an id-less
/// claim, and our own claim on a slot we will not adopt is invalidated. Both
/// leave a `claimed` row that the stale-claimed sweep collects, pod included,
/// because no sandbox row exists for the ID yet.
#[tracing::instrument(
    name = "substrate.k8s.pool.lookup_admitted_pooled_execution",
    skip_all,
    fields(
        sandbox_id = %sandbox_id,
        claim_id = %admitted.claim_id,
        result = tracing::field::Empty,
    )
)]
pub(super) async fn lookup_admitted_pooled_execution(
    pool: &PgPool,
    request: &PooledExecutionClaimRequest<'_>,
    admitted: AdmittedPoolClaim,
    sandbox_id: SandboxId,
    namespace: &str,
) -> Result<AdmittedPoolClaimLookup, ControllerError> {
    let span = tracing::Span::current();
    let mut tx = pool.begin().await.map_err(|err| {
        db_contention_error("failed to start admitted pool claim transaction", &err)
    })?;
    let reserved = WarmPoolStore::lock_reserved_slot(&mut tx, sandbox_id)
        .await
        .map_err(|err| pool_store_error("failed to read admitted pool slot", &err))?;
    let (lookup, result) = match reserved {
        None => (AdmittedPoolClaimLookup::Released, "slot_missing"),
        Some(ReservedPoolSlot {
            state: PoolSlotState::Bound,
            ..
        }) => (AdmittedPoolClaimLookup::Released, "slot_bound"),
        Some(reserved)
            if reserved.state == PoolSlotState::Claimed
                && reserved.claim_id == Some(admitted.claim_id) =>
        {
            if admitted_slot_matches_request(&reserved, request, admitted, namespace) {
                let (claim, pool_shape) = pooled_claim_from_slot(
                    &mut tx,
                    reserved.slot,
                    admitted.claim_id,
                    reserved.template_release_id,
                    reserved.content_hash,
                )
                .await?;
                (
                    AdmittedPoolClaimLookup::Adoptable {
                        claim: Box::new(claim),
                        pool_shape,
                    },
                    "claimed",
                )
            } else {
                WarmPoolStore::invalidate_claimed_slot_in(&mut tx, admitted.claim_id, sandbox_id)
                    .await
                    .map_err(|err| {
                        pool_store_error("failed to invalidate admitted pool slot", &err)
                    })?;
                (AdmittedPoolClaimLookup::ReleasePending, "slot_mismatch")
            }
        }
        Some(reserved) if reserved.state == PoolSlotState::Ready => {
            WarmPoolStore::fence_ready_slot(&mut tx, reserved.slot.id)
                .await
                .map_err(|err| pool_store_error("failed to fence admitted pool slot", &err))?;
            (AdmittedPoolClaimLookup::ReleasePending, "slot_ready")
        }
        Some(_) => (AdmittedPoolClaimLookup::ReleasePending, "slot_held"),
    };
    tx.commit().await.map_err(|err| {
        db_contention_error("failed to commit admitted pool claim transaction", &err)
    })?;
    span.record("result", result);
    Ok(lookup)
}

fn admitted_slot_matches_request(
    reserved: &ReservedPoolSlot,
    request: &PooledExecutionClaimRequest<'_>,
    admitted: AdmittedPoolClaim,
    namespace: &str,
) -> bool {
    let slot = &reserved.slot;
    let shape_matches = match (request.vcpu, request.memory_mib) {
        (Some(vcpu), Some(memory_mib)) => {
            slot.vcpu.get().get() == vcpu
                && u32::try_from(slot.memory.as_mib_floor()).ok() == Some(memory_mib)
        }
        (None, None) => true,
        _ => false,
    };
    slot.id == admitted.pool_slot_id
        && slot.pod_namespace == namespace
        && slot.pod_ip.is_some()
        && request
            .template_release_id
            .is_none_or(|release_id| release_id == reserved.template_release_id)
        // Either spelling of the digest: a create resolves a digest stored
        // bare to its canonical `sha256:` form, and the slot records the
        // release's stored spelling.
        && request.content_hash.is_none_or(|content_hash| {
            content_hash.trim_prefix("sha256:") == reserved.content_hash.trim_prefix("sha256:")
        })
        && shape_matches
}

/// Claim a ready slot for a create being admitted, inside the caller's
/// transaction.
///
/// The same any-shape claim an ordinary default-shape create makes
/// ([`WarmPoolStore::claim_ready_slot_for_release`]): this namespace, this
/// release, an enabled plaintext config. It only reserves the slot. Pod
/// liveness, node placement, and the bind are the executor's, once the create
/// runs ([`lookup_admitted_pooled_execution`]); a slot that fails them there
/// is collected by the pool and the create builds cold under the same ID.
#[tracing::instrument(
    name = "substrate.k8s.pool.claim_admission_pool_slot",
    skip_all,
    fields(result = tracing::field::Empty)
)]
pub(super) async fn claim_admission_pool_slot(
    tx: &mut Transaction<'_, Postgres>,
    template_release_id: Uuid,
    namespace: &str,
) -> Result<Option<AdmissionPoolClaim>, ControllerError> {
    let claim_id = Uuid::now_v7();
    let slot = WarmPoolStore::claim_ready_slot_for_release(
        tx,
        template_release_id,
        namespace,
        claim_id,
        false,
    )
    .await
    .map_err(|err| pool_store_error("failed to claim pool slot at admission", &err))?;
    tracing::Span::current().record("result", if slot.is_some() { "claimed" } else { "miss" });
    Ok(slot.map(|slot| AdmissionPoolClaim {
        claim: AdmittedPoolClaim {
            claim_id,
            pool_slot_id: slot.id,
        },
        sandbox_id: slot.reserved_sandbox_id,
    }))
}

/// Whether the slot reserving `sandbox_id` is a `cleanup_pending` work item:
/// a failed adoption whose sandbox the warm-pool reconciler will tear down.
pub(super) async fn admitted_pool_cleanup_pending(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<bool, ControllerError> {
    WarmPoolStore::new(pool.clone())
        .reserved_slot_cleanup_pending(sandbox_id)
        .await
        .map_err(|err| pool_store_error("failed to read admitted pool slot cleanup", &err))
}

fn pool_shape_from_domain(assignment: PoolAssignment) -> PooledExecutionPoolShape {
    PooledExecutionPoolShape::with_disk_mib(
        assignment.vcpu.get().get(),
        u32::try_from(assignment.memory.as_mib_floor())
            .expect("validated pool-config memory_mib storage fits u32"),
        assignment.disk_mib,
        assignment.disk_io_limits,
    )
}

async fn claim_ready_pool_slot(
    tx: &mut Transaction<'_, Postgres>,
    request: &PooledExecutionClaimRequest<'_>,
    template_release_id: Uuid,
    namespace: &str,
    claim_id: Uuid,
) -> Result<(Option<ClaimedPoolSlot>, Option<PooledExecutionPoolShape>), ControllerError> {
    let (slot, miss_shape) = match (request.vcpu, request.memory_mib) {
        (Some(vcpu), Some(memory_mib)) => {
            let pool_vcpu = VcpuCount::try_from_u32(vcpu).map_err(|error| {
                ControllerError::bad_request(format!("invalid vcpu for Kubernetes pool: {error}"))
            })?;
            let pool_memory = MemorySize::mib(u64::from(memory_mib)).map_err(|error| {
                ControllerError::bad_request(format!(
                    "invalid memory_mib for Kubernetes pool: {error}"
                ))
            })?;
            let shape = WarmPoolStore::preferred_enabled_shape_for_release(
                tx,
                template_release_id,
                namespace,
                Some((pool_vcpu, pool_memory)),
                request.volume_encryption,
            )
            .await
            .map_err(|err| pool_store_error("failed to resolve pool shape", &err))?;
            let Some(shape) = shape else {
                return Ok((None, None));
            };
            (
                WarmPoolStore::claim_ready_slot(
                    tx,
                    template_release_id,
                    pool_vcpu,
                    pool_memory,
                    namespace,
                    claim_id,
                    request.volume_encryption,
                )
                .await,
                Some(pool_shape_from_domain(shape)),
            )
        }
        (None, None) => {
            let slot = WarmPoolStore::claim_ready_slot_for_release(
                tx,
                template_release_id,
                namespace,
                claim_id,
                request.volume_encryption,
            )
            .await
            .map_err(|err| pool_store_error("failed to claim pool slot", &err))?;
            if slot.is_some() {
                return Ok((slot, None));
            };
            let miss_shape = WarmPoolStore::preferred_enabled_shape_for_release(
                tx,
                template_release_id,
                namespace,
                None,
                request.volume_encryption,
            )
            .await
            .map(|shape| shape.map(pool_shape_from_domain))
            .map_err(|err| pool_store_error("failed to resolve pool shape", &err))?;
            return Ok((None, miss_shape));
        }
        _ => {
            return Err(ControllerError::bad_request(
                "vcpu and memory_mib must be provided together for Kubernetes pool claims",
            ));
        }
    };
    slot.map(|slot| (slot, miss_shape))
        .map_err(|err| pool_store_error("failed to claim pool slot", &err))
}

pub(super) async fn persist_pooled_execution_claim(
    tx: &mut Transaction<'_, Postgres>,
    _request: &PooledExecutionClaimRequest<'_>,
    claim: &PooledExecutionClaim,
) -> Result<(), ControllerError> {
    let route_activation_id = pooled_route_activation_id(claim)?;

    // Stamp the slot bound first: it row-locks the slot for the rest of the
    // transaction and is the marker the stale-claimed sweep uses to tell a
    // committed handoff from one that died between claim and bind.
    let bound = WarmPoolStore::bind_claimed_slot(tx, claim.claim_id, claim.sandbox_id)
        .await
        .map_err(|err| pool_store_error("failed to stamp pool slot bound", &err))?;
    if !bound {
        return Err(ControllerError::internal(
            "claimed pool slot missing during bind stamp",
        ));
    }

    let source = WarmPoolStore::bind_source(tx, claim.claim_id, claim.sandbox_id)
        .await
        .map_err(|err| pool_store_error("failed to read pool slot bind source", &err))?
        .ok_or_else(|| {
            ControllerError::internal("claimed pool slot missing during execution bind")
        })?;

    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (
             sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             k8s_node_name, state, active, route_activation_id, pod_materialized_at
         )
         VALUES (
             $1, 1, $2, $3, $4, $5::inet, $6, 'running', TRUE, $7,
             CASE WHEN NULLIF(BTRIM($4::TEXT), '') IS NOT NULL THEN NOW() END
         )"
    ))
    .bind(claim.sandbox_id)
    .bind(&source.pod_name)
    .bind(&source.pod_namespace)
    .bind(source.pod_uid.as_deref())
    .bind(source.pod_ip.as_deref())
    .bind(source.k8s_node_name.as_deref())
    .bind(route_activation_id)
    .execute(&mut **tx)
    .await
    .map_err(|err| db_contention_error("failed to bind pool execution", &err))?;

    Ok(())
}

/// Returns whether this call took the slot out; `false` when it was no
/// longer claimed under this claim (already released, invalidated or bound).
pub(super) async fn invalidate_claimed_pool_slot(
    pool: &PgPool,
    claim: &PooledExecutionClaim,
) -> Result<bool, ControllerError> {
    WarmPoolStore::new(pool.clone())
        .invalidate_claimed_slot(claim.claim_id, claim.sandbox_id)
        .await
        .map_err(|err| pool_store_error("failed to invalidate claimed pool slot", &err))
}

pub(super) async fn mark_claimed_pool_slot_cleanup_pending(
    pool: &PgPool,
    claim: &PooledExecutionClaim,
) -> Result<(), ControllerError> {
    let mut tx = pool.begin().await.map_err(|err| {
        db_contention_error(
            "failed to start cleanup-pending pool slot transaction",
            &err,
        )
    })?;
    let marked =
        WarmPoolStore::mark_claimed_slot_cleanup_pending(&mut tx, claim.claim_id, claim.sandbox_id)
            .await
            .map_err(|err| {
                pool_store_error("failed to mark claimed pool slot cleanup_pending", &err)
            })?;
    if !marked {
        return Err(ControllerError::internal(
            "claimed pool slot missing during cleanup-pending mark",
        ));
    }
    tx.commit().await.map_err(|err| {
        db_contention_error(
            "failed to commit cleanup-pending pool slot transaction",
            &err,
        )
    })?;
    Ok(())
}

pub(super) async fn release_claimed_pool_slot(
    pool: &PgPool,
    claim: &PooledExecutionClaim,
) -> Result<(), ControllerError> {
    WarmPoolStore::new(pool.clone())
        .release_claimed_slot(claim.claim_id, claim.sandbox_id)
        .await
        .map_err(|err| pool_store_error("failed to release claimed pool slot", &err))?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct PoolTemplateReleaseRow {
    id: Uuid,
    content_hash: String,
}

/// Resolve the template release a pool claim or config upsert targets: the
/// release matching the requested content hash, under either spelling, when
/// one is pinned, otherwise the release of the digest tagged `latest` (the tag
/// may sit on the other spelling's original when it predates the shared rule),
/// otherwise the newest release of the template. Only digest originals are candidates
/// (`pplx_space_template_release_original::digest_original`, the release the
/// create path and the warm-pool store resolve the digest to): a twin bound to
/// another environment is registered after its original and would otherwise
/// be the newest, and the other spelling's original of a digest would hold
/// slots no create claims.
/// Template-domain logic; deliberately not part of the warm-pool store.
async fn resolve_pool_template_release(
    tx: &mut Transaction<'_, Postgres>,
    template_name: &str,
    content_hash: Option<&str>,
) -> Result<Option<PoolTemplateReleaseRow>, ControllerError> {
    /// Built once: every input is a literal or a bind placeholder.
    static QUERY: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT r.id, r.hash AS content_hash
             FROM template_releases r
             WHERE r.template_name = $1
               AND ($2::TEXT IS NULL
                    OR r.hash IN (replace($2, 'sha256:', ''),
                                  'sha256:' || replace($2, 'sha256:', '')))
               AND {digest_original}
             ORDER BY EXISTS (
                          SELECT 1
                          FROM template_release_tags t
                          JOIN template_releases tagged
                            ON tagged.id = t.template_release_id
                           AND tagged.template_name = t.template_name
                          WHERE t.template_name = r.template_name
                            AND t.tag = 'latest'
                            AND replace(tagged.hash, 'sha256:', '')
                              = replace(r.hash, 'sha256:', '')) DESC,
                      r.created_at DESC
             LIMIT 1",
            digest_original = digest_original("r"),
        ))
    });
    sqlx::query_as::<_, PoolTemplateReleaseRow>(sqlx::AssertSqlSafe(QUERY.as_str()))
        .bind(template_name)
        .bind(content_hash)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| db_contention_error("failed to resolve pool template release", &err))
}

/// The release `release_id` names, when it's a release of `template_name`
/// (and of `content_hash`, when given).
async fn named_pool_template_release(
    tx: &mut Transaction<'_, Postgres>,
    release_id: Uuid,
    template_name: &str,
    content_hash: Option<&str>,
) -> Result<Option<PoolTemplateReleaseRow>, ControllerError> {
    pplx_space_template_release_store::lookup::named_release(
        &mut **tx,
        release_id,
        template_name,
        content_hash,
    )
    .await
    .map(|release| {
        release.map(|release| PoolTemplateReleaseRow {
            id: release.id,
            content_hash: release.content_hash,
        })
    })
    .map_err(|err| db_contention_error("failed to read the named pool template release", &err))
}

/// The worker registered under the slot pod's Kubernetes node name.
async fn resolve_pool_slot_node_id(
    tx: &mut Transaction<'_, Postgres>,
    node_name: Option<&str>,
) -> Result<Option<NodeId>, sqlx::Error> {
    let Some(node_name) = node_name else {
        return Ok(None);
    };
    node_lookup::worker_for_k8s_node_name(&mut **tx, node_name).await
}

async fn fetch_pool_config(
    store: &K8sStore,
    id: Uuid,
) -> Result<Option<ClusterPoolConfigResponse>, ControllerError> {
    let row = WarmPoolStore::new(store.pool().clone())
        .config_with_counts(id)
        .await
        .map_err(|err| pool_store_error("failed to load cluster pool config", &err))?;

    row.map(TryInto::try_into).transpose()
}

impl TryFrom<PoolConfigWithCounts> for ClusterPoolConfigResponse {
    type Error = ControllerError;

    fn try_from(row: PoolConfigWithCounts) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            template_release_id: row.template_release_id,
            template_name: row.template_name,
            content_hash: row.content_hash,
            vcpu: i32_to_u32(row.vcpu, "vcpu")?,
            memory_mib: i32_to_u32(row.memory_mib, "memory_mib")?,
            disk_mib: row
                .disk_mib
                .map(|value| i32_to_u32(value, "disk_mib"))
                .transpose()?,
            target_ready_slots: i32_to_u32(row.target_ready_slots, "target_ready_slots")?,
            refill_per_tick: i32_to_u32(row.refill_per_tick, "refill_per_tick")?,
            max_warming_slots: i32_to_u32(row.max_warming_slots, "max_warming_slots")?,
            namespace: row.namespace,
            enabled: row.enabled,
            encryptable: row.encryptable,
            managed_by: row.managed_by,
            warming_slots: i64_count_to_u32(
                row.warming_slots.unwrap_or_default(),
                "warming_slots",
            )?,
            ready_slots: i64_count_to_u32(row.ready_slots.unwrap_or_default(), "ready_slots")?,
            claimed_slots: i64_count_to_u32(
                row.claimed_slots.unwrap_or_default(),
                "claimed_slots",
            )?,
            created_at: row
                .created_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            updated_at: row
                .updated_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
        })
    }
}

pub(super) fn pool_eligible_for_request(
    boot_mode: super::super::CreateBootMode,
    backend: &str,
    wait_until_ready: bool,
    disk_mib: Option<u32>,
    file_changes_enabled: bool,
    implicit_wake_enabled: bool,
) -> bool {
    super::super::default_pool_eligible_for_request(
        boot_mode,
        backend,
        wait_until_ready,
        disk_mib,
        file_changes_enabled,
        implicit_wake_enabled,
    )
}

#[cfg(test)]
mod tests;
