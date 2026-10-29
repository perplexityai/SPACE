//! A create's warm-pool step and, only when it ends cold, its activation
//! charge.
//!
//! Split from `create_with_sandbox_id_inner` so the order is testable against a
//! scripted substrate: every `SandboxController` create entry point runs on the
//! real substrate, and adopting a warm slot calls into the slot's pod.

use std::sync::{Arc, atomic::AtomicU64};

use pplx_space_common_core::types::{ClientKey, DiskIoLimits};
use pplx_space_events::CreateShapeSource;
use sqlx::PgPool;

use super::{
    ColdCreateShapeInput, CreatePathEmitter, PoolCreateObservation, PoolCreateResult,
    PooledCreateInput, resolve_cold_create_resources,
};
use crate::cluster_admission::{ClusterActivationClass, ClusterSandboxAdmission};
use crate::controllers::ControllerError;
use crate::controllers::sandbox::SandboxController;
use crate::controllers::sandbox::lifecycle::charge_cluster_activation;
use crate::controllers::substrate::{
    AdmittedPoolClaim, CreateBootMode, SandboxResources, SubstrateAdapter,
};
use crate::models::SandboxResponse;

/// Which pool path a create takes.
pub(super) enum PoolPath {
    /// The create was admitted against this slot; adopt it if
    /// `adoption_eligible`, or build cold under the slot's ID.
    Admitted {
        claim: AdmittedPoolClaim,
        adoption_eligible: bool,
    },
    /// A pin or a restore: never pooled, recorded with this observation.
    Bypass(PoolCreateObservation),
    /// Claim any ready slot that fits.
    Claim,
}

/// How a create left the warm-pool step.
pub(super) enum PoolStep {
    /// A warm-pool sandbox served the create, and no activation was charged:
    /// the slot's pod was already running.
    Served(Box<SandboxResponse>),
    /// The create builds cold. Its activation is already charged, after the
    /// cold shape resolved, so a create refused before then spends no token.
    Cold(ColdPlan),
}

/// What a cold create builds with.
pub(super) struct ColdPlan {
    pub(super) resources: SandboxResources,
    pub(super) disk_io_limits: DiskIoLimits,
    pub(super) shape_source: Option<CreateShapeSource>,
    /// Set when an admitted create builds cold instead: the slot's pod used
    /// this ID's generation 1.
    pub(super) skip_pool_reserved_generation: bool,
}

pub(super) struct PoolStepInput<'a> {
    pub(super) pool: &'a PgPool,
    pub(super) pooled: PooledCreateInput<'a>,
    pub(super) path: PoolPath,
    pub(super) cold_shape: ColdCreateShapeInput<'a>,
    pub(super) client_key: &'a ClientKey,
}

/// Run the create's warm-pool step, then charge a cold activation only when no
/// warm-pool sandbox served it.
///
/// A warm hit takes no token from the cluster bucket or the client's bucket:
/// both draw in the one `admit_activation` call below, which a hit returns
/// before.
pub(super) async fn warm_pool_step_then_charge<A: SubstrateAdapter + ?Sized>(
    substrate: &A,
    admission: Option<&ClusterSandboxAdmission>,
    input: PoolStepInput<'_>,
    create_path: &mut CreatePathEmitter<'_>,
    claim_duration_nanos: Arc<AtomicU64>,
) -> Result<PoolStep, ControllerError> {
    let PoolStepInput {
        pool,
        pooled,
        path,
        cold_shape,
        client_key,
    } = input;
    let boot_mode = cold_shape.boot_mode;
    let skip_pool_reserved_generation = matches!(path, PoolPath::Admitted { .. });
    let (response, observation) = match path {
        PoolPath::Admitted {
            claim,
            adoption_eligible,
        } => {
            let PoolCreateResult {
                response,
                observation,
            } = SandboxController::try_create_from_admitted_pool_claim_with_substrate(
                substrate,
                pooled,
                claim,
                adoption_eligible,
                claim_duration_nanos,
            )
            .await?;
            (response, observation)
        }
        PoolPath::Bypass(observation) => (None, observation),
        PoolPath::Claim => {
            let PoolCreateResult {
                response,
                observation,
            } = SandboxController::try_create_from_pool_with_substrate(
                substrate,
                pooled,
                claim_duration_nanos,
            )
            .await?;
            (response, observation)
        }
    };
    create_path.set_observation(observation);
    if let Some(response) = response {
        return Ok(PoolStep::Served(Box::new(response)));
    }
    if boot_mode == CreateBootMode::Pool {
        return Err(ControllerError::service_unavailable(
            "boot_mode=pool requested but no warm-pool sandbox is available",
        ));
    }
    let (resources, disk_io_limits, shape_source) =
        resolve_cold_create_resources(pool, cold_shape).await?;

    charge_cluster_activation(admission, ClusterActivationClass::Cold, client_key).await?;

    Ok(PoolStep::Cold(ColdPlan {
        resources,
        disk_io_limits,
        shape_source,
        skip_pool_reserved_generation,
    }))
}
