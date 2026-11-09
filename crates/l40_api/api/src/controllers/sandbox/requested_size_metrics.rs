//! Maps create, activation and resume outcomes onto the requested vCPU,
//! memory and disk distributions
//! (`crate::metrics::sandbox::api::requested_size`).
//! The emit sites stay one call each; what counts and how it is tagged is
//! decided here.

use pplx_space_events::CreateShapeSource;
use pplx_space_sandbox_metrics::admission::AdmissionDecision;

use super::ControllerError;
use super::queries::stored_resource;
use crate::controllers::substrate::SandboxResources;
use crate::metrics::sandbox::api::{
    RequestedCompute, RequestedKind, RequestedSize, SandboxApiSurface, requested_size,
};
use crate::models::Sandbox;

/// The template tag for a sandbox row that names none.
const UNKNOWN_TEMPLATE: &str = "unknown";

/// The admission tag for a call's result: accepted on success, rejected when
/// cluster activation admission refused it, and `None` (emit nothing) for
/// any other failure.
pub(super) fn admission<T>(result: &Result<T, ControllerError>) -> Option<AdmissionDecision> {
    match result {
        Ok(_) => Some(AdmissionDecision::Accepted),
        Err(error) if error.is_cluster_admission_rejection() => Some(AdmissionDecision::Rejected),
        Err(_) => None,
    }
}

/// Record a cold create's known disk size independently of its compute shape.
/// `source` gates only compute samples: a direct create does not resolve its
/// compute shape here but can still carry an explicit disk size.
pub(super) fn record_create(
    surface: SandboxApiSurface,
    admission: AdmissionDecision,
    template: &str,
    resources: &SandboxResources,
    source: Option<CreateShapeSource>,
) {
    emit(
        RequestedKind::Create,
        surface,
        admission,
        template,
        source.and_then(|_| create_size(resources)),
        source.map(defaulted),
        create_disk(resources),
    );
}

/// The size a cold create will store, or `None` when it cannot store it.
/// The create refuses an out-of-range size after admission, through this same
/// `stored_resource` bound, so that size must not be recorded either.
fn create_size(resources: &SandboxResources) -> Option<(u32, u32)> {
    let stored = |value, field| stored_resource(value, field).ok().flatten();
    effective(
        stored(resources.vcpu, "vcpu"),
        stored(resources.memory_mib, "memory_mib"),
    )
}

/// The positive rootfs size a cold create will store, under the same bound as
/// `create_size`. Skip unset, nonpositive, or unrepresentable values.
fn create_disk(resources: &SandboxResources) -> Option<u32> {
    positive(
        stored_resource(resources.disk_mib, "disk_mib")
            .ok()
            .flatten(),
    )
}

/// Record a warm-pool claim's size, read from the claimed sandbox row. The
/// pool config chose the compute size, so the compute samples are always
/// defaulted. A claim spends no cluster admission, so it is always accepted.
pub(super) fn record_pool_claim(surface: SandboxApiSurface, template: &str, sandbox: &Sandbox) {
    emit(
        RequestedKind::Create,
        surface,
        AdmissionDecision::Accepted,
        template,
        effective(sandbox.vcpu, sandbox.memory_mib),
        Some(true),
        positive(sandbox.disk_mib),
    );
}

/// Record an eligible activation or resume on success or admission rejection.
pub(super) fn record_lifecycle_action<T>(
    kind: Option<RequestedKind>,
    surface: SandboxApiSurface,
    sandbox: &Sandbox,
    result: &Result<T, ControllerError>,
) {
    let Some(kind) = kind else {
        return;
    };
    let Some(admission) = admission(result) else {
        return;
    };
    emit(
        kind,
        surface,
        admission,
        sandbox.template_id.as_deref().unwrap_or(UNKNOWN_TEMPLATE),
        effective(sandbox.vcpu, sandbox.memory_mib),
        None,
        positive(sandbox.disk_mib),
    );
}

/// Emit the known parts of one size independently: the vCPU and memory pair,
/// with `defaulted` on creates only, and disk without that tag.
/// Emit nothing when neither is known.
fn emit(
    kind: RequestedKind,
    surface: SandboxApiSurface,
    admission: AdmissionDecision,
    template: &str,
    size: Option<(u32, u32)>,
    defaulted: Option<bool>,
    disk_mib: Option<u32>,
) {
    if size.is_none() && disk_mib.is_none() {
        return;
    }
    requested_size(RequestedSize {
        kind,
        surface,
        admission,
        template,
        compute: size.map(|(vcpu, memory_mib)| RequestedCompute {
            vcpu,
            memory_mib,
            defaulted,
        }),
        disk_mib,
    });
}

/// Whether anything other than the request chose the vCPU and memory.
fn defaulted(source: CreateShapeSource) -> bool {
    match source {
        CreateShapeSource::Request => false,
        CreateShapeSource::PoolConfig
        | CreateShapeSource::Policy
        | CreateShapeSource::Build
        | CreateShapeSource::Default => true,
    }
}

/// The stored vCPU and memory a sandbox runs with, or `None` when either is
/// unset or not positive.
fn effective(vcpu: Option<i32>, memory_mib: Option<i32>) -> Option<(u32, u32)> {
    Some((positive(vcpu)?, positive(memory_mib)?))
}

fn positive(value: Option<i32>) -> Option<u32> {
    value
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

#[cfg(test)]
mod tests;
