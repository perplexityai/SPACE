//! Maps create, activation and resume outcomes onto the requested vCPU and
//! memory distributions (`crate::metrics::sandbox::api::requested_size`).
//! The emit sites stay one call each; what counts and how it is tagged is
//! decided here.

use pplx_space_events::CreateShapeSource;
use pplx_space_sandbox_metrics::admission::AdmissionDecision;

use super::ControllerError;
use super::queries::stored_resource;
use crate::controllers::substrate::SandboxResources;
use crate::metrics::sandbox::api::{
    RequestedKind, RequestedSize, SandboxApiSurface, requested_size,
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

/// Record a cold create's resolved size. `source` is `None` for a create
/// whose shape the API did not resolve, such as a direct create, which emits
/// nothing.
pub(super) fn record_create(
    surface: SandboxApiSurface,
    admission: AdmissionDecision,
    template: &str,
    resources: &SandboxResources,
    source: Option<CreateShapeSource>,
) {
    let Some(source) = source else {
        return;
    };
    emit(
        RequestedKind::Create,
        surface,
        admission,
        template,
        create_size(resources),
        Some(defaulted(source)),
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

/// Record a warm-pool claim's size, read from the claimed sandbox row. The
/// pool config chose that size, so it is always defaulted, and a claim spends
/// no cluster admission, so it is always accepted.
pub(super) fn record_pool_claim(surface: SandboxApiSurface, template: &str, sandbox: &Sandbox) {
    emit(
        RequestedKind::Create,
        surface,
        AdmissionDecision::Accepted,
        template,
        effective(sandbox.vcpu, sandbox.memory_mib),
        Some(true),
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
    );
}

/// Emit one sample, or nothing when the size is unknown.
fn emit(
    kind: RequestedKind,
    surface: SandboxApiSurface,
    admission: AdmissionDecision,
    template: &str,
    size: Option<(u32, u32)>,
    defaulted: Option<bool>,
) {
    let Some((vcpu, memory_mib)) = size else {
        return;
    };
    requested_size(RequestedSize {
        kind,
        surface,
        admission,
        template,
        vcpu,
        memory_mib,
        defaulted,
    });
}

/// Whether anything other than the request chose the size.
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
