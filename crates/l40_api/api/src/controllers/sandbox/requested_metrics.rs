//! Maps create outcomes onto the requested vCPU and memory
//! distributions (`pplx_space_metrics::sandbox::api::requested`). The emit
//! sites stay one call each; what counts and how it is tagged is decided here.

use pplx_space_events::CreateShapeSource;
use pplx_space_metrics::sandbox::admission::AdmissionDecision;
use pplx_space_metrics::sandbox::api::{
    RequestedKind, RequestedSize, SandboxApiSurface, requested,
};

use super::ControllerError;
use crate::controllers::substrate::SandboxResources;
use crate::models::Sandbox;

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
        effective(resources.vcpu, resources.memory_mib),
        Some(defaulted(source)),
    );
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
    requested(RequestedSize {
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

/// The vCPU and memory a sandbox runs with, or `None` when either is unset
/// or not positive.
fn effective<T>(vcpu: Option<T>, memory_mib: Option<T>) -> Option<(u32, u32)>
where
    u32: TryFrom<T>,
{
    Some((positive(vcpu)?, positive(memory_mib)?))
}

fn positive<T>(value: Option<T>) -> Option<u32>
where
    u32: TryFrom<T>,
{
    value
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
}

#[cfg(test)]
mod tests;
