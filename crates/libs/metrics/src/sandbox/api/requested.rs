//! Requested vCPU and guest-memory distributions after defaults, with memory
//! emitted in bytes. Each recorded call adds one sample to each distribution.
//!
//! Tags: `kind` (create/activate/resume), `surface`, `admission`, `template` and
//! request-scoped `client_id`. Creates also carry `defaulted`, false only when
//! the request supplied both sizes; warm-pool claims are always true.
//!
//! Callers record cold creates at admission, and activations/resumes on success
//! or admission rejection. Skip forks, unknown sizes and joined activations.
//! Resident resumes need no admission; runtime recovery may require it.

use crate::emit::{distribution, tag};

use super::{SandboxApiSurface, request_client_id};
use crate::sandbox::admission::AdmissionDecision;

const BYTES_PER_MIB: u64 = 1_048_576;

metric_vocabulary!(
    /// The kind of call that requested the size.
    RequestedKind {
        /// A cold or warm-pool create.
        Create => "create",
        /// A start, wake or restore of a dormant sandbox.
        Activate => "activate",
        /// A resume requested from the paused lifecycle.
        Resume => "resume",
    }
);

/// One create, activation or resume's requested size.
pub struct RequestedSize<'a> {
    pub kind: RequestedKind,
    pub surface: SandboxApiSurface,
    pub admission: AdmissionDecision,
    pub template: &'a str,
    pub vcpu: u32,
    pub memory_mib: u32,
    /// `None` for an activation or resume, which omits the tag.
    pub defaulted: Option<bool>,
}

/// Record the vCPU and memory one create, activation or resume requested. Reads
/// `client_id` from the request scope, so call it on the request's task.
pub fn requested(sample: RequestedSize<'_>) {
    let mut tags = vec![
        tag("kind", sample.kind),
        tag("surface", sample.surface),
        tag("admission", sample.admission),
        tag("template", sample.template),
        tag("client_id", request_client_id().as_str()),
    ];
    if let Some(defaulted) = sample.defaulted {
        tags.push(tag("defaulted", defaulted));
    }
    distribution("sandbox.api.requested.vcpu", f64::from(sample.vcpu), &tags);
    distribution(
        "sandbox.api.requested.memory_bytes",
        (u64::from(sample.memory_mib) * BYTES_PER_MIB) as f64,
        &tags,
    );
}
