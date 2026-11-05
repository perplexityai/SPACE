//! The vCPU and guest memory a sandbox is requested to run with, after
//! template and warm-pool defaults are applied.
//!
//! | Metric | Type | Value |
//! | --- | --- | --- |
//! | `sandbox.api.requested.vcpu` | distribution | vCPUs the sandbox will run with |
//! | `sandbox.api.requested.memory_bytes` | distribution | Guest memory in bytes (`memory_mib * 1048576`) |
//!
//! Both carry the same tags:
//!
//! - `kind`: `create` (cold and warm-pool creates), `activate` (a lifecycle
//!   action that activates a `created`, `stopped` or `suspended` sandbox) or
//!   `resume` (a resume requested from the `paused` lifecycle). Resuming an
//!   existing resident runtime spends no cluster admission; runtime recovery
//!   may require admission and can be rejected. Not `op`, which on other
//!   `sandbox.api.*` metrics names the API route.
//! - `surface`: `native` or `ecw`, the API the call arrived on.
//! - `admission`: `accepted`, or `rejected` when cluster activation admission
//!   refused the call ([`AdmissionDecision`], the cluster limiter's own
//!   vocabulary). Other failures emit nothing.
//! - `template`: the resolved template name for a create, the sandbox row's
//!   template otherwise (`unknown` when the row names none).
//! - `defaulted`: creates only. `false` when the request named both sizes;
//!   `true` when a build shape, the platform default, a warm-pool config or a
//!   warm-pool policy filled them. Warm-pool claims are always `true`.
//! - `client_id`: the request's bounded caller ([`request_client_id`]).
//!
//! One sample per create, activation or resume call, so a caller that
//! retries or several callers that wake the same sandbox each add one; a
//! caller that joins another's in-flight activation adds none. A cold create
//! records at its admission decision; an activation or resume records only
//! when it succeeds or admission refuses it. A call whose size is unknown
//! emits nothing, as do forks. `count:` on either distribution is the call
//! count.

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
