//! Node-manager wire contract: the generated `space.node.v1` protobuf types
//! and service stubs, plus the domain types and conversions those messages
//! carry. It is its own crate so code that speaks the contract, without
//! dialing node-manager's socket, need not link the client's connection and
//! retry code; `pplx-space-node-mgr-client` re-exports everything here.

pub mod proto {
    tonic::include_proto!("space.node.v1");
}

pub mod execution_fence;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheDiskSchedulingEffect {
    Open,
    WarnOnly,
    Full,
    Unavailable,
}

impl proto::CacheDiskStatus {
    pub fn admission_state_enum(&self) -> proto::CacheDiskAdmissionState {
        proto::CacheDiskAdmissionState::try_from(self.admission_state).unwrap_or_default()
    }

    pub fn scheduling_effect(&self) -> CacheDiskSchedulingEffect {
        match self.admission_state_enum() {
            proto::CacheDiskAdmissionState::Ready => CacheDiskSchedulingEffect::Open,
            proto::CacheDiskAdmissionState::Warning => CacheDiskSchedulingEffect::WarnOnly,
            proto::CacheDiskAdmissionState::Full => CacheDiskSchedulingEffect::Full,
            proto::CacheDiskAdmissionState::Unavailable
            | proto::CacheDiskAdmissionState::Unspecified => CacheDiskSchedulingEffect::Unavailable,
        }
    }
}
