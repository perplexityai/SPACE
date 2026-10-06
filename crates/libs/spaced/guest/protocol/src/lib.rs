//! Guest bridge and filesystem-worker wire contracts, including process,
//! filesystem, metadata, and networking services, plus their shared operations.
//! Host/control protocols and transport connectors belong to spaced-common.

pub mod proto {
    pub mod operations {
        tonic::include_proto!("space.spaced.operations.v1");
    }
    pub mod bridge_guest {
        // Legacy stat/list messages remain part of the deployed wire contract.
        #![allow(deprecated)]
        tonic::include_proto!("space.spaced.bridge.guest.v1");
        pub use super::operations::*;
    }
    pub mod worker {
        tonic::include_proto!("space.spaced.worker.v1");
    }
}

pub mod filesystem_limits;
pub mod process_limits;
pub mod validate;
pub mod wire;
pub use validate::*;
pub use wire::*;

pub fn invalid_argument_status(error: impl std::fmt::Display) -> tonic::Status {
    tonic::Status::invalid_argument(error.to_string())
}

impl std::fmt::Debug for proto::operations::SendInputRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SendInputRequest")
            .field("process_id", &self.process_id)
            .field("data", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests;

pub mod admission_queue;
