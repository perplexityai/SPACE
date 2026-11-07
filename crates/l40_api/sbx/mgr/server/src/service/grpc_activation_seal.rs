//! `SealActivations`, `UnsealActivations` and `ActivationSealState`: the
//! node-local control of the activation seal (see
//! [`crate::activation_seal`]).

use std::time::Duration;

use super::*;
use crate::activation_seal::{
    ActivationSealStatus, InFlightActivation, InFlightActivationKind, Sealed,
};
use crate::proto::{
    ActivationSealStateRequest, ActivationSealStateResponse, SealActivationsRequest,
    SealActivationsResponse, UnsealActivationsRequest, UnsealActivationsResponse,
};

impl SandboxManagerService {
    pub(super) async fn seal_activations(
        &self,
        request: Request<SealActivationsRequest>,
    ) -> Result<Response<SealActivationsResponse>, Status> {
        self.authorize_peer(&request, "SealActivations", ALLOW_ACTIVATION_SEAL_OPERATOR)?;
        self.require_readable_activation_seal()?;
        let SealActivationsRequest {
            epoch,
            wait_timeout,
        } = request.into_inner();
        let wait = match wait_timeout {
            None => Duration::ZERO,
            Some(wait) => Duration::try_from(wait).map_err(|error| {
                Status::invalid_argument(format!("invalid wait_timeout: {error}"))
            })?,
        };
        // `run` cancels the token as it shuts down, which ends the wait.
        let outcome = self
            .activation_seal
            .seal_and_wait(&self.db, epoch, wait, &self.shutdown_token)
            .await?;
        // A seal can be replaced or cleared by another call during the wait;
        // answer the state the wait ended in.
        Ok(Response::new(SealActivationsResponse {
            status: Some(status_to_wire(outcome.status)),
            quiesced: outcome.quiesced,
        }))
    }

    pub(super) async fn unseal_activations(
        &self,
        request: Request<UnsealActivationsRequest>,
    ) -> Result<Response<UnsealActivationsResponse>, Status> {
        self.authorize_peer(
            &request,
            "UnsealActivations",
            ALLOW_ACTIVATION_SEAL_OPERATOR,
        )?;
        self.require_readable_activation_seal()?;
        self.activation_seal
            .unseal(&self.db, request.into_inner().epoch)?;
        Ok(Response::new(UnsealActivationsResponse {
            status: Some(status_to_wire(self.activation_seal.status())),
        }))
    }

    pub(super) async fn activation_seal_state(
        &self,
        request: Request<ActivationSealStateRequest>,
    ) -> Result<Response<ActivationSealStateResponse>, Status> {
        self.authorize_peer(&request, "ActivationSealState", ALLOW_SANDBOX_OBSERVER)?;
        self.require_readable_activation_seal()?;
        Ok(Response::new(ActivationSealStateResponse {
            status: Some(status_to_wire(self.activation_seal.status())),
        }))
    }

    /// Why the seal could not be read at startup; `run` refuses to serve
    /// then, so only a service built some other way can answer with it.
    pub fn activation_seal_load_error(&self) -> Option<String> {
        self.activation_seal.load_error()
    }

    fn require_readable_activation_seal(&self) -> Result<(), Status> {
        match self.activation_seal.load_error() {
            Some(error) => Err(Status::internal(format!(
                "activation seal unreadable: {error}"
            ))),
            None => Ok(()),
        }
    }
}

fn status_to_wire(status: ActivationSealStatus) -> crate::proto::ActivationSealStatus {
    crate::proto::ActivationSealStatus {
        sealed_epoch: status.sealed.map(|sealed| sealed.epoch),
        sealed_since: status.sealed.map(sealed_since_to_wire),
        activations_admitted: status.activations_admitted,
        in_flight: status
            .in_flight
            .into_iter()
            .map(in_flight_to_wire)
            .collect(),
    }
}

fn sealed_since_to_wire(sealed: Sealed) -> prost_types::Timestamp {
    // Below `i64::MAX` milliseconds by the store's write.
    let since_ms = i64::try_from(sealed.since_ms).unwrap_or(i64::MAX);
    prost_types::Timestamp {
        seconds: since_ms.div_euclid(1_000),
        nanos: i32::try_from(since_ms.rem_euclid(1_000) * 1_000_000).unwrap_or(0),
    }
}

fn in_flight_to_wire(in_flight: InFlightActivation) -> crate::proto::InFlightActivation {
    crate::proto::InFlightActivation {
        kind: in_flight_kind_to_wire(in_flight.kind) as i32,
        sandbox_id: in_flight.sandbox_id,
        age: prost_types::Duration::try_from(in_flight.age).ok(),
    }
}

fn in_flight_kind_to_wire(kind: InFlightActivationKind) -> crate::proto::InFlightActivationKind {
    use crate::proto::InFlightActivationKind as Wire;
    match kind {
        InFlightActivationKind::Cold => Wire::Cold,
        InFlightActivationKind::Restore => Wire::Restore,
        InFlightActivationKind::ResidentResume => Wire::ResidentResume,
        InFlightActivationKind::Checkpoint => Wire::Checkpoint,
        InFlightActivationKind::StopFlush => Wire::StopFlush,
    }
}

#[cfg(test)]
mod tests;
