//! Capabilities belong to the transport used for dispatch, not a readiness cache.
use super::*;
use pplx_space_common_grpc::{GuestCapability, guest_capability_unsupported_status};
use pplx_space_spaced_common::proto::platform::GuestOs;

#[derive(Clone, Copy)]
pub(super) enum Requirement {
    Probe,
    Teardown,
    Operations(&'static [GuestOperation]),
    InteractiveProcessControl(&'static str),
}

#[derive(Clone)]
pub(super) struct OperationClient<C> {
    pub(super) client: C,
    meta: BridgeGuestClient<tonic::transport::Channel>,
    platform: Arc<tokio::sync::OnceCell<GuestPlatformObservation>>,
}

impl<C> OperationClient<C> {
    pub(super) fn new(client: C, channel: tonic::transport::Channel) -> Self {
        Self {
            client,
            meta: BridgeGuestClient::new(channel),
            platform: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    pub(super) async fn authorize(&self, required: Requirement) -> Result<(), Status> {
        let (required, rpc) = match required {
            Requirement::Probe | Requirement::Teardown => return Ok(()),
            Requirement::Operations(operations) => (operations, None),
            Requirement::InteractiveProcessControl(rpc) => (
                &[
                    GuestOperation::Process,
                    GuestOperation::InteractiveProcessControl,
                ][..],
                Some(rpc),
            ),
        };
        let platform = self
            .platform
            .get_or_try_init(|| async {
                let mut request = Request::new(BridgeGuestWaitReadyRequest {
                    timeout: Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 1_000_000,
                    }),
                    host_contract_revision:
                        pplx_space_spaced_common::platform::HOST_CONTRACT_REVISION,
                });
                request.set_timeout(Duration::from_secs(1));
                let response = self.meta.clone().wait_ready(request).await?.into_inner();
                GuestPlatformObservation::parse(&response)
            })
            .await?;
        for &operation in required {
            platform.require(operation, rpc)?;
        }
        Ok(())
    }
}

impl GuestPlatformObservation {
    pub(super) fn parse(response: &BridgeGuestWaitReadyResponse) -> Result<Self, Status> {
        pplx_space_spaced_common::validate_spaced_guest_compatibility(
            Some(&response.version),
            Some(&response.compatibility_family),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        match &response.platform {
            Some(wire) => pplx_space_spaced_common::platform::GuestPlatform::parse(
                wire.clone(),
                GuestOs::Linux,
            )
            .map(Self::Explicit),
            None if !pplx_space_spaced_common::spaced_guest_requires_platform_descriptor(
                &response.version,
            )
            .map_err(|error| Status::failed_precondition(error.to_string()))? =>
            {
                Ok(Self::LegacyLinux {
                    version: response.version.clone(),
                })
            }
            None => Err(Status::failed_precondition(
                "guest release requires a platform descriptor",
            )),
        }
    }

    pub(super) fn require(
        &self,
        operation: GuestOperation,
        rpc: Option<&str>,
    ) -> Result<(), Status> {
        let supported = match self {
            Self::Explicit(platform) => platform.supports(operation),
            Self::LegacyLinux { version } => match operation {
                GuestOperation::Unspecified => false,
                GuestOperation::InteractiveProcessControl => {
                    let supported = pplx_space_spaced_common::spaced_guest_supports_interactive_process_control(version)
                        .map_err(|error| Status::failed_precondition(error.to_string()))?;
                    if !supported {
                        return Err(guest_capability_unsupported_status(
                            GuestCapability::InteractiveProcessControl,
                            format!(
                                "guest spaced-svc version {version} does not support {}; rebuild or restore the sandbox with spaced guest version 0.1.2 or newer",
                                rpc.unwrap_or("interactive process control")
                            ),
                        ));
                    }
                    true
                }
                GuestOperation::Process
                | GuestOperation::ProcessEnvironment
                | GuestOperation::Files
                | GuestOperation::FileWatch
                | GuestOperation::Flush
                | GuestOperation::Resume
                | GuestOperation::LoopbackForward => true,
            },
        };
        if supported {
            Ok(())
        } else {
            Err(guest_capability_unsupported_status(
                GuestCapability::PlatformOperations,
                format!("guest does not support {}", operation.as_str_name()),
            ))
        }
    }
}

#[cfg(test)]
mod tests;
