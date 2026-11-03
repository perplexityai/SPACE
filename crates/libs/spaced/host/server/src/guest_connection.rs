//! Capabilities belong to the transport used for dispatch, not a readiness cache.
use super::*;
use pplx_space_common_core::types::guest_image::QualifiedGuestAgent;
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
    policy: QualifiedGuestAgent,
    meta: BridgeGuestClient<tonic::transport::Channel>,
    platform: Arc<tokio::sync::OnceCell<GuestPlatformObservation>>,
}

impl<C> OperationClient<C> {
    pub(super) fn new(
        client: C,
        channel: tonic::transport::Channel,
        policy: QualifiedGuestAgent,
    ) -> Self {
        Self {
            client,
            policy,
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
        match self.policy {
            QualifiedGuestAgent::LinuxV1 => {}
            QualifiedGuestAgent::Absent | QualifiedGuestAgent::WindowsV1 => {
                return Err(guest_capability_unsupported_status(
                    GuestCapability::PlatformOperations,
                    "image has no qualified guest operations",
                ));
            }
        }
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
                GuestPlatformObservation::parse(&response, self.policy)
            })
            .await?;
        for &operation in required {
            platform.require(operation, rpc)?;
        }
        Ok(())
    }
}

impl GuestPlatformObservation {
    pub(super) fn parse(
        response: &BridgeGuestWaitReadyResponse,
        policy: QualifiedGuestAgent,
    ) -> Result<Self, Status> {
        pplx_space_spaced_common::validate_spaced_guest_compatibility(
            Some(&response.version),
            Some(&response.compatibility_family),
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let expected = match policy {
            QualifiedGuestAgent::LinuxV1 => GuestOs::Linux,
            QualifiedGuestAgent::WindowsV1 => GuestOs::Windows,
            QualifiedGuestAgent::Absent => {
                return Err(Status::failed_precondition(
                    "image has no qualified guest agent",
                ));
            }
        };
        match &response.platform {
            Some(wire) => {
                let mut wire = wire.clone();
                // Peer claims cannot widen the trusted image's qualification.
                match policy {
                    QualifiedGuestAgent::LinuxV1 => {}
                    QualifiedGuestAgent::WindowsV1 | QualifiedGuestAgent::Absent => {
                        wire.operations.clear()
                    }
                }
                pplx_space_spaced_common::platform::GuestPlatform::parse(wire, expected)
                    .map(Self::Explicit)
            }
            None if expected == GuestOs::Linux
                && !pplx_space_spaced_common::spaced_guest_requires_platform_descriptor(
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
