//! Capabilities belong to the transport used for dispatch, not a readiness cache.
use super::*;
use pplx_space_common_core::types::guest_image::QualifiedGuestAgent;
use pplx_space_common_grpc::{GuestCapability, guest_capability_unsupported_status};
use pplx_space_spaced_common::proto::platform::GuestOs;

/// Readiness probes and teardown must work before operation negotiation.
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
            QualifiedGuestAgent::LinuxV1 | QualifiedGuestAgent::WindowsV1 => {}
            QualifiedGuestAgent::Absent => {
                return Err(guest_capability_unsupported_status(
                    GuestCapability::PlatformOperations,
                    "image has no qualified guest operations",
                ));
            }
        }
        let platform = self
            .platform
            .get_or_try_init(|| async {
                let request = Request::new(BridgeGuestWaitReadyRequest {
                    timeout: Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 1_000_000,
                    }),
                    host_contract_revision:
                        pplx_space_spaced_common::platform::HOST_CONTRACT_REVISION,
                });
                let response = negotiation_response(self.meta.clone().wait_ready(request))
                    .await?
                    .into_inner();
                GuestPlatformObservation::parse(&response, self.policy)
            })
            .await?;
        for &operation in required {
            platform.require(operation, rpc)?;
        }
        Ok(())
    }
}

// SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_8_1: capabilities shipped through 0.8.0.
// Keep this independent of what the current Linux guest advertises.
fn historical_linux_capabilities()
-> pplx_space_spaced_common::proto::platform::GuestPlatformDescriptor {
    use GuestOperation::*;
    pplx_space_spaced_common::proto::platform::GuestPlatformDescriptor {
        os: GuestOs::Linux as i32,
        operations: [
            Process,
            ProcessEnvironment,
            Files,
            FileWatch,
            Flush,
            Resume,
            LoopbackForward,
            InteractiveProcessControl,
        ]
        .map(|operation| operation as i32)
        .to_vec(),
    }
}

/// Only local pre-dispatch negotiation expiry becomes retryable. The enclosing
/// operation's budget/cancellation still bounds this future; peer cancellations
/// and failures after dispatch keep their original meaning.
async fn negotiation_response(
    response: impl std::future::Future<Output = Result<Response<BridgeGuestWaitReadyResponse>, Status>>,
) -> Result<Response<BridgeGuestWaitReadyResponse>, Status> {
    tokio::time::timeout(Duration::from_secs(1), response)
        .await
        .map_err(|_| {
            Status::deadline_exceeded("guest capability negotiation expired before dispatch")
        })?
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
        let mut interactive_upgrade_version = None;
        let wire = match &response.platform {
            Some(wire) => wire.clone(),
            // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_8_1: Linux through 0.8.0 may omit the descriptor.
            None if expected == GuestOs::Linux
                && !pplx_space_spaced_common::spaced_guest_requires_platform_descriptor(
                    &response.version,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))? =>
            {
                let mut wire = historical_linux_capabilities();
                // SPACED_COMPAT_REMOVE_WHEN_MIN_GUEST_0_1_2: preserve the operation-specific upgrade advice.
                if !pplx_space_spaced_common::spaced_guest_supports_interactive_process_control(
                    &response.version,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?
                {
                    wire.operations.retain(|operation| {
                        *operation != GuestOperation::InteractiveProcessControl as i32
                    });
                    interactive_upgrade_version = Some(response.version.clone());
                }
                wire
            }
            None => {
                return Err(Status::failed_precondition(
                    "guest release requires a platform descriptor",
                ));
            }
        };
        // Bound the received descriptor before filtering disallowed claims.
        let mut wire =
            pplx_space_spaced_common::platform::GuestPlatform::parse(wire, expected)?.wire();
        // Both explicit claims and version-derived capabilities obey image policy.
        wire.operations.retain(|raw| {
            GuestOperation::try_from(*raw).is_ok_and(|operation| {
                pplx_space_spaced_common::allowed_guest_operations(policy).contains(&operation)
            })
        });
        Ok(Self {
            platform: pplx_space_spaced_common::platform::GuestPlatform::parse(wire, expected)?,
            interactive_upgrade_version,
        })
    }

    pub(super) fn require(
        &self,
        operation: GuestOperation,
        rpc: Option<&str>,
    ) -> Result<(), Status> {
        if self.platform.supports(operation) {
            Ok(())
        } else {
            if operation == GuestOperation::InteractiveProcessControl
                && let Some(version) = &self.interactive_upgrade_version
            {
                return Err(guest_capability_unsupported_status(
                    GuestCapability::InteractiveProcessControl,
                    format!(
                        "guest spaced-svc version {version} does not support {}; rebuild or restore the sandbox with spaced guest version 0.1.2 or newer",
                        rpc.unwrap_or("interactive process control")
                    ),
                ));
            }
            Err(guest_capability_unsupported_status(
                GuestCapability::PlatformOperations,
                format!("guest does not support {}", operation.as_str_name()),
            ))
        }
    }
}

#[cfg(test)]
mod tests;
