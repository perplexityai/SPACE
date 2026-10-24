//! Execution-bound CA issuance. Leaves are signed by the gateway, not here.

use futures_util::future::BoxFuture;
use pplx_space_credmgr_client::proto::{
    IssueIntermediateCertificateRequest as NodeRequest,
    IssueIntermediateCertificateResponse as NodeResponse,
};
use pplx_space_spaced_common::proto::host_control::{
    IssueGatewayIntermediateCertificateRequest, IssueGatewayIntermediateCertificateResponse,
};
use tokio::sync::Semaphore;

use super::*;

const MAX_ISSUANCES: usize = 4;
const MAX_CERTIFICATE_BYTES: usize = 16 * 1024;
// Includes the UDS connection and node's 30s CA acquisition budget. This is a
// startup/renewal operation; never invoke it from a guest TLS handshake.
const ISSUANCE_TIMEOUT: Duration = Duration::from_secs(40);

type Issuer =
    Arc<dyn Fn(NodeRequest) -> BoxFuture<'static, Result<NodeResponse, Status>> + Send + Sync>;

#[derive(Clone)]
pub(super) struct GatewayAuthority {
    issuer: Issuer,
    capacity: Arc<Semaphore>,
}

impl std::fmt::Debug for GatewayAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayAuthority")
            .finish_non_exhaustive()
    }
}

impl GatewayAuthority {
    pub(super) fn new(
        endpoint: pplx_space_credmgr_client::CredmgrEndpoint,
        client: Arc<tokio::sync::OnceCell<pplx_space_credmgr_client::CredentialManagerGrpcClient>>,
    ) -> Self {
        Self {
            capacity: Arc::new(Semaphore::new(MAX_ISSUANCES)),
            issuer: Arc::new(move |request| {
                let client = client.clone();
                let endpoint = endpoint.clone();
                Box::pin(async move {
                    // This hop is always node-local. The node credential agent
                    // owns upstream authentication; do not use its TCP endpoint.
                    let mut client = client
                        .get_or_try_init(|| endpoint.connect())
                        .await
                        .map_err(|error| {
                            tracing::warn!(%error, "gateway CA node connection failed");
                            Status::unavailable("gateway CA node connection unavailable")
                        })?
                        .clone()
                        .max_decoding_message_size(2 * MAX_CERTIFICATE_BYTES + 1024);
                    let mut request = Request::new(request);
                    request.set_timeout(ISSUANCE_TIMEOUT);
                    client
                        .issue_intermediate_certificate(request)
                        .await
                        .map(Response::into_inner)
                })
            }),
        }
    }
}

fn can_issue_authority(state: SandboxState) -> bool {
    match state {
        SandboxState::Starting
        | SandboxState::Restoring
        | SandboxState::Started
        | SandboxState::Pausing
        | SandboxState::Paused
        | SandboxState::Resuming
        | SandboxState::Checkpointing => true,
        SandboxState::Creating
        | SandboxState::Created
        | SandboxState::Stopping
        | SandboxState::Stopped
        | SandboxState::Deleting
        | SandboxState::Suspending
        | SandboxState::Suspended
        | SandboxState::CleanupRequired
        | SandboxState::CleanupFailed
        | SandboxState::Quarantined
        | SandboxState::Failed => false,
    }
}

fn retry_incomplete_identity(state: SandboxState) -> bool {
    matches!(
        state,
        SandboxState::Starting | SandboxState::Restoring | SandboxState::Resuming
    )
}

fn incomplete_identity_status(state: SandboxState) -> Status {
    if retry_incomplete_identity(state) {
        return Status::unavailable("gateway CA execution identity is incomplete");
    }
    Status::failed_precondition("gateway CA execution identity is incomplete")
}

fn validate_gateway_authority_sandbox(
    sandbox: &crate::db::SandboxRow,
    shutting_down: bool,
) -> Result<(), Status> {
    if shutting_down
        || sandbox.pool_template.is_some()
        || !can_issue_authority(sandbox.state)
        || sandbox.user.kind() == SandboxOwnerKind::Orphaned
    {
        return Err(Status::failed_precondition(
            "gateway CA requires an assigned active execution",
        ));
    }
    let identity = &sandbox.space_api_route_identity;
    if identity
        .sandbox_id
        .is_some_and(|id| id.to_string() != sandbox.id)
    {
        return Err(Status::failed_precondition(
            "gateway CA execution identity does not match its sandbox",
        ));
    }
    if identity.sandbox_id.is_none()
        || identity.execution_generation.is_none()
        || identity.route_generation.is_none()
    {
        return Err(incomplete_identity_status(sandbox.state));
    }
    Ok(())
}

fn same_execution_authority(before: &crate::db::SandboxRow, after: &crate::db::SandboxRow) -> bool {
    // Ordinary policy, counters and eligible lifecycle transitions do not
    // change CA ownership. All route/activation identity fields do.
    before.id == after.id
        && before.user == after.user
        && before.uid == after.uid
        && before.netgtw_pid == after.netgtw_pid
        && before.template == after.template
        && before.pool_template == after.pool_template
        && before.space_api_route_identity == after.space_api_route_identity
}

impl SandboxManagerService {
    /// Bounded to 40 seconds including connection retries and both blocking
    /// authorization checks; a shorter caller deadline wins. Late results never cross an
    /// execution/owner/process change. Configuration is immutable for this
    /// service instance; shutdown cancels issuance before returning a result.
    pub(super) async fn issue_gateway_authority(
        &self,
        request: Request<IssueGatewayIntermediateCertificateRequest>,
    ) -> Result<IssueGatewayIntermediateCertificateResponse, Status> {
        let started = tokio::time::Instant::now();
        let caller_timeout = request
            .metadata()
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_grpc_timeout)
            .filter(|timeout| *timeout <= ISSUANCE_TIMEOUT);
        let deadline = started + caller_timeout.unwrap_or(ISSUANCE_TIMEOUT);
        let GatewayAuthority { issuer, capacity } = &self.gateway_authority;
        let permit = capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("gateway CA issuance capacity exceeded"))?;
        if request.get_ref().csr.is_empty()
            || request.get_ref().csr.len() > pplx_space_credmgr_client::MAX_INTERMEDIATE_CSR_BYTES
        {
            return Err(Status::invalid_argument("invalid gateway CSR size"));
        }
        let operation = async {
            let service = self.clone();
            let (before, request, permit) = tokio::task::spawn_blocking(move || {
                // Capacity remains owned by blocking work if its waiter times out.
                let before = service.gateway_authority_context(&request)?;
                Ok::<_, Status>((before, request, permit))
            })
            .await
            .map_err(|_| Status::internal("gateway CA authorization worker failed"))??;
            self.authorize_gateway_launch(
                &request,
                &before,
                &request.get_ref().launch_capability,
                request.get_ref().execution_generation,
            )
            .await?;
            let expected_anchor = crate::split_trust::gateway_anchor(
                &self.credential_manager_endpoint,
                &before.id,
                super::activation_support::spaced_host_socket_path(&before.id),
                before.state == crate::state::SandboxState::Started,
            )
            .await?;
            let issued = issuer(NodeRequest {
                csr: request.get_ref().csr.clone(),
                sandbox_id: expected_anchor.as_ref().map(|_| before.id.clone()),
            })
            .await?;
            self.authorize_gateway_launch(
                &request,
                &before,
                &request.get_ref().launch_capability,
                request.get_ref().execution_generation,
            )
            .await?;
            let service = self.clone();
            let after = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                service.gateway_authority_context(&request)
            })
            .await
            .map_err(|_| Status::internal("gateway CA authorization worker failed"))??;
            if !same_execution_authority(&before, &after) {
                return Err(Status::aborted(
                    "gateway CA execution ownership changed during issuance",
                ));
            }
            if expected_anchor
                .as_ref()
                .is_some_and(|expected| expected != &issued.issuer_certificate_der)
            {
                return Err(Status::failed_precondition(
                    "node returned a different sandbox anchor",
                ));
            }
            if [&issued.certificate_der, &issued.issuer_certificate_der]
                .into_iter()
                .any(|certificate| {
                    certificate.is_empty() || certificate.len() > MAX_CERTIFICATE_BYTES
                })
            {
                return Err(Status::internal("invalid gateway CA response size"));
            }
            Ok(IssueGatewayIntermediateCertificateResponse {
                certificate_der: issued.certificate_der,
                issuer_certificate_der: issued.issuer_certificate_der,
            })
        };
        tokio::select! {
            biased;
            _ = self.shutdown_token.cancelled() => Err(Status::unavailable("sandbox-manager is shutting down")),
            _ = tokio::time::sleep_until(deadline) => Err(if caller_timeout.is_some() {
                Status::cancelled("gateway CA caller deadline expired")
            } else {
                Status::unavailable("gateway CA issuance deadline; retry with the same key")
            }),
            result = operation => result,
        }
    }

    fn gateway_authority_context(
        &self,
        request: &Request<IssueGatewayIntermediateCertificateRequest>,
    ) -> Result<crate::db::SandboxRow, Status> {
        // Like GetNetworkPolicy, the registered UDS peer selects the sandbox.
        // CA issuance awaits another service, so its ownership is checked again
        // before returning the result.
        let sandbox = self.authenticated_net_gateway_host_control_sandbox(request)?;
        validate_gateway_authority_sandbox(&sandbox, self.shutdown_token.is_cancelled())?;
        Ok(sandbox)
    }
}

#[cfg(test)]
mod tests;
