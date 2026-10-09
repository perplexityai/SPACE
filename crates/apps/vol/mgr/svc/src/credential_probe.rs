use anyhow::{Context as _, ensure};
use pplx_space_credmgr_client::proto::{
    IssueIntermediateCertificateRequest, IssueSandboxArtifactDekRequest,
    ResolveNetworkCredentialsRequest, UnwrapSandboxArtifactDekRequest,
};
use tonic::Code;

pub async fn check() -> anyhow::Result<()> {
    check_client(pplx_space_credmgr_client::connect_from_env().await?).await
}

async fn check_client(
    mut client: pplx_space_credmgr_client::CredentialManagerGrpcClient,
) -> anyhow::Result<()> {
    // Empty requests cannot mint anything. The restricted listener must reject
    // privileged methods before their handlers can validate those requests.
    let ca = client
        .issue_intermediate_certificate(IssueIntermediateCertificateRequest::default())
        .await;
    ensure!(
        ca.err().is_some_and(|e| e.code() == Code::PermissionDenied),
        "CA RPC was not denied"
    );
    let credentials = client
        .resolve_network_credentials(ResolveNetworkCredentialsRequest::default())
        .await;
    ensure!(
        credentials
            .err()
            .is_some_and(|e| e.code() == Code::PermissionDenied),
        "network credential RPC was not denied"
    );
    // Malformed DEK requests stop at sandbox-id validation in both roles.
    // An empty ExchangeToken request is unsafe: legacy mode ignores fields and
    // could mint credentials. Full token exchange remains a release gate.
    let issue = client
        .issue_sandbox_artifact_dek(IssueSandboxArtifactDekRequest::default())
        .await;
    let status = issue
        .err()
        .context("empty DEK request unexpectedly succeeded")?;
    ensure!(
        status.code() == Code::InvalidArgument,
        "DEK probe expected INVALID_ARGUMENT, got {}",
        status.code()
    );
    let unwrap = client
        .unwrap_sandbox_artifact_dek(UnwrapSandboxArtifactDekRequest::default())
        .await;
    let status = unwrap
        .err()
        .context("empty unwrap request unexpectedly succeeded")?;
    ensure!(
        status.code() == Code::InvalidArgument,
        "unwrap probe expected INVALID_ARGUMENT, got {}",
        status.code()
    );
    Ok(())
}

#[cfg(test)]
mod tests;
