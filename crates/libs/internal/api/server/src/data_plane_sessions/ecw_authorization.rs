//! Authorizing an ECW sandbox service route request against the org's API keys and ECW credentials.

use super::*;

pub(super) async fn validate_unstable_sandbox_service_route_authorization(
    pool: &sqlx::PgPool,
    row: &SandboxServiceRouteRow,
    route_kind: SandboxServiceRouteKind,
    ecw_credential_keyring: &EcwCredentialKeyring,
    request: &UnstableResolveSandboxServiceRouteRequest,
) -> Result<(), Status> {
    match route_kind {
        SandboxServiceRouteKind::Agent => {
            if token_matches_ecw_credential(
                ecw_credential_keyring,
                row.sandbox_id,
                row.ecw_credential_epoch,
                credential_proof_sha256(
                    request,
                    SandboxServiceRouteCredentialKind::SandboxAgentToken,
                ),
                EcwCredentialKind::Envd,
            )? || api_key_belongs_to_org(
                pool,
                credential_proof_sha256(request, SandboxServiceRouteCredentialKind::OrgApiKey),
                row.org_id,
            )
            .await?
            {
                return Ok(());
            }
            // The traffic token is deliberately a narrower capability handed
            // to consumers of exposed service ports; it never authorizes the
            // envd route, which grants guest file and process control.
            Err(Status::unauthenticated(
                "invalid sandbox agent route credentials",
            ))
        }
        SandboxServiceRouteKind::ServicePort => {
            if token_matches_ecw_credential(
                ecw_credential_keyring,
                row.sandbox_id,
                row.ecw_credential_epoch,
                credential_proof_sha256(
                    request,
                    SandboxServiceRouteCredentialKind::SandboxTrafficToken,
                ),
                EcwCredentialKind::Traffic,
            )? {
                return Ok(());
            }
            Err(Status::permission_denied(
                "invalid sandbox service route credentials",
            ))
        }
        SandboxServiceRouteKind::Unspecified => {
            Err(Status::invalid_argument("route_kind is required"))
        }
    }
}

pub(super) fn credential_proof_sha256(
    request: &UnstableResolveSandboxServiceRouteRequest,
    kind: SandboxServiceRouteCredentialKind,
) -> Option<&str> {
    request
        .credential_proofs
        .iter()
        .filter(|proof| proof.kind == kind as i32)
        .map(|proof| proof.sha256.trim())
        .find(|sha256| !sha256.is_empty())
}

pub(super) async fn api_key_belongs_to_org(
    pool: &sqlx::PgPool,
    api_key_sha256: Option<&str>,
    org_id: OrgId,
) -> Result<bool, Status> {
    let Some(api_key_sha256) = api_key_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(false);
    };

    crate::api_key_store::authorizes_ecw_sandbox(pool, api_key_sha256, org_id)
        .await
        .map_err(|error| {
            Status::internal(format!("validate sandbox service route api key: {error}"))
        })
}

pub(super) fn token_matches_ecw_credential(
    keyring: &EcwCredentialKeyring,
    sandbox_id: SandboxId,
    credential_epoch: EcwCredentialEpoch,
    candidate_sha256: Option<&str>,
    kind: EcwCredentialKind,
) -> Result<bool, Status> {
    let Some(candidate_sha256) = candidate_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(false);
    };
    let result = keyring
        .match_sha256(sandbox_id, credential_epoch, kind, candidate_sha256)
        .map_err(|error| {
            pplx_space_metrics::internal_api::ecw_credential_verification(
                kind.as_str(),
                EcwCredentialVerificationResult::ConfigurationError,
            );
            Status::internal(format!("verify ECW credential: {error}"))
        })?;
    let metric_result = match result {
        EcwCredentialMatch::V2Current => EcwCredentialVerificationResult::V2Current,
        EcwCredentialMatch::V2Previous => EcwCredentialVerificationResult::V2Previous,
        EcwCredentialMatch::NoMatch => EcwCredentialVerificationResult::Invalid,
    };
    pplx_space_metrics::internal_api::ecw_credential_verification(kind.as_str(), metric_result);
    Ok(result != EcwCredentialMatch::NoMatch)
}
