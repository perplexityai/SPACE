//! Who SPACE claims to be on an intermediate-key RPC.
//!
//! The credential provider scopes intermediate keys — and seals grants — by the
//! *upstream* org id the client asserted, not by SPACE's `organizations.id`. A
//! federated org has to travel as its upstream id, or the provider derives a key scope the
//! grant does not name and the two can never agree. A local org has no upstream
//! id and its existing keys are already scoped by the SPACE uuid, so that is
//! what it keeps sending.
//!
//! The grant is stored per sandbox. When a specific sandbox triggered the
//! operation, that sandbox's own live grant is preferred so the broker sees
//! the user who authorized this sandbox. If it has no live grant, the freshest
//! org sibling's grant is the transitional fallback. Org-scoped operations
//! choose the freshest live grant directly. An org with no live grant sends
//! none — the provider decides whether that is fatal.

use pplx_space_common_svc::core::types::{FederatedGrantAudience, OrgId, SandboxId};
use pplx_space_common_svc::federated_identity::FEDERATED_OWNER_GRANT_PURPOSE;
use pplx_space_common_svc::sql;
use pplx_space_common_svc::sql::federated_service_grants::{
    FreshestLiveFederatedGrant, FreshestLiveFederatedGrantError, freshest_live_federated_grant,
    live_federated_grant_prefer_sandbox,
};

/// `organizations.owner_source` for an org whose identity came from a trusted upstream client.
const FEDERATED_IDENTITY_OWNER_SOURCE: &str = "federated_identity";

/// A sealed grant and the upstream user it names. Both travel in the clear
/// beside the sealed copy, which carries its own; the provider compares them
/// and refuses a mismatch, so presenting one without the other proves nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentedGrant {
    pub user_id: String,
    pub grant: String,
    /// Absolute expiry supplied alongside the opaque grant. SPACE never
    /// interprets the grant, but uses this envelope metadata as a local
    /// plaintext-cache ceiling in addition to the broker's authoritative cap.
    pub expires_at: jiff::Timestamp,
}

/// What one intermediate-key RPC says about the org it acts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntermediateKeyIdentity {
    /// Goes on the wire as the id within the credential provider's fixed SPACE tenant
    /// namespace.
    pub wire_org_id: String,
    pub grant: Option<PresentedGrant>,
}

impl IntermediateKeyIdentity {
    /// SPACE's own uuid, no grant: a local org, and the only thing a node-local
    /// instance could claim.
    pub fn space_org_only(org: OrgId) -> Self {
        Self {
            wire_org_id: org.as_uuid().to_string(),
            grant: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityLookupError {
    /// The database lookup failed and a retry may succeed.
    #[error("intermediate-key identity lookup for {org} is unavailable: {source}")]
    Unavailable {
        org: uuid::Uuid,
        #[source]
        source: IdentityLookupUnavailable,
    },
    /// Persisted identity metadata is internally inconsistent. Retrying the
    /// same request cannot repair it.
    #[error("intermediate-key identity for {org} is invalid: {message}")]
    Invalid { org: uuid::Uuid, message: String },
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityLookupUnavailable {
    #[error("organization identity query failed: {0}")]
    OrganizationQuery(#[source] sqlx::Error),
    #[error(transparent)]
    FederatedGrant(#[from] FreshestLiveFederatedGrantError),
}

impl IdentityLookupError {
    fn unavailable(org: OrgId, source: impl Into<IdentityLookupUnavailable>) -> Self {
        Self::Unavailable {
            org: org.as_uuid(),
            source: source.into(),
        }
    }

    fn invalid(org: OrgId, message: impl Into<String>) -> Self {
        Self::Invalid {
            org: org.as_uuid(),
            message: message.into(),
        }
    }
}

/// Resolves the identity for one org. A trait so the provider can be exercised
/// without a live Postgres.
#[tonic::async_trait]
pub trait IntermediateKeyIdentityLookup: Send + Sync + 'static {
    async fn identity(&self, org: OrgId) -> Result<IntermediateKeyIdentity, IdentityLookupError>;

    async fn identity_for_sandbox(
        &self,
        org: OrgId,
        sandbox: SandboxId,
    ) -> Result<IntermediateKeyIdentity, IdentityLookupError> {
        let _ = sandbox;
        self.identity(org).await
    }
}

/// Claims the SPACE org uuid and never a grant.
pub struct SpaceIntermediateKeyIdentity;

#[tonic::async_trait]
impl IntermediateKeyIdentityLookup for SpaceIntermediateKeyIdentity {
    async fn identity(&self, org: OrgId) -> Result<IntermediateKeyIdentity, IdentityLookupError> {
        Ok(IntermediateKeyIdentity::space_org_only(org))
    }
}

/// Reads the org's owner identity and its freshest live grant from Postgres.
pub struct PgIntermediateKeyIdentityLookup {
    pool: sqlx::PgPool,
    grant_audience: FederatedGrantAudience,
}

impl PgIntermediateKeyIdentityLookup {
    pub fn new(pool: sqlx::PgPool, grant_audience: FederatedGrantAudience) -> Self {
        Self {
            pool,
            grant_audience,
        }
    }

    async fn federated_upstream_org_id(
        &self,
        org: OrgId,
    ) -> Result<Option<String>, IdentityLookupError> {
        let owner: Option<(String, Option<String>)> = sqlx::query_as(sql!(
            "SELECT owner_source, upstream_org_id FROM organizations WHERE id = $1"
        ))
        .bind(org.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            IdentityLookupError::unavailable(
                org,
                IdentityLookupUnavailable::OrganizationQuery(error),
            )
        })?;

        let Some((owner_source, upstream_org_id)) = owner else {
            return Ok(None);
        };
        if owner_source != FEDERATED_IDENTITY_OWNER_SOURCE {
            return Ok(None);
        }
        upstream_org_id.map(Some).ok_or_else(|| {
            IdentityLookupError::invalid(org, "federated org has no upstream_org_id")
        })
    }

    fn present(
        org: OrgId,
        row: Option<FreshestLiveFederatedGrant>,
        requesting_sandbox: Option<SandboxId>,
    ) -> Result<Option<PresentedGrant>, IdentityLookupError> {
        let selected_sandbox = row.as_ref().map(|row| row.sandbox_id);
        let grant = match row {
            Some(row) => String::from_utf8(row.grant_blob).ok().map(|grant| {
                jiff::Timestamp::from_second(row.expires_at_unix)
                    .map(|expires_at| PresentedGrant {
                        user_id: row.upstream_subject_id,
                        grant,
                        expires_at,
                    })
                    .map_err(|error| {
                        IdentityLookupError::invalid(
                            org,
                            format!("grant expiry is outside the supported range: {error}"),
                        )
                    })
            }),
            None => None,
        }
        .transpose()?;

        let source = match (&grant, requesting_sandbox) {
            (Some(_), Some(sandbox)) if selected_sandbox == Some(sandbox) => "own_sandbox",
            (Some(_), Some(_)) => "org_fallback",
            (Some(_), None) => "org_wide",
            (None, _) => "none",
        };
        pplx_space_metrics::credmgr::federated_grant_identity_source(source);
        Ok(grant)
    }
}

#[tonic::async_trait]
impl IntermediateKeyIdentityLookup for PgIntermediateKeyIdentityLookup {
    async fn identity(&self, org: OrgId) -> Result<IntermediateKeyIdentity, IdentityLookupError> {
        let Some(upstream_org_id) = self.federated_upstream_org_id(org).await? else {
            return Ok(IntermediateKeyIdentity::space_org_only(org));
        };

        let grant = freshest_live_federated_grant(
            &self.pool,
            org,
            self.grant_audience.as_str(),
            FEDERATED_OWNER_GRANT_PURPOSE,
        )
        .await
        .map_err(|error| IdentityLookupError::unavailable(org, error))?;

        Ok(IntermediateKeyIdentity {
            wire_org_id: upstream_org_id,
            grant: Self::present(org, grant, None)?,
        })
    }

    async fn identity_for_sandbox(
        &self,
        org: OrgId,
        sandbox: SandboxId,
    ) -> Result<IntermediateKeyIdentity, IdentityLookupError> {
        let Some(upstream_org_id) = self.federated_upstream_org_id(org).await? else {
            return Ok(IntermediateKeyIdentity::space_org_only(org));
        };

        let grant = live_federated_grant_prefer_sandbox(
            &self.pool,
            org,
            self.grant_audience.as_str(),
            FEDERATED_OWNER_GRANT_PURPOSE,
            sandbox,
        )
        .await
        .map_err(|error| IdentityLookupError::unavailable(org, error))?;

        Ok(IntermediateKeyIdentity {
            wire_org_id: upstream_org_id,
            grant: Self::present(org, grant, Some(sandbox))?,
        })
    }
}

#[cfg(test)]
mod postgres_tests;
