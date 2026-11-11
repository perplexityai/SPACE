//! Reusable behavioral conformance suite for intermediate-key broker v2.
//!
//! A broker supplies a normal gRPC channel and a test-only control adapter.
//! The suite drives only the public v2 RPCs; the adapter selects in-band
//! failures and simulates a backing-provider migration so those otherwise
//! unreachable protocol promises can be verified without standardizing a
//! production control surface.

use std::time::Duration;

use tonic::transport::Channel;

use crate::v2::intermediate_key_broker_client::IntermediateKeyBrokerClient;
use crate::v2::issue_intermediate_key_response::Outcome as IssueOutcome;
use crate::v2::unwrap_intermediate_key_response::Outcome as UnwrapOutcome;
use crate::v2::{
    AuthorizationEvidence, IntermediateKeyMaterial, IssueFailureReason,
    IssueIntermediateKeyRequest, IssueIntermediateKeyResponse, TenantRef, UnwrapFailureReason,
    UnwrapIntermediateKeyRequest, UnwrapIntermediateKeyResponse, WrappedIntermediateKey,
};

/// How a conforming broker authorizes otherwise valid requests in its test
/// environment.
#[derive(Clone, Debug)]
pub enum AuthorizationProfile {
    /// Workload transport identity is sufficient; application evidence is
    /// absent on successful requests.
    TransportIdentityOnly,
    /// Application evidence is mandatory. Both tenants need valid evidence so
    /// the suite can distinguish tenant binding from authorization rejection.
    EvidenceRequired {
        primary: AuthorizationEvidence,
        other_tenant: AuthorizationEvidence,
        /// Maximum remaining evidence lifetime advertised by the fixture.
        max_cache_ttl: Duration,
    },
}

/// Valid and invalid inputs shared by one conformance run.
#[derive(Clone, Debug)]
pub struct ConformanceConfig {
    primary_tenant: TenantRef,
    other_tenant: TenantRef,
    authorization: AuthorizationProfile,
    unknown_authorization: AuthorizationEvidence,
    broker_max_cache_ttl: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConformanceConfigError {
    #[error("{field} must contain a non-empty namespace and id")]
    InvalidTenant { field: &'static str },
    #[error("primary_tenant and other_tenant must be different")]
    DuplicateTenant,
    #[error("{field} must contain a non-empty scheme and evidence")]
    InvalidAuthorization { field: &'static str },
    #[error("unknown_authorization must use a scheme different from supported evidence")]
    KnownAuthorizationScheme,
}

impl ConformanceConfig {
    pub fn new(
        primary_tenant: TenantRef,
        other_tenant: TenantRef,
        authorization: AuthorizationProfile,
        unknown_authorization: AuthorizationEvidence,
        broker_max_cache_ttl: Duration,
    ) -> Result<Self, ConformanceConfigError> {
        validate_tenant("primary_tenant", &primary_tenant)?;
        validate_tenant("other_tenant", &other_tenant)?;
        if primary_tenant == other_tenant {
            return Err(ConformanceConfigError::DuplicateTenant);
        }
        validate_authorization("unknown_authorization", &unknown_authorization)?;
        if let AuthorizationProfile::EvidenceRequired {
            primary,
            other_tenant,
            ..
        } = &authorization
        {
            validate_authorization("primary authorization", primary)?;
            validate_authorization("other-tenant authorization", other_tenant)?;
            if unknown_authorization.scheme == primary.scheme
                || unknown_authorization.scheme == other_tenant.scheme
            {
                return Err(ConformanceConfigError::KnownAuthorizationScheme);
            }
        }
        Ok(Self {
            primary_tenant,
            other_tenant,
            authorization,
            unknown_authorization,
            broker_max_cache_ttl,
        })
    }

    fn primary_authorization(&self) -> Option<AuthorizationEvidence> {
        match &self.authorization {
            AuthorizationProfile::TransportIdentityOnly => None,
            AuthorizationProfile::EvidenceRequired { primary, .. } => Some(primary.clone()),
        }
    }

    fn other_tenant_authorization(&self) -> Option<AuthorizationEvidence> {
        match &self.authorization {
            AuthorizationProfile::TransportIdentityOnly => None,
            AuthorizationProfile::EvidenceRequired { other_tenant, .. } => {
                Some(other_tenant.clone())
            }
        }
    }

    fn effective_cache_ttl_ceiling(&self) -> Duration {
        match self.authorization {
            AuthorizationProfile::TransportIdentityOnly => self.broker_max_cache_ttl,
            AuthorizationProfile::EvidenceRequired { max_cache_ttl, .. } => {
                self.broker_max_cache_ttl.min(max_cache_ttl)
            }
        }
    }
}

fn validate_tenant(field: &'static str, tenant: &TenantRef) -> Result<(), ConformanceConfigError> {
    if tenant.namespace.is_empty() || tenant.id.is_empty() {
        return Err(ConformanceConfigError::InvalidTenant { field });
    }
    Ok(())
}

fn validate_authorization(
    field: &'static str,
    authorization: &AuthorizationEvidence,
) -> Result<(), ConformanceConfigError> {
    if authorization.scheme.is_empty() || authorization.evidence.is_empty() {
        return Err(ConformanceConfigError::InvalidAuthorization { field });
    }
    Ok(())
}

/// Test-only controls required to reach every application outcome and to
/// simulate a backend migration. These controls are deliberately outside the
/// public broker service.
#[tonic::async_trait]
pub trait ConformanceControl: Send + Sync {
    async fn set_issue_failure(
        &self,
        reason: Option<IssueFailureReason>,
    ) -> Result<(), ConformanceControlError>;

    async fn set_unwrap_failure(
        &self,
        reason: Option<UnwrapFailureReason>,
    ) -> Result<(), ConformanceControlError>;

    async fn migrate_backing_provider(&self) -> Result<(), ConformanceControlError>;
}

#[derive(Debug, thiserror::Error)]
#[error("conformance fixture control failed: {detail}")]
pub struct ConformanceControlError {
    detail: String,
}

impl ConformanceControlError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConformanceError {
    #[error("{operation} returned a non-OK gRPC status: {source}")]
    Rpc {
        operation: &'static str,
        #[source]
        source: tonic::Status,
    },
    #[error(transparent)]
    Control(#[from] ConformanceControlError),
    #[error("broker violated {clause}: {detail}")]
    Violation {
        clause: &'static str,
        detail: String,
    },
}

/// Drives the public v2 RPCs against one broker instance.
pub struct IntermediateKeyBrokerConformance {
    client: IntermediateKeyBrokerClient<Channel>,
    config: ConformanceConfig,
}

#[derive(Clone, Debug)]
struct IssuedKey {
    tenant: TenantRef,
    key_id: String,
    wrapped_key: Vec<u8>,
}

impl IntermediateKeyBrokerConformance {
    pub fn new(client: IntermediateKeyBrokerClient<Channel>, config: ConformanceConfig) -> Self {
        Self { client, config }
    }

    /// Run the provider-neutral contract. A successful return proves:
    ///
    /// - the configured absent-evidence policy and unknown-scheme rejection;
    /// - tenant binding and required cache-TTL limits;
    /// - every defined in-band failure reason for both RPCs;
    /// - fresh issue/rotation behavior and historical-key reads; and
    /// - historical reads after a simulated backing-provider migration.
    pub async fn run<C: ConformanceControl>(
        &mut self,
        control: &C,
    ) -> Result<(), ConformanceError> {
        control.set_issue_failure(None).await?;
        control.set_unwrap_failure(None).await?;

        if matches!(
            self.config.authorization,
            AuthorizationProfile::EvidenceRequired { .. }
        ) {
            let tenant = self.config.primary_tenant.clone();
            self.expect_issue_failure(&tenant, None, IssueFailureReason::AuthorizationRejected)
                .await?;
        }

        let primary_tenant = self.config.primary_tenant.clone();
        let primary_authorization = self.config.primary_authorization();
        let first = self
            .expect_issue_key(&primary_tenant, primary_authorization.clone())
            .await?;
        let first_material = self
            .expect_unwrap_key(&first, primary_authorization.clone())
            .await?;
        self.validate_material(&first_material)?;

        let other_key = IssuedKey {
            tenant: self.config.other_tenant.clone(),
            key_id: first.key_id.clone(),
            wrapped_key: first.wrapped_key.clone(),
        };
        self.expect_unwrap_failure(
            &other_key,
            self.config.other_tenant_authorization(),
            UnwrapFailureReason::InvalidKey,
        )
        .await?;

        let unknown = Some(self.config.unknown_authorization.clone());
        self.expect_issue_failure(
            &primary_tenant,
            unknown.clone(),
            IssueFailureReason::AuthorizationRejected,
        )
        .await?;
        self.expect_unwrap_failure(&first, unknown, UnwrapFailureReason::AuthorizationRejected)
            .await?;

        self.exercise_issue_failures(control, &primary_tenant, primary_authorization.clone())
            .await?;
        self.exercise_unwrap_failures(control, &first, primary_authorization.clone())
            .await?;

        let second = self
            .expect_issue_key(&primary_tenant, primary_authorization.clone())
            .await?;
        if first.key_id == second.key_id && first.wrapped_key == second.wrapped_key {
            return Err(violation(
                "fresh issue",
                "a second successful issue returned the same key tuple",
            ));
        }
        let second_material = self
            .expect_unwrap_key(&second, primary_authorization.clone())
            .await?;
        self.validate_material(&second_material)?;
        if first_material.plaintext_key == second_material.plaintext_key {
            return Err(violation(
                "fresh issue",
                "two issued key versions returned identical plaintext",
            ));
        }
        let historical_before_migration = self
            .expect_unwrap_key(&first, primary_authorization.clone())
            .await?;
        if historical_before_migration.plaintext_key != first_material.plaintext_key {
            return Err(violation(
                "historical key stability",
                "the first key changed after issuing a second version",
            ));
        }

        control.migrate_backing_provider().await?;
        let first_after_migration = self
            .expect_unwrap_key(&first, primary_authorization.clone())
            .await?;
        let second_after_migration = self
            .expect_unwrap_key(&second, primary_authorization)
            .await?;
        if first_after_migration.plaintext_key != first_material.plaintext_key
            || second_after_migration.plaintext_key != second_material.plaintext_key
        {
            return Err(violation(
                "backend migration compatibility",
                "a historical key changed after the backing-provider migration",
            ));
        }
        Ok(())
    }

    async fn exercise_issue_failures<C: ConformanceControl>(
        &mut self,
        control: &C,
        tenant: &TenantRef,
        authorization: Option<AuthorizationEvidence>,
    ) -> Result<(), ConformanceError> {
        let mut reason = IssueFailureReason::InvalidTenant;
        loop {
            control.set_issue_failure(Some(reason)).await?;
            self.expect_issue_failure(tenant, authorization.clone(), reason)
                .await?;
            reason = match reason {
                IssueFailureReason::InvalidTenant => IssueFailureReason::AuthorizationRejected,
                IssueFailureReason::AuthorizationRejected => IssueFailureReason::AccessRevoked,
                IssueFailureReason::AccessRevoked => IssueFailureReason::Unavailable,
                IssueFailureReason::Unavailable => break,
                IssueFailureReason::Unspecified => {
                    return Err(violation(
                        "failure taxonomy",
                        "UNSPECIFIED is not an application decision",
                    ));
                }
            };
        }
        control.set_issue_failure(None).await?;
        Ok(())
    }

    async fn exercise_unwrap_failures<C: ConformanceControl>(
        &mut self,
        control: &C,
        key: &IssuedKey,
        authorization: Option<AuthorizationEvidence>,
    ) -> Result<(), ConformanceError> {
        let mut reason = UnwrapFailureReason::InvalidKey;
        loop {
            control.set_unwrap_failure(Some(reason)).await?;
            self.expect_unwrap_failure(key, authorization.clone(), reason)
                .await?;
            reason = match reason {
                UnwrapFailureReason::InvalidKey => UnwrapFailureReason::AuthorizationRejected,
                UnwrapFailureReason::AuthorizationRejected => UnwrapFailureReason::AccessRevoked,
                UnwrapFailureReason::AccessRevoked => UnwrapFailureReason::Unavailable,
                UnwrapFailureReason::Unavailable => break,
                UnwrapFailureReason::Unspecified => {
                    return Err(violation(
                        "failure taxonomy",
                        "UNSPECIFIED is not an application decision",
                    ));
                }
            };
        }
        control.set_unwrap_failure(None).await?;
        Ok(())
    }

    async fn issue(
        &mut self,
        tenant: &TenantRef,
        authorization: Option<AuthorizationEvidence>,
    ) -> Result<IssueIntermediateKeyResponse, ConformanceError> {
        self.client
            .issue_intermediate_key(IssueIntermediateKeyRequest {
                tenant: Some(tenant.clone()),
                authorization,
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(|source| ConformanceError::Rpc {
                operation: "IssueIntermediateKey",
                source,
            })
    }

    async fn unwrap(
        &mut self,
        key: &IssuedKey,
        authorization: Option<AuthorizationEvidence>,
    ) -> Result<UnwrapIntermediateKeyResponse, ConformanceError> {
        self.client
            .unwrap_intermediate_key(UnwrapIntermediateKeyRequest {
                tenant: Some(key.tenant.clone()),
                key_id: key.key_id.clone(),
                wrapped_key: key.wrapped_key.clone(),
                authorization,
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(|source| ConformanceError::Rpc {
                operation: "UnwrapIntermediateKey",
                source,
            })
    }

    async fn expect_issue_key(
        &mut self,
        tenant: &TenantRef,
        authorization: Option<AuthorizationEvidence>,
    ) -> Result<IssuedKey, ConformanceError> {
        match self.issue(tenant, authorization).await?.outcome {
            Some(IssueOutcome::Key(WrappedIntermediateKey {
                key_id,
                wrapped_key,
            })) if !key_id.is_empty() && !wrapped_key.is_empty() => Ok(IssuedKey {
                tenant: tenant.clone(),
                key_id,
                wrapped_key,
            }),
            Some(IssueOutcome::Key(_)) => Err(violation(
                "issue success shape",
                "key_id and wrapped_key must be non-empty",
            )),
            Some(IssueOutcome::Failure(failure)) => Err(violation(
                "issue success",
                format!("received failure reason {}", failure.reason),
            )),
            None => Err(violation("issue success shape", "response omitted outcome")),
        }
    }

    async fn expect_issue_failure(
        &mut self,
        tenant: &TenantRef,
        authorization: Option<AuthorizationEvidence>,
        expected: IssueFailureReason,
    ) -> Result<(), ConformanceError> {
        match self.issue(tenant, authorization).await?.outcome {
            Some(IssueOutcome::Failure(failure)) if failure.reason == expected as i32 => Ok(()),
            Some(IssueOutcome::Failure(failure)) => Err(violation(
                "issue failure taxonomy",
                format!(
                    "expected {expected:?}, received numeric reason {}",
                    failure.reason
                ),
            )),
            Some(IssueOutcome::Key(_)) => Err(violation(
                "issue failure taxonomy",
                format!("expected {expected:?}, received key material"),
            )),
            None => Err(violation(
                "issue failure taxonomy",
                "response omitted outcome",
            )),
        }
    }

    async fn expect_unwrap_key(
        &mut self,
        key: &IssuedKey,
        authorization: Option<AuthorizationEvidence>,
    ) -> Result<IntermediateKeyMaterial, ConformanceError> {
        match self.unwrap(key, authorization).await?.outcome {
            Some(UnwrapOutcome::Key(material)) => Ok(material),
            Some(UnwrapOutcome::Failure(failure)) => Err(violation(
                "unwrap success",
                format!("received failure reason {}", failure.reason),
            )),
            None => Err(violation(
                "unwrap success shape",
                "response omitted outcome",
            )),
        }
    }

    async fn expect_unwrap_failure(
        &mut self,
        key: &IssuedKey,
        authorization: Option<AuthorizationEvidence>,
        expected: UnwrapFailureReason,
    ) -> Result<(), ConformanceError> {
        match self.unwrap(key, authorization).await?.outcome {
            Some(UnwrapOutcome::Failure(failure)) if failure.reason == expected as i32 => Ok(()),
            Some(UnwrapOutcome::Failure(failure)) => Err(violation(
                "unwrap failure taxonomy",
                format!(
                    "expected {expected:?}, received numeric reason {}",
                    failure.reason
                ),
            )),
            Some(UnwrapOutcome::Key(_)) => Err(violation(
                "unwrap failure taxonomy",
                format!("expected {expected:?}, received key material"),
            )),
            None => Err(violation(
                "unwrap failure taxonomy",
                "response omitted outcome",
            )),
        }
    }

    fn validate_material(
        &self,
        material: &IntermediateKeyMaterial,
    ) -> Result<(), ConformanceError> {
        if material.plaintext_key.len() != 32 {
            return Err(violation(
                "plaintext key shape",
                format!(
                    "expected 32 bytes, received {}",
                    material.plaintext_key.len()
                ),
            ));
        }
        let ttl = parse_ttl(material.max_cache_ttl.as_ref())?;
        let ceiling = self.config.effective_cache_ttl_ceiling();
        if ttl > ceiling {
            return Err(violation(
                "cache TTL ceiling",
                format!("broker returned {ttl:?}, above {ceiling:?}"),
            ));
        }
        Ok(())
    }
}

fn parse_ttl(value: Option<&prost_types::Duration>) -> Result<Duration, ConformanceError> {
    let value = value.ok_or_else(|| violation("cache TTL shape", "max_cache_ttl is required"))?;
    if value.seconds < 0 || value.nanos < 0 || value.nanos > 999_999_999 {
        return Err(violation(
            "cache TTL shape",
            format!(
                "invalid protobuf duration ({}, {})",
                value.seconds, value.nanos
            ),
        ));
    }
    Ok(Duration::new(
        value.seconds.cast_unsigned(),
        value.nanos.cast_unsigned(),
    ))
}

fn violation(clause: &'static str, detail: impl Into<String>) -> ConformanceError {
    ConformanceError::Violation {
        clause,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests;
