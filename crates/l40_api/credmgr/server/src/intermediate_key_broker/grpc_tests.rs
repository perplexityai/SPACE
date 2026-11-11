use super::*;
use pplx_space_credential_provider_client::v2::intermediate_key_broker_server::{
    IntermediateKeyBroker, IntermediateKeyBrokerServer,
};
use pplx_space_credential_provider_client::v2::{
    IntermediateKeyMaterial, IssueFailure, IssueIntermediateKeyResponse, UnwrapFailure,
    UnwrapIntermediateKeyResponse, WrappedIntermediateKey,
};
use tonic::{Request, Response, Status};

enum Behavior {
    Key {
        plaintext: Vec<u8>,
        max_cache_ttl: Option<prost_types::Duration>,
    },
    Failure {
        reason: i32,
        detail: &'static str,
    },
    Issued {
        key_id: &'static str,
        wrapped_key: &'static [u8],
    },
    IssueFailed {
        reason: i32,
        detail: &'static str,
    },
    EmptyOutcome,
    Fail(tonic::Code),
    Hang,
}

struct FakeGrpcProvider {
    behavior: Behavior,
    /// Tenant and opaque authorization evidence as they arrived, so a test can
    /// assert what SPACE claimed rather than only what it got back.
    claimed: ClaimedRequests,
}

#[derive(Debug, PartialEq)]
struct ClaimedRequest {
    tenant: TenantRef,
    authorization: Option<AuthorizationEvidence>,
}

type ClaimedRequests = Arc<std::sync::Mutex<Vec<ClaimedRequest>>>;

/// A fake whose stable key id and opaque blob survive an implementation
/// generation change between issue and unwrap.
struct MigratingFakeProvider {
    generation: Arc<std::sync::atomic::AtomicU8>,
}

#[tonic::async_trait]
impl IntermediateKeyBroker for MigratingFakeProvider {
    async fn issue_intermediate_key(
        &self,
        request: Request<IssueIntermediateKeyRequest>,
    ) -> Result<Response<IssueIntermediateKeyResponse>, Status> {
        assert_bearer(&request);
        assert_eq!(self.generation.load(std::sync::atomic::Ordering::SeqCst), 1);
        Ok(Response::new(IssueIntermediateKeyResponse {
            outcome: Some(IssueOutcome::Key(WrappedIntermediateKey {
                key_id: "stable-key-id".to_string(),
                wrapped_key: b"portable-wrapped-key".to_vec(),
            })),
        }))
    }

    async fn unwrap_intermediate_key(
        &self,
        request: Request<UnwrapIntermediateKeyRequest>,
    ) -> Result<Response<UnwrapIntermediateKeyResponse>, Status> {
        assert_bearer(&request);
        assert_eq!(self.generation.load(std::sync::atomic::Ordering::SeqCst), 2);
        let request = request.into_inner();
        assert_eq!(request.key_id, "stable-key-id");
        assert_eq!(request.wrapped_key, b"portable-wrapped-key");
        Ok(Response::new(UnwrapIntermediateKeyResponse {
            outcome: Some(UnwrapOutcome::Key(IntermediateKeyMaterial {
                plaintext_key: vec![0x5a; 32],
                max_cache_ttl: Some(prost_types::Duration {
                    seconds: 60,
                    nanos: 0,
                }),
            })),
        }))
    }
}

/// Every request must carry the client bearer credential.
fn assert_bearer<T>(request: &Request<T>) {
    let authorization = request
        .metadata()
        .get("authorization")
        .expect("request must carry the client credential")
        .to_str()
        .unwrap();
    assert_eq!(authorization, "Bearer test-provider-token");
}

#[tonic::async_trait]
impl IntermediateKeyBroker for FakeGrpcProvider {
    async fn issue_intermediate_key(
        &self,
        request: Request<IssueIntermediateKeyRequest>,
    ) -> Result<Response<IssueIntermediateKeyResponse>, Status> {
        assert_bearer(&request);
        let claim = request.into_inner();
        let tenant = claim.tenant.expect("request must carry a tenant");
        assert!(!tenant.namespace.is_empty());
        assert!(!tenant.id.is_empty());
        self.claimed.lock().unwrap().push(ClaimedRequest {
            tenant,
            authorization: claim.authorization,
        });
        match &self.behavior {
            Behavior::Issued {
                key_id,
                wrapped_key,
            } => Ok(Response::new(IssueIntermediateKeyResponse {
                outcome: Some(IssueOutcome::Key(WrappedIntermediateKey {
                    key_id: (*key_id).to_string(),
                    wrapped_key: wrapped_key.to_vec(),
                })),
            })),
            Behavior::IssueFailed { reason, detail } => {
                Ok(Response::new(IssueIntermediateKeyResponse {
                    outcome: Some(IssueOutcome::Failure(IssueFailure {
                        reason: *reason,
                        detail: (*detail).to_string(),
                    })),
                }))
            }
            Behavior::EmptyOutcome => Ok(Response::new(IssueIntermediateKeyResponse {
                outcome: None,
            })),
            Behavior::Fail(code) => Err(Status::new(*code, "injected")),
            _ => Err(Status::unimplemented("not exercised by this test")),
        }
    }

    async fn unwrap_intermediate_key(
        &self,
        request: Request<UnwrapIntermediateKeyRequest>,
    ) -> Result<Response<UnwrapIntermediateKeyResponse>, Status> {
        assert_bearer(&request);
        let inner = request.into_inner();
        let tenant = inner.tenant.expect("request must carry a tenant");
        assert!(!tenant.namespace.is_empty());
        assert!(!tenant.id.is_empty());
        assert!(!inner.key_id.is_empty());
        self.claimed.lock().unwrap().push(ClaimedRequest {
            tenant,
            authorization: inner.authorization,
        });
        match &self.behavior {
            Behavior::Key {
                plaintext,
                max_cache_ttl,
            } => Ok(Response::new(UnwrapIntermediateKeyResponse {
                outcome: Some(UnwrapOutcome::Key(IntermediateKeyMaterial {
                    plaintext_key: plaintext.clone(),
                    max_cache_ttl: *max_cache_ttl,
                })),
            })),
            Behavior::Failure { reason, detail } => {
                Ok(Response::new(UnwrapIntermediateKeyResponse {
                    outcome: Some(UnwrapOutcome::Failure(UnwrapFailure {
                        reason: *reason,
                        detail: (*detail).to_string(),
                    })),
                }))
            }
            Behavior::EmptyOutcome => Ok(Response::new(UnwrapIntermediateKeyResponse {
                outcome: None,
            })),
            Behavior::Fail(code) => Err(Status::new(*code, "injected")),
            Behavior::Hang => {
                std::future::pending::<()>().await;
                unreachable!()
            }
            Behavior::Issued { .. } | Behavior::IssueFailed { .. } => {
                Err(Status::unimplemented("not exercised by this test"))
            }
        }
    }
}

/// One-off bearer-credential file matching [`assert_bearer`].
fn token_file() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("bk-token-{}", uuid::Uuid::now_v7()));
    std::fs::write(&path, "test-provider-token\n").unwrap();
    path
}

async fn loopback_channel(behavior: Behavior) -> tonic::transport::Channel {
    loopback_channel_recording(behavior).await.0
}

/// As [`loopback_channel`], plus the recorder of what each request claimed.
async fn loopback_channel_recording(
    behavior: Behavior,
) -> (tonic::transport::Channel, ClaimedRequests) {
    let claimed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = claimed.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(IntermediateKeyBrokerServer::new(FakeGrpcProvider {
                behavior,
                claimed: recorded,
            }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    let channel = tonic::transport::Endpoint::try_from(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (channel, claimed)
}

async fn provider_over_loopback(behavior: Behavior) -> GrpcIntermediateKeyBroker {
    GrpcIntermediateKeyBroker::from_channel(
        loopback_channel(behavior).await,
        test_protocol(),
        token_file(),
        Arc::new(crate::intermediate_key_identity::SpaceIntermediateKeyIdentity),
    )
}

async fn migrating_provider_over_loopback(
    generation: Arc<std::sync::atomic::AtomicU8>,
) -> GrpcIntermediateKeyBroker {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_generation = generation.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(IntermediateKeyBrokerServer::new(MigratingFakeProvider {
                generation: server_generation,
            }))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    let channel = tonic::transport::Endpoint::try_from(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    GrpcIntermediateKeyBroker::from_channel(
        channel,
        test_protocol(),
        token_file(),
        Arc::new(crate::intermediate_key_identity::SpaceIntermediateKeyIdentity),
    )
}

fn org_and_key() -> (OrgId, MasterKeyIdentifier) {
    (OrgId::new(), MasterKeyIdentifier::parse("bk-v1").unwrap())
}

/// An unreadable credential is local infrastructure trouble: retriable
/// UNAVAILABLE, decided before any bytes reach the provider.
#[tokio::test]
async fn missing_bearer_token_file_is_unavailable() {
    let channel = loopback_channel(Behavior::Key {
        plaintext: vec![0x42u8; 32],
        max_cache_ttl: None,
    })
    .await;
    let provider = GrpcIntermediateKeyBroker::from_channel(
        channel,
        test_protocol(),
        std::env::temp_dir().join("bk-token-that-does-not-exist"),
        Arc::new(crate::intermediate_key_identity::SpaceIntermediateKeyIdentity),
    );
    let (org, kid) = org_and_key();
    let err = provider
        .unwrap_intermediate_key(org, None, &kid, b"blob")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ProviderError::Unavailable(msg) if msg.contains("bearer token")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn round_trips_key_and_ttl_cap() {
    let provider = provider_over_loopback(Behavior::Key {
        plaintext: vec![0x42u8; 32],
        max_cache_ttl: Some(prost_types::Duration {
            seconds: 120,
            nanos: 0,
        }),
    })
    .await;
    let (org, kid) = org_and_key();
    let unwrapped = provider
        .unwrap_intermediate_key(org, None, &kid, b"blob")
        .await
        .unwrap();
    assert_eq!(*unwrapped.key, [0x42u8; 32]);
    assert_eq!(unwrapped.max_cache_ttl, Duration::from_secs(120));
}

/// Presence is explicit for message fields: a provider that sends a
/// zero cap is stating "do not cache", not omitting the field.
#[tokio::test]
async fn preserves_explicit_zero_ttl_cap() {
    let provider = provider_over_loopback(Behavior::Key {
        plaintext: vec![0x42u8; 32],
        max_cache_ttl: Some(prost_types::Duration {
            seconds: 0,
            nanos: 0,
        }),
    })
    .await;
    let (org, kid) = org_and_key();
    let unwrapped = provider
        .unwrap_intermediate_key(org, None, &kid, b"blob")
        .await
        .unwrap();
    assert_eq!(unwrapped.max_cache_ttl, Duration::ZERO);
}

#[tokio::test]
async fn rejects_negative_or_sign_inconsistent_ttl_cap() {
    let (org, kid) = org_and_key();
    for (seconds, nanos) in [(-1i64, 0i32), (0, -1), (1, -1), (-1, 1)] {
        let provider = provider_over_loopback(Behavior::Key {
            plaintext: vec![0x42u8; 32],
            max_cache_ttl: Some(prost_types::Duration { seconds, nanos }),
        })
        .await;
        let err = provider
            .unwrap_intermediate_key(org, None, &kid, b"blob")
            .await
            .unwrap_err();
        assert!(
            matches!(err, ProviderError::Malformed(_)),
            "({seconds}s, {nanos}ns) must be malformed, got {err:?}"
        );
    }
}

#[tokio::test]
async fn rejects_missing_required_ttl_cap() {
    let provider = provider_over_loopback(Behavior::Key {
        plaintext: vec![0x42u8; 32],
        max_cache_ttl: None,
    })
    .await;
    let (org, kid) = org_and_key();
    let err = provider
        .unwrap_intermediate_key(org, None, &kid, b"blob")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ProviderError::Malformed(detail) if detail.contains("max_cache_ttl")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn maps_in_band_failure_reasons_to_provider_errors() {
    let (org, kid) = org_and_key();
    let invalid = provider_over_loopback(Behavior::Failure {
        reason: UnwrapFailureReason::InvalidKey as i32,
        detail: "unknown key id",
    })
    .await;
    assert!(matches!(
        invalid.unwrap_intermediate_key(org, None, &kid, b"blob").await,
        Err(ProviderError::InvalidKey(detail)) if detail == "unknown key id"
    ));
    let rejected = provider_over_loopback(Behavior::Failure {
        reason: UnwrapFailureReason::AuthorizationRejected as i32,
        detail: "grant expired",
    })
    .await;
    assert!(matches!(
        rejected.unwrap_intermediate_key(org, None, &kid, b"blob").await,
        Err(ProviderError::AuthorizationRejected(detail)) if detail == "grant expired"
    ));
    let revoked = provider_over_loopback(Behavior::Failure {
        reason: UnwrapFailureReason::AccessRevoked as i32,
        detail: "org offboarded",
    })
    .await;
    assert!(matches!(
        revoked.unwrap_intermediate_key(org, None, &kid, b"blob").await,
        Err(ProviderError::AccessRevoked(detail)) if detail == "org offboarded"
    ));
    let backoff = provider_over_loopback(Behavior::Failure {
        reason: UnwrapFailureReason::Unavailable as i32,
        detail: "kms throttled",
    })
    .await;
    assert!(matches!(
        backoff.unwrap_intermediate_key(org, None, &kid, b"blob").await,
        Err(ProviderError::Unavailable(detail)) if detail == "kms throttled"
    ));
}

/// gRPC statuses are transport-level only. A PERMISSION_DENIED from an
/// auth middleware or proxy must surface as retriable unavailability —
/// never as a revocation, which would trigger org cleanup and sandbox
/// shutdown from a mere credential misconfiguration between services.
#[tokio::test]
async fn grpc_statuses_are_transport_level_only() {
    let (org, kid) = org_and_key();
    for code in [
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
        tonic::Code::NotFound,
        tonic::Code::Unavailable,
    ] {
        let provider = provider_over_loopback(Behavior::Fail(code)).await;
        let err = provider
            .unwrap_intermediate_key(org, None, &kid, b"blob")
            .await
            .unwrap_err();
        assert!(
            matches!(err, ProviderError::Unavailable(_)),
            "status {code:?} must map to Unavailable, got {err:?}"
        );
    }
}

#[tokio::test]
async fn rejects_unspecified_reason_and_missing_outcome() {
    let (org, kid) = org_and_key();
    let unspecified = provider_over_loopback(Behavior::Failure {
        reason: UnwrapFailureReason::Unspecified as i32,
        detail: "??",
    })
    .await;
    assert!(matches!(
        unspecified
            .unwrap_intermediate_key(org, None, &kid, b"blob")
            .await,
        Err(ProviderError::Malformed(_))
    ));
    let empty = provider_over_loopback(Behavior::EmptyOutcome).await;
    assert!(matches!(
        empty
            .unwrap_intermediate_key(org, None, &kid, b"blob")
            .await,
        Err(ProviderError::Malformed(_))
    ));
}

/// Paused tokio time auto-advances past the deadline while the fake
/// server holds the request pending, so this does not really take 10s.
#[tokio::test(start_paused = true)]
async fn times_out_hung_provider_call() {
    let provider = provider_over_loopback(Behavior::Hang).await;
    let (org, kid) = org_and_key();
    let err = provider
        .unwrap_intermediate_key(org, None, &kid, b"blob")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ProviderError::Unavailable(msg) if msg.contains("deadline")),
        "got {err:?}"
    );
}

/// Identity resolution and the RPC consume one shared deadline. A stuck
/// database lookup therefore cannot hold the per-key single-flight gate.
#[tokio::test(start_paused = true)]
async fn times_out_hung_identity_lookup() {
    struct HangingIdentity;

    #[tonic::async_trait]
    impl IntermediateKeyIdentityLookup for HangingIdentity {
        async fn identity(
            &self,
            _org: OrgId,
        ) -> Result<IntermediateKeyIdentity, crate::intermediate_key_identity::IdentityLookupError>
        {
            std::future::pending().await
        }
    }

    let provider = GrpcIntermediateKeyBroker::from_channel(
        loopback_channel(Behavior::Issued {
            key_id: "bk-v1",
            wrapped_key: b"wrapped",
        })
        .await,
        test_protocol(),
        token_file(),
        Arc::new(HangingIdentity),
    );
    let err = provider
        .issue_intermediate_key(OrgId::new(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ProviderError::Unavailable(msg) if msg.contains("deadline")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rejects_non_32_byte_key_material() {
    let provider = provider_over_loopback(Behavior::Key {
        plaintext: vec![0x42u8; 16],
        max_cache_ttl: None,
    })
    .await;
    let (org, kid) = org_and_key();
    assert!(matches!(
        provider
            .unwrap_intermediate_key(org, None, &kid, b"blob")
            .await,
        Err(ProviderError::Malformed(_))
    ));
}

#[tokio::test]
async fn issue_round_trips_key_id_and_blob() {
    let provider = provider_over_loopback(Behavior::Issued {
        key_id: "bk/2026-07-17/v1",
        wrapped_key: b"kms-wrapped",
    })
    .await;
    let issued = provider
        .issue_intermediate_key(OrgId::new(), None)
        .await
        .unwrap();
    assert_eq!(issued.key_id.as_str(), "bk/2026-07-17/v1");
    assert_eq!(issued.wrapped_key, b"kms-wrapped");
}

/// Provider-neutral conformance: a key issued before a simulated broker
/// backend migration remains unwrap-compatible afterwards. SPACE treats both
/// the key id and wrapped bytes as opaque and returns the same plaintext.
#[tokio::test]
async fn unwraps_key_issued_before_broker_backend_migration() {
    let generation = Arc::new(std::sync::atomic::AtomicU8::new(1));
    let provider = migrating_provider_over_loopback(generation.clone()).await;
    let org = OrgId::new();

    let issued = provider.issue_intermediate_key(org, None).await.unwrap();
    generation.store(2, std::sync::atomic::Ordering::SeqCst);
    let unwrapped = provider
        .unwrap_intermediate_key(org, None, &issued.key_id, &issued.wrapped_key)
        .await
        .unwrap();

    assert_eq!(*unwrapped.key, [0x5a; 32]);
}

#[tokio::test]
async fn issue_maps_in_band_failures_and_keeps_statuses_transport_level() {
    let org = OrgId::new();
    let revoked = provider_over_loopback(Behavior::IssueFailed {
        reason: IssueFailureReason::AccessRevoked as i32,
        detail: "org offboarded",
    })
    .await;
    assert!(matches!(
        revoked.issue_intermediate_key(org, None).await,
        Err(ProviderError::AccessRevoked(detail)) if detail == "org offboarded"
    ));
    let invalid = provider_over_loopback(Behavior::IssueFailed {
        reason: IssueFailureReason::InvalidTenant as i32,
        detail: "unknown org",
    })
    .await;
    assert!(matches!(
        invalid.issue_intermediate_key(org, None).await,
        Err(ProviderError::InvalidTenant(detail)) if detail == "unknown org"
    ));
    let rejected = provider_over_loopback(Behavior::IssueFailed {
        reason: IssueFailureReason::AuthorizationRejected as i32,
        detail: "grant expired",
    })
    .await;
    assert!(matches!(
        rejected.issue_intermediate_key(org, None).await,
        Err(ProviderError::AuthorizationRejected(detail)) if detail == "grant expired"
    ));
    let backoff = provider_over_loopback(Behavior::IssueFailed {
        reason: IssueFailureReason::Unavailable as i32,
        detail: "kms throttled",
    })
    .await;
    assert!(matches!(
        backoff.issue_intermediate_key(org, None).await,
        Err(ProviderError::Unavailable(detail)) if detail == "kms throttled"
    ));
    // An auth-middleware denial must stay retriable, never an outcome.
    let status = provider_over_loopback(Behavior::Fail(tonic::Code::PermissionDenied)).await;
    assert!(matches!(
        status.issue_intermediate_key(org, None).await,
        Err(ProviderError::Unavailable(_))
    ));
}

#[tokio::test]
async fn issue_rejects_malformed_responses() {
    let org = OrgId::new();
    let empty_key_id = provider_over_loopback(Behavior::Issued {
        key_id: "",
        wrapped_key: b"kms-wrapped",
    })
    .await;
    assert!(matches!(
        empty_key_id.issue_intermediate_key(org, None).await,
        Err(ProviderError::Malformed(_))
    ));
    let empty_blob = provider_over_loopback(Behavior::Issued {
        key_id: "bk/v1",
        wrapped_key: b"",
    })
    .await;
    assert!(matches!(
        empty_blob.issue_intermediate_key(org, None).await,
        Err(ProviderError::Malformed(_))
    ));
    let unspecified = provider_over_loopback(Behavior::IssueFailed {
        reason: IssueFailureReason::Unspecified as i32,
        detail: "??",
    })
    .await;
    assert!(matches!(
        unspecified.issue_intermediate_key(org, None).await,
        Err(ProviderError::Malformed(_))
    ));
    let empty = provider_over_loopback(Behavior::EmptyOutcome).await;
    assert!(matches!(
        empty.issue_intermediate_key(org, None).await,
        Err(ProviderError::Malformed(_))
    ));
}

/// A fixed answer, standing in for the Postgres lookup.
struct FixedIdentity(IntermediateKeyIdentity);

#[tonic::async_trait]
impl IntermediateKeyIdentityLookup for FixedIdentity {
    async fn identity(
        &self,
        _org: OrgId,
    ) -> Result<IntermediateKeyIdentity, crate::intermediate_key_identity::IdentityLookupError>
    {
        Ok(self.0.clone())
    }
}

/// A federated org travels as its upstream id with the grant beside it — not as
/// the SPACE uuid, which the credential provider would turn into a different key scope than
/// the one the grant is sealed for.
#[tokio::test]
async fn issue_claims_the_upstream_org_and_presents_the_grant() {
    let (channel, claimed) = loopback_channel_recording(Behavior::Issued {
        key_id: "bk-v1",
        wrapped_key: b"wrapped",
    })
    .await;
    let provider = GrpcIntermediateKeyBroker::from_channel(
        channel,
        test_protocol(),
        token_file(),
        Arc::new(FixedIdentity(IntermediateKeyIdentity {
            wire_org_id: "d36f7757-0073-4910-a984-ccf2073fc0d6".to_string(),
            grant: Some(crate::intermediate_key_identity::PresentedGrant {
                user_id: "upstream-user-7".to_string(),
                grant: "sealed-grant".to_string(),
                expires_at: jiff::Timestamp::from_second(2_000_000_000).unwrap(),
            }),
        })),
    );
    let (org, _) = org_and_key();

    provider.issue_intermediate_key(org, None).await.unwrap();

    assert_eq!(
        claimed.lock().unwrap().as_slice(),
        [ClaimedRequest {
            tenant: TenantRef {
                namespace: "urn:example:tenant".to_string(),
                id: "d36f7757-0073-4910-a984-ccf2073fc0d6".to_string(),
            },
            authorization: Some(AuthorizationEvidence {
                scheme: "urn:example:grant:v1".to_string(),
                subject: Some("upstream-user-7".to_string()),
                evidence: b"sealed-grant".to_vec(),
            }),
        }]
    );
}

#[tokio::test]
async fn unwrap_claims_the_upstream_org_and_presents_the_grant() {
    let (channel, claimed) = loopback_channel_recording(Behavior::Key {
        plaintext: vec![0x42; 32],
        max_cache_ttl: Some(prost_types::Duration {
            seconds: 120,
            nanos: 0,
        }),
    })
    .await;
    let provider = GrpcIntermediateKeyBroker::from_channel(
        channel,
        test_protocol(),
        token_file(),
        Arc::new(FixedIdentity(IntermediateKeyIdentity {
            wire_org_id: "d36f7757-0073-4910-a984-ccf2073fc0d6".to_string(),
            grant: Some(crate::intermediate_key_identity::PresentedGrant {
                user_id: "upstream-user-7".to_string(),
                grant: "sealed-grant".to_string(),
                expires_at: jiff::Timestamp::from_second(4_000_000_000).unwrap(),
            }),
        })),
    );
    let (org, key_id) = org_and_key();

    provider
        .unwrap_intermediate_key(org, None, &key_id, b"wrapped")
        .await
        .unwrap();

    assert_eq!(
        claimed.lock().unwrap().as_slice(),
        [ClaimedRequest {
            tenant: TenantRef {
                namespace: "urn:example:tenant".to_string(),
                id: "d36f7757-0073-4910-a984-ccf2073fc0d6".to_string(),
            },
            authorization: Some(AuthorizationEvidence {
                scheme: "urn:example:grant:v1".to_string(),
                subject: Some("upstream-user-7".to_string()),
                evidence: b"sealed-grant".to_vec(),
            }),
        }]
    );
}

/// A local org has no upstream id and no grant: it keeps claiming the SPACE
/// uuid, which is what its existing keys are already scoped by.
#[tokio::test]
async fn issue_claims_the_space_uuid_for_a_local_org() {
    let (channel, claimed) = loopback_channel_recording(Behavior::Issued {
        key_id: "bk-v1",
        wrapped_key: b"wrapped",
    })
    .await;
    let (org, _) = org_and_key();
    let provider = GrpcIntermediateKeyBroker::from_channel(
        channel,
        test_protocol(),
        token_file(),
        Arc::new(FixedIdentity(IntermediateKeyIdentity::space_org_only(org))),
    );

    provider.issue_intermediate_key(org, None).await.unwrap();

    assert_eq!(
        claimed.lock().unwrap().as_slice(),
        [ClaimedRequest {
            tenant: TenantRef {
                namespace: "urn:example:tenant".to_string(),
                id: org.as_uuid().to_string(),
            },
            authorization: None,
        }]
    );
}

/// An identity that cannot be read fails the RPC. Falling back to the SPACE
/// uuid would mint a key under a scope the org's grant can never name, and that
/// key is the one that gets persisted.
#[tokio::test]
async fn unreadable_identity_fails_the_rpc() {
    struct Broken;

    #[tonic::async_trait]
    impl IntermediateKeyIdentityLookup for Broken {
        async fn identity(
            &self,
            org: OrgId,
        ) -> Result<IntermediateKeyIdentity, crate::intermediate_key_identity::IdentityLookupError>
        {
            Err(
                crate::intermediate_key_identity::IdentityLookupError::Unavailable {
                    org: org.as_uuid(),
                    source: crate::intermediate_key_identity::IdentityLookupUnavailable::OrganizationQuery(
                        sqlx::Error::Io(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            "connection refused",
                        )),
                    ),
                },
            )
        }
    }

    let (channel, claimed) = loopback_channel_recording(Behavior::Issued {
        key_id: "bk-v1",
        wrapped_key: b"wrapped",
    })
    .await;
    let provider = GrpcIntermediateKeyBroker::from_channel(
        channel,
        test_protocol(),
        token_file(),
        Arc::new(Broken),
    );
    let (org, _) = org_and_key();

    let err = provider
        .issue_intermediate_key(org, None)
        .await
        .unwrap_err();

    assert!(
        matches!(&err, ProviderError::Unavailable(msg) if msg.contains("connection refused")),
        "got {err:?}"
    );
    assert!(
        claimed.lock().unwrap().is_empty(),
        "nothing may reach the provider without a resolved identity"
    );
}

/// Persisted identity-shape faults are permanent for the request and must not
/// be advertised as retriable provider availability failures.
#[tokio::test]
async fn invalid_identity_is_not_unavailable() {
    struct InvalidIdentity;

    #[tonic::async_trait]
    impl IntermediateKeyIdentityLookup for InvalidIdentity {
        async fn identity(
            &self,
            org: OrgId,
        ) -> Result<IntermediateKeyIdentity, crate::intermediate_key_identity::IdentityLookupError>
        {
            Err(
                crate::intermediate_key_identity::IdentityLookupError::Invalid {
                    org: org.as_uuid(),
                    message: "federated org has no upstream_org_id".to_string(),
                },
            )
        }
    }

    let (channel, claimed) = loopback_channel_recording(Behavior::Issued {
        key_id: "bk-v1",
        wrapped_key: b"wrapped",
    })
    .await;
    let provider = GrpcIntermediateKeyBroker::from_channel(
        channel,
        test_protocol(),
        token_file(),
        Arc::new(InvalidIdentity),
    );

    let err = provider
        .issue_intermediate_key(OrgId::new(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ProviderError::InvalidTenant(_)),
        "got {err:?}"
    );
    assert!(claimed.lock().unwrap().is_empty());
}

fn test_protocol() -> BrokerProtocolConfig {
    BrokerProtocolConfig::parse(Some("urn:example:tenant"), Some("urn:example:grant:v1")).unwrap()
}
