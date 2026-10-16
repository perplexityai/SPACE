use std::{collections::BTreeMap, fmt};
use uuid::Uuid;

use pplx_space_common_core::types::{
    NodeId, OrgId, RuntimeKind, TemplateReference, UpstreamIssuer, UpstreamOrgId,
    UpstreamSubjectId, UserId,
};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_public_api::{
    OperationId, OperationTargetType, PublicSandboxMetadataValue,
    SandboxResources as PublicSandboxResources,
};

use super::*;

use pplx_space_test_env::TestResult;

fn claimed_create_operation() -> ClaimedPublicOperation {
    ClaimedPublicOperation {
        id: OperationId::new(),
        org_id: OrgId::new(),
        actor_user_id: Some(UserId::new()),
        api_product: ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: None,
        target_id: None,
        payload: serde_json::json!({}),
        attempts: 1,
        claim_generation: 1,
        traceparent: None,
    }
}

fn claimed_create_operation_for(
    sandbox_id: SandboxId,
    org_id: OrgId,
    actor_user_id: UserId,
) -> ClaimedPublicOperation {
    let public_sandbox_id = PublicSandboxId::from(sandbox_id);
    ClaimedPublicOperation {
        id: OperationId::new(),
        org_id,
        actor_user_id: Some(actor_user_id),
        api_product: ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: None,
        target_id: Some(public_sandbox_id.to_string()),
        payload: CreateSandboxOperationPayload {
            sandbox_id: public_sandbox_id,
            request: public_create_request(),
            node_pool: None,
            federated_create: None,
            pool_claim: None,
        }
        .into_json(),
        attempts: 1,
        claim_generation: 1,
        traceparent: None,
    }
}

// Direct is only a placeholder for the existing-row tests, which never reach the
// substrate. The admitted re-execution test differs on Kubernetes in two places:
// its `sandbox_k8s_executions` check can only fail there, and its pool claim makes
// the executor ask `admitted_pool_cleanup_pending`, which Direct answers with the
// trait default `false` while Kubernetes reads the reserved slot, so a slot whose
// cleanup is pending yields workflow progress instead of `Succeeded`.
fn direct_sandbox_controller(pool: &PgPool) -> SandboxController {
    SandboxController::new(crate::controllers::substrate::Substrate::Direct(Box::new(
        crate::controllers::substrate::direct::DirectSubstrate::new(pool.clone()),
    )))
}

#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn native_create_executor_preserves_operator_policy_across_replacements() -> TestResult {
    use std::sync::{Arc, Mutex};

    use crate::controllers::substrate::kubernetes::tests::{
        running_pod_kubernetes_client, substrate_with_store_client_proxy_port_and_pod,
    };
    use crate::tls_interception_exemptions::TlsInterceptionExemptions;
    use pplx_space_net_common::NetworkPolicyV1;

    // The fully instrumented native create path needs more than libtest's default stack.
    if !pplx_space_test_env::enter_test_process(&[(
        "RUST_MIN_STACK",
        Some(std::ffi::OsStr::new("16777216")),
    )]) {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let org_id = seed_org(&db.pool).await?;
    let user_id = seed_user(&db.pool, org_id).await?;
    let policy: NetworkPolicyV1 = serde_json::from_value(serde_json::json!({
        "egress": {
            "default_action": "deny",
            "allow": [{"domain": "allowed.example.test"}],
            "deny": [{"domain": "blocked.example.test"}]
        },
        "tls_interception": "on"
    }))?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id, visibility, network_policy_json)
         VALUES ($1, 'seed', NULL, 'generic', $2)"
    ))
    .bind(Uuid::now_v7())
    .bind(serde_json::to_value(&policy)?)
    .execute(&db.pool)
    .await?;
    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'seed', 'fc', 'sha256:native-policy-test')"
    ))
    .bind(release_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'seed', 'latest')"
    ))
    .bind(release_id)
    .execute(&db.pool)
    .await?;

    let exemptions =
        TlsInterceptionExemptions::from_config(r#"[{"wildcard_domain":"*.relay.example.test"}]"#)?;
    let profiles = crate::egress_profiles::EgressProfiles::from_config(
        r#"{
        "sensor":{"default":true,"allow":[{"domain":"sensor.example.test"}],
                  "tls_interception_exemptions":[{"domain":"sensor.example.test"}]}
    }"#,
    )?;
    let owner = crate::federated_identity::FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse("profile-org")?,
        subject_id: UpstreamSubjectId::parse("profile-subject")?,
    };
    let effective_owner = crate::federated_identity::materialize_owner(&db.pool, &owner).await?;
    let grant = crate::federated_identity::FederatedServiceGrant::new(
        b"test-profile-grant".to_vec(),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )?;
    let captured_pods = Arc::new(Mutex::new(Vec::new()));
    // Native create probes the pod's TCP listener even with command readiness disabled.
    let pod_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    for (configured, federated, selected) in [
        (false, false, false),
        (true, false, false),
        (true, true, false), // Legacy federated payload: no saved selection.
        (true, true, true),
    ] {
        let substrate = crate::controllers::substrate::Substrate::Kubernetes(Box::new(
            substrate_with_store_client_proxy_port_and_pod(
                db.pool.clone(),
                running_pod_kubernetes_client(Arc::clone(&captured_pods), "127.0.0.1"),
                pod_listener.local_addr()?.port(),
                None,
            ),
        ));
        let controller = SandboxController::new(substrate)
            .with_egress_profiles(profiles.clone())
            .with_tls_interception_exemptions(configured.then(|| exemptions.clone()));
        let executor = CreateSandboxOperationExecutor::new(db.pool.clone(), controller);
        for inherit_template in [false, true] {
            let sandbox_id = SandboxId::new();
            let (org_id, user_id) = if federated {
                (effective_owner.org_id, effective_owner.user_id)
            } else {
                (org_id, user_id)
            };
            let mut operation = claimed_create_operation_for(sandbox_id, org_id, user_id);
            let mut context =
                crate::fork_operations::ForkFederatedCreateContext::from_request(&owner, &grant);
            // The payload freezes names, while execution uses current destinations.
            let admission = direct_sandbox_controller(&db.pool).with_egress_profiles(
                crate::egress_profiles::EgressProfiles::from_config(
                    r#"{"sensor":{"allow":[{"domain":"retired.example.test"}]}}"#,
                )?,
            );
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(crate::egress_profiles::HEADER, "+sensor".parse()?);
            context.egress_profiles = if selected {
                Some(admission.select_egress_profiles(&headers, true)?)
            } else {
                None
            };
            let mut request = public_create_request();
            request.network_policy = (!inherit_template).then(|| policy.clone());
            operation.payload = CreateSandboxOperationPayload {
                sandbox_id: sandbox_id.into(),
                request,
                node_pool: None,
                federated_create: federated.then_some(context),
                pool_claim: None,
            }
            .into_json();

            if selected {
                let unconfigured = CreateSandboxOperationExecutor::new(
                    db.pool.clone(),
                    executor
                        .controller
                        .clone()
                        .with_egress_profiles(Default::default()),
                );
                let result = Box::pin(unconfigured.execute(&operation)).await;
                assert_eq!(result.state, OperationState::Queued, "{result:?}");
                assert_eq!(result.retry_after, Some(CREATE_RETRY_AFTER));
                assert!(fetch_sandbox(&db.pool, sandbox_id).await?.is_none());
            }
            let result = Box::pin(executor.execute(&operation)).await;
            assert_eq!(result.state, OperationState::Succeeded, "{result:?}");
            assert_eq!(
                crate::egress_profiles::load(&db.pool, sandbox_id)
                    .await?
                    .as_names(),
                if selected { vec!["sensor"] } else { vec![] },
            );
            let persisted: Option<sqlx::types::Json<NetworkPolicyV1>> = sqlx::query_scalar(sql!(
                "SELECT network_policy_override_json FROM sandboxes WHERE id = $1"
            ))
            .bind(sandbox_id)
            .fetch_one(&db.pool)
            .await?;
            assert_eq!(persisted.is_some(), configured || !inherit_template);
            let effective = persisted.as_ref().map(|value| &value.0).unwrap_or(&policy);
            let mut expected = policy.clone();
            if configured {
                expected.tls_interception_exemptions = serde_json::from_value(serde_json::json!([
                    {"wildcard_domain": "*.relay.example.test"}
                ]))?;
            }
            if selected {
                assert!(effective.allows_domain("sensor.example.test"));
                assert!(!effective.allows_domain("retired.example.test"));
                assert!(!effective.allows_domain("blocked.example.test"));
                let updated_controller = executor.controller.clone().with_egress_profiles(
                    crate::egress_profiles::EgressProfiles::from_config(
                        r#"{"sensor":{"allow":[{"domain":"updated.example.test"}]}}"#,
                    )?,
                );
                assert!(!effective.allows_domain("unlisted.example.test"));
                for patch in [
                    ControllerSandboxNetworkPolicyPatch::Set {
                        policy: NetworkPolicyV1::deny_all(),
                    },
                    ControllerSandboxNetworkPolicyPatch::InheritTemplateDefault,
                ] {
                    let sandbox = fetch_sandbox(&db.pool, sandbox_id)
                        .await?
                        .expect("created sandbox");
                    updated_controller
                        .set_network_policy(&db.pool, &sandbox, patch, None)
                        .await?;
                    let updated = fetch_sandbox(&db.pool, sandbox_id)
                        .await?
                        .expect("updated sandbox");
                    let effective = updated.network_policy.expect("profile remains composed").0;
                    assert!(effective.allows_domain("updated.example.test"));
                    assert!(!effective.allows_domain("sensor.example.test"));
                    assert!(!effective.allows_domain("retired.example.test"));
                    assert!(!effective.allows_domain("unlisted.example.test"));
                    assert!(!effective.tls_interception_exemptions.contains(
                        &pplx_space_net_common::EgressDestinationV1::Domain(
                            "sensor.example.test".parse()?,
                        ),
                    ));
                    let sandbox = fetch_sandbox(&db.pool, sandbox_id).await?.unwrap();
                    let error = executor
                        .controller
                        .clone()
                        .with_egress_profiles(Default::default())
                        .set_network_policy(
                            &db.pool,
                            &sandbox,
                            ControllerSandboxNetworkPolicyPatch::Set {
                                policy: NetworkPolicyV1::deny_all(),
                            },
                            None,
                        )
                        .await
                        .expect_err("missing selected profile must fail composition");
                    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
                    assert_eq!(
                        fetch_sandbox(&db.pool, sandbox_id)
                            .await?
                            .unwrap()
                            .network_policy,
                        sandbox.network_policy,
                        "failed composition must preserve the saved policy"
                    );
                }
            } else {
                assert_eq!(effective, &expected);
            }
        }
    }
    assert_eq!(
        captured_pods.lock().expect("capture mutex poisoned").len(),
        8
    );
    Ok(())
}

async fn seed_org(pool: &PgPool) -> Result<OrgId, sqlx::Error> {
    let id = OrgId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug, byok_enabled)
         VALUES ($1, $2, $3, false)"
    ))
    .bind(id)
    .bind(format!("Org {id}"))
    .bind(format!("org-{}", id.as_uuid().simple()))
    .execute(pool)
    .await?;
    Ok(id)
}

async fn seed_user(pool: &PgPool, org_id: OrgId) -> Result<UserId, sqlx::Error> {
    let id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'hash', 'Test User', $3, 'member')"
    ))
    .bind(id)
    .bind(format!("{id}@example.test"))
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok(id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeedSandboxPlacement {
    Unplaced,
    OnNode(NodeId),
}

impl SeedSandboxPlacement {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unplaced => "unplaced",
            Self::OnNode(_) => "on_node",
        }
    }

    fn active_node_id(self) -> Option<NodeId> {
        match self {
            Self::Unplaced => None,
            Self::OnNode(node_id) => Some(node_id),
        }
    }
}

impl fmt::Display for SeedSandboxPlacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy)]
struct SandboxSeed {
    org_id: OrgId,
    user_id: UserId,
    lifecycle: SandboxLifecycle,
    placement: SeedSandboxPlacement,
}

async fn seed_sandbox(pool: &PgPool, seed: SandboxSeed) -> Result<SandboxId, sqlx::Error> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, network_policy_generation
         )
         VALUES ($1, $2, $3, 'k8s', $4, $5, $6, 1)"
    ))
    .bind(id)
    .bind(seed.user_id)
    .bind(seed.org_id)
    .bind(seed.lifecycle.as_str())
    .bind(seed.placement.as_str())
    .bind(seed.placement.active_node_id())
    .execute(pool)
    .await?;
    Ok(id)
}

#[tokio::test]
async fn unconfirmed_creating_row_retries_through_executor() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = seed_org(&db.pool).await?;
    let user_id = seed_user(&db.pool, org_id).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        SandboxSeed {
            org_id,
            user_id,
            lifecycle: SandboxLifecycle::Creating,
            placement: SeedSandboxPlacement::Unplaced,
        },
    )
    .await?;

    // A `creating` row with no `active_node_id` is an unconfirmed create
    // (in-flight, pre-placement, or a failed-rollback zombie). The executor
    // must retry, never report an idempotent success — otherwise the client
    // is told a sandbox exists that may never run.
    let executor =
        CreateSandboxOperationExecutor::new(db.pool.clone(), direct_sandbox_controller(&db.pool));
    let operation = claimed_create_operation_for(sandbox_id, org_id, user_id);
    let result = executor.execute(&operation).await;

    assert_eq!(result.state, OperationState::Queued);
    assert_eq!(result.retry_after, Some(CREATE_RETRY_AFTER));
    assert_eq!(
        result.requeue_reason,
        PublicOperationRequeueReason::TransientFailure,
        "the retry must count toward the budget"
    );
    assert!(
        fetch_sandbox(&db.pool, sandbox_id).await?.is_some(),
        "an unconfirmed creating row must not be deleted"
    );

    Ok(())
}

/// A retry can renew the grant after an earlier attempt already persisted the
/// row. Completion scrubs the grant from the payload, so the existing-row
/// branch must store it or the sandbox keeps the older, expiring grant.
#[tokio::test]
async fn existing_row_create_persists_the_renewed_grant() -> TestResult {
    let db = pg_test_db().await?;
    let controller = direct_sandbox_controller(&db.pool);
    let owner = crate::federated_identity::FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse("renewal-org")?,
        subject_id: UpstreamSubjectId::parse("renewal-subject")?,
    };
    let effective = crate::federated_identity::materialize_owner(&db.pool, &owner).await?;
    let org_id = effective.org_id;
    let user_id = effective.user_id;
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let expires_at = time::OffsetDateTime::from_unix_timestamp(now + 86_400)?;
    for (lifecycle, placement) in [
        (
            SandboxLifecycle::Started,
            SeedSandboxPlacement::OnNode(NodeId::new()),
        ),
        (SandboxLifecycle::Creating, SeedSandboxPlacement::Unplaced),
    ] {
        let sandbox_id = seed_sandbox(
            &db.pool,
            SandboxSeed {
                org_id,
                user_id,
                lifecycle,
                placement,
            },
        )
        .await?;
        let stored = crate::federated_identity::FederatedServiceGrant::new(
            b"first-attempt-grant".to_vec(),
            expires_at,
        )?
        .with_issued_at_unix(now - 600)?;
        controller
            .upsert_federated_service_grant(&db.pool, sandbox_id, &owner, &stored)
            .await?;
        let renewed = crate::federated_identity::FederatedServiceGrant::new(
            b"renewed-grant".to_vec(),
            expires_at,
        )?
        .with_issued_at_unix(now - 60)?;
        let mut operation = claimed_create_operation_for(sandbox_id, org_id, user_id);
        operation.payload = CreateSandboxOperationPayload {
            sandbox_id: PublicSandboxId::from(sandbox_id),
            request: public_create_request(),
            node_pool: None,
            federated_create: Some(
                crate::fork_operations::ForkFederatedCreateContext::from_request(&owner, &renewed),
            ),
            pool_claim: None,
        }
        .into_json();

        let result = CreateSandboxOperationExecutor::new(db.pool.clone(), controller.clone())
            .execute(&operation)
            .await;

        // Both outcomes of the existing-row branch — success, and the retry of
        // an unconfirmed row — must leave the renewed grant stored.
        let expected_state = match lifecycle {
            SandboxLifecycle::Creating => OperationState::Queued,
            _ => OperationState::Succeeded,
        };
        assert_eq!(result.state, expected_state, "{lifecycle}");
        let blob: Vec<u8> = sqlx::query_scalar(sql!(
            "SELECT grant_blob FROM sandbox_federated_service_grants WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(blob, b"renewed-grant", "{lifecycle}");
    }

    Ok(())
}

#[tokio::test]
async fn deleting_row_retries_through_executor() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = seed_org(&db.pool).await?;
    let user_id = seed_user(&db.pool, org_id).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        SandboxSeed {
            org_id,
            user_id,
            lifecycle: SandboxLifecycle::Deleting,
            placement: SeedSandboxPlacement::Unplaced,
        },
    )
    .await?;

    // A `deleting` row is the residue a create rollback leaves when substrate
    // teardown failed: it claims the row for deletion (`lifecycle='deleting'`)
    // so the leaked pod/VM stays reapable rather than deleting the row and
    // stranding the leak. A create retry that reached this row must not report
    // it as an idempotent success — the sandbox is on its way out. Retry.
    let executor =
        CreateSandboxOperationExecutor::new(db.pool.clone(), direct_sandbox_controller(&db.pool));
    let operation = claimed_create_operation_for(sandbox_id, org_id, user_id);
    let result = executor.execute(&operation).await;

    assert_eq!(result.state, OperationState::Queued);
    assert_eq!(result.retry_after, Some(CREATE_RETRY_AFTER));

    Ok(())
}

#[tokio::test]
async fn existing_rows_are_classified_exhaustively_through_executor() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = seed_org(&db.pool).await?;
    let user_id = seed_user(&db.pool, org_id).await?;
    let executor =
        CreateSandboxOperationExecutor::new(db.pool.clone(), direct_sandbox_controller(&db.pool));

    let cases = [
        (
            SandboxLifecycle::Created,
            SeedSandboxPlacement::Unplaced,
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Creating,
            SeedSandboxPlacement::Unplaced,
            OperationState::Queued,
        ),
        (
            SandboxLifecycle::Creating,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Queued,
        ),
        (
            SandboxLifecycle::Starting,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Started,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Restoring,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Pausing,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Paused,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Resuming,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Suspending,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Suspended,
            SeedSandboxPlacement::Unplaced,
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Stopping,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Stopped,
            SeedSandboxPlacement::Unplaced,
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Deleting,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Queued,
        ),
        (
            SandboxLifecycle::Checkpointing,
            SeedSandboxPlacement::OnNode(NodeId::new()),
            OperationState::Succeeded,
        ),
        (
            SandboxLifecycle::Quarantined,
            SeedSandboxPlacement::Unplaced,
            OperationState::Queued,
        ),
        (
            SandboxLifecycle::Failed,
            SeedSandboxPlacement::Unplaced,
            OperationState::Queued,
        ),
    ];

    for (lifecycle, placement, expected_state) in cases {
        let sandbox_id = seed_sandbox(
            &db.pool,
            SandboxSeed {
                org_id,
                user_id,
                lifecycle,
                placement,
            },
        )
        .await?;
        let result = executor
            .execute(&claimed_create_operation_for(sandbox_id, org_id, user_id))
            .await;

        assert_eq!(
            result.state, expected_state,
            "unexpected create-operation disposition for {lifecycle} with placement {placement:?}"
        );
        if expected_state == OperationState::Queued {
            assert_eq!(result.retry_after, Some(CREATE_RETRY_AFTER));
            assert_eq!(
                result.requeue_reason,
                PublicOperationRequeueReason::TransientFailure,
                "the retry for {lifecycle} must count toward the budget"
            );
        }
    }

    Ok(())
}

#[test]
fn generic_429_is_not_marked_admission_backpressure() {
    // A generic rate-limited controller 429 (no cluster-admission cause)
    // must NOT be treated as backpressure — otherwise the dispatcher would
    // re-queue it forever without ever exhausting the transient-error
    // budget. Backpressure keys off the admission cause, not the status.
    let op = claimed_create_operation();
    let err = crate::controllers::ControllerError::rate_limited("slow down")
        .with_retry_after_header(std::time::Duration::from_secs(1));
    let result = controller_error_result(&op, err);
    assert_eq!(
        result.requeue_reason,
        PublicOperationRequeueReason::TransientFailure
    );
}

#[tokio::test(flavor = "current_thread")]
async fn create_executor_retries_unpinned_transient_substrate_failure() {
    let db = pg_test_db().await.expect("Postgres fixture");
    let org_id = OrgId::new();
    let actor_user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (id, name, slug)
         VALUES ($1, 'Create operation retry test', $2)"
    ))
    .bind(org_id)
    .bind(format!("create-operation-retry-{org_id}"))
    .execute(&db.pool)
    .await
    .expect("organization insert");
    sqlx::query(sql!(
        "INSERT INTO users (id, email, password_hash, display_name, org_id, role)
         VALUES ($1, $2, 'unused', 'Create operation retry test', $3, 'member')"
    ))
    .bind(actor_user_id)
    .bind(format!(
        "create-operation-retry-{actor_user_id}@example.com"
    ))
    .bind(org_id)
    .execute(&db.pool)
    .await
    .expect("user insert");

    let template_release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, 'seed', NULL, NULL)
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .execute(&db.pool)
    .await
    .expect("template insert");
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'seed', 'fc', 'sha256:create-operation-retry')"
    ))
    .bind(template_release_id)
    .execute(&db.pool)
    .await
    .expect("template release insert");
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'seed', 'latest')"
    ))
    .bind(template_release_id)
    .execute(&db.pool)
    .await
    .expect("template release tag insert");

    let sandbox_id = PublicSandboxId::new();
    let operation = ClaimedPublicOperation {
        id: OperationId::new(),
        org_id,
        actor_user_id: Some(actor_user_id),
        api_product: ApiProduct::Native,
        kind: OperationKind::CreateSandbox,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(sandbox_id.to_string()),
        payload: CreateSandboxOperationPayload {
            sandbox_id,
            request: public_create_request(),
            node_pool: None,
            federated_create: None,
            pool_claim: None,
        }
        .into_json(),
        attempts: 1,
        claim_generation: 1,
        traceparent: None,
    };
    let controller =
        SandboxController::new(crate::controllers::substrate::Substrate::Direct(Box::new(
            crate::controllers::substrate::direct::DirectSubstrate::new(db.pool.clone()),
        )));
    let executor = CreateSandboxOperationExecutor::new(db.pool.clone(), controller);

    let result = executor.execute(&operation).await;

    assert_eq!(result.state, OperationState::Queued);
    assert_eq!(result.terminal_error_code, None);
    assert!(result.retry_after.is_some());
}

#[test]
fn cluster_admission_429_is_marked_backpressure() {
    let op = claimed_create_operation();
    let err = crate::controllers::ControllerError::rate_limited("cluster admission rate limited")
        .with_retry_after_header(std::time::Duration::from_secs(1))
        .with_retry_after_cause(
            crate::controllers::ControllerRetryAfterCause::ClusterActivationAdmission,
        );
    let result = controller_error_result(&op, err);
    assert_eq!(
        result.requeue_reason,
        PublicOperationRequeueReason::ActivationBackpressure
    );
}

#[test]
fn controller_create_request_uses_public_contract_defaults() {
    let controller = controller_create_request(public_create_request(), None);

    assert_eq!(controller.name.as_deref(), Some("native-create"));
    assert_eq!(controller.runtime.as_deref(), Some("fc"));
    assert_eq!(controller.boot_mode, None);
    assert!(!controller.wait_until_ready);
    assert_eq!(controller.vcpu, Some(2));
    assert_eq!(controller.memory_mib, Some(1024));
    assert_eq!(controller.disk_mib, None);
    assert_eq!(
        controller
            .network_policy
            .as_ref()
            .map(pplx_space_net_common::NetworkPolicyV1::default_action),
        Some(pplx_space_net_common::EgressDefaultAction::Allow)
    );
    assert_eq!(
        controller.metadata.get("owner"),
        Some(&ControllerSandboxMetadataValue::String(
            "native".to_string()
        )),
    );
    assert_eq!(
        controller.metadata.get("attempt"),
        Some(&ControllerSandboxMetadataValue::Number(1)),
    );
    assert_eq!(
        controller.metadata.get("debug"),
        Some(&ControllerSandboxMetadataValue::Bool(true)),
    );
    // The public contract never pins a pool.
    assert_eq!(controller.node_pool, None);
}

#[test]
fn controller_create_request_threads_node_pool() {
    let pool = NodePoolName::parse("isolated-test-pool").unwrap();
    let controller = controller_create_request(public_create_request(), Some(pool.clone()));
    assert_eq!(controller.node_pool, Some(pool));
}

#[test]
fn create_operation_payload_is_wire_compatible_with_old_shape() {
    // Unpinned payloads must serialize byte-identically to a pre-placement
    // producer: no `node_pool` key at all, so old executors accept them.
    let unpinned = CreateSandboxOperationPayload {
        sandbox_id: PublicSandboxId::new(),
        request: public_create_request(),
        node_pool: None,
        federated_create: None,
        pool_claim: None,
    };
    let value = serde_json::to_value(&unpinned).unwrap();
    assert!(
        value.get("node_pool").is_none(),
        "unpinned payload must omit node_pool: {value}"
    );

    // An old-shape payload (no key) deserializes via serde(default).
    let roundtrip: CreateSandboxOperationPayload = serde_json::from_value(value).unwrap();
    assert_eq!(roundtrip.node_pool, None);

    // A pin adds the key (rejected by an old deny_unknown_fields executor —
    // the consumer-first rollout precondition).
    let pinned = CreateSandboxOperationPayload {
        sandbox_id: PublicSandboxId::new(),
        request: public_create_request(),
        node_pool: Some(NodePoolName::parse("isolated-test-pool").unwrap()),
        federated_create: None,
        pool_claim: None,
    };
    let pinned_value = serde_json::to_value(&pinned).unwrap();
    assert_eq!(
        pinned_value.get("node_pool").and_then(|v| v.as_str()),
        Some("isolated-test-pool")
    );
}

#[test]
fn lifecycle_operation_payload_is_wire_compatible_with_old_shape() {
    let sandbox_id = PublicSandboxId::new();
    let ordinary = SandboxLifecycleOperationPayload {
        sandbox_id,
        action: SandboxLifecycleAction::Delete,
        rollback: None,
        create_rollback_fence: None,
    };
    let ordinary_value = ordinary.into_json();
    assert!(
        ordinary_value.get("create_rollback_fence").is_none(),
        "user DELETE payloads must remain byte-compatible with old consumers"
    );

    let old_shape = serde_json::json!({
        "sandbox_id": sandbox_id,
        "action": "delete"
    });
    let decoded: SandboxLifecycleOperationPayload = serde_json::from_value(old_shape).unwrap();
    assert!(decoded.create_rollback_fence.is_none());

    let compensation = SandboxLifecycleOperationPayload {
        sandbox_id,
        action: SandboxLifecycleAction::Delete,
        rollback: None,
        create_rollback_fence: Some(CreateRollbackExecutionFence::Direct {
            node_id: NodeId::new(),
            route_identity: None,
        }),
    }
    .into_json();
    assert!(
        compensation.get("create_rollback_fence").is_some(),
        "create compensation must persist the execution fence it re-proves on every attempt"
    );
}

#[test]
fn controller_update_request_uses_public_contract_fields() {
    let controller = controller_update_request(public_update_request());

    assert_eq!(controller.vcpu, Some(4));
    assert_eq!(controller.memory_mib, Some(2048));
    assert_eq!(controller.pids_max, None);
    match controller.network_policy.as_ref() {
        Some(ControllerSandboxNetworkPolicyPatch::Set { policy }) => {
            assert_eq!(
                policy.default_action(),
                pplx_space_net_common::EgressDefaultAction::Deny
            );
        }
        Some(ControllerSandboxNetworkPolicyPatch::InheritTemplateDefault) | None => {
            panic!("expected network policy set patch")
        }
    }
    assert_eq!(
        controller
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("owner")),
        Some(&ControllerSandboxMetadataValue::String(
            "updated".to_string()
        )),
    );
}

#[test]
fn lifecycle_projection_hides_internal_started_label() {
    assert_eq!(
        public_lifecycle_state("started").unwrap(),
        SandboxLifecycleState::Running,
    );
    assert_eq!(
        public_lifecycle_state("checkpointing").unwrap(),
        SandboxLifecycleState::Suspending,
    );
    assert_eq!(
        public_lifecycle_state("creating").unwrap(),
        SandboxLifecycleState::Creating,
    );
    public_lifecycle_state("renamed").unwrap_err();
}

#[test]
fn completed_lifecycle_target_state_accepts_idempotent_targets() {
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Start,
            SandboxLifecycleState::Running
        ),
        Some(SandboxLifecycleState::Running),
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Wake,
            SandboxLifecycleState::Running
        ),
        Some(SandboxLifecycleState::Running),
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Resume,
            SandboxLifecycleState::Running
        ),
        Some(SandboxLifecycleState::Running),
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Restore,
            SandboxLifecycleState::Running
        ),
        Some(SandboxLifecycleState::Running),
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Pause,
            SandboxLifecycleState::Paused
        ),
        Some(SandboxLifecycleState::Paused),
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Suspend,
            SandboxLifecycleState::Suspended
        ),
        Some(SandboxLifecycleState::Suspended),
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Stop,
            SandboxLifecycleState::Stopped
        ),
        Some(SandboxLifecycleState::Stopped),
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Kill,
            SandboxLifecycleState::Stopped
        ),
        Some(SandboxLifecycleState::Stopped),
    );
}

#[test]
fn firecracker_snapshot_restore_incompatibility_is_terminal() {
    let operation = claimed_create_operation();
    let error = ControllerError::internal(
        "node-api start error: code: 'Internal error', message: \
         \"load snapshot failed: Firecracker API PUT /snapshot/load: 400 Bad Request - \
         {\\\"fault_message\\\":\\\"Load snapshot error: Failed to restore from snapshot: \
         Failed to build microVM from snapshot: Failed to restore vCPUs: \
         Failed to run action on vcpu: Failed to set KVM vcpu xcrs: \
         Invalid argument (os error 22)\\\"}\"",
    );

    let result = controller_error_result(&operation, error);

    assert_eq!(result.state, OperationState::Failed);
    assert_eq!(
        result.terminal_error_code,
        Some(PublicErrorCode::FailedPrecondition)
    );
    assert_eq!(result.retry_after, None);
}

#[test]
fn create_precondition_is_terminal_with_controller_evidence() {
    let operation = claimed_create_operation();
    let evidence = "federated service grant is required";
    let error = ControllerError::from(
        crate::controllers::substrate::SubstrateError::FailedPrecondition {
            message: evidence.to_string(),
        },
    );

    let result = controller_error_result(&operation, error);

    assert_eq!(result.state, OperationState::Failed);
    assert_eq!(
        result.terminal_error_code,
        Some(PublicErrorCode::FailedPrecondition)
    );
    assert_eq!(result.retry_after, None);
    assert_eq!(
        result
            .terminal_error_json
            .as_ref()
            .and_then(|error| error.get("message"))
            .and_then(serde_json::Value::as_str),
        Some(evidence)
    );
}

#[test]
fn completed_lifecycle_target_state_rejects_active_transitions() {
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Start,
            SandboxLifecycleState::Stopped
        ),
        None,
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Pause,
            SandboxLifecycleState::Running
        ),
        None,
    );
    assert_eq!(
        completed_lifecycle_target_state(
            SandboxLifecycleAction::Stop,
            SandboxLifecycleState::Suspended
        ),
        None,
    );
}

fn public_create_request() -> pplx_space_public_api::CreateSandboxRequest {
    let mut env = BTreeMap::new();
    env.insert("A".to_string(), "B".to_string());

    let mut metadata = BTreeMap::new();
    metadata.insert(
        "owner".to_string(),
        PublicSandboxMetadataValue::String("native".to_string()),
    );
    metadata.insert("attempt".to_string(), PublicSandboxMetadataValue::Number(1));
    metadata.insert("debug".to_string(), PublicSandboxMetadataValue::Bool(true));

    pplx_space_public_api::CreateSandboxRequest {
        template: "seed:latest".parse::<TemplateReference>().unwrap(),
        runtime: Some(RuntimeKind::Firecracker),
        name: Some("native-create".to_string()),
        env,
        metadata,
        resources: Some(PublicSandboxResources {
            vcpu: Some(2),
            memory_mib: Some(1024),
            disk_mib: None,
        }),
        network_policy: Some(pplx_space_net_common::NetworkPolicyV1::allow_all()),
        auto_lifecycle: None,
        idle_timeout: None,
        idle_action: None,
    }
}

fn public_update_request() -> PublicUpdateSandboxRequest {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "owner".to_string(),
        PublicSandboxMetadataValue::String("updated".to_string()),
    );

    PublicUpdateSandboxRequest {
        metadata: Some(metadata),
        resources: Some(PublicSandboxResources {
            vcpu: Some(4),
            memory_mib: Some(2048),
            disk_mib: None,
        }),
        network_policy: Some(PublicSandboxNetworkPolicyPatch::Set {
            policy: pplx_space_net_common::NetworkPolicyV1::deny_all(),
        }),
        auto_lifecycle: None,
    }
}

fn admitted_pool_claim() -> CreateSandboxPoolClaim {
    CreateSandboxPoolClaim {
        claim_id: Uuid::now_v7(),
        pool_slot_id: Uuid::now_v7(),
    }
}

#[test]
fn create_operation_payload_pool_claim_is_optional_on_the_wire() {
    // Without a claim the payload is byte-identical to today's: no key.
    let cold = CreateSandboxOperationPayload {
        sandbox_id: PublicSandboxId::new(),
        request: public_create_request(),
        node_pool: None,
        federated_create: None,
        pool_claim: None,
    };
    let value = serde_json::to_value(&cold).unwrap();
    assert!(
        value.get("pool_claim").is_none(),
        "a payload without a claim must omit pool_claim: {value}"
    );

    // Payloads written before the field existed still parse, as cold creates.
    let mut old = value.clone();
    old.as_object_mut().unwrap().remove("pool_claim");
    let parsed: CreateSandboxOperationPayload = serde_json::from_value(old).unwrap();
    assert_eq!(parsed.pool_claim, None);

    // A claim round-trips exactly.
    let claim = admitted_pool_claim();
    let admitted = CreateSandboxOperationPayload {
        pool_claim: Some(claim),
        ..cold
    };
    let admitted_value = serde_json::to_value(&admitted).unwrap();
    assert_eq!(
        admitted_value["pool_claim"],
        serde_json::json!({
            "claim_id": claim.claim_id,
            "pool_slot_id": claim.pool_slot_id,
        })
    );
    let parsed: CreateSandboxOperationPayload = serde_json::from_value(admitted_value).unwrap();
    assert_eq!(parsed.pool_claim, Some(claim));

    // The claim object itself is closed, like the payload.
    let mut extended = serde_json::to_value(&admitted).unwrap();
    extended["pool_claim"]["unexpected"] = serde_json::json!(true);
    serde_json::from_value::<CreateSandboxOperationPayload>(extended).unwrap_err();
}

#[test]
fn pool_slot_release_wait_is_workflow_progress() {
    let op = claimed_create_operation();
    let err = crate::controllers::ControllerError::service_unavailable("slot still held")
        .with_retry_after_header(std::time::Duration::from_secs(15))
        .with_retry_after_cause(crate::controllers::ControllerRetryAfterCause::PoolSlotRelease);
    let result = controller_error_result(&op, err);
    assert_eq!(result.state, OperationState::Queued);
    assert_eq!(
        result.requeue_reason,
        PublicOperationRequeueReason::WorkflowProgress,
        "waiting for the pool to release a slot must not spend the retry budget"
    );
    assert_eq!(result.retry_after, Some(std::time::Duration::from_secs(15)));
}

#[tokio::test]
async fn admitted_create_re_execution_after_success_does_not_create_again() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = seed_org(&db.pool).await?;
    let user_id = seed_user(&db.pool, org_id).await?;
    let sandbox_id = seed_sandbox(
        &db.pool,
        SandboxSeed {
            org_id,
            user_id,
            lifecycle: SandboxLifecycle::Started,
            placement: SeedSandboxPlacement::OnNode(NodeId::new()),
        },
    )
    .await?;
    let executor =
        CreateSandboxOperationExecutor::new(db.pool.clone(), direct_sandbox_controller(&db.pool));
    let mut operation = claimed_create_operation_for(sandbox_id, org_id, user_id);
    let mut payload: CreateSandboxOperationPayload =
        serde_json::from_value(operation.payload.clone())?;
    payload.pool_claim = Some(admitted_pool_claim());
    operation.payload = payload.into_json();

    for _ in 0..2 {
        let result = executor.execute(&operation).await;
        assert_eq!(result.state, OperationState::Succeeded);
    }

    let sandboxes: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes WHERE id = $1"))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(sandboxes, 1);
    let executions: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        executions, 0,
        "re-execution must not start another execution"
    );
    Ok(())
}

/// Create compensation whose sandbox row is already gone still has to reach the
/// node. A best-effort user DELETE retires the row even when its node call
/// failed, so "the row is gone" says nothing about the guest; on the direct
/// substrate nothing else would ever tear it down. The fence here names a node
/// with no row, so reaching it fails, and the operation must not report success.
///
/// The control: an ordinary user DELETE of a missing row is still an idempotent
/// success, so the compensation result above is not just a broken executor.
#[tokio::test]
async fn compensation_for_a_missing_row_does_not_report_done_without_the_node() -> TestResult {
    let db = pg_test_db().await?;
    let org_id = seed_org(&db.pool).await?;
    let sandbox_id = PublicSandboxId::from(SandboxId::new());
    let executor = SandboxLifecycleOperationExecutor::new(
        db.pool.clone(),
        direct_sandbox_controller(&db.pool),
    );
    let operation = |kind, fence| ClaimedPublicOperation {
        id: OperationId::new(),
        org_id,
        actor_user_id: None,
        api_product: ApiProduct::Native,
        kind,
        target_type: Some(OperationTargetType::Sandbox),
        target_id: Some(sandbox_id.to_string()),
        payload: SandboxLifecycleOperationPayload {
            sandbox_id,
            action: SandboxLifecycleAction::Delete,
            rollback: None,
            create_rollback_fence: fence,
        }
        .into_json(),
        attempts: 1,
        claim_generation: 1,
        traceparent: None,
    };

    let compensation = executor
        .execute(&operation(
            OperationKind::CompensateCreateSandbox,
            Some(CreateRollbackExecutionFence::Direct {
                node_id: NodeId::new(),
                route_identity: None,
            }),
        ))
        .await;
    assert_ne!(
        compensation.state,
        OperationState::Succeeded,
        "compensation reported a teardown it never confirmed: {}",
        compensation.progress_message
    );

    let user_delete = executor
        .execute(&operation(OperationKind::DeleteSandbox, None))
        .await;
    assert_eq!(user_delete.state, OperationState::Succeeded);
    Ok(())
}
