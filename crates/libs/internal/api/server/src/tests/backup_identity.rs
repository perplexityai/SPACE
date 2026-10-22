//! Real TokenReview middleware, live pod GET, RPCs, and PostgreSQL ownership.
use super::*;
use crate::proto::{cluster_api_client::ClusterApiClient, cluster_api_server::ClusterApiServer};
use axum::{Json, routing::get};
use pplx_space_common_svc::auth::{self, test_support::*};
use std::sync::{
    Arc,
    atomic::{AtomicU16, Ordering},
};
use tokio_stream::wrappers::TcpListenerStream;

fn authenticated<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

#[rstest::rstest]
#[case(crate::backup_ownership::IdentityMode::Shadow)]
#[case(crate::backup_ownership::IdentityMode::Compatibility)]
#[case(crate::backup_ownership::IdentityMode::Strict)]
#[tokio::test]
async fn verified_backup_rpcs_preserve_retry_authority_and_reject_replacement(
    #[case] mode: crate::backup_ownership::IdentityMode,
) -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = fixture.service.pool.clone();
    let sandbox = seed_sandbox(&pool).await?;
    let node: NodeId = sqlx::query_scalar(sql!("SELECT active_node_id FROM sandboxes WHERE id=$1"))
        .bind(sandbox)
        .fetch_one(&pool)
        .await?;
    sqlx::query(sql!("UPDATE nodes SET managed_by=$2 WHERE id=$1"))
        .bind(node)
        .bind(pplx_space_common_core::types::SubstrateKind::Kubernetes)
        .execute(&pool)
        .await?;
    sqlx::query(sql!("INSERT INTO node_k8s(node_id,k8s_node_name,k8s_node_uid) VALUES($1,'worker-a','node-original')")).bind(node).execute(&pool).await?;
    sqlx::query(sql!("INSERT INTO sandbox_k8s_executions(sandbox_id,generation,pod_name,pod_namespace,state,active,k8s_node_name,k8s_node_uid) VALUES($1,1,'sandbox-pod','space','retired',false,'worker-a','node-original')")).bind(sandbox).execute(&pool).await?;
    let pod_status = Arc::new(AtomicU16::new(200));
    let status = pod_status.clone();
    let app = token_review_service(|_| {
        let mut review = service_account_status("space-cluster-api", "system:serviceaccount:space:workers");
        review.user.as_mut().unwrap().extra = Some([
            ("authentication.kubernetes.io/pod-name".into(), vec!["worker-daemon".into()]),
            ("authentication.kubernetes.io/pod-uid".into(), vec!["daemon-original".into()]),
            ("authentication.kubernetes.io/node-name".into(), vec!["worker-a".into()]),
            ("authentication.kubernetes.io/node-uid".into(), vec!["node-original".into()]),
        ].into()); review
    }).route("/api/v1/namespaces/space/pods/worker-daemon", get(move || {
        let status = status.load(Ordering::SeqCst);
        async move { (axum::http::StatusCode::from_u16(status).unwrap(), Json(if status == 200 {
            serde_json::json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"worker-daemon","uid":"daemon-original"},"spec":{"nodeName":"worker-a","serviceAccountName":"workers"},"status":{"phase":"Running","podIPs":[{"ip":"127.0.0.1"}]}})
        } else { serde_json::json!({"apiVersion":"v1","kind":"Status","status":"Failure","message":"synthetic verification failure","reason":"Unknown","code":status}) })) }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let kube_client = kube::Client::try_from(kube::Config::new(
        format!("http://{}", listener.local_addr()?).parse()?,
    ))?;
    let api = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, app).await
    }));
    let mut env = pplx_space_test_env::TestEnv::default();
    for (key, value) in [
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", "true"),
        ("KUBERNETES_SERVICE_HOST", "synthetic"),
        ("SPACE_K8S_TOKEN_AUTH_NAMESPACE", "space"),
        ("SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS", "workers"),
    ] {
        env.set(key, value);
    }
    let config = auth::NodeApiAuthConfig::from_environment(
        &env,
        auth::NodeRole::Cluster,
        "synthetic-static".into(),
        None,
    )?;
    let layer = auth::NodeApiAuthLayer::with_k8s_client_for_test(config, kube_client.clone());
    fixture.service.backup_identity = auth::NodeIdentityVerifier::new(kube_client);
    fixture.service.backup_identity_mode = mode;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let service = fixture.service.clone();
    let rpc = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(layer)
            .add_service(ClusterApiServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    }));
    let mut client = ClusterApiClient::connect(endpoint).await?;
    let token = projected_token("system:serviceaccount:space:workers");
    let artifact = LocalArtifactId::new();
    let first = SnapshotId::new();
    let request = |id: SnapshotId| BeginArtifactBackupRequest {
        sandbox_id: sandbox.to_string(),
        local_artifact_id: artifact.to_string(),
        backup_attempt_id: id.to_string(),
        storage_uri: test_storage_uri(sandbox, id),
        artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
        artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32,
        execution: Some(test_snapshot_execution()),
        execution_generation: Some(1),
        ..Default::default()
    };
    sqlx::query(sql!("UPDATE sandbox_k8s_executions SET k8s_node_uid='older-incarnation' WHERE sandbox_id=$1 AND generation=1")).bind(sandbox).execute(&pool).await?;
    if mode == crate::backup_ownership::IdentityMode::Shadow {
        // Reader rollout must tolerate historical inventory and verification
        // outages without changing the existing legacy admission policy.
        pod_status.store(503, Ordering::SeqCst);
        sqlx::query(sql!(
            "UPDATE node_k8s SET k8s_node_uid=NULL WHERE node_id=$1"
        ))
        .bind(node)
        .execute(&pool)
        .await?;
    } else {
        assert_eq!(
            client
                .begin_artifact_backup(authenticated(request(first), &token))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
    sqlx::query(sql!("UPDATE sandbox_k8s_executions SET k8s_node_uid='node-original' WHERE sandbox_id=$1 AND generation=1")).bind(sandbox).execute(&pool).await?;
    client
        .begin_artifact_backup(authenticated(request(first), &token))
        .await?;
    let source_uid: Option<String> =
        sqlx::query_scalar(sql!("SELECT source_node_uid FROM snapshots WHERE id=$1"))
            .bind(first)
            .fetch_one(&pool)
            .await?;
    if mode == crate::backup_ownership::IdentityMode::Shadow {
        assert_eq!(
            source_uid, None,
            "initial rollout must not issue proof before reader rollout"
        );
        sqlx::query(sql!(
            "UPDATE snapshots SET source_node_uid='node-original' WHERE id=$1"
        ))
        .bind(first)
        .execute(&pool)
        .await?;
        sqlx::query(sql!(
            "UPDATE node_k8s SET k8s_node_uid='node-original' WHERE node_id=$1"
        ))
        .bind(node)
        .execute(&pool)
        .await?;
        assert_eq!(
            client
                .touch_artifact_backup(authenticated(
                    TouchArtifactBackupRequest {
                        backup_attempt_id: first.to_string()
                    },
                    &token
                ))
                .await
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
        pod_status.store(200, Ordering::SeqCst);
        let retry = SnapshotId::new();
        client
            .begin_artifact_backup(authenticated(request(retry), &token))
            .await?;
        let retry_uid: Option<String> =
            sqlx::query_scalar(sql!("SELECT source_node_uid FROM snapshots WHERE id=$1"))
                .bind(retry)
                .fetch_one(&pool)
                .await?;
        assert_eq!(
            retry_uid.as_deref(),
            Some("node-original"),
            "shadow rollback must not downgrade verified retry provenance"
        );
        return Ok(());
    }
    assert_eq!(source_uid.as_deref(), Some("node-original"));
    // Move placement and materialize a successor. Prior verified bytes remain retryable.
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp=NOW()-INTERVAL '1 hour' WHERE id=$1"
    ))
    .bind(first)
    .execute(&pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id=NULL,target_node_id=NULL WHERE id=$1"
    ))
    .bind(sandbox)
    .execute(&pool)
    .await?;
    sqlx::query(sql!("INSERT INTO sandbox_k8s_executions(sandbox_id,generation,pod_name,pod_namespace,state,active,pod_materialized_at) VALUES($1,2,'successor-pod','space','running',true,NOW())")).bind(sandbox).execute(&pool).await?;
    let retry = SnapshotId::new();
    client
        .begin_artifact_backup(authenticated(request(retry), &token))
        .await?;
    let clocks: (bool,bool) = sqlx::query_as(sql!("SELECT timestamp=(SELECT timestamp FROM snapshots WHERE id=$1),last_attempt_at>timestamp FROM snapshots WHERE id=$2")).bind(first).bind(retry).fetch_one(&pool).await?;
    assert_eq!(clocks, (true, true));
    let touch = || TouchArtifactBackupRequest {
        backup_attempt_id: retry.to_string(),
    };
    let finalize = || FinalizeArtifactBackupRequest {
        backup_attempt_id: retry.to_string(),
        target_state: SuspendSnapshotTerminalState::Ready as i32,
        bytes_uploaded: 10,
        ..Default::default()
    };
    client
        .touch_artifact_backup(authenticated(touch(), &token))
        .await?;
    for (status, expected) in [(404, Code::PermissionDenied), (503, Code::Unavailable)] {
        pod_status.store(status, Ordering::SeqCst);
        assert_eq!(
            client
                .touch_artifact_backup(authenticated(touch(), &token))
                .await
                .unwrap_err()
                .code(),
            expected
        );
        assert_eq!(
            client
                .finalize_artifact_backup(authenticated(finalize(), &token))
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    pod_status.store(200, Ordering::SeqCst);
    // Static authentication cannot downgrade an already verified attempt.
    assert_eq!(
        client
            .touch_artifact_backup(authenticated(touch(), "synthetic-static"))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    sqlx::query(sql!(
        "UPDATE node_k8s SET k8s_node_uid='node-replacement' WHERE node_id=$1"
    ))
    .bind(node)
    .execute(&pool)
    .await?;
    assert_eq!(
        client
            .begin_artifact_backup(authenticated(request(SnapshotId::new()), &token))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        client
            .finalize_artifact_backup(authenticated(finalize(), &token))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    sqlx::query(sql!(
        "UPDATE node_k8s SET k8s_node_uid='node-original' WHERE node_id=$1"
    ))
    .bind(node)
    .execute(&pool)
    .await?;
    client
        .finalize_artifact_backup(authenticated(finalize(), &token))
        .await?;
    exercise_recovery(&mut client, &pool, &token, node).await?;
    drop(client);
    drop(rpc);
    drop(api);
    Ok(())
}

async fn exercise_recovery(
    client: &mut ClusterApiClient<tonic::transport::Channel>,
    pool: &sqlx::PgPool,
    token: &str,
    node: NodeId,
) -> TestResult {
    use pplx_space_common_core::types::{K8sNodeUid, SnapshotArtifact as File};
    use snapshot_store::recovery::{
        self, ApproveRecovery, CaptureTimeEvidence, RecoveryGrantId, RecoveryManifest,
        SealedValidation,
    };
    let actor = UserId::from_uuid(SEEDED_USER_ID);
    let uid: K8sNodeUid = "node-original".parse()?;
    for scenario in [
        "ready",
        "prior-registration",
        "expired",
        "revoked",
        "wrong-proof",
        "wrong-object",
        "changed-state",
        "newer-materialized",
        "abandoned-newer",
        "race-revoke",
    ] {
        let sandbox = seed_unplaced_sandbox(pool).await?;
        let artifact = LocalArtifactId::new();
        sqlx::query(sql!("UPDATE sandboxes SET lifecycle='quarantined',current_local_artifact_id=$2,current_local_artifact_kind='full',current_local_artifact_origin='suspend' WHERE id=$1")).bind(sandbox).bind(artifact).execute(pool).await?;
        sqlx::query(sql!("INSERT INTO sandbox_k8s_executions(sandbox_id,generation,pod_name,pod_namespace,state,active,created_at,terminated_at)
            VALUES($1,1,'retained-' || $1::text,'space','retired',false,NOW()-INTERVAL '2 hours',NOW()-INTERVAL '1 hour')")).bind(sandbox).execute(pool).await?;
        if scenario == "abandoned-newer" {
            sqlx::query(sql!("INSERT INTO sandbox_k8s_executions(sandbox_id,generation,pod_name,pod_namespace,state,active) VALUES($1,2,'abandoned-pod','space','failed',false)")).bind(sandbox).execute(pool).await?;
        }
        let manifest = RecoveryManifest {
            validation: SealedValidation {
                sealed_full_suspend: true,
                evidence_reference: "synthetic validation of sealed fixture".into(),
            },
            execution: snapshot_execution_metadata(Some(&test_snapshot_execution()))?,
            metadata_binding_digest: format!("sha256:{}", "33".repeat(32)).parse()?,
            vcpu: 2,
            memory_mib: 1024,
            template_id: "synthetic-template".parse()?,
            content_hash: None,
            artifacts: [File::Rootfs, File::Memory, File::VmState]
                .into_iter()
                .map(|artifact| snapshot_store::SnapshotArtifactDigest {
                    artifact,
                    digest: format!("sha256:{}", "22".repeat(32)).parse().unwrap(),
                    size_bytes: 32,
                })
                .collect(),
        };
        let id = RecoveryGrantId::new();
        let capture_evidence = CaptureTimeEvidence::RecordedCapture {
            reference: "synthetic durable capture record".into(),
        };
        let (captured_at, expires_at): (OffsetDateTime, OffsetDateTime) = sqlx::query_as(sql!(
            "SELECT NOW()-INTERVAL '90 minutes',NOW()+INTERVAL '1 hour'"
        ))
        .fetch_one(pool)
        .await?;
        if scenario == "prior-registration" {
            // A failed upload from the old incarnation must not prevent an
            // explicit approval for the currently authenticated uploader.
            sqlx::query(sql!("INSERT INTO snapshots(id,sandbox_id,type,state,timestamp,source_node_id,source_node_uid,local_artifact_id,artifact_kind,artifact_origin,generation)
                VALUES($1,$2,'full','failed',$3,$4,'prior-incarnation',$5,'full','suspend',1)"))
                .bind(SnapshotId::new()).bind(sandbox).bind(captured_at).bind(node).bind(artifact).execute(pool).await?;
        }
        let mut tx = pool.begin().await?;
        recovery::approve(
            &mut tx,
            ApproveRecovery {
                id,
                sandbox_id: sandbox,
                artifact_id: artifact,
                generation: ExecutionGeneration::new(1)?,
                uploader_node_id: node,
                uploader_node_uid: &uid,
                captured_at,
                capture_evidence: &capture_evidence,
                expires_at,
                actor,
                reason: "validate retained fixture",
                manifest: &manifest,
            },
        )
        .await?;
        tx.commit().await?;
        if scenario == "ready" {
            // The SQL constraints protect audit consistency even from a caller
            // that bypasses the typed approval route.
            for mutation in [
                "UPDATE backup_recovery_grants SET expires_at=approved_at+INTERVAL '25 hours' WHERE id=$1",
                "UPDATE backup_recovery_grants SET revoked_at=NOW() WHERE id=$1",
                "UPDATE backup_recovery_grants SET consumed_attempt_id=space_uuid_v7() WHERE id=$1",
                "UPDATE backup_recovery_grants SET active_attempt_id=id,consumed_attempt_id=id,revoked_at=NOW(),revoked_by=approved_by,revoke_reason='conflict' WHERE id=$1",
            ] {
                let mut constraint_tx = pool.begin().await?;
                let error = sqlx::query(sqlx::AssertSqlSafe(sql!(mutation)))
                    .bind(id)
                    .execute(&mut *constraint_tx)
                    .await
                    .unwrap_err();
                assert_eq!(
                    error
                        .as_database_error()
                        .and_then(|error| error.code())
                        .as_deref(),
                    Some("23514")
                );
                constraint_tx.rollback().await?;
            }
        }
        let attempt = SnapshotId::new();
        let begin = |attempt: SnapshotId| BeginArtifactBackupRequest {
            sandbox_id: sandbox.to_string(),
            local_artifact_id: artifact.to_string(),
            backup_attempt_id: attempt.to_string(),
            storage_uri: test_storage_uri(sandbox, attempt),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32,
            artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32,
            execution: Some(test_snapshot_execution()),
            execution_generation: Some(1),
            ..Default::default()
        };
        client
            .begin_artifact_backup(authenticated(begin(attempt), token))
            .await?;
        assert_eq!(
            client
                .begin_artifact_backup(authenticated(begin(SnapshotId::new()), token))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition,
            "one active attempt: {scenario}"
        );
        let captured: bool = sqlx::query_scalar(sql!(
            "SELECT timestamp=$2 AND last_attempt_at>timestamp FROM snapshots WHERE id=$1"
        ))
        .bind(attempt)
        .bind(captured_at)
        .fetch_one(pool)
        .await?;
        assert!(captured, "grant must preserve capture time");
        let finalize = |attempt: SnapshotId| {
            let uri = pplx_space_common_core::types::StorageUri::parse(&test_storage_uri(
                sandbox, attempt,
            ))
            .unwrap();
            let mut artifact_digests: Vec<_> = manifest
                .artifacts
                .iter()
                .map(|file| crate::proto::SnapshotArtifactDigest {
                    artifact: file.artifact.file_name().into(),
                    digest: file.digest.to_string(),
                    size_bytes: file.size_bytes,
                    upload_evidence: Some(crate::proto::SnapshotUploadEvidence {
                        object_key: format!("{}/{}", uri.key_prefix, file.artifact.file_name()),
                        wire_size_bytes: 16,
                        etag: Some("synthetic-object".into()),
                        version_id: None,
                        metadata_binding_digest: None,
                    }),
                })
                .collect();
            artifact_digests.push(crate::proto::SnapshotArtifactDigest {
                artifact: "metadata.json".into(),
                digest: format!("sha256:{}", "44".repeat(32)),
                size_bytes: 5,
                upload_evidence: Some(crate::proto::SnapshotUploadEvidence {
                    object_key: format!("{}/metadata.json", uri.key_prefix),
                    wire_size_bytes: 5,
                    etag: Some("synthetic-metadata".into()),
                    version_id: None,
                    metadata_binding_digest: Some(manifest.metadata_binding_digest.to_string()),
                }),
            });
            FinalizeArtifactBackupRequest {
                backup_attempt_id: attempt.to_string(),
                target_state: SuspendSnapshotTerminalState::Ready as i32,
                bytes_uploaded: 48,
                artifact_digests,
                failure_reason: None,
            }
        };
        match scenario {
            "expired" => {
                sqlx::query(sql!("UPDATE backup_recovery_grants SET approved_at=NOW()-INTERVAL '1 hour',expires_at=NOW()-INTERVAL '1 minute' WHERE id=$1")).bind(id).execute(pool).await?;
            }
            "revoked" => {
                let mut tx = pool.begin().await?;
                assert!(recovery::revoke(&mut tx, sandbox, id, actor, "synthetic revoke").await?);
                tx.commit().await?;
            }
            "changed-state" => {
                sqlx::query(sql!("UPDATE sandboxes SET lifecycle='failed' WHERE id=$1"))
                    .bind(sandbox)
                    .execute(pool)
                    .await?;
            }
            "newer-materialized" => {
                sqlx::query(sql!("INSERT INTO sandbox_k8s_executions(sandbox_id,generation,pod_name,pod_namespace,state,active,pod_materialized_at) VALUES($1,2,'newer-pod','space','retired',false,NOW())")).bind(sandbox).execute(pool).await?;
            }
            _ => (),
        }
        let mut request = finalize(attempt);
        if scenario == "wrong-proof" {
            request.artifact_digests[0].digest = format!("sha256:{}", "55".repeat(32));
        }
        if scenario == "wrong-object" {
            request.artifact_digests[0]
                .upload_evidence
                .as_mut()
                .unwrap()
                .object_key = "other-attempt/rootfs".into();
        }
        if scenario == "race-revoke" {
            let mut gate = pool.begin().await?;
            sqlx::query(sql!("SELECT id FROM sandboxes WHERE id=$1 FOR UPDATE"))
                .bind(sandbox)
                .execute(&mut *gate)
                .await?;
            let mut writer = client.clone();
            let token = token.to_owned();
            let ready = tokio::spawn(async move {
                writer
                    .finalize_artifact_backup(authenticated(request, &token))
                    .await
            });
            let revoke_pool = pool.clone();
            let revoke = tokio::spawn(async move {
                let mut tx = revoke_pool.begin().await?;
                let revoked =
                    recovery::revoke(&mut tx, sandbox, id, actor, "concurrent revoke").await?;
                tx.commit().await?;
                Ok::<_, recovery::RecoveryError>(revoked)
            });
            gate.commit().await?;
            let ready = ready.await?;
            let revoked = revoke.await??;
            assert_ne!(
                ready.is_ok(),
                revoked,
                "exactly one Ready/revoke transition may win"
            );
            let outcome:(bool,bool,bool)=sqlx::query_as(sql!("SELECT s.state='ready',g.consumed_attempt_id IS NOT DISTINCT FROM s.id,g.revoked_at IS NOT NULL FROM snapshots s JOIN backup_recovery_grants g ON s.recovery_grant_id=g.id WHERE s.id=$1")).bind(attempt).fetch_one(pool).await?;
            assert_eq!(outcome.0, outcome.1);
            assert_ne!(outcome.0, outcome.2);
            continue;
        }
        let result = client
            .finalize_artifact_backup(authenticated(request, token))
            .await;
        if matches!(scenario, "ready" | "abandoned-newer" | "prior-registration") {
            result?;
            client
                .begin_artifact_backup(authenticated(begin(attempt), token))
                .await?;
            let mut altered = begin(attempt);
            altered.storage_uri = altered.storage_uri.replace("test-bucket", "other-bucket");
            assert_eq!(
                client
                    .begin_artifact_backup(authenticated(altered, token))
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
            client
                .finalize_artifact_backup(authenticated(finalize(attempt), token))
                .await?;
            assert_eq!(
                client
                    .begin_artifact_backup(authenticated(begin(SnapshotId::new()), token))
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition,
                "consumed grant cannot confer ordinary retry authority"
            );
            let row:(String,bool)=sqlx::query_as(sql!("SELECT s.lifecycle,g.consumed_attempt_id=$2 FROM sandboxes s JOIN backup_recovery_grants g ON g.sandbox_id=s.id WHERE g.id=$1")).bind(id).bind(attempt).fetch_one(pool).await?;
            assert_eq!(row, ("quarantined".into(), true));
        } else {
            assert_eq!(
                result.unwrap_err().code(),
                Code::FailedPrecondition,
                "{scenario}"
            );
            let state: String = sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id=$1"))
                .bind(attempt)
                .fetch_one(pool)
                .await?;
            assert_eq!(state, "creating", "failed validation must roll back Ready");
            let mut failed = finalize(attempt);
            failed.target_state = SuspendSnapshotTerminalState::Failed as i32;
            failed.artifact_digests.clear();
            client
                .finalize_artifact_backup(authenticated(failed, token))
                .await?;
            if scenario == "wrong-proof" {
                let retry = SnapshotId::new();
                client
                    .begin_artifact_backup(authenticated(begin(retry), token))
                    .await?;
                client
                    .finalize_artifact_backup(authenticated(finalize(retry), token))
                    .await?;
            }
        }
    }
    Ok(())
}
