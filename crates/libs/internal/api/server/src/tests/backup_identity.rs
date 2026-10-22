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
    drop(client);
    drop(rpc);
    drop(api);
    Ok(())
}
