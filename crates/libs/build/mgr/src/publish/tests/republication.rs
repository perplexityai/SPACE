use super::*;

use std::ffi::OsStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use pplx_space_common_core::sql;
use pplx_space_common_svc::auth::{NodeApiAuthenticatedCaller, build::BuildAuthenticatedCaller};
use pplx_space_credmgr::{
    auth::{ClientService, K8sIdentity},
    in_memory::InMemoryKeyWrapper,
    proto::credential_manager_server::CredentialManagerServer,
    service::{CredentialManagerService, ExchangeTokenMode},
    storage_vend_store::PgStorageVendStore,
    tcp_auth::TcpCallerIdentity,
};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, pg_test_db};
use pplx_space_internal_api_server::ClusterApiService;
use pplx_space_node_api_client::proto::cluster_api_server::ClusterApiServer;
use pplx_space_test_env::{TestResult, enter_test_process};
use tokio_stream::wrappers::{TcpListenerStream, UnixListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Status, transport::Server};
use uuid::Uuid;

/// Exercise the builder, credential authority and node-scoped catalog together.
/// Authentication handshakes are replaced with admitted builder identities;
/// authorization, status metadata, publication and persistence use real services.
#[test]
fn node_scoped_builder_republishes_private_output_without_uploading() -> TestResult {
    let sockets = tempfile::tempdir()?;
    let credential_endpoint = format!("unix://{}", sockets.path().join("credmgr.sock").display());
    let object_store = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    object_store.set_nonblocking(true)?;
    let object_endpoint = format!("http://{}", object_store.local_addr()?);
    if !enter_test_process(&[
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
        (
            "SPACE_CREDMGR_ENDPOINT",
            Some(OsStr::new(&credential_endpoint)),
        ),
        (
            "SPACE_OBJECT_STORE_BUCKET",
            Some(OsStr::new("publication-test")),
        ),
        ("SPACE_OBJECT_STORE_REGION", Some(OsStr::new("us-east-1"))),
        (
            "SPACE_OBJECT_STORE_ENDPOINT",
            Some(OsStr::new(&object_endpoint)),
        ),
        ("SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID", None),
        ("SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY", None),
        ("SPACE_OBJECT_STORE_STATIC_SESSION_TOKEN", None),
    ]) {
        assert!(
            matches!(object_store.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
            "republication must make no object-store connections"
        );
        return Ok(());
    }
    block_on(private_republication())
}

async fn private_republication() -> TestResult {
    let db = pg_test_db().await?;
    let owner = OrgId::from_uuid(SEED_ORG_ID);
    let template = TemplateName::parse("private-output")?;
    let hash = sample_hash();
    let node_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'builder', 'k8s', 'ready')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    for name in [template.as_str(), "private-base", "unrelated-private"] {
        sqlx::query(sql!(
            "INSERT INTO templates (name, org_id, visibility, id)
             VALUES ($1, $2, 'org_specific', $3)"
        ))
        .bind(name)
        .bind(owner)
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        let release_id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO template_releases (id, template_name, type, hash, org_id)
             VALUES ($1, $2, 'fc', $3, $4)"
        ))
        .bind(release_id)
        .bind(name)
        .bind(hash.to_string())
        .bind(owner)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations
                 (template_release_id, storage_connection_string, org_id)
             VALUES ($1, $2, $3)"
        ))
        .bind(release_id)
        .bind(format!(
            "s3://original-bucket/templates/{name}/{hash}?region=us-east-1"
        ))
        .bind(owner)
        .execute(&db.pool)
        .await?;
    }
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants
             (id, template_id, base_template_name, org_id, builder_node_id)
         VALUES ($1, 'private-output', 'private-base', $2, $3)"
    ))
    .bind(Uuid::now_v7())
    .bind(owner)
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let shutdown = CancellationToken::new();
    let _shutdown_on_drop = shutdown.clone().drop_guard();
    let mut servers = tokio::task::JoinSet::new();
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let cluster_service = ClusterApiServer::with_interceptor(
        ClusterApiService::new(db.pool.clone()),
        |mut request: Request<()>| {
            request
                .extensions_mut()
                .insert(NodeApiAuthenticatedCaller::unprivileged_workload_for_test());
            request
                .extensions_mut()
                .insert(BuildAuthenticatedCaller::trusted_by_configuration());
            Ok(request)
        },
    );
    servers.spawn(
        Server::builder()
            .add_service(cluster_service)
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );

    let exchanges = Arc::new(AtomicUsize::new(0));
    let deny = Arc::new(AtomicBool::new(false));
    let observed_exchanges = Arc::clone(&exchanges);
    let deny_exchange = Arc::clone(&deny);
    let authority = CredentialManagerService::new(
        Arc::new(InMemoryKeyWrapper::new()),
        pplx_space_common_svc::PeerProcessCheckMode::Off,
    )
    .with_exchange_token_mode(ExchangeTokenMode::Enforce)
    .with_storage_vend_store(Arc::new(PgStorageVendStore::new(db.pool.clone())));
    let credential_service =
        CredentialManagerServer::with_interceptor(authority, move |mut request: Request<()>| {
            observed_exchanges.fetch_add(1, Ordering::SeqCst);
            if deny_exchange.load(Ordering::SeqCst) {
                return Err(Status::permission_denied("builder authorization revoked"));
            }
            // Use a temporary Unix socket for endpoint isolation while injecting
            // the projected-token identity the builder presents in Kubernetes.
            request
                .extensions_mut()
                .remove::<tonic::transport::server::UdsConnectInfo>();
            request
                .extensions_mut()
                .insert(TcpCallerIdentity::K8sJwt(K8sIdentity::for_test(
                    ClientService::BuildManager,
                )));
            Ok(request)
        });
    let credential_endpoint = std::env::var("SPACE_CREDMGR_ENDPOINT")?;
    let socket = credential_endpoint
        .strip_prefix("unix://")
        .expect("test Unix endpoint");
    let listener = tokio::net::UnixListener::bind(socket)?;
    servers.spawn(
        Server::builder()
            .add_service(credential_service)
            .serve_with_incoming_shutdown(
                UnixListenerStream::new(listener),
                shutdown.clone().cancelled_owned(),
            ),
    );

    let mut client = pplx_space_node_api_client::cluster::connect(endpoint, "test-token").await?;
    let lookup = |name: &str| FindTemplateReleaseByHashRequest {
        template_name: name.to_owned(),
        hash: hash.to_string(),
        caller_org_id: Some(owner.to_string()),
    };
    client
        .find_template_release_by_hash(lookup("private-base"))
        .await?;
    for name in [template.as_str(), "unrelated-private"] {
        assert_eq!(
            client
                .find_template_release_by_hash(lookup(name))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
    }

    let artifacts = tempfile::tempdir()?;
    write_ready_compressed_rootfs(artifacts.path());
    let manifest_path = artifacts
        .path()
        .join(TemplateArtifact::Manifest.file_name());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&manifest_path).await?)?;
    manifest["schema_version"] = TEMPLATE_MANIFEST_SCHEMA_VERSION.into();
    manifest["execution"] = serde_json::to_value(execution_for(Arch::Aarch64))?;
    tokio::fs::write(manifest_path, serde_json::to_vec(&manifest)?).await?;
    write_metadata(artifacts.path(), Arch::Aarch64);

    // Both populated metadata and legacy NULL must survive a republish whose
    // local manifest differs from the already published artifacts.
    for (tag, stored_execution) in [
        ("republished-legacy", None),
        (
            "republished",
            Some(serde_json::to_value(execution_for(Arch::X86_64))?),
        ),
    ] {
        sqlx::query(sql!(
            "UPDATE template_releases SET execution_metadata = $1
             WHERE template_name = 'private-output'"
        ))
        .bind(stored_execution)
        .execute(&db.pool)
        .await?;
        let before = stored_release(&db.pool).await?;
        publish_build_from_dir(
            &mut client,
            artifacts.path(),
            &template,
            hash,
            &[tag.to_owned()],
            TemplatePublication::Tenant(TemplateOrg::new(owner)),
        )
        .await?;
        assert_eq!(stored_release(&db.pool).await?, before);
        let tagged = client
            .find_template_release_by_hash(lookup(template.as_str()))
            .await;
        assert_eq!(
            tagged.unwrap_err().code(),
            Code::NotFound,
            "publishing must not grant read access"
        );
        let tag_count: i64 = sqlx::query_scalar(sql!(
            "SELECT COUNT(*) FROM template_release_tags t
             JOIN template_releases r ON r.id = t.template_release_id
             WHERE t.template_name = 'private-output' AND t.tag = $1 AND r.hash = $2"
        ))
        .bind(tag)
        .bind(hash.to_string())
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(tag_count, 1, "the new tag must name the existing release");
    }
    assert_eq!(
        exchanges.load(Ordering::SeqCst),
        2,
        "hidden output must reach the real authority"
    );
    assert_eq!(
        client
            .find_template_release_by_hash(lookup("unrelated-private"))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );

    let before = stored_release(&db.pool).await?;
    deny.store(true, Ordering::SeqCst);
    let error = publish_build_from_dir(
        &mut client,
        artifacts.path(),
        &template,
        hash,
        &["denied".to_owned()],
        TemplatePublication::Tenant(TemplateOrg::new(owner)),
    )
    .await
    .expect_err("unrelated permission denial must fail publication");
    assert!(
        matches!(error, PublishError::CredentialExchange(CredentialExchangeError::Exchange(status)) if status.code() == Code::PermissionDenied)
    );
    assert_eq!(stored_release(&db.pool).await?, before);
    let denied_tags: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM template_release_tags WHERE template_name = 'private-output' AND tag = 'denied'"
    )).fetch_one(&db.pool).await?;
    assert_eq!(denied_tags, 0);

    drop(client);
    shutdown.cancel();
    while let Some(result) = servers.join_next().await {
        result??;
    }
    Ok(())
}

async fn stored_release(
    pool: &sqlx::PgPool,
) -> TestResult<(Uuid, Option<serde_json::Value>, Vec<String>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT r.id, r.execution_metadata,
                ARRAY(SELECT l.storage_connection_string FROM template_release_storage_locations l
                      WHERE l.template_release_id = r.id ORDER BY l.storage_connection_string)
         FROM template_releases r WHERE r.template_name = 'private-output'"
    ))
    .fetch_one(pool)
    .await?)
}
