use super::*;
use pplx_space_common_core::sql;
use pplx_space_common_svc::auth::build::BuildAuthenticatedCaller;
use pplx_space_internal_api_server::ClusterApiService;
use pplx_space_node_api_client::proto::cluster_api_server::ClusterApiServer;
use pplx_space_test_env::TestResult;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, transport::Server};
use uuid::Uuid;

#[rstest::rstest]
#[case::owner("LOCK TABLE templates IN ACCESS EXCLUSIVE MODE")]
#[case::tags("LOCK TABLE template_release_tags IN ACCESS EXCLUSIVE MODE")]
#[case::grant("LOCK TABLE template_tenant_publication_grants IN ACCESS EXCLUSIVE MODE")]
fn builder_completes_same_publication_after_database_lock_releases(
    #[case] lock: &'static str,
) -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_RPC_INSECURE_DEV",
        Some(std::ffi::OsStr::new("true")),
    )]) {
        return Ok(());
    }
    block_on(async move {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let owner = OrgId::from_uuid(pplx_space_db_test_fixtures::SEED_ORG_ID);
        let template = TemplateName::parse("retry-output")?;
        let builder = Uuid::now_v7();
        let grant = TenantPublicationGrantId::new();
        sqlx::query(sql!(
            "INSERT INTO nodes (id, address, role, managed_by, state)
        VALUES ($1, '127.0.0.1', 'builder', 'space', 'ready')"
        ))
        .bind(builder)
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO templates (name, org_id, visibility, id)
        VALUES ('retry-output', $1, 'org_specific', $2)"
        ))
        .bind(owner)
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        sqlx::query(sql!("INSERT INTO template_tenant_publication_grants (id, template_name, org_id, builder_node_id)
        VALUES ($1, 'retry-output', $2, $3)"))
        .bind(grant).bind(owner).bind(builder).execute(&db.pool).await?;
        let service_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_with(
                (*db.pool.connect_options())
                    .clone()
                    .options([("lock_timeout", "100ms")]),
            )
            .await?;
        let mut blocker = db.pool.begin().await?;
        sqlx::query(sqlx::AssertSqlSafe(sql!(lock)))
            .execute(&mut *blocker)
            .await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let retry = Arc::new(tokio::sync::Notify::new());
        let second_attempt = Arc::clone(&retry);
        let service = ClusterApiServer::with_interceptor(
            ClusterApiService::new(service_pool),
            move |mut request: Request<()>| {
                if observed.fetch_add(1, Ordering::SeqCst) == 1 {
                    second_attempt.notify_one();
                }
                request
                    .extensions_mut()
                    .insert(BuildAuthenticatedCaller::trusted_by_configuration());
                Ok(request)
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let shutdown = CancellationToken::new();
        let _cleanup = shutdown.clone().drop_guard();
        let server = tokio::spawn(
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.clone().cancelled_owned(),
                ),
        );
        let mut client =
            pplx_space_node_api_client::cluster::connect(endpoint, "test-token").await?;
        let request = RegisterRequest::Release(Box::new(release_registration_request(
            &template,
            sample_hash(),
            &["latest".into()],
            vec![],
            TemplatePublication::Tenant {
                org: TemplateOrg::new(owner),
                grant: Some(grant),
            },
            None,
        )));
        // Keep the real retry loop; eliminate backoff wall time, not RPC or SQL work.
        let schedule = RegisterRetrySchedule {
            backoff: [Duration::ZERO; 4].iter(),
        };
        let release_lock = async {
            retry.notified().await;
            blocker
                .rollback()
                .await
                .map_err(|error| tonic::Status::internal(error.to_string()))
        };

        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::try_join!(
                register_with_retry_schedule(&mut client, &template, request, schedule),
                release_lock
            )
        })
        .await??;

        assert!(attempts.load(Ordering::SeqCst) >= 2);
        let (releases, grants): (i64, i64) = sqlx::query_as(sql!(
            "SELECT (SELECT count(*) FROM template_releases WHERE template_name = 'retry-output'),
                (SELECT count(*) FROM template_tenant_publication_grants WHERE id = $1)"
        ))
        .bind(grant)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!((releases, grants), (1, 0));
        let latest: String = sqlx::query_scalar(sql!(
            "SELECT r.hash FROM template_release_tags t
             JOIN template_releases r ON r.id = t.template_release_id
             WHERE t.template_name = 'retry-output' AND t.tag = 'latest'"
        ))
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(latest, sample_hash().to_string());
        shutdown.cancel();
        server.await??;
        Ok(())
    })
}
