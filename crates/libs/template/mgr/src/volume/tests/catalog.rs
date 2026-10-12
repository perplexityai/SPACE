use crate::volume::{PullFailure, PullOutcome, PullTrigger, VolumePuller};
use pplx_space_common_core::types::OrgId;
use pplx_space_common_svc::auth::{NodeApiAuthConfig, NodeApiAuthLayer, NodeRole};
use pplx_space_internal_api_server::ClusterApiService;
use pplx_space_node_api_client::proto::{
    RegisterTemplateReleaseRequest, RegisterTemplateRequest, TemplateVisibility,
    cluster_api_server::{ClusterApi, ClusterApiServer},
};
use pplx_space_test_env::{TestEnv, TestResult, enter_test_process};
use std::ffi::OsStr;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::TcpListenerStream;

const TEMPLATE: &str = "prefetch-catalog-fixture";
const TOKEN: &str = "synthetic-prefetch-catalog-token";
const CHILD_ENDPOINT: &str = "SPACE_TEST_PREFETCH_CATALOG_ENDPOINT";

struct Catalog {
    endpoint: String,
    token_file: tempfile::NamedTempFile,
    server: tokio::task::JoinHandle<()>,
    _database: pplx_space_db_test_fixtures::TestDb,
}

impl Drop for Catalog {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn publisher<T>(body: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(body);
    request.extensions_mut().insert(
        pplx_space_common_svc::auth::build::BuildAuthenticatedCaller::trusted_by_configuration(),
    );
    request
}

impl Catalog {
    async fn start(visibility: TemplateVisibility) -> TestResult<Self> {
        let database = pplx_space_db_test_fixtures::pg_test_db().await?;
        let mut service = ClusterApiService::new(database.pool.clone());
        // The shared PostgreSQL fixture owns this synthetic organization.
        let owner: OrgId = "00000000-0000-0000-0000-000000000003".parse()?;
        let org_id = (visibility == TemplateVisibility::OrgSpecific).then(|| owner.to_string());
        service
            .register_template(publisher(RegisterTemplateRequest {
                name: TEMPLATE.into(),
                org_id: org_id.clone(),
                ..Default::default()
            }))
            .await?;
        service
            .register_template_release(publisher(RegisterTemplateReleaseRequest {
                template_name: TEMPLATE.into(),
                runtime: "fc".into(),
                hash: format!("sha256:{}", "a".repeat(64)),
                tags: vec!["latest".into()],
                org_id,
                ..Default::default()
            }))
            .await?;
        if visibility == TemplateVisibility::Unspecified {
            let unavailable = sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost:1/unused")?;
            unavailable.close().await;
            service = service.with_visibility_pool(unavailable);
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let token_file = tempfile::NamedTempFile::new()?;
        std::fs::write(token_file.path(), TOKEN)?;
        let config = NodeApiAuthConfig::from_environment(
            &TestEnv::default(),
            NodeRole::Cluster,
            TOKEN.into(),
            None,
        )?;
        let auth = NodeApiAuthLayer::from_config(config).await?;
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(auth)
                .add_service(ClusterApiServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Ok(Self {
            endpoint,
            token_file,
            server,
            _database: database,
        })
    }
}

#[rstest::rstest]
#[case::generic(TemplateVisibility::Generic)]
#[case::private(TemplateVisibility::OrgSpecific)]
#[case::unknown(TemplateVisibility::Unspecified)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_catalog_resolution_controls_prefetch(
    #[case] visibility: TemplateVisibility,
    #[values(
        PullTrigger::PrefetchTag,
        PullTrigger::PrefetchLatestRelease,
        PullTrigger::OnDemand
    )]
    trigger: PullTrigger,
    #[values(None, Some("false"))] retired_setting: Option<&str>,
) -> TestResult {
    if std::env::var_os(CHILD_ENDPOINT).is_some() {
        assert!(enter_test_process(&[]));
        return assert_pull_outcome(visibility, trigger).await;
    }

    let catalog = Catalog::start(visibility).await?;
    // Keep the parent's authenticated server alive while the isolated child
    // exercises the production environment-driven client and pull entry point.
    assert!(!enter_test_process(&[
        (CHILD_ENDPOINT, Some(OsStr::new(&catalog.endpoint))),
        (
            "SPACE_INTERNAL_API_ENDPOINT",
            Some(OsStr::new(&catalog.endpoint))
        ),
        ("SPACE_INTERNAL_TOKEN", Some(OsStr::new(TOKEN))),
        (
            "SPACE_CLUSTER_API_TOKEN_PATH",
            Some(catalog.token_file.path().as_os_str())
        ),
        (
            "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY",
            Some(OsStr::new("off"))
        ),
        ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
        (
            "SPACE_TEMPLATE_PREFETCH_VISIBILITY_ENFORCEMENT",
            retired_setting.map(OsStr::new)
        ),
    ]));
    Ok(())
}

async fn assert_pull_outcome(visibility: TemplateVisibility, trigger: PullTrigger) -> TestResult {
    let release = crate::volume::find_release_inner(TEMPLATE, "", "latest", None).await?;
    assert_eq!(release.visibility, visibility);
    let db = Arc::new(Mutex::new(crate::db::test_conn()));

    let outcome = VolumePuller::new()
        .pull(db, TEMPLATE, &release.hash, "", trigger)
        .await;

    // No locations are registered: reaching the materializer is observable
    // without host cache writes or a volume-manager dependency.
    let expected = match (trigger, visibility) {
        (PullTrigger::OnDemand, _) | (_, TemplateVisibility::Generic) => {
            PullFailure::NoStorageLocations
        }
        (_, TemplateVisibility::OrgSpecific) => PullFailure::ClusterMetadataMissing,
        _ => PullFailure::TemplateNotGeneric,
    };
    assert!(
        matches!(outcome, PullOutcome::ClassifiedFailure(actual) if actual == expected),
        "{outcome:?}"
    );
    Ok(())
}
