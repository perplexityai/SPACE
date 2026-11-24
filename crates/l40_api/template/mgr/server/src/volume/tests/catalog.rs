use crate::volume::{PullFailure, PullOutcome, PullTrigger, VolumePuller};
use pplx_space_accounts::OrgId;
use pplx_space_internal_api_server::ClusterApiService;
use pplx_space_node_api_client::proto::{
    RegisterTemplateReleaseRequest, RegisterTemplateRequest, TemplateVisibility,
    cluster_api_server::{ClusterApi, ClusterApiServer},
};
use pplx_space_rpc_auth::{NodeApiAuthConfig, NodeApiAuthLayer, NodeRole};
use pplx_space_template_mgr_client::lookup_scope::TemplateLookupScope;
use pplx_space_test_env::{TestEnv, TestResult, enter_test_process};
use std::ffi::OsStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::body::Body;
use tonic::codegen::{BoxFuture, Service, http};

pub(crate) const TEMPLATE: &str = "prefetch-catalog-fixture";
pub(crate) const TOKEN: &str = "synthetic-prefetch-catalog-token";
const CHILD_ENDPOINT: &str = "SPACE_TEST_PREFETCH_CATALOG_ENDPOINT";

pub(crate) struct Catalog {
    pub(crate) endpoint: String,
    pub(crate) token_file: tempfile::NamedTempFile,
    pub(crate) lookup_scopes: Arc<Mutex<Vec<Option<TemplateLookupScope>>>>,
    pub(crate) rendezvous: Arc<Rendezvous>,
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
    request
        .extensions_mut()
        .insert(pplx_space_rpc_auth::build::BuildAuthenticatedCaller::trusted_by_configuration());
    request
}

/// Holds each request until `parties` have arrived, or for [`Self::WAIT`],
/// so a test can tell lookups that overlap from ones that run in turn.
pub(crate) struct Rendezvous {
    parties: usize,
    arrived: tokio::sync::watch::Sender<usize>,
    waited_out: AtomicBool,
}

impl Rendezvous {
    const WAIT: Duration = Duration::from_secs(2);

    async fn arrive(&self) {
        self.arrived.send_modify(|arrived| *arrived += 1);
        let mut arrived = self.arrived.subscribe();
        let met = tokio::time::timeout(
            Self::WAIT,
            arrived.wait_for(|arrived| *arrived >= self.parties),
        )
        .await;
        if met.is_err() {
            self.waited_out.store(true, Ordering::SeqCst);
        }
    }

    /// Requests that reached the catalog.
    pub(crate) fn arrived(&self) -> usize {
        *self.arrived.borrow()
    }

    /// Whether a request waited out [`Self::WAIT`] alone.
    pub(crate) fn waited_out(&self) -> bool {
        self.waited_out.load(Ordering::SeqCst)
    }
}

/// The catalog's service behind a [`Rendezvous`].
#[derive(Clone)]
struct Rendezvoused<S> {
    inner: S,
    rendezvous: Arc<Rendezvous>,
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for Rendezvoused<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for Rendezvoused<S>
where
    S: Service<http::Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        // `poll_ready` readied `self.inner`, so that's the one to call; the
        // clone stays behind for the next request.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let rendezvous = Arc::clone(&self.rendezvous);
        Box::pin(async move {
            rendezvous.arrive().await;
            inner.call(request).await
        })
    }
}

/// The catalog's service as a cluster that predates twins serves it: one
/// that doesn't know the environment selector of a release lookup, so it
/// answers as for no environment and leaves `environment_applied` false. The
/// selector is cleared from each tag and latest-releases request before the
/// service reads it.
#[derive(Clone)]
struct SelectorIgnored<S> {
    inner: S,
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for SelectorIgnored<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> SelectorIgnored<S> {
    /// `body`, a unary gRPC request, with `M`'s selector cleared by `clear`.
    /// A unary body is one frame: a compression flag byte, the message's
    /// length as 4 big-endian bytes, then the message. A test fake, so a
    /// request it can't read panics the connection rather than answer.
    async fn cleared<M: prost::Message + Default>(body: Body, clear: fn(&mut M)) -> Body {
        use http_body_util::BodyExt;
        use prost::bytes::{BufMut, Bytes};
        let framed = body.collect().await.expect("a request body").to_bytes();
        let (header, message) = framed.split_at(5);
        assert_eq!(header[0], 0, "the fake reads only uncompressed requests");
        let mut message = M::decode(message).expect("a release lookup request");
        clear(&mut message);
        let encoded = message.encode_to_vec();
        let mut reframed = Vec::with_capacity(5 + encoded.len());
        reframed.put_u8(0);
        reframed.put_u32(u32::try_from(encoded.len()).expect("a small test message"));
        reframed.extend_from_slice(&encoded);
        Body::new(http_body_util::Full::new(Bytes::from(reframed)))
    }
}

impl<S> Service<http::Request<Body>> for SelectorIgnored<S>
where
    S: Service<http::Request<Body>> + tonic::server::NamedService + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        use pplx_space_node_api_client::proto::{
            FindTemplateReleaseRequest, ListLatestTemplateReleasesRequest,
        };
        // As in `Rendezvoused`: call the readied service, keep a clone.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let method = request.uri().path().strip_prefix(&format!("/{}/", S::NAME));
        let method = method.map(str::to_owned);
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let body = match method.as_deref() {
                Some("FindTemplateRelease") => {
                    Self::cleared(body, |request: &mut FindTemplateReleaseRequest| {
                        request.environment = None;
                    })
                    .await
                }
                Some("ListLatestTemplateReleases") => {
                    Self::cleared(body, |request: &mut ListLatestTemplateReleasesRequest| {
                        request.environment = None;
                    })
                    .await
                }
                _ => body,
            };
            inner.call(http::Request::from_parts(parts, body)).await
        })
    }
}

impl Catalog {
    pub(crate) async fn start(visibility: TemplateVisibility) -> TestResult<Self> {
        Self::start_with(visibility, 0, false).await
    }

    /// A catalog that holds each request until `parties` have arrived; see
    /// [`Rendezvous`].
    pub(crate) async fn start_with_rendezvous(
        visibility: TemplateVisibility,
        parties: usize,
    ) -> TestResult<Self> {
        Self::start_with(visibility, parties, false).await
    }

    /// A catalog that ignores the environment selector, as one that predates
    /// twins does; see [`SelectorIgnored`].
    pub(crate) async fn start_ignoring_selector(
        visibility: TemplateVisibility,
    ) -> TestResult<Self> {
        Self::start_with(visibility, 0, true).await
    }

    async fn start_with(
        visibility: TemplateVisibility,
        parties: usize,
        ignore_selector: bool,
    ) -> TestResult<Self> {
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
        let lookup_scopes = Arc::new(Mutex::new(Vec::new()));
        let observed_scopes = Arc::clone(&lookup_scopes);
        let rendezvous = Arc::new(Rendezvous {
            parties,
            arrived: tokio::sync::watch::Sender::new(0),
            waited_out: AtomicBool::new(false),
        });
        let held = Arc::clone(&rendezvous);
        let api = ClusterApiServer::with_interceptor(service, move |request: tonic::Request<()>| {
            observed_scopes
                .lock()
                .unwrap()
                .push(TemplateLookupScope::from_request(&request)?);
            Ok(request)
        });
        let incoming = TcpListenerStream::new(listener);
        // Each arm is its own service type, so each builds its own server.
        let server = tokio::spawn(async move {
            let mut router = tonic::transport::Server::builder().layer(auth);
            let served = if ignore_selector {
                router
                    .add_service(Rendezvoused {
                        inner: SelectorIgnored { inner: api },
                        rendezvous: held,
                    })
                    .serve_with_incoming(incoming)
                    .await
            } else {
                router
                    .add_service(Rendezvoused {
                        inner: api,
                        rendezvous: held,
                    })
                    .serve_with_incoming(incoming)
                    .await
            };
            served.unwrap();
        });
        Ok(Self {
            endpoint,
            token_file,
            lookup_scopes,
            rendezvous,
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
    assert_eq!(
        *catalog.lookup_scopes.lock().unwrap(),
        vec![Some(TemplateLookupScope::Cluster); 2]
    );
    Ok(())
}

async fn assert_pull_outcome(visibility: TemplateVisibility, trigger: PullTrigger) -> TestResult {
    let release = crate::volume::find_release_inner(TEMPLATE, "", "latest", None).await?;
    assert_eq!(release.visibility, visibility);
    let db = crate::db::SharedConnection::new(crate::db::test_conn());

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

#[tokio::test]
async fn catalog_receipts_preserve_legacy_org_checks() -> TestResult {
    use pplx_space_node_api_client::proto::{
        FindTemplateReleaseByHashRequest, FindTemplateReleaseRequest,
        cluster_api_client::ClusterApiClient,
    };
    use pplx_space_template_mgr_client::lookup_scope::TemplateLookupScope;
    let catalog = Catalog::start(TemplateVisibility::OrgSpecific).await?;
    let mut client = ClusterApiClient::connect(catalog.endpoint.clone()).await?;
    let digest = format!("sha256:{}", "a".repeat(64));
    let owner = "00000000-0000-0000-0000-000000000003";
    let foreign = "00000000-0000-0000-0000-000000000004";
    for scope in [None, Some(TemplateLookupScope::Cluster)] {
        for org in [owner, foreign] {
            for by_hash in [false, true] {
                let metadata = |request: &mut tonic::metadata::MetadataMap| {
                    request.insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
                };
                let result = if by_hash {
                    let mut request = tonic::Request::new(FindTemplateReleaseByHashRequest {
                        environment: None,
                        template_name: TEMPLATE.into(),
                        hash: digest.clone(),
                        caller_org_id: Some(org.into()),
                        generic_only: false,
                    });
                    metadata(request.metadata_mut());
                    if let Some(scope) = scope {
                        scope.attach(&mut request);
                    }
                    client.find_template_release_by_hash(request).await
                } else {
                    let mut request = tonic::Request::new(FindTemplateReleaseRequest {
                        environment: None,
                        template_name: TEMPLATE.into(),
                        runtime: "fc".into(),
                        tag: "latest".into(),
                        caller_org_id: Some(org.into()),
                        generic_only: false,
                    });
                    metadata(request.metadata_mut());
                    if let Some(scope) = scope {
                        scope.attach(&mut request);
                    }
                    client.find_template_release(request).await
                };
                if org == foreign {
                    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
                } else {
                    let response = result?;
                    assert_eq!(response.get_ref().release.as_ref().unwrap().hash, digest);
                    assert_eq!(response.get_ref().authorized_org_id.as_deref(), Some(owner));
                    assert_eq!(
                        response
                            .metadata()
                            .contains_key("x-space-template-lookup-scope-received"),
                        scope.is_some()
                    );
                    assert!(
                        !response
                            .metadata()
                            .contains_key("x-space-template-lookup-scope")
                    );
                }
            }
        }
    }
    Ok(())
}
