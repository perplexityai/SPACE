//! ClusterApi behind node-api's real auth layer and a TokenReview double, for
//! this crate's tests and another crate's end-to-end test. Only a
//! dev-dependency enables the feature: it enrolls workers and grants firewall
//! rules authorities, which production never does.

use std::collections::HashMap;
use std::ffi::OsString;

use k8s_openapi::api::authentication::v1::TokenReviewSpec;
use pplx_space_accounts::NodeId;
use pplx_space_rpc_auth::test_support::{TokenReviewStatus, serve_token_reviews};
use pplx_space_rpc_auth::{NodeApiAuthConfig, NodeApiAuthLayer, NodeRole};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::ClusterApiService;
use crate::firewall_rules::Enrollment;

/// The static token the reviewed ClusterApi also accepts.
pub const REVIEWED_STATIC_TOKEN: &str = "synthetic-static-token";

/// Answers a TokenReview's spec.
pub trait Review: Fn(TokenReviewSpec) -> TokenReviewStatus + Clone + Send + Sync + 'static {}

impl<F> Review for F where
    F: Fn(TokenReviewSpec) -> TokenReviewStatus + Clone + Send + Sync + 'static
{
}

/// A reviewed ClusterApi, served until dropped.
pub struct ReviewedServer {
    /// Its plaintext `http://` address.
    pub endpoint: String,
    _server: AbortOnDropHandle<Result<(), tonic::transport::Error>>,
    _reviewer: AbortOnDropHandle<std::io::Result<()>>,
}

/// Serve the firewall rules RPCs on `pool` to node-ctl, with both authorities,
/// for the `enrolled` workers; tokens are reviewed by `review`.
pub async fn serve_firewall_rules(
    pool: sqlx::PgPool,
    enrolled: Vec<NodeId>,
    review: impl Review,
) -> ReviewedServer {
    let keyring = pplx_space_ecw_credential::EcwCredentialKeyring::from_current_key_for_test(
        b"firewall-rules-test-key",
    );
    let mut service = ClusterApiService::with_security_state(pool, None, keyring.into());
    service.firewall_enrollment = Enrollment::Fixture(enrolled);
    let node_ctl = Some("space-node-ctl");
    serve_reviewed(service, "space-node-ctl", node_ctl, node_ctl, review).await
}

/// Serve `service`, admitting the `allowed` service accounts
/// (comma-separated). With `firewall_rules_fetch` or `firewall_rules_report`,
/// that account holds the authority production withholds.
pub(crate) async fn serve_reviewed(
    service: ClusterApiService,
    allowed: &str,
    firewall_rules_fetch: Option<&str>,
    firewall_rules_report: Option<&str>,
    review: impl Review,
) -> ReviewedServer {
    let review_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the TokenReview double");
    let review_address = review_listener.local_addr().expect("TokenReview address");
    let kube_config = kube::Config::new(
        format!("http://{review_address}")
            .parse()
            .expect("TokenReview URI"),
    );
    let reviewer = AbortOnDropHandle::new(tokio::spawn(serve_token_reviews(
        review_listener,
        review,
        CancellationToken::new(),
    )));
    let env = Settings(HashMap::from([
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", "true".into()),
        (
            "SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED",
            "true".into(),
        ),
        ("SPACE_K8S_TOKEN_AUTH_NAMESPACE", "space".into()),
        ("KUBERNETES_SERVICE_HOST", "127.0.0.1".into()),
        (
            "SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS",
            allowed.into(),
        ),
    ]));
    let mut config = NodeApiAuthConfig::from_environment(
        &env,
        NodeRole::Cluster,
        REVIEWED_STATIC_TOKEN.into(),
        None,
    )
    .expect("reviewed auth configuration");
    if let Some(service_account) = firewall_rules_fetch {
        config = config.grant_firewall_rules_fetch_for_test(service_account);
    }
    if let Some(service_account) = firewall_rules_report {
        config = config.grant_firewall_rules_report_for_test(service_account);
    }
    let kube = kube::Client::try_from(kube_config).expect("TokenReview client");
    let auth = NodeApiAuthLayer::with_k8s_client_for_test(config, kube);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ClusterApi");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("ClusterApi address")
    );
    let server = AbortOnDropHandle::new(tokio::spawn(
        tonic::transport::Server::builder()
            .layer(auth)
            .add_service(crate::proto::cluster_api_server::ClusterApiServer::new(
                service,
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    ));
    ReviewedServer {
        endpoint,
        _server: server,
        _reviewer: reviewer,
    }
}

/// An owned configuration, never the process's.
struct Settings(HashMap<&'static str, String>);

impl pplx_space_env_vars::Environment for Settings {
    fn var_os(&self, name: &str) -> Option<OsString> {
        self.0.get(name).map(OsString::from)
    }
}
