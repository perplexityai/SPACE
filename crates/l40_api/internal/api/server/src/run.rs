//! The internal-api service body: node identity, the route-signing and ECW
//! credential material, the two `space_user` database pools, and the
//! `ClusterApi` server assembly. This is what the `space-internal-api-svc`
//! binary's `main` used to hold; the binary now only parses flags,
//! initializes telemetry, and calls [`run`].

use pplx_space_common_core::types::NodeId;

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;

use crate::auth;
use crate::{ClusterApiService, ClusterNodeInfoService};
use pplx_space_node_api_client::proto::cluster_api_server::ClusterApiServer;
use pplx_space_node_api_client::proto::node_info_server::NodeInfoServer;

/// The service name internal-api reports to telemetry and names its
/// `space_user` pool with.
pub const SERVICE_NAME: &str = "space-cluster-api";

/// Application name of the template-visibility sideline pool, kept distinct
/// from [`SERVICE_NAME`] so `pg_stat_activity` attributes a blocked lookup to
/// the sideline pool rather than to the general one.
const VISIBILITY_POOL_APPLICATION_NAME: &str = "space-cluster-api-visibility";

/// Application name of the runtime environment catalog sideline pool, distinct
/// for the same `pg_stat_activity` attribution.
const RUNTIME_CATALOG_POOL_APPLICATION_NAME: &str = "space-cluster-api-runtime-catalog";

/// Where the binary's `--listen` default points, exposed so the flag's default
/// lives next to the code that binds it.
pub const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:9800";

// A full, unpaged node census can exceed tonic's 4 MiB default on dense nodes.
// Keep a bounded receive allocation while allowing substantial census growth.
const CLUSTER_API_MAX_DECODE_BYTES: usize = 32 * 1024 * 1024;

fn cluster_api_server(service: ClusterApiService) -> ClusterApiServer<ClusterApiService> {
    ClusterApiServer::new(service).max_decoding_message_size(CLUSTER_API_MAX_DECODE_BYTES)
}

/// Connection ceiling for the template-visibility sideline pool.
///
/// Sized for steady-state concurrency, not peak: the lookup is one short
/// indexed read per release resolution, and the ceiling is the whole point —
/// see `connect_space_user_sideline_pool_lazy`. Three lets a couple of
/// concurrent lookups proceed while keeping the blast radius of a blocked
/// `templates` read far below the general pool's budget.
const VISIBILITY_POOL_MAX_CONNECTIONS: std::num::NonZeroU32 =
    std::num::NonZeroU32::new(3).expect("3 is non-zero");

/// Server-side `statement_timeout` for the template-visibility sideline pool.
///
/// The lookup is one indexed read on a small table, so anything approaching
/// this bound is contention rather than work — in practice a relation lock held
/// by `ALTER TABLE templates`. Cancelling server-side here keeps a blocked
/// lookup from adding the role's 10s `statement_timeout` to a sandbox-create
/// pull: the caller sees an ordinary query error and degrades the advisory
/// field to UNSPECIFIED. Applied per connection on this pool only, so no other
/// `space_user` query is affected.
const VISIBILITY_POOL_STATEMENT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(500);

/// Connection ceiling for the runtime environment catalog sideline pool. The
/// read is one primary-key scan of a table of a few rows, once per admitted
/// node per tick, so two connections cover steady state while capping what a
/// blocked `runtime_environments` read can hold.
const RUNTIME_CATALOG_POOL_MAX_CONNECTIONS: std::num::NonZeroU32 =
    std::num::NonZeroU32::new(2).expect("2 is non-zero");

/// Server-side `statement_timeout` for the runtime environment catalog sideline
/// pool, chosen as for [`VISIBILITY_POOL_STATEMENT_TIMEOUT`]: anything near it
/// is a relation lock, not work, and a cancelled read only omits the recipe
/// from that response.
const RUNTIME_CATALOG_POOL_STATEMENT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_millis(500);

const SPACE_NODE_ID_ENV: &str = "SPACE_NODE_ID";
const NODE_ID_PATH: &str = "/space/etc/node-id";
const DATABASE_URL_ENV: &str = "DATABASE_URL";

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. Environment-driven configuration (the bearer token,
/// `DATABASE_URL`, the route-signing and ECW credential material, Kubernetes
/// token auth) stays with the code that reads it.
#[derive(Debug, Clone)]
pub struct InternalApiRunConfig {
    /// TCP address to listen on.
    pub listen: String,
}

/// Why [`run`] returned without a clean shutdown.
#[derive(Debug, thiserror::Error)]
pub enum RunInternalApiError {
    #[error(transparent)]
    InternalTransport(#[from] pplx_space_common_grpc::internal_transport::TransportError),
    #[error("invalid or missing SPACE API route signing configuration")]
    RouteSigningConfig(
        #[source] pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error,
    ),
    #[error("invalid ECW credential signing configuration")]
    EcwCredentialConfig(#[source] pplx_space_common_svc::ecw_credential::EcwCredentialError),
    #[error(transparent)]
    NodeId(#[from] NodeIdError),
    /// The static `SPACE_INTERNAL_TOKEN` boot precondition was not met. The
    /// message is the `Status` message the auth helper produced, kept verbatim
    /// so the boot failure reads the same as before the split.
    #[error("{message}")]
    BearerToken { message: String },
    #[error("failed to read {DATABASE_URL_ENV} for the cluster API")]
    DatabaseUrl(#[source] std::env::VarError),
    #[error("failed to bind the cluster API listener at {listen}")]
    BindListener {
        listen: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to initialize the cluster API space_user pool")]
    Pool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error("failed to initialize the cluster API template-visibility pool")]
    VisibilityPool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error("failed to initialize the cluster API runtime environment catalog pool")]
    RuntimeCatalogPool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error("failed to build the cluster API reflection service")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error("failed to initialize ClusterApiService")]
    ClusterApiService(#[source] tonic::Status),
    #[error("failed to configure cluster API auth")]
    Auth(#[from] auth::AuthConfigError),
    #[error("failed to configure template publisher auth")]
    PublisherAuth(#[from] auth::build::BuildAuthError),
    #[error("cluster API server exited with an error")]
    Serve(#[from] tonic::transport::Error),
}

/// Why internal-api could not resolve or persist the node identity it reports.
#[derive(Debug, thiserror::Error)]
pub enum NodeIdError {
    #[error("failed to create node-id directory {path}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write temporary node-id {path}")]
    WriteTemporary {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to replace node-id at {path}")]
    Replace {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write node-id at {path} after atomic write failed ({atomic_error})")]
    WriteAfterAtomicFailure {
        path: PathBuf,
        atomic_error: Box<NodeIdError>,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read node-id {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid node-id in {path}")]
    Invalid {
        path: PathBuf,
        #[source]
        source: uuid::Error,
    },
}

fn write_node_id_atomically(path: &Path, node_id: NodeId) -> Result<(), NodeIdError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| NodeIdError::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let temp_path = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temp_path, format!("{node_id}\n")).map_err(|source| {
        NodeIdError::WriteTemporary {
            path: temp_path.clone(),
            source,
        }
    })?;
    std::fs::rename(&temp_path, path).map_err(|source| NodeIdError::Replace {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn write_node_id(path: &Path, node_id: NodeId) -> Result<(), NodeIdError> {
    match write_node_id_atomically(path, node_id) {
        Ok(()) => Ok(()),
        Err(atomic_error) if path.exists() => {
            std::fs::write(path, format!("{node_id}\n")).map_err(|source| {
                NodeIdError::WriteAfterAtomicFailure {
                    path: path.to_path_buf(),
                    atomic_error: Box::new(atomic_error),
                    source,
                }
            })
        }
        Err(err) => Err(err),
    }
}

fn read_node_id(path: &Path) -> Result<Option<uuid::Uuid>, NodeIdError> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(NodeIdError::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    let content = content.trim();
    if content.is_empty() {
        return Ok(None);
    }

    uuid::Uuid::parse_str(content)
        .map(Some)
        .map_err(|source| NodeIdError::Invalid {
            path: path.to_path_buf(),
            source,
        })
}

fn resolve_node_id_with_env(
    path: &Path,
    env_node_id: Option<String>,
) -> Result<(String, String), NodeIdError> {
    if let Some(node_id) = env_node_id
        .map(|node_id| node_id.trim().to_string())
        .filter(|node_id| !node_id.is_empty())
    {
        return Ok((node_id, SPACE_NODE_ID_ENV.to_string()));
    }

    if let Some(node_id) = read_node_id(path)? {
        return Ok((node_id.to_string(), path.display().to_string()));
    }

    let node_id = NodeId::new();
    write_node_id(path, node_id)?;
    Ok((node_id.to_string(), path.display().to_string()))
}

fn resolve_node_id(path: &Path) -> Result<(String, String), NodeIdError> {
    resolve_node_id_with_env(path, std::env::var(SPACE_NODE_ID_ENV).ok())
}

/// Serve the cluster control-plane gRPC API until `shutdown_token` is
/// cancelled, then drain in-flight requests and return.
pub async fn run(
    config: InternalApiRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunInternalApiError> {
    let route_ed25519_authority = Some(std::sync::Arc::new(
        pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority::load_required_from_env()
            .map_err(RunInternalApiError::RouteSigningConfig)?,
    ));
    let ecw_credential_keyring = std::sync::Arc::new(
        pplx_space_common_svc::ecw_credential::EcwCredentialKeyring::load_required_from_env()
            .map_err(RunInternalApiError::EcwCredentialConfig)?,
    );

    let (node_id, node_id_source) = resolve_node_id(Path::new(NODE_ID_PATH))?;
    tracing::info!(
        node_id = %node_id,
        source = %node_id_source,
        "resolved node identity"
    );

    let token = auth::required_bearer_token(auth::SPACE_INTERNAL_TOKEN_ENV).map_err(|status| {
        RunInternalApiError::BearerToken {
            message: status.message().to_string(),
        }
    })?;

    let database_url = std::env::var(DATABASE_URL_ENV).map_err(RunInternalApiError::DatabaseUrl)?;
    let listener = TcpListener::bind(&config.listen).await.map_err(|source| {
        RunInternalApiError::BindListener {
            listen: config.listen.clone(),
            source,
        }
    })?;
    tracing::info!(listen = %config.listen, "space-internal-api-svc listening");
    let pool =
        pplx_space_common_svc::sql::connect_space_user_pool_lazy(&database_url, SERVICE_NAME)
            .map_err(RunInternalApiError::Pool)?;
    tracing::info!("prepared lazy space_user cluster API database pool");

    // Sideline pool for the best-effort template-visibility lookup. It reads
    // `templates`, which the release queries do not, so an `ALTER TABLE
    // templates` blocks only the lookup — but on a shared pool each blocked
    // statement pins a connection until the role's `statement_timeout`, and a
    // drained pool fails the *release* query on acquire. Capped small so that
    // failure stays inside the lookup, which degrades to UNSPECIFIED, and given
    // its own short `statement_timeout` so a blocked lookup is cancelled
    // server-side instead of adding the role's 10s bound to a create pull.
    let visibility_pool = pplx_space_common_svc::sql::connect_space_user_sideline_pool_lazy(
        &database_url,
        VISIBILITY_POOL_APPLICATION_NAME,
        VISIBILITY_POOL_MAX_CONNECTIONS,
        Some(VISIBILITY_POOL_STATEMENT_TIMEOUT),
    )
    .map_err(RunInternalApiError::VisibilityPool)?;
    tracing::info!(
        max_connections = VISIBILITY_POOL_MAX_CONNECTIONS,
        statement_timeout_ms = VISIBILITY_POOL_STATEMENT_TIMEOUT.as_millis(),
        "prepared lazy template-visibility database pool"
    );

    // One shared cancellation token wired to SIGTERM/SIGINT, per the SPACE
    // shutdown convention (matches `space-cluster-controller`). The binary owns
    // the signal wiring; this is the token it handed over.
    let shutdown_signal = shutdown_token.clone();
    let shutdown = async move { shutdown_signal.cancelled().await };

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            pplx_space_node_api_client::proto::CLUSTER_FILE_DESCRIPTOR_SET,
        )
        .build_v1()?;
    let (_, health_service) = pplx_space_common_svc::grpc_health_service();

    let activation = std::sync::Arc::new(
        pplx_space_node_api_client::activation::RemoteIngressActivation::from_env()
            .await
            .map_err(RunInternalApiError::ClusterApiService)?,
    );
    let mut svc = ClusterApiService::from_env_with_security_state(
        pool,
        route_ed25519_authority,
        ecw_credential_keyring,
    )
    .await
    .map_err(RunInternalApiError::ClusterApiService)?
    .with_visibility_pool(visibility_pool);
    // The provisioning recipe's catalog read is best-effort too: a failed read
    // omits the recipe and still serves the policy. Sidelined so a blocked
    // `runtime_environments` read can't drain the pool the policy and node
    // authentication reads use. Built only while recipes are served: a lazy
    // pool still keeps its minimum connections open, and with the setting off
    // nothing reads the catalog.
    if svc.serves_provisioning_recipes() {
        let runtime_catalog_pool =
            pplx_space_common_svc::sql::connect_space_user_sideline_pool_lazy(
                &database_url,
                RUNTIME_CATALOG_POOL_APPLICATION_NAME,
                RUNTIME_CATALOG_POOL_MAX_CONNECTIONS,
                Some(RUNTIME_CATALOG_POOL_STATEMENT_TIMEOUT),
            )
            .map_err(RunInternalApiError::RuntimeCatalogPool)?;
        tracing::info!(
            max_connections = RUNTIME_CATALOG_POOL_MAX_CONNECTIONS,
            statement_timeout_ms = RUNTIME_CATALOG_POOL_STATEMENT_TIMEOUT.as_millis(),
            "prepared lazy runtime environment catalog database pool"
        );
        svc = svc.with_runtime_catalog_pool(runtime_catalog_pool);
    }
    svc.ingress_activation = Some(activation);
    let node_info = ClusterNodeInfoService::new(node_id);
    let publisher_auth =
        auth::build::BuildAuthLayer::for_template_publication(token.clone()).await?;
    let auth_config = auth::NodeApiAuthConfig::from_env(auth::NodeRole::Cluster, token, None)?;
    let auth_layer = auth::NodeApiAuthLayer::from_config(auth_config).await?;
    let visibility_observations = svc.visibility_observations.clone();

    let router = pplx_space_common_grpc::internal_transport::server_builder()?
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_mins(1)))
        // Trace first (outermost) so denied requests still produce server
        // spans; auth runs after tracing but before any routed service.
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .layer(auth_layer)
        .layer(publisher_auth)
        .add_service(health_service)
        .add_service(reflection)
        .add_service(cluster_api_server(svc))
        .add_service(NodeInfoServer::new(node_info));

    // Spawned after the last fallible setup step, so a replica that never
    // serves doesn't report clean zeros.
    tokio::spawn(crate::visibility_observations::emit(
        visibility_observations,
        shutdown_token.clone(),
    ));

    let serve_result = router
        .serve_with_incoming_shutdown(
            pplx_space_common_grpc::internal_transport::Incoming::new(
                TcpIncoming::from(listener).with_nodelay(Some(true)),
                shutdown_token.clone(),
            ),
            shutdown,
        )
        .await;

    shutdown_token.cancel();
    serve_result?;
    Ok(())
}

#[cfg(test)]
mod tests;
