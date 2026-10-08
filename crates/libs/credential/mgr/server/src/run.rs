//! The credential manager service: the KEK envelope authority that owns the
//! configured key backends and credential Postgres state, and the node-local agent that
//! fronts it on the sandbox Unix socket.
//!
//! This module is the whole service. The `space-credmgr-svc` binary is a thin
//! `main` that parses flags, initializes telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the service.

use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_svc::core::host::linux::identities::group_names;
use pplx_space_common_svc::core::types::{KekProvider, MasterKeyIdentifier};
use pplx_space_common_svc::{
    AcquireOutcome, DEFAULT_DRAIN_TIMEOUT, LocalHealthServer, PeerProcessCheckMode, ServiceMutex,
};
use tokio::task::JoinSet;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::ServerTlsConfig;

use crate::auth::CredentialUdsAccess;
use crate::aws_secrets_manager::AwsSecretsManagerResolver;
use crate::credential_injection_authority::CredentialInjectionAuthority;
use crate::credential_injection_bundle::{
    CredentialInjectionBundleMaterializer, CredentialProviderDispatcher,
};
use crate::credential_store::{CredentialStore, PgCredentialStore};
use crate::in_memory::InMemoryKeyWrapper;
use crate::intermediate_ca;
use crate::intermediate_ca::aws_private_ca::{AwsPrivateCaConfig, AwsPrivateCaIssuer, CA_ARN_ENV};
use crate::intermediate_key_broker::{
    BrokerProtocolConfig, BrokerProtocolConfigError, GrpcIntermediateKeyBroker,
    IntermediateKeyCacheConfig, IntermediateKeyWrapper,
};
use crate::intermediate_key_identity::PgIntermediateKeyIdentityLookup;
use crate::intermediate_key_store::PgWrappedIntermediateKeyStore;
use crate::kek_provisioning::{InMemoryKekProvisioner, IntermediateKeyProvisioner, KekProvisioner};
use crate::kek_store::{KekStore, PgKekStore};
use crate::key_wrapper::CompositeKeyWrapper;
use crate::node_ca::NodeCa;
use crate::node_service::NodeCredentialManager;
use crate::proto::credential_manager_server::{CredentialManager, CredentialManagerServer};
use crate::service::{CredentialManagerService, ExchangeTokenMode, ParseExchangeTokenModeError};
use crate::storage_exchange::VendRoleArn;
use crate::storage_vend_store::{PgStorageVendStore, StorageVendStore};
use crate::volume_wrap_store::{PgVolumeWrapStore, VolumeWrapStore};

/// Group name that controls who can connect to the credmgr socket.
/// Same group as sandbox-manager — callers are the same (api-svc, node-api-svc, net-gateway).
const SOCKET_GROUP: &str = group_names::SPACE_SBXCLIENTS;
/// The service name credmgr reports to telemetry, names its health and
/// rate-limit surfaces with, and authorizes its Unix-socket peers under.
pub const SERVICE_NAME: &str = "space-credential-manager";
const IN_MEMORY_KEK_ENABLED_ENV: &str = "SPACE_CREDMGR_IN_MEMORY_KEK_ENABLED";
const EXCHANGE_TOKEN_MODE_ENV: &str = "SPACE_CREDMGR_SCOPED_EXCHANGE_TOKEN_MODE";
const STORAGE_VEND_ROLE_ENV: &str = "SPACE_CREDMGR_S3_VEND_ROLE_ARN";
const PLATFORM_KEK_IDENTIFIER_ENV: &str = "SPACE_PLATFORM_KEK_IDENTIFIER";
// Hard-coded so two instances on the same host cannot be configured to use
// different mutex files and both acquire concurrently.
const SERVICE_MUTEX_PATH: &str = "/space/run/credential-manager/service.mutex";

mod volume_socket;

/// Which side of the control-plane split this process runs as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CredmgrRole {
    /// Owns the configured key backends and Postgres state; serves RPCs directly.
    #[default]
    Authority,
    /// Node-local proxy holding no keys or database; forwards to the
    /// upstream authority.
    Agent,
}

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. Environment-driven configuration (`DATABASE_URL`,
/// `SPACE_CREDMGR_ENDPOINT`, the AWS and projected-token settings) stays with
/// the code that reads it.
#[derive(Debug, Clone)]
pub struct CredmgrRunConfig {
    /// Operating role. `Authority` owns the configured key backends and Postgres state and
    /// serves database-backed RPCs directly; `Agent` is the node-local proxy
    /// that forwards every RPC to the upstream authority.
    pub role: CredmgrRole,
    /// Unix socket path for the gRPC service.
    pub socket_path: PathBuf,
    /// Optional restricted listener for volume-manager, provisioned before enabling.
    pub volume_socket_path: Option<PathBuf>,
    /// TCP listen address for the control-plane deployment. Without TLS
    /// material this requires `allow_plaintext`.
    pub tcp_listen: Option<SocketAddr>,
    /// Server certificate PEM. Set together with `tls_key`.
    pub tls_cert: Option<PathBuf>,
    /// Server private key PEM. Set together with `tls_cert`.
    pub tls_key: Option<PathBuf>,
    /// Permit a plaintext TCP listener. Non-cloud profiles only.
    pub allow_plaintext: bool,
    /// Skip the node-local Unix socket and serve TCP only.
    pub no_uds: bool,
    /// Instance-local health Unix socket, bound from startup so readiness can
    /// pass while this instance is still passive.
    pub health_socket_path: Option<PathBuf>,
    /// How long the passive instance waits for the predecessor to release the
    /// service mutex before exiting non-zero.
    pub service_mutex_timeout: Duration,
}

/// Why [`run`] returned without a clean shutdown.
#[derive(Debug, thiserror::Error)]
pub enum RunCredmgrError {
    #[error(
        "refusing to start a keyless authority on the node-local Unix socket: \
         role=authority with no DATABASE_URL answers health checks but fails every \
         database-backed crypto/injection call, so callers fail closed against \
         a pod that looks Ready. A node-local credmgr must run with --role agent and \
         SPACE_CREDMGR_ENDPOINT pointing at the control-plane authority; an authority must \
         be given DATABASE_URL."
    )]
    KeylessNodeAuthority,
    #[error("--volume-socket requires UDS and a path distinct from --socket")]
    InvalidVolumeSocket,
    #[error("invalid peer-process check mode: {0}")]
    PeerProcessCheck(#[source] pplx_space_common_svc::PeerProcessCheckModeEnvError),
    #[error("invalid {EXCHANGE_TOKEN_MODE_ENV}: {0}")]
    ExchangeTokenMode(#[source] ParseExchangeTokenModeError),
    #[error("{EXCHANGE_TOKEN_MODE_ENV} must be valid UTF-8")]
    ExchangeTokenModeNotUnicode,
    #[error("{STORAGE_VEND_ROLE_ENV} is required unless storage exchange is disabled")]
    MissingStorageVendRole,
    #[error("{STORAGE_VEND_ROLE_ENV} must be valid UTF-8")]
    StorageVendRoleNotUnicode,
    #[error("{STORAGE_VEND_ROLE_ENV} is not a valid IAM role ARN")]
    InvalidStorageVendRole,
    #[error("{EXCHANGE_TOKEN_MODE_ENV}=enforce requires DATABASE_URL")]
    ScopedExchangeMissingDatabase,
    #[error("--tls-cert/--tls-key require --tcp-listen")]
    TlsWithoutTcpListen,
    #[error("--tls-cert and --tls-key must be set together")]
    IncompleteTlsMaterial,
    #[error(
        "--no-uds requires --tcp-listen: skipping the node-local Unix socket \
         leaves the control-plane deployment with nothing to listen on"
    )]
    NoUdsWithoutTcpListen,
    #[error(
        "refusing to bind a plaintext TCP listener on {addr}: pass \
         --tls-cert/--tls-key for server TLS, or --allow-plaintext on \
         non-cloud profiles"
    )]
    PlaintextTcpRefused { addr: SocketAddr },
    #[error("failed to initialize the credential-manager space_creds pool: {0}")]
    DatabasePool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error("invalid intermediate-key broker URL {url:?}: {source}")]
    IntermediateKeyBrokerUrl {
        url: String,
        #[source]
        source: tonic::transport::Error,
    },
    #[error(
        "an https intermediate-key broker endpoint requires \
         SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_TLS_CA (CA bundle path)"
    )]
    MissingIntermediateKeyBrokerCa,
    #[error("failed to read provider CA bundle at {path}: {source}")]
    ReadIntermediateKeyBrokerCa {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("intermediate-key broker TLS configuration: {0}")]
    IntermediateKeyBrokerTls(#[source] tonic::transport::Error),
    #[error(
        "{token_file_env} (client bearer credential path) is required whenever \
         {url_env} is set",
        token_file_env = crate::INTERMEDIATE_KEY_BROKER_TOKEN_FILE_ENV,
        url_env = crate::INTERMEDIATE_KEY_BROKER_URL_ENV,
    )]
    MissingIntermediateKeyBrokerTokenFile,
    #[error("invalid intermediate-key broker protocol configuration: {0}")]
    IntermediateKeyBrokerProtocol(#[source] BrokerProtocolConfigError),
    #[error("invalid federated-identity policy: {0}")]
    FederatedIdentityPolicy(
        #[source] pplx_space_common_svc::federated_identity::FederatedIdentityPolicyError,
    ),
    #[error("invalid AWS Private CA configuration: {0}")]
    IntermediateCaConfig(#[source] crate::intermediate_ca::aws_private_ca::ConfigError),
    #[error(transparent)]
    SecretsManagerReaderRole(#[from] crate::aws_secrets_manager::ReaderRoleArnError),
    #[error(
        "SPACE_PLATFORM_KEK_PROVIDER=in-memory requires \
         SPACE_CREDMGR_IN_MEMORY_KEK_ENABLED=true"
    )]
    InMemoryPlatformKekDisabled,
    #[error("SPACE_PLATFORM_KEK_PROVIDER=in-memory requires DATABASE_URL")]
    InMemoryPlatformKekMissingDatabase,
    #[error(
        "SPACE_PLATFORM_KEK_IDENTIFIER is required when \
         SPACE_PLATFORM_KEK_PROVIDER=in-memory"
    )]
    MissingPlatformKekIdentifier,
    #[error("SPACE_PLATFORM_KEK_IDENTIFIER is invalid: {0}")]
    InvalidPlatformKekIdentifier(
        #[source] pplx_space_common_svc::core::types::MasterKeyIdentifierParseError,
    ),
    #[error(
        "SPACE_PLATFORM_KEK_PROVIDER=intermediate-key-broker requires DATABASE_URL and a \
         non-empty SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_URL"
    )]
    MissingIntermediateKeyBrokerPlatformConfig,
    #[error("failed to bind the credential-manager health socket: {0}")]
    HealthSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to acquire the credential-manager service mutex: {0}")]
    ServiceMutex(#[source] pplx_space_common_svc::service_mutex::AcquireError),
    #[error("failed to bind the credential-manager socket: {0}")]
    BindSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("volume credential socket preflight failed for {path}: {source}")]
    VolumeSocketPreflight {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("volume credential socket preflight task failed: {0}")]
    VolumeSocketPreflightTask(#[source] tokio::task::JoinError),
    #[error(
        "the node credential agent serves the node-local Unix socket; \
         --no-uds is invalid in agent mode"
    )]
    AgentNoUds,
    #[error(
        "the node credential agent does not bind its own TCP/TLS listener; it \
         forwards to the upstream authority configured via SPACE_CREDMGR_ENDPOINT"
    )]
    AgentTcpListener,
    #[error("invalid SPACE_CREDMGR_ENDPOINT for the node credential agent: {0}")]
    AgentUpstreamEndpoint(#[source] pplx_space_credmgr_client::EndpointConfigError),
    #[error(transparent)]
    AgentUpstream(#[from] crate::node_service::AgentUpstreamError),
    #[error("node credential agent could not connect to the upstream authority: {0}")]
    AgentUpstreamConnect(#[source] pplx_space_credmgr_client::ConnectError),
    #[error("could not generate this node's intermediate CA keypair: {0}")]
    NodeCaKeypair(#[source] rcgen::Error),
    /// A gRPC server returned before shutdown was requested.
    #[error("{server_kind} gRPC server exited unexpectedly")]
    ServerExited {
        server_kind: CredentialManagerServerKind,
    },
    #[error("{server_kind} gRPC server exited unexpectedly: {source}")]
    ServerExitedWithError {
        server_kind: CredentialManagerServerKind,
        #[source]
        source: ServeError,
    },
    #[error("{server_kind} gRPC server failed during drain: {source}")]
    ServerFailedDuringDrain {
        server_kind: CredentialManagerServerKind,
        #[source]
        source: ServeError,
    },
    #[error("credential-manager gRPC server task ended unexpectedly: {0}")]
    ServerTask(#[source] tokio::task::JoinError),
    #[error("credential-manager gRPC server exited unexpectedly and drain timed out")]
    DrainTimedOut,
    /// The control-plane (`--no-uds`) listener, which has no service mutex and
    /// no supervisor loop around it.
    #[error(transparent)]
    ControlPlaneServer(#[from] ServeError),
}

/// Why one of the credential-manager gRPC listeners failed to start or serve.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("non-Kubernetes TCP listener requires SPACE_INTERNAL_TOKEN: {0}")]
    InternalToken(#[source] tonic::Status),
    #[error("invalid credmgr projected-token (k8sAuth) configuration: {0}")]
    K8sAuthConfig(#[source] crate::k8s_identity::K8sAuthConfigError),
    #[error("failed to initialize the TokenReview client: {0}")]
    TokenReviewClient(#[source] crate::k8s_identity::K8sAuthConfigError),
    #[error("failed to read TLS PEM at {path}: {source}")]
    ReadTlsPem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid TLS configuration: {0}")]
    TlsConfig(#[source] tonic::transport::Error),
    #[error("UDS gRPC server failed: {0}")]
    UdsServer(#[source] tonic::transport::Error),
    #[error("TCP gRPC server failed: {0}")]
    TcpServer(#[source] tonic::transport::Error),
}

/// Whether `DATABASE_URL` is set to a non-empty value. The authority's
/// Encrypt/Decrypt, the volume-DEK RPCs, and credential-injection resolution
/// all require it.
fn database_url_present() -> bool {
    std::env::var("DATABASE_URL").is_ok_and(|v| !v.is_empty())
}

/// Opt-in: crash on a keyless-authority misconfiguration instead of limping
/// along Ready-but-broken. Off by default so existing environments keep their
/// current behavior unchanged; turned on per-environment (via the chart) once
/// that environment's node-local credmgr runs as an `--role agent`. Env:
/// `SPACE_CREDMGR_FAIL_ON_KEYLESS_AUTHORITY`.
fn fail_on_keyless_authority() -> bool {
    matches!(
        std::env::var("SPACE_CREDMGR_FAIL_ON_KEYLESS_AUTHORITY")
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Ok("1" | "true" | "yes")
    )
}

/// Reject a startup configuration that can only fail *silently* at runtime,
/// **when strict validation is enabled** (`fail_on_keyless_authority`).
///
/// The load-bearing case is the node-local credmgr that defaults to
/// `--role authority` (the clap default) but is given no `DATABASE_URL`: it
/// binds the node Unix socket and answers gRPC health checks — so Kubernetes
/// marks the pod Ready — while every crypto/injection RPC returns
/// `FailedPrecondition`. Downstream callers (sandbox-manager's
/// volume-encryption gate, volume-manager's DEK/credential exchange) then fail
/// closed against a service that looks healthy, which is exactly the opaque,
/// hard-to-localize outage this guard exists to prevent.
///
/// A node-local credmgr must run with `--role agent` (forwarding to the
/// control-plane authority via `SPACE_CREDMGR_ENDPOINT`); a real authority must
/// be given `DATABASE_URL`. When `strict` is set this misconfiguration is a
/// crash loop with a clear reason; when not set (the default) it is left to the
/// existing behavior so environments that have not migrated to agent mode are
/// untouched.
fn validate_startup_config(
    role: CredmgrRole,
    no_uds: bool,
    has_database_url: bool,
    strict: bool,
) -> Result<(), RunCredmgrError> {
    if strict && role == CredmgrRole::Authority && !no_uds && !has_database_url {
        return Err(RunCredmgrError::KeylessNodeAuthority);
    }
    Ok(())
}

/// The flag relationships clap declares for the binary (`requires` on
/// `--tls-cert`/`--tls-key` and on `--no-uds`), re-checked here because [`run`]
/// and [`CredmgrRunConfig`] are public: a caller assembling the config in code
/// gets the same shapes rejected as a typed error, instead of a panic or a
/// listener it did not ask for.
fn validate_flag_shape(config: &CredmgrRunConfig) -> Result<(), RunCredmgrError> {
    if config
        .volume_socket_path
        .as_ref()
        .is_some_and(|path| config.no_uds || path == &config.socket_path)
    {
        return Err(RunCredmgrError::InvalidVolumeSocket);
    }
    if config.no_uds && config.tcp_listen.is_none() {
        return Err(RunCredmgrError::NoUdsWithoutTcpListen);
    }
    // Half-set TLS material is never a plaintext listener: a caller that
    // supplied a certificate meant to encrypt the connection, so falling back
    // to `--allow-plaintext` here would downgrade it silently.
    if config.tls_cert.is_some() != config.tls_key.is_some() {
        return Err(RunCredmgrError::IncompleteTlsMaterial);
    }
    if config.tcp_listen.is_none() && config.tls_cert.is_some() {
        return Err(RunCredmgrError::TlsWithoutTcpListen);
    }
    Ok(())
}

/// How the TCP listener (if any) is secured. Parsed from the flag
/// combination up front so misconfiguration fails at startup, loudly,
/// instead of degrading to an unintended plaintext listener.
///
/// Note this is *transport* security only. Caller identity on TCP is the
/// projected ServiceAccount token validated via TokenReview (see
/// `CredmgrTcpAuthLayer`), not a client certificate — credmgr terminates
/// server-side TLS but no longer requests or inspects a client cert.
enum TcpSecurity {
    ServerTls { cert: PathBuf, key: PathBuf },
    Plaintext,
}

impl TcpSecurity {
    fn from_config(
        config: &CredmgrRunConfig,
    ) -> Result<Option<(SocketAddr, Self)>, RunCredmgrError> {
        let Some(addr) = config.tcp_listen else {
            if config.tls_cert.is_some() {
                return Err(RunCredmgrError::TlsWithoutTcpListen);
            }
            return Ok(None);
        };
        // `validate_flag_shape` has already rejected a half-set pair, so a
        // missing certificate here means none was supplied at all.
        match (&config.tls_cert, &config.tls_key) {
            (Some(cert), Some(key)) => Ok(Some((
                addr,
                Self::ServerTls {
                    cert: cert.clone(),
                    key: key.clone(),
                },
            ))),
            (Some(_), None) | (None, Some(_)) => Err(RunCredmgrError::IncompleteTlsMaterial),
            (None, None) if config.allow_plaintext => Ok(Some((addr, Self::Plaintext))),
            (None, None) => Err(RunCredmgrError::PlaintextTcpRefused { addr }),
        }
    }
}

/// Run the credential manager until `shutdown_token` fires or a fatal
/// condition ends it. Telemetry must already be initialized by the caller.
pub async fn run(
    config: CredmgrRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunCredmgrError> {
    // Log the resolved operating shape up front so the active branch (agent vs
    // authority, node UDS vs control-plane TCP, database present or not) is
    // unambiguous in the logs, then reject shapes that can only fail silently.
    let has_database_url = database_url_present();
    let strict = fail_on_keyless_authority();
    tracing::info!(
        role = ?config.role,
        no_uds = config.no_uds,
        tcp_listen = ?config.tcp_listen,
        has_database_url,
        fail_on_keyless_authority = strict,
        upstream_endpoint = %std::env::var("SPACE_CREDMGR_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "<unset>".to_string()),
        "credential manager startup configuration resolved"
    );
    validate_flag_shape(&config)?;
    validate_startup_config(config.role, config.no_uds, has_database_url, strict)?;

    // Resolved before role dispatch so BOTH roles hard-fail on a
    // set-but-unrecognized value instead of one path silently ignoring it.
    let peer_process_check_mode =
        PeerProcessCheckMode::from_env().map_err(RunCredmgrError::PeerProcessCheck)?;

    if config.role == CredmgrRole::Agent {
        return run_agent(config, peer_process_check_mode, shutdown_token).await;
    }

    let tcp = TcpSecurity::from_config(&config)?;
    if config.no_uds {
        let Some((addr, security)) = tcp else {
            return Err(RunCredmgrError::NoUdsWithoutTcpListen);
        };
        let service = initialize_authority_service(peer_process_check_mode).await?;
        tracing::info!(
            tcp_listen = %addr,
            "credential manager active; listening on TCP"
        );
        serve_tcp_until(addr, security, service, shutdown_token).await?;
        return Ok(());
    }

    let Some((service, node_local)) = initialize_node_local(
        &config,
        &shutdown_token,
        Path::new(SERVICE_MUTEX_PATH),
        initialize_authority_service(peer_process_check_mode),
    )
    .await?
    else {
        return Ok(());
    };
    let listener = CredentialListeners::bind(&config)?;

    tracing::info!(
        socket_path = %config.socket_path.display(),
        tcp_listen = ?config.tcp_listen,
        "credential manager active; listening"
    );

    match tcp {
        None => {
            serve_node_local_uds(
                listener,
                service,
                config.socket_path.clone(),
                node_local,
                shutdown_token,
            )
            .await?
        }
        Some((addr, security)) => {
            serve_node_local_uds_and_tcp(
                listener,
                addr,
                security,
                service,
                config.socket_path.clone(),
                node_local,
                shutdown_token,
            )
            .await?
        }
    }

    Ok(())
}

async fn initialize_authority_service(
    peer_process_check_mode: PeerProcessCheckMode,
) -> Result<CredentialManagerService, RunCredmgrError> {
    // Unset: `legacy` when a vend role is configured, otherwise `disabled`.
    let exchange_token_mode = match std::env::var(EXCHANGE_TOKEN_MODE_ENV) {
        Ok(raw) => raw.parse().map_err(RunCredmgrError::ExchangeTokenMode)?,
        Err(std::env::VarError::NotPresent) => {
            if std::env::var_os(STORAGE_VEND_ROLE_ENV).is_some() {
                ExchangeTokenMode::Legacy
            } else {
                ExchangeTokenMode::Disabled
            }
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(RunCredmgrError::ExchangeTokenModeNotUnicode);
        }
    };
    tracing::info!(
        env = EXCHANGE_TOKEN_MODE_ENV,
        mode = %exchange_token_mode,
        "storage credential exchange rollout mode resolved"
    );
    let storage_vend_role =
        resolve_storage_vend_role(exchange_token_mode, std::env::var(STORAGE_VEND_ROLE_ENV))?;
    // Database access is what enables the Encrypt/Decrypt RPCs (cmk_id →
    // KEK row resolution) and, below, the intermediate-key-broker blob store.
    // The control-plane deployment sets DATABASE_URL; node-local deployments
    // don't, and those RPCs fail with FAILED_PRECONDITION there. The
    // authority is entirely credential-fenced work, so its one pool is the
    // space_creds lane.
    let pool = match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() => {
            let credentials_pool =
                pplx_space_common_svc::sql::CredentialsPool::connect_lazy(&url, SERVICE_NAME)
                    .map_err(RunCredmgrError::DatabasePool)?;
            tracing::info!("prepared lazy space_creds credential-manager database pool");
            Some(credentials_pool.pool().clone())
        }
        _ => None,
    };
    if exchange_token_mode == ExchangeTokenMode::Enforce && pool.is_none() {
        return Err(RunCredmgrError::ScopedExchangeMissingDatabase);
    }

    let private_ca_config =
        AwsPrivateCaConfig::from_env().map_err(RunCredmgrError::IntermediateCaConfig)?;
    let secrets_manager_role_arn = crate::aws_secrets_manager::reader_role_arn_from_env()?;
    // Database-backed credential resolution reads secrets only through the
    // organization-scoped reader role. Without a role, or on node-local
    // deployments without DATABASE_URL, references are not resolvable.
    if pool.is_some() && secrets_manager_role_arn.is_none() {
        tracing::warn!(
            env = crate::aws_secrets_manager::SECRETS_MANAGER_ROLE_ARN_ENV,
            "no Secrets Manager reader role; credential references are not resolvable"
        );
    }
    let secrets_manager_role_arn = secrets_manager_role_arn.filter(|_| pool.is_some());
    let (private_ca_issuer, aws_secrets_manager_resolver) =
        if private_ca_config.is_none() && secrets_manager_role_arn.is_none() {
            tracing::info!("AWS integrations disabled; skipping AWS SDK configuration");
            (None, None)
        } else {
            let aws_config = load_authority_aws_config().await;
            let private_ca_issuer = private_ca_config
                .map(|config| Arc::new(AwsPrivateCaIssuer::from_config(config, &aws_config)));
            let resolver = secrets_manager_role_arn
                .map(|role| Arc::new(AwsSecretsManagerResolver::new(&aws_config, role)));
            (private_ca_issuer, resolver)
        };

    let platform_kek_provider = pplx_space_common_svc::core::types::platform_default_kek_provider();
    let in_memory_enabled = pplx_space_common_core::env::env_flag(
        pplx_space_common_core::env::test_hook_var(IN_MEMORY_KEK_ENABLED_ENV).as_deref(),
        false,
    );
    let mut composite = CompositeKeyWrapper::new();
    if in_memory_enabled {
        composite = composite.with_in_memory(InMemoryKeyWrapper::new());
        tracing::warn!(
            env = IN_MEMORY_KEK_ENABLED_ENV,
            "credential manager has the in-memory KEK backend enabled: \
             provider='in-memory' KEKs DO NOT survive restarts"
        );
    }

    // Intermediate-key broker backend: requires both the database
    // (wrapped-blob store) and broker egress, i.e. the control-plane
    // authority. Node-local instances never install it and fail closed
    // with UnsupportedProvider for intermediate-key-broker orgs.
    let mut kek_provisioner: Option<Arc<dyn KekProvisioner>> = None;
    if platform_kek_provider == Some(KekProvider::InMemory) {
        if !in_memory_enabled {
            return Err(RunCredmgrError::InMemoryPlatformKekDisabled);
        }
        let pool = pool
            .as_ref()
            .ok_or(RunCredmgrError::InMemoryPlatformKekMissingDatabase)?;
        let identifier = pplx_space_common_core::env::test_hook_var(PLATFORM_KEK_IDENTIFIER_ENV)
            .filter(|value| !value.is_empty())
            .ok_or(RunCredmgrError::MissingPlatformKekIdentifier)?;
        let identifier = MasterKeyIdentifier::parse(identifier)
            .map_err(RunCredmgrError::InvalidPlatformKekIdentifier)?;
        kek_provisioner = Some(Arc::new(InMemoryKekProvisioner::new(
            pool.clone(),
            identifier,
        )));
        tracing::warn!(
            "in-memory KEK provisioning enabled for local development; key inventory writes are authority-owned"
        );
    }
    if let Some(pool) = &pool
        && let Ok(url) = std::env::var(crate::INTERMEDIATE_KEY_BROKER_URL_ENV)
        && !url.is_empty()
    {
        let channel = intermediate_key_broker_channel(&url)?;
        // The provider vends plaintext key material, so the client always
        // authenticates itself; network reachability is not the
        // authorization boundary. The file shape supports a projected
        // ServiceAccount token (read fresh per request, kubelet-rotated).
        let token_file = std::env::var(crate::INTERMEDIATE_KEY_BROKER_TOKEN_FILE_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or(RunCredmgrError::MissingIntermediateKeyBrokerTokenFile)?;
        // The authority is the only instance with the database, so it is the
        // only one that can say what a federated org's upstream id is. A
        // node-local instance never installs this backend.
        let identity = Arc::new(PgIntermediateKeyIdentityLookup::new(
            pool.clone(),
            pplx_space_common_svc::federated_identity::FederatedIdentityPolicy::from_env()
                .map_err(RunCredmgrError::FederatedIdentityPolicy)?
                .grant_audience,
        ));
        let provider = Arc::new(GrpcIntermediateKeyBroker::from_channel(
            channel,
            BrokerProtocolConfig::from_env()
                .map_err(RunCredmgrError::IntermediateKeyBrokerProtocol)?,
            token_file,
            identity,
        ));
        let store = Arc::new(PgWrappedIntermediateKeyStore::new(pool.clone()));
        composite = composite.with_intermediate_key_broker(IntermediateKeyWrapper::new(
            provider.clone(),
            store,
            intermediate_key_cache_config()?,
        ));
        // Availability and provider-selection policy are separate. Wiring this
        // backend lets the authority UNWRAP already-enrolled intermediate-key-broker
        // orgs; whether a keyless org should ENROLL onto intermediate-key-broker is
        // the platform default's decision. Install the provisioner (which mints
        // an intermediate-key-broker KEK on an org's first Encrypt / sandbox volume
        // DEK) only when the platform default selects intermediate-key-broker, so a
        // keyless org is never silently enrolled onto it.
        // Single source of truth (shared with the admin BYOK-enable gate) so
        // the sites can never disagree on whether a keyless org is supported —
        // a drift would let the api enable BYOK
        // while this authority has no provisioner, creating encrypted sandboxes
        // that can't start.
        if platform_kek_provider == Some(KekProvider::IntermediateKeyBroker) {
            kek_provisioner = Some(Arc::new(IntermediateKeyProvisioner::new(
                pool.clone(),
                provider,
            )));
            tracing::info!("intermediate-key broker enabled with lazy KEK provisioning");
        } else {
            tracing::info!(
                "intermediate-key broker enabled (unwrap only; platform default \
                 does not select intermediate-key-broker for new orgs)"
            );
        }
    }
    if platform_kek_provider == Some(KekProvider::IntermediateKeyBroker)
        && kek_provisioner.is_none()
    {
        return Err(RunCredmgrError::MissingIntermediateKeyBrokerPlatformConfig);
    }
    // No type ascription: `Arc<CompositeKeyWrapper>` is unsized to
    // `Arc<dyn KeyWrapper>` automatically at the call site below.
    let key_wrapper = Arc::new(composite);

    let mut service = CredentialManagerService::new(key_wrapper, peer_process_check_mode)
        .with_exchange_token_mode(exchange_token_mode);
    if let Some(role) = storage_vend_role {
        service = service.with_storage_vend_role(role);
    }
    if let Some(issuer) = private_ca_issuer {
        service = service.with_intermediate_ca_issuer(issuer);
    } else {
        tracing::info!("no {CA_ARN_ENV}; IssueIntermediateCertificate is disabled");
    }
    match pool {
        Some(pool) => {
            let mut providers = CredentialProviderDispatcher::default();
            if let Some(resolver) = aws_secrets_manager_resolver {
                providers = providers.with_aws_secrets_manager(resolver);
            }
            let kek_store: Arc<dyn KekStore> = Arc::new(PgKekStore::new(pool.clone()));
            let credential_store: Arc<dyn CredentialStore> =
                Arc::new(PgCredentialStore::new(pool.clone()));
            let storage_vend_store: Arc<dyn StorageVendStore> =
                Arc::new(PgStorageVendStore::new(pool.clone()));
            let credential_injection_authority = CredentialInjectionAuthority::new(
                pool.clone(),
                CredentialInjectionBundleMaterializer::new(providers),
            );
            let volume_wrap_store: Arc<dyn VolumeWrapStore> =
                Arc::new(PgVolumeWrapStore::new(pool));
            service = service
                .with_kek_store(kek_store)
                .with_credential_store(credential_store)
                .with_storage_vend_store(storage_vend_store)
                .with_credential_injection_authority(credential_injection_authority)
                .with_volume_wrap_store(volume_wrap_store);
            if let Some(provisioner) = kek_provisioner {
                service = service.with_kek_provisioner(provisioner);
            }
            tracing::info!(
                "database configured; ExchangeToken, Encrypt/Decrypt, \
                 ResolveNetworkCredentials, and GetOrIssueSandboxVolumeDek enabled"
            );
        }
        None => {
            tracing::info!(
                "no DATABASE_URL; Encrypt/Decrypt and \
                 ResolveNetworkCredentials are disabled on this instance"
            );
        }
    }

    Ok(service)
}

fn resolve_storage_vend_role(
    mode: ExchangeTokenMode,
    configured: Result<String, std::env::VarError>,
) -> Result<Option<VendRoleArn>, RunCredmgrError> {
    if mode == ExchangeTokenMode::Disabled {
        return Ok(None);
    }
    let raw = match configured {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => {
            return Err(RunCredmgrError::MissingStorageVendRole);
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(RunCredmgrError::StorageVendRoleNotUnicode);
        }
    };
    VendRoleArn::parse(raw)
        .map(Some)
        .map_err(|_| RunCredmgrError::InvalidStorageVendRole)
}

/// Load the shared AWS SDK configuration only when an AWS-backed feature is
/// enabled. Explicit credentials deliberately bypass the default chain: on a
/// bare EC2 development host, querying instance metadata can otherwise select
/// the host role instead of the operator-injected credentials.
async fn load_authority_aws_config() -> aws_config::SdkConfig {
    let loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    let env_creds = match (
        std::env::var("AWS_ACCESS_KEY_ID")
            .ok()
            .filter(|s| !s.is_empty()),
        std::env::var("AWS_SECRET_ACCESS_KEY")
            .ok()
            .filter(|s| !s.is_empty()),
    ) {
        (Some(access_key_id), Some(secret_access_key)) => {
            Some(aws_credential_types::Credentials::new(
                access_key_id,
                secret_access_key,
                std::env::var("AWS_SESSION_TOKEN")
                    .ok()
                    .filter(|s| !s.is_empty()),
                None,
                "space-credmgr-env",
            ))
        }
        _ => None,
    };
    match env_creds {
        Some(creds) => {
            tracing::info!(
                "AWS SDK bound to injected AWS_* environment credentials \
                 (default credential chain bypassed)"
            );
            loader.credentials_provider(creds).load().await
        }
        None => {
            tracing::info!("AWS SDK using the default credential chain");
            loader.load().await
        }
    }
}

/// Intermediate-key cache policy: the default TTL, which sits inside the
/// 60s–900s window so the revocation SLA can never exceed 15 minutes.
fn intermediate_key_cache_config() -> Result<IntermediateKeyCacheConfig, RunCredmgrError> {
    Ok(IntermediateKeyCacheConfig::default())
}

/// Channel to the intermediate-key broker. An https endpoint requires an
/// explicit CA bundle (the credmgr client's explicit-CA convention); http is
/// for local development only. `connect_lazy` so startup never blocks on
/// provider availability — the backend's per-RPC deadline bounds each call.
fn intermediate_key_broker_channel(
    url: &str,
) -> Result<tonic::transport::Channel, RunCredmgrError> {
    let endpoint = tonic::transport::Endpoint::from_shared(url.to_string()).map_err(|source| {
        RunCredmgrError::IntermediateKeyBrokerUrl {
            url: url.to_string(),
            source,
        }
    })?;
    let endpoint = if url.starts_with("https://") {
        let ca_path = std::env::var("SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_TLS_CA")
            .ok()
            .filter(|value| !value.is_empty())
            .ok_or(RunCredmgrError::MissingIntermediateKeyBrokerCa)?;
        let pem = std::fs::read(&ca_path).map_err(|source| {
            RunCredmgrError::ReadIntermediateKeyBrokerCa {
                path: ca_path.clone(),
                source,
            }
        })?;
        endpoint
            .tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(pem)),
            )
            .map_err(RunCredmgrError::IntermediateKeyBrokerTls)?
    } else {
        endpoint
    };
    Ok(endpoint.connect_lazy())
}

struct NodeLocalActivation {
    health_server: LocalHealthServer,
    service_mutex: ServiceMutex,
}

/// Which listener a supervised credential-manager gRPC server serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialManagerServerKind {
    Uds,
    Tcp,
}

type ServerJoinResult =
    Result<(CredentialManagerServerKind, Result<(), ServeError>), tokio::task::JoinError>;

impl CredentialManagerServerKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Uds => "credential-manager UDS",
            Self::Tcp => "credential-manager TCP",
        }
    }
}

impl fmt::Display for CredentialManagerServerKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

async fn initialize_node_local<T, F>(
    config: &CredmgrRunConfig,
    shutdown_token: &CancellationToken,
    service_mutex_path: &Path,
    initialize: F,
) -> Result<Option<(T, NodeLocalActivation)>, RunCredmgrError>
where
    F: Future<Output = Result<T, RunCredmgrError>>,
{
    let initialized = initialize.await?;
    if let Some(path) = config.volume_socket_path.clone() {
        let standard_socket = config.socket_path.clone();
        // Passive readiness lets the rollout retire the predecessor, so even
        // publishing health must wait until the new listener can be provisioned.
        tokio::task::spawn_blocking(move || {
            volume_socket::preflight(&path, &standard_socket)
                .map_err(|source| RunCredmgrError::VolumeSocketPreflight { path, source })
        })
        .await
        .map_err(RunCredmgrError::VolumeSocketPreflightTask)??;
    }
    let Some(node_local) = prepare_node_local(config, shutdown_token, service_mutex_path).await?
    else {
        return Ok(None);
    };
    Ok(Some((initialized, node_local)))
}

async fn prepare_node_local(
    config: &CredmgrRunConfig,
    shutdown_token: &CancellationToken,
    service_mutex_path: &Path,
) -> Result<Option<NodeLocalActivation>, RunCredmgrError> {
    let health_server = LocalHealthServer::spawn_optional(
        config.health_socket_path.as_deref(),
        pplx_space_common_svc::UnixSocketOptions::new(0o660),
        SERVICE_NAME,
        shutdown_token.clone(),
    )
    .map_err(RunCredmgrError::HealthSocket)?;
    let health_socket = config
        .health_socket_path
        .as_ref()
        .map_or_else(|| "disabled".to_string(), |path| path.display().to_string());

    tracing::info!(
        health_socket = %health_socket,
        service_mutex = %service_mutex_path.display(),
        "space-credmgr-svc passive; waiting for service mutex"
    );

    let service_mutex = match ServiceMutex::acquire_with_timeout(
        service_mutex_path,
        shutdown_token,
        config.service_mutex_timeout,
    )
    .await
    .map_err(RunCredmgrError::ServiceMutex)?
    {
        AcquireOutcome::Acquired(mutex) => mutex,
        AcquireOutcome::PassiveExit => {
            tracing::warn!("terminated while passive; exiting cleanly");
            health_server.join().await;
            return Ok(None);
        }
    };

    tracing::info!(
        service_mutex = %service_mutex.path().display(),
        "acquired service mutex; transitioning to active"
    );
    health_server.mark_starting();

    Ok(Some(NodeLocalActivation {
        health_server,
        service_mutex,
    }))
}

fn bind_credential_manager_socket(
    path: &Path,
) -> Result<tokio::net::UnixListener, RunCredmgrError> {
    pplx_space_common_svc::bind_unix_socket(
        path,
        pplx_space_common_svc::UnixSocketOptions::new(0o770)
            .with_group(pplx_space_common_svc::SocketGroup::Required(SOCKET_GROUP)),
    )
    .map_err(RunCredmgrError::BindSocket)
}

struct CredentialListeners {
    standard: tokio::net::UnixListener,
    volume: Option<tokio::net::UnixListener>,
}

impl CredentialListeners {
    fn bind(config: &CredmgrRunConfig) -> Result<Self, RunCredmgrError> {
        let volume = config
            .volume_socket_path
            .as_ref()
            .map(|path| {
                pplx_space_common_svc::bind_unix_socket(path, volume_socket::options())
                    .map_err(RunCredmgrError::BindSocket)
            })
            .transpose()?;
        Ok(Self {
            standard: bind_credential_manager_socket(&config.socket_path)?,
            volume,
        })
    }

    fn spawn<S: CredentialManager + Clone + Send + 'static>(
        self,
        servers: &mut JoinSet<(CredentialManagerServerKind, Result<(), ServeError>)>,
        service: S,
        shutdown: &CancellationToken,
    ) {
        let mut listeners = vec![(self.standard, CredentialUdsAccess::Standard)];
        if let Some(volume) = self.volume {
            listeners.push((volume, CredentialUdsAccess::Volume));
        }
        for (listener, access) in listeners {
            let service = service.clone();
            let shutdown = shutdown.clone();
            servers.spawn(async move {
                (
                    CredentialManagerServerKind::Uds,
                    serve_uds_until(listener, service, shutdown, access).await,
                )
            });
        }
    }
}

async fn serve_uds_until<S>(
    listener: tokio::net::UnixListener,
    service: S,
    shutdown_token: CancellationToken,
    access: CredentialUdsAccess,
) -> Result<(), ServeError>
where
    S: CredentialManager,
{
    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    pplx_space_common_svc::grpc_server_builder()
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .layer(
            pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(
                "space-credential-manager",
            ),
        )
        .layer(access)
        .add_service(health_service)
        .add_service(CredentialManagerServer::new(service))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
            shutdown_token.cancelled().await
        })
        .await
        .map_err(ServeError::UdsServer)
}

async fn serve_node_local_uds<S>(
    listener: CredentialListeners,
    service: S,
    production_socket: PathBuf,
    node_local: NodeLocalActivation,
    shutdown_token: CancellationToken,
) -> Result<(), RunCredmgrError>
where
    S: CredentialManager + Clone + Send + 'static,
{
    let production_shutdown_token = CancellationToken::new();
    let mut servers = JoinSet::new();
    listener.spawn(&mut servers, service, &production_shutdown_token);

    tracing::info!(
        socket_path = %production_socket.display(),
        "space-credmgr-svc active; listening on production socket"
    );
    node_local.health_server.activate(production_socket);

    supervise_node_local_servers(
        servers,
        node_local,
        shutdown_token,
        production_shutdown_token,
    )
    .await
}

async fn serve_node_local_uds_and_tcp(
    listener: CredentialListeners,
    addr: SocketAddr,
    security: TcpSecurity,
    service: CredentialManagerService,
    production_socket: PathBuf,
    node_local: NodeLocalActivation,
    shutdown_token: CancellationToken,
) -> Result<(), RunCredmgrError> {
    let production_shutdown_token = CancellationToken::new();
    let mut servers = JoinSet::new();
    listener.spawn(&mut servers, service.clone(), &production_shutdown_token);
    let tcp_shutdown_token = production_shutdown_token.clone();
    servers.spawn(async move {
        (
            CredentialManagerServerKind::Tcp,
            serve_tcp_until(addr, security, service, tcp_shutdown_token).await,
        )
    });

    tracing::info!(
        socket_path = %production_socket.display(),
        tcp_listen = %addr,
        "space-credmgr-svc active; listening on production socket and TCP"
    );
    node_local.health_server.activate(production_socket);

    supervise_node_local_servers(
        servers,
        node_local,
        shutdown_token,
        production_shutdown_token,
    )
    .await
}

async fn supervise_node_local_servers(
    mut servers: JoinSet<(CredentialManagerServerKind, Result<(), ServeError>)>,
    node_local: NodeLocalActivation,
    shutdown_token: CancellationToken,
    production_shutdown_token: CancellationToken,
) -> Result<(), RunCredmgrError> {
    let first_exit = tokio::select! {
        biased;
        _ = shutdown_token.cancelled() => None,
        result = servers.join_next() => Some(
            result.expect("node-local credential-manager server set is non-empty")
        ),
    };
    let exited_unexpectedly = first_exit.is_some();
    if exited_unexpectedly {
        shutdown_token.cancel();
    }

    let NodeLocalActivation {
        health_server,
        service_mutex,
    } = node_local;
    let drain = async {
        production_shutdown_token.cancel();
        let mut results = Vec::new();
        if let Some(result) = first_exit {
            results.push(result);
        }
        while let Some(result) = servers.join_next().await {
            results.push(result);
        }
        health_server.join().await;
        results
    };

    let results = match tokio::time::timeout(DEFAULT_DRAIN_TIMEOUT, drain).await {
        Ok(results) => results,
        Err(_) => {
            tracing::warn!(
                drain_timeout_secs = DEFAULT_DRAIN_TIMEOUT.as_secs(),
                "drain budget exhausted; exiting and relying on supervisor SIGKILL"
            );
            drop(service_mutex);
            if exited_unexpectedly {
                return Err(RunCredmgrError::DrainTimedOut);
            }
            return Ok(());
        }
    };

    drop(service_mutex);

    let mut results = results.into_iter();
    if exited_unexpectedly {
        let first = results
            .next()
            .expect("unexpected server exit produces one join result");
        server_join_result(first, true)?;
    }
    for result in results {
        server_join_result(result, false)?;
    }

    Ok(())
}

fn server_join_result(
    result: ServerJoinResult,
    exited_unexpectedly: bool,
) -> Result<(), RunCredmgrError> {
    match result {
        Ok((server_kind, Ok(()))) if exited_unexpectedly => {
            Err(RunCredmgrError::ServerExited { server_kind })
        }
        Ok((_, Ok(()))) => Ok(()),
        Ok((server_kind, Err(source))) if exited_unexpectedly => {
            Err(RunCredmgrError::ServerExitedWithError {
                server_kind,
                source,
            })
        }
        Ok((server_kind, Err(source))) => Err(RunCredmgrError::ServerFailedDuringDrain {
            server_kind,
            source,
        }),
        Err(error) => Err(RunCredmgrError::ServerTask(error)),
    }
}

/// Run as the node credential agent: bind the node-local Unix socket and
/// forward every RPC to the upstream authority. Holds no broker credential,
/// plaintext branching key, or database; those belong to the control-plane
/// authority this agent fronts.
async fn run_agent(
    config: CredmgrRunConfig,
    peer_process_check_mode: PeerProcessCheckMode,
    shutdown_token: CancellationToken,
) -> Result<(), RunCredmgrError> {
    if config.no_uds {
        return Err(RunCredmgrError::AgentNoUds);
    }
    if config.tcp_listen.is_some() || config.tls_cert.is_some() {
        return Err(RunCredmgrError::AgentTcpListener);
    }

    // The upstream must be a TCP/TLS authority. A unix:// or unset endpoint
    // resolves to this agent's own socket, which would loop back on itself.
    let endpoint =
        crate::CredmgrEndpoint::from_env().map_err(RunCredmgrError::AgentUpstreamEndpoint)?;
    crate::node_service::validate_upstream(&endpoint)?;

    let Some((upstream, node_local)) = initialize_node_local(
        &config,
        &shutdown_token,
        Path::new(SERVICE_MUTEX_PATH),
        async move {
            endpoint
                .connect()
                .await
                .map_err(RunCredmgrError::AgentUpstreamConnect)
        },
    )
    .await?
    else {
        return Ok(());
    };
    let listener = CredentialListeners::bind(&config)?;

    tracing::info!(
        socket_path = %config.socket_path.display(),
        "node credential agent listening; forwarding CredentialManager RPCs upstream"
    );

    // Wired unconditionally, with no node-side switch for MITM. An authority
    // with no root CA configured refuses to certify gateway CAs on exactly the
    // clusters that are not running MITM — the one place the decision is
    // already made.
    let node_ca = Arc::new(NodeCa::new(upstream.clone()).map_err(RunCredmgrError::NodeCaKeypair)?);

    serve_node_local_uds(
        listener,
        NodeCredentialManager::new(
            upstream,
            node_ca,
            peer_process_check_mode,
            shutdown_token.clone(),
        ),
        config.socket_path,
        node_local,
        shutdown_token,
    )
    .await
}

async fn serve_tcp_until(
    addr: SocketAddr,
    security: TcpSecurity,
    service: CredentialManagerService,
    shutdown_token: CancellationToken,
) -> Result<(), ServeError> {
    // Kubernetes and direct-node listeners deliberately have disjoint
    // credentials. A TokenReview-configured listener never reads or accepts
    // SPACE_INTERNAL_TOKEN; static auth exists only where there is no apiserver.
    let auth_layer = match crate::k8s_identity::K8sAuthConfig::from_env()
        .map_err(ServeError::K8sAuthConfig)?
    {
        Some(config) => {
            tracing::info!(
                %addr,
                namespace = config.namespace(),
                audience = config.audience(),
                "credential manager TCP listener validating projected ServiceAccount tokens; \
                 the static internal token is not accepted"
            );
            crate::tcp_auth::CredmgrTcpAuthLayer::projected_token(
                crate::k8s_identity::CredmgrTokenReviewer::new(config)
                    .await
                    .map_err(ServeError::TokenReviewClient)?,
            )
        }
        None => {
            tracing::info!(
                %addr,
                "credential manager TCP listener using static-token auth only (k8sAuth disabled)"
            );
            let static_token = pplx_space_common_svc::auth::required_bearer_token(
                pplx_space_common_svc::auth::SPACE_INTERNAL_TOKEN_ENV,
            )
            .map_err(ServeError::InternalToken)?;
            crate::tcp_auth::CredmgrTcpAuthLayer::static_token(static_token)
        }
    };

    let mut builder = pplx_space_common_svc::grpc_server_builder();
    match &security {
        TcpSecurity::ServerTls { cert, key } => {
            let identity =
                tonic::transport::Identity::from_pem(read_pem(cert).await?, read_pem(key).await?);
            builder = builder
                .tls_config(ServerTlsConfig::new().identity(identity))
                .map_err(ServeError::TlsConfig)?;
            tracing::info!(%addr, "credential manager TCP listener using server-side TLS");
        }
        TcpSecurity::Plaintext => {
            tracing::warn!(
                %addr,
                "credential manager TCP listener is PLAINTEXT (--allow-plaintext); \
                 this is only acceptable on non-cloud profiles"
            );
        }
    }

    // The auth layer authenticates every non-health request and injects the
    // resolved caller identity; the health service stays exempt inside the
    // layer so Kubernetes gRPC probes work without a credential.
    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    builder
        .layer(pplx_space_common_svc::grpc_rate_limiter_layer(
            pplx_space_common_svc::GrpcRateLimiterConfig::enforce("space-credential-manager-tcp")
                .with_default_limits(
                    pplx_space_common_svc::GrpcRateLimiterLimits::conservative_worker_default(),
                )
                .with_method_limits(
                    intermediate_ca::ISSUE_INTERMEDIATE_METHOD_PATH,
                    intermediate_ca::issue_rate_limits(),
                )
                // TCP peers carry no Linux peer identity; rate-limit them
                // as one shared bucket instead of rejecting outright.
                .with_unknown_peer_policy(pplx_space_common_svc::UnknownPeerPolicy::SharedBucket),
        ))
        .layer(auth_layer)
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .add_service(health_service)
        .add_service(CredentialManagerServer::new(service))
        .serve_with_shutdown(addr, async move { shutdown_token.cancelled().await })
        .await
        .map_err(ServeError::TcpServer)
}

async fn read_pem(path: &Path) -> Result<Vec<u8>, ServeError> {
    tokio::fs::read(path)
        .await
        .map_err(|source| ServeError::ReadTlsPem {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests;
