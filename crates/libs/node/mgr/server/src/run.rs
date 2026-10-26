//! The node-manager service body: host admission and cgroup setup, the
//! network slot pool, the SQLite store, and the Unix-socket gRPC surface the
//! node's other services call.
//!
//! The `space-nodemgr-svc` binary is a thin `main` that parses flags,
//! initializes telemetry, and calls [`run`], so a deployment that needs
//! private wiring can supply its own `main` against this library without
//! copying any of the service.

use std::path::{Path, PathBuf};
use std::time::Duration;

use pplx_space_common_svc::core::host::linux::identities::group_names;
use pplx_space_common_svc::{
    AcquireOutcome, DEFAULT_DRAIN_TIMEOUT, LocalHealthServer, ServiceMutex,
};
use serde::Deserialize;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[cfg(target_os = "linux")]
use crate::disk_io_reservation::AwsImdsInstanceTypeProvider;
use crate::disk_io_reservation::DiskIoReservationConfig;
use crate::memory_admission::MemoryAdmissionConfig;
use crate::proto::node_manager_server::NodeManagerServer;
use crate::service::NodeManagerService;
use crate::slot_pool::{
    NetworkPoolConfig, ServicePortIngressIsolation, ServicePortIngressIsolationParseError,
};

/// The service name node-manager reports to telemetry, names its health and
/// rate-limit surfaces with, and binds its production socket under.
pub const SERVICE_NAME: &str = "space-node-manager";

/// Group name that controls who can connect to the node-manager socket.
const SOCKET_GROUP: &str = group_names::SPACE_NODECLIENTS;
/// Where the binary looks for the node-manager TOML config by default.
pub const DEFAULT_NODE_CONFIG_PATH: &str = "/space/config/nodemgr.toml";
// Hard-coded so two instances on the same host cannot be configured to use
// different mutex files and both acquire concurrently.
const SERVICE_MUTEX_PATH: &str = "/space/run/node-manager/service.mutex";
const RUNTIME_SUPERVISOR_CONNECT_RETRY_HEALTH_SERVICE: &str = "runtime-supervisor-connect-retry";
const RUNTIME_SUPERVISOR_ACTIVATOR: &str = "/opt/space/bin/node-manager-runtime-supervisor.sh";
const RUNTIME_SUPERVISOR_RESTART_REQUIRED: &str = "/space/run/runtime-supervisor/restart-required";
const RUNTIME_SUPERVISOR_WORK_GATE: &str =
    "/space/run/dependency-drain/sandbox-manager/work-v1.lock";
const RUNTIME_SUPERVISOR_HEALTH_SOCKET: &str =
    "/space/run/runtime-supervisor/runtime-supervisor.sock";
const GRPC_HEALTH_PROBE: &str = "/opt/space/bin/grpc_health_probe";
const ENV_ACTIVATE_RUNTIME_SUPERVISOR_ON_START: &str =
    "SPACE_NODEMGR_ACTIVATE_RUNTIME_SUPERVISOR_ON_START";
const ENV_MAX_SLOTS: &[&str] = &["SPACE_NODEMGR_NETWORK_POOL_MAX_SLOTS"];
const ENV_TARGET_READY: &[&str] = &["SPACE_NODEMGR_NETWORK_POOL_TARGET"];
const ENV_RETAINED_READY: &[&str] = &["SPACE_NODEMGR_NETWORK_POOL_RETAIN_READY"];
const ENV_MIN_READY: &[&str] = &["SPACE_NODEMGR_NETWORK_POOL_MIN_READY"];
const ENV_SERVICE_PORT_INGRESS_ISOLATION: &[&str] =
    &["SPACE_NODEMGR_SERVICE_PORT_INGRESS_ISOLATION"];
const ENV_DISK_IO_RESERVATION_ENABLED: &[&str] = &["SPACE_NODEMGR_DISK_IO_RESERVATION_ENABLED"];
const DISK_IO_SANDBOX_CAPACITY_PERCENT: u64 = 70;
const DISK_IO_READ_WEIGHT_PERCENT: u64 = 50;
const DISK_IO_WRITE_WEIGHT_PERCENT: u64 = 50;
static NODE_MANAGER_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("../../../../../db/node-manager");
const REQUIRED_RUNTIME_GROUPS: &[&str] = &[
    group_names::SPACE_NODECLIENTS,
    group_names::SPACE_SBXMGR,
    group_names::SPACE_CACHE_RW,
    group_names::SPACE_SIDECARCLIENTS,
];

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. The TOML config file and the `SPACE_NODEMGR_*`
/// environment overrides stay with the code that reads them, because the
/// precedence between the three is part of the service, not of the caller.
#[derive(Debug, Clone)]
pub struct NodeMgrRunConfig {
    /// Production node-manager Unix socket, bound only after the service
    /// mutex is acquired.
    pub socket_path: PathBuf,
    /// Instance-local health Unix socket, bound from startup so readiness can
    /// pass while this instance is still passive.
    pub health_socket_path: Option<PathBuf>,
    /// How long the passive instance waits for the predecessor to release the
    /// service mutex before exiting non-zero.
    pub service_mutex_timeout: Duration,
    /// Node-manager TOML config path. A missing file is the default config.
    pub config_path: PathBuf,
    /// Maximum number of network slots, which is the node's sandbox
    /// concurrency cap; `None` defers to the config file, then the
    /// environment, then the default.
    pub network_pool_max_slots: Option<u32>,
    /// Desired number of ready network slots maintained in the background.
    pub network_pool_target: Option<u32>,
    /// Ready slots to keep after bursts before pruning surplus idle slots.
    pub network_pool_retain_ready: Option<u32>,
    /// Minimum ready slots before the node should be treated as schedulable.
    pub network_pool_min_ready: Option<u32>,
    /// Optional fixed pod network namespace path to prime every ready slot in.
    pub pod_netns_path: Option<String>,
    /// Service-port ingress isolation policy: auto, shared_host, or
    /// exclusive_host.
    pub service_port_ingress_isolation: Option<String>,
    /// Firecracker host cgroup memory.max in MiB for launched sandboxes.
    pub sandbox_cgroup_memory_mib: Option<u64>,
    /// Firecracker cgroup cpu.max vCPU count for launched sandboxes.
    pub sandbox_cgroup_vcpu_count: Option<u32>,
    /// Node memory kept available in addition to a new sandbox cgroup demand,
    /// in MiB.
    pub memory_admission_reserve_mib: Option<u64>,
}

/// Why [`run`] returned without a clean shutdown. Every variant is a startup
/// failure that must stop the process: node-manager writes host-wide cgroup,
/// loop-device, and firewall state, so a partially configured host is worse
/// than a restart. The configuration variants name the file, environment
/// variable, or value that was misread.
#[derive(Debug, thiserror::Error)]
pub enum RunNodeMgrError {
    #[error("execution selection configuration: {0}")]
    SelectionConfiguration(#[from] crate::config::SelectionConfigurationError),
    #[error(transparent)]
    DiskIoCeiling(#[from] crate::service::disk_io_ceiling::InvalidCeiling),
    #[error("failed to read node config {path}: {source}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse node config {path}: {source}")]
    ParseConfig {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("{name} must be a u32")]
    EnvNotU32 {
        name: &'static str,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("{name} must be true or false, got {value:?}")]
    EnvNotBool { name: &'static str, value: String },
    #[error(
        "service-port ingress isolation must be auto, shared_host, or exclusive_host, got {value}"
    )]
    ServicePortIngressIsolation {
        value: String,
        #[source]
        source: ServicePortIngressIsolationParseError,
    },
    #[error("sandbox_cgroup.vcpu_count must be greater than 0")]
    SandboxCgroupVcpuCountZero,
    #[error(
        "sandbox_cgroup.memory_mib must be greater than sandbox memory headroom \
         ({headroom_mib} MiB), got {memory_mib}"
    )]
    SandboxCgroupMemoryBelowHeadroom { memory_mib: u64, headroom_mib: u64 },
    #[error("failed to look up a required node-manager runtime group: {0}")]
    RuntimeGroupLookup(#[source] pplx_space_common_svc::bootstrap::UnixGroupLookupError),
    #[error("missing required node-manager runtime groups: {}", groups.join(", "))]
    MissingRuntimeGroups { groups: Vec<&'static str> },
    #[error("failed to run runtime-supervisor activator: {0}")]
    RuntimeSupervisorActivator(#[source] std::io::Error),
    #[error("runtime-supervisor activator exited with {status}")]
    RuntimeSupervisorActivatorFailed { status: std::process::ExitStatus },
    #[error("failed to initialize AWS IMDS client: {0}")]
    AwsImdsClient(String),
    #[error("failed to resolve sandbox disk I/O capacity profile: {0}")]
    ResolveDiskIoCapacityProfile(String),
    #[error("failed to bind the node-manager health socket: {0}")]
    HealthSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to acquire the node-manager service mutex: {0}")]
    ServiceMutex(#[source] pplx_space_common_svc::service_mutex::AcquireError),
    #[error("failed to apply sandbox parent CPU limit: {0}")]
    ApplyParentCpuLimit(String),
    #[error("failed to apply sandbox parent memory limit: {0}")]
    ApplyParentMemoryLimit(String),
    #[error("failed to apply sandbox parent disk I/O reservation: {0}")]
    ApplyParentDiskIoReservation(String),
    #[error("failed to migrate node-manager's SQLite database: {0}")]
    Migrate(#[source] pplx_space_sqlite::SqliteError),
    /// Incompatible ownership or slot-store state found before any host limit
    /// could shrink beneath live sandboxes.
    #[error("node-manager slot pool startup validation failed: {0}")]
    ValidateStartup(#[source] crate::slot_pool::PoolOpenError),
    #[error("failed to start the node-manager service: {0}")]
    NodeManagerService(#[source] crate::slot_pool::PoolOpenError),
    #[error("failed to establish execution authority before admission: {0}")]
    ExecutionAuthority(#[source] tonic::Status),
    #[error("failed to remove legacy root namespace service-port DNAT rules: {0}")]
    RemoveLegacyRootIngressDnatRules(#[source] crate::firewall::FirewallError),
    #[error("failed to bind the node-manager socket: {0}")]
    BindSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct NodeConfig {
    network_pool: NetworkPoolFileConfig,
    sandbox_cgroup: SandboxCgroupFileConfig,
    memory_admission: MemoryAdmissionFileConfig,
    disk_io_reservation: DiskIoReservationFileConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DiskIoReservationFileConfig {
    enabled: Option<bool>,
    target_path: Option<PathBuf>,
    capacity_profile: Option<String>,
    sandbox_capacity_percent: Option<u64>,
    read_weight_percent: Option<u64>,
    write_weight_percent: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NetworkPoolFileConfig {
    max_slots: Option<u32>,
    #[serde(alias = "target")]
    target_ready_slots: Option<u32>,
    #[serde(alias = "retain_ready")]
    retained_ready_slots: Option<u32>,
    #[serde(alias = "min_ready")]
    min_ready_slots: Option<u32>,
    pod_netns_path: Option<String>,
    service_port_ingress_isolation: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SandboxCgroupFileConfig {
    vcpu_count: Option<u32>,
    memory_mib: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct MemoryAdmissionFileConfig {
    reserve_mib: Option<u64>,
}

fn read_node_config(path: &Path) -> Result<NodeConfig, RunNodeMgrError> {
    if !path.exists() {
        return Ok(NodeConfig::default());
    }

    let contents = std::fs::read_to_string(path).map_err(|source| RunNodeMgrError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&contents).map_err(|source| RunNodeMgrError::ParseConfig {
        path: path.to_path_buf(),
        source,
    })
}

fn env_u32(names: &[&'static str]) -> Result<Option<u32>, RunNodeMgrError> {
    for name in names {
        let Ok(value) = std::env::var(name) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            return Ok(None);
        }
        return value
            .parse::<u32>()
            .map_err(|source| RunNodeMgrError::EnvNotU32 { name, source })
            .map(Some);
    }
    Ok(None)
}

fn env_string(names: &[&str]) -> Option<String> {
    for name in names {
        let Ok(value) = std::env::var(name) else {
            continue;
        };
        let value = value.trim();
        return (!value.is_empty()).then(|| value.to_string());
    }
    None
}

fn env_bool(names: &[&'static str]) -> Result<Option<bool>, RunNodeMgrError> {
    for name in names {
        let Ok(value) = std::env::var(name) else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            return Ok(None);
        }
        return match value {
            "true" => Ok(Some(true)),
            "false" => Ok(Some(false)),
            _ => Err(RunNodeMgrError::EnvNotBool {
                name,
                value: value.to_string(),
            }),
        };
    }
    Ok(None)
}

fn kubernetes_node_signal_present() -> bool {
    ["SPACE_K8S_NODE_NAME", "KUBERNETES_SERVICE_HOST"]
        .into_iter()
        .any(|name| {
            std::env::var(name)
                .ok()
                .is_some_and(|value| !value.trim().is_empty())
        })
}

fn resolve_service_port_ingress_isolation(
    value: Option<&str>,
    kubernetes_signal_present: bool,
) -> Result<ServicePortIngressIsolation, RunNodeMgrError> {
    let value = value.unwrap_or("auto").trim();
    if value.is_empty() || value == "auto" {
        return Ok(if kubernetes_signal_present {
            ServicePortIngressIsolation::SharedHost
        } else {
            ServicePortIngressIsolation::ExclusiveHost
        });
    }

    value
        .parse::<ServicePortIngressIsolation>()
        .map_err(|source| RunNodeMgrError::ServicePortIngressIsolation {
            value: value.to_string(),
            source,
        })
}

/// All listed settings are non-secret. Log only this explicit allow-list,
/// never an environment dump or the shared TOML document.
fn setting<T: std::fmt::Debug>(
    name: &str,
    cli: Option<T>,
    env: Option<T>,
    file: Option<T>,
    default: T,
) -> T {
    let (value, source) = if let Some(value) = cli {
        (value, "cli")
    } else if let Some(value) = env {
        (value, "env")
    } else if let Some(value) = file {
        (value, "file")
    } else {
        (default, "default")
    };
    tracing::info!(setting = name, source, value = ?value, "resolved startup-only node-manager setting");
    value
}

fn resolve_pool_config(
    cli: &NodeMgrRunConfig,
    file: &NetworkPoolFileConfig,
) -> Result<NetworkPoolConfig, RunNodeMgrError> {
    let max_slots = setting(
        "network_pool.max_slots",
        cli.network_pool_max_slots,
        env_u32(ENV_MAX_SLOTS)?,
        file.max_slots,
        crate::net::DEFAULT_MAX_SLOTS,
    );
    let mut config = NetworkPoolConfig::new(max_slots);
    config.target_ready_slots = setting(
        "network_pool.target_ready_slots",
        cli.network_pool_target,
        env_u32(ENV_TARGET_READY)?,
        file.target_ready_slots,
        config.target_ready_slots,
    );
    config.retained_ready_slots = setting(
        "network_pool.retained_ready_slots",
        cli.network_pool_retain_ready,
        env_u32(ENV_RETAINED_READY)?,
        file.retained_ready_slots,
        config.retained_ready_slots,
    );
    config.min_ready_slots = setting(
        "network_pool.min_ready_slots",
        cli.network_pool_min_ready,
        env_u32(ENV_MIN_READY)?,
        file.min_ready_slots,
        config.min_ready_slots,
    );
    config.pod_netns_path = setting(
        "network_pool.pod_netns_path",
        cli.pod_netns_path.clone().map(Some),
        None,
        file.pod_netns_path.clone().map(Some),
        None,
    );
    let isolation = setting(
        "network_pool.service_port_ingress_isolation",
        cli.service_port_ingress_isolation.clone(),
        env_string(ENV_SERVICE_PORT_INGRESS_ISOLATION),
        file.service_port_ingress_isolation.clone(),
        "auto".to_owned(),
    );
    config.service_port_ingress_isolation =
        resolve_service_port_ingress_isolation(Some(&isolation), kubernetes_node_signal_present())?;
    Ok(config)
}

fn resolve_sandbox_cgroup_config(
    cli: &NodeMgrRunConfig,
    file: &SandboxCgroupFileConfig,
) -> Result<crate::service::SandboxCgroupConfig, RunNodeMgrError> {
    let mut config = crate::service::SandboxCgroupConfig::default();
    config.vcpu_count = setting(
        "sandbox_cgroup.vcpu_count",
        cli.sandbox_cgroup_vcpu_count.map(Some),
        None,
        file.vcpu_count.map(Some),
        config.vcpu_count,
    );
    config.memory_mib = setting(
        "sandbox_cgroup.memory_mib",
        cli.sandbox_cgroup_memory_mib.map(Some),
        None,
        file.memory_mib.map(Some),
        config.memory_mib,
    );
    if config.vcpu_count == Some(0) {
        return Err(RunNodeMgrError::SandboxCgroupVcpuCountZero);
    }
    if let Some(memory_mib) = config.memory_mib {
        let headroom_mib = crate::cgroup::DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB;
        if memory_mib <= headroom_mib {
            return Err(RunNodeMgrError::SandboxCgroupMemoryBelowHeadroom {
                memory_mib,
                headroom_mib,
            });
        }
    }
    Ok(config)
}

fn resolve_memory_admission_config(
    cli: &NodeMgrRunConfig,
    file: &MemoryAdmissionFileConfig,
) -> Result<MemoryAdmissionConfig, RunNodeMgrError> {
    Ok(MemoryAdmissionConfig {
        reserve_mib: setting(
            "memory_admission.reserve_mib",
            cli.memory_admission_reserve_mib,
            None,
            file.reserve_mib,
            MemoryAdmissionConfig::default().reserve_mib,
        ),
    })
}

fn resolve_disk_io_reservation_config(
    file: DiskIoReservationFileConfig,
) -> Result<DiskIoReservationConfig, RunNodeMgrError> {
    let defaults = DiskIoReservationConfig::default();
    Ok(DiskIoReservationConfig {
        enabled: setting(
            "disk_io_reservation.enabled",
            None,
            env_bool(ENV_DISK_IO_RESERVATION_ENABLED)?.map(Some),
            file.enabled.map(Some),
            defaults.enabled,
        ),
        target_path: setting(
            "disk_io_reservation.target_path",
            None,
            None,
            file.target_path,
            defaults.target_path,
        ),
        capacity_profile: setting(
            "disk_io_reservation.capacity_profile",
            None,
            None,
            file.capacity_profile.map(Some),
            defaults.capacity_profile,
        ),
        sandbox_capacity_percent: setting(
            "disk_io_reservation.sandbox_capacity_percent",
            None,
            None,
            file.sandbox_capacity_percent.map(Some),
            Some(DISK_IO_SANDBOX_CAPACITY_PERCENT),
        ),
        read_weight_percent: setting(
            "disk_io_reservation.read_weight_percent",
            None,
            None,
            file.read_weight_percent.map(Some),
            Some(DISK_IO_READ_WEIGHT_PERCENT),
        ),
        write_weight_percent: setting(
            "disk_io_reservation.write_weight_percent",
            None,
            None,
            file.write_weight_percent.map(Some),
            Some(DISK_IO_WRITE_WEIGHT_PERCENT),
        ),
    })
}

async fn resolve_disk_io_capacity_profile(
    config: &mut DiskIoReservationConfig,
) -> Result<(), RunNodeMgrError> {
    #[cfg(target_os = "linux")]
    {
        let instance_type_provider =
            AwsImdsInstanceTypeProvider::new().map_err(RunNodeMgrError::AwsImdsClient)?;
        config
            .resolve_capacity_profile(&instance_type_provider)
            .await
            .map_err(RunNodeMgrError::ResolveDiskIoCapacityProfile)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = config;
        Ok(())
    }
}

fn validate_runtime_groups() -> Result<(), RunNodeMgrError> {
    let mut missing = Vec::new();
    for group in REQUIRED_RUNTIME_GROUPS {
        if !pplx_space_common_svc::unix_group_exists(group)
            .map_err(RunNodeMgrError::RuntimeGroupLookup)?
        {
            missing.push(*group);
        }
    }
    if !missing.is_empty() {
        return Err(RunNodeMgrError::MissingRuntimeGroups { groups: missing });
    }
    Ok(())
}

fn activate_runtime_supervisor_on_start() -> Result<(), RunNodeMgrError> {
    let mut command = crate::host_command::Command::new(RUNTIME_SUPERVISOR_ACTIVATOR);
    command.args([
        "activate",
        RUNTIME_SUPERVISOR_RESTART_REQUIRED,
        RUNTIME_SUPERVISOR_WORK_GATE,
        RUNTIME_SUPERVISOR_HEALTH_SOCKET,
        GRPC_HEALTH_PROBE,
    ]);
    let status = crate::host_command::status(&mut command)
        .map_err(RunNodeMgrError::RuntimeSupervisorActivator)?;
    if !status.success() {
        return Err(RunNodeMgrError::RuntimeSupervisorActivatorFailed { status });
    }
    Ok(())
}

fn runtime_supervisor_activation_enabled(raw: Option<&str>) -> bool {
    pplx_space_common_svc::core::env::env_flag(raw, false)
}

/// Run node-manager until `shutdown_token` fires or a startup step fails.
/// Telemetry must already be initialized by the caller.
pub async fn run(
    cli: NodeMgrRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunNodeMgrError> {
    // The file is shared with other node services. Parse it once; only the
    // sections owned by this daemon reject unknown fields.
    let file = read_node_config(&cli.config_path)?;
    let pool_config = resolve_pool_config(&cli, &file.network_pool)?;
    let sandbox_cgroup_config = resolve_sandbox_cgroup_config(&cli, &file.sandbox_cgroup)?;
    let memory_admission_config = resolve_memory_admission_config(&cli, &file.memory_admission)?;
    let mut disk_io_reservation_config =
        resolve_disk_io_reservation_config(file.disk_io_reservation)?;
    let service_settings = crate::config::ServiceSettings::from_env()?;
    let disk_io_ceiling = crate::service::disk_io_ceiling::DiskIoCeiling::from_env()?;
    tracing::info!(
        maximum_iops = disk_io_ceiling.configured(),
        "operator rootfs disk IOPS ceiling resolved; unset leaves assignments unchanged"
    );
    let disk_metrics_config = crate::service::node_disk_metrics::NodeDiskMetricsConfig::default();
    let cgroup_reclaim_interval = crate::service::cgroup_reclaim::RECLAIM_INTERVAL;
    let btrfs_balance_enabled = crate::service::btrfs_balance::enabled_from_env();
    validate_runtime_groups()?;

    let health_server = LocalHealthServer::spawn_optional(
        cli.health_socket_path.as_deref(),
        pplx_space_common_svc::UnixSocketOptions::new(0o660),
        SERVICE_NAME,
        shutdown_token.clone(),
    )
    .map_err(RunNodeMgrError::HealthSocket)?;
    let health_socket = cli
        .health_socket_path
        .as_ref()
        .map_or_else(|| "disabled".to_string(), |path| path.display().to_string());

    tracing::info!(
        health_socket = %health_socket,
        service_mutex = SERVICE_MUTEX_PATH,
        "space-node-manager passive; waiting for service mutex"
    );

    let service_mutex = match ServiceMutex::acquire_with_timeout(
        SERVICE_MUTEX_PATH,
        &shutdown_token,
        cli.service_mutex_timeout,
    )
    .await
    .map_err(RunNodeMgrError::ServiceMutex)?
    {
        AcquireOutcome::Acquired(mutex) => mutex,
        AcquireOutcome::PassiveExit => {
            tracing::warn!("terminated while passive; exiting cleanly");
            health_server.join().await;
            return Ok(());
        }
    };

    tracing::info!(
        service_mutex = %service_mutex.path().display(),
        "acquired service mutex; transitioning to active"
    );
    health_server.mark_starting();
    // Blocking startup work retains a clone if its awaiting future is dropped.
    let service_mutex = std::sync::Arc::new(service_mutex);

    // Reject incompatible ownership before any host limit can shrink beneath
    // live sandboxes. Pool construction repeats this check before row repair.
    let db_path = Path::new(pplx_space_common_svc::core::host::linux::paths::NODE_MANAGER_DB);
    pplx_space_sqlite::migrate("node-manager", db_path, &NODE_MANAGER_MIGRATOR)
        .await
        .map_err(RunNodeMgrError::Migrate)?;
    crate::slot_pool::NetworkPool::validate_startup(&pool_config, db_path)
        .map_err(RunNodeMgrError::ValidateStartup)?;

    let activate_runtime_supervisor = std::env::var(ENV_ACTIVATE_RUNTIME_SUPERVISOR_ON_START).ok();
    if runtime_supervisor_activation_enabled(activate_runtime_supervisor.as_deref()) {
        activate_runtime_supervisor_on_start()?;
    }

    // Resolve the actual host shape before any cgroup writes. An explicit
    // profile remains available for non-EC2 tests and emergency overrides.
    resolve_disk_io_capacity_profile(&mut disk_io_reservation_config).await?;

    // Host-cgroup writes happen only after we hold the mutex: two coexisting
    // instances would otherwise clobber each other's parent cgroup limits.
    let parent_cpu_reserved_count = crate::cgroup::default_parent_cpu_reserved_count();
    let sandbox_parent_cpu_limit =
        crate::cgroup::apply_parent_cpu_reservation(parent_cpu_reserved_count)
            .map_err(RunNodeMgrError::ApplyParentCpuLimit)?;
    let sandbox_parent_memory_limit = crate::cgroup::apply_parent_memory_limit(
        sandbox_cgroup_config.memory_mib_or_default(),
        pool_config.max_slots,
    )
    .map_err(RunNodeMgrError::ApplyParentMemoryLimit)?;
    let sandbox_parent_disk_io_reservation =
        crate::disk_io_reservation::apply_parent_disk_io_reservation(&disk_io_reservation_config)
            .map_err(RunNodeMgrError::ApplyParentDiskIoReservation)?;

    #[cfg(target_os = "linux")]
    {
        // Loop-device capacity, before any sandbox can ask for a device. A node
        // whose loop module loaded without the image's max_loop drop-in in
        // effect is stuck at the kernel default of 8, which a handful of
        // device-mapper-backed sandbox rootfs stacks exhausts — and the failure
        // surfaces inside losetup, not as a capacity error. Raising it here
        // does not depend on the node image, a reboot, or a module reload.
        //
        // Non-fatal: a node that cannot add devices still serves sandboxes
        // until it runs out, and refusing to start would take away the
        // capacity it does have.
        let loop_target = crate::loop_devices::resolve_target(
            crate::loop_devices::override_from_env().as_deref(),
            pool_config.max_slots,
        );
        match loop_target {
            Ok(target) => {
                // The ioctls are ~0.6ms each, so a first-boot pass over a few
                // thousand devices is seconds of blocking work; keep it off the
                // runtime thread.
                let ensured = tokio::task::spawn_blocking(move || {
                    (target, crate::loop_devices::ensure_capacity(target))
                })
                .await;
                match ensured {
                    Ok((target, Ok(report))) => {
                        let present = report.present();
                        // Which half did work says something different about
                        // the node. Kernel devices missing means the loop
                        // module loaded without the image's max_loop drop-in in
                        // effect, which is a node-image fact worth warning
                        // about. Missing /dev nodes for devices that already
                        // exist is just what a tmpfs /dev looks like on every
                        // start, and warning about it every restart would train
                        // operators to ignore the line that matters.
                        if report.changed_nothing() {
                            tracing::info!(
                                device_target = %target,
                                present,
                                "loop-device capacity already satisfied"
                            );
                        } else if report.kernel_devices_added > 0 {
                            tracing::warn!(
                                device_target = %target,
                                present,
                                kernel_devices_added = report.kernel_devices_added,
                                device_nodes_created = report.device_nodes_created,
                                "added loop devices; this node's loop module loaded without the \
                                 image max_loop drop-in in effect"
                            );
                        } else {
                            tracing::info!(
                                device_target = %target,
                                present,
                                device_nodes_created = report.device_nodes_created,
                                "created device nodes for loop devices the kernel already had; \
                                 this /dev is not devtmpfs-managed"
                            );
                        }
                    }
                    Ok((target, Err(error))) => tracing::warn!(
                        device_target = %target,
                        %error,
                        "failed to raise loop-device capacity; sandbox creates will fail once \
                         the node's existing loop devices are exhausted"
                    ),
                    Err(error) => {
                        tracing::warn!(%error, "loop-device capacity task failed to run");
                    }
                }
            }
            Err(error) => tracing::warn!(
                %error,
                env = crate::loop_devices::LOOP_DEVICE_TARGET_ENV,
                "ignoring malformed loop-device target; leaving the node's capacity as found"
            ),
        }

        if pool_config.service_port_ingress_isolation == ServicePortIngressIsolation::SharedHost {
            let removed = crate::firewall::remove_root_ingress_dnat_rules()
                .map_err(RunNodeMgrError::RemoveLegacyRootIngressDnatRules)?;
            if removed > 0 {
                tracing::warn!(
                    removed,
                    "removed legacy root namespace service-port DNAT rules before node-manager activation"
                );
            }
        }
    }

    let task_tracker = TaskTracker::new();
    // Initialization can start pool workers. Drain them on every exit while
    // retaining active ownership, including a later initialization failure.
    let active_result: Result<(), RunNodeMgrError> = async {
        let service = match NodeManagerService::with_memory_admission_config_and_task_tracker(
            pool_config.clone(),
            sandbox_cgroup_config,
            memory_admission_config,
            service_settings,
            task_tracker.clone(),
            shutdown_token.clone(),
            service_mutex.clone(),
        )
        .await {
            Ok(service) => service.with_disk_io_ceiling(disk_io_ceiling),
            Err(crate::slot_pool::PoolOpenError::StartupCancelled) => return Ok(()),
            Err(error) => return Err(RunNodeMgrError::NodeManagerService(error)),
        };

        service.verify_execution_authority().await
            .map_err(RunNodeMgrError::ExecutionAuthority)?;
        if shutdown_token.is_cancelled() {
            return Ok(());
        }
        // Prepare only the separate managed store and staging root. Failure
        // leaves execution unchanged; each mutation rechecks its own prerequisites.
        // Leftover deletion runs separately from readiness.
        service.prepare_artifact_store().await;
        service.start_guest_network_reconciler();

        let sandbox_cgroup_vcpu_count = sandbox_cgroup_config
            .vcpu_count
            .map_or_else(|| "-".to_string(), |value| value.to_string());
        let sandbox_cgroup_memory_mib = sandbox_cgroup_config
            .memory_mib
            .map_or_else(|| "-".to_string(), |value| value.to_string());
        let sandbox_guest_memory_max_mib = sandbox_cgroup_config.guest_memory_max_mib();
        let sandbox_memory_headroom_mib = crate::cgroup::DEFAULT_SANDBOX_MEMORY_HEADROOM_MIB;

        let listener = pplx_space_common_svc::bind_unix_socket(
            &cli.socket_path,
            pplx_space_common_svc::UnixSocketOptions::new(0o770)
                .with_group(pplx_space_common_svc::SocketGroup::Required(SOCKET_GROUP)),
        )
        .map_err(RunNodeMgrError::BindSocket)?;

        tracing::info!(
            socket_path = %cli.socket_path.display(),
            config_path = %cli.config_path.display(),
            max_slots = pool_config.max_slots,
            target_ready_slots = pool_config.target_ready_slots,
            retained_ready_slots = pool_config.retained_ready_slots,
            min_ready_slots = pool_config.min_ready_slots,
            pod_netns_path = pool_config.pod_netns_path.as_deref().unwrap_or("-"),
            service_port_ingress_isolation = %pool_config.service_port_ingress_isolation,
            sandbox_cgroup_vcpu_count,
            sandbox_cgroup_memory_mib,
            sandbox_guest_memory_max_mib,
            sandbox_memory_headroom_mib,
            memory_admission_reserve_mib = memory_admission_config.reserve_mib,
            sandbox_cgroup_parent_cpu_reserved_count = sandbox_parent_cpu_limit.reserved_count,
            sandbox_cgroup_parent_cpu_max = sandbox_parent_cpu_limit.cpu_max,
            sandbox_cgroup_parent_memory_mib = sandbox_parent_memory_limit.sandbox_cgroup_memory_mib,
            sandbox_cgroup_parent_memory_max_slots = sandbox_parent_memory_limit.max_slots,
            sandbox_cgroup_parent_memory_max = sandbox_parent_memory_limit.memory_max,
            sandbox_cgroup_parent_memory_swap_max = sandbox_parent_memory_limit.memory_swap_max,
            sandbox_cgroup_parent_disk_io_mode = sandbox_parent_disk_io_reservation.mode(),
            sandbox_cgroup_parent_disk_io_device_count = sandbox_parent_disk_io_reservation.device_count(),
            sandbox_cgroup_parent_disk_io = ?sandbox_parent_disk_io_reservation,
            "space-node-manager active; listening on production socket"
        );

        health_server.activate(cli.socket_path.clone());

        // Stop both listeners before draining accepted host work under the mutex.
        let production_shutdown_token = CancellationToken::new();

        crate::service::node_disk_metrics::start_node_disk_metrics(
            service.disk_usage_cache(),
            disk_metrics_config,
            shutdown_token.child_token(),
            &task_tracker,
        );
        crate::service::network_pool_metrics::start_network_pool_metrics(
            service.pool(),
            shutdown_token.child_token(),
            &task_tracker,
        );
        service
            .disk_admission()
            .start_refresh_loop(shutdown_token.child_token(), &task_tracker);
        service.start_prepared_rootfs_reaper(shutdown_token.child_token(), &task_tracker);
        service.start_disk_io_ceiling_reconciler(shutdown_token.child_token(), &task_tracker);
        crate::service::cgroup_reclaim::start_orphan_cgroup_reclaim(
            service.pool(),
            cgroup_reclaim_interval,
            shutdown_token.child_token(),
            &task_tracker,
        );
        crate::service::btrfs_balance::start_btrfs_balance_reconciler(
            btrfs_balance_enabled,
            shutdown_token.child_token(),
            &task_tracker,
        );
        let (health_reporter, health_service) = pplx_space_common_svc::grpc_health_service();
        health_reporter
            .set_service_status(
                RUNTIME_SUPERVISOR_CONNECT_RETRY_HEALTH_SERVICE,
                tonic_health::ServingStatus::Serving,
            )
            .await;
        let production_shutdown = {
            let token = production_shutdown_token.clone();
            async move { token.cancelled().await }
        };
        let server_handle = tokio::spawn(
            pplx_space_common_svc::grpc_server_builder()
                .layer(pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(SERVICE_NAME))
                .layer(pplx_space_common_svc::grpc_trace_layer())
                .add_service(health_service)
                .add_service(NodeManagerServer::new(service.clone()))
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), production_shutdown),
        );

        shutdown_token.cancelled().await;

        let drain = async {
            // Accepted handlers may still register work. Join them before trusting
            // an empty tracker; tracked parents retain ownership of child work.
            production_shutdown_token.cancel();
            service.begin_cleanup_shutdown();
            task_tracker.close();
            let (server_result, ()) = tokio::join!(server_handle, health_server.join());
            task_tracker.wait().await;
            server_result
        };

        if let Err(error) = drain_before_handoff(drain, DEFAULT_DRAIN_TIMEOUT).await {
            tracing::warn!(%error, "node-manager gRPC server task ended unexpectedly");
        }
        Ok(())
    }
    .await;
    shutdown_token.cancel();
    task_tracker.close();
    drain_before_handoff(task_tracker.wait(), DEFAULT_DRAIN_TIMEOUT).await;
    drop(service_mutex);
    active_result
}

/// A deadline signals escalation, never permission to release exclusion.
async fn drain_before_handoff<F: std::future::Future>(drain: F, budget: Duration) -> F::Output {
    tokio::pin!(drain);
    match tokio::time::timeout(budget, &mut drain).await {
        Ok(result) => result,
        Err(_) => {
            tracing::error!(
                drain_timeout_secs = budget.as_secs(),
                "drain budget exhausted; retaining service mutex until mutations stop; operator must verify child termination before forced handoff"
            );
            drain.await
        }
    }
}

#[cfg(test)]
mod tests;
