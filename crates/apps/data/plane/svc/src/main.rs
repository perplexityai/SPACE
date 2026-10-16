use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use clap::Parser;
use pplx_space_data_plane_server::{
    DEFAULT_ATTACH_DEADLINE_MS, DEFAULT_BACKEND_CHANNEL_POOL_SIZE, DEFAULT_LISTEN,
    DEFAULT_MAX_ATTACHED_STREAMS, DEFAULT_MAX_ATTACHED_STREAMS_PER_ORG,
    DEFAULT_MAX_ATTACHED_STREAMS_PER_SANDBOX, DEFAULT_MAX_CONCURRENT_UPLOADS,
    DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES, DEFAULT_REVOCATION_CHECK_INTERVAL_MS,
    DEFAULT_ROUTE_CACHE_TTL_MS, DEFAULT_UNAUTHENTICATED_UPGRADE_CAP_PER_IP,
    DEFAULT_UPLOAD_ADMISSION_WAIT, DataPlaneConfig, UploadAdmissionConfig,
};

// Same allocator choice as space-api-svc: glibc arenas keep the peak of an
// upload burst resident after the traffic stops, so RSS tracks the largest
// burst rather than live uploads. Static musl builds keep the target allocator.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Without background threads, jemalloc purges an arena's dirty pages only when
// that arena next allocates or frees, so a replica that goes idle after a burst
// keeps the burst resident just as glibc does. Background threads purge on the
// decay timer instead. jemalloc defines `malloc_conf` as a weak symbol, so this
// definition replaces it; `_RJEM_MALLOC_CONF` still overrides it at startup.
// The symbol is a C `const char *`: a reference to a sized array is a thin
// pointer with that layout, where `&[u8]` would be a (pointer, length) pair.
// jemalloc reads the string before `main`, so the assertion keeps a dropped
// terminator from becoming an out-of-bounds read that no test would observe.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const JEMALLOC_CONF: &[u8; 23] = b"background_thread:true\0";
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const _: () = assert!(matches!(JEMALLOC_CONF.last(), Some(0)));
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[unsafe(export_name = "_rjem_malloc_conf")]
static MALLOC_CONF: &[u8; 23] = JEMALLOC_CONF;

const SERVICE_NAME: &str = "space-data-plane";

#[derive(Parser)]
#[command(
    name = "space-data-plane-svc",
    about = "SPACE public data-plane stream gateway",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Listen address.
    #[arg(long, default_value = DEFAULT_LISTEN)]
    listen: String,

    /// Internal stream-load listen address, read by the cluster controller to
    /// order scale-in. Unset serves no internal listener. Keep this port off
    /// the public Service.
    #[arg(long)]
    internal_listen: Option<String>,

    /// First-frame attach deadline for upgraded sockets without a header token.
    #[arg(long, default_value_t = DEFAULT_ATTACH_DEADLINE_MS)]
    attach_deadline_ms: u64,

    /// Maximum unauthenticated upgraded sockets per source IP.
    #[arg(long, default_value_t = DEFAULT_UNAUTHENTICATED_UPGRADE_CAP_PER_IP)]
    unauthenticated_upgrade_cap_per_ip: usize,

    /// Maximum authenticated attached native streams per replica.
    #[arg(long, default_value_t = DEFAULT_MAX_ATTACHED_STREAMS)]
    max_attached_streams: usize,

    /// Maximum authenticated attached native streams per org on this replica.
    #[arg(long, default_value_t = DEFAULT_MAX_ATTACHED_STREAMS_PER_ORG)]
    max_attached_streams_per_org: usize,

    /// Maximum authenticated attached native streams per sandbox on this replica.
    #[arg(long, default_value_t = DEFAULT_MAX_ATTACHED_STREAMS_PER_SANDBOX)]
    max_attached_streams_per_sandbox: usize,

    /// HTTP/2 channels kept per backend node-api endpoint.
    #[arg(long, default_value_t = DEFAULT_BACKEND_CHANNEL_POOL_SIZE)]
    backend_channel_pool_size: usize,

    /// Interval between revocation checks for long-lived attached streams.
    #[arg(long, default_value_t = DEFAULT_REVOCATION_CHECK_INTERVAL_MS)]
    revocation_check_interval_ms: u64,

    /// Short-lived TTL for token-scoped route resolution cache entries.
    #[arg(long, default_value_t = DEFAULT_ROUTE_CACHE_TTL_MS)]
    route_cache_ttl_ms: u64,

    /// Admit `POST /files` uploads under the per-replica concurrency and
    /// in-flight byte budget below; rejected uploads get 429. Accepts the
    /// usual boolean spellings (`true`/`1`/`yes`/`on` and their negations).
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_UPLOAD_ADMISSION",
        action = clap::ArgAction::Set,
        value_parser = clap::builder::BoolishValueParser::new(),
        num_args = 0..=1,
        default_value_t = false,
        default_missing_value = "true"
    )]
    upload_admission: bool,

    /// Maximum concurrent `POST /files` uploads per replica.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_CONCURRENT_UPLOADS",
        default_value_t = DEFAULT_MAX_CONCURRENT_UPLOADS
    )]
    max_concurrent_uploads: NonZeroUsize,

    /// In-flight upload bytes per replica, reserved from `Content-Length`
    /// (the write path's body limit when absent: 256 MiB, or 1 MiB for JSON).
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_MAX_IN_FLIGHT_UPLOAD_BYTES",
        default_value_t = DEFAULT_MAX_IN_FLIGHT_UPLOAD_BYTES
    )]
    max_in_flight_upload_bytes: NonZeroU64,

    /// How long an upload waits for admission before it's rejected.
    #[arg(
        long,
        env = "SPACE_DATA_PLANE_UPLOAD_ADMISSION_WAIT_MS",
        default_value_t = DEFAULT_UPLOAD_ADMISSION_WAIT.as_millis() as u64
    )]
    upload_admission_wait_ms: u64,
}

impl From<Cli> for DataPlaneConfig {
    fn from(cli: Cli) -> Self {
        Self {
            listen: cli.listen,
            internal_listen: cli.internal_listen,
            attach_deadline: Duration::from_millis(cli.attach_deadline_ms),
            unauthenticated_upgrade_cap_per_ip: cli.unauthenticated_upgrade_cap_per_ip,
            max_attached_streams: cli.max_attached_streams,
            max_attached_streams_per_org: cli.max_attached_streams_per_org,
            max_attached_streams_per_sandbox: cli.max_attached_streams_per_sandbox,
            backend_channel_pool_size: cli.backend_channel_pool_size,
            revocation_check_interval: Duration::from_millis(
                cli.revocation_check_interval_ms.max(1),
            ),
            route_cache_ttl: Duration::from_millis(cli.route_cache_ttl_ms.max(1)),
            upload_admission: cli.upload_admission.then(|| UploadAdmissionConfig {
                max_concurrent_uploads: cli.max_concurrent_uploads,
                max_in_flight_bytes: cli.max_in_flight_upload_bytes,
                wait: Duration::from_millis(cli.upload_admission_wait_ms),
            }),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
    let shutdown_token = pplx_space_common_svc::shutdown_token();

    let result = pplx_space_data_plane_server::run(cli.into(), shutdown_token).await;
    pplx_space_common_svc::shutdown_telemetry(telemetry).await;
    Ok(result?)
}

#[cfg(test)]
mod tests;
