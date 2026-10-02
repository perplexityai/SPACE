mod credential_probe;
#[cfg(target_os = "linux")]
mod identity_handoff;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use pplx_space_common_core::host::linux::paths::{
    VOLUME_MANAGER_DB, VOLUME_MANAGER_SOCKET, VOLUME_MANAGER_STAGING_DIR,
};
use pplx_space_volmgr::executor::DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES;
use pplx_space_volmgr::{
    DEFAULT_CODEC_CONCURRENCY, DEFAULT_CODEC_PRIORITY_RESERVE, DEFAULT_PULL_CONCURRENCY,
    DEFAULT_PUSH_CONCURRENCY, DEFAULT_SERVER_DRAIN_TIMEOUT_SECS,
    DEFAULT_SERVICE_MUTEX_TIMEOUT_SECS, DEFAULT_TRANSFER_DRAIN_TIMEOUT_SECS,
    DEFAULT_TRANSFER_MAX_ATTEMPTS, DEFAULT_TRANSFER_TIMEOUT_SECS, PullPriorityBand, SERVICE_NAME,
    VolmgrRunConfig,
};

#[derive(Parser)]
#[command(
    name = "space-volmgr-svc",
    about = "SPACE volume manager — S3-backed volume attachment and file transfer",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Prepare retained node files for the selected identity, then exit (root only).
    #[cfg(target_os = "linux")]
    #[arg(
        long,
        value_enum,
        conflicts_with_all = ["check_volume_credentials", "db", "socket", "staging_dir"]
    )]
    prepare_volume_identity: Option<identity_handoff::Identity>,

    /// Require this retained identity under the service lock before preparing files.
    #[cfg(target_os = "linux")]
    #[arg(long, value_enum, requires = "prepare_volume_identity")]
    expected_volume_identity: Option<identity_handoff::Identity>,

    /// Verify the restricted production credential listener without minting secrets.
    #[arg(long)]
    check_volume_credentials: bool,

    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Unix socket path for the gRPC service. Bound only after the service
    /// mutex is acquired.
    #[arg(long, default_value = VOLUME_MANAGER_SOCKET)]
    socket: PathBuf,

    /// Instance-local health Unix socket. When supplied, it is bound before
    /// waiting for the service mutex so a surge successor is Ready while
    /// passive. It must live on a pod-private mount.
    #[arg(long)]
    health_socket: Option<PathBuf>,

    /// Maximum time to remain ready while waiting for the predecessor's
    /// service mutex. After this budget, the process becomes NotReady and
    /// continues waiting.
    /// With --prepare-volume-identity, expiry instead fails the handoff.
    #[arg(long, default_value_t = DEFAULT_SERVICE_MUTEX_TIMEOUT_SECS)]
    service_mutex_timeout_secs: u64,

    /// SQLite database path for volume attachment and job state.
    #[arg(long, default_value = VOLUME_MANAGER_DB)]
    db: String,

    /// Directory for in-flight push sources. Each push reflinks its
    /// source here at submit time so the upload is immune to later
    /// unlinks of the caller's original path. Must live on the same
    /// filesystem as the push sources (typically `/space/cache`, btrfs)
    /// so reflinks stay O(1).
    #[arg(long, default_value = VOLUME_MANAGER_STAGING_DIR)]
    staging_dir: PathBuf,

    /// Maximum concurrent S3 upload (push) tasks. Paired with the
    /// object-store `MULTIPART_CONCURRENCY` default this gives ~128
    /// in-flight S3 flows per node — enough to saturate a 25 Gbps NIC
    /// without oversubscribing on smaller instance shapes.
    #[arg(long, default_value_t = DEFAULT_PUSH_CONCURRENCY)]
    push_concurrency: usize,

    /// Maximum concurrent S3 download (pull) tasks.
    #[arg(long, default_value_t = DEFAULT_PULL_CONCURRENCY)]
    pull_concurrency: usize,

    /// Maximum concurrent CPU-bound zstd passes (compress or decompress)
    /// across all transfer jobs. Each pass is single-threaded, so running
    /// more of them than the pod's CPU quota just time-slices every job
    /// slower. 0 auto-sizes to the CPU quota.
    #[arg(long, default_value_t = DEFAULT_CODEC_CONCURRENCY)]
    codec_concurrency: usize,

    /// Codec permits reserved for jobs at or above the lowest pull priority
    /// band, carved from the total so protected restores don't queue behind
    /// the bulk codec backlog. Keep it small — codec is CPU-bound at the
    /// quota, so this trades a little bulk throughput for protected-band
    /// latency. Ignored when no pull priority bands are configured.
    #[arg(long, default_value_t = DEFAULT_CODEC_PRIORITY_RESERVE)]
    codec_priority_reserve: usize,

    /// Process-wide high watermark for streamed output awaiting writeback.
    /// Producers resume after the background reaper drains half this amount.
    #[arg(long, default_value_t = DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES)]
    stream_writeback_budget_bytes: u64,

    /// Protected pull capacity as MIN_PRIORITY:CONCURRENCY:ACTIVE_JOBS. Repeat for more bands.
    #[arg(
        long = "pull-priority-band",
        value_name = "MIN_PRIORITY:CONCURRENCY:ACTIVE_JOBS"
    )]
    pull_priority_bands: Vec<PullPriorityBand>,

    /// Per-attempt timeout for a single transfer, in seconds.
    #[arg(long, default_value_t = DEFAULT_TRANSFER_TIMEOUT_SECS)]
    transfer_timeout_secs: u64,

    /// Maximum retry attempts before a transfer job is permanently failed.
    #[arg(long, default_value_t = DEFAULT_TRANSFER_MAX_ATTEMPTS)]
    transfer_max_attempts: i32,

    /// How long to wait for in-flight RPCs after SIGTERM, in seconds.
    #[arg(long, default_value_t = DEFAULT_SERVER_DRAIN_TIMEOUT_SECS)]
    server_drain_timeout_secs: u64,

    /// How long to wait for outstanding transfers before the gRPC server stops, in seconds.
    /// Interrupted transfers remain persisted and are re-queued on startup.
    #[arg(long, default_value_t = DEFAULT_TRANSFER_DRAIN_TIMEOUT_SECS)]
    drain_timeout_secs: u64,

    /// Compress snapshot files with zstd before uploading to S3.
    #[arg(
        long,
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true
    )]
    compress: bool,
    /// Decode zstd pulls inline with the ranged download (one pass)
    /// instead of a separate read-back decompress step.
    #[arg(
        long,
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true
    )]
    stream_decode_pulls: bool,
}

impl From<Cli> for VolmgrRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            socket_path: cli.socket,
            health_socket_path: cli.health_socket,
            service_mutex_timeout: Duration::from_secs(cli.service_mutex_timeout_secs),
            db_path: cli.db,
            staging_dir: cli.staging_dir,
            push_concurrency: cli.push_concurrency,
            pull_concurrency: cli.pull_concurrency,
            codec_concurrency: cli.codec_concurrency,
            codec_priority_reserve: cli.codec_priority_reserve,
            stream_writeback_budget_bytes: cli.stream_writeback_budget_bytes,
            pull_priority_bands: cli.pull_priority_bands,
            transfer_timeout: Duration::from_secs(cli.transfer_timeout_secs),
            transfer_max_attempts: cli.transfer_max_attempts,
            server_drain_timeout: Duration::from_secs(cli.server_drain_timeout_secs),
            transfer_drain_timeout: Duration::from_secs(cli.drain_timeout_secs),
            compress: cli.compress,
            stream_decode_pulls: cli.stream_decode_pulls,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    #[cfg(target_os = "linux")]
    if let Some(identity) = cli.prepare_volume_identity {
        return identity_handoff::prepare(
            std::path::Path::new("/space"),
            identity,
            Duration::from_secs(cli.service_mutex_timeout_secs),
            cli.expected_volume_identity,
        );
    }
    pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure volume-manager Tokio runtime")?
        .block_on(async {
            if cli.check_volume_credentials {
                return tokio::time::timeout(Duration::from_secs(20), credential_probe::check())
                    .await
                    .context("credential probe timed out")?;
            }
            let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
            let shutdown_token = pplx_space_common_svc::shutdown_token();
            let result = pplx_space_volmgr::run(cli.into(), shutdown_token).await;
            pplx_space_common_svc::shutdown_telemetry(telemetry).await;
            Ok(result?)
        })
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
