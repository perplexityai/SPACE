use clap::Parser;
use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};
use pplx_space_volmgr_client::proto::{
    AttachVolumeRequest, BatchDecryptionConfig, BatchEncryptionConfig, DetachVolumeRequest,
    ExistingFilePolicy, GetTransferJobRequest, ListTransferJobsRequest, ListVolumesRequest,
    PullFileBatchEntry, PullFileRequest, PullFilesAsyncRequest, PushFileBatchEntry,
    PushFileRequest, PushFilesAsyncRequest, SnapshotArtifact, TransferJob, TransferJobStatus,
    VolumeSpec,
};

#[derive(Parser)]
#[command(
    name = "space-volmgr",
    about = "SPACE volume manager CLI (debug tool)",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Volume-manager Unix socket path.
    #[arg(long, default_value = pplx_space_volmgr_client::VOLMGR_SOCKET)]
    socket: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Attach an S3-backed volume to a sandbox.
    Attach {
        /// Sandbox ID.
        #[arg(long)]
        sandbox_id: String,
        /// User email used for volume access auditing.
        #[arg(long, default_value = "")]
        user_email: String,
        /// Mount name visible in the guest.
        #[arg(long)]
        name: String,
        /// S3 bucket name.
        #[arg(long)]
        s3_bucket: String,
        /// Optional key prefix within the bucket.
        #[arg(long, default_value = "")]
        s3_prefix: String,
        /// AWS region for the S3 bucket.
        #[arg(long)]
        s3_region: String,
        /// Attach the volume read-only.
        #[arg(long)]
        read_only: bool,
        /// Optional BYOK key reference.
        #[arg(long)]
        byok_key_id: Option<String>,
    },
    /// Detach a volume from a sandbox.
    Detach {
        /// Sandbox ID.
        #[arg(long)]
        sandbox_id: String,
        /// Volume mount name.
        #[arg(long)]
        volume_name: String,
    },
    /// List volumes attached to a sandbox.
    Volumes {
        /// Sandbox ID.
        #[arg(long)]
        sandbox_id: String,
    },
    /// Upload a local file to S3 and wait for completion.
    PushFile {
        /// Principal on whose behalf the transfer runs: sandbox:<uuid> or template:<name>.
        #[arg(long)]
        subject: String,
        /// Absolute source path on the worker node.
        #[arg(long)]
        local_path: String,
        /// S3 destination URI, including region query parameter.
        #[arg(long)]
        s3_uri: String,
    },
    /// Download a file from S3 and wait for completion.
    PullFile {
        /// Principal on whose behalf the transfer runs: sandbox:<uuid> or template:<name>.
        #[arg(long)]
        subject: String,
        /// S3 source URI, including region query parameter.
        #[arg(long)]
        s3_uri: String,
        /// Absolute destination path on the worker node.
        #[arg(long)]
        local_path: String,
        /// Write exactly the bytes served by S3 without transparent zstd decompression.
        #[arg(long)]
        raw: bool,
        /// Scheduling priority. Higher values run first.
        #[arg(long, default_value_t = 0)]
        priority: i32,
    },
    /// Submit an upload job and return immediately.
    SubmitPush {
        /// Principal on whose behalf the transfer runs: sandbox:<uuid> or template:<name>.
        #[arg(long)]
        subject: String,
        /// Absolute source path on the worker node.
        #[arg(long)]
        local_path: String,
        /// S3 destination URI, including region query parameter.
        #[arg(long)]
        s3_uri: String,
    },
    /// Submit a download job and return immediately.
    SubmitPull {
        /// Principal on whose behalf the transfer runs: sandbox:<uuid> or template:<name>.
        #[arg(long)]
        subject: String,
        /// S3 source URI, including region query parameter.
        #[arg(long)]
        s3_uri: String,
        /// Absolute destination path on the worker node.
        #[arg(long)]
        local_path: String,
        /// Write exactly the bytes served by S3 without transparent zstd decompression.
        #[arg(long)]
        raw: bool,
        /// Scheduling priority. Higher values run first.
        #[arg(long, default_value_t = 0)]
        priority: i32,
    },
    /// Submit a multi-file artifact upload (`PushFilesAsync`) and return
    /// the artifact `job_id` plus the wrap (hex) when encryption is set.
    /// Debug/test surface — production callers go through the client
    /// crate. Entries are repeated `--entry role:local_path:s3_uri`
    /// triples; the role appears in the per-chunk AAD and must be
    /// unique within the artifact for encrypted batches.
    PushArtifact {
        /// Principal for the artifact: `sandbox:<uuid>` for encrypted
        /// batches; any subject for plaintext.
        #[arg(long)]
        subject: String,
        /// Caller-minted UUID identity of the artifact. Reused on
        /// retries to recover the same wrap.
        #[arg(long)]
        artifact_id: String,
        /// One per file: `role:local_path:s3_uri`. Repeatable.
        #[arg(long = "entry", required = true)]
        entries: Vec<String>,
        /// Expected owning org UUID of the sandbox; encrypts the batch
        /// under that org's active KEK. Omit for a plaintext batch.
        #[arg(long)]
        organization_id: Option<String>,
    },
    /// Submit a multi-file artifact download (`PullFilesAsync`) and
    /// return the artifact `job_id`. Debug/test surface mirroring
    /// `push-artifact`; entries are `role:local_path:s3_uri` triples
    /// (same field order as `push-artifact` — the s3 URI goes last so
    /// the splitter's third field absorbs its inner colons).
    PullArtifact {
        /// Principal for the artifact: `sandbox:<uuid>` for encrypted
        /// batches; any subject for plaintext.
        #[arg(long)]
        subject: String,
        /// Same `artifact_id` supplied to the original `push-artifact`.
        #[arg(long)]
        artifact_id: String,
        /// One per file: `role:local_path:s3_uri`. Repeatable.
        #[arg(long = "entry", required = true)]
        entries: Vec<String>,
        /// Expected owning org UUID of the sandbox; omit for a plaintext
        /// batch.
        #[arg(long, requires = "wrap_hex")]
        organization_id: Option<String>,
        /// Wrapped DEK as hex (from the `push-artifact` response or
        /// cluster-api's snapshot sidecar). Required for encrypted
        /// batches.
        #[arg(long)]
        wrap_hex: Option<String>,
    },
    /// Show one transfer job.
    Job {
        /// Transfer job ID.
        job_id: String,
    },
    /// List transfer jobs.
    Jobs {
        /// Filter by subject: sandbox:<uuid> or template:<name>.
        #[arg(long)]
        subject: Option<String>,
        /// Filter by transfer status.
        #[arg(long)]
        status: Option<TransferStatus>,
        /// Maximum jobs to return. 0 uses the server default.
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum TransferStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl TransferStatus {
    fn to_proto(self) -> TransferJobStatus {
        match self {
            Self::Pending => TransferJobStatus::Pending,
            Self::Running => TransferJobStatus::Running,
            Self::Completed => TransferJobStatus::Completed,
            Self::Failed => TransferJobStatus::Failed,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pplx_space_common_cli::init_logging("space-volmgr");
    let cli = Cli::parse();
    let mut client = pplx_space_volmgr_client::connect(&cli.socket)
        .await
        .map_err(|err| anyhow::anyhow!("cannot connect to volume-manager: {err}"))?;

    match cli.command {
        Commands::Attach {
            sandbox_id,
            user_email,
            name,
            s3_bucket,
            s3_prefix,
            s3_region,
            read_only,
            byok_key_id,
        } => {
            client
                .attach_volume(AttachVolumeRequest {
                    sandbox_id: sandbox_id.clone(),
                    user_email,
                    volume: Some(VolumeSpec {
                        name: name.clone(),
                        s3_bucket,
                        s3_prefix,
                        s3_region,
                        read_only,
                        byok_key_id,
                    }),
                })
                .await?;
            println!("attached volume {name} to {sandbox_id}");
        }
        Commands::Detach {
            sandbox_id,
            volume_name,
        } => {
            client
                .detach_volume(DetachVolumeRequest {
                    sandbox_id: sandbox_id.clone(),
                    volume_name: volume_name.clone(),
                })
                .await?;
            println!("detached volume {volume_name} from {sandbox_id}");
        }
        Commands::Volumes { sandbox_id } => {
            let response = client
                .list_volumes(ListVolumesRequest { sandbox_id })
                .await?
                .into_inner();
            print_volumes(response.volumes);
        }
        Commands::PushFile {
            subject,
            local_path,
            s3_uri,
        } => {
            let response = client
                .push_file(PushFileRequest {
                    subject_id: subject,
                    local_path,
                    s3_uri,
                })
                .await?
                .into_inner();
            println!("uploaded {} bytes", response.bytes_uploaded);
        }
        Commands::PullFile {
            subject,
            s3_uri,
            local_path,
            raw,
            priority,
        } => {
            let response = client
                .pull_file(PullFileRequest {
                    subject_id: subject,
                    s3_uri,
                    local_path,
                    raw,
                    file_mode: None,
                    existing_file_policy: ExistingFilePolicy::LegacySkipPresent as i32,
                    priority,
                    snapshot_artifact: SnapshotArtifact::Unspecified as i32,
                    transfer_scope_id: None,
                })
                .await?
                .into_inner();
            println!("downloaded {} bytes", response.bytes_downloaded);
        }
        Commands::SubmitPush {
            subject,
            local_path,
            s3_uri,
        } => {
            let response = client
                .submit_push_file(PushFileRequest {
                    subject_id: subject,
                    local_path,
                    s3_uri,
                })
                .await?
                .into_inner();
            println!("{}", response.job_id);
        }
        Commands::SubmitPull {
            subject,
            s3_uri,
            local_path,
            raw,
            priority,
        } => {
            let response = client
                .submit_pull_file(PullFileRequest {
                    subject_id: subject,
                    s3_uri,
                    local_path,
                    raw,
                    file_mode: None,
                    existing_file_policy: ExistingFilePolicy::LegacySkipPresent as i32,
                    priority,
                    snapshot_artifact: SnapshotArtifact::Unspecified as i32,
                    transfer_scope_id: None,
                })
                .await?
                .into_inner();
            println!("{}", response.job_id);
        }
        Commands::PushArtifact {
            subject,
            artifact_id,
            entries,
            organization_id,
        } => {
            let mut files = Vec::with_capacity(entries.len());
            for raw in &entries {
                let mut parts = raw.splitn(3, ':');
                let (Some(role), Some(local_path), Some(s3_uri)) =
                    (parts.next(), parts.next(), parts.next())
                else {
                    anyhow::bail!("--entry expects `role:local_path:s3_uri`, got {raw:?}");
                };
                files.push(PushFileBatchEntry {
                    role: role.to_string(),
                    local_path: local_path.to_string(),
                    s3_uri: s3_uri.to_string(),
                    snapshot_artifact: SnapshotArtifact::Unspecified as i32,
                });
            }
            let encryption =
                organization_id.map(|organization_id| BatchEncryptionConfig { organization_id });
            let response = client
                .push_files_async(PushFilesAsyncRequest {
                    subject_id: subject,
                    files,
                    encryption,
                    artifact_id,
                })
                .await?
                .into_inner();
            println!("job_id={}", response.job_id);
            if let Some(wrap) = response.wrapped_dek {
                let mut hex = String::with_capacity(wrap.len() * 2);
                for b in &wrap {
                    use std::fmt::Write as _;
                    write!(&mut hex, "{b:02x}").expect("writing to a String cannot fail");
                }
                println!("wrap_hex={hex}");
                println!("wrap_len={}", wrap.len());
            }
        }
        Commands::PullArtifact {
            subject,
            artifact_id,
            entries,
            organization_id,
            wrap_hex,
        } => {
            let mut files = Vec::with_capacity(entries.len());
            for raw in &entries {
                let mut parts = raw.splitn(3, ':');
                let (Some(role), Some(local_path), Some(s3_uri)) =
                    (parts.next(), parts.next(), parts.next())
                else {
                    anyhow::bail!("--entry expects `role:local_path:s3_uri`, got {raw:?}");
                };
                files.push(PullFileBatchEntry {
                    role: role.to_string(),
                    local_path: local_path.to_string(),
                    s3_uri: s3_uri.to_string(),
                    file_mode: None,
                    snapshot_artifact: SnapshotArtifact::Unspecified as i32,
                });
            }
            let encryption = match (organization_id, wrap_hex) {
                (Some(organization_id), Some(hex)) => Some(BatchDecryptionConfig {
                    organization_id,
                    wrapped_dek: hex_to_bytes(&hex)?,
                }),
                (None, None) => None,
                _ => anyhow::bail!(
                    "--organization-id and --wrap-hex must be set together for an encrypted batch"
                ),
            };
            let response = client
                .pull_files_async(PullFilesAsyncRequest {
                    subject_id: subject,
                    files,
                    encryption,
                    artifact_id,
                })
                .await?
                .into_inner();
            println!("job_id={}", response.job_id);
        }
        Commands::Job { job_id } => {
            let response = client
                .get_transfer_job(GetTransferJobRequest { job_id })
                .await?
                .into_inner();
            let job = response
                .job
                .ok_or_else(|| anyhow::anyhow!("volume-manager returned an empty job"))?;
            print_job(&job);
        }
        Commands::Jobs {
            subject,
            status,
            limit,
        } => {
            let response = client
                .list_transfer_jobs(ListTransferJobsRequest {
                    subject_id: subject,
                    status_filter: status.map(|s| s.to_proto() as i32),
                    limit,
                })
                .await?
                .into_inner();
            print_jobs(response.jobs);
        }
    }

    Ok(())
}

fn print_job(job: &TransferJob) {
    println!("id: {}", job.job_id);
    println!("status: {}", status_name(job.status));
    println!("kind: {}", job.kind);
    println!("subject: {}", job.subject_id);
    println!("local_path: {}", job.local_path);
    println!("s3_uri: {}", job.s3_uri);
    println!("bytes_transferred: {}", job.bytes_transferred);
    println!("created_at: {}", job.created_at);
    if let Some(started_at) = &job.started_at {
        println!("started_at: {started_at}");
    }
    if let Some(completed_at) = &job.completed_at {
        println!("completed_at: {completed_at}");
    }
    if let Some(error) = &job.error {
        println!("error: {error}");
    }
}

fn print_jobs(jobs: Vec<TransferJob>) {
    if jobs.is_empty() {
        println!("No transfer jobs.");
        return;
    }

    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "Job ID",
        "Status",
        "Kind",
        "Subject",
        "Bytes",
        "Created",
        "Completed",
        "Error",
    ]);

    for job in jobs {
        table.add_row(vec![
            job.job_id,
            status_name(job.status).to_string(),
            job.kind,
            job.subject_id,
            job.bytes_transferred.to_string(),
            job.created_at,
            job.completed_at.unwrap_or_default(),
            job.error.unwrap_or_default(),
        ]);
    }

    println!("{table}");
}

fn print_volumes(volumes: Vec<VolumeSpec>) {
    if volumes.is_empty() {
        println!("No volumes.");
        return;
    }

    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "Name",
        "Bucket",
        "Prefix",
        "Region",
        "Read Only",
        "BYOK Key",
    ]);

    for volume in volumes {
        table.add_row(vec![
            volume.name,
            volume.s3_bucket,
            volume.s3_prefix,
            volume.s3_region,
            volume.read_only.to_string(),
            volume.byok_key_id.unwrap_or_default(),
        ]);
    }

    println!("{table}");
}

/// Parse a hex-encoded byte string (no `0x` prefix, no separators).
/// Used by `pull-artifact --wrap-hex` to consume the wrap that
/// `push-artifact` emits on stdout.
fn hex_to_bytes(s: &str) -> anyhow::Result<Vec<u8>> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        anyhow::bail!("--wrap-hex must have even length (got {} chars)", s.len());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for (chunk_index, chunk) in s.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let i = chunk_index * 2;
        let hex_pair = std::str::from_utf8(chunk).map_err(|e| {
            anyhow::anyhow!("--wrap-hex contains non-UTF-8 bytes at offset {i}: {e}")
        })?;
        out.push(u8::from_str_radix(hex_pair, 16).map_err(|e| {
            anyhow::anyhow!("--wrap-hex contains non-hex bytes at offset {i}: {e}")
        })?);
    }
    Ok(out)
}

fn status_name(status: i32) -> &'static str {
    match TransferJobStatus::try_from(status).unwrap_or(TransferJobStatus::Unspecified) {
        TransferJobStatus::Pending => "pending",
        TransferJobStatus::Running => "running",
        TransferJobStatus::Completed => "completed",
        TransferJobStatus::Failed => "failed",
        TransferJobStatus::Cancelling => "cancelling",
        TransferJobStatus::Cancelled => "cancelled",
        TransferJobStatus::Unspecified => "unknown",
    }
}
