use super::cleanup_support::{ActivationCleanupSpec, CleanupLockTryResult};
use super::create_support::{sanitize_process_env, validate_path_component};
use super::tail_support::{host_architecture, sandbox_execution_metadata};
use super::*;

fn import_header_execution_metadata(
    header: &crate::proto::ImportSandboxHeader,
) -> Result<pplx_space_common_core::types::ExecutionMetadata, Status> {
    let runtime_kind = header
        .runtime
        .parse::<pplx_space_common_core::types::RuntimeKind>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let execution = match header.execution.as_ref() {
        Some(execution) => pplx_space_common_core::types::ExecutionMetadata::try_from(execution)
            .map_err(|error| Status::invalid_argument(error.to_string())),
        None if runtime_kind == pplx_space_common_core::types::RuntimeKind::Firecracker => Ok(
            pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
                host_architecture().map_err(|error| Status::invalid_argument(error.to_string()))?,
                vec![],
            ),
        ),
        None => Err(Status::invalid_argument(format!(
            "import header is missing execution metadata for runtime {runtime_kind}"
        ))),
    }?;
    if execution.runtime().kind != runtime_kind {
        return Err(Status::invalid_argument(
            "import runtime does not match execution metadata",
        ));
    }
    // This transport carries no image contract. Qualify its legacy semantics
    // against the bound boot protocol before allocating or copying files.
    pplx_space_node_runtime_core::boot::GuestProfile::resolve(&execution, Default::default())
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(execution)
}

#[derive(Clone)]
struct ValidatedSandboxImport {
    sandbox_id: SandboxId,
    auto_lifecycle: AutoLifecyclePolicy,
    network_policy_authority: crate::db::PolicyAuthority,
    auto_lifecycle_authority: crate::db::PolicyAuthority,
    process_env: BTreeMap<String, String>,
    execution: pplx_space_common_core::types::ExecutionMetadata,
}

struct SandboxImportPaths {
    sandbox_base: String,
    instance_dir: String,
    snapshot_dir: String,
    instance_staging: String,
    snapshot_staging: String,
}

impl SandboxImportPaths {
    fn new(sandbox_id: &str) -> Self {
        let sandbox_base =
            pplx_space_common_core::host::linux::paths::sandbox_cache_dir(sandbox_id);
        let instance_dir = format!("{sandbox_base}/instance");
        let snapshot_dir = format!("{sandbox_base}/snapshot");
        let import_id = uuid::Uuid::now_v7();
        let instance_staging = format!("{instance_dir}.{import_id}.importing");
        let snapshot_staging = format!("{snapshot_dir}.{import_id}.importing");
        Self {
            sandbox_base,
            instance_dir,
            snapshot_dir,
            instance_staging,
            snapshot_staging,
        }
    }
}

fn validate_import_sandbox_file_name(name: &str) -> Result<(&str, bool), Status> {
    let (base_name, is_snapshot_file) = if let Some(base) = name.strip_prefix("snapshot/") {
        (base, true)
    } else {
        (name, false)
    };
    if base_name.contains("..")
        || base_name.contains('/')
        || base_name.contains('\\')
        || base_name.is_empty()
        || base_name == "."
    {
        return Err(Status::invalid_argument(format!(
            "invalid file name in sandbox stream: {name:?}"
        )));
    }
    Ok((base_name, is_snapshot_file))
}

async fn read_import_sandbox_header(
    stream: &mut (impl tokio_stream::Stream<Item = Result<ImportSandboxRequest, Status>> + Unpin),
    deadline: receive::ImportDeadline,
) -> Result<crate::proto::ImportSandboxHeader, Status> {
    let first = receive::next_message(stream, deadline)
        .await?
        .ok_or_else(|| Status::invalid_argument("empty import stream"))?;

    match first.payload {
        Some(crate::proto::import_sandbox_request::Payload::Header(header)) => Ok(header),
        _ => Err(Status::invalid_argument(
            "first message must be an ImportSandboxHeader",
        )),
    }
}

fn validate_import_sandbox_header(
    header: &crate::proto::ImportSandboxHeader,
) -> Result<ValidatedSandboxImport, Status> {
    if header.volume_encryption {
        return Err(Status::failed_precondition(
            "encrypted sandbox transfer is not supported",
        ));
    }
    let sandbox_id = header.sandbox_id.parse::<SandboxId>().map_err(|error| {
        Status::invalid_argument(format!("invalid sandbox_id in import stream: {error}"))
    })?;

    validate_path_component("template_id", &header.template_id)?;
    validate_path_component("content_hash", &header.content_hash)?;
    if let Some(size_mib) = header.rootfs_size_mb {
        pplx_space_common_core::sandbox_storage::validate_rootfs_mib(size_mib)
            .map_err(|error| Status::invalid_argument(format!("rootfs_size_mb: {error}")))?;
    }
    let process_env: BTreeMap<String, String> = header.env.clone().into_iter().collect();
    let process_env = sanitize_process_env(process_env, "sandbox_import");
    let execution = import_header_execution_metadata(header)?;

    let auto_lifecycle = header
        .auto_lifecycle
        .as_ref()
        .map(policy_from_proto)
        .transpose()?
        .unwrap_or_else(AutoLifecyclePolicy::disabled);
    let network_policy_authority =
        crate::db::PolicyAuthority::from_optional_proto(header.network_policy_authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .unwrap_or(crate::db::PolicyAuthority::Cluster);
    let auto_lifecycle_authority =
        crate::db::PolicyAuthority::from_optional_proto(header.auto_lifecycle_authority)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .unwrap_or(crate::db::PolicyAuthority::Cluster);
    Ok(ValidatedSandboxImport {
        sandbox_id,
        auto_lifecycle,
        network_policy_authority,
        auto_lifecycle_authority,
        process_env,
        execution,
    })
}

/// How long a transfer waits for one delta-base lookup, dial included. Exports
/// and a sandbox import's check before its body look the base up before
/// taking the sandbox's wake lock, artifact lock and cleanup fence. A delta
/// import's apply makes two more while it holds them, one just before it
/// applies the diff and one after ([`apply_delta_to_verified_base`]); a
/// snapshot import makes only those, after its body, since it can't tell a
/// delta import from a full one before then.
pub(super) const DELTA_BASE_LOOKUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Why a transfer has no delta base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NoDeltaBase {
    /// This node holds no ready build of the digest: template-manager
    /// answered `NOT_FOUND`, or the transfer names no template build. Pulling
    /// the template can supply one.
    Absent,
    /// A ready build is there but can't serve as a base, or template-manager
    /// refused the lookup itself: its rootfs fails the size check or isn't a
    /// regular file, the hash is invalid, or the answer came for another
    /// lookup scope. Retrying or pulling won't help.
    Unusable(String),
    /// Template-manager didn't answer within [`DELTA_BASE_LOOKUP_TIMEOUT`],
    /// couldn't be reached, or failed transiently, so whether this node holds
    /// a usable build is unknown.
    Unanswered(String),
}

/// Lookup statuses that leave the answer unknown, as opposed to ones that
/// say this lookup can't succeed.
fn unanswered(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted
            | tonic::Code::Cancelled
            | tonic::Code::Internal
            | tonic::Code::Unknown
    )
}

/// The rootfs of a ready build of `template@content_hash` on this node, the
/// only safe base for a rootfs delta. A build dir alone says nothing: one can
/// exist without a catalog row, as a transient recovery failure or an
/// interrupted pull leaves it. So the base comes from a `cached_only` lookup,
/// which answers from a ready row whose rootfs passes template-manager's
/// checks and never pulls, bounded by [`DELTA_BASE_LOOKUP_TIMEOUT`].
pub(super) async fn verify_delta_base(
    templates: &dyn crate::template_resolver::TemplateResolver,
    template: &str,
    content_hash: &str,
) -> Result<std::path::PathBuf, NoDeltaBase> {
    if template.is_empty() || content_hash.is_empty() {
        return Err(NoDeltaBase::Absent);
    }
    let mut request =
        tonic::Request::new(pplx_space_template_mgr_client::proto::GetTemplateRequest {
            template: Some(pplx_space_template_mgr_client::proto::TemplateRef {
                name: template.to_string(),
                content_hash: Some(content_hash.to_string()),
                tag: None,
                environment: None,
            }),
            vcpu: None,
            memory_mib: None,
            caller_org_id: None,
            cached_only: true,
        });
    let scope = pplx_space_template_mgr_client::lookup_scope::TemplateLookupScope::LocalExecution;
    scope.attach(&mut request);
    let Ok(lookup) =
        tokio::time::timeout(DELTA_BASE_LOOKUP_TIMEOUT, templates.get_template(request)).await
    else {
        tracing::warn!(
            template,
            content_hash,
            "no verified delta base: template lookup timed out"
        );
        return Err(NoDeltaBase::Unanswered(format!(
            "no answer within {DELTA_BASE_LOOKUP_TIMEOUT:?}"
        )));
    };
    match lookup {
        Ok(response) => {
            if let Err(status) = scope.observe_response(&response) {
                tracing::warn!(
                    template,
                    content_hash,
                    %status,
                    "no verified delta base: template lookup answered for another scope"
                );
                return Err(NoDeltaBase::Unusable(status.message().to_string()));
            }
            let response = response.into_inner();
            // A template-manager that predates `cached_only` ignores it and
            // answers an ordinary lookup, which skips the checks a delta base
            // needs. Only its echo says they ran.
            if !response.cached_only {
                tracing::warn!(
                    template,
                    content_hash,
                    "no verified delta base: template-manager doesn't support cached_only lookups"
                );
                return Err(NoDeltaBase::Unusable(
                    "template-manager answered without cached_only checks; upgrade it".to_string(),
                ));
            }
            Ok(std::path::Path::new(&response.template_dir)
                .join(pplx_space_common_core::types::TEMPLATE_ROOTFS_FILE))
        }
        Err(crate::template_resolver::TemplateLookupError::Rpc(status))
            if status.code() == tonic::Code::NotFound =>
        {
            tracing::info!(
                template,
                content_hash,
                "no verified delta base: no ready build on this node"
            );
            Err(NoDeltaBase::Absent)
        }
        Err(crate::template_resolver::TemplateLookupError::Rpc(status))
            if !unanswered(status.code()) =>
        {
            tracing::warn!(
                template,
                content_hash,
                %status,
                "no verified delta base: template-manager refused the build or the lookup"
            );
            Err(NoDeltaBase::Unusable(status.message().to_string()))
        }
        Err(error) => {
            tracing::warn!(
                template,
                content_hash,
                %error,
                "no verified delta base: template lookup failed"
            );
            Err(NoDeltaBase::Unanswered(error.to_string()))
        }
    }
}

/// [`verify_delta_base`] for an export, which sends the full rootfs whatever
/// the reason there's no base.
pub(super) async fn verified_template_rootfs(
    templates: &dyn crate::template_resolver::TemplateResolver,
    template: &str,
    content_hash: &str,
) -> Option<std::path::PathBuf> {
    verify_delta_base(templates, template, content_hash)
        .await
        .ok()
}

/// A delta base looked up before a transfer takes its sandbox's locks, for
/// the build the sandbox pinned then.
pub(super) struct EarlyDeltaBase {
    template: String,
    content_hash: String,
    rootfs: std::path::PathBuf,
}

impl EarlyDeltaBase {
    /// The verified base of `template@content_hash`, when `wanted` and the
    /// sandbox pins a build.
    pub(super) async fn look_up(
        templates: &dyn crate::template_resolver::TemplateResolver,
        wanted: bool,
        template: &str,
        content_hash: Option<&str>,
    ) -> Option<Self> {
        let content_hash = content_hash.filter(|_| wanted)?;
        let rootfs = verified_template_rootfs(templates, template, content_hash).await?;
        Some(Self {
            template: template.to_string(),
            content_hash: content_hash.to_string(),
            rootfs,
        })
    }

    /// The base, if the sandbox, read again under its locks, still pins the
    /// build it was looked up for.
    pub(super) fn for_pinned(
        base: Option<Self>,
        template: &str,
        content_hash: &str,
    ) -> Option<std::path::PathBuf> {
        base.filter(|base| base.template == template && base.content_hash == content_hash)
            .map(|base| base.rootfs)
    }
}

async fn delta_import_base(
    templates: &dyn crate::template_resolver::TemplateResolver,
    header: &crate::proto::ImportSandboxHeader,
) -> Result<Option<std::path::PathBuf>, Status> {
    if !header.is_delta {
        return Ok(None);
    }
    required_delta_base(templates, &header.template_id, &header.content_hash)
        .await
        .map(Some)
}

/// The verified base a delta import applies its diff to. `FAILED_PRECONDITION`
/// when this node holds no ready build, or holds one that can't serve as a
/// base, each with its own message; a retryable `UNAVAILABLE` with the cause
/// when template-manager couldn't say.
pub(super) async fn required_delta_base(
    templates: &dyn crate::template_resolver::TemplateResolver,
    template: &str,
    content_hash: &str,
) -> Result<std::path::PathBuf, Status> {
    verify_delta_base(templates, template, content_hash)
        .await
        .map_err(|reason| match reason {
            NoDeltaBase::Absent => Status::failed_precondition(format!(
                "delta import requires template '{template}' with hash '{content_hash}' on this node. Pull the template first, or use full transfer."
            )),
            NoDeltaBase::Unusable(reason) => Status::failed_precondition(format!(
                "template '{template}' with hash '{content_hash}' can't serve as a delta base on this node: {reason}. Use full transfer."
            )),
            NoDeltaBase::Unanswered(cause) => Status::unavailable(format!(
                "template-manager didn't confirm a delta base for '{template}' with hash '{content_hash}': {cause}. Retry, or use full transfer."
            )),
        })
}

pub(super) mod receive;

async fn receive_sandbox_import_body(
    stream: &mut (impl tokio_stream::Stream<Item = Result<ImportSandboxRequest, Status>> + Unpin),
    paths: &SandboxImportPaths,
    is_delta: bool,
    deadline: receive::ImportDeadline,
) -> Result<bool, Status> {
    let mut receiver = receive::SparseImport::sandbox(is_delta);
    let mut snapshot_staging_created = false;
    while let Some(message) = receive::next_message(stream, deadline).await? {
        let Some(crate::proto::import_sandbox_request::Payload::Chunk(chunk)) = message.payload
        else {
            return Err(Status::data_loss("expected file chunk after import header"));
        };
        let (name, snapshot) = validate_import_sandbox_file_name(&chunk.file_name)?;
        if !snapshot
            && ((is_delta && name == "rootfs.ext4") || (!is_delta && name == "rootfs.ext4.diff"))
        {
            return Err(Status::data_loss(
                "rootfs encoding disagrees with import header",
            ));
        }
        let directory = if snapshot {
            &paths.snapshot_staging
        } else {
            &paths.instance_staging
        };
        if snapshot && !snapshot_staging_created {
            tokio::fs::create_dir_all(directory)
                .await
                .map_err(|error| Status::internal(format!("create snapshot staging: {error}")))?;
            snapshot_staging_created = true;
        }
        receiver
            .write(
                &chunk.file_name,
                &std::path::Path::new(directory).join(name),
                chunk.offset,
                &chunk.data,
                chunk.eof,
            )
            .await?;
    }
    receiver.finish()?;
    let rootfs = if is_delta {
        "rootfs.ext4.diff"
    } else {
        "rootfs.ext4"
    };
    if !receiver.nonempty(rootfs) {
        return Err(Status::data_loss(format!(
            "sandbox import missing {rootfs}"
        )));
    }
    let has_snapshot = receiver.has_snapshot();
    if has_snapshot
        && (!receiver.nonempty("snapshot/vmstate") || !receiver.nonempty("snapshot/memory"))
    {
        return Err(Status::data_loss(
            "sandbox import has an incomplete suspend snapshot",
        ));
    }
    Ok(has_snapshot)
}

/// Applies a delta import's diff to the verified base. The base is looked up
/// again here, not reused from the check before the body arrived. What keeps a
/// build from eviction while a transfer reads it is the LRU touch a
/// `cached_only` hit makes: template-manager's eviction planner skips builds
/// touched within its grace period (1800 s; overridable only in test-hook
/// builds). The body can take until the import deadline, an hour, so the first
/// touch can lapse; this lookup touches the build again just before the read.
/// The touch is best effort: a GC tick that planned the build's eviction
/// before it can still remove the build, which
/// [`apply_delta_to_verified_base`] catches.
async fn apply_sandbox_delta_import(
    service: &SandboxManagerService,
    header: &crate::proto::ImportSandboxHeader,
    paths: &SandboxImportPaths,
) -> Result<(), Status> {
    if !header.is_delta {
        return Ok(());
    }
    let diff_file = format!("{}/rootfs.ext4.diff", paths.instance_staging);
    if !std::path::Path::new(&diff_file).exists() {
        return Err(Status::data_loss(
            "delta import: rootfs.ext4.diff not found in stream",
        ));
    }
    apply_delta_to_verified_base(
        service,
        &header.template_id,
        &header.content_hash,
        std::path::PathBuf::from(&diff_file),
        std::path::PathBuf::from(format!("{}/rootfs.ext4", paths.instance_staging)),
        pplx_space_common_core::sandbox_storage::MAX_SANDBOX_ROOTFS_BYTES,
    )
    .await?;
    let _ = tokio::fs::remove_file(diff_file).await;
    Ok(())
}

/// Applies a delta import's `diff` over the verified base of `template` at
/// `content_hash`, writing `dest`, and keeps the result only if the base it
/// read was that verified build.
///
/// The apply reads the base through a [`PinnedDeltaBase`], and the build is
/// looked up again after; anything but a lookup that vouches for the pinned
/// file is `ABORTED`, retryable, and `dest` stays unpublished in its staging
/// directory.
pub(super) async fn apply_delta_to_verified_base(
    service: &SandboxManagerService,
    template: &str,
    content_hash: &str,
    diff: std::path::PathBuf,
    dest: std::path::PathBuf,
    max_output_bytes: u64,
) -> Result<(), Status> {
    let templates = service.template_resolver.as_ref();
    let base = required_delta_base(templates, template, content_hash).await?;
    let pin = service
        .blocking_artifact_io(move |_| {
            let pin = PinnedDeltaBase::open(base).map_err(|error| {
                Status::failed_precondition(format!("apply rootfs delta: {error}"))
            })?;
            pplx_space_node_runtime_core::file_delta::apply_diff_with_limit(
                pin.path(),
                &diff,
                &dest,
                max_output_bytes,
            )
            .map_err(|error| Status::failed_precondition(format!("apply rootfs delta: {error}")))?;
            Ok(pin)
        })
        .await?;
    let after = required_delta_base(templates, template, content_hash).await?;
    let vouched = tokio::task::spawn_blocking(move || pin.vouched_by(&after))
        .await
        .map_err(|error| Status::internal(format!("spawn_blocking: {error}")))?;
    if !vouched {
        tracing::warn!(
            template,
            content_hash,
            "delta base changed while the diff was applied"
        );
        return Err(Status::aborted(format!(
            "the delta base for template '{template}' with hash '{content_hash}' changed while the diff was applied. Retry."
        )));
    }
    Ok(())
}

/// A delta base's `rootfs.ext4` held open while a transfer reads it by path,
/// so a lookup made after the read can tell whether the read saw the build
/// the catalog vouches for.
///
/// The build can be evicted and pulled again between a lookup and the read.
/// A pull renames `rootfs.ext4` into place before it checks the digest, and
/// writes the build's row only after the check passes; a failed check
/// deletes the file. So the file at the path can briefly be unverified, but
/// never while a ready row vouches for it. The open descriptor keeps the
/// pinned inode allocated, so no later file can reuse its device and inode
/// numbers, and nothing links an unlinked file back into place. A path that
/// still names the pinned inode after the read named it throughout, and a
/// ready row then means those were verified bytes.
pub(super) struct PinnedDeltaBase {
    path: std::path::PathBuf,
    _file: std::fs::File,
    identity: (u64, u64),
}

impl PinnedDeltaBase {
    /// Opens `path`, refusing one that doesn't name the file it opened, such
    /// as a symlink.
    pub(super) fn open(path: std::path::PathBuf) -> std::io::Result<Self> {
        let file = std::fs::File::open(&path)?;
        let identity = identity_of(&file.metadata()?);
        if identity_of(&std::fs::symlink_metadata(&path)?) != identity {
            return Err(std::io::Error::other(format!(
                "{} changed while it was opened",
                path.display()
            )));
        }
        Ok(Self {
            path,
            _file: file,
            identity,
        })
    }

    pub(super) fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Whether `answer`, the base a lookup made after the read answered
    /// with, is this pinned file at the same path.
    pub(super) fn vouched_by(&self, answer: &std::path::Path) -> bool {
        answer == self.path
            && std::fs::symlink_metadata(answer)
                .is_ok_and(|metadata| identity_of(&metadata) == self.identity)
    }
}

fn identity_of(metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}

/// Computes a delta export's diff of `current` against the verified base
/// `base`, written to `diff`, and keeps it only if a lookup after the
/// computation vouches for the base file it read ([`PinnedDeltaBase`]).
/// Returns whether to send the diff; otherwise `diff` is removed and the
/// export sends the full rootfs. Runs on a blocking thread, and makes its
/// lookup on `runtime`.
pub(super) fn diff_against_verified_base(
    runtime: &tokio::runtime::Handle,
    templates: &dyn crate::template_resolver::TemplateResolver,
    (template, content_hash): (&str, &str),
    base: std::path::PathBuf,
    current: &std::path::Path,
    diff: &std::path::Path,
) -> bool {
    let kept = PinnedDeltaBase::open(base).is_ok_and(|pin| {
        pplx_space_node_runtime_core::file_delta::compute_diff(pin.path(), current, diff).is_ok()
            && runtime
                .block_on(verified_template_rootfs(templates, template, content_hash))
                .is_some_and(|after| pin.vouched_by(&after))
    });
    if !kept {
        tracing::warn!(
            template,
            content_hash,
            "delta export sends the full rootfs: no diff against a base the catalog still vouches for"
        );
        let _ = std::fs::remove_file(diff);
    }
    kept
}

async fn publish_imported_instance(
    service: &SandboxManagerService,
    paths: &SandboxImportPaths,
) -> Result<(), Status> {
    let source = paths.instance_staging.clone();
    let destination = paths.instance_dir.clone();
    service
        .blocking_artifact_io(move |_| {
            // A nonempty delta stream can reconstruct an empty disk. Validate
            // the final file before giving either encoding a canonical path.
            pplx_space_node_runtime_core::snapshot_files::validate_snapshot_rootfs(
                std::path::Path::new(&source),
            )
            .map_err(|error| Status::data_loss(format!("invalid imported rootfs: {error}")))?;
            crate::db::transfer::rename_unoccupied(
                std::path::Path::new(&source),
                std::path::Path::new(&destination),
            )
            .map_err(|error| Status::internal(format!("publish imported instance: {error}")))
        })
        .await
}

async fn publish_imported_snapshot(
    paths: &SandboxImportPaths,
    has_suspend_snapshot: bool,
) -> Result<(), Status> {
    let snapshots_dir = format!("{}/snapshots", paths.sandbox_base);
    let _ = tokio::fs::create_dir_all(&snapshots_dir).await;

    if has_suspend_snapshot {
        if tokio::fs::symlink_metadata(&paths.snapshot_dir)
            .await
            .is_ok()
        {
            return Err(Status::already_exists(
                "import snapshot target already exists",
            ));
        }
        tokio::fs::rename(&paths.snapshot_staging, &paths.snapshot_dir)
            .await
            .map_err(|e| Status::internal(format!("rename snapshot staging: {e}")))?;
    }

    Ok(())
}

async fn write_imported_sandbox_metadata(
    sandbox_id: &str,
    header: &crate::proto::ImportSandboxHeader,
    execution: pplx_space_common_core::types::ExecutionMetadata,
) -> Result<pplx_space_common_core::types::SandboxArtifactMetadata, Status> {
    let parsed_sandbox_id = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let template = pplx_space_common_core::types::TemplateName::parse(&header.template_id)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let content_hash = header
        .content_hash
        .parse::<pplx_space_common_core::types::ContentHash>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let sandbox_metadata = pplx_space_common_core::types::SandboxArtifactMetadata::new(
        parsed_sandbox_id,
        execution,
        template,
        Some(content_hash),
    );
    let sandbox_metadata_bytes = serde_json::to_vec_pretty(&sandbox_metadata)
        .map_err(|error| Status::internal(format!("serialize sandbox metadata: {error}")))?;
    let sandbox_metadata_path =
        pplx_space_common_core::host::linux::paths::sandbox_metadata_path(sandbox_id);
    let sandbox_metadata_staging = format!("{sandbox_metadata_path}.importing");
    tokio::fs::write(&sandbox_metadata_staging, sandbox_metadata_bytes)
        .await
        .map_err(|error| Status::internal(format!("write sandbox metadata: {error}")))?;
    tokio::fs::rename(&sandbox_metadata_staging, &sandbox_metadata_path)
        .await
        .map_err(|error| Status::internal(format!("publish sandbox metadata: {error}")))?;

    Ok(sandbox_metadata)
}

impl SandboxManagerService {
    pub(super) async fn export_sandbox(
        &self,
        request: Request<ExportSandboxRequest>,
    ) -> Result<Response<SbxExportSandboxStream>, Status> {
        self.authorize_peer(&request, "ExportSandbox", ALLOW_LOCAL_ARTIFACT_OPERATOR)?;
        let req = request.into_inner();
        let pinned = self.lookup_sandbox(&req.sandbox_id)?;
        let early_base = EarlyDeltaBase::look_up(
            self.template_resolver.as_ref(),
            req.use_delta,
            &pinned.template,
            pinned.template_hash.as_deref(),
        )
        .await;
        let id = pinned.id;
        let wake = self.auto_lifecycle.wake_lock(&id).lock_owned().await;
        let artifact = self.auto_lifecycle.artifact_lock(&id).lock_owned().await;
        let CleanupLockTryResult::Acquired(fence) = self.try_cleanup_fence(&id) else {
            return Err(Status::aborted("sandbox cleanup is in progress"));
        };
        // A fenced upgrade may be booting the instance or restoring it.
        self.refuse_runtime_upgrade_fence(&id)?;
        let sandbox = self.lookup_sandbox(&id)?;

        if !sandbox.guest_image.legacy_transport() {
            return Err(Status::failed_precondition(
                "firmware sandboxes cannot be exported or distributed",
            ));
        }
        // Reject export of a running sandbox — rootfs is being mutated
        if !matches!(
            sandbox.state,
            SandboxState::Stopped
                | SandboxState::Suspended
                | SandboxState::Created
                | SandboxState::Paused
        ) {
            return Err(Status::failed_precondition(
                "sandbox export requires a stopped, suspended, or created sandbox",
            ));
        }

        // Fail closed for encrypted sandboxes: the export stream carries the
        // plaintext instance rootfs, and an encrypted sandbox's rootfs is a
        // dm-crypt origin+COW pair. Streaming it here would either break the
        // import or silently downgrade the sandbox to plaintext.
        if sandbox.volume_encryption {
            return Err(Status::failed_precondition(
                "cannot export a sandbox with encrypted volumes",
            ));
        }

        let instance_dir =
            pplx_space_common_core::host::linux::paths::sandbox_instance_dir(&sandbox.id);
        if !std::path::Path::new(&instance_dir).is_dir() {
            return Err(Status::not_found("sandbox instance directory not found"));
        }

        let sandbox_id = sandbox.id.clone();
        let template = sandbox.template.clone();
        let template_hash = sandbox.template_hash.clone().ok_or_else(|| {
            Status::failed_precondition("sandbox has no pinned template content hash")
        })?;
        let runtime = sandbox.runtime.clone();
        let execution = sandbox_execution_metadata(&sandbox.id)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let owner = sandbox.user.to_string();
        let snapshots_enabled = sandbox.snapshots_enabled;
        let created_at = sandbox.created_at.clone();
        let guest_uptime_secs = sandbox.guest_uptime_secs;
        let process_env = sandbox.process_env.clone();
        let rootfs_size_mb = sandbox.rootfs_size_mb;
        let delta_base = EarlyDeltaBase::for_pinned(early_base, &template, &template_hash);
        let include_snapshot = sandbox.state == SandboxState::Suspended;
        let writer = self.state_writer_lease("ExportSandbox")?;
        let lease = sandbox
            .current_local_artifact_id
            .map(|id| {
                self.local_artifacts
                    .check_out_for_restore(id)
                    .map_err(|error| {
                        Status::failed_precondition(format!("sandbox artifact is in use: {error}"))
                    })
            })
            .transpose()?;
        let network_policy = sandbox
            .network_policy_override
            .as_ref()
            .map(network_policy_to_proto);

        let (tx, stream) = super::grpc_sandbox_transfer::receive::export_channel();

        let service = self.clone();
        let errors = tx.clone();
        let resolver = Arc::clone(&self.template_resolver);
        let lookup_runtime = tokio::runtime::Handle::current();
        self.task_tracker
            .spawn(super::drain::with_state_writer(writer, async move {
                let result = service
                    .blocking_export_io(move |_| {
                        let _ownership = (wake, artifact, fence, lease);
                        (|| {
                            // Check if we can compute a delta against the template rootfs
                            let sandbox_rootfs = format!("{instance_dir}/rootfs.ext4");
                            let mut required = vec![sandbox_rootfs.clone()];
                            if include_snapshot {
                                let directory =
                                    pplx_space_common_core::host::linux::paths::sandbox_cache_dir(
                                        &sandbox_id,
                                    );
                                required.extend([
                                    format!("{directory}/snapshot/vmstate"),
                                    format!("{directory}/snapshot/memory"),
                                ]);
                            }
                            for path in required {
                                match std::fs::metadata(&path) {
                                    Ok(metadata) if metadata.is_file() && metadata.len() > 0 => {}
                                    _ => {
                                        let _ = tx.blocking_send(Err(Status::data_loss(format!(
                                            "incomplete sandbox artifact: {path}"
                                        ))));
                                        return;
                                    }
                                }
                            }
                            // If delta is possible, compute it to a temp file
                            let (diff_path, is_delta) = if let Some(template_rootfs) = delta_base {
                                let diff_id = uuid::Uuid::now_v7();
                                let diff = format!("{instance_dir}/../.rootfs.diff.{diff_id}.tmp");
                                if diff_against_verified_base(
                                    &lookup_runtime,
                                    resolver.as_ref(),
                                    (&template, &template_hash),
                                    template_rootfs,
                                    std::path::Path::new(&sandbox_rootfs),
                                    std::path::Path::new(&diff),
                                ) {
                                    (Some(diff), true)
                                } else {
                                    (None, false)
                                }
                            } else {
                                (None, false)
                            };

                            // RAII guard: clean up temp diff file on any exit path
                            struct DiffCleanup(Option<String>);
                            impl Drop for DiffCleanup {
                                fn drop(&mut self) {
                                    if let Some(ref p) = self.0 {
                                        let _ = std::fs::remove_file(p);
                                    }
                                }
                            }
                            let _diff_guard = DiffCleanup(diff_path.clone());

                            let snapshot_working_dir =
                                pplx_space_common_core::host::linux::paths::sandbox_cache_dir(
                                    &sandbox_id,
                                ) + "/snapshot";
                            let (files, total_size) = match receive::sandbox_export_files(
                                std::path::Path::new(&instance_dir),
                                include_snapshot
                                    .then(|| std::path::Path::new(&snapshot_working_dir)),
                                diff_path.as_deref().map(std::path::Path::new),
                            ) {
                                Ok(manifest) => manifest,
                                Err(error) => {
                                    let _ = tx.blocking_send(Err(error));
                                    return;
                                }
                            };

                            // Send header
                            let header = ExportSandboxChunk {
                                payload: Some(crate::proto::export_sandbox_chunk::Payload::Header(
                                    crate::proto::ExportSandboxHeader {
                                        sandbox_id: sandbox_id.clone(),
                                        template_id: template,
                                        content_hash: template_hash,
                                        total_size_bytes: total_size,
                                        runtime,
                                        owner,
                                        snapshots_enabled,
                                        created_at,
                                        guest_uptime_secs,
                                        is_delta,
                                        env: process_env.clone().into_iter().collect(),
                                        auto_lifecycle: Some(policy_to_proto(
                                            &sandbox.auto_lifecycle,
                                        )),
                                        rootfs_size_mb,
                                        network_policy,
                                        network_policy_generation: sandbox
                                            .network_policy_generation,
                                        network_policy_authority: sandbox
                                            .network_policy_authority
                                            .as_proto_i32(),
                                        auto_lifecycle_authority: sandbox
                                            .auto_lifecycle_authority
                                            .as_proto_i32(),
                                        execution: Some((&execution).into()),
                                        volume_encryption: sandbox.volume_encryption,
                                        disk_io_max_operations_per_sec: Some(
                                            sandbox.disk_io_limits.max_operations_per_sec(),
                                        ),
                                        disk_io_max_bandwidth_mib_per_sec: Some(
                                            sandbox.disk_io_limits.max_bandwidth_mib_per_sec(),
                                        ),
                                    },
                                )),
                            };
                            if tx.blocking_send(Ok(header)).is_err() {
                                return;
                            }

                            for (file_name, path) in files {
                                if let Err(error) = receive::stream_sparse_file(
                                    &path,
                                    true,
                                    || tx.is_closed(),
                                    |offset, data, eof| {
                                        tx.blocking_send(Ok(ExportSandboxChunk {
                                            payload: Some(
                                                crate::proto::export_sandbox_chunk::Payload::Chunk(
                                                    crate::proto::ExportSandboxFileChunk {
                                                        file_name: file_name.clone(),
                                                        offset,
                                                        data,
                                                        eof,
                                                    },
                                                ),
                                            ),
                                        }))
                                        .map_err(|_| {
                                            Status::cancelled("export consumer disconnected")
                                        })
                                    },
                                ) {
                                    let _ = tx.blocking_send(Err(error));
                                    return;
                                }
                            }

                            // diff file cleanup handled by _diff_guard (RAII Drop)
                        })();
                        Ok(())
                    })
                    .await;
                errors.finish(result);
            }));

        Ok(Response::new(stream))
    }

    async fn reject_existing_sandbox_import(&self, sandbox_id: SandboxId) -> Result<(), Status> {
        let permit = Arc::clone(&self.cleanup_io)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("sandbox storage is shutting down"))?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let db = db
                .lock()
                .map_err(|_| Status::internal("sandbox db mutex poisoned"))?;
            if crate::db::read::sandbox_view(&db, &sandbox_id.to_string())
                .exists()
                .map_err(|error| Status::internal(format!("inspect import identity: {error}")))?
            {
                return Err(Status::already_exists(
                    "sandbox identity already exists on this node",
                ));
            }
            Ok(())
        })
        .await
        .map_err(|error| Status::internal(format!("import identity lookup failed: {error}")))?
    }

    fn reserve_imported_sandbox_row(
        &self,
        header: &crate::proto::ImportSandboxHeader,
        sandbox_owner: &SandboxOwner,
        validated: &ValidatedSandboxImport,
        cleanup_id: SandboxCleanupEpisodeId,
        paths: &SandboxImportPaths,
    ) -> Result<(), Status> {
        let sandbox_id = &header.sandbox_id;
        let parsed_sandbox_id = sandbox_id
            .parse::<SandboxId>()
            .map_err(|e| Status::invalid_argument(format!("invalid sandbox id: {e}")))?;
        let db = self
            .db
            .lock()
            .map_err(|_| Status::internal("sandbox db mutex poisoned"))?;
        if crate::db::read::sandbox_view(&db, sandbox_id)
            .exists()
            .map_err(|error| Status::internal(format!("inspect import identity: {error}")))?
        {
            return Err(Status::already_exists(
                "sandbox identity became occupied during import",
            ));
        }

        match std::fs::symlink_metadata(&paths.sandbox_base) {
            Ok(_) => return Err(Status::already_exists("sandbox cache already exists")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Status::internal(format!("inspect sandbox cache: {error}"))),
        }
        let uuid8 = SandboxRuntimeLabel::from_sandbox_id(parsed_sandbox_id);
        let network_policy_override = super::network_policy::optional_network_policy_from_proto(
            header.network_policy.as_ref(),
        )?;
        let auto_lifecycle_authority = validated.auto_lifecycle_authority;

        crate::db::transfer::reserve_import(
            &db,
            crate::db::transfer::ImportReservation {
                params: crate::db::InsertSandboxParams {
                    guest_image: Default::default(),
                    id: sandbox_id,
                    uuid8: &uuid8,
                    state: SandboxState::Creating,
                    template: &header.template_id,
                    template_hash: Some(&header.content_hash),
                    user: sandbox_owner,
                    runtime: &header.runtime,
                    snapshots_enabled: header.snapshots_enabled,
                    rootfs_size_mb: header.rootfs_size_mb,
                    disk_io_limits: pplx_space_common_core::types::DiskIoLimits::try_from_optional(
                        header.disk_io_max_operations_per_sec,
                        header.disk_io_max_bandwidth_mib_per_sec,
                    )
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
                    pool_template: None,
                    volume_encryption: header.volume_encryption,
                    process_env: &validated.process_env,
                    auto_lifecycle: &validated.auto_lifecycle,
                    network_policy_override: network_policy_override.as_ref(),
                    network_policy_generation: header.network_policy_generation,
                    network_policy_authority: validated.network_policy_authority,
                    auto_lifecycle_authority,
                },
                episode: crate::db::CleanupEpisodeSpec {
                    id: cleanup_id,
                    completion: crate::db::CleanupCompletion::DeleteRow,
                    keep_filesystem: false,
                    entered_from_state: None,
                    activation_state: SandboxState::Creating,
                    execution_generation: None,
                    activation_attempt_id: None,
                },
                execution: &validated.execution,
                created_at: &header.created_at,
                uptime: header.guest_uptime_secs,
                cache_root: std::path::Path::new(&paths.sandbox_base),
            },
        )
        .map_err(|error| match error {
            crate::db::transfer::ReserveImportError::Claim(ref cause)
                if cause.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                Status::already_exists("sandbox cache already exists")
            }
            _ => Status::internal(format!("reserve import: {error}")),
        })?;
        Ok(())
    }

    pub(super) async fn import_sandbox(
        &self,
        request: Request<tonic::Streaming<ImportSandboxRequest>>,
    ) -> Result<Response<ImportSandboxResponse>, Status> {
        let peer = self.authorize_peer(&request, "ImportSandbox", ALLOW_LOCAL_ARTIFACT_OPERATOR)?;
        let service = self.clone();
        let owned = super::restart_admission::inherit_admission_cancellation(async move {
            service
                .import_sandbox_owned(request.into_inner(), peer)
                .await
        });
        self.spawn_state_writer("ImportSandbox", owned)?
            .await
            .map_err(|error| Status::internal(format!("import task failed: {error}")))?
    }

    async fn import_sandbox_owned(
        &self,
        mut stream: impl tokio_stream::Stream<Item = Result<ImportSandboxRequest, Status>> + Unpin,
        peer: AuthorizedUdsPeer,
    ) -> Result<Response<ImportSandboxResponse>, Status> {
        let deadline = receive::ImportDeadline::new();
        let mut header = read_import_sandbox_header(&mut stream, deadline).await?;
        let validated = validate_import_sandbox_header(&header)?;
        let owner = Self::authorize_sandbox_owner_label(&peer, &header.owner)?;
        super::network_runtime::require_local_authority_peer(
            peer.principal(),
            validated.network_policy_authority,
        )?;
        super::network_runtime::require_local_authority_peer(
            peer.principal(),
            validated.auto_lifecycle_authority,
        )?;
        header.sandbox_id = validated.sandbox_id.to_string();
        let sandbox_id = header.sandbox_id.clone();
        // A delta import without a verified base is refused before its body
        // arrives, and before taking the sandbox's locks. Ahead of that
        // lookup, a cleanup in progress still answers ABORTED and an existing
        // sandbox ALREADY_EXISTS, as they would without a delta, so a retry of
        // an import that already landed isn't told to pull its template. Both
        // checks run again under the locks below, which stay authoritative.
        if header.is_delta {
            if !matches!(
                self.try_cleanup_fence(&sandbox_id),
                CleanupLockTryResult::Acquired(_)
            ) {
                return Err(Status::aborted("sandbox cleanup is in progress"));
            }
            self.reject_existing_sandbox_import(validated.sandbox_id)
                .await?;
            delta_import_base(self.template_resolver.as_ref(), &header).await?;
        }
        let _wake = super::restart_admission::wait_for_activation_admission(
            &sandbox_id,
            self.auto_lifecycle.wake_lock(&sandbox_id).lock_owned(),
        )
        .await?;
        let _artifact = super::restart_admission::wait_for_activation_admission(
            &sandbox_id,
            self.auto_lifecycle.artifact_lock(&sandbox_id).lock_owned(),
        )
        .await?;
        let CleanupLockTryResult::Acquired(_fence) = self.try_cleanup_fence(&sandbox_id) else {
            return Err(Status::aborted("sandbox cleanup is in progress"));
        };
        self.reject_existing_sandbox_import(validated.sandbox_id)
            .await?;

        let paths = SandboxImportPaths::new(&sandbox_id);
        let cleanup_id = SandboxCleanupEpisodeId::new();
        let reservation_header = header.clone();
        let reservation_paths = SandboxImportPaths::new(&sandbox_id);
        let reservation = validated.clone();
        self.blocking_io(move |service| {
            service.reserve_imported_sandbox_row(
                &reservation_header,
                &owner,
                &reservation,
                cleanup_id,
                &reservation_paths,
            )
        })
        .await?;
        let cleanup = self.activation_cleanup_guard(
            &sandbox_id,
            &SandboxRuntimeLabel::from_sandbox_id(validated.sandbox_id),
            ActivationCleanupSpec {
                cleanup_id,
                completion: crate::db::CleanupCompletion::DeleteRow,
                context: "cleanup interrupted sandbox import",
                interrupted_reason_code: SandboxQuarantineReasonCode::CreateInterrupted,
                interrupted_reason: "sandbox import did not complete",
            },
        )?;
        let instance_staging = paths.instance_staging.clone();
        self.blocking_artifact_io(move |_| {
            receive::create_transfer_directory(std::path::Path::new(&instance_staging))
        })
        .await?;
        let has_suspend_snapshot =
            receive_sandbox_import_body(&mut stream, &paths, header.is_delta, deadline).await?;
        apply_sandbox_delta_import(self, &header, &paths).await?;
        publish_imported_instance(self, &paths).await?;
        publish_imported_snapshot(&paths, has_suspend_snapshot).await?;
        write_imported_sandbox_metadata(&sandbox_id, &header, validated.execution).await?;
        let durable_root = paths.sandbox_base.clone();
        self.blocking_artifact_io(move |_| {
            let root = std::path::Path::new(&durable_root);
            for directory in [
                root.join("instance"),
                root.join("snapshot"),
                root.to_owned(),
            ] {
                if directory.is_dir() {
                    crate::db::transfer::sync_import_directory(&directory).map_err(|error| {
                        Status::internal(format!("sync imported sandbox: {error}"))
                    })?;
                }
            }
            Ok(())
        })
        .await?;
        let completion_id = sandbox_id.clone();
        if !self
            .storage(move |db| {
                crate::db::transfer::complete_import(
                    db,
                    &completion_id,
                    cleanup_id,
                    has_suspend_snapshot,
                )
                .map_err(|error| Status::internal(format!("complete import: {error}")))
            })
            .await?
        {
            return Err(Status::aborted("sandbox import was superseded"));
        }
        cleanup.complete_success();
        let sandbox = self.lookup_sandbox(&sandbox_id)?;
        Ok(Response::new(ImportSandboxResponse {
            sandbox_id,
            auto_lifecycle: Some(policy_to_proto(&sandbox.auto_lifecycle)),
            auto_lifecycle_status: Some(status_to_proto(
                &self.auto_lifecycle_runtime_state(&sandbox).public_status(),
            )),
        }))
    }
}

#[cfg(test)]
mod delta_e2e_tests;
#[cfg(test)]
mod tests;
