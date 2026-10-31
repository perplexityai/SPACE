use pplx_space_common_core::types::{JobId, OrgId};

pub mod blob;
mod blob_storage;
mod egress;
mod environment;
mod execution_binding;
pub mod object_store;
pub mod oci;
pub mod publish;
pub mod recipe;
mod runtime;
mod ssh;
mod storage;
mod vm;

use std::fmt;
use std::fs;
use std::future::Future;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::time::{Duration, Instant};

use pplx_space_common_core::{
    host::linux::identities::group_names,
    types::{
        BUILDER_SANDBOX_OWNER, ContentHash, DEFAULT_GUEST_USER, DEFAULT_TEMPLATE_TAG,
        PolicyAuthority, TemplateBuildId,
    },
};
use pplx_space_common_svc::page_cache;
pub use pplx_space_metrics::template::TemplateBuildErrorKind;
pub use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_net_common::proto::NetworkPolicyV1 as NetworkPolicyV1Proto;
use pplx_space_node_mgr_client::proto::{
    CompletePreparedRootfsRequest, PreparedRootfsDisposition, ReservePreparedRootfsRequest,
};
use pplx_space_sbx_mgr_client::SandboxManagerGrpcClient;
use pplx_space_sbx_mgr_client::proto::{CreateRequest, DeleteRequest, StartRequest, StopRequest};
use pplx_space_template_mgr_client::proto::{
    DeleteTemplateBuildRequest, ListTemplatesRequest, RegisterTemplateRequest, TemplateRef,
};
use tempfile::TempDir;
use tracing::Instrument as _;

pub use egress::{BuildVmEgressPolicyError, build_vm_egress_policy_from_env};
use environment::StepEnvironment;
pub use environment::{BuildEnvironmentError, PreparedRecipe};
use execution_binding::BuildExecutionBinding;
pub use execution_binding::BuildExecutionBindingError;
pub use pplx_space_common_core::types::{SystemPublicationGrantId, TenantPublicationGrantId};
pub use publish::{
    PublishError, TemplateOrg, TemplatePublication, publish_build, register_build_template,
};
pub use recipe::RecipeError;
pub use ssh::SshError;
pub use storage::StorageAdmissionError;

use recipe::{CopyPayload, GuestKernelPin, Origin, PackageManager, Recipe, Step};
use ssh::SshSession;
use vm::TEMPLATE_DIR;

const UBUNTU_APT_MIRROR_ENV: &str = "SPACE_UBUNTU_APT_MIRROR";
/// Foreground flush before terminating the build VM. Wait for it before
/// requesting guest exit so the last recipe step reaches the backing disk.
/// Sysrq sync starts writeback asynchronously; immediate reset can race it.
/// Subsequent e2fsck journal recovery can then discard the final recipe write,
/// even when the guest command itself returned success.
const GUEST_FLUSH_CMD: &str = "sync; sync";

const SSH_READY_TIMEOUT: Duration = Duration::from_secs(300);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const TEMPLATE_ARTIFACT_HEADROOM_MB: u32 = 512;
const DEFAULT_BUILD_VM_ROOTFS_SIZE_MB: u32 = 20 * 1024;
const SCRATCH_SOURCE_TYPE_UBUNTU_MINIMAL_CLOUD_SQUASHFS: &str = "ubuntu-minimal-cloud-squashfs";
const SCRATCH_GUEST_ARTIFACT_SUBDIR: &str = "data/spaced";
const SCRATCH_SCRIPT_STAGE_SUBDIR: &str = "builds/scratch-scripts";
const SCRATCH_OUTPUT_ARTIFACTS: &[&str] = &["rootfs.ext4", "metadata.json"];
const BUNDLED_BUILD_SEED_SCRIPT: &str = include_str!("../scripts/build-seed.sh");
const BUNDLED_ROOTFS_BUILDER_SCRIPT: &str =
    include_str!("../../../../../templates/firecracker/seed/rootfs-builder.sh");

use pplx_space_common_core::types::TemplateMetadata;

pub struct TemplateBuilder {
    caller_org: Option<OrgId>,
    environment: Option<pplx_space_common_core::types::EnvironmentReference>,
    network_policy: NetworkPolicyV1,
}

struct ScratchScriptBundle {
    _tempdir: TempDir,
    helper_path: PathBuf,
    rootfs_builder_path: PathBuf,
}

pub struct BuildOutcome {
    pub content_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildPhase {
    ValidateRecipe,
    ResolveBase,
    ConnectSandboxManager,
    CreateBuildVm,
    PrepareOciRootfs,
    StartBuildVm,
    WaitSsh,
    ConfigureGuest,
    RecipeStep,
    ShutdownGuest,
    StopSandbox,
    CopyRootfs,
    DeleteSandbox,
    CompactRootfs,
    WriteMetadata,
    FixPermissions,
    RegisterTemplate,
    Publish,
    ScratchBuild,
    NormalizeScratchOutput,
}

impl BuildPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            BuildPhase::ValidateRecipe => "validate_recipe",
            BuildPhase::ResolveBase => "resolve_base",
            BuildPhase::ConnectSandboxManager => "connect_sandbox_manager",
            BuildPhase::CreateBuildVm => "create_build_vm",
            BuildPhase::PrepareOciRootfs => "prepare_oci_rootfs",
            BuildPhase::StartBuildVm => "start_build_vm",
            BuildPhase::WaitSsh => "wait_ssh",
            BuildPhase::ConfigureGuest => "configure_guest",
            BuildPhase::RecipeStep => "recipe_step",
            BuildPhase::ShutdownGuest => "shutdown_guest",
            BuildPhase::StopSandbox => "stop_sandbox",
            BuildPhase::CopyRootfs => "copy_rootfs",
            BuildPhase::DeleteSandbox => "delete_sandbox",
            BuildPhase::CompactRootfs => "compact_rootfs",
            BuildPhase::WriteMetadata => "write_metadata",
            BuildPhase::FixPermissions => "fix_permissions",
            BuildPhase::RegisterTemplate => "register_template",
            BuildPhase::Publish => "publish",
            BuildPhase::ScratchBuild => "scratch_build",
            BuildPhase::NormalizeScratchOutput => "normalize_scratch_output",
        }
    }
}

impl fmt::Display for BuildPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

struct BaseTemplateBuild {
    build_id: TemplateBuildId,
    resolved_reference: String,
    rootfs_path: String,
    metadata: TemplateMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GuestRuntimeMetadata {
    spaced_version: String,
    spaced_compatibility_family: String,
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error(transparent)]
    ExecutionBinding(#[from] BuildExecutionBindingError),
    #[error("no guest exit strategy for build runtime {0}")]
    UnsupportedGuestExit(pplx_space_common_core::types::RuntimeKind),
    #[error("recipe builds require an ext4 image with a guest agent; import opaque disks instead")]
    UnsupportedGuestImage,
    #[error(transparent)]
    Environment(#[from] BuildEnvironmentError),
    #[error(transparent)]
    Recipe(#[from] RecipeError),
    #[error(transparent)]
    Ssh(#[from] SshError),
    #[error(transparent)]
    OciImageAssembly(#[from] oci::OciImageAssemblyError),
    #[error(transparent)]
    OciGuestSeed(#[from] oci::GuestSeedError),
    #[error("invalid spaced version {version}: {source}")]
    InvalidSpacedVersion {
        version: String,
        #[source]
        source: semver::Error,
    },
    #[error(transparent)]
    InvalidSandboxId(#[from] pplx_space_common_core::ids::InvalidSandboxId),
    #[error(transparent)]
    TemplateRef(#[from] pplx_space_template_mgr_client::TemplateRefFromReferenceError),
    #[error("{0}")]
    Message(String),
    #[error("cannot reserve prepared rootfs capacity: {0}")]
    RootfsReservation(#[source] Box<tonic::Status>),
    #[error("{operation} failed to start: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation} exited unsuccessfully")]
    CommandFailed { operation: &'static str },
    #[error("step {step_num} failed: {step}: {source}")]
    Step {
        step_num: usize,
        step: String,
        #[source]
        source: SshError,
    },
}

impl BuildError {
    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    /// The bounded cause metrics report for this failure, decided by the
    /// error's type alone: its message can carry recipe or image content.
    pub fn error_kind(&self) -> TemplateBuildErrorKind {
        match self {
            Self::Environment(_)
            | Self::Recipe(_)
            | Self::TemplateRef(_)
            | Self::OciGuestSeed(_)
            | Self::UnsupportedGuestImage => TemplateBuildErrorKind::Recipe,
            Self::Ssh(error) => error.error_kind(),
            Self::Step { source, .. } => source.recipe_step_error_kind(),
            Self::OciImageAssembly(error) => error.error_kind(),
            Self::RootfsReservation(status) if status.code() == tonic::Code::ResourceExhausted => {
                TemplateBuildErrorKind::StorageAdmission
            }
            Self::ExecutionBinding(_)
            | Self::UnsupportedGuestExit(_)
            | Self::RootfsReservation(_)
            | Self::InvalidSpacedVersion { .. }
            | Self::InvalidSandboxId(_)
            | Self::Message(_)
            | Self::Io { .. }
            | Self::CommandFailed { .. } => TemplateBuildErrorKind::Other,
        }
    }
}

/// A build failure whose bounded cause is known from its type, for the
/// `error_kind` of the failed step's metric.
trait BuildFailureKind {
    fn error_kind(&self) -> TemplateBuildErrorKind;
}

impl BuildFailureKind for BuildError {
    fn error_kind(&self) -> TemplateBuildErrorKind {
        BuildError::error_kind(self)
    }
}

async fn build_phase<T, E, F>(template_id: &str, phase: BuildPhase, future: F) -> Result<T, E>
where
    E: fmt::Display + BuildFailureKind,
    F: Future<Output = Result<T, E>>,
{
    let started_at = Instant::now();
    let span = tracing::info_span!(
        "template.build.phase",
        otel.kind = "internal",
        otel.name = "template.build.phase",
        "operation.name" = "template.build.phase",
        template_id = %template_id,
        phase = %phase,
    );

    async move {
        tracing::info!(
            template_id = %template_id,
            phase = %phase,
            "build phase started"
        );
        let result = future.await;
        let step_result =
            pplx_space_metrics::template::TemplateBuildStepResult::from_ok(result.is_ok());
        let result_tag = step_result.as_str();
        pplx_space_metrics::template::build_step_completed(
            template_id,
            phase.as_str(),
            step_result,
            result.as_ref().err().map(BuildFailureKind::error_kind),
            started_at,
        );
        let duration_ms = started_at.elapsed().as_millis() as u64;
        match &result {
            Ok(_) => tracing::info!(
                template_id = %template_id,
                phase = %phase,
                duration_ms,
                result = result_tag,
                "build phase completed"
            ),
            Err(error) => tracing::warn!(
                template_id = %template_id,
                phase = %phase,
                duration_ms,
                result = result_tag,
                error = %error,
                "build phase failed"
            ),
        }
        result
    }
    .instrument(span)
    .await
}

pub(crate) fn effective_uid() -> Option<u32> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|line| line.starts_with("Uid:"))
        .and_then(|line| line.split_whitespace().nth(2))
        .and_then(|value| value.parse::<u32>().ok())
}

pub(crate) fn command_as_root(program: &str) -> Command {
    if effective_uid() == Some(0) {
        Command::new(program)
    } else {
        let mut cmd = Command::new("sudo");
        cmd.arg(program);
        cmd
    }
}

/// Name of a build job's staging directory under the template's name dir.
/// Must stay in sync with template-manager's `RegisterTemplate` staging-dir
/// validation, which requires this prefix.
pub fn build_staging_dir_name(job_id: &JobId) -> String {
    format!("build.importing.{job_id}")
}

/// Best-effort removal of a build job's staging dir, e.g. after the owning
/// build was failed by the expired-lease reaper. Contents may be root-owned
/// (the rootfs is copied in via elevated `cp`), so removal is elevated too.
pub fn remove_build_staging_dir(template_id: &str, job_id: &JobId) {
    let dir = format!(
        "{TEMPLATE_DIR}/{template_id}/{}",
        build_staging_dir_name(job_id)
    );
    remove_staging_dir_as_root(&dir);
}

fn remove_staging_dir_as_root(dir: &str) {
    // Plain removal first: it covers the common cases (dir already consumed
    // by registration, or this process is root and owns everything) without
    // shelling out. Escalate only when contents are root-owned and we're not
    // root (devbox builds).
    match std::fs::remove_dir_all(dir) {
        Ok(()) => return,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(_) => {}
    }
    match command_as_root("rm").args(["-rf", "--", dir]).status() {
        Ok(status) if status.success() => {}
        Ok(status) => {
            tracing::warn!(
                dir = %dir,
                status = %status,
                "failed to remove build staging dir"
            );
        }
        Err(error) => {
            tracing::warn!(
                dir = %dir,
                error = %error,
                "failed to start build staging dir removal"
            );
        }
    }
}

fn run_root_command(command: &mut Command, operation: &'static str) -> Result<(), BuildError> {
    let status = command
        .status()
        .map_err(|source| BuildError::Io { operation, source })?;
    if status.success() {
        Ok(())
    } else {
        Err(BuildError::CommandFailed { operation })
    }
}

/// Non-recursive ownership/group-write grant on a template cache path. Used
/// on the template's name dir so template-manager (space-cache-rw group) can
/// create its register staging next to a build's staging dir, rename it into
/// the digest layout, and delete the consumed staging dir. The recursive
/// variant stays scoped to the staging dir itself — the name dir holds
/// sibling digest dirs and concurrent builds' staging dirs.
fn grant_template_cache_group_access(dir: &str) -> Result<(), BuildError> {
    let cache_owner = format!("root:{}", group_names::SPACE_CACHE_RW);
    run_root_command(
        command_as_root("chown").args([&cache_owner, dir]),
        "grant template cache group ownership",
    )?;
    run_root_command(
        command_as_root("chmod").args(["g+w", dir]),
        "grant template cache group write access",
    )
}

async fn stop_sandbox_best_effort(
    sbx: &mut SandboxManagerGrpcClient,
    sandbox_id: &str,
    reason: &'static str,
) {
    match sbx
        .stop(StopRequest {
            sandbox_id: sandbox_id.to_string(),
            template_filter: None,
            blocking: true,
            immediate_kill: false,
        })
        .await
    {
        Ok(_) => {}
        Err(error) => tracing::warn!(
            sandbox_id = %sandbox_id,
            reason,
            error = %error,
            "failed to stop sandbox during build cleanup"
        ),
    }
}

async fn delete_sandbox_best_effort(
    sbx: &mut SandboxManagerGrpcClient,
    sandbox_id: &str,
    reason: &'static str,
) {
    match sbx
        .delete(DeleteRequest {
            sandbox_id: sandbox_id.to_string(),
            template_filter: None,
            blocking: true,
        })
        .await
    {
        Ok(_) => {}
        Err(error) => tracing::warn!(
            sandbox_id = %sandbox_id,
            reason,
            error = %error,
            "failed to delete sandbox during build cleanup"
        ),
    }
}

async fn complete_prepared_rootfs_best_effort(
    node: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox_id: &str,
    disposition: PreparedRootfsDisposition,
    reason: &'static str,
) {
    if let Err(error) = node
        .complete_prepared_rootfs(CompletePreparedRootfsRequest {
            sandbox_id: sandbox_id.to_string(),
            disposition: disposition as i32,
        })
        .await
    {
        tracing::warn!(
            sandbox_id,
            reason,
            error = %error,
            "failed to complete prepared rootfs cleanup"
        );
    }
}

/// Removes the build job's staging dir on drop. The dir is build-manager's
/// to remove in every outcome: on failure it holds a partial multi-GB rootfs
/// copy, and on successful registration template-manager has copied the
/// content into the digest layout but deliberately left the source intact
/// (the RegisterTemplate retry contract). Held across the whole build so a
/// panic unwinding into the executor's `catch_unwind` reaps it too.
struct BuildStagingGuard {
    dir: String,
}

impl BuildStagingGuard {
    fn new(dir: impl Into<String>) -> Self {
        Self { dir: dir.into() }
    }
}

impl Drop for BuildStagingGuard {
    fn drop(&mut self) {
        remove_staging_dir_as_root(&self.dir);
    }
}

fn build_vm_create_request(
    base_template_name: String,
    base_content_hash: String,
    rootfs_size_mb: Option<u32>,
    environment: Option<pplx_space_common_core::types::EnvironmentReference>,
    network_policy: &NetworkPolicyV1,
) -> CreateRequest {
    let mut request = CreateRequest {
        environment: None,
        template: base_template_name,
        owner: BUILDER_SANDBOX_OWNER.to_string(),
        runtime: "fc".to_string(),
        snapshots_enabled: false,
        autostart: false,
        pool_template: None,
        volume_encryption: false,
        boot_mode: 1, // BOOT_MODE_COLD: build sandboxes mutate and resize disk state.
        blocking: false,
        sandbox_id: None,
        content_hash: Some(base_content_hash),
        env: Default::default(),
        auto_lifecycle: None,
        rootfs_size_mb,
        // Build VMs need internet egress to fetch packages, install
        // language toolchains, etc. The base template (e.g. seed) has a
        // deny_all default policy meant for end-user sandboxes; without
        // this override, apt-get and curl inside the build VM time out.
        // The policy is set at create, before any recipe step runs.
        network_policy: Some(NetworkPolicyV1Proto::from(network_policy)),
        network_policy_generation: 0,
        network_policy_authority: PolicyAuthority::Local.as_proto_i32(),
        auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
        template_network_policy: None,
        template_network_policy_generation: 0,
        template_network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
        route_identity: None,
        disk_io_max_operations_per_sec: None,
        disk_io_max_bandwidth_mib_per_sec: None,
    };
    if let Some(reference) = environment {
        pplx_space_sbx_mgr_client::environment::bind(&mut request, reference);
    }
    request
}

fn build_vm_rootfs_size_mb(runtime_disk_size_mb: Option<u32>) -> u32 {
    runtime_disk_size_mb.unwrap_or(DEFAULT_BUILD_VM_ROOTFS_SIZE_MB)
}

fn build_vm_start_request(sandbox_id: String, vcpu: u32, memory_mib: u32) -> StartRequest {
    StartRequest {
        activation_auto_lifecycle: None,
        operation_activation_attempt_id: None,
        restore_artifact_as_new: None,
        expected_local_artifact_id: None,
        sandbox_id,
        vcpu: Some(vcpu),
        memory_mib: Some(memory_mib),
        checkpoint: None,
        from_warm: false,
        blocking: true,
        netns_path: None,
        ready_timeout_secs: None,
        target_state: 0,
        suspend_restore_storage_uri: None,
        suspend_restore_source_node: None,
        route_identity: None,
    }
}

#[cfg(test)]
mod base_reference_tests;

impl Default for TemplateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateBuilder {
    pub fn new() -> Self {
        Self {
            caller_org: None,
            environment: None,
            network_policy: NetworkPolicyV1::allow_all(),
        }
    }

    /// Egress policy for the build VM; see [`build_vm_egress_policy_from_env`].
    pub fn with_network_policy(mut self, network_policy: NetworkPolicyV1) -> Self {
        self.network_policy = network_policy;
        self
    }

    pub fn with_environment(
        mut self,
        environment: Option<pplx_space_common_core::types::EnvironmentReference>,
    ) -> Self {
        self.environment = environment;
        self
    }

    pub fn with_caller_org(mut self, caller_org: Option<OrgId>) -> Self {
        self.caller_org = caller_org;
        self
    }

    pub async fn build(
        &self,
        recipe_label: &str,
        recipe: Recipe,
        template_id: &str,
        job_id: &JobId,
    ) -> Result<BuildOutcome, BuildError> {
        self.build_prepared(
            recipe_label,
            PreparedRecipe::new(recipe)?,
            template_id,
            job_id,
        )
        .await
    }

    pub async fn build_prepared(
        &self,
        recipe_label: &str,
        prepared: PreparedRecipe,
        template_id: &str,
        job_id: &JobId,
    ) -> Result<BuildOutcome, BuildError> {
        // OciRootfs owns cleanup on early return or cancellation. Successful
        // assembly consumes this same mount and explicitly awaits its unmount.
        let mut mounted_image = None;
        let recipe = prepared.recipe();
        let (base, base_raw, oci_image) = match &recipe.origin {
            Origin::FromScratch { .. } => {
                build_phase(template_id, BuildPhase::ValidateRecipe, async {
                    validate_scratch_recipe(recipe)?;
                    Ok::<(), BuildError>(())
                })
                .await?;
                return self
                    .build_from_scratch(recipe_label, template_id, job_id, recipe)
                    .await;
            }
            Origin::FromTemplate { base, base_raw } => (base.clone(), base_raw.clone(), None),
            Origin::FromOciImage {
                base,
                base_raw,
                reference,
            } => {
                let seeds = oci::GuestSeeds::from_env()?;
                let selected = if seeds.applies_to(base) {
                    let image = oci::mount(oci::OciMountRequest {
                        cache_dir: Path::new(
                            pplx_space_common_core::host::linux::paths::FIRECRACKER_CACHE_DIR,
                        )
                        .join("builds/oci"),
                        reference: reference.clone(),
                    })
                    .await
                    .map_err(oci::OciImageAssemblyError::from)?;
                    let selected = seeds.select(image.path(), base)?;
                    mounted_image = Some(image);
                    selected
                } else {
                    base.clone()
                };
                let selected_raw = if selected == *base {
                    base_raw.clone()
                } else {
                    selected.to_string()
                };
                (selected, selected_raw, Some(reference.clone()))
            }
        };
        let vcpu = recipe.build_config.vcpu.unwrap_or(4);
        let mem_mib = recipe.build_config.memory_mib.unwrap_or(4096);
        // The recorded memory is the template's default; sandboxes that ask
        // for less are checked again at cold boot.
        if let Some(balloon) = recipe.build_config.balloon {
            balloon
                .validate_for_memory_mib(mem_mib)
                .map_err(|error| BuildError::message(error.to_string()))?;
        }

        println!("=== Building template '{template_id}' from recipe '{recipe_label}' ===");
        println!("  Base: {base_raw}");
        if let Some(source) = oci_source_message(oci_image.as_ref()) {
            println!("{source}");
        }
        println!("  VM:   {vcpu} vCPU, {mem_mib} MiB RAM");
        println!("  Steps: {}", recipe.steps.len());

        // Resolve base template path via template-manager
        let resolved_base = build_phase(template_id, BuildPhase::ResolveBase, async {
            let mut tmpl_client = pplx_space_template_mgr_client::connect()
                .await
                .map_err(|e| {
                    BuildError::message(format!("cannot connect to template-manager: {e}"))
                })?;
            let resp = tmpl_client
                .get_template(pplx_space_template_mgr_client::proto::GetTemplateRequest {
                    caller_org_id: self.caller_org.map(|org| org.to_string()),
                    template: Some(TemplateRef::try_from(&base)?),
                    vcpu: None,
                    memory_mib: None,
                })
                .await
                .map_err(|e| {
                    BuildError::message(format!("GetTemplate failed for base '{base_raw}': {e}"))
                })?;
            let inner = resp.into_inner();
            let authorized_org = inner.authorized_org_id.as_deref()
                .map(str::parse::<OrgId>)
                .transpose()
                .map_err(|error| BuildError::message(format!(
                    "GetTemplate for base '{base_raw}' returned invalid authorized org {:?}; expected {:?}: {error}",
                    inner.authorized_org_id, self.caller_org,
                )))?;
            if authorized_org != self.caller_org {
                return Err(BuildError::message(format!(
                    "GetTemplate for base '{base_raw}' authorization mismatch: expected org {:?}, returned {:?}",
                    self.caller_org, authorized_org,
                )));
            }
            let template_dir = inner.template_dir;
            let resolved_template = inner.template.ok_or_else(|| {
                BuildError::message(format!(
                    "GetTemplate for base '{base_raw}' returned no template"
                ))
            })?;
            let base_name = pplx_space_common_core::types::TemplateName::parse(
                resolved_template.name,
            )
            .map_err(|error| {
                BuildError::message(format!(
                    "GetTemplate for base '{base_raw}' returned invalid template name: {error}"
                ))
            })?;
            let base_content_hash = resolved_template.content_hash.ok_or_else(|| {
                BuildError::message(format!(
                    "GetTemplate for base '{base_raw}' returned no content_hash"
                ))
            })?;
            let base_content_hash = ContentHash::from_str(&base_content_hash).map_err(|error| {
                BuildError::message(format!(
                    "GetTemplate for base '{base_raw}' returned invalid content_hash: {error}"
                ))
            })?;
            if base_name != *base.name()
                || base
                    .artifact_digest()
                    .is_some_and(|hash| hash.content_hash() != base_content_hash)
            {
                return Err(BuildError::message(format!(
                    "GetTemplate for base '{base_raw}' returned a different base release: expected name '{}' and digest {:?}, returned '{base_name}@{base_content_hash}'",
                    base.name(), base.artifact_digest(),
                )));
            }
            let build_id = TemplateBuildId::new(base_name, base_content_hash);
            let metadata = execution_binding::read_metadata(
                &Path::new(&template_dir).join("metadata.json"),
            )?;
            let resolved_base = BaseTemplateBuild {
                resolved_reference: format!("{}@{}", build_id.template(), build_id.content_hash()),
                rootfs_path: format!("{template_dir}/rootfs.ext4"),
                build_id,
                metadata,
            };
            Ok::<BaseTemplateBuild, BuildError>(resolved_base)
        })
        .await?;
        let base_metadata = &resolved_base.metadata;
        if !base_metadata
            .guest_image
            .unwrap_or_default()
            .legacy_transport()
        {
            return Err(BuildError::UnsupportedGuestImage);
        }
        let base_execution = base_metadata
            .execution_metadata()
            .map_err(BuildExecutionBindingError::from)?
            .ok_or(BuildExecutionBindingError::Missing(
                "base execution metadata",
            ))?;
        let execution_binding =
            BuildExecutionBinding::for_base(self.environment, base_execution).await?;
        let guest_exit_command = vm::guest_exit_command(execution_binding.as_ref().map_or(
            base_execution.runtime().kind,
            BuildExecutionBinding::runtime_kind,
        ))?;

        println!("  Resolved base: {}", resolved_base.resolved_reference);
        if !std::path::Path::new(&resolved_base.rootfs_path).exists() {
            return Err(BuildError::message(format!(
                "{} not found",
                resolved_base.rootfs_path
            )));
        }

        let oci_inputs = if let Some(reference) = oci_image {
            let binaries = scratch_guest_bin_dir()?;
            let metadata = current_guest_runtime_metadata(&binaries).await?;
            Some((reference, metadata, binaries))
        } else {
            None
        };

        // Each build job writes into its own staging dir under the template's
        // name dir; template-manager consumes the dir at registration.
        // Job-scoped staging is what lets same-template builds run
        // concurrently without clobbering each other's output.
        let staging_dir_name = build_staging_dir_name(job_id);
        let output_dir = format!("{TEMPLATE_DIR}/{template_id}/{staging_dir_name}");
        let staging_guard = BuildStagingGuard::new(&output_dir);

        // Connect to sbx-manager
        let mut sbx = build_phase(template_id, BuildPhase::ConnectSandboxManager, async {
            pplx_space_sbx_mgr_client::connect()
                .await
                .map_err(|e| BuildError::message(format!("cannot connect to sbx-manager: {e}")))
        })
        .await?;

        let build_rootfs_size_mib = build_vm_rootfs_size_mb(recipe.build_config.disk_size_mb);

        // Register the build sandbox before materializing its rootfs.
        let sandbox_id = build_phase(template_id, BuildPhase::CreateBuildVm, async {
            let request = build_vm_create_request(
                resolved_base.build_id.template().as_str().to_string(),
                resolved_base.build_id.content_hash().to_string(),
                Some(build_rootfs_size_mib),
                execution_binding
                    .as_ref()
                    .map(BuildExecutionBinding::create_environment),
                &self.network_policy,
            );
            Ok::<String, BuildError>(
                pplx_space_sbx_mgr_client::environment::create(&mut sbx, request)
                    .await
                    .map_err(|error| {
                        BuildError::message(format!("sandbox create failed: {error}"))
                    })?
                    .sandbox_id,
            )
        })
        .await?;

        let oci_guest_runtime = if let Some((reference, guest_runtime, guest_bin_dir)) = oci_inputs
        {
            let cache_dir =
                Path::new(pplx_space_common_core::host::linux::paths::FIRECRACKER_CACHE_DIR)
                    .join("builds/oci");
            let seed_rootfs = resolved_base.rootfs_path.clone();
            let bundle_version = guest_runtime.spaced_version.clone();
            let result = build_phase(template_id, BuildPhase::PrepareOciRootfs, async {
                let mut node = pplx_space_node_mgr_client::connect()
                    .await
                    .map_err(|error| {
                        BuildError::message(format!("cannot connect to node-manager: {error}"))
                    })?;
                let target_rootfs = node
                    .reserve_prepared_rootfs(ReservePreparedRootfsRequest {
                        sandbox_id: sandbox_id.clone(),
                        rootfs_size_mb: build_rootfs_size_mib,
                    })
                    .await
                    .map_err(|error| BuildError::RootfsReservation(Box::new(error)))?
                    .into_inner()
                    .rootfs_path;
                let assembly_request = oci::OciImageAssemblyRequest {
                    seed_rootfs: PathBuf::from(seed_rootfs),
                    target_rootfs: PathBuf::from(target_rootfs),
                    target_rootfs_size_mib: build_rootfs_size_mib,
                    reference,
                    cache_dir,
                    guest_bin_dir,
                    bundle_version,
                };
                let assembly_result = match mounted_image.take() {
                    Some(image) => oci::assemble_from_mounted_image(assembly_request, image).await,
                    None => oci::assemble_from_image(assembly_request).await,
                };
                if let Err(error) = assembly_result {
                    complete_prepared_rootfs_best_effort(
                        &mut node,
                        &sandbox_id,
                        PreparedRootfsDisposition::Abort,
                        "OCI rootfs assembly failed",
                    )
                    .await;
                    return Err(BuildError::from(error));
                }
                if let Err(error) = node
                    .complete_prepared_rootfs(CompletePreparedRootfsRequest {
                        sandbox_id: sandbox_id.clone(),
                        disposition: PreparedRootfsDisposition::Commit as i32,
                    })
                    .await
                {
                    complete_prepared_rootfs_best_effort(
                        &mut node,
                        &sandbox_id,
                        PreparedRootfsDisposition::Abort,
                        "OCI rootfs commit failed",
                    )
                    .await;
                    return Err(BuildError::message(format!(
                        "cannot commit prepared rootfs: {error}"
                    )));
                }
                Ok(())
            })
            .await;
            if let Err(error) = result {
                delete_sandbox_best_effort(&mut sbx, &sandbox_id, "OCI rootfs assembly failed")
                    .await;
                return Err(error);
            }
            Some(guest_runtime)
        } else {
            None
        };

        // Start sandbox (allocate UID/network, launch VM)
        let start_resp = build_phase(template_id, BuildPhase::StartBuildVm, async {
            match sbx
                .start(build_vm_start_request(
                    sandbox_id.clone(),
                    vcpu as u32,
                    mem_mib,
                ))
                .await
            {
                Ok(resp) => Ok(resp.into_inner()),
                Err(e) => {
                    delete_sandbox_best_effort(&mut sbx, &sandbox_id, "sandbox start failed").await;
                    Err(BuildError::message(format!("sandbox start failed: {e}")))
                }
            }
        })
        .await?;

        println!(
            "Booting build VM in namespace {} (sandbox {})...",
            start_resp.namespace,
            pplx_space_common_core::ids::sandbox_label(&sandbox_id)?,
        );

        // Wait for SSH
        let ssh = SshSession::new(vm::GUEST_IP.to_string(), start_resp.namespace)?;
        if let Err(e) = build_phase(template_id, BuildPhase::WaitSsh, async {
            ssh.wait_ready(SSH_READY_TIMEOUT).map_err(BuildError::from)
        })
        .await
        {
            stop_sandbox_best_effort(&mut sbx, &sandbox_id, "ssh readiness failed").await;
            delete_sandbox_best_effort(&mut sbx, &sandbox_id, "ssh readiness failed").await;
            return Err(e);
        }

        let runtime_build = match build_phase(template_id, BuildPhase::ConfigureGuest, async {
            let runtime = runtime::RuntimeBuild::prepare(&ssh, recipe).await?;
            configure_guest_apt_mirror(&ssh)?;
            Ok::<_, BuildError>(runtime)
        })
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                stop_sandbox_best_effort(&mut sbx, &sandbox_id, "guest configuration failed").await;
                delete_sandbox_best_effort(&mut sbx, &sandbox_id, "guest configuration failed")
                    .await;
                return Err(error);
            }
        };

        // Execute steps
        for (i, (named_step, step_env)) in
            recipe.steps.iter().zip(prepared.environments()).enumerate()
        {
            let step_num = i + 1;
            let step_count = recipe.steps.len();
            println!("\n=== Step {step_num}/{}: {} ===", step_count, named_step);

            let started_at = Instant::now();
            let step_span = tracing::info_span!(
                "template.build.step",
                template_id = %template_id,
                phase = %BuildPhase::RecipeStep,
                step_num,
                step_count,
                step = %named_step,
            );
            let step_result = if matches!(named_step.step, Step::SetRuntime(_) | Step::ClearRuntime)
            {
                runtime_build.apply_step(&ssh, &named_step.step).await
            } else {
                step_span.in_scope(|| {
                    tracing::info!("build recipe step started");
                    execute_step(named_step, &ssh, &step_env)
                })
            };
            let step_metric_result =
                pplx_space_metrics::template::TemplateBuildStepResult::from_ok(step_result.is_ok());
            let result_tag = step_metric_result.as_str();
            pplx_space_metrics::template::build_step_completed(
                template_id,
                BuildPhase::RecipeStep.as_str(),
                step_metric_result,
                step_result
                    .as_ref()
                    .err()
                    .map(SshError::recipe_step_error_kind),
                started_at,
            );
            let duration_ms = started_at.elapsed().as_millis() as u64;
            step_span.in_scope(|| match &step_result {
                Ok(()) => tracing::info!(
                    duration_ms,
                    result = result_tag,
                    "build recipe step completed"
                ),
                Err(error) => tracing::warn!(
                    duration_ms,
                    result = result_tag,
                    error = %error,
                    "build recipe step failed"
                ),
            });

            if let Err(e) = step_result {
                stop_sandbox_best_effort(&mut sbx, &sandbox_id, "recipe step failed").await;
                delete_sandbox_best_effort(&mut sbx, &sandbox_id, "recipe step failed").await;
                return Err(BuildError::Step {
                    step_num,
                    step: named_step.to_string(),
                    source: e,
                });
            }
        }

        if let Err(error) = runtime_build.finalize(&ssh).await {
            stop_sandbox_best_effort(&mut sbx, &sandbox_id, "runtime finalization failed").await;
            delete_sandbox_best_effort(&mut sbx, &sandbox_id, "runtime finalization failed").await;
            return Err(error);
        }
        if runtime_build.requires_durable_stop()
            && let Err(error) = ssh.exchange(GUEST_FLUSH_CMD, &[], 0).await
        {
            stop_sandbox_best_effort(&mut sbx, &sandbox_id, "runtime flush failed").await;
            delete_sandbox_best_effort(&mut sbx, &sandbox_id, "runtime flush failed").await;
            return Err(error.into());
        }

        // Graceful shutdown
        println!("\n=== All steps completed, finalizing template ===");
        println!("Shutting down build VM...");
        build_phase(template_id, BuildPhase::ShutdownGuest, async {
            // Flush the guest filesystem to disk and WAIT for it before issuing
            // the backgrounded guest exit. This is the durability barrier that
            // guarantees the build's final recipe step survives the snapshot —
            // see GUEST_FLUSH_CMD. Best-effort: if the flush call fails we still
            // attempt the exit, but log it since a failed flush is exactly the
            // condition that can drop the last write.
            if let Err(e) = ssh.exec(GUEST_FLUSH_CMD, "root", &StepEnvironment::default()) {
                tracing::warn!(error = %e, "guest pre-shutdown sync failed; last write may not be durable");
                eprintln!("Guest pre-shutdown sync failed: {e}");
            }
            if let Err(e) = ssh.exec(guest_exit_command, "root", &StepEnvironment::default()) {
                tracing::warn!(error = %e, "guest exit request failed; force cleanup may be needed");
                eprintln!("Guest exit request failed, force cleaning: {e}");
            }
            if let Err(error) = vm::wait_for_exit(&sandbox_id, start_resp.pid, SHUTDOWN_TIMEOUT).await {
                tracing::warn!(%error, shutdown_timeout_secs = SHUTDOWN_TIMEOUT.as_secs(), "build VM graceful exit unconfirmed; requesting runtime cleanup");
            }
            Ok::<(), BuildError>(())
        })
        .await?;

        // Stop sandbox (kill VM, release network, keep filesystem for rootfs copy)
        if let Err(error) = build_phase(template_id, BuildPhase::StopSandbox, async {
            sbx.stop(StopRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
                // The successful flush above makes forced shutdown durable.
                immediate_kill: runtime_build.requires_durable_stop(),
            })
            .await
            .map_err(|e| BuildError::message(format!("sandbox stop failed: {e}")))?;
            Ok::<(), BuildError>(())
        })
        .await
        {
            delete_sandbox_best_effort(&mut sbx, &sandbox_id, "runtime stop failed").await;
            return Err(error);
        }

        // For runtime-bearing images, confirmed Stop proves the runtime cgroups
        // are gone; no container-local /proc check can strengthen that barrier.
        // Copy the modified rootfs out BEFORE delete removes it.
        let cache_rootfs =
            pplx_space_common_core::host::linux::paths::sandbox_rootfs_path(&sandbox_id);
        let final_rootfs = format!("{output_dir}/rootfs.ext4");
        if let Err(e) = build_phase(template_id, BuildPhase::CopyRootfs, async {
            std::fs::create_dir_all(&output_dir).map_err(|source| BuildError::Io {
                operation: "create output directory",
                source,
            })?;
            // Cache dir is owned by sandbox UID — need elevated copy.
            let cp_status = command_as_root("cp")
                .args(["--reflink=auto", &cache_rootfs, &final_rootfs])
                .status();
            if !cp_status.is_ok_and(|s| s.success()) {
                return Err(BuildError::message(format!(
                    "cannot copy rootfs to {final_rootfs}"
                )));
            }
            page_cache::advise_path_dontneed(Path::new(&cache_rootfs), "copy build rootfs input");
            page_cache::advise_path_dontneed(Path::new(&final_rootfs), "copy build rootfs output");
            Ok::<(), BuildError>(())
        })
        .await
        {
            tracing::warn!(sandbox_id = %sandbox_id, "deleting sandbox after rootfs copy failure");
            delete_sandbox_best_effort(&mut sbx, &sandbox_id, "rootfs copy failed").await;
            return Err(e);
        }

        let sandbox_metadata_path =
            pplx_space_common_core::host::linux::paths::sandbox_metadata_path(&sandbox_id);
        let execution = match tokio::fs::read(&sandbox_metadata_path).await {
            Ok(bytes) => serde_json::from_slice::<
                pplx_space_common_core::types::SandboxArtifactMetadata,
            >(&bytes)
            .map_err(|error| {
                BuildError::message(format!(
                    "invalid sandbox runtime metadata {sandbox_metadata_path}: {error}"
                ))
            })
            .map(|metadata| metadata.execution),
            Err(source) => Err(BuildError::Io {
                operation: "read sandbox runtime metadata",
                source,
            }),
        };

        // Delete sandbox (remove filesystem + DB row)
        if let Err(error) = build_phase(template_id, BuildPhase::DeleteSandbox, async {
            sbx.delete(DeleteRequest {
                sandbox_id: sandbox_id.clone(),
                template_filter: None,
                blocking: true,
            })
            .await
            .map_err(|e| BuildError::message(format!("sandbox delete failed: {e}")))?;
            Ok::<(), BuildError>(())
        })
        .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                error = %error,
                "sandbox delete failed after rootfs copy"
            );
        }
        let execution = execution?;
        let execution = match &execution_binding {
            Some(binding) => binding.bind(execution)?,
            None => execution,
        };

        // Shrink rootfs for storage/transfer, then add a small fixed amount of
        // free space. If the recipe declares a runtime disk size, template-manager
        // expands the local template rootfs to metadata.disk_size_mb before
        // publishing the build as ready.
        build_phase(template_id, BuildPhase::CompactRootfs, async {
            let headroom_mb = TEMPLATE_ARTIFACT_HEADROOM_MB;
            println!("Shrinking rootfs to minimum size...");
            let shrink_ok = command_as_root("e2fsck")
                .args(["-fy", &final_rootfs])
                .status()
                .map(|s| s.success() || s.code() == Some(1)) // exit 1 = errors fixed
                .unwrap_or(false)
                && command_as_root("resize2fs")
                    .args(["-M", &final_rootfs])
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false);
            if !shrink_ok {
                return Err(BuildError::CommandFailed {
                    operation: "rootfs shrink (e2fsck or resize2fs -M)",
                });
            }

            // Truncate the file to match the shrunk filesystem size.
            // resize2fs -M shrinks the fs but leaves the file at its original size.
            let fs_size = command_as_root("dumpe2fs")
                .args(["-h", &final_rootfs])
                .output()
                .ok()
                .and_then(|o| {
                    let out = String::from_utf8_lossy(&o.stdout);
                    let blocks = out
                        .lines()
                        .find(|l| l.starts_with("Block count:"))
                        .and_then(|l| l.split_whitespace().last())
                        .and_then(|s| s.parse::<u64>().ok());
                    let block_size = out
                        .lines()
                        .find(|l| l.starts_with("Block size:"))
                        .and_then(|l| l.split_whitespace().last())
                        .and_then(|s| s.parse::<u64>().ok());
                    blocks.zip(block_size).map(|(b, s)| b * s)
                });
            let fs_size = match fs_size {
                Some(s) => s,
                None => {
                    return Err(BuildError::message(
                        "could not determine filesystem size via dumpe2fs",
                    ));
                }
            };

            let target_size = fs_size + (headroom_mb as u64 * 1024 * 1024);
            // Round up to next MiB to avoid truncating below filesystem size
            let target_mb = target_size.div_ceil(1024 * 1024);
            if let Some(runtime_disk_mb) = recipe.build_config.disk_size_mb
                && target_mb > u64::from(runtime_disk_mb)
            {
                return Err(BuildError::message(format!(
                    "compacted rootfs target {target_mb} MiB exceeds disk_size_mb {runtime_disk_mb} MiB; increase disk_size_mb"
                )));
            }
            println!("Adding {headroom_mb} MiB artifact headroom for storage/transfer...");
            let truncate_ok = command_as_root("truncate")
                .args(["-s", &format!("{target_mb}M",), &final_rootfs])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !truncate_ok {
                return Err(BuildError::message(format!(
                    "failed to truncate rootfs to {target_mb} MiB"
                )));
            }
            let resize_ok = command_as_root("resize2fs")
                .args([&final_rootfs])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if !resize_ok {
                return Err(BuildError::CommandFailed {
                    operation: "resize2fs after truncate",
                });
            }
            page_cache::advise_path_dontneed(Path::new(&final_rootfs), "compact build rootfs");
            Ok::<(), BuildError>(())
        })
        .await?;

        // Write metadata
        let now = now_iso8601();
        let (spaced_version, spaced_compatibility_family) =
            template_guest_runtime_metadata(&resolved_base, oci_guest_runtime.as_ref());
        let mut tmeta = TemplateMetadata {
            guest_image: None,
            schema_version: None,
            execution: None,
            id: template_id.to_string(),
            name: template_id.to_string(),
            description: recipe.metadata.description.clone(),
            base: base_raw,
            base_resolved: Some(resolved_base.resolved_reference),
            built_at: now,
            spaced_version,
            spaced_compatibility_family,
            recipe: recipe_label.to_string(),
            steps_count: recipe.steps.len(),
            // The recipe build flow always knows these. Wrap in Some(_)
            // for the shared `Option<_>` shape that also tolerates
            // seed/imported builds that don't write them.
            vcpu: Some(vcpu),
            memory_mib: Some(mem_mib),
            disk_size_mb: recipe.build_config.disk_size_mb,
            balloon: recipe.build_config.balloon,
            // Recipe builds don't write `arch` today — the seed builder
            // and downstream consumers fall back to the manifest's
            // `arch` field. Leaving None preserves that behavior.
            arch: None,
        };
        tmeta.set_execution_metadata(execution);
        let meta_path = format!("{output_dir}/metadata.json");
        let meta_json = serde_json::to_string_pretty(&tmeta).expect("serialize template metadata");
        build_phase(template_id, BuildPhase::WriteMetadata, async {
            std::fs::write(&meta_path, meta_json).map_err(|source| BuildError::Io {
                operation: "write template metadata",
                source,
            })?;
            Ok::<(), BuildError>(())
        })
        .await?;

        // Fix ownership so space-tmplmgr (in space-cache-rw group) can manage templates.
        // sudo cp creates files as root:root; we need root:space-cache-rw with g+w.
        // The recursive grant stays scoped to this job's staging dir — the name
        // dir holds sibling digest dirs and concurrent builds' staging dirs —
        // while the name dir itself gets a non-recursive grant so
        // template-manager can create its register staging, rename it, and
        // delete this staging dir (write+exec on the parent).
        build_phase(template_id, BuildPhase::FixPermissions, async {
            let cache_owner = format!("root:{}", group_names::SPACE_CACHE_RW);
            run_root_command(
                command_as_root("chown").args(["-R", &cache_owner, &output_dir]),
                "fix template build ownership",
            )?;
            run_root_command(
                command_as_root("chmod").args(["-R", "g+w", &output_dir]),
                "fix template build group write access",
            )?;
            grant_template_cache_group_access(&format!("{TEMPLATE_DIR}/{template_id}"))?;
            Ok::<(), BuildError>(())
        })
        .await?;

        println!("\n=== Template '{template_id}' built successfully ===");
        println!("  Location: {output_dir}/");

        if let Ok(meta) = std::fs::metadata(&final_rootfs) {
            let size_mb = meta.len() / (1024 * 1024);
            println!("  Size:     {size_mb} MiB");
        }

        // Register with template-manager. The staging dir is ours to remove
        // in every outcome — registration copies the content into the digest
        // layout and deliberately leaves the source intact so a retry after
        // a lost response converges on the published digest. The guard drops
        // (and reaps) when this function returns.
        let content_hash = build_phase(template_id, BuildPhase::RegisterTemplate, async {
            register_built_template(template_id, &staging_dir_name, false).await
        })
        .await?;
        drop(staging_guard);
        Ok(BuildOutcome {
            content_hash: Some(content_hash),
        })
    }

    async fn build_from_scratch(
        &self,
        recipe_label: &str,
        template_id: &str,
        job_id: &JobId,
        recipe: &Recipe,
    ) -> Result<BuildOutcome, BuildError> {
        let scripts = write_scratch_scripts()?;
        let helper_path = scripts.helper_path.clone();
        let rootfs_builder_path = scripts.rootfs_builder_path.clone();
        let staging_dir_name = build_staging_dir_name(job_id);
        let final_output_dir = format!("{TEMPLATE_DIR}/{template_id}/{staging_dir_name}");
        // Bound for its Drop: reaps the staging dir when this function
        // returns, in every outcome.
        let _staging_guard = BuildStagingGuard::new(&final_output_dir);
        let Origin::FromScratch {
            source,
            guest_kernel,
        } = &recipe.origin
        else {
            return Err(BuildError::message("scratch recipe origin is required"));
        };
        let guest_bin_dir = scratch_guest_bin_dir()?;
        let spaced_version = guest_binary_version(&guest_bin_dir).await?;
        let workspace_cargo_toml = scratch_workspace_cargo_toml();
        let cache_dir = format!(
            "{}/builds/seed-cache",
            pplx_space_common_core::host::linux::paths::FIRECRACKER_CACHE_DIR
        );

        println!("=== Building template '{template_id}' from recipe '{recipe_label}' ===");
        println!("  From:  scratch");
        println!(
            "  Source: {} {} ({})",
            source.release, source.version, SCRATCH_SOURCE_TYPE_UBUNTU_MINIMAL_CLOUD_SQUASHFS
        );

        let mut cmd = tokio::process::Command::new(&helper_path);
        cmd.arg("--id").arg(template_id);
        cmd.env("SPACE_SEED_OUTPUT_DIR", &final_output_dir);
        cmd.env("SPACE_SEED_CACHE_DIR", &cache_dir);
        cmd.env(
            "SPACE_SEED_SOURCE_TYPE",
            SCRATCH_SOURCE_TYPE_UBUNTU_MINIMAL_CLOUD_SQUASHFS,
        );
        cmd.env("SPACE_SEED_UBUNTU_RELEASE", &source.release);
        cmd.env("SPACE_SEED_UBUNTU_VERSION", &source.version);
        cmd.env("SPACE_SEED_UBUNTU_DATE", &source.date);
        cmd.env("SPACE_SEED_SPACED_VERSION", &spaced_version);
        cmd.env("SPACE_SEED_GUEST_BIN_DIR", &guest_bin_dir);
        cmd.env("SPACE_SEED_ROOTFS_BUILDER_SCRIPT", &rootfs_builder_path);
        let execution_binding = BuildExecutionBinding::for_scratch(
            self.environment,
            guest_kernel.as_ref(),
            std::env::consts::ARCH
                .parse()
                .map_err(BuildExecutionBindingError::from)?,
        )
        .await?;
        match &execution_binding {
            Some(binding) => binding.configure_scratch(&mut cmd, guest_kernel.as_ref())?,
            None => execution_binding::configure_legacy_scratch(&mut cmd, guest_kernel.as_ref()),
        }
        if let Some(cargo_toml) = workspace_cargo_toml {
            cmd.env("SPACE_SEED_CARGO_TOML", cargo_toml);
        }
        cmd.stdin(Stdio::inherit());
        cmd.stdout(Stdio::inherit());
        cmd.stderr(Stdio::inherit());

        build_phase(template_id, BuildPhase::ScratchBuild, async {
            let status = cmd.status().await.map_err(|source| BuildError::Io {
                operation: "launch scratch template builder",
                source,
            })?;
            if !status.success() {
                return Err(BuildError::CommandFailed {
                    operation: "scratch template build",
                });
            }
            Ok::<(), BuildError>(())
        })
        .await?;

        build_phase(template_id, BuildPhase::NormalizeScratchOutput, async {
            normalize_scratch_output_permissions(&final_output_dir)?;
            grant_template_cache_group_access(&format!("{TEMPLATE_DIR}/{template_id}"))?;
            page_cache::advise_path_dontneed(
                &Path::new(&final_output_dir).join("rootfs.ext4"),
                "scratch build rootfs output",
            );
            Ok::<(), BuildError>(())
        })
        .await?;

        if let Some(binding) = execution_binding {
            let metadata_path = Path::new(&final_output_dir).join("metadata.json");
            let bytes = std::fs::read(&metadata_path).map_err(|source| BuildError::Io {
                operation: "read scratch template metadata",
                source,
            })?;
            let bytes = binding.bind_document(&bytes)?;
            use std::io::Write as _;
            let mut temporary =
                tempfile::NamedTempFile::new_in(&final_output_dir).map_err(|source| {
                    BuildError::Io {
                        operation: "create bound metadata staging file",
                        source,
                    }
                })?;
            temporary
                .write_all(&bytes)
                .map_err(|source| BuildError::Io {
                    operation: "write bound scratch template metadata",
                    source,
                })?;
            // Preserve the mode established by the scratch output normalizer.
            temporary
                .as_file()
                .set_permissions(
                    std::fs::metadata(&metadata_path)
                        .map_err(|source| BuildError::Io {
                            operation: "stat scratch template metadata",
                            source,
                        })?
                        .permissions(),
                )
                .map_err(|source| BuildError::Io {
                    operation: "set bound metadata permissions",
                    source,
                })?;
            temporary
                .persist(&metadata_path)
                .map_err(|error| BuildError::Io {
                    operation: "publish bound scratch template metadata",
                    source: error.error,
                })?;
        }

        println!("Scratch build output: {final_output_dir}");
        let content_hash = build_phase(template_id, BuildPhase::RegisterTemplate, async {
            register_built_template(template_id, &staging_dir_name, true).await
        })
        .await?;

        Ok(BuildOutcome {
            content_hash: Some(content_hash),
        })
    }
}

fn oci_source_message(
    reference: Option<&oci_spec::distribution::Reference>,
) -> Option<&'static str> {
    reference.map(|_| "  Source: OCI image")
}

/// Registers the staged build, retrying on failure. Safe to retry because
/// the staging dir survives registration: a lost response after a successful
/// publish converges on the already-published digest instead of failing the
/// build.
///
/// The retry window must outlast a template-manager restart: the DaemonSet
/// rolls on every deploy, and builds have one job attempt — if registration
/// gives up, the staging guard deletes the only local artifact of a build
/// that may have taken 15+ minutes. Five minutes of 10s polls comfortably
/// covers a pod restart and modest rollout delays.
async fn register_built_template(
    template_id: &str,
    staging_dir_name: &str,
    scratch: bool,
) -> Result<String, BuildError> {
    const ATTEMPTS: u32 = 30;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(10);
    let mut last_error = None;
    for attempt in 1..=ATTEMPTS {
        match register_built_template_once(template_id, staging_dir_name, scratch).await {
            Ok(content_hash) => return Ok(content_hash),
            Err(error) => {
                tracing::warn!(
                    template_id,
                    attempt,
                    error = %error,
                    "template registration attempt failed"
                );
                last_error = Some(error);
                if attempt < ATTEMPTS {
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }
    Err(last_error.expect("at least one registration attempt"))
}

async fn register_built_template_once(
    template_id: &str,
    staging_dir_name: &str,
    scratch: bool,
) -> Result<String, BuildError> {
    let Ok(mut tmpl_client) = pplx_space_template_mgr_client::connect().await else {
        if scratch {
            eprintln!("  WARNING: could not connect to template-manager to register scratch build");
        } else {
            eprintln!("  WARNING: could not connect to template-manager to register build");
        }
        return Err(BuildError::message(
            "could not connect to template-manager to register build",
        ));
    };

    match tmpl_client
        .register_template(RegisterTemplateRequest {
            template: Some(TemplateRef {
                name: template_id.to_string(),
                content_hash: None,
                tag: Some(DEFAULT_TEMPLATE_TAG.to_string()),
            }),
            network_policy: None,
            staging_dir: Some(staging_dir_name.to_string()),
        })
        .await
    {
        Ok(resp) => {
            let inner = resp.into_inner();
            let registered_hash = inner
                .template
                .as_ref()
                .and_then(|t| t.content_hash.as_deref())
                .map(str::to_owned);
            if scratch {
                println!("Registered '{template_id}'");
            }
            println!(
                "  Content hash: {}",
                registered_hash.as_deref().unwrap_or("<unknown>")
            );
            registered_hash.ok_or_else(|| {
                BuildError::message(format!(
                    "template-manager registration for '{template_id}' returned no content hash"
                ))
            })
        }
        Err(e) => {
            eprintln!("  WARNING: template-manager registration failed: {e}");
            Err(BuildError::message(format!(
                "template-manager registration failed: {e}"
            )))
        }
    }
}

/// Deletes this node's other local builds of `template_id`, keeping
/// `keep_hash`. Callers must ensure no concurrent build or publish of the
/// same template is in flight — the build-manager executor gates this on
/// having no sibling pending/running job for the subject.
pub async fn prune_replaced_template_builds(template_id: &str, keep_hash: &str) {
    let Ok(mut tmpl_client) = pplx_space_template_mgr_client::connect().await else {
        eprintln!("  WARNING: could not connect to template-manager to prune old local builds");
        return;
    };

    let response = match tmpl_client
        .list_templates(ListTemplatesRequest::default())
        .await
    {
        Ok(response) => response.into_inner(),
        Err(error) => {
            eprintln!("  WARNING: could not list templates to prune old local builds: {error}");
            return;
        }
    };

    let old_hashes: Vec<String> = response
        .templates
        .into_iter()
        .filter(|template| template.name == template_id)
        .filter_map(|template| template.content_hash)
        .filter(|hash| hash != keep_hash)
        .collect();

    if old_hashes.is_empty() {
        return;
    }

    let mut pruned = 0usize;
    for hash in old_hashes {
        let delete = tmpl_client
            .delete_template_build(DeleteTemplateBuildRequest {
                template: Some(TemplateRef {
                    name: template_id.to_string(),
                    content_hash: Some(hash.clone()),
                    tag: None,
                }),
            })
            .await;
        match delete {
            Ok(_) => {
                pruned += 1;
            }
            Err(error) => {
                eprintln!(
                    "  WARNING: could not prune old local build {template_id}@{hash}: {error}"
                );
            }
        }
    }

    if pruned > 0 {
        println!("  Pruned old local builds: {pruned}");
    }
}

fn validate_scratch_recipe(recipe: &Recipe) -> Result<(), BuildError> {
    if !recipe.steps.is_empty() {
        return Err(BuildError::message(
            "from = \"scratch\" recipes do not support [[steps]] yet",
        ));
    }
    if recipe.build_config.vcpu.is_some()
        || recipe.build_config.memory_mib.is_some()
        || recipe.build_config.disk_size_mb.is_some()
        || recipe.build_config.balloon.is_some()
    {
        return Err(BuildError::message(
            "build_config is not supported for scratch recipes yet",
        ));
    }
    if !matches!(recipe.origin, Origin::FromScratch { .. }) {
        return Err(BuildError::message("scratch recipe origin is required"));
    }
    Ok(())
}

fn write_scratch_scripts() -> Result<ScratchScriptBundle, BuildError> {
    let scratch_script_root =
        Path::new(pplx_space_common_core::host::linux::paths::FIRECRACKER_CACHE_DIR)
            .join(SCRATCH_SCRIPT_STAGE_SUBDIR);
    fs::create_dir_all(&scratch_script_root).map_err(|source| BuildError::Io {
        operation: "create scratch script staging dir",
        source,
    })?;
    let tempdir = tempfile::Builder::new()
        .prefix("space-builder-scratch-")
        .tempdir_in(&scratch_script_root)
        .map_err(|source| BuildError::Io {
            operation: "create scratch script bundle dir",
            source,
        })?;
    let helper_path = tempdir.path().join("build-seed.sh");
    let rootfs_builder_path = tempdir.path().join("rootfs-builder.sh");

    write_executable_script(&helper_path, BUNDLED_BUILD_SEED_SCRIPT)?;
    write_executable_script(&rootfs_builder_path, BUNDLED_ROOTFS_BUILDER_SCRIPT)?;

    Ok(ScratchScriptBundle {
        _tempdir: tempdir,
        helper_path,
        rootfs_builder_path,
    })
}

fn write_executable_script(path: &Path, contents: &str) -> Result<(), BuildError> {
    fs::write(path, contents).map_err(|source| BuildError::Io {
        operation: "write bundled scratch script",
        source,
    })?;
    #[cfg(unix)]
    {
        let mut perms = fs::metadata(path)
            .map_err(|source| BuildError::Io {
                operation: "stat bundled scratch script",
                source,
            })?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).map_err(|source| BuildError::Io {
            operation: "chmod bundled scratch script",
            source,
        })?;
    }
    Ok(())
}

fn scratch_guest_bin_dir() -> Result<PathBuf, BuildError> {
    let exe_path = std::env::current_exe().map_err(|source| BuildError::Io {
        operation: "resolve current executable path",
        source,
    })?;

    for candidate in scratch_guest_bin_candidates(&exe_path) {
        if candidate.join("spaced").is_file() && candidate.join("spaced-svc").is_file() {
            return Ok(candidate);
        }
    }

    Err(BuildError::message(
        "could not locate spaced/spaced-svc in the packaged or node data/spaced artifact directory",
    ))
}

fn scratch_guest_bin_candidates(exe_path: &Path) -> Vec<PathBuf> {
    exe_path
        .parent()
        .and_then(|dir| {
            dir.parent()
                .map(|parent| parent.join(SCRATCH_GUEST_ARTIFACT_SUBDIR))
        })
        .into_iter()
        // Packaged artifacts take precedence. Direct deployments keep guest
        // assets outside the content-addressed service executable directory.
        .chain([PathBuf::from("/space/data/spaced")])
        .collect()
}

fn detect_spaced_version() -> Result<String, BuildError> {
    if let Some(cargo_toml) = scratch_workspace_cargo_toml()
        && let Some(version) = workspace_package_version(&cargo_toml)?
    {
        return Ok(version);
    }

    Ok(env!("CARGO_PKG_VERSION").to_string())
}

// Packaged binaries share the builder's release. Node-installed fallback assets
// are deployed independently, so their own versions must supply the metadata.
async fn guest_binary_version(binaries: &Path) -> Result<String, BuildError> {
    if binaries != Path::new("/space/data/spaced") {
        return detect_spaced_version();
    }
    installed_guest_binary_version(binaries).await
}

async fn installed_guest_binary_version(binaries: &Path) -> Result<String, BuildError> {
    let mut version = None;
    for name in ["spaced", "spaced-svc"] {
        let mut command = tokio::process::Command::new(binaries.join(name));
        command.arg("--version").kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(5), command.output())
            .await
            .map_err(|_| BuildError::message(format!("{name} version query timed out")))?
            .map_err(|source| BuildError::Io {
                operation: "read installed guest binary version",
                source,
            })?;
        let text = std::str::from_utf8(&output.stdout)
            .map_err(|error| BuildError::message(format!("invalid {name} version: {error}")))?;
        let fields = text.split_whitespace().collect::<Vec<_>>();
        let [reported_name, reported_version] = fields.as_slice() else {
            return Err(BuildError::message(format!(
                "{name} did not report its version"
            )));
        };
        if !output.status.success() || *reported_name != name {
            return Err(BuildError::message(format!(
                "{name} did not report its version"
            )));
        }
        if version
            .as_deref()
            .is_some_and(|previous| previous != *reported_version)
        {
            return Err(BuildError::message(
                "installed spaced and spaced-svc versions differ",
            ));
        }
        version = Some((*reported_version).to_owned());
    }
    version.ok_or_else(|| BuildError::message("missing installed guest version"))
}

async fn current_guest_runtime_metadata(
    binaries: &Path,
) -> Result<GuestRuntimeMetadata, BuildError> {
    let spaced_version = guest_binary_version(binaries).await?;
    let spaced_compatibility_family =
        pplx_space_spaced_common::spaced_compatibility_family_for_version(&spaced_version)
            .map_err(|source| BuildError::InvalidSpacedVersion {
                version: spaced_version.clone(),
                source,
            })?;
    Ok(GuestRuntimeMetadata {
        spaced_version,
        spaced_compatibility_family,
    })
}

fn template_guest_runtime_metadata(
    resolved_base: &BaseTemplateBuild,
    oci_guest_runtime: Option<&GuestRuntimeMetadata>,
) -> (Option<String>, Option<String>) {
    if let Some(metadata) = oci_guest_runtime {
        return (
            Some(metadata.spaced_version.clone()),
            Some(metadata.spaced_compatibility_family.clone()),
        );
    }
    let metadata = &resolved_base.metadata;
    (
        metadata.spaced_version.clone(),
        metadata.spaced_compatibility_family.clone(),
    )
}

fn scratch_workspace_cargo_toml() -> Option<PathBuf> {
    let mut current = std::env::current_exe().ok()?.parent()?.to_path_buf();
    loop {
        let cargo_toml = current.join("Cargo.toml");
        if cargo_toml.is_file() {
            return Some(cargo_toml);
        }
        if !current.pop() {
            break;
        }
    }

    let space_cargo_toml = PathBuf::from("/space/Cargo.toml");
    space_cargo_toml.is_file().then_some(space_cargo_toml)
}

fn workspace_package_version(cargo_toml_path: &Path) -> Result<Option<String>, BuildError> {
    let cargo_toml = fs::read_to_string(cargo_toml_path).map_err(|source| BuildError::Io {
        operation: "read workspace Cargo.toml",
        source,
    })?;
    let parsed = toml::from_str::<toml::Table>(&cargo_toml)
        .map_err(|e| BuildError::message(format!("parse workspace Cargo.toml: {e}")))?;

    Ok(parsed
        .get("workspace")
        .and_then(|workspace| workspace.get("package"))
        .and_then(|package| package.get("version"))
        .and_then(toml::Value::as_str)
        .map(ToOwned::to_owned))
}

fn normalize_scratch_output_permissions(output_dir: &str) -> Result<(), BuildError> {
    let cache_owner = format!("root:{}", group_names::SPACE_CACHE_RW);
    let targets = scratch_output_permission_targets(Path::new(output_dir))?;
    for target in targets {
        let status = command_as_root("chown")
            .arg(&cache_owner)
            .arg(&target)
            .status()
            .map_err(|source| BuildError::Io {
                operation: "normalize scratch output permissions",
                source,
            })?;
        if !status.success() {
            return Err(BuildError::CommandFailed {
                operation: "normalize scratch output permissions",
            });
        }

        let status = command_as_root("chmod")
            .arg("g+w")
            .arg(&target)
            .status()
            .map_err(|source| BuildError::Io {
                operation: "normalize scratch output permissions",
                source,
            })?;
        if !status.success() {
            return Err(BuildError::CommandFailed {
                operation: "normalize scratch output permissions",
            });
        }
    }
    Ok(())
}

fn scratch_output_permission_targets(output_dir: &Path) -> Result<Vec<PathBuf>, BuildError> {
    let mut targets = Vec::with_capacity(SCRATCH_OUTPUT_ARTIFACTS.len() + 1);
    let output_metadata = fs::symlink_metadata(output_dir).map_err(|source| BuildError::Io {
        operation: "stat scratch output directory",
        source,
    })?;
    if !output_metadata.is_dir() {
        return Err(BuildError::message(format!(
            "scratch output path is not a directory: {}",
            output_dir.display()
        )));
    }
    targets.push(output_dir.to_path_buf());

    for artifact in SCRATCH_OUTPUT_ARTIFACTS {
        let path = output_dir.join(artifact);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() => targets.push(path),
            Ok(_) => {
                return Err(BuildError::message(format!(
                    "scratch output artifact is not a regular file: {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(BuildError::Io {
                    operation: "stat scratch output artifact",
                    source,
                });
            }
        }
    }

    Ok(targets)
}

fn configure_guest_apt_mirror(ssh: &SshSession) -> Result<(), SshError> {
    let configured = std::env::var(UBUNTU_APT_MIRROR_ENV).ok();
    let Some(cmd) = guest_apt_mirror_command(configured.as_deref()) else {
        return Ok(());
    };
    ssh.exec(&cmd, "root", &StepEnvironment::default())
}

fn guest_apt_mirror_command(configured: Option<&str>) -> Option<String> {
    // Seed creation configures its own mirror. Inherited roots own their source
    // transport and trust setup, so only an explicit operator setting overrides it.
    let mirror = shell_quote(configured?);
    Some(format!(
        "set -euo pipefail\n\
         mirror={mirror}\n\
         if [ -x /usr/local/sbin/space-configure-apt-mirror ]; then\n\
           SPACE_UBUNTU_APT_MIRROR=\"$mirror\" /usr/local/sbin/space-configure-apt-mirror\n\
         else\n\
           mkdir -p /etc/apt/apt.conf.d\n\
           printf 'Acquire::GzipIndexes \"true\";\\n' > /etc/apt/apt.conf.d/99space-apt-mirror\n\
           files=$(find /etc/apt -type f \\( -name '*.sources' -o -name 'sources.list' \\) 2>/dev/null || true)\n\
           if [ -n \"$files\" ]; then\n\
             sed -i -E \"s#https?://(archive|security)\\.ubuntu\\.com/ubuntu/?#$mirror#g\" $files\n\
           fi\n\
         fi"
    ))
}

/// Execute a single build step.
fn execute_step(
    named_step: &recipe::NamedStep,
    ssh: &SshSession,
    step_env: &StepEnvironment<'_>,
) -> Result<(), SshError> {
    match &named_step.step {
        Step::SetRuntime(_) | Step::ClearRuntime => Err(SshError::CopyPayload(
            "runtime steps require the finalization owner".into(),
        )),
        Step::Env { .. } => Ok(()),
        Step::PackageInstall {
            manager: PackageManager::Apt,
            packages,
            ..
        } => {
            let pkgs = packages.join(" ");
            let clean =
                "apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*.deb";
            let install = format!(
                "DEBIAN_FRONTEND=noninteractive DEBCONF_NOWARNINGS=yes \
                 apt-get install -y --no-install-recommends --fix-missing \
                 -o Dpkg::Options::=--force-confdef \
                 -o Dpkg::Options::=--force-confold \
                 {pkgs}"
            );
            let cmd = format!(
                "({clean} && apt-get update && {install}) \
                 || (echo 'apt retry 1...' && sleep 10 && {clean} && apt-get update && {install}) \
                 || (echo 'apt retry 2...' && sleep 30 && {clean} && apt-get update && {install})"
            );
            ssh.exec(&cmd, "root", step_env)
        }
        Step::PackageInstall {
            manager: PackageManager::Pip,
            packages,
            extra_args,
        } => {
            let resolved_extra: Vec<String> = extra_args
                .iter()
                .map(|a| resolve_env_var_in_string(a, step_env))
                .collect();

            let extra = if resolved_extra.is_empty() {
                String::new()
            } else {
                format!(" {}", resolved_extra.join(" "))
            };
            let pkgs = packages
                .iter()
                .map(|p| shell_safe_package(p))
                .collect::<Vec<_>>()
                .join(" ");
            let cmd = format!(
                "python3 -m pip install --quiet --break-system-packages --root-user-action=ignore{extra} {pkgs}"
            );
            ssh.exec(&cmd, "root", step_env)
        }
        Step::PackageInstall {
            manager: PackageManager::Npm,
            packages,
            ..
        } => {
            let pkgs = packages.join(" ");
            let clean = "npm cache clean --force";
            let install =
                format!("cd /home/{DEFAULT_GUEST_USER} && npm install --force --silent {pkgs}");
            let cmd = format!(
                "({install}) \
                 || (echo 'npm retry 1...' && sleep 15 && {clean} && {install}) \
                 || (echo 'npm retry 2...' && sleep 30 && {clean} && {install})"
            );
            ssh.exec(&cmd, DEFAULT_GUEST_USER, step_env)
        }
        Step::PackageInstall {
            manager: PackageManager::NpmGlobal,
            packages,
            ..
        } => {
            let pkgs = packages.join(" ");
            let clean = "npm cache clean --force";
            let install = format!("npm install -g --silent {pkgs}");
            let cmd = format!(
                "({install}) \
                 || (echo 'npm retry 1...' && sleep 15 && {clean} && {install}) \
                 || (echo 'npm retry 2...' && sleep 30 && {clean} && {install})"
            );
            ssh.exec(&cmd, "root", step_env)
        }
        Step::Run { command, user } => {
            let user = user.as_deref().unwrap_or("root");
            ssh.exec(command, user, step_env)
        }
        Step::Copy {
            payload,
            dest,
            source_path,
        } => {
            let staged = materialize_copy_payload(payload, source_path.as_deref())?;
            copy_staged_payload_to_guest(ssh, staged.path(), dest)
        }
        Step::PlaywrightInstall { browsers } => {
            let browsers_str = browsers.join(" ");
            let cmd_deps = format!("npx playwright install-deps {browsers_str}");
            ssh.exec(&cmd_deps, "root", step_env)?;
            let cmd_install =
                format!("HOME=/home/{DEFAULT_GUEST_USER} npx playwright install {browsers_str}");
            ssh.exec(&cmd_install, DEFAULT_GUEST_USER, step_env)
        }
    }
}

fn copy_staged_payload_to_guest(
    ssh: &SshSession,
    source: &Path,
    dest: &str,
) -> Result<(), SshError> {
    let source_name = source
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            SshError::CopyPayload(format!("copy source has no filename: {}", source.display()))
        })?;
    let remote_stage_name = source
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            SshError::CopyPayload(format!(
                "staged payload has no staging directory: {}",
                source.display()
            ))
        })?;
    let source = source
        .to_str()
        .ok_or_else(|| SshError::CopyPayload("staged path is not UTF-8".into()))?;
    let remote_stage_dir = guest_path_join("/tmp", remote_stage_name);
    ssh.exec(
        &format!(
            "rm -rf {dir} && mkdir -p {dir}",
            dir = shell_quote(&remote_stage_dir)
        ),
        "root",
        &StepEnvironment::default(),
    )?;
    let remote_source = guest_path_join(&remote_stage_dir, source_name);
    let finalize = guest_copy_finalize_command(&remote_source, dest);
    let result = ssh
        .copy(source, &remote_stage_dir, "root")
        .and_then(|()| ssh.exec(&finalize, "root", &StepEnvironment::default()));
    let cleanup = ssh.exec(
        &format!("rm -rf {}", shell_quote(&remote_stage_dir)),
        "root",
        &StepEnvironment::default(),
    );
    result.and(cleanup)
}

fn guest_path_join(parent: &str, name: &str) -> String {
    format!("{}/{name}", parent.trim_end_matches('/'))
}

fn guest_copy_finalize_command(source_path: &str, target_path: &str) -> String {
    format!(
        r#"set -e
source_path={source_path}
target_path={target_path}

if [ -L "$source_path" ] || [ -f "$source_path" ]; then
    mkdir -p "$(dirname "$target_path")"
    mv "$source_path" "$target_path"
elif [ -d "$source_path" ]; then
    mkdir -p "$target_path"
    find "$source_path" -mindepth 1 -maxdepth 1 -exec mv {{}} "$target_path/" \;
else
    echo "Error: staged copy source is neither file, directory, nor symlink: $source_path" >&2
    exit 1
fi"#,
        source_path = shell_quote(source_path),
        target_path = shell_quote(target_path),
    )
}

struct StagedCopyPayload {
    _tempdir: TempDir,
    path: PathBuf,
}

impl StagedCopyPayload {
    fn path(&self) -> &Path {
        &self.path
    }
}

fn materialize_copy_payload(
    payload: &CopyPayload,
    source_path: Option<&str>,
) -> Result<StagedCopyPayload, SshError> {
    let tempdir = tempfile::Builder::new()
        .prefix("space-builder-copy-")
        .tempdir()
        .map_err(|err| SshError::CopyPayload(format!("create staging dir: {err}")))?;

    let path = match payload {
        CopyPayload::InlineBytes(bytes) => {
            let path = tempdir.path().join("payload");
            fs::write(&path, bytes)
                .map_err(|err| SshError::CopyPayload(format!("write inline payload: {err}")))?;
            path
        }
        CopyPayload::BlobSha256(sha256) => {
            let blob_path = blob::blob_path(sha256)
                .map_err(|err| SshError::CopyPayload(format!("resolve blob {sha256}: {err}")))?;
            if !blob_path.exists() {
                return Err(SshError::CopyPayload(format!("blob {sha256} not found")));
            }
            extract_tar_payload(&blob_path, tempdir.path(), source_path)?
        }
    };

    Ok(StagedCopyPayload {
        _tempdir: tempdir,
        path,
    })
}

fn extract_tar_payload(
    archive: &Path,
    dst: &Path,
    source_path: Option<&str>,
) -> Result<PathBuf, SshError> {
    match source_path {
        Some(source_path) => extract_requested_tar_payload(archive, dst, source_path),
        None => extract_single_top_level_tar_payload(archive, dst),
    }
}

fn extract_single_top_level_tar_payload(archive: &Path, dst: &Path) -> Result<PathBuf, SshError> {
    extract_matching_tar_entries(archive, dst, |path| Some(path.to_path_buf()))?;

    let entries = fs::read_dir(dst)
        .map_err(|err| SshError::CopyPayload(format!("read extracted payload dir: {err}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| SshError::CopyPayload(format!("read extracted payload entry: {err}")))?;
    match entries.as_slice() {
        [entry] => Ok(entry.path()),
        [] => Err(SshError::CopyPayload("tar payload is empty".into())),
        _ => Err(SshError::CopyPayload(
            "tar payload must contain exactly one top-level entry".into(),
        )),
    }
}

fn extract_requested_tar_payload(
    archive: &Path,
    dst: &Path,
    source_path: &str,
) -> Result<PathBuf, SshError> {
    let source_path = Path::new(source_path.trim_end_matches('/'));
    validate_tar_entry_path(source_path)?;
    let staged_name = source_path
        .file_name()
        .ok_or_else(|| {
            SshError::CopyPayload(format!(
                "tar payload source path has no filename: {}",
                source_path.display()
            ))
        })?
        .to_owned();
    let staged = PathBuf::from(staged_name);

    let extracted_entries = extract_matching_tar_entries(archive, dst, |path| {
        requested_tar_entry_target(path, source_path, &staged)
    })?;

    if extracted_entries > 0 {
        Ok(dst.join(staged))
    } else {
        Err(SshError::CopyPayload(format!(
            "tar payload source path not found: {}",
            source_path.display()
        )))
    }
}

fn requested_tar_entry_target(path: &Path, source_path: &Path, staged: &Path) -> Option<PathBuf> {
    path.strip_prefix(source_path).ok().map(|relative_path| {
        if relative_path.as_os_str().is_empty() {
            staged.to_path_buf()
        } else {
            staged.join(relative_path)
        }
    })
}

fn extract_matching_tar_entries(
    archive_path: &Path,
    dst: &Path,
    mut target_for_path: impl FnMut(&Path) -> Option<PathBuf>,
) -> Result<usize, SshError> {
    let file = std::fs::File::open(archive_path)
        .map_err(|err| SshError::CopyPayload(format!("open tar payload: {err}")))?;
    let mut archive = tar::Archive::new(file);
    archive.set_preserve_permissions(false);
    archive.set_preserve_ownerships(false);
    archive.set_overwrite(false);

    let mut extracted_entries = 0;
    for entry in archive
        .entries()
        .map_err(|err| SshError::CopyPayload(format!("read tar payload: {err}")))?
    {
        let mut entry =
            entry.map_err(|err| SshError::CopyPayload(format!("read tar entry: {err}")))?;
        let path = entry
            .path()
            .map_err(|err| SshError::CopyPayload(format!("read tar entry path: {err}")))?
            .into_owned();
        validate_tar_entry_path(&path)?;

        let entry_type = entry.header().entry_type();
        let target = target_for_path(&path);
        validate_supported_tar_entry(target.as_deref().unwrap_or(&path), entry_type, &mut entry)?;

        let Some(target) = target else {
            continue;
        };
        validate_tar_entry_path(&target)?;
        unpack_tar_entry_to_target(&path, entry_type, &mut entry, dst, &target)?;
        extracted_entries += 1;
    }

    page_cache::advise_path_dontneed(archive_path, "extract build copy payload input");
    Ok(extracted_entries)
}

fn unpack_tar_entry_to_target(
    path: &Path,
    entry_type: tar::EntryType,
    entry: &mut tar::Entry<impl std::io::Read>,
    dst: &Path,
    target: &Path,
) -> Result<(), SshError> {
    if entry_type.is_dir() {
        return create_staged_tar_dirs(dst, target);
    }

    if let Some(parent) = target.parent() {
        create_staged_tar_dirs(dst, parent)?;
    }
    entry.unpack(dst.join(target)).map(|_| ()).map_err(|err| {
        SshError::CopyPayload(format!("extract tar entry {}: {err}", path.display()))
    })
}

fn create_staged_tar_dirs(dst: &Path, relative: &Path) -> Result<(), SshError> {
    // Staging is a private TempDir with one writer. Inspect each component before
    // descending so earlier archive entries cannot redirect subsequent writes.
    let mut directory = dst.to_path_buf();
    for component in relative.components() {
        directory.push(component);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err(SshError::CopyPayload(format!(
                    "tar payload parent is not a real directory: {}",
                    directory.display()
                )));
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&directory).map_err(|err| {
                    SshError::CopyPayload(format!(
                        "create staged directory {}: {err}",
                        directory.display()
                    ))
                })?;
            }
            Err(err) => {
                return Err(SshError::CopyPayload(format!(
                    "inspect staged directory {}: {err}",
                    directory.display()
                )));
            }
        }
    }
    Ok(())
}

fn validate_supported_tar_entry(
    path: &Path,
    entry_type: tar::EntryType,
    entry: &mut tar::Entry<impl std::io::Read>,
) -> Result<(), SshError> {
    if entry_type.is_hard_link() {
        return Err(SshError::CopyPayload(format!(
            "tar payload contains unsupported hard-link entry: {}",
            path.display()
        )));
    }
    if entry_type.is_symlink() {
        let target = entry
            .link_name()
            .map_err(|err| SshError::CopyPayload(format!("read tar symlink target: {err}")))?
            .ok_or_else(|| {
                SshError::CopyPayload(format!(
                    "tar payload symlink {} has empty target",
                    path.display()
                ))
            })?;
        validate_tar_symlink_target(path, &target)?;
    } else if !entry_type.is_file() && !entry_type.is_dir() {
        return Err(SshError::CopyPayload(format!(
            "tar payload contains unsupported entry type {entry_type:?}: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_tar_entry_path(path: &Path) -> Result<(), SshError> {
    if path.as_os_str().is_empty() {
        return Err(SshError::CopyPayload(
            "tar payload contains empty entry path".into(),
        ));
    }
    for component in path.components() {
        match component {
            std::path::Component::Normal(_) | std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                return Err(SshError::CopyPayload(format!(
                    "tar payload contains unsafe entry path: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

/// Reject symlink targets that lexically escape staging after path mapping.
/// Writes must also reject symlink ancestors, since links can change resolution.
///
/// The link's effective target is `link_path.parent()/target`. We walk that
/// joined path component-by-component, popping on `..` and rejecting if the
/// virtual cursor would step above the staging root. Absolute targets and
/// any `RootDir` / `Prefix` components are rejected outright.
fn validate_tar_symlink_target(link_path: &Path, target: &Path) -> Result<(), SshError> {
    if target.as_os_str().is_empty() {
        return Err(SshError::CopyPayload(format!(
            "tar payload symlink {} has empty target",
            link_path.display()
        )));
    }
    if target.is_absolute() {
        return Err(SshError::CopyPayload(format!(
            "tar payload symlink target is absolute: {} -> {}",
            link_path.display(),
            target.display()
        )));
    }
    let mut depth: usize = link_path
        .parent()
        .map(|p| {
            p.components()
                .filter(|component| matches!(component, std::path::Component::Normal(_)))
                .count()
        })
        .unwrap_or(0);
    for component in target.components() {
        match component {
            std::path::Component::Normal(_) | std::path::Component::CurDir => {
                if matches!(component, std::path::Component::Normal(_)) {
                    depth += 1;
                }
            }
            std::path::Component::ParentDir => {
                if depth == 0 {
                    return Err(SshError::CopyPayload(format!(
                        "tar payload symlink target escapes staging root: {} -> {}",
                        link_path.display(),
                        target.display()
                    )));
                }
                depth -= 1;
            }
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err(SshError::CopyPayload(format!(
                    "tar payload symlink target has unsafe component: {} -> {}",
                    link_path.display(),
                    target.display()
                )));
            }
        }
    }
    Ok(())
}

/// Resolve `$VAR` references using a provided env map.
fn resolve_env_var_in_string(s: &str, env: &StepEnvironment<'_>) -> String {
    let mut resolved = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find('$') {
        let (before, after) = rest.split_at(pos);
        resolved.push_str(before);
        let Some(after_dollar) = after.strip_prefix('$') else {
            break;
        };
        let end = after_dollar
            .char_indices()
            .find(|(_, c)| !c.is_ascii_alphanumeric() && *c != '_')
            .map(|(index, _)| index)
            .unwrap_or(after_dollar.len());
        let (var_name, after_var) = after_dollar.split_at(end);
        if var_name.is_empty() {
            resolved.push('$');
            rest = after_dollar;
            continue;
        }
        let val = env
            .get(var_name)
            .map(|value| value.expose())
            .unwrap_or_default();
        resolved.push_str(val);
        rest = after_var;
    }
    resolved.push_str(rest);
    resolved
}

fn shell_quote(value: &str) -> String {
    let escaped = value.replace('\'', "'\\''");
    format!("'{escaped}'")
}

/// Quote a pip package spec for shell safety.
fn shell_safe_package(pkg: &str) -> String {
    if pkg.contains('<') || pkg.contains('>') || pkg.contains('[') || pkg.contains(' ') {
        format!("'{pkg}'")
    } else {
        pkg.to_string()
    }
}

fn now_iso8601() -> String {
    jiff::Zoned::now()
        .strftime("%Y-%m-%dT%H:%M:%S%:z")
        .to_string()
}

#[cfg(test)]
mod build_staging_tests;

#[cfg(test)]
mod scratch_output_tests;

#[cfg(test)]
mod tar_symlink_tests;

#[cfg(test)]
mod env_expansion_tests;

#[cfg(all(test, target_os = "linux"))]
mod apt_mirror_tests;
