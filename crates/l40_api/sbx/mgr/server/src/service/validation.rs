use pplx_space_common_core::types::SandboxId;

use super::*;

pub(super) fn snapshot_type_from_proto(value: i32) -> Result<crate::proto::SnapshotType, Status> {
    use crate::proto::SnapshotType;
    let snapshot_type = SnapshotType::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown snapshot type value {value}")))?;
    Ok(match snapshot_type {
        SnapshotType::Unspecified | SnapshotType::Full => SnapshotType::Full,
        SnapshotType::Disk => SnapshotType::Disk,
    })
}

pub(super) fn optional_positive_or_default(
    field: &'static str,
    requested: Option<u32>,
    default: u32,
) -> Result<u32, Status> {
    match requested {
        Some(0) => Err(Status::invalid_argument(format!(
            "{field} must be greater than 0 when set"
        ))),
        Some(value) => Ok(value),
        None => Ok(default),
    }
}

pub(super) fn canonical_batched_sandbox_ids(ids: &[String]) -> Result<Vec<String>, Status> {
    use pplx_space_common_core::inventory::{InventoryBudget, MAX_ENCODED_BYTES, MAX_ITEMS};
    let mut budget = InventoryBudget::new(MAX_ITEMS, MAX_ENCODED_BYTES);
    for id in ids {
        budget
            .charge(id.len())
            .map_err(crate::inventory_budget::exceeded)?;
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for id in ids.iter().map(|id| id.trim()).filter(|id| !id.is_empty()) {
        let parsed = id.parse::<SandboxId>().map_err(|_| {
            Status::invalid_argument(format!(
                "sandbox_ids must contain canonical sandbox UUIDs; invalid id '{id}'"
            ))
        })?;
        let canonical = parsed.to_string();
        if id != canonical {
            return Err(Status::invalid_argument(format!(
                "sandbox_ids must contain canonical sandbox UUIDs; got non-canonical id '{id}'"
            )));
        }
        if seen.insert(canonical.clone()) {
            out.push(canonical);
        }
    }
    Ok(out)
}

pub(super) fn duration_ceil_secs(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
}

pub(super) fn normalize_arch(arch: &str) -> &str {
    match arch {
        "amd64" => "x86_64",
        "arm64" => "aarch64",
        other => other,
    }
}

pub(super) fn reject_incompatible_template_arch(
    template: &str,
    content_hash: &str,
    template_arch: &str,
) -> Result<(), tonic::Status> {
    let node_arch = normalize_arch(std::env::consts::ARCH);
    let template_arch = normalize_arch(template_arch);
    if template_arch == node_arch {
        return Ok(());
    }

    tracing::error!(
        template = %template,
        content_hash = %content_hash,
        template_arch = %template_arch,
        node_arch = %node_arch,
        "rejecting sandbox create from template built for different architecture"
    );
    Err(tonic::Status::failed_precondition(format!(
        "template '{template}' ({content_hash}) was built for {template_arch}, but this node is {node_arch}"
    )))
}

pub(super) fn template_metadata_arch(template_dir: &str) -> Option<String> {
    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let metadata_json = std::fs::read_to_string(metadata_path).ok()?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json).ok()?;
    metadata
        .get("arch")
        .and_then(serde_json::Value::as_str)
        .map(|arch| normalize_arch(arch).to_string())
}

pub(super) fn template_execution_metadata(
    template_dir: &str,
    fallback_arch: &str,
) -> Result<pplx_space_common_core::types::ExecutionMetadata, Status> {
    use pplx_space_common_core::types::{Arch, ExecutionMetadata, TemplateMetadata};

    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let metadata = match std::fs::read(&metadata_path) {
        Ok(bytes) => serde_json::from_slice::<TemplateMetadata>(&bytes).map_err(|error| {
            Status::failed_precondition(format!(
                "parse template execution metadata {}: {error}",
                metadata_path.display()
            ))
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => TemplateMetadata::default(),
        Err(error) => {
            return Err(Status::internal(format!(
                "read template execution metadata {}: {error}",
                metadata_path.display()
            )));
        }
    };
    match metadata.execution_metadata().map_err(|error| {
        Status::failed_precondition(format!(
            "invalid template execution metadata {}: {error}",
            metadata_path.display()
        ))
    })? {
        Some(execution) => Ok(execution.clone()),
        None => {
            let architecture = normalize_arch(metadata.arch.as_deref().unwrap_or(fallback_arch))
                .parse::<Arch>()
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            Ok(ExecutionMetadata::initial_firecracker(architecture, vec![]))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct TemplateRuntimeDefaults {
    pub(super) vcpu: Option<u32>,
    pub(super) memory_mib: Option<u32>,
    pub(super) balloon: TemplateBalloon,
}

/// The template's opt-in balloon device. Unlike the sizing defaults, a
/// present but invalid setting is kept so a cold boot can reject it rather
/// than silently boot without the device the template asked for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) enum TemplateBalloon {
    #[default]
    Absent,
    Configured(pplx_space_common_core::types::BalloonConfig),
    Invalid(String),
}

impl TemplateBalloon {
    fn from_metadata(value: Option<serde_json::Value>) -> Self {
        match value {
            None => Self::Absent,
            Some(value) => match serde_json::from_value(value) {
                Ok(config) => Self::Configured(config),
                Err(error) => Self::Invalid(error.to_string()),
            },
        }
    }

    /// The device a cold boot with `memory_mib` of guest memory renders.
    pub(super) fn for_memory_mib(
        &self,
        memory_mib: u32,
    ) -> Result<Option<pplx_space_common_core::types::BalloonConfig>, Status> {
        match self {
            Self::Absent => Ok(None),
            Self::Configured(config) => config
                .validate_for_memory_mib(memory_mib)
                .map(|()| Some(*config))
                .map_err(|error| {
                    Status::failed_precondition(format!("template metadata balloon: {error}"))
                }),
            Self::Invalid(error) => Err(Status::failed_precondition(format!(
                "template metadata balloon: {error}"
            ))),
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct TemplateMetadataReader {
    vcpu: Option<u8>,
    memory_mib: Option<u32>,
    balloon: Option<serde_json::Value>,
}

impl TemplateMetadataReader {
    fn runtime_defaults(self) -> TemplateRuntimeDefaults {
        let positive = |value: Option<u32>| value.filter(|value| *value > 0);
        TemplateRuntimeDefaults {
            vcpu: self.vcpu.filter(|value| *value > 0).map(u32::from),
            memory_mib: positive(self.memory_mib),
            balloon: TemplateBalloon::from_metadata(self.balloon),
        }
    }
}

pub(super) fn template_runtime_defaults(template_dir: &str) -> TemplateRuntimeDefaults {
    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let Ok(metadata_json) = std::fs::read_to_string(metadata_path) else {
        return TemplateRuntimeDefaults::default();
    };
    let Ok(metadata) = serde_json::from_str::<TemplateMetadataReader>(&metadata_json) else {
        return TemplateRuntimeDefaults::default();
    };
    metadata.runtime_defaults()
}

#[derive(Debug, Default)]
pub(super) struct TemplateSpacedCompatibility {
    pub(super) version: Option<String>,
    pub(super) compatibility_family: Option<String>,
}

pub(super) fn template_metadata_spaced_compatibility(
    template_dir: &str,
) -> TemplateSpacedCompatibility {
    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let Ok(metadata_json) = std::fs::read_to_string(metadata_path) else {
        return TemplateSpacedCompatibility::default();
    };
    let Ok(metadata) = serde_json::from_str::<serde_json::Value>(&metadata_json) else {
        return TemplateSpacedCompatibility::default();
    };
    TemplateSpacedCompatibility {
        version: metadata
            .get("spaced_version")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        compatibility_family: metadata
            .get("spaced_compatibility_family")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
    }
}

pub(super) fn reject_incompatible_template_spaced_compatibility(
    template: &str,
    content_hash: &str,
    template_version: Option<&str>,
    template_compatibility_family: Option<&str>,
) -> Result<(), tonic::Status> {
    pplx_space_spaced_common::validate_spaced_guest_compatibility(
        template_version,
        template_compatibility_family,
    )
    .map_err(|error| {
        tracing::error!(
            template,
            content_hash,
            %error,
            "rejecting template with unsupported spaced guest identity"
        );
        tonic::Status::failed_precondition(format!(
            "template '{template}' ({content_hash}): {error}"
        ))
    })
}

/// A bound request must be positively acknowledged before its response is
/// persisted or used to launch. Older node-managers discard the optional field.
pub(super) fn require_environment_acknowledgement(
    requested: &pplx_space_common_core::types::ExecutionMetadata,
    resolved: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<(), Status> {
    if let Some(expected) = requested.environment()
        && resolved.environment() != Some(expected)
    {
        return Err(Status::failed_precondition(format!(
            "node-manager did not acknowledge requested execution environment {expected}"
        )));
    }
    Ok(())
}

/// Explicit selection cannot relabel a template. Selected and non-default
/// bindings may use snapshots at create and cluster-owned identity once the
/// serving node admits them, and non-default bindings may also fill and adopt
/// pools and boot warm; paths that capture or load memory also require the
/// environment's qualified memory-state support.
pub(super) fn validate_environment_create(
    request: &crate::proto::CreateRequest,
    environment: Option<pplx_space_common_core::types::EnvironmentReference>,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<bool, tonic::Status> {
    if environment.is_some() && environment != execution.environment() {
        return Err(tonic::Status::failed_precondition(
            "requested environment differs from the template binding",
        ));
    }
    let runtime = execution.runtime().kind;
    if runtime == pplx_space_common_core::types::RuntimeKind::CloudHypervisor
        && (request
            .sandbox_id
            .as_deref()
            .is_some_and(|id| !id.is_empty())
            || request.route_identity.is_some())
    {
        return Err(Status::failed_precondition(
            "selected environment requires qualified cluster placement",
        ));
    }
    let capabilities = runtime.baseline_capabilities();
    if !capabilities.prewarmed_pool
        && (is_pool_member(request) || request.boot_mode == crate::proto::BootMode::Pool as i32)
    {
        return Err(Status::failed_precondition(
            "runtime does not support warm pools",
        ));
    }
    if !capabilities.snapshot
        && (request.snapshots_enabled
            || request.boot_mode != crate::proto::BootMode::Cold as i32
            || request.pool_template.is_some())
    {
        return Err(Status::failed_precondition(
            "runtime supports cold creation only",
        ));
    }
    if execution.runtime().kind == pplx_space_common_core::types::RuntimeKind::CloudHypervisor
        && execution.environment().is_none()
    {
        return Err(Status::failed_precondition(
            "standalone runtime requires a bound template",
        ));
    }
    let requested_runtime = request
        .runtime
        .parse::<pplx_space_common_core::types::RuntimeKind>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if requested_runtime != execution.runtime().kind {
        return Err(Status::failed_precondition(
            "requested runtime differs from the template binding",
        ));
    }
    let non_default = is_non_default_execution(execution);
    // A selected create always boots cold, so only its snapshots load memory.
    let memory_state = if environment.is_some() {
        request.snapshots_enabled
    } else {
        requests_memory_state(request)
    };
    if (non_default || environment.is_some()) && memory_state && requires_cold_creation(execution) {
        return Err(tonic::Status::failed_precondition(
            "execution environment does not support the memory-state operations that pools, snapshots and base-snapshot boot require",
        ));
    }
    Ok(non_default)
}

/// Pool donors are matched by build, so every member of a build must carry the
/// build's own binding. Checked from the request alone, before any node or
/// template call.
pub(super) fn validate_selected_create_fields(
    request: &crate::proto::CreateRequest,
) -> Result<(), tonic::Status> {
    if is_pool_member(request) {
        return Err(tonic::Status::failed_precondition(
            "pool members take their template's binding; select the environment when building the template",
        ));
    }
    Ok(())
}

/// An empty pool template names no pool, as everywhere else in Create.
fn is_pool_member(request: &crate::proto::CreateRequest) -> bool {
    request
        .pool_template
        .as_deref()
        .is_some_and(|value| !value.is_empty())
}

/// Pool members are paused for adoption and cloning, base-snapshot boot loads
/// memory, and enabled snapshots capture it.
fn requests_memory_state(request: &crate::proto::CreateRequest) -> bool {
    is_pool_member(request)
        || request.snapshots_enabled
        || request.boot_mode != crate::proto::BootMode::Cold as i32
}

/// Node-local cold creation may select a compatible consumer before its first
/// persisted execution. Cluster-directed work and pools keep the exact binding
/// their placement and donors were chosen for.
pub(super) fn allows_node_local_environment_selection(
    request: &crate::proto::CreateRequest,
) -> bool {
    request.sandbox_id.as_deref().is_none_or(str::is_empty)
        && request.route_identity.is_none()
        && !requests_memory_state(request)
}

/// A non-default base snapshot boots only under the environment that captured
/// it. Restore selects its consumer from this producer, so a snapshot captured
/// under another binding would silently move the sandbox off its template's
/// environment. E0 and legacy executions keep their existing contract, in
/// which missing provenance means E0, and refuse only a snapshot recorded
/// under a non-default environment.
pub(super) fn base_snapshot_matches_execution(
    snapshot_dir: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> bool {
    let producer = super::create_support::read_base_snapshot_execution_metadata(
        std::path::Path::new(snapshot_dir),
    );
    if !is_non_default_execution(execution) {
        // Unreadable provenance keeps failing where it always has, at staging.
        let Ok(producer) = producer else {
            return true;
        };
        if !is_non_default_execution(producer.execution()) {
            return true;
        }
        tracing::info!(
            snapshot_dir,
            producer = ?producer.execution().environment(),
            "base snapshot was captured under a non-default environment; booting cold"
        );
        return false;
    }
    match producer {
        Ok(producer) if producer.execution().environment() == execution.environment() => true,
        Ok(producer) => {
            tracing::info!(
                snapshot_dir,
                producer = ?producer.execution().environment(),
                consumer = ?execution.environment(),
                "base snapshot was captured under another environment; booting cold"
            );
            false
        }
        Err(error) => {
            tracing::warn!(
                snapshot_dir,
                %error,
                "base snapshot provenance is unreadable; booting cold"
            );
            false
        }
    }
}

/// Decode the additive input before normal admission. Both legacy inputs must
/// be absent so an old server cannot ignore selection and execute default work.
pub(super) fn normalize_environment_input(
    request: &mut crate::proto::CreateRequest,
) -> Result<Option<pplx_space_common_core::types::EnvironmentReference>, Status> {
    let Some(input) = request.environment.take() else {
        return Ok(None);
    };
    if !request.template.is_empty() || !request.runtime.is_empty() {
        return Err(Status::invalid_argument("provide exactly one create input"));
    }
    let reference = input.reference.parse().map_err(
        |error: pplx_space_common_core::types::DigestParseError| {
            Status::invalid_argument(error.to_string())
        },
    )?;
    request.template = input.template;
    Ok(Some(reference))
}

/// The initial environment's reference for `arch`, computed once per
/// architecture: admission checks it on every create.
fn initial_environment_reference(
    arch: pplx_space_common_core::types::Arch,
) -> Option<pplx_space_common_core::types::EnvironmentReference> {
    use pplx_space_common_core::types::{Arch, EnvironmentReference};
    use pplx_space_node_runtime_core::catalog::InitialEnvironment;
    static X86: std::sync::OnceLock<Option<EnvironmentReference>> = std::sync::OnceLock::new();
    static ARM: std::sync::OnceLock<Option<EnvironmentReference>> = std::sync::OnceLock::new();
    let cache = match arch {
        Arch::X86_64 => &X86,
        Arch::Aarch64 => &ARM,
    };
    *cache.get_or_init(|| {
        let initial = match InitialEnvironment::for_architecture(arch) {
            Ok(initial) => initial,
            Err(error) => {
                tracing::warn!(%error, "initial environment pins unavailable");
                return None;
            }
        };
        match initial.definition() {
            Ok(definition) => Some(definition.reference()),
            Err(error) => {
                tracing::warn!(%error, "initial environment definition unavailable");
                None
            }
        }
    })
}

/// Fleet placement and pool identity must understand a complete binding before
/// a non-default template can use those paths, independently of snapshot support.
pub(super) fn is_non_default_execution(
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> bool {
    // Only the x86-64 initial binding is the default, on every architecture.
    execution.environment().is_some_and(|reference| {
        Some(reference)
            != initial_environment_reference(pplx_space_common_core::types::Arch::X86_64)
    })
}

/// Bound environments require qualified memory-state support before any
/// capture or warm creation. Legacy records retain their existing contract.
pub(super) fn requires_cold_creation(
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> bool {
    let Some(reference) = execution.environment() else {
        return false;
    };
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    match CompiledEnvironment::for_architecture(execution.guest().architecture) {
        Ok(environments) => !environments.into_iter().any(|entry| {
            entry.supports_memory_state() && entry.definition().reference() == reference
        }),
        Err(error) => {
            tracing::warn!(%error, "additional environment catalog unavailable; retaining initial memory-state support");
            Some(reference) != initial_environment_reference(execution.guest().architecture)
        }
    }
}

#[cfg(test)]
mod tests;

/// Reject timer-driven operations at policy admission, before a sandbox can
/// become resident with a policy its adapter will never execute.
pub(super) fn validate_runtime_lifecycle_policy(
    runtime: pplx_space_common_core::types::RuntimeKind,
    policy: &pplx_space_common_core::types::AutoLifecyclePolicy,
) -> Result<(), Status> {
    let capabilities = runtime.baseline_capabilities();
    if !capabilities.pause_resume
        && policy.contains_action(pplx_space_common_core::types::AutoLifecycleIdleAction::Pause)
    {
        return Err(Status::failed_precondition(
            "runtime does not support automatic pause",
        ));
    }
    if !capabilities.snapshot
        && policy.contains_action(pplx_space_common_core::types::AutoLifecycleIdleAction::Suspend)
    {
        return Err(Status::failed_precondition(
            "runtime does not support automatic suspend",
        ));
    }
    Ok(())
}

/// Runtime support does not qualify every installed environment for persisted
/// state. In particular, historical cold-only CH identities remain cold-only.
pub(super) fn validate_execution_lifecycle_policy(
    execution: Option<&pplx_space_common_core::types::ExecutionMetadata>,
    policy: &pplx_space_common_core::types::AutoLifecyclePolicy,
) -> Result<(), Status> {
    if execution.is_some_and(requires_cold_creation)
        && policy.contains_action(pplx_space_common_core::types::AutoLifecycleIdleAction::Suspend)
    {
        return Err(Status::failed_precondition(
            "execution environment does not support automatic suspend",
        ));
    }
    Ok(())
}
