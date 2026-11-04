//! The runtime upgrade decided when a sandbox is resumed or started.
//!
//! When a stopped sandbox starts or a suspended one resumes, the control plane
//! compares the environment it last ran under with the cluster's environment
//! policy (`pplx_space_runtime_environment_policy_store`) and the certified
//! edges in the environment catalog (`runtime_environments`), and picks one of
//! [`RuntimePlan`]'s outcomes:
//!
//! * the environment still executes, so the sandbox starts or restores as
//!   recorded;
//! * a memory edge reaches the cluster default, so a suspended sandbox
//!   restores normally and the node's restore-consumer selection loads the
//!   checkpoint under the default;
//! * an upgrade edge reaches the default, so the node runs its runtime upgrade
//!   (`UpgradeRuntime`) instead: a fenced cold boot of the disk under the
//!   default that keeps files, loses memory and records the default as the
//!   sandbox's consumer without rewriting the checkpoint's producer. A
//!   suspended sandbox's memory is discarded only when an operator blocked its
//!   environment;
//! * nothing reaches the default, or only an upgrade edge reaches it from an
//!   environment no operator blocked for a suspended sandbox, so the sandbox
//!   is stranded and the activation is refused until an operator acts.
//!
//! A stopped sandbox on a `deprecated` environment is upgraded as well when an
//! edge allows it: a cold start loses nothing a plain start would keep. A
//! suspended one on a `deprecated` environment restores as recorded. With no
//! cluster default the policy narrows nothing, and every activation proceeds
//! as recorded.
//!
//! Shipped disabled behind [`RuntimeUpgradeAtActivation`]. The decision only
//! chooses what to ask the node for; the node re-checks the edge against its
//! compiled catalog and enforces its applied execution policy regardless.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::types::{
    EnvironmentReference, INITIAL_FIRECRACKER_RUNTIME_VERSION,
    INITIAL_FIRECRACKER_STATE_FORMAT_VERSION, LocalArtifactId, SandboxId,
};
use pplx_space_runtime_environment_policy_store::{
    RuntimeEnvironmentPolicy, RuntimeEnvironmentState,
};

#[cfg(test)]
mod tests;

/// Whether resume and start decide a runtime upgrade.
///
/// Off, every activation proceeds as recorded, as before. The decision runs
/// in each process that drives activations, the API and the cluster
/// controller, so both read the same switch ([`Self::ENV`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeUpgradeAtActivation {
    #[default]
    Disabled,
    Enabled,
}

impl RuntimeUpgradeAtActivation {
    /// `1`, `true`, `yes`, `on` or `enabled`, in any case, turns the decision
    /// on; unset or anything else keeps it off.
    pub const ENV: &'static str = "SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED";

    pub fn from_enabled(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }

    pub fn from_env() -> Self {
        Self::from_enabled(pplx_space_common_core::env::env_flag_var(Self::ENV, false))
    }

    pub fn is_enabled(self) -> bool {
        self == Self::Enabled
    }
}

/// The activation being decided.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum UpgradeActivation {
    /// A stopped sandbox starts: its last execution's environment is the one
    /// it would boot under.
    Start,
    /// A suspended sandbox resumes: its checkpoint's producer is the one that
    /// would load it.
    Restore,
}

impl UpgradeActivation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Restore => "restore",
        }
    }
}

/// An execution as the control plane recorded it: the binding, when the
/// record has one, and the legacy identity columns otherwise.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecordedExecution {
    /// `execution_metadata.environment`, unparsed.
    pub environment: Option<String>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    /// The version of the record's kernel component.
    pub kernel_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

/// One row of the environment catalog, `runtime_environments`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogEnvironment {
    pub reference: EnvironmentReference,
    pub runtime: String,
    pub runtime_version: String,
    pub kernel_version: String,
    pub state_format_name: String,
    pub state_format_version: String,
    /// Environments this one can be cold-boot upgraded to.
    pub upgrade_edges: BTreeSet<EnvironmentReference>,
    /// Upgrade edges whose target also loads this one's checkpoints.
    pub memory_edges: BTreeSet<EnvironmentReference>,
}

/// The environment `recorded` ran under, as far as the control plane can tell
/// without the node's boot-component digests.
///
/// A bound record names its environment. An unbound one is attributed as
/// narrowly as the control plane can approach node-manager, which applies an
/// environment's eligibility to unbound work only when that work matches the
/// environment's definition exactly, boot-component digests included, and
/// runs any other unbound record as a historical contract while the cluster
/// permits legacy unbound work. The catalog carries no digests, so an unbound
/// record counts only when it carries every identity field and they match
/// exactly one catalog environment. A record that matches here but not on the
/// node fails closed there: the node's upgrade finds no certified edge from
/// it and refuses before the fence. A record from before
/// execution metadata, with no runtime or state-format version, counts as the
/// stock E0 identity (Firecracker 1.15.1 with format 9.0.0, the
/// `INITIAL_FIRECRACKER_*` constants) only when `legacy_unbound_allowed` is
/// `Some(false)`, where node-manager admits such a checkpoint only as E0; a
/// kernel the record names must match too, and none is assumed. Everything
/// else is attributed to nothing: the activation proceeds as recorded and the
/// node decides, as it would without this module.
pub fn attribute(
    recorded: &RecordedExecution,
    catalog: &[CatalogEnvironment],
    legacy_unbound_allowed: Option<bool>,
) -> Option<EnvironmentReference> {
    if let Some(environment) = &recorded.environment {
        return environment.parse().ok();
    }
    if let (
        Some(runtime),
        Some(runtime_version),
        Some(kernel_version),
        Some(state_format_name),
        Some(state_format_version),
    ) = (
        &recorded.runtime,
        &recorded.runtime_version,
        &recorded.kernel_version,
        &recorded.state_format_name,
        &recorded.state_format_version,
    ) {
        return only_match(catalog, |environment| {
            &environment.runtime == runtime
                && &environment.runtime_version == runtime_version
                && &environment.kernel_version == kernel_version
                && &environment.state_format_name == state_format_name
                && &environment.state_format_version == state_format_version
        });
    }
    let pre_metadata =
        recorded.runtime_version.is_none() && recorded.state_format_version.is_none();
    if !pre_metadata || legacy_unbound_allowed != Some(false) {
        return None;
    }
    let agrees =
        |recorded: Option<&str>, catalog: &str| recorded.is_none_or(|value| value == catalog);
    only_match(catalog, |environment| {
        environment.runtime_version == INITIAL_FIRECRACKER_RUNTIME_VERSION
            && environment.state_format_version == INITIAL_FIRECRACKER_STATE_FORMAT_VERSION
            && agrees(recorded.runtime.as_deref(), &environment.runtime)
            && agrees(
                recorded.kernel_version.as_deref(),
                &environment.kernel_version,
            )
            && agrees(
                recorded.state_format_name.as_deref(),
                &environment.state_format_name,
            )
    })
}

/// The one catalog environment `matches` accepts, if exactly one does.
fn only_match(
    catalog: &[CatalogEnvironment],
    matches: impl Fn(&CatalogEnvironment) -> bool,
) -> Option<EnvironmentReference> {
    let mut found = catalog.iter().filter(|environment| matches(environment));
    let first = found.next()?;
    found.next().is_none().then_some(first.reference)
}

/// Why an environment's sandboxes are moved off it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum UpgradeCause {
    /// The environment is `blocked`.
    Blocked,
    /// The environment has no state while the cluster has a default, so no
    /// node executes it. Only stopped sandboxes move: nothing an operator
    /// wrote names the environment, so nothing consents to losing a
    /// suspended sandbox's memory.
    Unlisted,
    /// The environment is `deprecated`; only stopped sandboxes move.
    Deprecated,
}

impl UpgradeCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Blocked => "blocked",
            Self::Unlisted => "unlisted",
            Self::Deprecated => "deprecated",
        }
    }
}

impl fmt::Display for UpgradeCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A move from `from` to the cluster default `to`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnvironmentMove {
    pub from: EnvironmentReference,
    pub to: EnvironmentReference,
    pub cause: UpgradeCause,
}

/// What an activation does about its runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimePlan {
    /// Start or restore under the recorded environment.
    AsRecorded,
    /// Restore as usual; a memory edge lets the node load the checkpoint
    /// under the default.
    RestoreOnDefault(EnvironmentMove),
    /// Cold-boot the disk under the default by the node's runtime upgrade.
    ColdBootUpgrade(EnvironmentMove),
    /// No certified edge reaches the default from an environment that may not
    /// execute: refuse the activation.
    Stranded(EnvironmentMove),
}

impl RuntimePlan {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AsRecorded => "as_recorded",
            Self::RestoreOnDefault(_) => "restore_on_default",
            Self::ColdBootUpgrade(_) => "cold_boot_upgrade",
            Self::Stranded(_) => "stranded",
        }
    }
}

/// Decide what `activation` does about a sandbox that last ran under `from`.
/// `from_catalog` is `from`'s catalog row, whose edges are the certified ones;
/// an environment missing from the catalog has none.
pub fn decide(
    policy: &RuntimeEnvironmentPolicy,
    activation: UpgradeActivation,
    from: EnvironmentReference,
    from_catalog: Option<&CatalogEnvironment>,
) -> RuntimePlan {
    // Without a default the policy narrows nothing on any node.
    let Some(to) = policy.default_environment else {
        return RuntimePlan::AsRecorded;
    };
    // The default is always `allowed`, which the store and the policy's
    // foreign key both enforce, so a sandbox on it stays.
    let cause = match policy.environments.get(&from) {
        Some(RuntimeEnvironmentState::Allowed) => return RuntimePlan::AsRecorded,
        // A deprecated environment still executes: a suspended sandbox
        // restores on it, keeping its memory.
        Some(RuntimeEnvironmentState::Deprecated) if activation == UpgradeActivation::Restore => {
            return RuntimePlan::AsRecorded;
        }
        Some(RuntimeEnvironmentState::Deprecated) => UpgradeCause::Deprecated,
        Some(RuntimeEnvironmentState::Blocked) => UpgradeCause::Blocked,
        None => UpgradeCause::Unlisted,
    };
    let to_default = EnvironmentMove { from, to, cause };
    let memory_edge =
        from_catalog.is_some_and(|environment| environment.memory_edges.contains(&to));
    let upgrade_edge =
        from_catalog.is_some_and(|environment| environment.upgrade_edges.contains(&to));
    if activation == UpgradeActivation::Restore && memory_edge {
        RuntimePlan::RestoreOnDefault(to_default)
    } else if upgrade_edge
        // A cold boot discards a suspended sandbox's memory, which only an
        // operator's block of the environment consents to. Without one, the
        // sandbox is stranded until the operator allows its environment or
        // blocks it.
        && (activation == UpgradeActivation::Start || cause == UpgradeCause::Blocked)
    {
        RuntimePlan::ColdBootUpgrade(to_default)
    } else if cause == UpgradeCause::Deprecated {
        // Still executes; without an edge it starts where it is.
        RuntimePlan::AsRecorded
    } else {
        RuntimePlan::Stranded(to_default)
    }
}

/// Names one upgrade so a retry reuses it: the same sandbox, move, cause and
/// activation subject always name the same operation, and anything that
/// changes what is upgraded (a newer checkpoint, a later execution) or the
/// authorization it runs under names a new one.
const OPERATION_NAMESPACE: uuid::Uuid = uuid::uuid!("2f1d8f0e-6c0b-4c4b-9a37-9c4f1d3b7e52");

impl EnvironmentMove {
    /// What the upgrade runs under: the environment's state. A suspended
    /// sandbox's memory is lost only under a block, the audited admin write
    /// that names the environment. A missing state names nothing, since
    /// setting the first default leaves every other environment without one,
    /// and a deprecated environment moves only stopped sandboxes, so neither
    /// consents to memory loss.
    pub fn authorization(
        &self,
        activation: UpgradeActivation,
    ) -> pplx_space_sbx_mgr_client::proto::RuntimeUpgradeAuthorization {
        pplx_space_sbx_mgr_client::proto::RuntimeUpgradeAuthorization {
            authorization_id: format!("runtime-environment-{}:{}", self.cause, self.from),
            reason: format!(
                "environment {} is {}; {} moves to the cluster default {}",
                self.from,
                self.cause,
                activation.as_str(),
                self.to
            ),
            memory_loss: activation == UpgradeActivation::Restore
                && self.cause == UpgradeCause::Blocked,
        }
    }

    /// The operation id for upgrading `sandbox_id` at `activation`, where
    /// `subject` identifies what is booted: the checkpoint for a restore,
    /// the recorded execution for a start.
    pub fn operation_id(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        subject: &str,
    ) -> uuid::Uuid {
        uuid::Uuid::new_v5(
            &OPERATION_NAMESPACE,
            // The cause names the authorization, which a retry must repeat.
            format!(
                "{sandbox_id}/{}/{}/{}/{}/{subject}",
                activation.as_str(),
                self.cause,
                self.from,
                self.to
            )
            .as_bytes(),
        )
    }

    /// The `WakeRequest.runtime_upgrade` that asks the node for this move.
    /// `checkpoint` is the local artifact of the checkpoint a restore was
    /// decided from, which the node must find pinned to boot it; a restore
    /// always names one.
    pub fn wake_upgrade(
        &self,
        sandbox_id: SandboxId,
        activation: UpgradeActivation,
        subject: &str,
        checkpoint: Option<LocalArtifactId>,
    ) -> pplx_space_sbx_mgr_client::proto::WakeRuntimeUpgrade {
        pplx_space_sbx_mgr_client::proto::WakeRuntimeUpgrade {
            operation_id: self
                .operation_id(sandbox_id, activation, subject)
                .to_string(),
            target_environment: self.to.to_string(),
            authorization: Some(self.authorization(activation)),
            checkpoint_local_artifact_id: checkpoint.map(|checkpoint| checkpoint.to_string()),
        }
    }
}

/// The refusal for a stranded sandbox. Not retryable: it holds until an
/// operator allows the environment, certifies an edge or changes the
/// default.
pub fn stranded_error(sandbox_id: SandboxId, stranded: EnvironmentMove) -> super::SubstrateError {
    super::SubstrateError::FailedPrecondition {
        message: format!(
            "sandbox {sandbox_id} last ran under execution environment {}, which is {} and has \
             no certified upgrade edge to the cluster default {}; an operator must allow the \
             environment or certify an edge",
            stranded.from, stranded.cause, stranded.to
        ),
    }
}

/// How long a policy read is reused. Activations read the policy on every
/// resume and start, which a short reuse keeps off the database; a policy
/// change reaches activations within this long, and nodes enforce it on
/// their own regardless.
pub const POLICY_CACHE_TTL: Duration = Duration::from_secs(5);

/// A policy and when it was read.
type CachedPolicy = (tokio::time::Instant, Arc<RuntimeEnvironmentPolicy>);

/// The policy, read through a short-lived cache shared by a substrate's
/// clones.
#[derive(Clone, Debug, Default)]
pub struct RuntimePolicyCache {
    cached: Arc<tokio::sync::Mutex<Option<CachedPolicy>>>,
}

impl RuntimePolicyCache {
    /// The cached policy, or one read through `read` once it's older than
    /// [`POLICY_CACHE_TTL`]. A failed read caches nothing.
    pub async fn get<F, Fut, E>(&self, read: F) -> Result<Arc<RuntimeEnvironmentPolicy>, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<RuntimeEnvironmentPolicy, E>>,
    {
        // Held across the read, so concurrent activations share one.
        let mut cached = self.cached.lock().await;
        if let Some((read_at, policy)) = cached.as_ref()
            && read_at.elapsed() < POLICY_CACHE_TTL
        {
            return Ok(Arc::clone(policy));
        }
        let policy = Arc::new(read().await?);
        *cached = Some((tokio::time::Instant::now(), Arc::clone(&policy)));
        Ok(policy)
    }
}
