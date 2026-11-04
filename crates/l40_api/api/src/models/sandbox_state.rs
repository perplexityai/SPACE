use pplx_space_common_core::types::{NodeId, SandboxLifecycle};

/// What the admin response's `lifecycle` and `status` and the ECW sandbox
/// state are rendered from: the row's stored lifecycle and the status its node
/// last observed, kept apart so each surface decides how the two combine.
/// Native `/v1` state does not read it: it renders the stored lifecycle alone
/// (`public_sandbox_projection`).
///
/// Both stay raw strings. Rows carry lifecycles outside `SandboxLifecycle`
/// (`deleted`, `cleanup_required`), and the renders read padded, mixed-case
/// and unknown node statuses as they come.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxStateInput {
    stored_lifecycle: Option<String>,
    observed_status: Option<String>,
    observation: Observation,
}

/// Whether the observed status describes the row's current execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Observation {
    #[default]
    CurrentExecution,
    /// Published by a node other than the row's active node, such as the
    /// node the sandbox left or the one it was lost on.
    OtherNode,
}

impl SandboxStateInput {
    /// `stored_lifecycle` is absent for a sandbox only a node reports;
    /// `observed_status` is absent without an observed record or when the
    /// record carries no status. The observation is taken to describe the
    /// row's current execution.
    pub fn new(stored_lifecycle: Option<String>, observed_status: Option<String>) -> Self {
        Self {
            stored_lifecycle,
            observed_status,
            observation: Observation::CurrentExecution,
        }
    }

    /// A row read with the status `observed_node` last published for it. The
    /// observation describes the row's current execution only when that node
    /// is the row's active node; the record carries no execution generation
    /// to compare.
    pub fn observed_on(
        stored_lifecycle: String,
        active_node: Option<NodeId>,
        observed_status: Option<String>,
        observed_node: Option<NodeId>,
    ) -> Self {
        let observation = match (active_node, observed_node) {
            (Some(active), Some(observed)) if active == observed => Observation::CurrentExecution,
            _ => Observation::OtherNode,
        };
        Self {
            stored_lifecycle: Some(stored_lifecycle),
            observed_status,
            observation,
        }
    }

    /// A row read without its node's observation.
    pub fn stored(lifecycle: impl Into<String>) -> Self {
        Self::new(Some(lifecycle.into()), None)
    }

    pub fn stored_lifecycle(&self) -> Option<&str> {
        self.stored_lifecycle.as_deref()
    }

    pub fn observed_status(&self) -> Option<&str> {
        self.observed_status.as_deref()
    }

    /// The lifecycle the admin response reports, in both `lifecycle` and
    /// `status`.
    pub fn reported_lifecycle(&self) -> Option<String> {
        // The node's observed status wins over the stored lifecycle.
        self.observed_status()
            .or(self.stored_lifecycle())
            .map(str::to_string)
    }

    /// Whether the row is a delete tombstone, `deleted` or `cleanup_required`:
    /// terminal, and outside `SandboxLifecycle`.
    pub fn is_tombstone(&self) -> bool {
        self.stored_lifecycle().is_some_and(is_tombstone)
    }

    /// The lifecycle the ECW renders read. The stored lifecycle decides every
    /// terminal and at-rest state (see `stored_lifecycle_decides`); the
    /// observed status refines any other state only when it describes the
    /// row's current execution, that is, when the row's active node published
    /// it.
    pub fn rendered_lifecycle(&self) -> Option<&str> {
        let stored = self.stored_lifecycle();
        if stored.is_some_and(stored_lifecycle_decides) {
            return stored;
        }
        match self.observation {
            Observation::CurrentExecution => self.observed_status().or(stored),
            Observation::OtherNode => stored,
        }
    }
}

/// Whether the stored lifecycle decides what ECW callers see, whatever the
/// node last observed: the terminal states (`failed`, `quarantined` and the
/// two delete tombstones) and the at-rest ones (`stopped`, `suspended`). The
/// control plane writes these, and a node still reporting a running execution
/// is usually behind the row. Not always: a `failed` row's VM can still run on
/// its active node. On Kubernetes the failed-sandbox cleanup
/// (`SPACE_FAILED_SANDBOX_CLEANUP_ENABLED`, on by default) can repair such a
/// row to `started` once a probe of its node finds the VM started; that repair
/// does not run on Direct. A start of a `stopped` sandbox can leave the row
/// `stopped` until its post-start stamp, which a start that fails after its
/// node started the sandbox does not write. Callers see the stored value
/// meanwhile. In-flight lifecycles, `deleting` and `stopping` included, keep
/// following the active node's observation.
fn stored_lifecycle_decides(stored: &str) -> bool {
    let normalized = stored.trim().to_ascii_lowercase();
    match normalized.parse::<SandboxLifecycle>() {
        Ok(
            SandboxLifecycle::Failed
            | SandboxLifecycle::Quarantined
            | SandboxLifecycle::Stopped
            | SandboxLifecycle::Suspended,
        ) => true,
        Ok(
            SandboxLifecycle::Created
            | SandboxLifecycle::Creating
            | SandboxLifecycle::Starting
            | SandboxLifecycle::Started
            | SandboxLifecycle::Restoring
            | SandboxLifecycle::Pausing
            | SandboxLifecycle::Paused
            | SandboxLifecycle::Resuming
            | SandboxLifecycle::Suspending
            | SandboxLifecycle::Stopping
            | SandboxLifecycle::Deleting
            | SandboxLifecycle::Checkpointing,
        ) => false,
        // An unknown value keeps reading the node.
        Err(_) => is_tombstone(&normalized),
    }
}

fn is_tombstone(stored: &str) -> bool {
    matches!(
        stored.trim().to_ascii_lowercase().as_str(),
        "deleted" | "cleanup_required"
    )
}

#[cfg(test)]
mod tests;
