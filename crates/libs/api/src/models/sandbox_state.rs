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

    /// The lifecycle a caller-facing state is rendered from. The stored
    /// lifecycle decides every terminal and at-rest state; the observed
    /// status refines only a live one, and only when it describes the row's
    /// current execution.
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

/// Whether a stored lifecycle is one the node's observation may not override:
/// the control plane put the row there, and a node still reporting the
/// execution it had is behind it.
fn stored_lifecycle_decides(stored: &str) -> bool {
    let normalized = stored.trim().to_ascii_lowercase();
    match normalized.parse::<SandboxLifecycle>() {
        Ok(
            SandboxLifecycle::Failed
            | SandboxLifecycle::Quarantined
            | SandboxLifecycle::Stopped
            | SandboxLifecycle::Suspended
            | SandboxLifecycle::Deleting,
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
