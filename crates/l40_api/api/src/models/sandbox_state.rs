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
}

impl SandboxStateInput {
    /// `stored_lifecycle` is absent for a sandbox only a node reports;
    /// `observed_status` is absent without an observed record or when the
    /// record carries no status.
    pub fn new(stored_lifecycle: Option<String>, observed_status: Option<String>) -> Self {
        Self {
            stored_lifecycle,
            observed_status,
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
}
