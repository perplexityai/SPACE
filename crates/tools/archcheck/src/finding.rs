use crate::config::Mode;

/// One reported problem. `path` is repository-relative with `/` separators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub rule: &'static str,
    pub behaviour: &'static str,
    pub path: String,
    /// The rest of the message after the path, such as `": 1200 code lines…"`.
    pub detail: String,
    pub mode: Mode,
    /// Where a pull request shows the finding.
    pub location: Location,
}

/// A repository-relative file and a 1-based line in it: line 1 of the file a
/// finding is about, or, for a baseline entry whose file is gone, the entry's
/// line in the baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub path: String,
    pub line: usize,
}

impl Finding {
    /// The path followed by the detail, as one sentence.
    pub fn message(&self) -> String {
        format!("{}{}", self.path, self.detail)
    }
}
