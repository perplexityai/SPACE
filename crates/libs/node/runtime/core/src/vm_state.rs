//! Resident execution states consumed by lifecycle reconciliation. An adapter
//! must report an observation failure rather than invent a state it cannot read.

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VmState {
    NotStarted,
    Running,
    Paused,
}

impl VmState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "Not started",
            Self::Running => "Running",
            Self::Paused => "Paused",
        }
    }
}

impl fmt::Display for VmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
