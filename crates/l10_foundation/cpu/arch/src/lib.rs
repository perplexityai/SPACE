//! The CPU architecture a template is built for and a sandbox runs on, with
//! its canonical names and the aliases it parses from.
//!
//! It is its own crate because it is a generic value type: both the template
//! and the execution models in `pplx-space-common-core` name it, and keeping
//! it in either one would make the other depend on it.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// CPU architecture for templates and build outputs.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_cpu_arch::Arch;
/// assert!(matches!(Arch::Aarch64, Arch::Aarch64));
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Arch {
    X86_64,
    Aarch64,
}

impl Arch {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
            Self::Aarch64 => "aarch64",
        }
    }
}

impl std::fmt::Display for Arch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported CPU architecture '{0}'")]
pub struct ArchParseError(String);

impl std::str::FromStr for Arch {
    type Err = ArchParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "x86_64" | "amd64" => Ok(Self::X86_64),
            "aarch64" | "arm64" => Ok(Self::Aarch64),
            other => Err(ArchParseError(other.to_string())),
        }
    }
}
