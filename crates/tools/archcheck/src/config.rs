//! `ARCHITECTURE.toml`: which rules run, their parameters, and each
//! behaviour's mode.
//!
//! Every table denies unknown keys so a typo or a section for a rule that does
//! not exist is a configuration error rather than a silently skipped check.

use std::fmt;

use serde::Deserialize;

pub const CONFIG_FILE: &str = "ARCHITECTURE.toml";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchitectureConfig {
    pub rules: RulesConfig,
}

/// One field per rule, named by the rule's id.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RulesConfig {
    pub file_size: FileSizeConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSizeConfig {
    /// Code lines allowed per production Rust file.
    pub limit: usize,
    pub no_crossing: BehaviourConfig,
    pub no_new_entries: BehaviourConfig,
    pub no_raised_caps: BehaviourConfig,
    pub listed_no_growth: BehaviourConfig,
    pub stale_entries: BehaviourConfig,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BehaviourConfig {
    pub mode: Mode,
}

/// Whether a behaviour's findings fail the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Reported, never fails the check.
    Advisory,
    /// Reported and fails the check.
    Required,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Advisory => "advisory",
            Self::Required => "required",
        }
    }

    pub fn is_required(self) -> bool {
        matches!(self, Self::Required)
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
