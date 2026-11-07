//! Sparse materialization vocabulary for decompressed artifacts: the rollout
//! mode a transfer requests and the layout a replica ends up with.

use std::fmt;
use std::str::FromStr;

/// Rollout mode for sparse materialization of decompressed artifacts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationMode {
    #[default]
    Disabled,
    Shadow,
    Enabled,
}

impl MaterializationMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Shadow => "shadow",
            Self::Enabled => "enabled",
        }
    }

    pub const fn is_disabled(mode: &Self) -> bool {
        matches!(mode, Self::Disabled)
    }

    pub const fn dedup_suffix(self) -> &'static str {
        match self {
            Self::Disabled => "",
            Self::Shadow => "\0shadow",
            Self::Enabled => "\0enabled",
        }
    }
}

impl fmt::Display for MaterializationMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for MaterializationMode {
    type Err = MaterializationModeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "shadow" => Ok(Self::Shadow),
            "enabled" => Ok(Self::Enabled),
            _ => Err(MaterializationModeParseError {
                value: value.to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("materialization mode must be disabled, shadow, or enabled; got {value:?}")]
pub struct MaterializationModeParseError {
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationLayout {
    Dense,
    Sparse,
}

impl MaterializationLayout {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dense => "dense",
            Self::Sparse => "sparse",
        }
    }
}

impl fmt::Display for MaterializationLayout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests;
