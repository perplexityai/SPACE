use std::{fmt, str::FromStr};

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("invalid template desired state {0:?}")]
pub struct TemplateDesiredStateParseError(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemplateDesiredState {
    Present,
    Deleting,
    Deleted,
}

impl TemplateDesiredState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }

    pub const fn is_present(self) -> bool {
        matches!(self, Self::Present)
    }

    pub const fn is_absent_desired(self) -> bool {
        matches!(self, Self::Deleting | Self::Deleted)
    }
}

impl fmt::Display for TemplateDesiredState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TemplateDesiredState {
    type Err = TemplateDesiredStateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "present" => Ok(Self::Present),
            "deleting" => Ok(Self::Deleting),
            "deleted" => Ok(Self::Deleted),
            other => Err(TemplateDesiredStateParseError(other.to_string())),
        }
    }
}

impl TryFrom<String> for TemplateDesiredState {
    type Error = TemplateDesiredStateParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<TemplateDesiredState> for String {
    fn from(value: TemplateDesiredState) -> Self {
        value.as_str().to_string()
    }
}
