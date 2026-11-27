use super::TemplateName;
use std::{fmt, str::FromStr};

/// Templates whose recipes are maintained and shipped with SPACE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub enum SystemTemplate {
    Seed,
    CodeRuntime,
}

impl SystemTemplate {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::CodeRuntime => "code-runtime",
        }
    }
}

impl fmt::Display for SystemTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unsupported system template {0:?}; expected seed or code-runtime")]
pub struct SystemTemplateParseError(String);

impl FromStr for SystemTemplate {
    type Err = SystemTemplateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "seed" => Ok(Self::Seed),
            "code-runtime" => Ok(Self::CodeRuntime),
            _ => Err(SystemTemplateParseError(value.to_string())),
        }
    }
}

impl From<SystemTemplate> for TemplateName {
    fn from(template: SystemTemplate) -> Self {
        Self::parse(template.as_str()).expect("system template name is valid")
    }
}

#[cfg(test)]
mod tests;
