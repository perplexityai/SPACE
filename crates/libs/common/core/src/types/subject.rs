//! Subject identity for credential exchange and resource scope.
//!
//! A subject is the principal a request is performed on behalf of. Wire form
//! is `"<kind>:<id>"` — `"sandbox:<uuid>"` for sandbox-scoped work,
//! `"template:<name>"` for template-scoped work like volume-pull on behalf of
//! template-manager. Parsed once at the wire boundary and passed as a typed
//! value through internal call graphs.

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use pplx_space_sandbox_model::{SandboxId, SandboxIdParseError};
use pplx_space_template_model::{TemplateName, TemplateParseError};

/// Principal on whose behalf a credential exchange or transfer runs.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::SubjectId;
/// let s: SubjectId = "template:seed".parse().unwrap();
/// assert_eq!(s.to_string(), "template:seed");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum SubjectId {
    Sandbox(SandboxId),
    Template(TemplateName),
}

/// Closed-set tag for the kind half of a [`SubjectId`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SubjectKind {
    Sandbox,
    Template,
}

impl SubjectKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sandbox => "sandbox",
            Self::Template => "template",
        }
    }
}

impl fmt::Display for SubjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SubjectId {
    /// Returns which kind of subject this is.
    pub fn kind(&self) -> SubjectKind {
        match self {
            Self::Sandbox(_) => SubjectKind::Sandbox,
            Self::Template(_) => SubjectKind::Template,
        }
    }
}

impl fmt::Display for SubjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sandbox(id) => write!(f, "sandbox:{id}"),
            Self::Template(name) => write!(f, "template:{name}"),
        }
    }
}

/// Errors produced when parsing a [`SubjectId`] from its wire form.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum SubjectIdParseError {
    #[error("subject id must be of the form 'sandbox:<uuid>' or 'template:<name>'")]
    Malformed,
    #[error("unknown subject kind {kind:?}; expected 'sandbox' or 'template'")]
    UnknownKind { kind: String },
    #[error("invalid sandbox id: {0}")]
    Sandbox(#[from] SandboxIdParseError),
    #[error("invalid template name: {0}")]
    Template(#[from] TemplateParseError),
}

impl FromStr for SubjectId {
    type Err = SubjectIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (kind, body) = value
            .split_once(':')
            .ok_or(SubjectIdParseError::Malformed)?;
        if body.is_empty() {
            return Err(SubjectIdParseError::Malformed);
        }
        match kind {
            "sandbox" => Ok(Self::Sandbox(body.parse()?)),
            "template" => Ok(Self::Template(body.parse()?)),
            other => Err(SubjectIdParseError::UnknownKind {
                kind: other.to_string(),
            }),
        }
    }
}

impl TryFrom<String> for SubjectId {
    type Error = SubjectIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<SubjectId> for String {
    fn from(value: SubjectId) -> Self {
        value.to_string()
    }
}

#[cfg(test)]
mod tests;
