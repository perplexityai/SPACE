//! Native template startup intent, compiled by the builder into rootfs units.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{EnvMap, GuestPath, GuestUserName, ProcessCommand};

pub const TEMPLATE_RUNTIME_BUILD_MASK: &str = "systemd.mask=space-apps.target";
pub const MAX_TEMPLATE_RUNTIME_BYTES: usize = 256 * 1024;
pub const TEMPLATE_RUNTIME_TIMEOUT: Duration = Duration::from_secs(60);

/// Commands and environment values are deliberately redacted in Debug.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "AppDeclaration", into = "AppDeclaration")]
pub struct RuntimeApp {
    name: RuntimeAppName,
    command: ProcessCommand,
    user: GuestUserName,
    working_directory: GuestPath,
    env: EnvMap,
    readiness: RuntimeReadiness,
    startup_timeout: Duration,
    startup_policy: RuntimeStartupPolicy,
}

/// Whether an app is required for this template. Currently enforced only during
/// base-snapshot preparation; sandbox lifecycle readiness remains agent-only.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeStartupPolicy {
    #[default]
    BestEffort,
    Required,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeReadiness {
    Launch,
    Exec(ProcessCommand),
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateRuntimeError {
    #[error("runtime declaration exceeds the size limit")]
    TooLarge,
    // serde errors can include tenant input. Keep it out of generic error logs.
    #[error("invalid runtime declaration encoding or schema")]
    Encoding,
    #[error("runtime requires 1 to 16 uniquely named apps")]
    Apps,
    #[error(
        "app name must be 1 to 64 lowercase ASCII letters, digits, hyphens or underscores, starting with a letter"
    )]
    Name,
    #[error("unsupported runtime declaration schema")]
    Schema,
    #[error("invalid runtime command")]
    Command,
    #[error("runtime working_directory must be absolute")]
    WorkingDirectory,
    #[error("runtime startup_timeout_secs must be between 1 and 60")]
    Timeout,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppDeclaration {
    name: RuntimeAppName,
    command: Vec<String>,
    user: GuestUserName,
    working_directory: String,
    #[serde(default)]
    env: EnvMap,
    readiness: Readiness,
    startup_timeout_secs: u64,
    #[serde(default)]
    startup_policy: RuntimeStartupPolicy,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Readiness {
    Launch,
    Exec { command: Vec<String> },
}

impl TryFrom<AppDeclaration> for RuntimeApp {
    type Error = TemplateRuntimeError;

    fn try_from(value: AppDeclaration) -> Result<Self, Self::Error> {
        if !value.working_directory.starts_with('/') {
            return Err(TemplateRuntimeError::WorkingDirectory);
        }
        if !(1..=TEMPLATE_RUNTIME_TIMEOUT.as_secs()).contains(&value.startup_timeout_secs) {
            return Err(TemplateRuntimeError::Timeout);
        }
        Ok(Self {
            name: value.name,
            command: ProcessCommand::new(value.command)
                .map_err(|_| TemplateRuntimeError::Command)?,
            user: value.user,
            working_directory: GuestPath::parse(value.working_directory)
                .map_err(|_| TemplateRuntimeError::WorkingDirectory)?,
            env: value.env,
            readiness: match value.readiness {
                Readiness::Launch => RuntimeReadiness::Launch,
                Readiness::Exec { command } => RuntimeReadiness::Exec(
                    ProcessCommand::new(command).map_err(|_| TemplateRuntimeError::Command)?,
                ),
            },
            startup_timeout: Duration::from_secs(value.startup_timeout_secs),
            startup_policy: value.startup_policy,
        })
    }
}

impl From<RuntimeApp> for AppDeclaration {
    fn from(value: RuntimeApp) -> Self {
        Self {
            name: value.name,
            command: value.command.argv().to_vec(),
            user: value.user,
            working_directory: value.working_directory.expose().to_owned(),
            env: value.env,
            readiness: match value.readiness {
                RuntimeReadiness::Launch => Readiness::Launch,
                RuntimeReadiness::Exec(command) => Readiness::Exec {
                    command: command.argv().to_vec(),
                },
            },
            startup_timeout_secs: value.startup_timeout.as_secs(),
            startup_policy: value.startup_policy,
        }
    }
}

/// A template replaces or inherits its complete app set as one declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Declaration", into = "Declaration")]
pub struct TemplateRuntime {
    apps: Vec<RuntimeApp>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Declaration {
    schema_version: u32,
    apps: Vec<RuntimeApp>,
}

impl TryFrom<Declaration> for TemplateRuntime {
    type Error = TemplateRuntimeError;
    fn try_from(value: Declaration) -> Result<Self, Self::Error> {
        if !matches!(value.schema_version, 1 | 2) {
            return Err(TemplateRuntimeError::Schema);
        }
        let names: std::collections::BTreeSet<_> =
            value.apps.iter().map(|app| app.name()).collect();
        if !(1..=16).contains(&value.apps.len()) || names.len() != value.apps.len() {
            return Err(TemplateRuntimeError::Apps);
        }
        Ok(Self { apps: value.apps })
    }
}
impl From<TemplateRuntime> for Declaration {
    fn from(value: TemplateRuntime) -> Self {
        Self {
            schema_version: value.schema_version(),
            apps: value.apps,
        }
    }
}

/// Restricted names are safe verbatim systemd instances and directory components.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RuntimeAppName(String);
impl RuntimeAppName {
    pub fn parse(value: impl Into<String>) -> Result<Self, TemplateRuntimeError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
            || !value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        {
            return Err(TemplateRuntimeError::Name);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn unit(&self) -> String {
        format!("space-runtime@{}.service", self.0)
    }
}
impl TryFrom<String> for RuntimeAppName {
    type Error = TemplateRuntimeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}
impl TryFrom<&str> for RuntimeAppName {
    type Error = TemplateRuntimeError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}
impl std::str::FromStr for RuntimeAppName {
    type Err = TemplateRuntimeError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}
impl From<RuntimeAppName> for String {
    fn from(value: RuntimeAppName) -> Self {
        value.0
    }
}

impl TemplateRuntime {
    /// Older builders must reject required-app intent instead of dropping it.
    pub fn schema_version(&self) -> u32 {
        if self
            .apps
            .iter()
            .any(|app| app.startup_policy() == RuntimeStartupPolicy::Required)
        {
            2
        } else {
            1
        }
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TemplateRuntimeError> {
        if bytes.len() > MAX_TEMPLATE_RUNTIME_BYTES {
            return Err(TemplateRuntimeError::TooLarge);
        }
        let text = std::str::from_utf8(bytes).map_err(|_| TemplateRuntimeError::Encoding)?;
        toml::from_str(text).map_err(|_| TemplateRuntimeError::Encoding)
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, TemplateRuntimeError> {
        let bytes = toml::to_string(self)
            .map_err(|_| TemplateRuntimeError::Encoding)?
            .into_bytes();
        if bytes.len() > MAX_TEMPLATE_RUNTIME_BYTES {
            return Err(TemplateRuntimeError::TooLarge);
        }
        Ok(bytes)
    }
    pub fn apps(&self) -> &[RuntimeApp] {
        &self.apps
    }
}

impl RuntimeApp {
    pub fn name(&self) -> &RuntimeAppName {
        &self.name
    }
    pub fn command(&self) -> &ProcessCommand {
        &self.command
    }
    pub fn user(&self) -> &GuestUserName {
        &self.user
    }
    pub fn working_directory(&self) -> &GuestPath {
        &self.working_directory
    }
    pub fn env(&self) -> &EnvMap {
        &self.env
    }
    pub fn readiness(&self) -> &RuntimeReadiness {
        &self.readiness
    }
    pub fn startup_policy(&self) -> RuntimeStartupPolicy {
        self.startup_policy
    }
    pub fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }
}

#[cfg(test)]
mod tests;
