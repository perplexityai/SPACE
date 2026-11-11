//! Shared native recipe boundary. The core declaration owns validation.
use crate::proto::recipe::{RuntimeApp, SetRuntime, runtime_app};
use pplx_space_common_core::types::{
    RuntimeReadiness, RuntimeStartupPolicy, TemplateRuntime, TemplateRuntimeError,
};

impl std::fmt::Debug for SetRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetRuntime").finish_non_exhaustive()
    }
}
impl std::fmt::Debug for RuntimeApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeApp")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for runtime_app::ExecReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecReady").finish_non_exhaustive()
    }
}

pub fn encode(runtime: &TemplateRuntime) -> SetRuntime {
    SetRuntime {
        schema_version: runtime.schema_version(),
        apps: runtime
            .apps()
            .iter()
            .map(|runtime| RuntimeApp {
                name: runtime.name().as_str().to_owned(),
                command: runtime.command().argv().to_vec(),
                user: runtime.user().as_str().to_owned(),
                working_directory: runtime.working_directory().expose().to_owned(),
                env: runtime
                    .env()
                    .as_map()
                    .iter()
                    .map(|(key, value)| (key.as_str().to_owned(), value.expose().to_owned()))
                    .collect(),
                readiness: Some(match runtime.readiness() {
                    RuntimeReadiness::Launch => {
                        runtime_app::Readiness::Launch(runtime_app::LaunchReady {})
                    }
                    RuntimeReadiness::Exec(command) => {
                        runtime_app::Readiness::Exec(runtime_app::ExecReady {
                            command: command.argv().to_vec(),
                        })
                    }
                }),
                startup_timeout_secs: runtime.startup_timeout().as_secs(),
                startup_policy: match runtime.startup_policy() {
                    RuntimeStartupPolicy::BestEffort => {
                        runtime_app::StartupPolicy::BestEffort as i32
                    }
                    RuntimeStartupPolicy::Required => runtime_app::StartupPolicy::Required as i32,
                },
            })
            .collect(),
    }
}

pub fn decode(runtime: SetRuntime) -> Result<TemplateRuntime, TemplateRuntimeError> {
    let apps = runtime
        .apps
        .into_iter()
        .map(|app| {
            let startup_policy = match runtime_app::StartupPolicy::try_from(app.startup_policy)
                .map_err(|_| TemplateRuntimeError::Encoding)?
            {
                runtime_app::StartupPolicy::BestEffort => "best-effort",
                runtime_app::StartupPolicy::Required => "required",
            };
            let readiness = match app.readiness.ok_or(TemplateRuntimeError::Command)? {
                runtime_app::Readiness::Launch(_) => serde_json::json!({"type": "launch"}),
                runtime_app::Readiness::Exec(probe) => {
                    serde_json::json!({"type": "exec", "command": probe.command})
                }
            };
            Ok(serde_json::json!({
                "name": app.name, "command": app.command, "user": app.user,
                "working_directory": app.working_directory, "env": app.env,
                "readiness": readiness, "startup_timeout_secs": app.startup_timeout_secs,
                "startup_policy": startup_policy,
            }))
        })
        .collect::<Result<Vec<_>, TemplateRuntimeError>>()?;
    let result: TemplateRuntime = serde_json::from_value(serde_json::json!({
        "schema_version": runtime.schema_version, "apps": apps,
    }))
    .map_err(|_| TemplateRuntimeError::Encoding)?;
    result.canonical_bytes()?;
    Ok(result)
}

#[cfg(test)]
mod tests;
