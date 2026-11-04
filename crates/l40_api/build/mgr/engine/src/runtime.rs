//! Build-owned systemd compilation. All guest file I/O uses the existing SSH channel.
use crate::{
    BuildError,
    recipe::{Origin, Recipe, Step},
    ssh::{SshError, SshSession},
};
use pplx_space_common_core::types::TemplateRuntime;

mod render;
use render::{Files, MAX_BUNDLE_BYTES};
const READ: &str = include_str!("runtime/read.sh");
const CHECK: &str = include_str!("runtime/check.sh");
const CLEAR: &str = include_str!("runtime/clear.sh");
const INSTALL: &str = include_str!("runtime/install.sh");

pub(crate) struct RuntimeBuild {
    expected: Files,
    archive: Vec<u8>,
    managed: bool,
}
impl RuntimeBuild {
    pub async fn prepare(ssh: &SshSession, recipe: &Recipe) -> Result<Self, BuildError> {
        // OCI origins import no apps, even if their selected guest bundle has them.
        if matches!(recipe.origin, Origin::FromOciImage { .. }) {
            ssh.exchange(CLEAR, &[], 0).await?;
        }
        let parent = Self::read(ssh).await?;
        let edit = recipe.steps.last().map(|step| &step.step);
        let managed =
            !parent.is_empty() || matches!(edit, Some(Step::SetRuntime(_) | Step::ClearRuntime));
        if managed {
            ssh.exchange(CHECK, &[], 0).await?;
        }
        if !parent.is_empty() {
            let declaration = parent
                .get("runtime.toml")
                .ok_or_else(|| BuildError::message("runtime declaration missing"))?;
            let runtime = TemplateRuntime::from_bytes(declaration)
                .map_err(|e| BuildError::message(e.to_string()))?;
            let mut names = runtime
                .apps()
                .iter()
                .map(|app| app.name().unit())
                .collect::<Vec<_>>();
            names.extend(["runtime.toml".into(), "space-apps.target".into()]);
            names.sort();
            // Inheritance preserves the parent's policy bytes across builder
            // upgrades; only SetRuntime compiles with the current renderer.
            if !parent.keys().eq(names.iter()) {
                return Err(BuildError::message(
                    "runtime files changed outside recipe; rebuild from recipe",
                ));
            }
        }
        let expected = match edit {
            Some(Step::SetRuntime(runtime)) => render::render(runtime).map_err(runtime_io)?,
            Some(Step::ClearRuntime) => Files::new(),
            _ => parent,
        };
        let archive = render::archive(&expected).map_err(runtime_io)?;
        Ok(Self {
            expected,
            archive,
            managed,
        })
    }
    async fn read(ssh: &SshSession) -> Result<Files, BuildError> {
        let bytes = ssh.exchange(READ, &[], MAX_BUNDLE_BYTES).await?;
        if bytes.is_empty() {
            return Ok(Files::new());
        }
        render::read_archive(&bytes).map_err(runtime_io)
    }
    pub async fn apply_step(&self, ssh: &SshSession, step: &Step) -> Result<(), SshError> {
        match step {
            Step::SetRuntime(_) => {
                ssh.exchange(INSTALL, &self.archive, 0).await?;
            }
            Step::ClearRuntime => {
                ssh.exchange(CLEAR, &[], 0).await?;
            }
            _ => {}
        }
        Ok(())
    }
    pub async fn finalize(&self, ssh: &SshSession) -> Result<(), BuildError> {
        if Self::read(ssh).await? != self.expected {
            return Err(BuildError::message(
                "runtime files changed outside recipe; use SetRuntime or ClearRuntime",
            ));
        }
        if self.managed {
            ssh.exchange(CHECK, &[], 0).await?;
        }
        Ok(())
    }
    pub fn requires_durable_stop(&self) -> bool {
        self.managed
    }
}

#[cfg(test)]
mod tests;

fn runtime_io(source: std::io::Error) -> BuildError {
    BuildError::Io {
        operation: "compile or inspect runtime units",
        source,
    }
}
