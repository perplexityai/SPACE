//! A companion binding preserves the original execution-identity file for
//! rollback readers. It is launch intent, not independent sandbox authority.
use super::*;
use pplx_space_execution_spec::EnvironmentReference;

impl ExecutionStore {
    pub(crate) fn runtime_environment(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
    ) -> io::Result<Option<EnvironmentReference>> {
        let Some(_directory) = self.directory(false)? else {
            return Ok(None);
        };
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(self.root.join(format!("{sandbox}.environment")))
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid runtime environment binding",
            )
        };
        let metadata = file.metadata()?;
        // ENV1 + execution UUID + canonical sha256 environment reference.
        if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() != 111 {
            return Err(invalid());
        }
        let mut bytes = [0; 111];
        file.read_exact(&mut bytes)?;
        if &bytes[..4] != b"ENV1" {
            return Err(invalid());
        }
        let identity: SandboxActivationAttemptId = std::str::from_utf8(&bytes[4..40])
            .map_err(|_| invalid())?
            .parse()
            .map_err(|_| invalid())?;
        if identity != execution {
            return Ok(None);
        }
        std::str::from_utf8(&bytes[40..])
            .map_err(|_| invalid())?
            .parse()
            .map(Some)
            .map_err(|_| invalid())
    }

    /// Serialize with launch under the execution read lock and launch lock.
    /// Never let a retry relabel an already resident runtime as another profile.
    pub(crate) fn record_runtime_environment(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
        environment: EnvironmentReference,
    ) -> io::Result<()> {
        if self.read(sandbox)? != Some(execution) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "execution changed",
            ));
        }
        if let Some(previous) = self.runtime_environment(sandbox, execution)? {
            return if previous == environment {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "runtime environment changed",
                ))
            };
        }
        let directory = self
            .directory(true)?
            .ok_or_else(|| io::Error::other("execution store disappeared"))?;
        let temporary = self.root.join(format!(".{sandbox}.environment.new"));
        match fs::remove_file(&temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temporary)?;
        file.write_all(b"ENV1")?;
        file.write_all(execution.to_string().as_bytes())?;
        file.write_all(environment.to_string().as_bytes())?;
        file.sync_all()?;
        fs::rename(temporary, self.root.join(format!("{sandbox}.environment")))?;
        directory.sync_all()
    }
}
