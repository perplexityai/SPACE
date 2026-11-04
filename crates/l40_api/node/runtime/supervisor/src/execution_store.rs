//! Root-owned execution identities survive supervisor and node-manager restarts.
//! Identity changes take the runtime write lock; launch budgets additionally
//! serialize with updates through the Firecracker launch lock.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;

use pplx_space_common_core::types::{SandboxActivationAttemptId, SandboxId, VcpuCount};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct ExecutionStore {
    root: PathBuf,
}

impl Default for ExecutionStore {
    fn default() -> Self {
        Self {
            root: "/space/run/runtime-supervisor/executions".into(),
        }
    }
}

impl ExecutionStore {
    #[cfg(test)]
    pub(crate) fn at(root: PathBuf) -> Self {
        Self { root }
    }

    fn directory(&self, create: bool) -> io::Result<Option<File>> {
        if create {
            match fs::DirBuilder::new().mode(0o700).create(&self.root) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        let directory = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&self.root)
        {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound && !create => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = directory.metadata()?;
        if metadata.uid() != nix::unistd::Uid::effective().as_raw() || metadata.mode() & 0o077 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "execution store must be owned by the service and mode 0700",
            ));
        }
        Ok(Some(directory))
    }

    pub(crate) fn read(
        &self,
        sandbox: SandboxId,
    ) -> io::Result<Option<SandboxActivationAttemptId>> {
        let Some(_directory) = self.directory(false)? else {
            return Ok(None);
        };
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(self.root.join(sandbox.to_string()))
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() != 36 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid execution identity file",
            ));
        }
        let mut raw = String::new();
        file.take(37).read_to_string(&mut raw)?;
        raw.parse()
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    pub(crate) fn write(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
    ) -> io::Result<()> {
        let directory = self
            .directory(true)?
            .ok_or_else(|| io::Error::other("execution store disappeared"))?;
        // One fixed staging name per sandbox bounds interrupted writes. Only
        // this root-owned daemon can mutate the directory, under the same lock.
        let temporary = self.root.join(format!(".{sandbox}.new"));
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
        file.write_all(execution.to_string().as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, self.root.join(sandbox.to_string()))?;
        directory.sync_all()
    }

    /// The companion format leaves the original identity readable by older
    /// supervisors. Missing evidence never grants authority to raise CPU quota.
    pub(crate) fn cpu_budget(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
    ) -> io::Result<Option<VcpuCount>> {
        let Some(_directory) = self.directory(false)? else {
            return Ok(None);
        };
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(self.root.join(format!("{sandbox}.cpu")))
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() != 44 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid CPU budget file",
            ));
        }
        let mut bytes = [0; 44];
        file.read_exact(&mut bytes)?;
        if &bytes[..4] != b"CPU1" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown CPU budget format",
            ));
        }
        let identity: SandboxActivationAttemptId = std::str::from_utf8(&bytes[4..40])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if identity != execution {
            return Ok(None);
        }
        VcpuCount::try_from_u32(u32::from_be_bytes([
            bytes[40], bytes[41], bytes[42], bytes[43],
        ]))
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Publish before spawning and retain the budget on uncertain launch failure.
    /// A replay cannot rewrite it from a different launch request.
    pub(crate) fn record_cpu_budget(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
        budget: VcpuCount,
    ) -> io::Result<()> {
        if self.read(sandbox)? != Some(execution) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "execution changed",
            ));
        }
        if let Some(previous) = self.cpu_budget(sandbox, execution)? {
            return if budget <= previous {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "launch CPU budget exceeds the recorded execution budget",
                ))
            };
        }
        let directory = self
            .directory(true)?
            .ok_or_else(|| io::Error::other("execution store disappeared"))?;
        let temporary = self.root.join(format!(".{sandbox}.cpu.new"));
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
        file.write_all(b"CPU1")?;
        file.write_all(execution.to_string().as_bytes())?;
        file.write_all(&budget.get().get().to_be_bytes())?;
        file.sync_all()?;
        fs::rename(temporary, self.root.join(format!("{sandbox}.cpu")))?;
        directory.sync_all()
    }

    pub(crate) fn remove(&self, sandbox: SandboxId) -> io::Result<()> {
        let Some(directory) = self.directory(false)? else {
            return Ok(());
        };
        // Retirement already checked identity and quiescence under the write
        // lock, so it cannot erase a replacement execution's budget.
        for name in [format!(".{sandbox}.cpu.new"), format!("{sandbox}.cpu")] {
            match fs::remove_file(self.root.join(name)) {
                Ok(()) => directory.sync_all()?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        match fs::remove_file(self.root.join(sandbox.to_string())) {
            Ok(()) => directory.sync_all(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}
