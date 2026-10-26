//! Device intent is written before spawn, including the CPU-only case. A lost
//! reply may replay that intent, but cannot replace it with different leases.
use super::*;
use pplx_space_execution_common::devices::DeviceExecution;

const MAX_RECORD_BYTES: u64 = 64 * 1024;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "format", deny_unknown_fields)]
enum DeviceRecord {
    #[serde(rename = "device_execution_v1")]
    V1 {
        execution_id: SandboxActivationAttemptId,
        devices: Option<DeviceExecution>,
    },
    #[serde(rename = "device_execution_stopped_v2")]
    Stopped {
        execution_id: SandboxActivationAttemptId,
        previous_execution_id: Option<SandboxActivationAttemptId>,
        devices: Option<DeviceExecution>,
    },
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl ExecutionStore {
    fn device_record(&self, sandbox: SandboxId) -> io::Result<Option<DeviceRecord>> {
        let Some(_directory) = self.directory(false)? else {
            return Ok(None);
        };
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(self.root.join(format!("{sandbox}.devices")))
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.len() > MAX_RECORD_BYTES
            || metadata.uid() != nix::unistd::Uid::effective().as_raw()
            || metadata.mode() & 0o077 != 0
        {
            return Err(invalid("unsafe device execution record"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_RECORD_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(invalid("oversized device execution record"));
        }
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Used before adoption, under the runtime/launch locks. Historical CPU
    /// processes may lack this journal; a device-bearing process may not.
    pub(crate) fn check_device_adoption(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
        devices: Option<&DeviceExecution>,
    ) -> io::Result<()> {
        match self.device_record(sandbox)? {
            Some(DeviceRecord::V1 {
                execution_id,
                devices: stored,
            }) if execution_id == execution && stored.as_ref() == devices => Ok(()),
            Some(DeviceRecord::Stopped {
                execution_id,
                previous_execution_id,
                devices: None,
            }) if devices.is_none()
                && (execution_id == execution || previous_execution_id == Some(execution)) =>
            {
                Ok(())
            }
            None if devices.is_none() => Ok(()),
            _ => Err(invalid(
                "runtime device identity is missing or differs from the recorded intent",
            )),
        }
    }

    /// The caller proves runtime quiescence before publishing a new intent.
    /// A predecessor's record can then be replaced after the execution fence
    /// has advanced; durable device ownership remains with node-manager.
    pub(crate) fn record_device_intent(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
        devices: Option<&DeviceExecution>,
    ) -> io::Result<()> {
        if self.read(sandbox)? != Some(execution) {
            return Err(invalid("execution changed"));
        }
        match self.device_record(sandbox)? {
            Some(DeviceRecord::V1 { execution_id, .. }) if execution_id == execution => {
                return self.check_device_adoption(sandbox, execution, devices);
            }
            Some(DeviceRecord::Stopped {
                execution_id,
                previous_execution_id,
                ..
            }) if execution_id == execution || previous_execution_id == Some(execution) => {
                // Keep the tombstone even for a CPU-only retry. Otherwise a
                // delayed device request could replace that retry's record.
                return self.check_device_adoption(sandbox, execution, devices);
            }
            _ => {}
        }
        self.write_device_record(
            sandbox,
            &DeviceRecord::V1 {
                execution_id: execution,
                devices: devices.cloned(),
            },
        )
    }

    /// Called under runtime write exclusion before stopping processes. A stop
    /// can overtake a queued launch before it writes intent. Fence that case too,
    /// and retain the predecessor during an interrupted execution bind.
    pub(crate) fn stop_device_execution(
        &self,
        sandbox: SandboxId,
        execution: SandboxActivationAttemptId,
    ) -> io::Result<()> {
        let current = self.read(sandbox)?;
        let devices = match self.device_record(sandbox)? {
            Some(DeviceRecord::V1 { devices, .. } | DeviceRecord::Stopped { devices, .. }) => {
                devices
            }
            None => None,
        };
        self.write_device_record(
            sandbox,
            &DeviceRecord::Stopped {
                execution_id: execution,
                previous_execution_id: current.filter(|previous| *previous != execution),
                devices,
            },
        )
    }

    fn write_device_record(&self, sandbox: SandboxId, record: &DeviceRecord) -> io::Result<()> {
        let bytes = serde_json::to_vec(record)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(invalid("oversized device execution record"));
        }
        let directory = self
            .directory(true)?
            .ok_or_else(|| io::Error::other("execution store disappeared"))?;
        let temporary = self.root.join(format!(".{sandbox}.devices.new"));
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
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(temporary, self.root.join(format!("{sandbox}.devices")))?;
        directory.sync_all()
    }
}
