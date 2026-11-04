//! Root-owned launch evidence shared by the supervisor and node manager.
//! Never reconstruct it from a live numeric PID.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::SandboxId;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::launch::ProcessHandle;

pub use pplx_space_common_core::host::linux::gateway::LAUNCH_CAPABILITY_ENV as CAPABILITY_ENV;
const MAX_WITNESS_BYTES: u64 = 16 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum GatewayLaunchError {
    #[error("gateway launch evidence unavailable: {0}")]
    Io(#[from] io::Error),
    #[error("gateway launch evidence malformed")]
    Decode(#[from] serde_json::Error),
    #[error("gateway launch evidence does not match the caller; cycle the gateway")]
    Mismatch,
    #[error("gateway launch capability generation failed")]
    Random,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Incarnation {
    boot_id: String,
    start_ticks: u64,
    executable_device: u64,
    executable_inode: u64,
    network_namespace: u64,
    cgroup_namespace: u64,
}

#[derive(Serialize, Deserialize)]
struct Witness {
    sandbox_id: SandboxId,
    pid: u32,
    uid: u32,
    capability: String,
    incarnation: Incarnation,
}

impl Drop for Witness {
    fn drop(&mut self) {
        self.capability.zeroize();
    }
}

impl Witness {
    fn authorize(
        &self,
        sandbox_id: SandboxId,
        pid: u32,
        uid: u32,
        capability: &[u8],
    ) -> Result<(), GatewayLaunchError> {
        if self.sandbox_id != sandbox_id
            || self.pid != pid
            || self.uid != uid
            || capability.len() != 64
            || !bool::from(self.capability.as_bytes().ct_eq(capability))
        {
            return Err(GatewayLaunchError::Mismatch);
        }
        Ok(())
    }
}

/// Kept secret in memory and passed only in the isolated gateway's environment.
pub fn new_capability() -> Result<Zeroizing<String>, GatewayLaunchError> {
    let mut bytes = Zeroizing::new([0_u8; 32]);
    SystemRandom::new()
        .fill(bytes.as_mut())
        .map_err(|_| GatewayLaunchError::Random)?;
    Ok(Zeroizing::new(
        bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
    ))
}

fn incarnation(handle: &ProcessHandle) -> Result<Incarnation, GatewayLaunchError> {
    let proc = PathBuf::from(format!("/proc/{}", handle.pid()));
    let result = (|| {
        let stat = fs::read_to_string(proc.join("stat"))?;
        let start_ticks = stat
            .rsplit_once(')')
            .and_then(|(_, fields)| fields.split_whitespace().nth(19))
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid process start time")
            })?;
        let executable = fs::metadata(proc.join("exe"))?;
        Ok(Incarnation {
            boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")?,
            start_ticks,
            executable_device: executable.dev(),
            executable_inode: executable.ino(),
            network_namespace: fs::metadata(proc.join("ns/net"))?.ino(),
            // The textual cgroup path depends on the reader's namespace.
            // Membership is checked separately against the shared host mount.
            cgroup_namespace: fs::metadata(proc.join("ns/cgroup"))?.ino(),
        })
    })();
    handle
        .inspect_while_running(result)?
        .ok_or(GatewayLaunchError::Mismatch)
}

fn require_identity(
    handle: &ProcessHandle,
    sandbox_id: SandboxId,
    uid: u32,
) -> Result<(), GatewayLaunchError> {
    if handle.credentials_match(uid, uid)? != Some(true)
        || handle.program_matches(paths::NET_GATEWAY_SVC_BIN)? != Some(true)
        || !crate::cgroup::sidecar_cgroup_process_ids(&sandbox_id.to_string())?
            .contains(&handle.pid())
    {
        return Err(GatewayLaunchError::Mismatch);
    }
    Ok(())
}

fn root() -> Result<PathBuf, GatewayLaunchError> {
    // `/space` is host-mounted into node-manager; its private `/run` is not
    // shared with the host supervisor. Deployment and socket binding create
    // this parent before serving requests. Never repair an untrusted parent.
    let parent = Path::new(paths::RUNTIME_SUPERVISOR_STATE_DIR);
    let metadata = fs::symlink_metadata(parent)?;
    if !metadata.is_dir() || metadata.uid() != 0 || metadata.permissions().mode() & 0o022 != 0 {
        return Err(GatewayLaunchError::Mismatch);
    }
    let root = parent.join("gateway-launches");
    match fs::DirBuilder::new().mode(0o700).create(&root) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(&root)?;
    if !metadata.is_dir() || metadata.uid() != 0 || metadata.permissions().mode() & 0o077 != 0 {
        return Err(GatewayLaunchError::Mismatch);
    }
    Ok(root)
}

/// The caller must hold the supervisor's launch lock and the original child's
/// pidfd acquired before making that child eligible for reaping.
pub fn record(
    sandbox_id: SandboxId,
    handle: &ProcessHandle,
    uid: u32,
    capability: &str,
) -> Result<(), GatewayLaunchError> {
    require_identity(handle, sandbox_id, uid)?;
    let witness = Witness {
        sandbox_id,
        pid: handle.pid(),
        uid,
        capability: capability.to_owned(),
        incarnation: incarnation(handle)?,
    };
    let bytes = Zeroizing::new(serde_json::to_vec(&witness)?);
    let root = root()?;
    // The launch lock excludes live predecessors. Reclaim only this sandbox's
    // unfinished write, bounding crash leftovers to one temporary per identity.
    let temporary = root.join(format!("{sandbox_id}.pending"));
    remove_if_present(&temporary)?;
    let result = (|| -> Result<(), GatewayLaunchError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, root.join(sandbox_id.to_string()))?;
        File::open(&root)?.sync_all()?;
        Ok(())
    })();
    if result.is_err()
        && let Err(error) = fs::remove_file(temporary)
    {
        tracing::warn!(%error, "remove incomplete gateway launch evidence");
    }
    result
}

fn read_witness(path: &Path) -> Result<Witness, GatewayLaunchError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != 0
        || metadata.permissions().mode() & 0o077 != 0
        || metadata.len() > MAX_WITNESS_BYTES
    {
        return Err(GatewayLaunchError::Mismatch);
    }
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(MAX_WITNESS_BYTES + 1).read_to_end(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Authenticate against launch-time evidence, never against recaptured evidence.
pub fn validate(
    sandbox_id: SandboxId,
    pid: u32,
    uid: u32,
    capability: &[u8],
) -> Result<(), GatewayLaunchError> {
    let witness = read_witness(&root()?.join(sandbox_id.to_string()))?;
    witness.authorize(sandbox_id, pid, uid, capability)?;
    let handle = ProcessHandle::open(pid).map_err(|_| GatewayLaunchError::Mismatch)?;
    require_identity(&handle, sandbox_id, uid)?;
    if incarnation(&handle)? != witness.incarnation {
        return Err(GatewayLaunchError::Mismatch);
    }
    Ok(())
}

/// Existing processes without launch evidence must be cycled, not adopted by
/// minting fresh authority for whichever process currently owns their PID.
pub fn require_recorded(
    sandbox_id: SandboxId,
    pid: u32,
    uid: u32,
) -> Result<(), GatewayLaunchError> {
    let witness = read_witness(&root()?.join(sandbox_id.to_string()))?;
    validate(sandbox_id, pid, uid, witness.capability.as_bytes())
}

/// Called only while holding the supervisor's exclusive runtime lock, so a
/// predecessor's cleanup cannot unlink evidence for a replacement gateway.
pub fn remove(sandbox_id: SandboxId) -> Result<(), GatewayLaunchError> {
    let root = root()?;
    remove_if_present(&root.join(sandbox_id.to_string()))?;
    remove_if_present(&root.join(format!("{sandbox_id}.pending")))
}

fn remove_if_present(path: &Path) -> Result<(), GatewayLaunchError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests;
