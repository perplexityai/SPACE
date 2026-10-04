use std::num::NonZeroU32;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const CONFIG_PATH: &str = "/space/etc/sandbox.toml";

#[derive(Clone, Debug, Deserialize, Default)]
pub struct SandboxConfig {
    /// Optional guest RAM cap in MiB for any single VM.
    pub max_memory_mib: Option<u32>,
    /// Use hugepage-backed memory for VMs. Requires hugetlbfs at /dev/hugepages.
    /// Default: false. Set true on bare metal (AWS) for better TLB performance.
    #[serde(default)]
    pub hugepages: bool,
    /// Seconds of their sustained rates a sandbox's rootfs operations and
    /// bandwidth buckets hold. Default:
    /// [`DEFAULT_DISK_IO_BURST_SECS`](pplx_space_common_core::types::DEFAULT_DISK_IO_BURST_SECS).
    pub disk_io_burst_secs: Option<NonZeroU32>,
}

impl SandboxConfig {
    pub fn disk_io_burst_secs(&self) -> NonZeroU32 {
        self.disk_io_burst_secs
            .unwrap_or(pplx_space_common_core::types::DEFAULT_DISK_IO_BURST_SECS)
    }

    pub fn validate_snapshot_restore(&self) -> Result<(), &'static str> {
        if self.hugepages {
            Err("snapshot restore requires file-backed memory; set hugepages = false")
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigLoadError {
    #[error("cannot read sandbox configuration {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid sandbox configuration {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid sandbox configuration {path}: max_memory_mib must be positive")]
    InvalidMemoryCap { path: PathBuf },
}

pub fn load() -> Result<SandboxConfig, ConfigLoadError> {
    load_from(Path::new(CONFIG_PATH))
}

fn load_from(path: &Path) -> Result<SandboxConfig, ConfigLoadError> {
    let content = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(SandboxConfig::default());
        }
        Err(source) => {
            return Err(ConfigLoadError::Read {
                path: path.to_owned(),
                source,
            });
        }
    };
    let config: SandboxConfig =
        toml::from_str(&content).map_err(|source| ConfigLoadError::Parse {
            path: path.to_owned(),
            source,
        })?;
    if config.max_memory_mib == Some(0) {
        return Err(ConfigLoadError::InvalidMemoryCap {
            path: path.to_owned(),
        });
    }
    // Restore capability is checked by restore operations, not at startup:
    // hugepage-backed cold starts remain valid with the file backend.
    Ok(config)
}

#[cfg(test)]
mod tests;
