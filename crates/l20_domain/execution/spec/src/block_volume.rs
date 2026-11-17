//! Creation-time storage intent and the stable one-data-drive guest topology.
//! Cloud device names are deliberately not part of this contract.

use std::{fmt, str::FromStr};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BlockVolumeSpecError {
    #[error("only the ebs block-volume backend is supported")]
    Backend,
    #[error("permissions must be read_write or read_only")]
    Permissions,
    #[error("block-volume size must be 128 or 256 GiB")]
    Size,
    #[error("mount must be an absolute, normalized, non-root path outside system directories")]
    Mount,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockVolumeBackend {
    Ebs,
}

impl BlockVolumeBackend {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ebs => "ebs",
        }
    }
}

impl fmt::Display for BlockVolumeBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BlockVolumeBackend {
    type Err = BlockVolumeSpecError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ebs" => Ok(Self::Ebs),
            _ => Err(BlockVolumeSpecError::Backend),
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockVolumePermissions {
    ReadWrite,
    ReadOnly,
}

impl BlockVolumePermissions {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "read_write",
            Self::ReadOnly => "read_only",
        }
    }

    pub const fn is_read_only(self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

impl fmt::Display for BlockVolumePermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BlockVolumePermissions {
    type Err = BlockVolumeSpecError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "read_write" => Ok(Self::ReadWrite),
            "read_only" => Ok(Self::ReadOnly),
            _ => Err(BlockVolumeSpecError::Permissions),
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuestBlockMountPath(String);

impl GuestBlockMountPath {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for GuestBlockMountPath {
    type Error = BlockVolumeSpecError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() > 4095
            || !value.starts_with('/')
            || value == "/"
            || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        {
            return Err(BlockVolumeSpecError::Mount);
        }
        let mut parts = value
            .strip_prefix('/')
            .ok_or(BlockVolumeSpecError::Mount)?
            .split('/');
        let first = parts.next().ok_or(BlockVolumeSpecError::Mount)?;
        if first.len() > 255
            || matches!(
                first,
                "" | "."
                    | ".."
                    | "bin"
                    | "boot"
                    | "dev"
                    | "etc"
                    | "lib"
                    | "lib32"
                    | "lib64"
                    | "proc"
                    | "run"
                    | "sbin"
                    | "sys"
                    | "usr"
                    | "space"
            )
            || parts.any(|part| part.is_empty() || part == "." || part == ".." || part.len() > 255)
        {
            return Err(BlockVolumeSpecError::Mount);
        }
        Ok(Self(value))
    }
}

impl From<GuestBlockMountPath> for String {
    fn from(value: GuestBlockMountPath) -> Self {
        value.0
    }
}

impl fmt::Display for GuestBlockMountPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u32", into = "u32"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockVolumeSizeGiB(u32);

impl BlockVolumeSizeGiB {
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for BlockVolumeSizeGiB {
    type Error = BlockVolumeSpecError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            128 | 256 => Ok(Self(value)),
            _ => Err(BlockVolumeSpecError::Size),
        }
    }
}

impl From<BlockVolumeSizeGiB> for u32 {
    fn from(value: BlockVolumeSizeGiB) -> Self {
        value.0
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockVolumeMount {
    #[cfg_attr(feature = "serde", serde(rename = "type"))]
    pub backend: BlockVolumeBackend,
    pub size_gib: BlockVolumeSizeGiB,
    pub permissions: BlockVolumePermissions,
    pub mount: GuestBlockMountPath,
}

/// Topology v1 has one stable non-root virtio-blk drive. A node resolves and
/// pins the actual host device separately; a caller cannot supply a host path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestDataDrive {
    pub permissions: BlockVolumePermissions,
}

impl GuestDataDrive {
    pub const TOPOLOGY_VERSION: u32 = 1;
    pub const DRIVE_ID: &'static str = "data0";
    pub const JAIL_PATH: &'static str = "/block-volumes/data0";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_paths_are_canonical_and_do_not_shadow_system_directories() {
        for invalid in [
            "",
            "/",
            "data",
            "//data",
            "/data/",
            "/data//child",
            "/data/../child",
            "/data/./child",
            "/proc/data",
            "/dev",
            "/usr/local",
            "/space/data",
            "/data\0suffix",
            "/data\nsuffix",
        ] {
            assert_eq!(
                GuestBlockMountPath::try_from(invalid.to_owned()),
                Err(BlockVolumeSpecError::Mount),
                "{invalid:?}"
            );
        }
        for valid in [
            "/data",
            "/foo/bar",
            "/mnt/data",
            "/dataset",
            "/home/user/data",
        ] {
            assert_eq!(
                GuestBlockMountPath::try_from(valid.to_owned())
                    .unwrap()
                    .as_str(),
                valid
            );
        }
        for prefix in ["/", "/data/"] {
            GuestBlockMountPath::try_from(format!("{prefix}{}", "a".repeat(255))).unwrap();
            assert_eq!(
                GuestBlockMountPath::try_from(format!("{prefix}{}", "a".repeat(256))),
                Err(BlockVolumeSpecError::Mount)
            );
        }
    }

    #[cfg(feature = "serde")]
    #[test]
    fn wire_contract_round_trips_and_rejects_unimplemented_values() {
        let mount = serde_json::json!({
            "type": "ebs", "size_gib": 128, "permissions": "read_only", "mount": "/data"
        });
        let parsed: BlockVolumeMount = serde_json::from_value(mount.clone()).unwrap();
        assert!(parsed.permissions.is_read_only());
        assert_eq!(serde_json::to_value(parsed).unwrap(), mount);
        for (field, value) in [
            ("type", serde_json::json!("nfs")),
            ("size_gib", serde_json::json!(129)),
            ("permissions", serde_json::json!("write")),
            ("mount", serde_json::json!("/data/../etc")),
            ("host_path", serde_json::json!("/dev/nvme0n1")),
        ] {
            let mut invalid = mount.clone();
            invalid[field] = value;
            assert!(
                serde_json::from_value::<BlockVolumeMount>(invalid).is_err(),
                "{field}"
            );
        }
        for field in ["type", "size_gib", "permissions", "mount"] {
            let mut invalid = mount.clone();
            invalid.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<BlockVolumeMount>(invalid).is_err(),
                "{field}"
            );
        }
    }
}
