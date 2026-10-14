//! Canonical node filesystem paths for SPACE runtime state.
//!
//! Node services build many paths from sandbox IDs, template build IDs, and
//! snapshot names. These wrappers centralize the layout so callers pass a
//! typed root and typed domain identifiers instead of hand-concatenating
//! strings.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_common_core::types::*;
//! let root = NodeSpacePath::parse("/space")?;
//! let sandbox_id = SandboxId::from_uuid(uuid::Uuid::nil());
//! let run_dir = NodeSandboxRunDir::new(&root, sandbox_id);
//!
//! assert!(run_dir.as_path().to_string_lossy().ends_with(&sandbox_id.to_string()));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::path::{Component, Path, PathBuf};

use super::sandbox::SandboxId;
use super::snapshot::SnapshotName;
use super::template::TemplateBuildId;

/// Default root for runtime state on a node.
pub const SPACE_RUN_DIR: &str = "/space/run";
/// Prefix used when validating paths that must live below `/space/run`.
pub const SPACE_RUN_PREFIX: &str = "/space/run/";
/// Default root for cached node artifacts.
pub const SPACE_CACHE_DIR: &str = "/space/cache";
/// Firecracker runtime state root.
pub const FIRECRACKER_RUN_DIR: &str = "/space/run/firecracker";
/// Firecracker cache root.
pub const FIRECRACKER_CACHE_DIR: &str = "/space/cache/firecracker";
/// Prefix used when validating paths that must live below the Firecracker cache.
pub const FIRECRACKER_CACHE_PREFIX: &str = "/space/cache/firecracker/";
/// Per-sandbox Firecracker runtime directory root.
pub const SANDBOX_RUN_DIR: &str = "/space/run/firecracker/sandboxes";
/// Per-sandbox Firecracker cache directory root.
pub const SANDBOX_CACHE_DIR: &str = "/space/cache/firecracker/sandboxes";
/// Reserved staging directory for cross-sandbox durable artifact copies.
pub const FORK_COPY_STAGING_DIR_NAME: &str = ".fork-copies";
/// Firecracker jail directory root.
pub const FIRECRACKER_JAIL_DIR: &str = "/space/cache/firecracker/jails";
/// Firecracker's log file, in its jail root.
pub const FIRECRACKER_LOG_FILE: &str = "fc.log";
/// The FIFO Firecracker writes its metrics flushes to, in its jail root.
pub const FIRECRACKER_METRICS_FIFO: &str = "fc-metrics.fifo";

/// A sandbox's jail root, the chroot the jailer builds under
/// [`FIRECRACKER_JAIL_DIR`] as `<chroot base>/<exec file name>/<id>/root`.
pub fn jail_root(sandbox_id: &str) -> PathBuf {
    PathBuf::from(format!(
        "{FIRECRACKER_JAIL_DIR}/firecracker/{sandbox_id}/root"
    ))
}
/// Template artifact directory root.
pub const TEMPLATE_DIR: &str = "/space/cache/firecracker/templates";

/// Directory name used under a sandbox cache directory for snapshots.
pub const SNAPSHOT_DIR_NAME: &str = "snapshots";
/// Root filesystem artifact name for template builds.
pub const TEMPLATE_ROOTFS_FILE: &str = "rootfs.ext4";
/// zstd-compressed root filesystem transfer artifact name for template builds.
pub const TEMPLATE_ROOTFS_ZSTD_FILE: &str = "rootfs.ext4.zst";
/// Compression codec used for `rootfs.ext4.zst` template transfer artifacts.
pub const TEMPLATE_ROOTFS_ZSTD_CODEC: &str = "zstd";
/// Compression level used for `rootfs.ext4.zst` template transfer artifacts.
pub const TEMPLATE_ROOTFS_ZSTD_LEVEL: i32 = 3;

/// Error returned when a node path is not safe for deriving SPACE paths.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// assert!(NodeSpacePath::parse("relative/path").is_err());
/// assert!(NodeSpacePath::parse("/space/../etc").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum NodePathError {
    #[error("node path must be absolute")]
    NotAbsolute,
    #[error("node path must not contain parent traversal")]
    ParentTraversal,
}

/// Absolute filesystem root used for deriving node-local SPACE paths.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let root = NodeSpacePath::parse("/space")?;
/// assert_eq!(root.as_path().to_string_lossy(), "/space");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeSpacePath(PathBuf);

impl NodeSpacePath {
    /// Parses an absolute node filesystem path.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// assert!(NodeSpacePath::parse("/space").is_ok());
    /// assert!(NodeSpacePath::parse("space").is_err());
    /// assert!(NodeSpacePath::parse("/space/../etc").is_err());
    /// ```
    pub fn parse(value: impl Into<PathBuf>) -> Result<Self, NodePathError> {
        let value = value.into();
        if !value.is_absolute() {
            return Err(NodePathError::NotAbsolute);
        }
        if value
            .components()
            .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(NodePathError::ParentTraversal);
        }
        Ok(Self(value))
    }

    /// Returns the path as a borrowed [`Path`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let root = NodeSpacePath::parse("/space")?;
    /// assert!(root.as_path().is_absolute());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

/// Runtime directory for one sandbox on a node.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let root = NodeSpacePath::parse("/space")?;
/// let id = SandboxId::from_uuid(uuid::Uuid::nil());
/// let dir = NodeSandboxRunDir::new(&root, id);
/// assert!(dir.as_path().to_string_lossy().contains("/run/firecracker/sandboxes/"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeSandboxRunDir(PathBuf);

impl NodeSandboxRunDir {
    /// Derives the runtime directory for a sandbox ID below the node root.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let root = NodeSpacePath::parse("/space")?;
    /// let id = SandboxId::from_uuid(uuid::Uuid::nil());
    /// let dir = NodeSandboxRunDir::new(&root, id);
    /// assert!(dir.as_path().to_string_lossy().ends_with(&id.to_string()));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(root: &NodeSpacePath, id: SandboxId) -> Self {
        Self(
            root.as_path()
                .join(space_relative(SANDBOX_RUN_DIR))
                .join(id.to_string()),
        )
    }

    /// Returns the derived sandbox runtime directory.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let root = NodeSpacePath::parse("/space")?;
    /// let dir = NodeSandboxRunDir::new(&root, SandboxId::from_uuid(uuid::Uuid::nil()));
    /// assert!(dir.as_path().is_absolute());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

/// Root filesystem artifact path for one template build.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let root = NodeSpacePath::parse("/space")?;
/// let template = TemplateName::parse("seed")?;
/// let hash = ContentHash::new(Digest::sha256([0; 32]));
/// let build = TemplateBuildId::new(template, hash);
/// let path = NodeTemplateRootfsPath::new(&root, &build);
///
/// assert!(path.as_path().to_string_lossy().ends_with(TEMPLATE_ROOTFS_FILE));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeTemplateRootfsPath(PathBuf);

impl NodeTemplateRootfsPath {
    /// Derives the `rootfs.ext4` artifact path for a template build.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let root = NodeSpacePath::parse("/space")?;
    /// let template = TemplateName::parse("seed")?;
    /// let hash = ContentHash::new(Digest::sha256([1; 32]));
    /// let build = TemplateBuildId::new(template, hash);
    /// let path = NodeTemplateRootfsPath::new(&root, &build);
    /// assert!(path.as_path().to_string_lossy().contains("/templates/seed/"));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(root: &NodeSpacePath, build: &TemplateBuildId) -> Self {
        Self(
            root.as_path()
                .join(space_relative(TEMPLATE_DIR))
                .join(build.template().as_str())
                .join(build.content_hash().to_string())
                .join(TEMPLATE_ROOTFS_FILE),
        )
    }

    /// Returns the derived template root filesystem path.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let root = NodeSpacePath::parse("/space")?;
    /// let template = TemplateName::parse("seed")?;
    /// let hash = ContentHash::new(Digest::sha256([2; 32]));
    /// let build = TemplateBuildId::new(template, hash);
    /// let path = NodeTemplateRootfsPath::new(&root, &build);
    /// assert!(path.as_path().is_absolute());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

/// Snapshot directory for one sandbox snapshot name.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let root = NodeSpacePath::parse("/space")?;
/// let id = SandboxId::from_uuid(uuid::Uuid::nil());
/// let name = SnapshotName::parse("before-upgrade")?;
/// let dir = NodeSnapshotDir::new(&root, id, &name);
///
/// assert!(dir.as_path().to_string_lossy().ends_with("before-upgrade"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NodeSnapshotDir(PathBuf);

impl NodeSnapshotDir {
    /// Derives the cache directory for a named sandbox snapshot.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let root = NodeSpacePath::parse("/space")?;
    /// let name = SnapshotName::parse("base")?;
    /// let dir = NodeSnapshotDir::new(&root, SandboxId::from_uuid(uuid::Uuid::nil()), &name);
    /// assert!(dir.as_path().to_string_lossy().contains("/snapshots/base"));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(root: &NodeSpacePath, id: SandboxId, name: &SnapshotName) -> Self {
        Self(
            root.as_path()
                .join(space_relative(SANDBOX_CACHE_DIR))
                .join(id.to_string())
                .join(SNAPSHOT_DIR_NAME)
                .join(name.as_str()),
        )
    }

    /// Returns the derived snapshot directory.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::*;
    /// let root = NodeSpacePath::parse("/space")?;
    /// let name = SnapshotName::parse("base")?;
    /// let dir = NodeSnapshotDir::new(&root, SandboxId::from_uuid(uuid::Uuid::nil()), &name);
    /// assert!(dir.as_path().is_absolute());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

fn space_relative(path: &'static str) -> &'static str {
    path.strip_prefix("/space/")
        .expect("node filesystem constants must live under /space")
}

#[cfg(test)]
mod tests;
