//! Optional guest-seed selection from the mounted image's OS identity.
//!
//! `SPACE_OCI_GUEST_SEEDS` is an optional JSON map from OS `ID`, then
//! `VERSION_ID`, to a template reference, for example
//! `{"ubuntu":{"24.04":"seed-noble:validated"}}`. Only OCI recipes based
//! on the stock `seed` name participate, including the digest-pinned base sent
//! by the API. Missing configuration or an unmatched identity retains the
//! original base. Configure system-visible seeds and prefer immutable digests.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use pplx_space_common_core::types::TemplateReference;

const ENV: &str = "SPACE_OCI_GUEST_SEEDS";
const MAX_OS_RELEASE_BYTES: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum GuestSeedError {
    #[error("{ENV} is not valid UTF-8: {0}")]
    Environment(#[source] std::env::VarError),
    #[error("{ENV} must map OS IDs and version IDs to template references: {0}")]
    Configuration(#[source] serde_json::Error),
    #[error("cannot read OCI os-release: {0}")]
    Read(#[from] std::io::Error),
    #[error("OCI os-release must be a regular file inside the image, at most 64 KiB")]
    InvalidFile,
}

#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct GuestSeeds(BTreeMap<String, BTreeMap<String, TemplateReference>>);

impl GuestSeeds {
    pub fn from_env() -> Result<Self, GuestSeedError> {
        match std::env::var(ENV) {
            Ok(value) => serde_json::from_str(&value).map_err(GuestSeedError::Configuration),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(error) => Err(GuestSeedError::Environment(error)),
        }
    }

    /// The API pins the stock seed before dispatch, so match its name across
    /// both tag and digest selectors. Custom seed names retain their selection.
    pub fn applies_to(&self, base: &TemplateReference) -> bool {
        !self.0.is_empty() && base.name().as_str() == "seed"
    }

    pub fn select(
        &self,
        root: &Path,
        base: &TemplateReference,
    ) -> Result<TemplateReference, GuestSeedError> {
        if !self.applies_to(base) {
            return Ok(base.clone());
        }
        let Some(release) = read_os_release(root)? else {
            return Ok(base.clone());
        };
        let value = |key| {
            release
                .lines()
                .rev()
                .filter_map(|line| line.trim().split_once('='))
                .find(|(name, _)| *name == key)
                .and_then(|(_, value)| identity_value(value))
        };
        Ok(value("ID")
            .zip(value("VERSION_ID"))
            .and_then(|(id, version)| self.0.get(id)?.get(version))
            .unwrap_or(base)
            .clone())
    }
}

// ID and VERSION_ID use simple tokens. Never source an image-provided file in
// a shell; unsupported expressions cannot opt an image into a configured seed.
fn identity_value(value: &str) -> Option<&str> {
    let value = value.trim();
    let value = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value);
    (!value.is_empty()
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c)))
    .then_some(value)
}

fn read_os_release(root: &Path) -> Result<Option<String>, GuestSeedError> {
    // The containerd view is read-only throughout inspection and assembly.
    let root = root.canonicalize()?;
    for relative in ["etc/os-release", "usr/lib/os-release"] {
        let path = match resolve_image_path(&root, Path::new(relative)) {
            Ok(path) => path,
            Err(GuestSeedError::Read(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            Err(error) => return Err(error),
        };
        if !path.metadata()?.is_file() {
            return Err(GuestSeedError::InvalidFile);
        }
        let mut contents = String::new();
        fs::File::open(path)?
            .take(MAX_OS_RELEASE_BYTES + 1)
            .read_to_string(&mut contents)?;
        if contents.len() as u64 > MAX_OS_RELEASE_BYTES {
            return Err(GuestSeedError::InvalidFile);
        }
        return Ok(Some(contents));
    }
    Ok(None)
}

// Resolve every component in the immutable image view. Host canonicalization
// treats absolute image symlinks as host paths, unlike the guest's filesystem.
fn resolve_image_path(root: &Path, path: &Path) -> Result<PathBuf, GuestSeedError> {
    let mut resolved = root.to_path_buf();
    let mut pending = path.to_path_buf();
    let mut links = 0;
    loop {
        let mut components = pending.components();
        let Some(component) = components.next() else {
            return Ok(resolved);
        };
        let remainder = components.as_path().to_path_buf();
        match component {
            Component::RootDir => resolved = root.to_path_buf(),
            Component::ParentDir => {
                if resolved != root {
                    resolved.pop();
                }
            }
            Component::CurDir => {}
            Component::Normal(name) => {
                resolved.push(name);
                if fs::symlink_metadata(&resolved)?.file_type().is_symlink() {
                    links += 1;
                    if links > 40 {
                        return Err(GuestSeedError::InvalidFile);
                    }
                    let target = fs::read_link(&resolved)?;
                    resolved.pop();
                    pending = target.join(remainder);
                    continue;
                }
            }
            Component::Prefix(_) => return Err(GuestSeedError::InvalidFile),
        }
        pending = remainder;
    }
}

#[cfg(test)]
mod tests;
