//! Guest path and path-fragment types.
//!
//! These wrappers separate host-safe path components from guest-provided paths
//! and redact paths that may reveal customer workspace structure.

use std::fmt;
use std::path::{Component, Path};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::process::GuestUserName;

pub const MAX_GUEST_PATH_BYTES: usize = 4096;
const MAX_RELATIVE_GLOB_PATTERN_BYTES: usize = 4096;
const MAX_SAFE_PATH_COMPONENT_BYTES: usize = 255;

/// Error returned when a path-like value fails validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// assert!(SafePathComponent::parse("../root").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct PathParseError(&'static str);

/// Single path component that is safe to join under a host-owned directory.
///
/// This rejects separators, NUL, `.`, `..`, and components longer than 255
/// bytes so it can be used for names such as template or snapshot components
/// without deferring a `NAME_MAX` failure to the filesystem syscall.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let component = SafePathComponent::parse("snapshot-a").unwrap();
/// assert_eq!(component.as_str(), "snapshot-a");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SafePathComponent(String);

impl SafePathComponent {
    /// Parses a safe single path component.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert!(SafePathComponent::parse("rootfs").is_ok());
    /// assert!(SafePathComponent::parse("..").is_err());
    /// assert!(SafePathComponent::parse("a".repeat(256)).is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, PathParseError> {
        let value = value.into();
        validate_component(&value)?;
        Ok(Self(value))
    }

    /// Returns the validated component string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let component = SafePathComponent::parse("rootfs").unwrap();
    /// assert_eq!(component.as_str(), "rootfs");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SafePathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SafePathComponent {
    type Err = PathParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SafePathComponent {
    type Error = PathParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SafePathComponent> for String {
    fn from(value: SafePathComponent) -> Self {
        value.0
    }
}

/// Path inside a guest filesystem.
///
/// Guest paths are redacted in `Debug` output because they may contain customer
/// workspace names or file layout details.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let path = GuestPath::parse("/workspace/src/main.rs").unwrap();
/// assert_eq!(format!("{path:?}"), "GuestPath(<redacted>)");
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GuestPath(String);

impl GuestPath {
    /// Parses a non-empty, bounded guest path without control characters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let path = GuestPath::parse("/workspace").unwrap();
    /// assert_eq!(path.expose(), "/workspace");
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, PathParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_GUEST_PATH_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(PathParseError(
                "guest path must be non-empty, bounded, and control-free",
            ));
        }
        Ok(Self(value))
    }

    /// Exposes the raw guest path at an intentional boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let path = GuestPath::parse("/workspace").unwrap();
    /// assert_eq!(path.expose(), "/workspace");
    /// ```
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for GuestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GuestPath(<redacted>)")
    }
}

impl FromStr for GuestPath {
    type Err = PathParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Returns the conventional Linux home directory for a guest user.
///
/// This is used at API boundaries where the guest OS path has to be
/// deterministic before a request reaches the guest agent. Linux templates use
/// `/root` for root and `/home/<user>` for regular users.
pub fn conventional_linux_guest_home(user: &GuestUserName) -> Result<GuestPath, PathParseError> {
    if user.as_str() == "root" {
        GuestPath::parse("/root")
    } else {
        GuestPath::parse(format!("/home/{}", user.as_str()))
    }
}

/// Normalizes a caller-provided Linux guest path against a guest home directory.
///
/// Absolute paths stay rooted at the guest filesystem root. Relative paths are
/// resolved under `home`. `.` and an empty path resolve to `home`; `..` is
/// cleaned lexically without escaping the guest filesystem root.
pub fn normalize_linux_guest_path(
    path: &str,
    home: &GuestPath,
) -> Result<GuestPath, PathParseError> {
    validate_linux_guest_path_input(path)?;
    let path = Path::new(path);
    let rooted = if path.is_absolute() {
        clean_linux_guest_path(path)?
    } else {
        clean_linux_guest_path(&Path::new(home.expose()).join(path))?
    };
    GuestPath::parse(rooted)
}

/// Validates a caller-provided Linux guest path before semantic normalization.
///
/// Empty paths are accepted here because callers can normalize them to an
/// effective guest home directory later.
pub fn validate_linux_guest_path_input(path: &str) -> Result<(), PathParseError> {
    if path.len() > MAX_GUEST_PATH_BYTES || path.chars().any(char::is_control) {
        return Err(PathParseError(
            "guest path must be bounded and control-free",
        ));
    }
    if is_windows_guest_path(path) {
        return Err(PathParseError(
            "windows-style guest paths are not supported for linux sandboxes",
        ));
    }
    Ok(())
}

fn clean_linux_guest_path(path: &Path) -> Result<String, PathParseError> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                parts.pop();
            }
            Component::Normal(part) => {
                let Some(part) = part.to_str() else {
                    return Err(PathParseError("guest path must be valid utf-8"));
                };
                parts.push(part);
            }
            Component::Prefix(_) => {
                return Err(PathParseError(
                    "windows-style guest paths are not supported for linux sandboxes",
                ));
            }
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn is_windows_guest_path(path: &str) -> bool {
    path.starts_with("\\\\")
        || path.starts_with("//")
        || path
            .as_bytes()
            .get(..2)
            .is_some_and(|prefix| matches!(prefix, [drive, b':'] if drive.is_ascii_alphabetic()))
}

/// Relative path inside a guest filesystem.
///
/// This is useful for file APIs that must not accept absolute paths or `..`
/// traversal from a caller.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let path = RelativeGuestPath::parse("src/main.rs").unwrap();
/// assert_eq!(path.expose(), "src/main.rs");
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RelativeGuestPath(String);

impl RelativeGuestPath {
    /// Parses a bounded relative guest path without control chars or traversal.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert!(RelativeGuestPath::parse("src/main.rs").is_ok());
    /// assert!(RelativeGuestPath::parse("../secret").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, PathParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_GUEST_PATH_BYTES
            || value.chars().any(char::is_control)
            || value.starts_with('/')
            || Path::new(&value)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(PathParseError(
                "relative guest path must be bounded, relative, control-free, and traversal-free",
            ));
        }
        Ok(Self(value))
    }

    /// Exposes the raw relative guest path at an intentional boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let path = RelativeGuestPath::parse("src/main.rs").unwrap();
    /// assert_eq!(path.expose(), "src/main.rs");
    /// ```
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RelativeGuestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RelativeGuestPath(<redacted>)")
    }
}

impl FromStr for RelativeGuestPath {
    type Err = PathParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

/// Guest path used as a filesystem watch root.
///
/// This wrapper distinguishes watch roots from arbitrary guest paths in
/// watcher and synchronization APIs.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let root = WatchRoot::new(GuestPath::parse("/workspace").unwrap());
/// assert_eq!(root.guest_path().expose(), "/workspace");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WatchRoot(GuestPath);

impl WatchRoot {
    /// Wraps a guest path as a watch root.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let root = WatchRoot::new(GuestPath::parse("/workspace").unwrap());
    /// assert_eq!(root.guest_path().expose(), "/workspace");
    /// ```
    pub const fn new(value: GuestPath) -> Self {
        Self(value)
    }

    /// Returns the watched guest path.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let root = WatchRoot::new(GuestPath::parse("/workspace").unwrap());
    /// assert_eq!(root.guest_path().expose(), "/workspace");
    /// ```
    pub const fn guest_path(&self) -> &GuestPath {
        &self.0
    }
}

/// Relative glob pattern evaluated inside a guest context.
///
/// The pattern must be bounded, relative, control-free, and traversal-free so
/// callers cannot glob from an absolute host or guest root by accident.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let pattern = RelativeGlobPattern::parse("src/**/*.rs").unwrap();
/// assert_eq!(pattern.as_str(), "src/**/*.rs");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RelativeGlobPattern(String);

impl RelativeGlobPattern {
    /// Parses a bounded relative glob pattern without parent traversal.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert!(RelativeGlobPattern::parse("src/**/*.rs").is_ok());
    /// assert!(RelativeGlobPattern::parse("/tmp/*").is_err());
    /// assert!(RelativeGlobPattern::parse("../../../etc/**").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, PathParseError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_RELATIVE_GLOB_PATTERN_BYTES
            || value.chars().any(char::is_control)
            || value.starts_with('/')
            || Path::new(&value)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(PathParseError(
                "relative glob must be bounded, relative, control-free, and traversal-free",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated glob pattern string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let pattern = RelativeGlobPattern::parse("src/**/*.rs").unwrap();
    /// assert_eq!(pattern.as_str(), "src/**/*.rs");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RelativeGlobPattern {
    type Error = PathParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<RelativeGlobPattern> for String {
    fn from(value: RelativeGlobPattern) -> Self {
        value.0
    }
}

fn validate_component(value: &str) -> Result<(), PathParseError> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.contains('\0')
        || value == "."
        || value == ".."
    {
        return Err(PathParseError("path component is not safe"));
    }
    if value.len() > MAX_SAFE_PATH_COMPONENT_BYTES {
        return Err(PathParseError("path component is too long"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
