//! Everything a rule may inspect, loaded once before any rule runs.

use std::{
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

use crate::{
    baseline::{BASELINE_FILE, BaseBaseline, Baseline},
    config::{ArchitectureConfig, CONFIG_FILE},
};

/// The root manifest, whose `[workspace] members` decide which files are
/// checked.
const ROOT_MANIFEST: &str = "Cargo.toml";

/// Characters Cargo expands as globs in `[workspace] members`.
const GLOB_CHARACTERS: &[char] = &['*', '?', '[', ']', '{', '}'];

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("could not read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("could not parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("{path} has no [workspace] members list")]
    NoWorkspaceMembers { path: PathBuf },
    #[error(
        "{path}: workspace member {member} contains glob characters; list every member explicitly"
    )]
    GlobMember { path: PathBuf, member: String },
    #[error(
        "{path}: workspace member {member:?} has an empty, `.` or `..` component; write it as a \
         plain relative path"
    )]
    UnnormalizedMember { path: PathBuf, member: String },
    #[error("workspace member {member} has no Cargo.toml at {manifest}")]
    MissingMember { member: String, manifest: PathBuf },
    /// Debug-formatted, so the invalid bytes are shown escaped.
    #[error("{path:?} is not valid UTF-8")]
    NonUtf8Path { path: PathBuf },
    #[error("no Cargo.toml with a [workspace] table found at or above {start}")]
    NoWorkspaceRoot { start: PathBuf },
}

/// A production Rust file and its code-line count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionFile {
    /// Repository-relative, `/`-separated.
    pub path: String,
    pub code_lines: usize,
}

#[derive(Debug)]
pub struct Context {
    pub config: ArchitectureConfig,
    pub baseline: Baseline,
    /// Behaviours that compare with the base branch's baseline are skipped
    /// unless it is [`BaseBaseline::Loaded`].
    pub base_baseline: BaseBaseline,
    /// Every production Rust file of the root `Cargo.toml`'s workspace
    /// members, sorted by path.
    pub files: Vec<ProductionFile>,
}

impl Context {
    pub fn load(root: &Path) -> Result<Self, LoadError> {
        let baseline = Baseline::load(&root.join(BASELINE_FILE))?;
        Self::load_with_baseline(root, baseline)
    }

    pub fn load_with_baseline(root: &Path, baseline: Baseline) -> Result<Self, LoadError> {
        let config = parse_toml(&root.join(CONFIG_FILE))?;
        let members = load_members(&root.join(ROOT_MANIFEST))?;
        let files = production_files(root, &members)?;
        Ok(Self {
            config,
            baseline,
            base_baseline: BaseBaseline::NotGiven,
            files,
        })
    }

    pub fn file(&self, path: &str) -> Option<&ProductionFile> {
        self.files
            .binary_search_by(|file| file.path.as_str().cmp(path))
            .ok()
            .and_then(|index| self.files.get(index))
    }
}

/// The nearest ancestor of `start` (inclusive) whose `Cargo.toml` declares a
/// `[workspace]`.
pub fn find_workspace_root(start: &Path) -> Result<PathBuf, LoadError> {
    for dir in start.ancestors() {
        let manifest = dir.join(ROOT_MANIFEST);
        if manifest.is_file() && parse_toml::<toml::Table>(&manifest)?.contains_key("workspace") {
            return Ok(dir.to_path_buf());
        }
    }
    Err(LoadError::NoWorkspaceRoot {
        start: start.to_path_buf(),
    })
}

/// A sibling test module (`tests.rs`, `<name>_tests.rs`), not production code.
pub fn is_test_file(name: &str) -> bool {
    name == "tests.rs" || name.ends_with("_tests.rs")
}

/// A `tests/` directory under `src/` holds test modules (`#[cfg(test)] mod
/// tests;` with a `tests/mod.rs`), so nothing below it is production code.
/// `scripts/check-rust-error-policy.sh` excludes `*/tests/*` the same way.
pub fn is_test_directory(name: &OsStr) -> bool {
    name == "tests"
}

/// Lines that are non-empty after trimming and do not start with `//`. Line,
/// doc and inner-doc comments are free; block comments and attributes count.
/// Lines end at LF or CRLF, where rustc ends a `//` comment and counts line
/// numbers; a lone CR, U+2028 or U+2029 ends nothing, so a CR-only file is one
/// line. A leading byte order mark is not part of the first line.
pub fn count_code_lines(text: &str) -> usize {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//"))
        .count()
}

pub(crate) fn read(path: &Path) -> Result<String, LoadError> {
    std::fs::read_to_string(path).map_err(|source| LoadError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn parse_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, LoadError> {
    parse_toml_with_text(path).map(|(value, _)| value)
}

/// [`parse_toml`], also returning the text it parsed.
pub(crate) fn parse_toml_with_text<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<(T, String), LoadError> {
    let text = read(path)?;
    match toml::from_str(&text) {
        Ok(value) => Ok((value, text)),
        Err(source) => Err(LoadError::Parse {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Only the part of the root manifest the inventory needs; other keys are
/// Cargo's business.
#[derive(Deserialize)]
struct RootManifest {
    workspace: Option<WorkspaceManifest>,
}

#[derive(Deserialize)]
struct WorkspaceManifest {
    members: Option<Vec<String>>,
}

fn load_members(path: &Path) -> Result<Vec<String>, LoadError> {
    let manifest: RootManifest = parse_toml(path)?;
    let members = manifest
        .workspace
        .and_then(|workspace| workspace.members)
        .ok_or_else(|| LoadError::NoWorkspaceMembers {
            path: path.to_path_buf(),
        })?;
    for member in &members {
        let error = if member.contains(GLOB_CHARACTERS) {
            LoadError::GlobMember {
                path: path.to_path_buf(),
                member: member.clone(),
            }
        } else if member
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        {
            // Files are keyed by the path they are found under, so `a/../a`
            // would not match the baseline's `a`.
            LoadError::UnnormalizedMember {
                path: path.to_path_buf(),
                member: member.clone(),
            }
        } else {
            continue;
        };
        return Err(error);
    }
    Ok(members)
}

fn production_files(root: &Path, members: &[String]) -> Result<Vec<ProductionFile>, LoadError> {
    let mut paths = Vec::new();
    for member in members {
        let manifest = root.join(member).join("Cargo.toml");
        if !manifest.is_file() {
            return Err(LoadError::MissingMember {
                member: member.clone(),
                manifest,
            });
        }
        // A member directory or `src` that is itself a symlink is followed;
        // symlinks below `src/` are not.
        let src = root.join(member).join("src");
        if src.is_dir() {
            collect_rust_files(&src, &mut paths)?;
        }
    }
    let mut files = paths
        .into_iter()
        .map(|path| {
            Ok(ProductionFile {
                code_lines: count_code_lines(&read(&path)?),
                path: relative_path(root, &path)?,
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    // Sort by the string form: `Path` ordering is per component and would not
    // match the baseline's key order or `Context::file`'s binary search.
    files.sort_by(|a, b| a.path.cmp(&b.path));
    // A member nested inside another member's `src/` is walked twice.
    files.dedup_by(|a, b| a.path == b.path);
    Ok(files)
}

/// Symlinks are neither files nor directories here, so they are skipped.
fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), LoadError> {
    let read_error = |source| LoadError::Read {
        path: dir.to_path_buf(),
        source,
    };
    for entry in std::fs::read_dir(dir).map_err(read_error)? {
        let entry = entry.map_err(read_error)?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(read_error)?;
        if file_type.is_dir() {
            if !is_test_directory(&entry.file_name()) {
                collect_rust_files(&path, out)?;
            }
            continue;
        }
        // Only `.rs` names have to be UTF-8, so the extension is checked on
        // the raw name first.
        if !file_type.is_file() || path.extension() != Some(OsStr::new("rs")) {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return Err(LoadError::NonUtf8Path { path });
        };
        if !is_test_file(name) {
            out.push(path);
        }
    }
    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String, LoadError> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let parts: Option<Vec<&str>> = relative
        .components()
        .map(|component| component.as_os_str().to_str())
        .collect();
    match parts {
        Some(parts) => Ok(parts.join("/")),
        None => Err(LoadError::NonUtf8Path {
            path: path.to_path_buf(),
        }),
    }
}
