//! `Glob`: list regular files under a directory whose relative path matches a
//! gitignore-style glob, without spawning a process.
//!
//! The semantics are those of `rg --files --follow --hidden --no-ignore
//! --sort path --glob /<pattern>` with one `--glob '!**/<dir>/**'` per skipped
//! directory, so callers that ran that command get the same matches in the
//! same order: the pattern is anchored at `search_dir`, `*`, `?` and `[...]`
//! never cross `/`, `**` spans directories, braces are literal, hidden entries
//! are included and symbolic links are followed (a link back to an ancestor is
//! not entered twice). Unreadable directories and dangling links are skipped.

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use globset::{GlobBuilder, GlobMatcher};
use pplx_space_spaced_guest_protocol::proto::bridge_guest::{GlobMatch, GlobResponse};
use tonic::Status;

use super::metadata::system_time_to_timestamp;

/// The most matches one call returns, and the default when none is requested.
pub(super) const MAX_GLOB_RESULTS: usize = 100_000;
/// Stays under the 4 MiB gRPC message cap with room for field framing.
const MAX_GLOB_RESPONSE_BYTES: usize = 3 * 1024 * 1024;
/// Per-match framing on top of the path: tags, lengths, size and timestamp.
const GLOB_MATCH_OVERHEAD_BYTES: usize = 32;
/// Directory entries examined before the walk stops as truncated.
const MAX_GLOB_VISITED_ENTRIES: usize = 2_000_000;
/// Leaves the host's metadata-call budget time to relay the response.
pub(super) const GLOB_WALK_BUDGET: Duration = Duration::from_secs(8);

pub(super) struct GlobQuery {
    pub(super) root: PathBuf,
    pub(super) pattern: String,
    pub(super) skip_dirs: Vec<String>,
    pub(super) max_results: usize,
    pub(super) deadline: Instant,
}

struct Matchers {
    pattern: Option<GlobMatcher>,
    skip: Vec<GlobMatcher>,
}

fn build_glob(glob: &str) -> Result<GlobMatcher, Status> {
    GlobBuilder::new(glob)
        .literal_separator(true)
        .backslash_escape(true)
        .build()
        .map(|glob| glob.compile_matcher())
        .map_err(|error| Status::invalid_argument(format!("invalid glob: {error}")))
}

/// Applies gitignore line rules to an anchored glob: unescaped trailing
/// whitespace is dropped, and a trailing `/` matches directories only, which
/// never yields a file.
fn gitignore_glob(glob: &str) -> Option<String> {
    let mut glob = glob.to_owned();
    while glob.ends_with([' ', '\t']) && !glob.ends_with("\\ ") {
        glob.pop();
    }
    if glob.ends_with('/') {
        return None;
    }
    Some(glob)
}

impl Matchers {
    fn new(pattern: &str, skip_dirs: &[String]) -> Result<Self, Status> {
        let pattern = pattern.replace('{', "\\{").replace('}', "\\}");
        let pattern = pattern.strip_prefix("./").unwrap_or(&pattern);
        let pattern = pattern.trim_start_matches('/');
        let pattern = gitignore_glob(pattern)
            .map(|glob| build_glob(&glob))
            .transpose()?;
        let skip = skip_dirs
            .iter()
            .filter_map(|directory| gitignore_glob(&format!("**/{directory}/**")))
            .map(|glob| build_glob(&glob))
            .collect::<Result<_, _>>()?;
        Ok(Self { pattern, skip })
    }

    fn skipped(&self, relative: &str) -> bool {
        self.skip.iter().any(|glob| glob.is_match(relative))
    }

    fn matches(&self, relative: &str) -> bool {
        self.pattern
            .as_ref()
            .is_some_and(|glob| glob.is_match(relative))
            && !self.skipped(relative)
    }
}

struct Directory {
    relative: String,
    identity: Option<(u64, u64)>,
    entries: std::vec::IntoIter<(String, PathBuf)>,
}

fn read_sorted(path: &Path) -> Vec<(String, PathBuf)> {
    let Ok(read_dir) = fs::read_dir(path) else {
        return Vec::new();
    };
    let mut entries: Vec<_> = read_dir
        .filter_map(Result::ok)
        .filter_map(|entry| {
            // Paths travel as UTF-8 strings; other names cannot be returned.
            let name = entry.file_name().into_string().ok()?;
            Some((name, entry.path()))
        })
        .collect();
    entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    entries
}

fn identity(metadata: &fs::Metadata) -> (u64, u64) {
    (metadata.dev(), metadata.ino())
}

fn join(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

pub(super) fn glob(query: GlobQuery) -> Result<GlobResponse, Status> {
    let matchers = Matchers::new(&query.pattern, &query.skip_dirs)?;
    let limit = match query.max_results {
        0 => MAX_GLOB_RESULTS,
        limit => limit.min(MAX_GLOB_RESULTS),
    };
    let mut response = GlobResponse::default();
    // A missing or non-directory root has no matches, as with the command.
    let Ok(root) = fs::metadata(&query.root) else {
        return Ok(response);
    };
    if !root.is_dir() || matchers.pattern.is_none() {
        return Ok(response);
    }
    let mut ancestors = HashSet::from([identity(&root)]);
    let mut stack = vec![Directory {
        relative: String::new(),
        identity: None,
        entries: read_sorted(&query.root).into_iter(),
    }];
    let mut response_bytes = 0_usize;
    let mut visited = 0_usize;
    while let Some(directory) = stack.last_mut() {
        let Some((name, path)) = directory.entries.next() else {
            if let Some(identity) = stack.pop().and_then(|directory| directory.identity) {
                ancestors.remove(&identity);
            }
            continue;
        };
        visited += 1;
        if visited > MAX_GLOB_VISITED_ENTRIES
            || (visited.is_multiple_of(1024) && Instant::now() >= query.deadline)
        {
            response.truncated = true;
            break;
        }
        let relative = join(&directory.relative, &name);
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if metadata.is_dir() {
            let identity = identity(&metadata);
            // `**/<dir>/**` excludes everything beneath a matching directory,
            // so its contents need not be read.
            if matchers.skipped(&format!("{relative}/_")) || !ancestors.insert(identity) {
                continue;
            }
            stack.push(Directory {
                entries: read_sorted(&path).into_iter(),
                relative,
                identity: Some(identity),
            });
            continue;
        }
        if !metadata.is_file() || !matchers.matches(&relative) {
            continue;
        }
        response_bytes = response_bytes.saturating_add(relative.len() + GLOB_MATCH_OVERHEAD_BYTES);
        if response_bytes > MAX_GLOB_RESPONSE_BYTES {
            response.truncated = true;
            break;
        }
        response.matches.push(GlobMatch {
            path: relative,
            size_bytes: metadata.len(),
            modified_at: metadata.modified().ok().and_then(system_time_to_timestamp),
        });
        if response.matches.len() >= limit {
            response.truncated = true;
            break;
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests;
