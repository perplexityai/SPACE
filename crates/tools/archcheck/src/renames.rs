//! The files a pull request renamed, as git detects them, so that an entry
//! moved with its file is compared with the entry it had rather than
//! counted as new.
//!
//! The input is `git diff -z --name-status --find-renames <base> HEAD`, as
//! CI passes it with `--renames`: records of a status, then one path, or two
//! for a rename or copy, each ended by a NUL. Only renames (`R<score>`) are
//! kept. Git pairs a deleted file with an added one only when their contents
//! are similar, so an unrelated new file never takes over a deleted file's
//! entry.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use thiserror::Error;

use crate::context::LoadError;

#[cfg(test)]
mod tests;

/// Why a `--renames` file is not `git diff -z --name-status` output.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RenamesError {
    #[error(
        "{status:?} is not a git status; give the output of git diff -z --name-status \
         --find-renames"
    )]
    Status { status: String },
    #[error("the {status} record ends before its paths")]
    Truncated { status: String },
    /// Each field of `-z` output ends with a NUL, so input that does not
    /// may have been cut off, possibly in the middle of a path.
    #[error(
        "the last record does not end with a NUL; give the complete output of git diff -z \
         --name-status --find-renames"
    )]
    Unterminated,
    /// Git renames a file once and to one file, so this is not its output.
    #[error("{path} is in more than one rename")]
    Repeated { path: String },
}

/// Each renamed file's old path, by its new path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Renames {
    sources: BTreeMap<String, String>,
}

impl Renames {
    pub fn load(path: &Path) -> Result<Self, LoadError> {
        let bytes = fs::read(path).map_err(|source| LoadError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&bytes).map_err(|source| LoadError::Renames {
            path: path.to_path_buf(),
            source,
        })
    }

    /// A path that is not UTF-8 names no entry, so its rename is dropped.
    pub fn parse(bytes: &[u8]) -> Result<Self, RenamesError> {
        let mut renames = Self::default();
        if bytes.is_empty() {
            return Ok(renames);
        }
        let bytes = bytes
            .strip_suffix(b"\0")
            .ok_or(RenamesError::Unterminated)?;
        let mut fields = bytes.split(|byte| *byte == 0);
        // Every path in a rename, old or new: a path in two renames could let
        // two entries be compared with one base entry. Git's --find-renames
        // output can't have one, without break detection (-B), since a rename
        // pairs a deleted path with an added one.
        let mut renamed = BTreeSet::new();
        while let Some(status) = fields.next() {
            let status = String::from_utf8_lossy(status).into_owned();
            let Some(letter) = status_letter(&status) else {
                // A long field is not a status: show its start.
                return Err(RenamesError::Status {
                    status: status.chars().take(80).collect(),
                });
            };
            let count = if matches!(letter, 'R' | 'C') { 2 } else { 1 };
            let paths: Vec<&[u8]> = fields.by_ref().take(count).collect();
            if paths.len() < count {
                return Err(RenamesError::Truncated { status });
            }
            // A copy keeps its source, so only a rename moves an entry.
            let (true, [old, new]) = (letter == 'R', &paths[..]) else {
                continue;
            };
            let (Ok(old), Ok(new)) = (std::str::from_utf8(old), std::str::from_utf8(new)) else {
                continue;
            };
            if let Some(path) = [old, new].into_iter().find(|path| !renamed.insert(*path)) {
                return Err(RenamesError::Repeated {
                    path: path.to_owned(),
                });
            }
            renames.sources.insert(new.to_owned(), old.to_owned());
        }
        Ok(renames)
    }

    /// The path `path` was renamed from, if it was.
    pub fn source(&self, path: &str) -> Option<&str> {
        self.sources.get(path).map(String::as_str)
    }
}

/// The letter of a git status such as `M` or `R100`: an upper-case letter,
/// then a similarity score for a rename or copy.
fn status_letter(status: &str) -> Option<char> {
    let mut chars = status.chars();
    let letter = chars.next().filter(char::is_ascii_uppercase)?;
    chars.all(|c| c.is_ascii_digit()).then_some(letter)
}
