//! `architecture-baseline.toml`: existing violations a rule tolerates for now.
//!
//! Each rule that needs a baseline owns one table. `cargo arch
//! --write-baseline` updates and removes entries but never adds one; a missing
//! table means the rule tolerates nothing, which can only make checks
//! stricter. The base branch's copy, which a pull request's baseline is
//! compared with, is read leniently so that a change to the schema does not
//! break the comparison.

use std::{collections::BTreeMap, io, path::Path, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use toml::Spanned;

use crate::context::{LoadError, parse_toml_with_text, read};

pub const BASELINE_FILE: &str = "architecture-baseline.toml";

const HEADER: &str = "\
# Existing violations of the rules in ARCHITECTURE.toml, tolerated until fixed.
# Updated by `cargo arch --write-baseline`, which never adds an entry; see
# crates/tools/archcheck/README.md.
#
# [file_size] maps each production Rust file over the code-line limit to its cap,
# the code-line count it had when listed. Only remove entries or lower caps: a
# pull request that adds an entry or raises a cap is reported.
";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Baseline {
    /// Repository-relative path to the file's code-line cap.
    pub file_size: BTreeMap<String, usize>,
    /// The 1-based line of each `[file_size]` key in the file the baseline
    /// was loaded from, where a finding about the entry itself points.
    #[serde(skip)]
    pub file_size_lines: BTreeMap<String, usize>,
}

/// The baseline as written, with the position of each key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineFile {
    #[serde(default)]
    file_size: BTreeMap<Spanned<String>, usize>,
}

/// The base branch's copy: only `[file_size]`, other tables and keys ignored.
#[derive(Deserialize)]
struct BaseBaselineFile {
    #[serde(default)]
    file_size: BTreeMap<String, usize>,
}

/// The base branch's baseline, given with `--base-baseline`.
#[derive(Debug, Default)]
pub enum BaseBaseline {
    #[default]
    NotGiven,
    /// Not TOML, or a `[file_size]` that is not a table of integers.
    Unusable {
        path: PathBuf,
        /// The 1-based line the error starts on, when it has a position.
        line: Option<usize>,
        error: toml::de::Error,
    },
    Loaded(Baseline),
}

#[derive(Debug, Error)]
pub enum WriteBaselineError {
    #[error(transparent)]
    Load(#[from] LoadError),
    #[error("could not serialize the baseline: {0}")]
    Serialize(#[from] toml::ser::Error),
    #[error("could not write {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl Baseline {
    /// A key that names no production file is reported as a stale entry.
    pub fn load(path: &Path) -> Result<Self, LoadError> {
        let (file, text): (BaselineFile, String) = parse_toml_with_text(path)?;
        let mut baseline = Self::default();
        for (key, cap) in file.file_size {
            let line = line_of(&text, key.span().start);
            let key = key.into_inner();
            baseline.file_size_lines.insert(key.clone(), line);
            baseline.file_size.insert(key, cap);
        }
        Ok(baseline)
    }

    /// The base branch's copy at `path`. Only an unreadable file is an
    /// error; one that does not parse is [`BaseBaseline::Unusable`].
    pub fn load_base(path: &Path) -> Result<BaseBaseline, LoadError> {
        let text = read(path)?;
        Ok(match toml::from_str::<BaseBaselineFile>(&text) {
            Ok(file) => BaseBaseline::Loaded(Self {
                file_size: file.file_size,
                file_size_lines: BTreeMap::new(),
            }),
            Err(error) => BaseBaseline::Unusable {
                path: path.to_path_buf(),
                line: error.span().map(|span| line_of(&text, span.start)),
                error,
            },
        })
    }

    pub fn write(&self, path: &Path) -> Result<(), WriteBaselineError> {
        let text = format!("{HEADER}\n{}", toml::to_string(self)?);
        std::fs::write(path, text).map_err(|source| WriteBaselineError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

/// The 1-based line of byte `offset` in `text`.
fn line_of(text: &str, offset: usize) -> usize {
    text.get(..offset).unwrap_or_default().matches('\n').count() + 1
}
