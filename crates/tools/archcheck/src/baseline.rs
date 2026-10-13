//! `architecture-baseline.toml`: existing violations a rule tolerates for now.
//!
//! Each rule that needs a baseline owns a table: `[file_size]`, and one
//! `[pattern_ratchet.<instance>]` per configured instance. `cargo arch
//! --write-baseline` updates and removes the entries of every table, or of one
//! with `--write-baseline=<table>`, but never adds an entry or creates a
//! `pattern_ratchet` table: those are hand edits, which the base branch
//! comparison then reports. (`[file_size]` is always written, empty if the
//! file had none.) A missing table means the rule tolerates nothing, which can
//! only make checks stricter. The base branch's copy, which a pull request's baseline is
//! compared with, is read leniently and table by table, so that a change to
//! the schema or a renamed instance does not break the comparison.

use std::{collections::BTreeMap, io, path::Path, path::PathBuf};

use serde::{Deserialize, Serialize, de::IgnoredAny};
use thiserror::Error;
use toml::{
    Spanned,
    de::{DeTable, DeValue, ValueDeserializer},
};

use crate::context::{LoadError, parse_toml_with_text, read};

pub const BASELINE_FILE: &str = "architecture-baseline.toml";

const HEADER: &str = "\
# Existing violations of the rules in ARCHITECTURE.toml, tolerated until fixed.
# Updated by `cargo arch --write-baseline[=<table>]`, which never adds an entry
# or a [pattern_ratchet.*] table; see crates/tools/archcheck/README.md.
#
# [file_size] maps each production Rust file over the code-line limit to its cap,
# the code-line count it had when listed. [pattern_ratchet.<instance>] maps each
# production Rust file outside the instance's allowed paths to its cap on code
# lines matching its patterns. Only remove entries or lower caps: a pull request
# that adds an entry or raises a cap is reported.
";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Baseline {
    /// Repository-relative path to the file's cap, its code-line count when
    /// it was listed.
    pub file_size: BTreeMap<String, usize>,
    /// The 1-based line of each `file_size` key in the file the baseline
    /// was loaded from, where a finding about the entry itself points.
    #[serde(skip)]
    pub file_size_lines: BTreeMap<String, usize>,
    /// Each `pattern_ratchet` instance's table: repository-relative path to
    /// the file's cap on matching code lines.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub pattern_ratchet: BTreeMap<String, BTreeMap<String, usize>>,
    /// The 1-based line of each key in a `[pattern_ratchet.<instance>]`
    /// table, as for `file_size_lines`.
    #[serde(skip)]
    pub pattern_ratchet_lines: BTreeMap<String, BTreeMap<String, usize>>,
}

/// The baseline as written, with the position of each key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineFile {
    #[serde(default)]
    file_size: BTreeMap<Spanned<String>, usize>,
    #[serde(default)]
    pattern_ratchet: BTreeMap<String, BTreeMap<Spanned<String>, usize>>,
}

/// The base branch's copy, for `[file_size]`: other tables and keys ignored.
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
    /// Not TOML, so no table can be compared.
    Unusable(UnusableBase),
    Loaded(BaseTables),
}

/// The tables of a base branch's copy that is TOML, each read on its own so
/// that one that does not parse skips only its own comparisons.
#[derive(Debug)]
pub struct BaseTables {
    file_size: BaseTable,
    /// Every `[pattern_ratchet.<instance>]` table, configured or not, or why
    /// `pattern_ratchet` is not a table of tables.
    pattern_ratchet: Result<BTreeMap<String, BaseTable>, Box<UnusableBase>>,
}

/// One table of the base branch's copy, or why it can't be compared.
type BaseTable = Result<BTreeMap<String, usize>, Box<UnusableBase>>;

/// Part of the base branch's copy that does not parse.
#[derive(Debug)]
pub struct UnusableBase {
    pub path: PathBuf,
    /// The 1-based line the error starts on, when it has a position.
    pub line: Option<usize>,
    pub error: toml::de::Error,
}

/// Why behaviours that compare with the base branch's copy are skipped.
#[derive(Debug)]
pub enum BaseSkip<'a> {
    NotGiven,
    Unusable(&'a UnusableBase),
}

impl BaseSkip<'_> {
    /// The line reporting the skip of `behaviours`, such as
    /// `file_size.no_new_entries and file_size.no_raised_caps skipped`.
    pub fn message(&self, behaviours: &str) -> String {
        match self {
            Self::NotGiven => format!("{behaviours} (no --base-baseline given)"),
            Self::Unusable(UnusableBase { path, line, error }) => {
                let at = line
                    .map(|line| format!(" at line {line}"))
                    .unwrap_or_default();
                format!(
                    "warning: could not parse {}{at}: {}; {behaviours}",
                    path.display(),
                    error.message(),
                )
            }
        }
    }
}

/// `table`'s entries, or why its comparisons are skipped.
fn usable(table: &BaseTable) -> Result<&BTreeMap<String, usize>, BaseSkip<'_>> {
    table
        .as_ref()
        .map_err(|unusable| BaseSkip::Unusable(unusable))
}

/// The table a base branch's copy without it compares as: no entries.
static NO_ENTRIES: BTreeMap<String, usize> = BTreeMap::new();

impl BaseBaseline {
    /// The base branch's `[file_size]` table, empty if it has none.
    pub fn file_size(&self) -> Result<&BTreeMap<String, usize>, BaseSkip<'_>> {
        usable(&self.tables()?.file_size)
    }

    /// The base branch's `[pattern_ratchet.<instance>]` table, empty if it has
    /// none. Its tables for other instances are never read, so one for an
    /// instance this branch renamed or removed is ignored, even if it does not
    /// parse.
    pub fn pattern_ratchet(
        &self,
        instance: &str,
    ) -> Result<&BTreeMap<String, usize>, BaseSkip<'_>> {
        let instances = self
            .tables()?
            .pattern_ratchet
            .as_ref()
            .map_err(|unusable| BaseSkip::Unusable(unusable))?;
        match instances.get(instance) {
            Some(table) => usable(table),
            None => Ok(&NO_ENTRIES),
        }
    }

    /// Every instance the base branch's copy has a `[pattern_ratchet.<instance>]`
    /// table for, usable or not; none if `pattern_ratchet` can't be read.
    pub fn pattern_ratchet_instances(&self) -> impl Iterator<Item = &str> {
        self.tables()
            .ok()
            .and_then(|tables| tables.pattern_ratchet.as_ref().ok())
            .into_iter()
            .flat_map(|instances| instances.keys().map(String::as_str))
    }

    fn tables(&self) -> Result<&BaseTables, BaseSkip<'_>> {
        match self {
            Self::NotGiven => Err(BaseSkip::NotGiven),
            Self::Unusable(unusable) => Err(BaseSkip::Unusable(unusable)),
            Self::Loaded(tables) => Ok(tables),
        }
    }
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
    #[error("no baseline table {table:?}; expected one of: {known}")]
    UnknownTable { table: String, known: String },
}

/// The name of one baseline table, as `--write-baseline=<table>` takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TableName<'a> {
    FileSize,
    PatternRatchet(&'a str),
}

impl<'a> TableName<'a> {
    fn parse(name: &'a str) -> Option<Self> {
        match name.split_once('.') {
            None if name == "file_size" => Some(Self::FileSize),
            Some(("pattern_ratchet", instance)) => Some(Self::PatternRatchet(instance)),
            _ => None,
        }
    }
}

impl Baseline {
    /// Parses `path`, keeping each key's line for findings about the entry.
    pub fn load(path: &Path) -> Result<Self, LoadError> {
        let (file, text): (BaselineFile, String) = parse_toml_with_text(path)?;
        let mut baseline = Self::default();
        (baseline.file_size, baseline.file_size_lines) = with_lines(&text, file.file_size);
        for (instance, table) in file.pattern_ratchet {
            let (caps, lines) = with_lines(&text, table);
            baseline.pattern_ratchet.insert(instance.clone(), caps);
            baseline.pattern_ratchet_lines.insert(instance, lines);
        }
        Ok(baseline)
    }

    /// The base branch's copy at `path`. Only an unreadable file is an
    /// error; one that is not TOML is [`BaseBaseline::Unusable`], and a table
    /// that does not parse is unusable on its own.
    pub fn load_base(path: &Path) -> Result<BaseBaseline, LoadError> {
        let text = read(path)?;
        let unusable = |error: toml::de::Error| {
            Box::new(UnusableBase {
                path: path.to_path_buf(),
                line: error.span().map(|span| line_of(&text, span.start)),
                error,
            })
        };
        let document = match DeTable::parse(&text) {
            Ok(document) => document.into_inner(),
            Err(error) => return Ok(BaseBaseline::Unusable(*unusable(error))),
        };
        let file_size = toml::from_str::<BaseBaselineFile>(&text)
            .map(|file| file.file_size)
            .map_err(unusable);
        let pattern_ratchet = match document
            .into_iter()
            .find(|(key, _)| key.get_ref() == "pattern_ratchet")
        {
            None => Ok(BTreeMap::new()),
            Some((_, value)) => base_instances(value, &unusable),
        };
        Ok(BaseBaseline::Loaded(BaseTables {
            file_size,
            pattern_ratchet,
        }))
    }

    /// Every table, by the name `--write-baseline=<table>` takes:
    /// `file_size`, then `pattern_ratchet.<instance>` for each instance.
    pub fn table_names(&self) -> Vec<String> {
        let instances = self.pattern_ratchet.keys();
        std::iter::once("file_size".to_owned())
            .chain(instances.map(|instance| format!("pattern_ratchet.{instance}")))
            .collect()
    }

    /// The table `name` names, if this baseline has it.
    pub fn table(&self, name: &str) -> Option<&BTreeMap<String, usize>> {
        match TableName::parse(name)? {
            TableName::FileSize => Some(&self.file_size),
            TableName::PatternRatchet(instance) => self.pattern_ratchet.get(instance),
        }
    }

    /// Replaces the table `name` names with `fresh`'s, leaving every other
    /// table as it is, and says whether it did: if `fresh` lacks the table,
    /// nothing changes.
    pub fn replace_table(&mut self, fresh: &Self, name: &str) -> bool {
        let (Some(entries), Some(table)) = (fresh.table(name), TableName::parse(name)) else {
            return false;
        };
        let entries = entries.clone();
        match table {
            TableName::FileSize => self.file_size = entries,
            TableName::PatternRatchet(instance) => {
                self.pattern_ratchet.insert(instance.to_owned(), entries);
            }
        }
        true
    }

    /// Writes the header and every table in generated form: sorted keys, one
    /// table after another. A baseline loaded from a generated file and
    /// written back is byte-identical; hand-added comments, quoting or order
    /// are not kept.
    pub fn write(&self, path: &Path) -> Result<(), WriteBaselineError> {
        let text = format!("{HEADER}\n{}", toml::to_string(self)?);
        std::fs::write(path, text).map_err(|source| WriteBaselineError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Each instance's table under the base copy's `pattern_ratchet`, each
/// parsed on its own.
fn base_instances(
    value: Spanned<DeValue<'_>>,
    unusable: &impl Fn(toml::de::Error) -> Box<UnusableBase>,
) -> Result<BTreeMap<String, BaseTable>, Box<UnusableBase>> {
    // Only a table can hold instance tables; serde says what it is instead.
    if let Err(error) =
        BTreeMap::<String, IgnoredAny>::deserialize(ValueDeserializer::from(value.clone()))
    {
        return Err(unusable(error));
    }
    let DeValue::Table(instances) = value.into_inner() else {
        return Ok(BTreeMap::new());
    };
    Ok(instances
        .into_iter()
        .map(|(instance, table)| {
            let caps = BTreeMap::<String, usize>::deserialize(ValueDeserializer::from(table))
                .map_err(unusable);
            (instance.into_inner().into_owned(), caps)
        })
        .collect())
}

/// A table's caps, and the 1-based line of each key in `text`.
fn with_lines(
    text: &str,
    table: BTreeMap<Spanned<String>, usize>,
) -> (BTreeMap<String, usize>, BTreeMap<String, usize>) {
    let mut caps = BTreeMap::new();
    let mut lines = BTreeMap::new();
    for (key, cap) in table {
        let line = line_of(text, key.span().start);
        let key = key.into_inner();
        lines.insert(key.clone(), line);
        caps.insert(key, cap);
    }
    (caps, lines)
}

/// The 1-based line of byte `offset` in `text`.
fn line_of(text: &str, offset: usize) -> usize {
    text.get(..offset).unwrap_or_default().matches('\n').count() + 1
}
