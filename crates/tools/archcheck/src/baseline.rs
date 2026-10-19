//! `architecture-baseline.toml`: existing violations a rule tolerates for now.
//!
//! Each rule that needs a baseline owns a table: `[file_size]`, one
//! `[pattern_ratchet.<instance>]` per configured instance, and
//! `[crate_names]`. `cargo arch --write-baseline` updates and removes the
//! entries of every table, or of one with `--write-baseline=<table>`, but
//! never adds an entry or creates a `pattern_ratchet` table: those are hand
//! edits, which the base branch comparison then reports. (`[file_size]` and
//! `[crate_names]` are always written, empty if the file had none.) A missing
//! table means the rule tolerates nothing, which can only make checks
//! stricter. The base branch's copy, which a pull request's baseline is
//! compared with, is read leniently and table by table, so that a change to
//! the schema or a renamed instance does not break the comparison.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map},
    io,
    path::Path,
    path::PathBuf,
};

use serde::{Deserialize, Serialize, Serializer, de::IgnoredAny, ser};
use thiserror::Error;
use toml::{
    Spanned,
    de::{DeTable, DeValue, ValueDeserializer},
};

use crate::{
    context::{LoadError, parse_toml_with_text, read},
    rules::{self, CountRule, CrateNames, FileSize, PatternRatchet},
};

#[cfg(test)]
mod tests;

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

/// Written above `[crate_names]`, which follows the other tables.
const CRATE_NAMES_COMMENT: &str = "\
# [crate_names] maps each workspace package with a catch-all name (a part such
# as `common`, `utils` or `core`) to its member path. Only remove entries: a pull
# request that adds one is reported, and fails the check.
";

/// Every table of caps, by the name `--write-baseline=<table>` takes:
/// `<rule>`, or `<rule>.<instance>` for a rule with instances; and
/// `[crate_names]`, which maps names to paths. The default is the baseline
/// of an empty file: a `[file_size]` and a `[crate_names]` table without
/// entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Baseline {
    tables: BTreeMap<String, Table>,
    /// Package name to its member path, as `cargo metadata` reports it.
    /// Written by [`Baseline::write`] after the other tables.
    pub crate_names: BTreeMap<String, String>,
    /// The 1-based line of each `[crate_names]` key in the file the baseline
    /// was loaded from.
    pub crate_names_lines: BTreeMap<String, usize>,
}

/// One table: repository-relative path to the file's cap, its count when it
/// was listed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Table<V = usize> {
    caps: BTreeMap<String, V>,
    /// The 1-based line of each key in the file the baseline was loaded
    /// from, where a finding about the entry itself points.
    lines: BTreeMap<String, usize>,
}

impl<V> Default for Table<V> {
    fn default() -> Self {
        Self {
            caps: BTreeMap::new(),
            lines: BTreeMap::new(),
        }
    }
}

/// The baseline as written, with the position of each key.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineFile {
    #[serde(default)]
    file_size: BTreeMap<Spanned<String>, usize>,
    #[serde(default)]
    pattern_ratchet: BTreeMap<String, BTreeMap<Spanned<String>, usize>>,
    #[serde(default)]
    crate_names: BTreeMap<Spanned<String>, String>,
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
    /// Every table, by name as in [`Baseline`]: `[file_size]`, empty if the
    /// copy has none, and every `[pattern_ratchet.<instance>]`, configured or
    /// not.
    tables: BTreeMap<String, BaseTable>,
    /// Each rule whose tables were read: `Ok`, or, for a rule with instances
    /// whose key is not a table of tables, or for `[crate_names]`, why none
    /// of them can be read.
    rules: BTreeMap<&'static str, Result<(), Box<UnusableBase>>>,
    /// The package names `[crate_names]` lists; `None` when the copy has no
    /// such table, as before the rule existed.
    crate_names: Option<BTreeSet<String>>,
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
    /// [`Baseline::load_base`] does not read the rule's tables, which it
    /// only allows for a rule whose [`rules::Rule::baseline_rule`] is `None`,
    /// or one that is not registered.
    NotRead,
}

impl BaseSkip<'_> {
    /// The line reporting the skip of `behaviours`, such as
    /// `file_size.no_new_entries and file_size.no_raised_caps skipped`.
    pub fn message(&self, behaviours: &str) -> String {
        match self {
            Self::NotGiven => format!("{behaviours} (no --base-baseline given)"),
            Self::NotRead => {
                format!("{behaviours} (archcheck does not read these tables from a base baseline)")
            }
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

/// The table a baseline or base branch's copy without it checks or compares
/// as: no entries.
pub(crate) static NO_ENTRIES: BTreeMap<String, usize> = BTreeMap::new();

impl BaseBaseline {
    /// The base branch's table `name`, such as `file_size` or
    /// `pattern_ratchet.<instance>`, empty if it has none. Only the table
    /// asked for is read, so one for an instance this branch renamed or
    /// removed is ignored, even if it does not parse. A table of a rule
    /// [`Baseline::load_base`] did not read is not taken as missing.
    pub fn table(&self, name: &str) -> Result<&BTreeMap<String, usize>, BaseSkip<'_>> {
        let tables = self.tables()?;
        match tables.rules.get(rule_of(name)) {
            None => return Err(BaseSkip::NotRead),
            Some(Err(unusable)) => return Err(BaseSkip::Unusable(unusable)),
            Some(Ok(())) => {}
        }
        match tables.tables.get(name) {
            Some(table) => usable(table),
            None => Ok(&NO_ENTRIES),
        }
    }

    /// Every table the base branch's copy has, usable or not, by name as
    /// [`Self::table`] takes it; none of a rule whose key can't be read.
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables()
            .ok()
            .into_iter()
            .flat_map(|tables| tables.tables.keys().map(String::as_str))
    }

    /// The package names the base branch's `[crate_names]` lists, or `None`
    /// if it has no such table.
    pub fn crate_names(&self) -> Result<Option<&BTreeSet<String>>, BaseSkip<'_>> {
        let tables = self.tables()?;
        match tables.rules.get(CrateNames::NAME) {
            None => Err(BaseSkip::NotRead),
            Some(Err(unusable)) => Err(BaseSkip::Unusable(unusable)),
            Some(Ok(())) => Ok(tables.crate_names.as_ref()),
        }
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

impl Default for Baseline {
    fn default() -> Self {
        Self::from_file(BaselineFile::default(), "")
    }
}

impl Baseline {
    /// Parses `path`, keeping each key's line for findings about the entry.
    pub fn load(path: &Path) -> Result<Self, LoadError> {
        let (file, text): (BaselineFile, String) = parse_toml_with_text(path)?;
        Ok(Self::from_file(file, &text))
    }

    /// `file`'s tables, with the line of each key in `text`, which it was
    /// parsed from.
    fn from_file(file: BaselineFile, text: &str) -> Self {
        let mut tables = BTreeMap::new();
        tables.insert(FileSize::NAME.to_owned(), with_lines(text, file.file_size));
        for (instance, table) in file.pattern_ratchet {
            tables.insert(
                format!("{}.{instance}", PatternRatchet::NAME),
                with_lines(text, table),
            );
        }
        let crate_names = with_lines(text, file.crate_names);
        Self {
            tables,
            crate_names: crate_names.caps,
            crate_names_lines: crate_names.lines,
        }
    }

    /// The base branch's copy at `path`. Only an unreadable file is an
    /// error; one that is not TOML is [`BaseBaseline::Unusable`], and a table
    /// that does not parse is unusable on its own. Each rule's tables are read
    /// on their own, from its own key; a registered rule whose tables are not
    /// read is an error.
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
        let mut tables = BTreeMap::new();
        let mut read = BTreeMap::new();
        let file_size = toml::from_str::<BaseBaselineFile>(&text)
            .map(|file| file.file_size)
            .map_err(unusable);
        tables.insert(FileSize::NAME.to_owned(), file_size);
        read.insert(FileSize::NAME, Ok(()));
        let mut instances = Ok(());
        let mut crate_names = Ok(None);
        for (key, value) in document {
            match key.get_ref().as_ref() {
                PatternRatchet::NAME => {
                    instances = base_instances(value, &unusable).map(|instances| {
                        tables.extend(instances.into_iter().map(|(instance, table)| {
                            (format!("{}.{instance}", PatternRatchet::NAME), table)
                        }));
                    });
                }
                CrateNames::NAME => crate_names = base_crate_names(value).map(Some),
                _ => {}
            }
        }
        read.insert(PatternRatchet::NAME, instances);
        let crate_names = match crate_names {
            Ok(names) => {
                read.insert(CrateNames::NAME, Ok(()));
                names
            }
            Err(error) => {
                read.insert(CrateNames::NAME, Err(unusable(error)));
                None
            }
        };
        if let Some(rule) = rules::registered()
            .iter()
            .filter_map(|rule| rule.baseline_rule())
            .find(|rule| !read.contains_key(rule))
        {
            return Err(LoadError::UnreadBaseTables {
                path: path.to_path_buf(),
                rule,
            });
        }
        Ok(BaseBaseline::Loaded(BaseTables {
            tables,
            rules: read,
            crate_names,
        }))
    }

    /// Every table, by the name `--write-baseline=<table>` takes, in the
    /// order they are written: `file_size`, then `pattern_ratchet.<instance>`
    /// for each instance, then `crate_names`.
    pub fn table_names(&self) -> Vec<String> {
        let tables = self.tables.keys().cloned();
        tables
            .chain(std::iter::once(CrateNames::NAME.to_owned()))
            .collect()
    }

    /// The table of caps `name` names, if this baseline has it. `crate_names`,
    /// which maps names to paths, is not one.
    pub fn table(&self, name: &str) -> Option<&BTreeMap<String, usize>> {
        self.tables.get(name).map(|table| &table.caps)
    }

    /// The number of entries in the table `name` names, if this baseline has
    /// it.
    pub fn table_len(&self, name: &str) -> Option<usize> {
        if name == CrateNames::NAME {
            return Some(self.crate_names.len());
        }
        self.table(name).map(BTreeMap::len)
    }

    /// The 1-based line of `path`'s entry in table `name` of the file this
    /// baseline was loaded from.
    pub fn entry_line(&self, name: &str, path: &str) -> Option<usize> {
        self.tables.get(name)?.lines.get(path).copied()
    }

    /// Sets the entries of table `name`, creating it if this baseline lacks
    /// it.
    pub(crate) fn insert_table(&mut self, name: String, caps: BTreeMap<String, usize>) {
        self.tables.entry(name).or_default().caps = caps;
    }

    /// Replaces the table `name` names with `fresh`'s, leaving every other
    /// table as it is, and says whether it did: if `fresh` lacks the table,
    /// nothing changes.
    pub fn replace_table(&mut self, fresh: &Self, name: &str) -> bool {
        if name == CrateNames::NAME {
            self.crate_names = fresh.crate_names.clone();
            return true;
        }
        let Some(caps) = fresh.table(name) else {
            return false;
        };
        self.insert_table(name.to_owned(), caps.clone());
        true
    }

    /// Writes the header and every table in generated form: sorted keys, one
    /// table after another, `[crate_names]` last after its comment. A
    /// baseline loaded from a generated file and written back is
    /// byte-identical; hand-added comments, quoting or order are not kept.
    pub fn write(&self, path: &Path) -> Result<(), WriteBaselineError> {
        let crate_names =
            toml::to_string(&BTreeMap::from([(CrateNames::NAME, &self.crate_names)]))?;
        let text = format!(
            "{HEADER}\n{}\n{CRATE_NAMES_COMMENT}{crate_names}",
            toml::to_string(self)?
        );
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

/// The package names the base copy's `[crate_names]` lists; its values are
/// ignored.
fn base_crate_names(value: Spanned<DeValue<'_>>) -> Result<BTreeSet<String>, toml::de::Error> {
    BTreeMap::<String, IgnoredAny>::deserialize(ValueDeserializer::from(value))
        .map(|names| names.into_keys().collect())
}

/// The rule a table name belongs to: `<rule>` of `<rule>.<instance>`.
pub(crate) fn rule_of(table: &str) -> &str {
    table.split_once('.').map_or(table, |(rule, _)| rule)
}

/// A table's values, and the 1-based line of each key in `text`.
fn with_lines<V>(text: &str, entries: BTreeMap<Spanned<String>, V>) -> Table<V> {
    let mut table = Table::default();
    for (key, cap) in entries {
        let line = line_of(text, key.span().start);
        let key = key.into_inner();
        table.lines.insert(key.clone(), line);
        table.caps.insert(key, cap);
    }
    table
}

/// The tables as the file has them: `[<rule>]`, then each
/// `[<rule>.<instance>]` under the rule's key, which is left out if the rule
/// has no tables. A rule can't have both: TOML could not tell its entries
/// from its instances. `[crate_names]` is not among them: [`Baseline::write`]
/// writes it last, after its comment.
impl Serialize for Baseline {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(untagged)]
        enum Entry<'a> {
            Table(&'a BTreeMap<String, usize>),
            Instances(BTreeMap<&'a str, &'a BTreeMap<String, usize>>),
        }
        let mut rules: BTreeMap<&str, Entry<'_>> = BTreeMap::new();
        for (name, table) in &self.tables {
            let (rule, instance) = match name.split_once('.') {
                None => (name.as_str(), None),
                Some((rule, instance)) => (rule, Some(instance)),
            };
            match (rules.entry(rule), instance) {
                (btree_map::Entry::Vacant(entry), None) => {
                    entry.insert(Entry::Table(&table.caps));
                }
                (btree_map::Entry::Vacant(entry), Some(instance)) => {
                    entry.insert(Entry::Instances(BTreeMap::from([(instance, &table.caps)])));
                }
                (btree_map::Entry::Occupied(mut entry), Some(instance)) => {
                    let Entry::Instances(instances) = entry.get_mut() else {
                        return Err(both_forms(rule));
                    };
                    instances.insert(instance, &table.caps);
                }
                (btree_map::Entry::Occupied(_), None) => return Err(both_forms(rule)),
            }
        }
        rules.serialize(serializer)
    }
}

fn both_forms<E: ser::Error>(rule: &str) -> E {
    E::custom(format!(
        "[{rule}] and [{rule}.<instance>] tables can't both be written"
    ))
}

/// The 1-based line of byte `offset` in `text`.
pub(crate) fn line_of(text: &str, offset: usize) -> usize {
    text.get(..offset).unwrap_or_default().matches('\n').count() + 1
}
