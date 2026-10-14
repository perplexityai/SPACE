//! `ARCHITECTURE.toml`: which rules run, their parameters, and each
//! behaviour's mode.
//!
//! Every table denies unknown keys so a typo or a section for a rule that does
//! not exist is a configuration error rather than a silently skipped check.

use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::Path,
};

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use regex::RegexSet;
use serde::Deserialize;
use thiserror::Error;
use toml::Spanned;

use crate::{
    baseline::{UnusableBase, line_of},
    context::{LoadError, read},
};

pub const CONFIG_FILE: &str = "ARCHITECTURE.toml";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchitectureConfig {
    pub rules: RulesConfig,
}

/// One field per rule, named by the rule's id.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RulesConfig {
    pub file_size: FileSizeConfig,
    /// `[rules.pattern_ratchet.<instance>]` tables; none when absent.
    #[serde(default)]
    pub pattern_ratchet: BTreeMap<InstanceName, PatternRatchetConfig>,
    pub crate_names: CrateNamesConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSizeConfig {
    /// Code lines allowed per production Rust file.
    pub limit: usize,
    pub no_crossing: BehaviourConfig,
    pub no_new_entries: BehaviourConfig,
    pub no_raised_caps: BehaviourConfig,
    pub listed_no_growth: BehaviourConfig,
    pub stale_entries: BehaviourConfig,
}

/// One named instance of `pattern_ratchet`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatternRatchetConfig {
    /// A code line matching any of these regexes is restricted.
    pub patterns: Patterns,
    /// Repository-relative path globs whose files may match freely.
    pub allow: AllowList,
    pub no_crossing: BehaviourConfig,
    pub no_new_entries: BehaviourConfig,
    pub no_raised_caps: BehaviourConfig,
    pub listed_no_growth: BehaviourConfig,
    pub stale_entries: BehaviourConfig,
}

/// The name of a rule instance, as it appears in finding codes and baseline
/// table names: lowercase ASCII letters, digits and underscores, starting with
/// a letter.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(try_from = "String")]
pub struct InstanceName(String);

#[derive(Debug, Error)]
#[error(
    "instance name {0:?} must be lowercase ASCII letters, digits and underscores, starting with \
     a letter"
)]
pub struct InstanceNameError(String);

impl InstanceName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for InstanceName {
    type Error = InstanceNameError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        let mut chars = name.chars();
        let valid = chars.next().is_some_and(|c| c.is_ascii_lowercase())
            && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if valid {
            Ok(Self(name))
        } else {
            Err(InstanceNameError(name))
        }
    }
}

impl Borrow<str> for InstanceName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Regexes matched against each code line, without its line ending.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct Patterns(RegexSet);

#[derive(Debug, Error)]
pub enum PatternsError {
    #[error("patterns must list at least one regex")]
    Empty,
    #[error(transparent)]
    Regex(#[from] regex::Error),
}

impl Patterns {
    pub fn is_match(&self, line: &str) -> bool {
        self.0.is_match(line)
    }
}

impl TryFrom<Vec<String>> for Patterns {
    type Error = PatternsError;

    fn try_from(patterns: Vec<String>) -> Result<Self, Self::Error> {
        if patterns.is_empty() {
            return Err(PatternsError::Empty);
        }
        Ok(Self(RegexSet::new(patterns)?))
    }
}

/// Globs over repository-relative, `/`-separated paths. `*` and `?` stay
/// within one path component; `**` spans any number of them.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct AllowList(GlobSet);

impl AllowList {
    pub fn is_match(&self, path: &str) -> bool {
        self.0.is_match(path)
    }
}

impl TryFrom<Vec<String>> for AllowList {
    type Error = globset::Error;

    fn try_from(globs: Vec<String>) -> Result<Self, Self::Error> {
        let mut set = GlobSetBuilder::new();
        for glob in globs {
            set.add(GlobBuilder::new(&glob).literal_separator(true).build()?);
        }
        Ok(Self(set.build()?))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrateNamesConfig {
    pub banned_tokens: BannedTokens,
    pub allow: AllowedNames,
    pub no_new: BehaviourConfig,
    pub stale_entries: BehaviourConfig,
    pub policy_changed: BehaviourConfig,
}

/// Parts of a package name, split on `-` and `_`, that make it a catch-all
/// name. Each can match a part: non-empty, lowercase, without `-` or `_`.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct BannedTokens(Vec<String>);

#[derive(Debug, Error)]
pub enum BannedTokensError {
    #[error("banned_tokens is empty, so crate_names would check nothing")]
    Empty,
    #[error(
        "banned token {0:?} can never match: tokens are matched against the parts of a \
         lowercased name split on `-` and `_`, so each must be non-empty, lowercase, and \
         without `-` or `_`"
    )]
    Unmatchable(String),
}

impl TryFrom<Vec<String>> for BannedTokens {
    type Error = BannedTokensError;

    fn try_from(tokens: Vec<String>) -> Result<Self, Self::Error> {
        if tokens.is_empty() {
            return Err(BannedTokensError::Empty);
        }
        if let Some(token) = tokens.iter().find(|token| {
            token.is_empty() || token.contains(['-', '_']) || token.to_lowercase() != **token
        }) {
            return Err(BannedTokensError::Unmatchable(token.clone()));
        }
        Ok(Self(tokens))
    }
}

impl BannedTokens {
    pub fn contains(&self, part: &str) -> bool {
        self.0.iter().any(|token| token == part)
    }
}

/// Exact package names exempt from `crate_names` although a part is banned.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct AllowedNames(BTreeSet<String>);

#[derive(Debug, Error)]
pub enum AllowedNamesError {
    #[error("allow entry {0:?} is not a lowercase package name")]
    NotLowercase(String),
    #[error("allow lists {0:?} twice")]
    Duplicate(String),
}

impl TryFrom<Vec<String>> for AllowedNames {
    type Error = AllowedNamesError;

    fn try_from(names: Vec<String>) -> Result<Self, Self::Error> {
        let mut allow = BTreeSet::new();
        for name in names {
            if name.is_empty() || name.to_lowercase() != name {
                return Err(AllowedNamesError::NotLowercase(name));
            }
            if allow.contains(&name) {
                return Err(AllowedNamesError::Duplicate(name));
            }
            allow.insert(name);
        }
        Ok(Self(allow))
    }
}

impl AllowedNames {
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }
}

/// `[rules.crate_names]` as `crate_names.policy_changed` compares it: each
/// part, and the 1-based line it is written on. Read leniently, so that the
/// base branch's copy can have another schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrateNamesPolicy {
    /// The line of the `banned_tokens` key, if any.
    pub banned_tokens_line: Option<usize>,
    pub banned_tokens: BTreeMap<String, usize>,
    /// The line of the `allow` key, if any.
    pub allow_line: Option<usize>,
    pub allow: BTreeMap<String, usize>,
    /// Behaviour name to its mode, as written, and the mode's line.
    pub modes: BTreeMap<String, (String, usize)>,
}

#[derive(Deserialize)]
struct PolicyFile {
    #[serde(default)]
    rules: PolicyRules,
}

#[derive(Default, Deserialize)]
struct PolicyRules {
    crate_names: Option<PolicyTable>,
}

/// The base branch's `[rules.crate_names]`, given with `--base-architecture`.
#[derive(Debug, Default)]
pub enum BaseArchitecture {
    #[default]
    NotGiven,
    /// Not TOML, or a `[rules.crate_names]` of another shape.
    Unusable(UnusableBase),
    /// `None` when the copy has no `[rules.crate_names]`.
    Loaded(Option<CrateNamesPolicy>),
}

/// `flatten` and `untagged` would lose the spans, so each behaviour is a
/// field.
#[derive(Deserialize)]
struct PolicyTable {
    banned_tokens: Option<Spanned<Vec<Spanned<String>>>>,
    allow: Option<Spanned<Vec<Spanned<String>>>>,
    no_new: Option<PolicyBehaviour>,
    stale_entries: Option<PolicyBehaviour>,
    policy_changed: Option<PolicyBehaviour>,
}

#[derive(Deserialize)]
struct PolicyBehaviour {
    mode: Option<Spanned<String>>,
}

impl CrateNamesPolicy {
    /// `[rules.crate_names]` of `text`, or `None` without one. Other tables
    /// and unknown keys are ignored.
    pub fn parse(text: &str) -> Result<Option<Self>, toml::de::Error> {
        let file: PolicyFile = toml::from_str(text)?;
        let Some(table) = file.rules.crate_names else {
            return Ok(None);
        };
        let line = |span: std::ops::Range<usize>| line_of(text, span.start);
        let list = |list: Option<Spanned<Vec<Spanned<String>>>>| match list {
            None => (None, BTreeMap::new()),
            Some(list) => (
                Some(line(list.span())),
                list.into_inner()
                    .into_iter()
                    .map(|item| (item.get_ref().clone(), line(item.span())))
                    .collect(),
            ),
        };
        let (banned_tokens_line, banned_tokens) = list(table.banned_tokens);
        let (allow_line, allow) = list(table.allow);
        let modes = [
            ("no_new", table.no_new),
            ("stale_entries", table.stale_entries),
            ("policy_changed", table.policy_changed),
        ]
        .into_iter()
        .filter_map(|(behaviour, entry)| {
            let mode = entry?.mode?;
            Some((
                behaviour.to_owned(),
                (mode.get_ref().clone(), line(mode.span())),
            ))
        })
        .collect();
        Ok(Some(Self {
            banned_tokens_line,
            banned_tokens,
            allow_line,
            allow,
            modes,
        }))
    }

    /// The base branch's `ARCHITECTURE.toml` at `path`, given with
    /// `--base-architecture`. Only an unreadable file is an error; one that
    /// does not parse is [`BaseArchitecture::Unusable`].
    pub fn load_base(path: &Path) -> Result<BaseArchitecture, LoadError> {
        let text = read(path)?;
        Ok(match Self::parse(&text) {
            Ok(policy) => BaseArchitecture::Loaded(policy),
            Err(error) => BaseArchitecture::Unusable(UnusableBase {
                path: path.to_path_buf(),
                line: error.span().map(|span| line_of(&text, span.start)),
                error,
            }),
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BehaviourConfig {
    pub mode: Mode,
}

/// Whether a behaviour's findings fail the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Reported, never fails the check.
    Advisory,
    /// Reported and fails the check.
    Required,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Advisory => "advisory",
            Self::Required => "required",
        }
    }

    pub fn is_required(self) -> bool {
        matches!(self, Self::Required)
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
