//! No new catch-all crate names, such as `common`, `utils` or `core`.
//!
//! A name matches when one of its parts, once lowercased and split on `-` and
//! `_`, is a banned token; a crate matches when its package name or its
//! library's name does, unless `allow` names it. `no_new` reports a matching
//! crate that the baseline does not list at its path, and a baseline entry
//! the base branch's copy lacks, so the baseline is not a way around the rule;
//! without that copy's table, only [`BOOTSTRAP`] may be listed.
//! `stale_entries` asks for an entry or an `allow` name to be removed once it
//! no longer applies. `policy_changed` shows every change to
//! `[rules.crate_names]` compared with the base branch's `ARCHITECTURE.toml`,
//! so a pull request can't loosen the rule unnoticed.

use std::collections::BTreeSet;

use crate::{
    baseline::{BASELINE_FILE, BaseSkip, Baseline},
    config::{
        BaseArchitecture, CONFIG_FILE, CrateNamesConfig, CrateNamesPolicy, Mode, RulesConfig,
    },
    context::{Context, Crate},
    finding::{Finding, Location},
    rules::{NotAdded, Rule},
};

pub struct CrateNames;

impl CrateNames {
    /// The rule's name, as in its finding codes and baseline table.
    pub const NAME: &'static str = "crate_names";
}

const RULE: &str = CrateNames::NAME;
const NO_NEW: &str = "no_new";
const STALE_ENTRIES: &str = "stale_entries";
const POLICY_CHANGED: &str = "policy_changed";

/// The crates that matched when the rule was added. Without a base branch
/// copy of `[crate_names]` to compare with (before the table reached the
/// base branch, or when the base can't be resolved), these are the only
/// names the baseline may list.
pub const BOOTSTRAP: [&str; 16] = [
    "pplx-space-common-cli",
    "pplx-space-common-connection-admission",
    "pplx-space-common-core",
    "pplx-space-common-grpc",
    "pplx-space-common-grpc-lb",
    "pplx-space-common-io",
    "pplx-space-common-jobs",
    "pplx-space-common-lru-reaper",
    "pplx-space-common-proto-build",
    "pplx-space-common-svc",
    "pplx-space-common-work-coalescer",
    "pplx-space-credential-injection-core",
    "pplx-space-execution-common",
    "pplx-space-net-common",
    "pplx-space-node-runtime-core",
    "pplx-space-spaced-common",
];

/// Which name of a crate matched, and on which banned token.
struct CatchAll {
    token: String,
    /// The library's name, when it matched and the package name did not.
    lib: Option<String>,
}

/// The names a pull request's baseline may list.
enum Listable<'a> {
    /// The base branch's copy has the table.
    Base(&'a BTreeSet<String>),
    /// No copy of the table to compare with.
    Bootstrap,
    /// The base branch's table, or its whole copy, does not parse.
    Nothing,
}

impl Listable<'_> {
    fn contains(&self, name: &str) -> bool {
        match self {
            Self::Base(names) => names.contains(name),
            Self::Bootstrap => BOOTSTRAP.contains(&name),
            Self::Nothing => false,
        }
    }

    fn reason(&self) -> String {
        match self {
            Self::Base(_) => "not in the base branch's copy".to_owned(),
            Self::Bootstrap => format!(
                "not one of the {} crates listed when the rule was added, and there is no base \
                 branch copy of [crate_names] to compare with",
                BOOTSTRAP.len()
            ),
            Self::Nothing => {
                format!("the base branch's {BASELINE_FILE} or its [crate_names] does not parse")
            }
        }
    }
}

impl Rule for CrateNames {
    fn check(&self, ctx: &Context) -> Vec<Finding> {
        let config = &ctx.config.rules.crate_names;
        let listed = &ctx.baseline.crate_names;
        let mut findings = Vec::new();

        let mode = config.no_new.mode;
        for krate in &ctx.crates {
            let Some(catch_all) = catch_all(config, krate) else {
                continue;
            };
            let manifest = krate.manifest();
            let (detail, line) = match (listed.get(&krate.name), &catch_all.lib) {
                (Some(path), _) if *path == krate.member => continue,
                (Some(path), _) => (
                    format!(
                        ": `{}` is listed in {BASELINE_FILE} at {path}, but this crate is at {}. \
                         An entry only covers the crate at its path: if the listed crate moved \
                         here, update the entry's path; otherwise name this crate for what it \
                         does.",
                        krate.name, krate.member
                    ),
                    krate.name_line,
                ),
                (None, None) => (
                    format!(
                        ": `{}` uses `{}`, a catch-all name. {}",
                        krate.name,
                        catch_all.token,
                        advice()
                    ),
                    krate.name_line,
                ),
                (None, Some(lib)) => (
                    format!(
                        ": the library of `{}` is named `{lib}`, which uses `{}`, a catch-all \
                         name. {}",
                        krate.name,
                        catch_all.token,
                        advice()
                    ),
                    krate.lib_name_line.unwrap_or(krate.name_line),
                ),
            };
            findings.push(Finding {
                rule: RULE,
                instance: None,
                behaviour: NO_NEW,
                path: manifest.clone(),
                detail,
                mode,
                location: Location {
                    path: manifest,
                    line,
                },
            });
        }

        let listable = match ctx.base_baseline.crate_names() {
            Ok(Some(names)) => Listable::Base(names),
            Ok(None) | Err(BaseSkip::NotGiven | BaseSkip::NotRead) => Listable::Bootstrap,
            Err(BaseSkip::Unusable(_)) => Listable::Nothing,
        };
        for name in listed.keys().filter(|name| !listable.contains(name)) {
            findings.push(entry_finding(
                BASELINE_FILE,
                ctx.baseline.crate_names_lines.get(name).copied(),
                NO_NEW,
                mode,
                name,
                format!(
                    " is in [crate_names] in {BASELINE_FILE} but {}; catch-all crate names can't \
                     be added. Name the crate for what it does instead.",
                    listable.reason()
                ),
            ));
        }

        let mode = config.stale_entries.mode;
        for name in listed.keys() {
            let detail = match ctx.crates.iter().find(|krate| krate.name == *name) {
                None => format!(
                    ": listed in {BASELINE_FILE} but no workspace member has this name. Remove the \
                     entry; a renamed crate is checked under its new name."
                ),
                Some(krate) if catch_all(config, krate).is_none() => format!(
                    ": listed in {BASELINE_FILE} but no longer a catch-all name. Remove the entry."
                ),
                // At another path, it is a `no_new` finding.
                Some(_) => continue,
            };
            findings.push(entry_finding(
                BASELINE_FILE,
                ctx.baseline.crate_names_lines.get(name).copied(),
                STALE_ENTRIES,
                mode,
                name,
                detail,
            ));
        }
        for name in config.allow.iter() {
            let detail = match ctx.crates.iter().find(|krate| krate.name == *name) {
                None => format!(
                    ": in [rules.crate_names] allow in {CONFIG_FILE} but no workspace member has \
                     this name. Remove it from allow."
                ),
                Some(krate) if catch_all_unless_allowed(config, krate).is_none() => format!(
                    ": in [rules.crate_names] allow in {CONFIG_FILE} but not a catch-all name. \
                     Remove it from allow."
                ),
                Some(_) => continue,
            };
            let line = ctx.crate_names_policy.allow.get(name).copied();
            findings.push(entry_finding(
                CONFIG_FILE,
                line,
                STALE_ENTRIES,
                mode,
                name,
                detail,
            ));
        }

        if let BaseArchitecture::Loaded(base) = &ctx.base_architecture {
            let mode = config.policy_changed.mode;
            findings.extend(
                policy_changes(base.as_ref(), &ctx.crate_names_policy)
                    .into_iter()
                    .map(|(line, detail)| Finding {
                        rule: RULE,
                        instance: None,
                        behaviour: POLICY_CHANGED,
                        path: CONFIG_FILE.to_owned(),
                        detail,
                        mode,
                        location: Location {
                            path: CONFIG_FILE.to_owned(),
                            line,
                        },
                    }),
            );
        }

        findings
    }

    /// Not called for a base branch's copy that is not TOML at all; `no_new`
    /// then counts every entry as added, which its findings say.
    fn skipped(&self, ctx: &Context) -> Vec<String> {
        let bootstrap = format!(
            "{RULE}.{NO_NEW} allows only the {} crates listed when the rule was added in \
             [crate_names]",
            BOOTSTRAP.len()
        );
        match ctx.base_baseline.crate_names() {
            Ok(Some(_)) => Vec::new(),
            Ok(None) => vec![format!(
                "the base branch's {BASELINE_FILE} has no [crate_names] table; {bootstrap}"
            )],
            Err(skip @ (BaseSkip::NotGiven | BaseSkip::NotRead)) => vec![skip.message(&bootstrap)],
            Err(skip @ BaseSkip::Unusable(_)) => vec![skip.message(&format!(
                "{RULE}.{NO_NEW} counts every [crate_names] entry as added"
            ))],
        }
    }

    fn tables(&self, _config: &RulesConfig) -> Vec<String> {
        vec![RULE.to_owned()]
    }

    fn baseline_rule(&self) -> Option<&'static str> {
        Some(RULE)
    }

    fn other_skips(&self, ctx: &Context) -> Vec<String> {
        let policy = format!("{RULE}.{POLICY_CHANGED} skipped");
        match &ctx.base_architecture {
            BaseArchitecture::Loaded(_) => Vec::new(),
            BaseArchitecture::NotGiven => {
                vec![format!("{policy} (no --base-architecture given)")]
            }
            BaseArchitecture::Unusable(unusable) => {
                vec![BaseSkip::Unusable(unusable).message(&policy)]
            }
        }
    }

    /// Keeps each entry whose crate still matches at the entry's path. An
    /// entry whose crate is at another path is kept as it is: updating the
    /// path is a deliberate edit. A matching crate without an entry is not
    /// added: that takes a hand-written entry, which `no_new` then reports.
    fn update_baseline(&self, ctx: &Context, baseline: &mut Baseline) -> Vec<NotAdded> {
        let config = &ctx.config.rules.crate_names;
        let listed = &ctx.baseline.crate_names;
        let mut not_added = Vec::new();
        for krate in &ctx.crates {
            let Some(catch_all) = catch_all(config, krate) else {
                continue;
            };
            match listed.get(&krate.name) {
                Some(path) => {
                    if *path != krate.member {
                        not_added.push(not_added_line(format!(
                            "not updated: {} is listed at {path} but is at {}; update the entry's \
                             path by hand if it moved",
                            krate.name, krate.member
                        )));
                    }
                    baseline
                        .crate_names
                        .insert(krate.name.clone(), path.clone());
                }
                None => not_added.push(not_added_line(format!(
                    "not added: {} ({}) uses `{}`, a catch-all name; name the crate for what it \
                     does",
                    krate.name,
                    krate.manifest(),
                    catch_all.token
                ))),
            }
        }
        not_added
    }
}

/// A line for `--write-baseline` without an entry to paste: one added by
/// hand would be a `no_new` finding.
fn not_added_line(line: String) -> NotAdded {
    NotAdded {
        table: RULE.to_owned(),
        entry: None,
        line,
    }
}

fn advice() -> String {
    format!(
        "Name the crate for what it does, and split it if it holds unrelated things. Existing \
         catch-all crates are listed in {BASELINE_FILE}; new ones can't be added."
    )
}

/// How `krate` matches, unless `allow` names it.
fn catch_all(config: &CrateNamesConfig, krate: &Crate) -> Option<CatchAll> {
    if config.allow.contains(&krate.name) {
        return None;
    }
    catch_all_unless_allowed(config, krate)
}

/// How `krate` matches: its package name first, then its library's.
fn catch_all_unless_allowed(config: &CrateNamesConfig, krate: &Crate) -> Option<CatchAll> {
    if let Some(token) = banned_token(config, &krate.name) {
        return Some(CatchAll { token, lib: None });
    }
    let lib = krate.lib.as_ref()?;
    banned_token(config, lib).map(|token| CatchAll {
        token,
        lib: Some(lib.clone()),
    })
}

/// The first part of `name`, lowercased and split on `-` and `_`, that is a
/// banned token.
fn banned_token(config: &CrateNamesConfig, name: &str) -> Option<String> {
    name.to_lowercase()
        .split(['-', '_'])
        .find(|part| config.banned_tokens.contains(part))
        .map(str::to_owned)
}

/// Each difference of `policy` from `base` (`None`: the base has no table),
/// as the line it is shown on and the finding's detail.
fn policy_changes(
    base: Option<&CrateNamesPolicy>,
    policy: &CrateNamesPolicy,
) -> Vec<(usize, String)> {
    let table = "[rules.crate_names]";
    let tokens_line = policy.banned_tokens_line.unwrap_or(1);
    let Some(base) = base else {
        return vec![(
            tokens_line,
            format!(": {table} is not in the base branch's copy; the whole policy is new."),
        )];
    };
    let mut changes = Vec::new();
    for (token, &line) in &policy.banned_tokens {
        if !base.banned_tokens.contains_key(token) {
            changes.push((
                line,
                format!(": `{token}` was added to {table} banned_tokens."),
            ));
        }
    }
    for token in base.banned_tokens.keys() {
        if !policy.banned_tokens.contains_key(token) {
            changes.push((
                tokens_line,
                format!(
                    ": `{token}` was removed from {table} banned_tokens, so crate names using it \
                     are no longer checked."
                ),
            ));
        }
    }
    for (name, &line) in &policy.allow {
        if !base.allow.contains_key(name) {
            changes.push((
                line,
                format!(": `{name}` was added to {table} allow, exempting it from the rule."),
            ));
        }
    }
    for name in base.allow.keys() {
        if !policy.allow.contains_key(name) {
            changes.push((
                policy.allow_line.unwrap_or(1),
                format!(": `{name}` was removed from {table} allow."),
            ));
        }
    }
    let behaviours: BTreeSet<&String> = base.modes.keys().chain(policy.modes.keys()).collect();
    for behaviour in behaviours {
        let old = base.modes.get(behaviour).map(|(mode, _)| mode.as_str());
        let new = policy.modes.get(behaviour);
        if old != new.map(|(mode, _)| mode.as_str()) {
            changes.push((
                new.map_or(1, |(_, line)| *line),
                format!(
                    ": {RULE}.{behaviour} mode changed from {} to {}.",
                    old.unwrap_or("unset"),
                    new.map_or("unset", |(mode, _)| mode.as_str())
                ),
            ));
        }
    }
    changes
}

/// A finding about an entry of `file`, shown on its line.
fn entry_finding(
    file: &str,
    line: Option<usize>,
    behaviour: &'static str,
    mode: Mode,
    name: &str,
    detail: String,
) -> Finding {
    Finding {
        rule: RULE,
        instance: None,
        behaviour,
        path: name.to_owned(),
        detail,
        mode,
        location: Location {
            path: file.to_owned(),
            line: line.unwrap_or(1),
        },
    }
}
