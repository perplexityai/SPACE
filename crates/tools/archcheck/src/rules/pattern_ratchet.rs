//! Per-file ratchets on code lines that match a set of patterns.
//!
//! Each `[rules.pattern_ratchet.<instance>]` restricts code lines matching
//! any of its `patterns` to the files its `allow` globs match, and has a
//! `[pattern_ratchet.<instance>]` baseline table of its own. `no_crossing`
//! keeps files outside the allowed paths and the table free of matching
//! lines; `no_new_entries` and `no_raised_caps` report entries a pull request
//! adds to the table and caps it raises, compared with the base branch's
//! copy, so the table is not a way around `no_crossing`; `listed_no_growth`
//! keeps every listed file from gaining matching lines past its cap;
//! `stale_entries` asks for an entry to be lowered or removed once its file
//! has fewer matching lines, none, is allowed, or is gone. Instances are
//! independent: each reads only its own configuration and tables.

use std::collections::BTreeMap;

use crate::{
    baseline::{BASELINE_FILE, Baseline},
    config::{Mode, PatternRatchetConfig},
    context::{Context, ProductionFile, code_lines},
    finding::{Finding, Location},
    rules::{NotAdded, Rule},
};

pub struct PatternRatchet;

const RULE: &str = "pattern_ratchet";
const NO_CROSSING: &str = "no_crossing";
const NO_NEW_ENTRIES: &str = "no_new_entries";
const NO_RAISED_CAPS: &str = "no_raised_caps";
const LISTED_NO_GROWTH: &str = "listed_no_growth";
const STALE_ENTRIES: &str = "stale_entries";

/// The code lines of one file that an instance restricts.
#[derive(Clone, Copy)]
struct Matches {
    count: usize,
    first_line: usize,
}

impl Rule for PatternRatchet {
    fn check(&self, ctx: &Context) -> Vec<Finding> {
        ctx.config
            .rules
            .pattern_ratchet
            .iter()
            .flat_map(|(instance, config)| check_instance(ctx, instance.as_str(), config))
            .collect()
    }

    /// A line per configured instance whose comparisons are skipped, then
    /// one per table of the base branch's copy for an instance that is not
    /// configured, so that removing or renaming an instance shows in the log.
    fn skipped(&self, ctx: &Context) -> Vec<String> {
        let instances = &ctx.config.rules.pattern_ratchet;
        let skipped = instances.keys().filter_map(|instance| {
            let skip = ctx.base_baseline.pattern_ratchet(instance.as_str()).err()?;
            Some(skip.message(&format!("{} skipped", base_comparisons(instance.as_str()))))
        });
        let unconfigured = ctx
            .base_baseline
            .pattern_ratchet_instances()
            .filter(|instance| !instances.contains_key(*instance))
            .map(|instance| {
                format!("base baseline has [{RULE}.{instance}], which is not configured; skipped")
            });
        skipped.chain(unconfigured).collect()
    }

    fn base_comparisons(&self, ctx: &Context) -> Vec<String> {
        let instances = ctx.config.rules.pattern_ratchet.keys();
        instances
            .map(|instance| base_comparisons(instance.as_str()))
            .collect()
    }

    /// Keeps each listed file that still has matching lines, at its current
    /// count, even above its cap, which `no_raised_caps` then reports. A file
    /// without an entry is not added, and an instance without a table gets
    /// none: either takes a hand edit, which `no_new_entries` then reports.
    fn update_baseline(&self, ctx: &Context, baseline: &mut Baseline) -> Vec<NotAdded> {
        let mut not_added = Vec::new();
        for (instance, config) in &ctx.config.rules.pattern_ratchet {
            let listed = ctx.baseline.pattern_ratchet.get(instance.as_str());
            let advice = if listed.is_some() {
                "move them behind an allowed path, or add an entry by hand if this is a \
                 deliberate exception"
                    .to_owned()
            } else {
                format!(
                    "{BASELINE_FILE} has no [{RULE}.{instance}] table; for a new instance, add \
                     the table and its entries by hand"
                )
            };
            let mut table = BTreeMap::new();
            for file in &ctx.files {
                let Some(matches) = restricted(config, file) else {
                    continue;
                };
                if listed.is_some_and(|caps| caps.contains_key(&file.path)) {
                    table.insert(file.path.clone(), matches.count);
                } else {
                    not_added.push(NotAdded {
                        table: format!("{RULE}.{instance}"),
                        path: file.path.clone(),
                        count: matches.count,
                        line: format!(
                            "not added: {} ({} matching the {instance} patterns); {advice}",
                            file.path,
                            code_lines_text(matches.count),
                        ),
                    });
                }
            }
            if listed.is_some() {
                baseline.pattern_ratchet.insert(instance.to_string(), table);
            }
        }
        not_added
    }
}

/// The behaviours of `instance` that compare with the base branch's copy.
fn base_comparisons(instance: &str) -> String {
    let code = format!("{RULE}.{instance}");
    format!("{code}.{NO_NEW_ENTRIES} and {code}.{NO_RAISED_CAPS}")
}

fn check_instance(ctx: &Context, instance: &str, config: &PatternRatchetConfig) -> Vec<Finding> {
    let empty = BTreeMap::new();
    let caps = ctx.baseline.pattern_ratchet.get(instance).unwrap_or(&empty);
    let entry_lines = ctx.baseline.pattern_ratchet_lines.get(instance);
    let finding =
        |behaviour: &'static str, mode: Mode, path: &str, detail: String, location: Location| {
            Finding {
                rule: RULE,
                instance: Some(instance.to_owned()),
                behaviour,
                path: path.to_owned(),
                detail,
                mode,
                location,
            }
        };
    // A finding about an entry itself is shown on the entry's line.
    let at_entry = |path: &str| Location {
        path: BASELINE_FILE.to_owned(),
        line: entry_lines
            .and_then(|lines| lines.get(path))
            .copied()
            .unwrap_or(1),
    };
    // The first matching line, where a file's matches start.
    let at_first_match = |path: &str, matches: Matches| Location {
        path: path.to_owned(),
        line: matches.first_line,
    };
    let mut findings = Vec::new();

    let table = format!("[{RULE}.{instance}] in {BASELINE_FILE}");
    let mode = config.no_crossing.mode;
    let mut matched = BTreeMap::new();
    for file in &ctx.files {
        let Some(matches) = restricted(config, file) else {
            continue;
        };
        if !caps.contains_key(&file.path) {
            findings.push(finding(
                NO_CROSSING,
                mode,
                &file.path,
                format!(
                    ": {} the {instance} patterns, first on line {}, outside the paths \
                     [rules.{RULE}.{instance}] in ARCHITECTURE.toml allows. Move that code \
                     behind an allowed path. If the file was renamed, move its entry in {table} \
                     to the new path; {NO_NEW_ENTRIES} reports the moved entry as new, for a \
                     reviewer to accept.",
                    match_count(matches.count),
                    matches.first_line,
                ),
                at_first_match(&file.path, matches),
            ));
        }
        matched.insert(file.path.as_str(), matches);
    }

    if let Ok(base) = ctx.base_baseline.pattern_ratchet(instance) {
        let mode = config.no_new_entries.mode;
        for path in caps.keys().filter(|path| !base.contains_key(*path)) {
            findings.push(finding(
                NO_NEW_ENTRIES,
                mode,
                path,
                format!(
                    " is in {table} but not in the base branch's copy; files outside the paths \
                     [rules.{RULE}.{instance}] in ARCHITECTURE.toml allows can't be added (a \
                     renamed file counts as added). Move that code behind an allowed path \
                     instead."
                ),
                at_entry(path),
            ));
        }
        let mode = config.no_raised_caps.mode;
        for (path, &cap) in caps {
            if let Some(&base_cap) = base.get(path)
                && cap > base_cap
            {
                findings.push(finding(
                    NO_RAISED_CAPS,
                    mode,
                    path,
                    format!(
                        ": cap raised from {base_cap} to {cap} in {table}; caps should only go \
                         down. Move that code behind an allowed path instead."
                    ),
                    at_entry(path),
                ));
            }
        }
    }

    let growth_mode = config.listed_no_growth.mode;
    let stale_mode = config.stale_entries.mode;
    let mut stale = Vec::new();
    for (path, &cap) in caps {
        // The file is gone, so the finding points at its entry.
        if ctx.file(path).is_none() {
            stale.push(finding(
                STALE_ENTRIES,
                stale_mode,
                path,
                format!(
                    ": listed in {table} but no longer a production Rust file. Remove the \
                     entry; a renamed file is checked under its new path."
                ),
                at_entry(path),
            ));
            continue;
        }
        match matched.get(path.as_str()).copied() {
            None => stale.push(finding(
                STALE_ENTRIES,
                stale_mode,
                path,
                format!(
                    " has no code lines left matching the {instance} patterns outside its \
                     allowed paths. Remove it from {table}."
                ),
                Location {
                    path: path.clone(),
                    line: 1,
                },
            )),
            Some(matches) if matches.count > cap => {
                let advice = if growth_mode.is_required() {
                    "Move new matches behind an allowed path."
                } else {
                    "Once this rule is required, move new matches behind an allowed path."
                };
                findings.push(finding(
                    LISTED_NO_GROWTH,
                    growth_mode,
                    path,
                    format!(
                        " grew from {cap} to {} code lines matching the {instance} patterns. \
                         {advice}",
                        matches.count,
                    ),
                    at_first_match(path, matches),
                ));
            }
            Some(matches) if matches.count < cap => stale.push(finding(
                STALE_ENTRIES,
                stale_mode,
                path,
                format!(
                    " shrank from {cap} to {count} code lines matching the {instance} patterns. \
                     Lower its cap in {table} to {count}.",
                    count = matches.count,
                ),
                at_first_match(path, matches),
            )),
            Some(_) => {}
        }
    }
    // Each behaviour's findings stay contiguous for the human report.
    findings.append(&mut stale);

    findings
}

/// `file`'s code lines that match `config`'s patterns, unless its path is
/// allowed or none do.
fn restricted(config: &PatternRatchetConfig, file: &ProductionFile) -> Option<Matches> {
    if config.allow.is_match(&file.path) {
        return None;
    }
    let mut lines = code_lines(&file.text)
        .filter(|(_, line)| config.patterns.is_match(line))
        .map(|(number, _)| number);
    let first_line = lines.next()?;
    Some(Matches {
        count: 1 + lines.count(),
        first_line,
    })
}

/// `1 code line matches` or `2 code lines match`.
fn match_count(count: usize) -> String {
    if count == 1 {
        "1 code line matches".to_owned()
    } else {
        format!("{count} code lines match")
    }
}

/// `1 code line` or `2 code lines`.
fn code_lines_text(count: usize) -> String {
    if count == 1 {
        "1 code line".to_owned()
    } else {
        format!("{count} code lines")
    }
}
