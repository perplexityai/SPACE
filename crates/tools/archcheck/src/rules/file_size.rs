//! A code-line limit on production Rust files.
//!
//! `no_crossing` keeps files outside the baseline under the limit;
//! `no_new_entries` reports entries a pull request adds to the baseline, so
//! the baseline is not a way around `no_crossing`; `listed_no_growth` keeps
//! every baseline entry from growing past its cap; `stale_entries` asks for
//! an entry to be lowered or removed once its file shrinks, falls under the
//! limit, or is gone. The last two are separate so that requiring no growth
//! does not also fail the change that shrinks a file.

use crate::{
    baseline::{BASELINE_FILE, Baseline},
    config::Mode,
    context::Context,
    finding::{Finding, Location},
    rules::Rule,
};

pub struct FileSize;

const RULE: &str = "file_size";
const NO_CROSSING: &str = "no_crossing";
const NO_NEW_ENTRIES: &str = "no_new_entries";
const LISTED_NO_GROWTH: &str = "listed_no_growth";
const STALE_ENTRIES: &str = "stale_entries";

impl Rule for FileSize {
    fn check(&self, ctx: &Context) -> Vec<Finding> {
        let config = &ctx.config.rules.file_size;
        let limit = config.limit;
        let caps = &ctx.baseline.file_size;
        let mut findings = Vec::new();

        let mode = config.no_crossing.mode;
        for file in &ctx.files {
            if file.code_lines > limit && !caps.contains_key(&file.path) {
                findings.push(finding(
                    NO_CROSSING,
                    mode,
                    &file.path,
                    format!(
                        ": {} code lines, over the {}-line limit (comments and blank lines are \
                         not counted). Split it into submodules by responsibility.",
                        file.code_lines,
                        thousands(limit),
                    ),
                ));
            }
        }

        if let Some(base) = &ctx.base_baseline {
            let mode = config.no_new_entries.mode;
            for path in caps
                .keys()
                .filter(|path| !base.file_size.contains_key(*path))
            {
                findings.push(Finding {
                    location: entry_location(ctx, path),
                    ..finding(
                        NO_NEW_ENTRIES,
                        mode,
                        path,
                        format!(
                            " was added to {BASELINE_FILE}; new files must stay under the {}-line \
                             limit. Split the file instead.",
                            thousands(limit),
                        ),
                    )
                });
            }
        }

        let growth_mode = config.listed_no_growth.mode;
        let stale_mode = config.stale_entries.mode;
        let mut stale = Vec::new();
        for (path, &cap) in caps {
            match ctx.file(path).map(|file| file.code_lines) {
                // The file is gone, so the finding points at its entry.
                None => stale.push(Finding {
                    location: entry_location(ctx, path),
                    ..finding(
                        STALE_ENTRIES,
                        stale_mode,
                        path,
                        format!(
                            ": listed in {BASELINE_FILE} but no longer a production Rust file. \
                             Remove the entry; a renamed file is checked under its new path."
                        ),
                    )
                }),
                Some(lines) if lines <= limit => stale.push(finding(
                    STALE_ENTRIES,
                    stale_mode,
                    path,
                    format!(
                        " is down to {lines} code lines, within the {}-line limit. Remove it \
                         from {BASELINE_FILE}.",
                        thousands(limit),
                    ),
                )),
                Some(lines) if lines > cap => {
                    let advice = if growth_mode.is_required() {
                        "Split the file before adding to it."
                    } else {
                        "Once this rule is required, split the file before adding to it."
                    };
                    findings.push(finding(
                        LISTED_NO_GROWTH,
                        growth_mode,
                        path,
                        format!(" grew from {cap} to {lines} code lines. {advice}"),
                    ));
                }
                Some(lines) if lines < cap => stale.push(finding(
                    STALE_ENTRIES,
                    stale_mode,
                    path,
                    format!(
                        " shrank from {cap} to {lines} code lines. Lower its cap in \
                         {BASELINE_FILE} to {lines}."
                    ),
                )),
                Some(_) => {}
            }
        }
        // Each behaviour's findings stay contiguous for the human report.
        findings.append(&mut stale);

        findings
    }

    fn skipped(&self, ctx: &Context) -> Vec<String> {
        match ctx.base_baseline {
            Some(_) => Vec::new(),
            None => vec![format!(
                "{RULE}.{NO_NEW_ENTRIES} skipped (no --base-baseline given)"
            )],
        }
    }

    /// Keeps each listed file still over the limit, at its current size. A
    /// file without an entry is not added: that takes a hand-written entry,
    /// which `no_new_entries` then reports.
    fn update_baseline(&self, ctx: &Context, baseline: &mut Baseline) -> Vec<String> {
        let limit = ctx.config.rules.file_size.limit;
        let mut not_added = Vec::new();
        for file in ctx.files.iter().filter(|file| file.code_lines > limit) {
            if ctx.baseline.file_size.contains_key(&file.path) {
                baseline
                    .file_size
                    .insert(file.path.clone(), file.code_lines);
            } else {
                not_added.push(format!(
                    "not added: {} ({} code lines); split it, or add an entry by hand if this is \
                     a deliberate exception",
                    file.path, file.code_lines
                ));
            }
        }
        not_added
    }
}

/// An entry's line in the baseline, for findings about the entry itself.
fn entry_location(ctx: &Context, path: &str) -> Location {
    Location {
        path: BASELINE_FILE.to_owned(),
        line: ctx.baseline.file_size_lines.get(path).copied().unwrap_or(1),
    }
}

/// A finding shown on line 1 of the file it is about.
fn finding(behaviour: &'static str, mode: Mode, path: &str, detail: String) -> Finding {
    Finding {
        rule: RULE,
        behaviour,
        path: path.to_owned(),
        detail,
        mode,
        location: Location {
            path: path.to_owned(),
            line: 1,
        },
    }
}

/// `1000` as `1,000`.
fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}
