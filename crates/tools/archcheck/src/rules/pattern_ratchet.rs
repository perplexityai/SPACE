//! Per-file ratchets on code lines that match a set of patterns.
//!
//! Each `[rules.pattern_ratchet.<instance>]` restricts code lines matching
//! any of its `patterns` to the files its `allow` globs match: a file outside
//! them with a matching line violates it, and the instance's own
//! `[pattern_ratchet.<instance>]` baseline table caps each listed file's
//! matching lines; [`super::ratchet`] has the behaviours. An optional `include`
//! list limits the instance to the files its globs match; it checks every
//! production file without one. `include_layers` and `allow_layers` do the
//! same by the layer of a file's crate, as the layers rule gives it, so that
//! an instance reads the same before and after a crate moves into its layer
//! directory. Instances are independent: each reads only its own
//! configuration and tables.

use crate::{
    baseline::BASELINE_FILE,
    config::{InstanceName, Layer, PatternRatchetConfig, RulesConfig},
    context::{Context, ProductionFile, code_lines},
    rules::{
        layers::file_layer,
        ratchet::{Count, CountRatchet, CountRule, Modes, Wording},
    },
};

pub struct PatternRatchet;

const RULE: &str = "pattern_ratchet";

impl CountRule for PatternRatchet {
    const NAME: &'static str = RULE;

    fn ratchets<'a>(&self, config: &'a RulesConfig) -> Vec<Box<dyn CountRatchet + 'a>> {
        config
            .pattern_ratchet
            .iter()
            .map(|(instance, config)| {
                Box::new(Instance { instance, config }) as Box<dyn CountRatchet + 'a>
            })
            .collect()
    }
}

struct Instance<'a> {
    instance: &'a InstanceName,
    config: &'a PatternRatchetConfig,
}

impl Instance<'_> {
    /// Whether `file`, whose crate is in `layer`, is among the paths and
    /// layers the instance checks. A crate without a layer is in none.
    fn checks(&self, file: &ProductionFile, layer: Option<Layer>) -> bool {
        let config = self.config;
        config
            .include
            .as_ref()
            .is_none_or(|include| include.is_match(&file.path))
            && config
                .include_layers
                .as_ref()
                .is_none_or(|include| layer.is_some_and(|layer| include.layers().contains(layer)))
    }

    /// Whether `file`, whose crate is in `layer`, may match freely.
    fn allows(&self, file: &ProductionFile, layer: Option<Layer>) -> bool {
        self.config.allow.is_match(&file.path)
            || layer.is_some_and(|layer| self.config.allow_layers.contains(layer))
    }

    /// The layer of `file`'s crate, looked up only for an instance that
    /// uses layers.
    fn layer(&self, ctx: &Context, file: &ProductionFile) -> Option<Layer> {
        let config = self.config;
        if config.include_layers.is_none() && config.allow_layers.is_empty() {
            return None;
        }
        file_layer(ctx, &file.path)
    }

    /// Where a match may go: `an allowed path`, or `an allowed path or
    /// layer` for an instance with `allow_layers`.
    fn allowed_place(&self) -> &'static str {
        if self.config.allow_layers.is_empty() {
            "an allowed path"
        } else {
            "an allowed path or layer"
        }
    }

    /// What the instance allows: `the paths`, or `the paths and layers`.
    fn allowed_scope(&self) -> &'static str {
        if self.config.allow_layers.is_empty() {
            "the paths"
        } else {
            "the paths and layers"
        }
    }

    /// The instance's baseline table and where it is.
    fn table(&self) -> String {
        format!("[{RULE}.{}] in {BASELINE_FILE}", self.instance)
    }
}

impl CountRatchet for Instance<'_> {
    fn instance(&self) -> Option<&str> {
        Some(self.instance.as_str())
    }

    fn modes(&self) -> Modes {
        let config = self.config;
        Modes {
            no_crossing: config.no_crossing.mode,
            no_new_entries: config.no_new_entries.mode,
            no_raised_caps: config.no_raised_caps.mode,
            listed_no_growth: config.listed_no_growth.mode,
            stale_entries: config.stale_entries.mode,
        }
    }

    /// `file`'s code lines that match the patterns, unless it is outside the
    /// checked paths and layers, allowed, or none do. Findings about them
    /// are shown on the first.
    fn measure(&self, ctx: &Context, file: &ProductionFile) -> Option<Count> {
        let layer = self.layer(ctx, file);
        if !self.checks(file, layer) || self.allows(file, layer) {
            return None;
        }
        let mut lines = code_lines(&file.text)
            .filter(|(_, line)| self.config.patterns.is_match(line))
            .map(|(number, _)| number);
        let first_line = lines.next()?;
        Some(Count {
            count: 1 + lines.count(),
            first_line,
        })
    }

    fn wording(&self) -> Wording {
        let instance = self.instance;
        let (scope, place) = (self.allowed_scope(), self.allowed_place());
        Wording {
            place: self.table(),
            violators: format!(
                "files outside {scope} [rules.{RULE}.{instance}] in ARCHITECTURE.toml allows"
            ),
            fix: format!("Move that code behind {place}"),
            unit: format!("code lines matching the {instance} patterns"),
            growth_fix: format!("move new matches behind {place}"),
            not_added_fix: format!("move them behind {place}"),
        }
    }

    fn crossing(&self, count: Count) -> String {
        let instance = self.instance;
        format!(
            ": {} the {instance} patterns, first on line {}, outside {} \
             [rules.{RULE}.{instance}] in ARCHITECTURE.toml allows. Move that code behind {}.",
            match_count(count.count),
            count.first_line,
            self.allowed_scope(),
            self.allowed_place(),
        )
    }

    fn cleared(&self, ctx: &Context, file: &ProductionFile) -> String {
        let instance = self.instance;
        let layer = self.layer(ctx, file);
        if !self.checks(file, layer) {
            let scope = if self.config.include_layers.is_some() {
                "the paths and layers"
            } else {
                "the paths"
            };
            return format!(
                " is outside {scope} [rules.{RULE}.{instance}] in ARCHITECTURE.toml includes"
            );
        }
        match layer.filter(|layer| self.config.allow_layers.contains(*layer)) {
            Some(layer) => format!(
                " is in a crate in {layer}, which [rules.{RULE}.{instance}] allow_layers in \
                 ARCHITECTURE.toml allows"
            ),
            None => format!(
                " has no code lines left matching the {instance} patterns outside its allowed {}",
                if self.config.allow_layers.is_empty() {
                    "paths"
                } else {
                    "paths and layers"
                }
            ),
        }
    }

    fn counted(&self, count: usize) -> String {
        format!(
            "{} matching the {} patterns",
            code_lines_text(count),
            self.instance
        )
    }
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
