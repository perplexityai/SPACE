//! `storage_access` and `codec_features`: nothing above Storage talks to a
//! database or object storage directly, Domain never does, and Foundation
//! does only in a storage helper, which names nothing SPACE-specific.
//!
//! The drivers are the packages `[rules.layers.storage] drivers` lists. A
//! storage helper is a Foundation crate with a required (normal or build,
//! not optional) dependency on a driver, or one `helpers` lists, such as a
//! connector for a driver's client that does not depend on the driver
//! itself. A Foundation crate `[rules.layers] legacy` lists is never a
//! helper, so it is checked like any other; test support and tools are left
//! out.
//!
//! - `storage_access`: only stores and helpers may have a normal or build
//!   dependency on a driver or helper. An optional one counts too, except
//!   from a Foundation crate, where it is a codec behind a feature, such as
//!   `sqlx` impls for an identifier type, whose enabling `codec_features`
//!   covers.
//! - `codec_features`: a driver feature of a crate outside Storage, one that
//!   turns on an optional dependency on a driver or helper, itself, through
//!   the features it requests of an optional dependency, or by forwarding to
//!   another crate's driver feature, may be enabled only by stores and
//!   helpers: with `features = […]` on the dependency, through its default
//!   features, or by forwarding from `[features]`. A Foundation crate's
//!   enabling behind a feature of its own, on an optional dependency or by
//!   forwarding, is exempt as its optional driver dependency is: that
//!   feature is a driver feature, whose enabling is checked. A store's own
//!   driver features are not checked: depending on a store is the way to
//!   reach storage.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    Crossing, GUIDE, RULE, config_finding,
    graph::{Graph, Member, list, used_items},
};
use crate::{
    config::{CONFIG_FILE, Layer, LayersConfig},
    context::{Context, Crate, Dependency},
    finding::{Finding, Location},
};

pub(super) const STORAGE_ACCESS: &str = "storage_access";
pub(super) const CODEC_FEATURES: &str = "codec_features";

/// Why a crate is a storage helper.
pub(super) enum Helper {
    /// Its required dependency on this driver.
    Derived(String),
    /// `[rules.layers.storage] helpers` lists it.
    Listed,
}

/// What a driver feature turns on.
#[derive(Clone)]
struct Reach {
    /// The driver or helper it reaches.
    target: String,
    /// The other crate's driver feature it forwards to, as
    /// `<package>/<feature>`, when it reaches the target through one.
    via: Option<String>,
}

/// One way a crate enables another crate's feature.
struct Enabling<'b> {
    /// The crate whose feature it enables.
    target: &'b str,
    feature: String,
    line: usize,
    /// How, for the finding.
    how: String,
    /// Whether only a feature of the crate's own turns it on: an optional
    /// dependency's, or a `[features]` item's.
    behind_feature: bool,
}

/// The storage checks' verdicts, worked out once for checking and for
/// `--write-baseline`.
pub(super) struct Storage<'a> {
    pub helpers: BTreeMap<&'a str, Helper>,
    /// Each driver feature of each crate outside Storage, by crate and
    /// feature.
    features: BTreeMap<&'a str, BTreeMap<String, Reach>>,
    /// `storage_access` violations, as `(from, driver or helper)`.
    pub access: BTreeMap<(String, String), Crossing>,
    /// `codec_features` violations, as `(from, "<package>/<feature>")`.
    pub codecs: BTreeMap<(String, String), Crossing>,
}

/// Whether the storage checks leave `member` out: it has no layer, or is
/// test support or a tool.
fn left_out(member: &Member<'_>) -> Option<&'static str> {
    match member.layer {
        None => Some("has no layer"),
        Some(Layer::TestSupport) => Some("is test support"),
        Some(Layer::Tools) => Some("is a tool"),
        Some(_) => None,
    }
}

/// The normal and build dependencies of `member`.
fn production<'a>(member: &Member<'a>) -> impl Iterator<Item = &'a Dependency> + use<'a> {
    let krate: &'a Crate = member.krate;
    krate
        .dependencies
        .iter()
        .filter(|dependency| !dependency.is_dev())
}

impl<'a> Storage<'a> {
    pub fn new(ctx: &Context, config: &LayersConfig, graph: &'a Graph<'a>) -> Self {
        let drivers = &config.storage.drivers;
        let mut helpers = BTreeMap::new();
        for member in graph.members.values() {
            if member.layer != Some(Layer::Foundation) || config.legacy.contains(member.name()) {
                continue;
            }
            let driver = production(member)
                .find(|dependency| !dependency.optional && drivers.contains(&dependency.package));
            if let Some(driver) = driver {
                helpers.insert(member.name(), Helper::Derived(driver.package.clone()));
            } else if config.storage.helpers.contains(member.name()) {
                helpers.insert(member.name(), Helper::Listed);
            }
        }
        let mut storage = Self {
            helpers,
            features: BTreeMap::new(),
            access: BTreeMap::new(),
            codecs: BTreeMap::new(),
        };
        storage.features = storage.driver_features(config, graph);
        for member in graph.members.values() {
            if left_out(member).is_some() || storage.may_use(member) {
                continue;
            }
            storage.check_access(ctx, config, graph, member);
            storage.check_codecs(graph, member);
        }
        storage
    }

    /// Whether `member` is a store or a storage helper, which may use
    /// drivers and helpers and enable driver features.
    fn may_use(&self, member: &Member<'_>) -> bool {
        member.layer == Some(Layer::Storage) || self.helpers.contains_key(member.name())
    }

    /// Whether `package` is a driver or a storage helper.
    fn is_target(&self, config: &LayersConfig, package: &str) -> bool {
        config.storage.drivers.contains(package) || self.helpers.contains_key(package)
    }

    /// Each crate's driver features, outside Storage, to a fixpoint: a
    /// feature that turns on an optional dependency on a driver or helper,
    /// or forwards to a feature that does, its own or another such crate's.
    fn driver_features(
        &self,
        config: &LayersConfig,
        graph: &'a Graph<'a>,
    ) -> BTreeMap<&'a str, BTreeMap<String, Reach>> {
        let members: Vec<&Member<'a>> = graph
            .members
            .values()
            .filter(|member| member.layer != Some(Layer::Storage))
            .collect();
        let tables: BTreeMap<&str, BTreeMap<String, Vec<String>>> = members
            .iter()
            .map(|member| (member.name(), features_of(member)))
            .collect();
        let mut found: BTreeMap<&'a str, BTreeMap<String, Reach>> = BTreeMap::new();
        loop {
            let mut changed = false;
            for member in &members {
                let Some(table) = tables.get(member.name()) else {
                    continue;
                };
                for (feature, enables) in table {
                    if found
                        .get(member.name())
                        .is_some_and(|known| known.contains_key(feature))
                    {
                        continue;
                    }
                    let reach = enables
                        .iter()
                        .find_map(|item| self.reach(config, graph, &found, member, item));
                    if let Some(reach) = reach {
                        found
                            .entry(member.name())
                            .or_default()
                            .insert(feature.clone(), reach);
                        changed = true;
                    }
                }
            }
            if !changed {
                return found;
            }
        }
    }

    /// What one item of `member`'s feature turns on, if it reaches a driver
    /// or helper: `dep:<key>` or `<key>/<feature>` of an optional dependency
    /// on one, or of an optional dependency whose `features` or default
    /// features include a driver feature; `<key>/<feature>` or
    /// `<key>?/<feature>` of another crate's driver feature; or another of
    /// its own driver features.
    fn reach(
        &self,
        config: &LayersConfig,
        graph: &Graph<'_>,
        found: &BTreeMap<&str, BTreeMap<String, Reach>>,
        member: &Member<'_>,
        item: &str,
    ) -> Option<Reach> {
        let direct = |key: &str| {
            production(member)
                .find(|dependency| {
                    dependency.key() == key
                        && dependency.optional
                        && self.is_target(config, &dependency.package)
                })
                .map(|dependency| Reach {
                    target: dependency.package.clone(),
                    via: None,
                })
        };
        // Turning on an optional dependency turns on the features it
        // requests of it too.
        let requested = |key: &str| {
            production(member)
                .filter(|dependency| dependency.key() == key && dependency.optional)
                .find_map(|dependency| {
                    let target = graph.member(&dependency.package)?;
                    let known = found.get(target.name())?;
                    dependency
                        .features
                        .iter()
                        .map(String::as_str)
                        .chain(dependency.uses_default_features.then_some("default"))
                        .find_map(|feature| {
                            Some(Reach {
                                target: known.get(feature)?.target.clone(),
                                via: Some(format!("{}/{feature}", target.name())),
                            })
                        })
                })
        };
        if let Some(key) = item.strip_prefix("dep:") {
            return direct(key).or_else(|| requested(key));
        }
        let Some((key, feature)) = item.split_once('/') else {
            let reach = found.get(member.name())?.get(item)?;
            return Some(Reach {
                target: reach.target.clone(),
                via: Some(format!("{}/{item}", member.name())),
            });
        };
        let weak = key.ends_with('?');
        let key = key.trim_end_matches('?');
        if !weak && let Some(reach) = direct(key) {
            return Some(reach);
        }
        production(member)
            .filter(|dependency| dependency.key() == key)
            .find_map(|dependency| {
                let target = graph.member(&dependency.package)?;
                let reach = found.get(target.name())?.get(feature)?;
                Some(Reach {
                    target: reach.target.clone(),
                    via: Some(format!("{}/{feature}", target.name())),
                })
            })
            .or_else(|| if weak { None } else { requested(key) })
    }

    /// `storage_access` for `member`, which is neither left out, a store
    /// nor a helper.
    fn check_access(
        &mut self,
        ctx: &Context,
        config: &LayersConfig,
        graph: &Graph<'_>,
        member: &Member<'_>,
    ) {
        let Some(layer) = member.layer else { return };
        let from = member.name();
        for dependency in production(member) {
            let to = &dependency.package;
            if !self.is_target(config, to) || (dependency.optional && layer == Layer::Foundation) {
                continue;
            }
            let key = (from.to_owned(), to.clone());
            if self.access.contains_key(&key) {
                continue;
            }
            let (what, lib) = match (self.helpers.get(to.as_str()), graph.member(to)) {
                (Some(Helper::Derived(driver)), Some(helper)) => (
                    format!("a storage helper (it has a required dependency on {driver})"),
                    helper.krate.lib.clone().unwrap_or_default(),
                ),
                (Some(_), Some(helper)) => (
                    format!("a storage helper ([rules.{RULE}.storage] helpers lists it)"),
                    helper.krate.lib.clone().unwrap_or_default(),
                ),
                _ => (
                    format!("a driver ([rules.{RULE}.storage] drivers lists it)"),
                    to.replace('-', "_"),
                ),
            };
            let items = used_items(&ctx.files, member.krate, dependency, &lib);
            let items = if items.is_empty() {
                String::new()
            } else {
                format!(" ({})", list(&items, 5))
            };
            let no_helper = format!(
                "Moving it into a new storage helper does not let {from} depend on that helper: \
                 {RULE}.{STORAGE_ACCESS} checks that edge too."
            );
            let fixes = match layer {
                Layer::Domain => format!(
                    "Fix: move the code that uses it{items}, such as a codec's impls, into the \
                     store that persists these types, where a wrapper type can carry them."
                ),
                Layer::Foundation if config.legacy.contains(from) => format!(
                    "Fix: move the code that uses it{items} into a store or a storage helper, \
                     as splitting {from} up would: [rules.{RULE}] legacy lists it, so it is no \
                     storage helper itself."
                ),
                Layer::Foundation => format!(
                    "Fix one of: if {from} names nothing SPACE-specific, list it in \
                     [rules.{RULE}.storage] helpers in {CONFIG_FILE}, which \
                     {RULE}.policy_changed reports; or move the code that uses it{items} into a \
                     store or a storage helper."
                ),
                // Neither an app nor a client may use a store: an app goes
                // through the API crate that owns the state, a client
                // through its server's protocol.
                Layer::App => format!(
                    "Fix: an app reaches state only through the l40_api crate that owns it: move \
                     the code that uses it{items} into that crate, or a store it uses, and call \
                     the API crate. {no_helper}"
                ),
                _ if member.is_client() => format!(
                    "Fix: a client reaches state only through its server's protocol, and never \
                     depends on a store: move the code that uses it{items} into the server, \
                     behind a store, and add a call for it to the protocol. {no_helper}"
                ),
                _ => format!(
                    "Fix one of: move the code that uses it{items} into a store in l30_storage \
                     that lists {from} in [rules.{RULE}] store_users, and call the store; or, \
                     for a generic mechanism that names nothing SPACE-specific, into a storage \
                     helper in l10_foundation that such a store calls. {no_helper}"
                ),
            };
            let optional = if dependency.optional {
                " (optional, so a feature turns it on)"
            } else {
                ""
            };
            let detail = format!(
                ": {from} ({layer}) depends on {to}, {what}, {}{optional}, but only l30_storage crates and \
                 storage helpers may: nothing above l30_storage reaches a database or object \
                 storage directly, l20_domain never does, and l10_foundation does only in a \
                 helper that names nothing SPACE-specific. {fixes} See {GUIDE}.",
                dependency.describe(),
            );
            self.access.insert(
                key,
                Crossing {
                    location: Location {
                        path: member.krate.manifest(),
                        line: dependency.line,
                    },
                    detail,
                    summary: format!("{from} ({layer}) -> {to}"),
                },
            );
        }
    }

    /// Each feature of a crate with driver features that `member` enables:
    /// on a normal or build dependency, or by forwarding from `[features]`.
    fn enablings<'b>(&self, graph: &'b Graph<'_>, member: &Member<'_>) -> Vec<Enabling<'b>> {
        let mut enabled = Vec::new();
        for dependency in production(member) {
            let Some(target) = self.featured(graph, &dependency.package) else {
                continue;
            };
            let declared = dependency.describe();
            for feature in &dependency.features {
                enabled.push(Enabling {
                    target,
                    feature: feature.clone(),
                    line: dependency.line,
                    how: format!("with features = [\"{feature}\"] on {declared}"),
                    behind_feature: dependency.optional,
                });
            }
            if dependency.uses_default_features {
                enabled.push(Enabling {
                    target,
                    feature: "default".to_owned(),
                    line: dependency.line,
                    how: format!("through the default features of {declared}"),
                    behind_feature: dependency.optional,
                });
            }
        }
        for (own, items) in &member.krate.features {
            for (item, line) in items {
                let Some((key, feature)) = item.split_once('/') else {
                    continue;
                };
                let key = key.trim_end_matches('?');
                for dependency in production(member).filter(|dependency| dependency.key() == key) {
                    if let Some(target) = self.featured(graph, &dependency.package) {
                        enabled.push(Enabling {
                            target,
                            feature: feature.to_owned(),
                            line: *line,
                            how: format!("by forwarding \"{item}\" from its own feature `{own}`"),
                            behind_feature: true,
                        });
                    }
                }
            }
        }
        enabled
    }

    /// `codec_features` for `member`, which is neither left out, a store
    /// nor a helper. A Foundation crate's enabling behind a feature of its
    /// own is exempt, as its optional driver dependency is: that feature is
    /// then a driver feature itself, whose enabling this checks.
    fn check_codecs(&mut self, graph: &Graph<'_>, member: &Member<'_>) {
        let Some(layer) = member.layer else { return };
        let from = member.name();
        for enabling in self.enablings(graph, member) {
            if enabling.behind_feature && layer == Layer::Foundation {
                continue;
            }
            let Enabling {
                target,
                feature,
                line,
                how,
                ..
            } = enabling;
            let Some(reach) = self
                .features
                .get(target)
                .and_then(|features| features.get(&feature))
            else {
                continue;
            };
            let entry = format!("{target}/{feature}");
            let key = (from.to_owned(), entry.clone());
            if self.codecs.contains_key(&key) {
                continue;
            }
            let turns_on = match &reach.via {
                None => format!("turns on its optional dependency on {}", reach.target),
                Some(via) => format!("reaches {} through {via}", reach.target),
            };
            let detail = format!(
                ": {from} ({layer}) enables the `{feature}` feature of {target}, {how}, which \
                 {turns_on}, but only l30_storage crates and storage helpers may turn on a driver \
                 feature. Fix: drop the feature, move the code that needs it, such as a codec or \
                 a client, into a store in l30_storage (which may enable the feature itself) or a \
                 storage helper, and reach the data through that. See {GUIDE}.",
            );
            self.codecs.insert(
                key,
                Crossing {
                    location: Location {
                        path: member.krate.manifest(),
                        line,
                    },
                    detail,
                    summary: format!("{from} ({layer}) -> {entry}"),
                },
            );
        }
    }

    /// The member `package` names, if it has driver features: a crate
    /// outside Storage.
    fn featured<'b>(&self, graph: &'b Graph<'_>, package: &str) -> Option<&'b str> {
        let member = graph.member(package)?;
        self.features
            .contains_key(member.name())
            .then_some(member.name())
    }

    /// Why `(from, to)` in `check`'s table is no violation.
    pub fn stale_reason(
        &self,
        config: &LayersConfig,
        graph: &Graph<'_>,
        check: &str,
        from: &str,
        to: &str,
    ) -> String {
        let Some(member) = graph.member(from) else {
            return format!("no workspace member is named {from}");
        };
        if let Some(why) = left_out(member) {
            return format!("{from} {why}, which the storage checks leave out");
        }
        if member.layer == Some(Layer::Storage) {
            return format!("{from} is a store, which may reach storage");
        }
        if self.helpers.contains_key(from) {
            return format!("{from} is a storage helper, which may reach storage");
        }
        if check == STORAGE_ACCESS {
            if !self.is_target(config, to) {
                return format!(
                    "{to} is neither a driver in [rules.{RULE}.storage] nor a storage helper"
                );
            }
            return match production(member).find(|dependency| dependency.package == to) {
                None => format!("{from} has no normal or build dependency on {to}"),
                Some(_) => format!(
                    "{from}'s dependency on {to} is optional in an l10_foundation crate, a codec \
                     behind a feature, whose enabling {RULE}.{CODEC_FEATURES} checks"
                ),
            };
        }
        let Some((package, feature)) = to.split_once('/') else {
            return format!("{to} is not <package>/<feature>");
        };
        match graph.member(package) {
            None => format!("no workspace member is named {package}"),
            Some(target) if target.layer == Some(Layer::Storage) => {
                format!("{package} is a store, whose features are not checked")
            }
            Some(_)
                if !self
                    .features
                    .get(package)
                    .is_some_and(|features| features.contains_key(feature)) =>
            {
                format!("`{feature}` of {package} does not turn on a driver or storage helper")
            }
            Some(_)
                if member.layer == Some(Layer::Foundation)
                    && self.enablings(graph, member).iter().any(|enabling| {
                        enabling.behind_feature
                            && enabling.target == package
                            && enabling.feature == feature
                    }) =>
            {
                format!(
                    "{from} enables it only behind a feature of its own in an l10_foundation \
                     crate, a codec, whose enabling {RULE}.{CODEC_FEATURES} checks"
                )
            }
            Some(_) => format!("{from} does not enable it"),
        }
    }
}

/// `member`'s features, with Cargo's implicit one for each optional
/// dependency that no `dep:` item names.
pub(super) fn features_of(member: &Member<'_>) -> BTreeMap<String, Vec<String>> {
    let mut table: BTreeMap<String, Vec<String>> = member
        .krate
        .features
        .iter()
        .map(|(feature, items)| {
            let items = items.iter().map(|(item, _)| item.clone()).collect();
            (feature.clone(), items)
        })
        .collect();
    let named: BTreeSet<&str> = member
        .krate
        .features
        .values()
        .flatten()
        .filter_map(|(item, _)| item.strip_prefix("dep:"))
        .collect();
    for dependency in production(member).filter(|dependency| dependency.optional) {
        let key = dependency.key();
        if !named.contains(key) {
            table
                .entry(key.to_owned())
                .or_insert_with(|| vec![format!("dep:{key}")]);
        }
    }
    table
}

/// `layers.stale_entries` for `[rules.layers.storage]`: a `helpers` entry
/// that is no Foundation crate, is legacy, or is a helper by its own
/// dependency already. An entry for a crate without a layer is left to
/// `layers.placement`.
pub(super) fn stale_entries(
    ctx: &Context,
    config: &LayersConfig,
    graph: &Graph<'_>,
    storage: &Storage<'_>,
) -> Vec<Finding> {
    let policy = &ctx.layers_policy.storage;
    let fallback = policy.line.unwrap_or(ctx.layers_policy.line);
    let mut findings = Vec::new();
    let table = format!("[rules.{RULE}.storage]");
    for helper in config.storage.helpers.iter() {
        let reason = match graph.member(helper).map(|member| member.layer) {
            None => "no workspace member has this name".to_owned(),
            Some(None) => continue,
            Some(Some(layer)) if layer != Layer::Foundation => {
                format!("a storage helper is an l10_foundation crate, and its layer is {layer}")
            }
            Some(_) if config.legacy.contains(helper) => format!(
                "[rules.{RULE}] legacy lists it too, which keeps a crate from being a storage \
                 helper"
            ),
            Some(_) => match storage.helpers.get(helper.as_str()) {
                Some(Helper::Derived(driver)) => {
                    format!("its required dependency on {driver} already makes it a storage helper")
                }
                _ => continue,
            },
        };
        findings.push(config_finding(
            "stale_entries",
            &config.stale_entries,
            helper,
            policy.helpers.get(helper).copied().unwrap_or(fallback),
            format!(": is in {table} helpers in {CONFIG_FILE}, but {reason}. Remove it."),
        ));
    }
    findings
}
