//! `storage_access` and `codec_features`: nothing above Storage talks to a
//! database or object storage directly, Foundation does only in a storage
//! helper, which names nothing SPACE-specific, and Domain and other
//! Foundation crates hold at most a codec, behind an optional dependency on
//! a codec facade.
//!
//! The drivers are the packages `[rules.layers.storage] drivers` lists. The
//! storage helpers are the Foundation crates `helpers` lists, the full list:
//! derivation only checks that it is complete, so a Foundation crate with a
//! required (normal or build, not optional) dependency on a driver that
//! `helpers` doesn't list is a `storage_access` finding until it is added. A
//! listed helper need not depend on a driver, such as a connector for a
//! driver's client that does not depend on the driver itself. Test support
//! and tools are left out. The codec facades are the Foundation crates
//! `codec_facades` lists, other than helpers: each re-exports only a
//! driver's codec items, behind features, those its `exports` list, so that
//! list is what a codec may use.
//!
//! - `storage_access`: only stores and helpers may have a normal or build
//!   dependency on a driver, helper or codec facade, optional or not, with
//!   two exceptions, both optional normal dependencies: a Foundation or
//!   Domain crate's on a facade, a codec behind a feature, such as `sqlx`
//!   impls for an identifier type, whose enabling `codec_features` covers;
//!   and a facade's own on a driver. Its findings also cover a facade's
//!   source beyond its listed exports ([`facade`]), which no baseline entry
//!   tolerates.
//! - `codec_features`: a driver feature of a crate outside Storage, one that
//!   turns on an optional dependency on a driver or helper, itself, through
//!   the features it requests of an optional dependency, or by forwarding to
//!   another crate's driver feature, may be enabled by stores and helpers:
//!   with `features = […]` on the dependency, through its default
//!   features, or by forwarding from `[features]`; and by a Foundation or
//!   Domain crate only behind a feature of its own, on an optional
//!   dependency or by forwarding, as its codec is exempt: that feature is
//!   then a driver feature, whose enabling is checked. A store's own
//!   driver features are not checked: depending on a store is the way to
//!   reach storage.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    Crossing, GUIDE, RULE, config_finding, facade,
    graph::{Graph, Member, list, used_items},
};
use crate::{
    config::{CONFIG_FILE, CodecFacade, Layer, LayersConfig},
    context::{Context, Crate, Dependency},
    finding::{Finding, Location},
};

pub(super) const STORAGE_ACCESS: &str = "storage_access";

/// How a codec facade may reach a driver, for the fixes that say so.
pub(super) const FACADE_FIX: &str = "a codec facade reaches a driver only through an optional \
                                     normal dependency behind its own features, and never \
                                     becomes a store or a storage helper";
pub(super) const CODEC_FEATURES: &str = "codec_features";

/// What `[rules.layers.storage] helpers` lists a storage helper for.
pub(super) enum Helper {
    /// Its required dependency on this driver, which derivation sees too.
    Derived(String),
    /// Nothing derivation sees: it has no required driver dependency.
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
    /// The codec facades: the Foundation crates `codec_facades` lists that
    /// are not helpers.
    facades: BTreeSet<&'a str>,
    /// The Foundation crates derivation finds that `helpers` doesn't list,
    /// each with the drivers it requires: no helper until listed.
    unlisted: BTreeMap<&'a str, BTreeSet<&'a str>>,
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
pub(super) fn production<'a>(
    member: &Member<'a>,
) -> impl Iterator<Item = &'a Dependency> + use<'a> {
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
        let mut unlisted = BTreeMap::new();
        for member in graph.members.values() {
            if member.layer != Some(Layer::Foundation) {
                continue;
            }
            // In manifest order, the first named in messages.
            let required: Vec<&str> = production(member)
                .filter(|dependency| !dependency.optional && drivers.contains(&dependency.package))
                .map(|dependency| dependency.package.as_str())
                .collect();
            match (
                required.first(),
                config.storage.helpers.contains(member.name()),
            ) {
                (Some(driver), true) => {
                    helpers.insert(member.name(), Helper::Derived((*driver).to_owned()));
                }
                (None, true) => {
                    helpers.insert(member.name(), Helper::Listed);
                }
                // A facade's required driver dependency is no reason to
                // list it in helpers, which would make it no facade.
                (Some(_), false) if config.storage.codec_facades.contains_key(member.name()) => {}
                (Some(_), false) => {
                    unlisted.insert(member.name(), required.into_iter().collect());
                }
                (None, false) => {}
            }
        }
        let facades = graph
            .members
            .values()
            .filter(|member| {
                member.layer == Some(Layer::Foundation)
                    && config.storage.codec_facades.contains_key(member.name())
                    && !config.storage.helpers.contains(member.name())
            })
            .map(|member| member.name())
            .collect();
        let mut storage = Self {
            helpers,
            facades,
            unlisted,
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

    /// Whether `from`'s dependency on `to` is a required driver dependency
    /// that makes `from` a storage helper `helpers` lacks.
    pub fn lacks_helper(&self, from: &str, to: &str) -> bool {
        self.unlisted
            .get(from)
            .is_some_and(|drivers| drivers.contains(to))
    }

    /// Whether `package` is a driver or a storage helper.
    pub(super) fn is_target(&self, config: &LayersConfig, package: &str) -> bool {
        config.storage.drivers.contains(package) || self.helpers.contains_key(package)
    }

    /// Whether `package` is a codec facade.
    pub(super) fn is_facade(&self, package: &str) -> bool {
        self.facades.contains(package)
    }

    /// Whether `dependency` of `member` is a codec, which `storage_access`
    /// exempts: an optional normal dependency of a Foundation or Domain
    /// crate on a codec facade, or of a facade on a driver.
    fn is_codec(
        &self,
        config: &LayersConfig,
        member: &Member<'_>,
        dependency: &Dependency,
    ) -> bool {
        let to = dependency.package.as_str();
        dependency.optional
            && dependency.kind.is_none()
            && ((self.is_facade(to)
                && matches!(member.layer, Some(Layer::Foundation | Layer::Domain)))
                || (self.is_facade(member.name()) && config.storage.drivers.contains(to)))
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
            let facade = self.is_facade(to);
            if !(facade || self.is_target(config, to)) || self.is_codec(config, member, dependency)
            {
                continue;
            }
            // What a Foundation or Domain crate's codec depends on instead
            // of `to`, if a facade exports any of that driver.
            let covering: Vec<String> = self
                .facades
                .iter()
                .filter(|&&facade| {
                    facade != from
                        && config
                            .storage
                            .codec_facades
                            .get(facade)
                            .and_then(|entry| entry.exports.get(to.as_str()))
                            .is_some_and(|paths| paths.iter().next().is_some())
                        && graph.member(facade).is_some_and(|member| {
                            production(member).any(|dependency| {
                                dependency.kind.is_none() && dependency.package == *to
                            })
                        })
                })
                .map(|&facade| facade.to_owned())
                .collect();
            let codec = (!covering.is_empty()).then(|| {
                format!(
                    "depend on {} (optional, behind your codec feature) for encode/decode impls",
                    or_list(&covering)
                )
            });
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
                (None, Some(target)) if facade => (
                    format!("a codec facade ([rules.{RULE}.storage] codec_facades lists it)"),
                    target.krate.lib.clone().unwrap_or_default(),
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
            let fixes = match (layer, &codec) {
                (Layer::Domain | Layer::Foundation, _) if facade => format!(
                    "Fix: a codec facade is a codec's only behind a feature: make {to} an \
                     optional normal dependency, under [dependencies] or a \
                     [target.'cfg(…)'.dependencies] table, and turn it on from a feature of \
                     {from}'s own, which {RULE}.{CODEC_FEATURES} lets only stores and storage \
                     helpers enable, directly or through a feature of another l10_foundation or \
                     l20_domain crate's own that forwards to it."
                ),
                (Layer::Domain, Some(codec)) => format!(
                    "Fix: {codec}; anything else belongs in a store: move the code that uses \
                     it{items} into the store that persists these types, where a wrapper type \
                     can carry them."
                ),
                (Layer::Domain, None) => format!(
                    "Fix: move the code that uses it{items}, such as a codec's impls, into the \
                     store that persists these types, where a wrapper type can carry them."
                ),
                // Listing a facade in helpers would make it no facade.
                (Layer::Foundation, _) if self.is_facade(from) => format!(
                    "Fix: {FACADE_FIX}: if {to} is a driver, make it an optional normal \
                     dependency behind the facade's features; otherwise drop it."
                ),
                // Derivation sees a helper that the list lacks.
                (Layer::Foundation, _) if self.lacks_helper(from, to) => {
                    format!(
                        "Fix one of: [rules.{RULE}.storage] helpers in {CONFIG_FILE} lists every \
                         storage helper, and a required driver dependency makes {from} one, but \
                         the list lacks it: if {from} names nothing SPACE-specific, add it to \
                         helpers; {RULE}.policy_changed reports the change for review. Otherwise \
                         move the code that uses it{items} into a store or a storage helper."
                    )
                }
                (Layer::Foundation, _) => format!(
                    "Fix one of: {}if {from} names nothing SPACE-specific, list it in \
                     [rules.{RULE}.storage] helpers in {CONFIG_FILE}, which \
                     {RULE}.policy_changed reports; or move the code that uses it{items} into a \
                     store or a storage helper.",
                    codec
                        .as_ref()
                        .map(|codec| format!("{codec}; "))
                        .unwrap_or_default()
                ),
                // Neither an app nor a client may use a store: an app goes
                // through the API crate that owns the state, a client
                // through its server's protocol.
                (Layer::App, _) => format!(
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
                ": {from} ({layer}) depends on {to}, {what}, {}{optional}, but only l30_storage \
                 crates and storage helpers may: nothing above l30_storage reaches a database or \
                 object storage directly, l10_foundation does only in a helper that names \
                 nothing SPACE-specific, and l20_domain and other l10_foundation crates hold at \
                 most a codec, behind an optional normal dependency on a codec facade. {fixes} \
                 See {GUIDE}.",
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
    /// nor a helper. A Foundation or Domain crate's enabling behind a
    /// feature of its own is exempt, as its codec is: that feature is then
    /// a driver feature itself, whose enabling this checks.
    fn check_codecs(&mut self, graph: &Graph<'_>, member: &Member<'_>) {
        let Some(layer) = member.layer else { return };
        let from = member.name();
        for enabling in self.enablings(graph, member) {
            if enabling.behind_feature && matches!(layer, Layer::Foundation | Layer::Domain) {
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
                 feature, directly or through a feature of an l10_foundation or l20_domain \
                 crate's own that forwards to it. Fix: drop the feature, move the code that needs it, such as a codec or \
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

    /// `storage_access` findings for what each codec facade's source holds
    /// beyond the re-exports its `[rules.layers.storage.codec_facades]`
    /// entry lists, read by [`facade::read`], and for a facade whose library
    /// is not `src/lib.rs`, the source that is read. A source finding is on
    /// line 1 of the file, naming the item, or, for a file syn can't parse,
    /// giving syn's error without a position. Each is reported as it is found,
    /// with no baseline entry to tolerate it: the re-exports are what every
    /// codec may use. Then `layers.stale_entries`
    /// for its exports that allow nothing ([`stale_exports`]).
    pub fn facade_exports(
        &self,
        ctx: &Context,
        config: &LayersConfig,
        graph: &Graph<'_>,
    ) -> Vec<Finding> {
        let mut findings = Vec::new();
        let mut stale = Vec::new();
        for &facade in &self.facades {
            let Some(member) = graph.member(facade) else {
                continue;
            };
            let krate = member.krate;
            let src = if krate.member.is_empty() {
                "src/".to_owned()
            } else {
                format!("{}/src/", krate.member)
            };
            let entry = format!("[rules.{RULE}.storage.codec_facades.\"{facade}\"]");
            let mut finding = |path: &str, line: usize, reason: String| {
                findings.push(Finding {
                    rule: RULE,
                    instance: Some(STORAGE_ACCESS.to_owned()),
                    behaviour: "no_crossing",
                    path: path.to_owned(),
                    detail: format!(
                        ": {facade} is a codec facade ([rules.{RULE}.storage] codec_facades \
                         lists it), whose source holds only `pub mod` blocks of re-exports and \
                         `pub use` items of drivers it depends on, each an export that \
                         {entry} lists, with `cfg` and `doc` attributes and comments, since \
                         what it re-exports every codec may use; {reason}. No baseline entry \
                         tolerates this. Fix: remove it, or, for an item that encoding or \
                         decoding a value needs, re-export it and add it to the exports, which \
                         {RULE}.policy_changed reports for review. See {GUIDE}."
                    ),
                    mode: config.storage_access.no_crossing.mode,
                    location: Location {
                        path: path.to_owned(),
                        line,
                    },
                });
            };
            let lib = format!("{src}lib.rs");
            if krate.lib_path.as_deref() != Some(lib.as_str()) {
                let actual = krate.lib_path.as_deref().map_or_else(
                    || "it has no library".to_owned(),
                    |path| format!("its library is {path}"),
                );
                finding(
                    &krate.manifest(),
                    krate.name_line,
                    format!("{actual}, and only {lib} is read"),
                );
                continue;
            }
            if ctx.file(&lib).is_none() {
                finding(
                    &krate.manifest(),
                    krate.name_line,
                    format!("{lib} is no file archcheck reads"),
                );
                continue;
            }
            // What its code can name: its normal dependencies, not its
            // build-dependencies.
            let drivers: BTreeMap<String, String> = production(member)
                .filter(|dependency| {
                    dependency.kind.is_none()
                        && config.storage.drivers.contains(&dependency.package)
                })
                .map(|dependency| {
                    (
                        dependency.key().replace('-', "_"),
                        dependency.package.clone(),
                    )
                })
                .collect();
            let listed = config.storage.codec_facades.get(facade);
            // Whether every file was read, so that an export no file
            // re-exports is one the facade lacks.
            let mut read_all = true;
            let mut exported = BTreeSet::new();
            for file in ctx.files.iter().filter(|file| file.path.starts_with(&src)) {
                let source = facade::read(&file.text, &drivers);
                read_all &= source.problems.is_empty();
                for problem in source.problems {
                    finding(&file.path, 1, problem);
                }
                for export in source.exports {
                    exported.insert((export.driver.clone(), export.path.clone()));
                    let allowed = listed.is_some_and(|listed| {
                        listed
                            .exports
                            .get(&export.driver)
                            .is_some_and(|paths| paths.contains(&export.path))
                    });
                    if !allowed {
                        finding(
                            &file.path,
                            1,
                            format!(
                                "it re-exports {}::{}, which its exports do not list for {}",
                                export.root, export.path, export.driver
                            ),
                        );
                    }
                }
            }
            if let Some(listed) = listed {
                stale.extend(stale_exports(
                    ctx,
                    config,
                    facade,
                    listed,
                    &drivers,
                    read_all.then_some(&exported),
                ));
            }
        }
        findings.extend(stale);
        findings
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
            if !(self.is_target(config, to) || self.is_facade(to)) {
                return format!(
                    "{to} is neither a driver in [rules.{RULE}.storage], a storage helper nor a \
                     codec facade"
                );
            }
            let layer = member
                .layer
                .map(|layer| layer.to_string())
                .unwrap_or_default();
            return match production(member).find(|dependency| dependency.package == to) {
                None => format!("{from} has no normal or build dependency on {to}"),
                Some(_) if self.is_facade(to) => format!(
                    "{from}'s dependency on {to}, a codec facade, is optional and normal in an \
                     {layer} crate: a codec behind a feature, whose enabling \
                     {RULE}.{CODEC_FEATURES} checks"
                ),
                Some(_) => format!(
                    "{from} is a codec facade, and its dependency on {to} is optional and \
                     normal: a driver feature, whose enabling {RULE}.{CODEC_FEATURES} checks"
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
                if matches!(member.layer, Some(Layer::Foundation | Layer::Domain))
                    && self.enablings(graph, member).iter().any(|enabling| {
                        enabling.behind_feature
                            && enabling.target == package
                            && enabling.feature == feature
                    }) =>
            {
                format!(
                    "{from} enables it only behind a feature of its own in an {} crate, a \
                     codec, whose enabling {RULE}.{CODEC_FEATURES} checks",
                    member
                        .layer
                        .map(|layer| layer.to_string())
                        .unwrap_or_default()
                )
            }
            Some(_) => format!("{from} does not enable it"),
        }
    }
}

/// `layers.stale_entries` for `facade`'s `exports`, so that the lists only
/// shrink: a driver key that names no driver of its normal dependencies
/// (`drivers`, keyed by the name its code uses), a key whose list is empty,
/// and, when every source file was read and re-exports `exported`, a listed
/// path no file re-exports.
fn stale_exports(
    ctx: &Context,
    config: &LayersConfig,
    facade: &str,
    listed: &CodecFacade,
    drivers: &BTreeMap<String, String>,
    exported: Option<&BTreeSet<(String, String)>>,
) -> Vec<Finding> {
    let policy = &ctx.layers_policy.storage.codec_facades;
    let (lines, facade_line) = match policy.entries.get(facade) {
        Some((lines, line)) => (Some(lines), *line),
        None => (None, policy.line.unwrap_or(ctx.layers_policy.line)),
    };
    let line_of = |driver: &str, path: Option<&str>| {
        lines
            .and_then(|lines| match path {
                Some(path) => lines.get(&format!("{driver}::{path}")).copied(),
                None => lines
                    .iter()
                    .find(|(item, _)| item.starts_with(&format!("{driver}::")))
                    .map(|(_, line)| *line),
            })
            .unwrap_or(facade_line)
    };
    let table = format!("[rules.{RULE}.storage.codec_facades.\"{facade}\"] exports");
    let mut findings = Vec::new();
    let mut stale = |line: usize, detail: String| {
        findings.push(config_finding(
            "stale_entries",
            &config.stale_entries,
            facade,
            line,
            detail,
        ));
    };
    for (driver, paths) in &listed.exports {
        if !drivers.values().any(|package| package == driver) {
            stale(
                line_of(driver, None),
                format!(
                    ": {table} in {CONFIG_FILE} has an entry for {driver}, but {facade} has no \
                     normal dependency on {driver} that [rules.{RULE}.storage] drivers lists, so \
                     it allows nothing. Remove the entry."
                ),
            );
            continue;
        }
        if paths.iter().next().is_none() {
            stale(
                facade_line,
                format!(
                    ": {table} in {CONFIG_FILE} lists nothing for {driver}, which allows \
                     nothing. Remove the entry."
                ),
            );
            continue;
        }
        let Some(exported) = exported else { continue };
        for path in paths.iter() {
            if !exported.contains(&(driver.clone(), path.clone())) {
                stale(
                    line_of(driver, Some(path)),
                    format!(
                        ": {table} in {CONFIG_FILE} lists {driver}::{path}, but no file of \
                         {facade}'s src/ re-exports it. Remove it."
                    ),
                );
            }
        }
    }
    findings
}

/// `names` as alternatives for a sentence: `a`, `a or b`, `a, b or c`.
fn or_list(names: &[String]) -> String {
    match names.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} or {last}", rest.join(", ")),
        _ => names.join(""),
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
/// that is no Foundation crate, and a `codec_facades` entry that is no
/// Foundation crate or is a helper. An entry for a crate without a layer is
/// left to `layers.placement`.
pub(super) fn stale_entries(
    ctx: &Context,
    config: &LayersConfig,
    graph: &Graph<'_>,
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
            Some(_) => continue,
        };
        findings.push(config_finding(
            "stale_entries",
            &config.stale_entries,
            helper,
            policy.helpers.get(helper).copied().unwrap_or(fallback),
            format!(": is in {table} helpers in {CONFIG_FILE}, but {reason}. Remove it."),
        ));
    }
    for facade in config.storage.codec_facades.keys() {
        let reason = match graph.member(facade).map(|member| member.layer) {
            None => "no workspace member has this name".to_owned(),
            Some(None) => continue,
            Some(Some(layer)) if layer != Layer::Foundation => {
                format!("a codec facade is an l10_foundation crate, and its layer is {layer}")
            }
            Some(_) if config.storage.helpers.contains(facade) => {
                "helpers lists it too, and a storage helper may reach storage already".to_owned()
            }
            Some(_) => continue,
        };
        findings.push(config_finding(
            "stale_entries",
            &config.stale_entries,
            facade,
            policy
                .codec_facades
                .entries
                .get(facade)
                .map(|(_, line)| *line)
                .unwrap_or(fallback),
            format!(": is in {table} codec_facades in {CONFIG_FILE}, but {reason}. Remove it."),
        ));
    }
    findings
}
