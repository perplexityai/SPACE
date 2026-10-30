//! `domain_dependencies` and `model_protocol`: Domain does no I/O, and its
//! models never depend on its protocols.
//!
//! A Domain crate is a protocol (wire schemas and the conversions between
//! them and the models) if its name ends in `-protocol` or `-proto`, or
//! `[rules.layers] protocols` lists it; every other Domain crate is a model.
//!
//! - `domain_dependencies`: no Domain crate has a normal or build dependency,
//!   optional or not, on a fenced package: one `[rules.layers.domain_fences]`
//!   lists, such as an async runtime or an HTTP client, or a Foundation
//!   crate, legacy or not, with a required (normal or build, not optional)
//!   dependency on a fenced package, which fences it under that package's
//!   family. A fenced package that is a driver or a storage helper is left
//!   to `storage_access`.
//! - `model_protocol`: no model has a normal or build dependency on a
//!   protocol.

use std::collections::BTreeMap;

use super::{
    Crossing, GUIDE, RULE, config_finding,
    graph::{Graph, Member, list, used_items},
    storage::{Storage, production},
};
use crate::{
    config::{CONFIG_FILE, Layer, LayersConfig},
    context::Context,
    finding::{Finding, Location},
};

pub(super) const DOMAIN_DEPENDENCIES: &str = "domain_dependencies";
pub(super) const MODEL_PROTOCOL: &str = "model_protocol";

/// The name suffixes that make a Domain crate a protocol.
const PROTOCOL_SUFFIXES: [&str; 2] = ["-protocol", "-proto"];

/// Why a package is fenced for Domain.
struct Fence {
    /// The `[rules.layers.domain_fences]` family it is fenced under.
    family: String,
    /// For a Foundation crate fenced by its own dependencies, the fenced
    /// package its required dependency is on; `None` for a listed package.
    via: Option<String>,
}

impl Fence {
    /// What fences the package, after its name in a finding.
    fn describe(&self) -> String {
        let family = &self.family;
        match &self.via {
            None => format!("which [rules.{RULE}.domain_fences] {family} lists"),
            Some(via) => format!(
                "an l10_foundation crate whose required dependency on {via} fences it under \
                 [rules.{RULE}.domain_fences] {family}"
            ),
        }
    }
}

/// The fenced packages, to why: those `[rules.layers.domain_fences]` lists,
/// and, to a fixpoint, each Foundation crate, legacy or not, with a
/// required dependency on a fenced package, by the earliest such dependency
/// its manifest declares when the crate is reached. A listed Foundation
/// crate that its dependencies fence too is derived, so that its entry is
/// stale.
fn fences(config: &LayersConfig, graph: &Graph<'_>) -> BTreeMap<String, Fence> {
    let mut fences: BTreeMap<String, Fence> = config
        .domain_fences
        .iter()
        .map(|(package, family)| {
            let fence = Fence {
                family: family.to_owned(),
                via: None,
            };
            (package.to_owned(), fence)
        })
        .collect();
    let candidates: Vec<&Member<'_>> = graph
        .members
        .values()
        .filter(|member| member.layer == Some(Layer::Foundation))
        .collect();
    loop {
        let mut changed = false;
        for member in &candidates {
            if fences
                .get(member.name())
                .is_some_and(|fence| fence.via.is_some())
            {
                continue;
            }
            let Some((dependency, family)) = production(member)
                .filter(|dependency| !dependency.optional)
                .filter_map(|dependency| {
                    let fence = fences.get(&dependency.package)?;
                    Some((dependency, fence.family.clone()))
                })
                .min_by_key(|(dependency, _)| dependency.line)
            else {
                continue;
            };
            let fence = Fence {
                family,
                via: Some(dependency.package.clone()),
            };
            fences.insert(member.name().to_owned(), fence);
            changed = true;
        }
        if !changed {
            return fences;
        }
    }
}

/// The Domain checks' verdicts, worked out once for checking and for
/// `--write-baseline`.
pub(super) struct Domain {
    /// The fenced packages, to why ([`fences`]).
    fences: BTreeMap<String, Fence>,
    /// `domain_dependencies` violations, as `(from, fenced package)`.
    pub dependencies: BTreeMap<(String, String), Crossing>,
    /// `model_protocol` violations, as `(model, protocol)`.
    pub model_protocol: BTreeMap<(String, String), Crossing>,
}

/// The suffix of `name` that makes a Domain crate a protocol, if it has one.
pub(super) fn protocol_suffix(name: &str) -> Option<&'static str> {
    PROTOCOL_SUFFIXES
        .into_iter()
        .find(|suffix| name.ends_with(suffix))
}

/// Why `member` is a protocol, if it is one: a Domain crate with a
/// protocol's name, or one `protocols` lists.
fn protocol(config: &LayersConfig, member: &Member<'_>) -> Option<String> {
    if member.layer != Some(Layer::Domain) {
        return None;
    }
    if let Some(suffix) = protocol_suffix(member.name()) {
        return Some(format!("its name ends in {suffix}"));
    }
    config
        .protocols
        .contains(member.name())
        .then(|| format!("[rules.{RULE}] protocols lists it"))
}

impl Domain {
    pub fn new(
        ctx: &Context,
        config: &LayersConfig,
        graph: &Graph<'_>,
        storage: &Storage<'_>,
    ) -> Self {
        let mut domain = Self {
            fences: fences(config, graph),
            dependencies: BTreeMap::new(),
            model_protocol: BTreeMap::new(),
        };
        for member in graph
            .members
            .values()
            .filter(|member| member.layer == Some(Layer::Domain))
        {
            domain.check_dependencies(ctx, config, graph, storage, member);
            if protocol(config, member).is_none() {
                domain.check_model(ctx, config, graph, member);
            }
        }
        domain
    }

    /// `domain_dependencies` for `member`, a Domain crate.
    fn check_dependencies(
        &mut self,
        ctx: &Context,
        config: &LayersConfig,
        graph: &Graph<'_>,
        storage: &Storage<'_>,
        member: &Member<'_>,
    ) {
        let from = member.name();
        for dependency in production(member) {
            let to = &dependency.package;
            let Some(fence) = self.fences.get(to) else {
                continue;
            };
            let key = (from.to_owned(), to.clone());
            if storage.is_target(config, to) || self.dependencies.contains_key(&key) {
                continue;
            }
            let lib = graph
                .member(to)
                .and_then(|target| target.krate.lib.clone())
                .unwrap_or_else(|| to.replace('-', "_"));
            let items = used_items(&ctx.files, member.krate, dependency, &lib);
            let items = if items.is_empty() {
                String::new()
            } else {
                format!(" ({})", list(&items, 5))
            };
            let optional = if dependency.optional {
                " (optional, so a feature turns it on)"
            } else {
                ""
            };
            let family = &fence.family;
            let detail = format!(
                ": {from} (l20_domain) depends on {to}, {}, {}{optional}, but l20_domain crates \
                 do no I/O, and domain_fences lists the packages that do it, such as async \
                 runtimes, HTTP clients, cloud SDKs and Kubernetes clients. Fix one of: move the \
                 code that uses it{items} into the l40_api crate that does the I/O, or into an \
                 l10_foundation mechanism that names nothing SPACE-specific, and have {from} \
                 take plain values or define a trait for it; or, if {from} is not Domain code, \
                 move it to its layer. See {GUIDE}.",
                fence.describe(),
                dependency.describe(),
            );
            self.dependencies.insert(
                key,
                Crossing {
                    location: Location {
                        path: member.krate.manifest(),
                        line: dependency.line,
                    },
                    detail,
                    summary: format!("{from} (l20_domain) -> {to} ({family})"),
                },
            );
        }
    }

    /// `model_protocol` for `member`, a model.
    fn check_model(
        &mut self,
        ctx: &Context,
        config: &LayersConfig,
        graph: &Graph<'_>,
        member: &Member<'_>,
    ) {
        let from = member.name();
        for dependency in production(member) {
            let Some(target) = graph.member(&dependency.package) else {
                continue;
            };
            let Some(why) = protocol(config, target) else {
                continue;
            };
            let to = target.name();
            let key = (from.to_owned(), to.to_owned());
            if self.model_protocol.contains_key(&key) {
                continue;
            }
            let lib = target.krate.lib.as_deref().unwrap_or_default();
            let items = used_items(&ctx.files, member.krate, dependency, lib);
            let items = if items.is_empty() {
                String::new()
            } else {
                format!(" ({})", list(&items, 5))
            };
            let detail = format!(
                ": {from} (a model in l20_domain) depends on {to} (a protocol: {why}), {}, but \
                 a model never depends on a protocol: protocols hold wire schemas and convert \
                 between them and the models, so the edge goes the other way. Fix one of: move \
                 what {from} uses{items} into a model, and have {to} convert to it; or, if \
                 {from} holds wire code too, split that into a protocol crate (named \
                 *-protocol), which may depend on both. See {GUIDE}.",
                dependency.describe(),
            );
            self.model_protocol.insert(
                key,
                Crossing {
                    location: Location {
                        path: member.krate.manifest(),
                        line: dependency.line,
                    },
                    detail,
                    summary: format!("{from} (model) -> {to} (protocol)"),
                },
            );
        }
    }

    /// Why `(from, to)` in `check`'s table is no violation.
    pub fn stale_reason(
        &self,
        config: &LayersConfig,
        graph: &Graph<'_>,
        storage: &Storage<'_>,
        check: &str,
        from: &str,
        to: &str,
    ) -> String {
        let Some(member) = graph.member(from) else {
            return format!("no workspace member is named {from}");
        };
        if member.layer != Some(Layer::Domain) {
            return format!("{from} is not in l20_domain");
        }
        let declared = production(member).any(|dependency| dependency.package == to);
        if check == DOMAIN_DEPENDENCIES {
            return if !self.fences.contains_key(to) {
                format!(
                    "{to} is not fenced: no [rules.{RULE}.domain_fences] family lists it, and it \
                     is no l10_foundation crate with a required dependency on a fenced package"
                )
            } else if storage.is_target(config, to) {
                format!(
                    "{to} is a driver or a storage helper, which {RULE}.storage_access checks \
                     instead"
                )
            } else if !declared {
                format!("{from} has no normal or build dependency on {to}")
            } else {
                format!("{RULE}.{DOMAIN_DEPENDENCIES} no longer reports it")
            };
        }
        if protocol(config, member).is_some() {
            return format!("{from} is a protocol, which may depend on protocols");
        }
        match graph.member(to) {
            None => format!("no workspace member is named {to}"),
            Some(target) if protocol(config, target).is_none() => {
                format!("{to} is not a protocol")
            }
            Some(_) if !declared => format!("{from} has no normal or build dependency on {to}"),
            Some(_) => format!("{RULE}.{MODEL_PROTOCOL} no longer reports it"),
        }
    }
}

/// `layers.stale_entries` for `protocols` entries that are no Domain crate
/// or a protocol by name already, and `[rules.layers.domain_fences]`
/// entries that `storage_access` checks instead or that their own
/// dependencies fence already.
pub(super) fn stale_entries(
    ctx: &Context,
    config: &LayersConfig,
    graph: &Graph<'_>,
    domain: &Domain,
    storage: &Storage<'_>,
) -> Vec<Finding> {
    let policy = &ctx.layers_policy;
    let mut findings = Vec::new();
    for package in config.protocols.iter() {
        let reason = match graph.member(package).map(|member| member.layer) {
            None => "no workspace member has this name".to_owned(),
            // Its placement finding covers a crate without a layer.
            Some(None) => continue,
            Some(Some(Layer::Domain)) => match protocol_suffix(package) {
                Some(suffix) => format!("its name ends in {suffix}, which makes it a protocol"),
                None => continue,
            },
            Some(Some(layer)) => {
                format!("a protocol is an l20_domain crate, and its layer is {layer}")
            }
        };
        findings.push(config_finding(
            "stale_entries",
            &config.stale_entries,
            package,
            policy
                .protocols
                .get(package)
                .copied()
                .unwrap_or(policy.line),
            format!(": is in [rules.{RULE}] protocols in {CONFIG_FILE}, but {reason}. Remove it."),
        ));
    }
    let fences = &policy.domain_fences;
    for package in config.domain_fences.packages() {
        let reason = if storage.is_target(config, package) {
            format!(
                "it is a driver or a storage helper, whose dependents in l20_domain \
                 {RULE}.storage_access reports"
            )
        } else if let Some(fence) = domain.fences.get(package)
            && let Some(via) = &fence.via
        {
            format!(
                "its required dependency on {via} fences it already, under {}",
                fence.family
            )
        } else {
            continue;
        };
        let line = fences
            .entries
            .get(package)
            .map(|(_, line)| *line)
            .or(fences.line)
            .unwrap_or(policy.line);
        findings.push(config_finding(
            "stale_entries",
            &config.stale_entries,
            package,
            line,
            format!(
                ": is in [rules.{RULE}.domain_fences] in {CONFIG_FILE}, but {reason}. Remove it."
            ),
        ));
    }
    findings
}
