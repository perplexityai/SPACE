//! The layer order between workspace crates.
//!
//! Every member belongs to a layer: `l10_foundation`, `l20_domain`,
//! `l30_storage`, `l40_api` and `l50_app`, bottom-up, or `test_support` or
//! `tools` beside them. A crate in `crates/<layer>/` takes its layer from the
//! directory; any other crate from its `[rules.layers] unmoved` entry, until
//! it moves. The checks:
//!
//! - `placement`: every member has a layer, and no `unmoved` entry is new.
//! - `layout`: below its layer directory, a crate's path is its package name
//!   split on `-`, optionally with one leaf directory without `-` below it,
//!   and no crate is inside another.
//! - `direction` and `dev_direction`: normal and build edges follow the
//!   layer matrix, and dev edges point at the same or a lower layer or at
//!   test support; each with a baseline table of the edges tolerated today.
//! - `test_only`: test support only through dev-dependencies, nothing
//!   depends on tools.
//! - `clients`: a client depends only on Domain, Foundation and other
//!   clients.
//! - `store_edges`: an API or Storage crate depends on a store only if the
//!   store lists it as a user.
//! - `storage_access` and `codec_features`: only stores and storage helpers
//!   depend on a database or object storage driver, or turn on another
//!   crate's driver feature; each with a baseline table ([`storage`]).
//! - `domain_dependencies` and `model_protocol`: no Domain crate depends on
//!   a package that does I/O, and no model on a protocol; each with a
//!   baseline table ([`domain`]).
//! - `stale_entries` and `policy_changed`: entries of `[rules.layers]` that
//!   no longer apply, and every change to it from the base branch's copy,
//!   including its removal.
//!
//! Baseline tables are keyed by package name, so moving a crate into its
//! layer directory leaves them alone.

mod domain;
mod graph;
mod policy;
mod storage;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use graph::{
    Edge, Graph, Member, Problem, allowed_targets, dev_problem, list, may_depend, name_segments,
    production_problem, used_items,
};

use domain::{DOMAIN_DEPENDENCIES, Domain, MODEL_PROTOCOL};
use storage::{CODEC_FEATURES, STORAGE_ACCESS, Storage};

use crate::{
    baseline::{BASELINE_FILE, BaseBaseline, BaseLayersTable, BaseSkip, Baseline, EdgeTable},
    config::{
        BaseArchitecture, BehaviourConfig, CONFIG_FILE, EdgeBehaviours, Layer, LayersConfig, Mode,
        RulesConfig,
    },
    context::Context,
    finding::{Finding, Location},
    rules::{NotAdded, Rule},
};

pub struct Layers;

impl Layers {
    /// The rule's name, as in its finding codes and baseline tables.
    pub const NAME: &'static str = "layers";

    /// Its baseline tables, `[layers.<check>]`, as `--write-baseline=<table>`
    /// takes them.
    pub fn table_names() -> Vec<String> {
        CHECKS
            .iter()
            .map(|check| format!("{RULE}.{check}"))
            .collect()
    }

    /// [`Self::table_names`] for a sentence: `a, b and c`.
    pub fn table_list() -> String {
        and_list(&Self::table_names())
    }
}

const RULE: &str = Layers::NAME;
const DIRECTION: &str = "direction";
const DEV_DIRECTION: &str = "dev_direction";
/// The checks with a baseline table, `[layers.<check>]`.
const CHECKS: [&str; 6] = [
    DIRECTION,
    DEV_DIRECTION,
    STORAGE_ACCESS,
    CODEC_FEATURES,
    DOMAIN_DEPENDENCIES,
    MODEL_PROTOCOL,
];
/// Where a finding sends the reader for what belongs in each layer.
const GUIDE: &str = "crates/tools/archcheck/README.md#layers";
/// What the comparisons with the base branch's `ARCHITECTURE.toml` are, for
/// skip lines.
const POLICY_COMPARISONS: &str =
    "layers.policy_changed and layers.placement's check for new unmoved entries";

/// A violation a baselined check's table lists as `(from, to)`.
#[derive(Clone)]
struct Crossing {
    /// The manifest line that declares it, where its finding is shown.
    location: Location,
    /// The finding's text after the path.
    detail: String,
    /// The edge for a `not added` line, such as `a (l40_api) -> sqlx`.
    summary: String,
}

/// An edge with a problem, and the finding's text after the path.
struct Violation<'a> {
    problem: Problem,
    edge: Edge<'a>,
    detail: String,
}

/// Every edge's problems, worked out once for checking and for
/// `--write-baseline`.
struct Verdicts<'a> {
    violations: Vec<Violation<'a>>,
    /// Each normal or build edge, as `(from, to)`.
    production: BTreeSet<(String, String)>,
    /// Each dev edge that no normal or build edge duplicates.
    dev: BTreeSet<(String, String)>,
    /// The `allowed_edges` entries that allow an edge with a problem.
    used_exceptions: BTreeSet<(String, String)>,
}

impl<'a> Verdicts<'a> {
    fn new(ctx: &Context, config: &LayersConfig, graph: &'a Graph<'a>) -> Self {
        let (production, dev) = graph.edges();
        let mut verdicts = Self {
            violations: Vec::new(),
            production: production.iter().map(Edge::key).collect(),
            dev: dev.iter().map(Edge::key).collect(),
            used_exceptions: BTreeSet::new(),
        };
        for edge in production {
            let (problem, excepted) = production_problem(config, &edge);
            if excepted {
                verdicts.used_exceptions.insert(edge.key());
            }
            if let Some(problem) = problem {
                let detail = detail(ctx, graph, problem, &edge);
                verdicts.violations.push(Violation {
                    problem,
                    edge,
                    detail,
                });
            }
        }
        for edge in dev {
            if let Some(problem) = dev_problem(&edge) {
                let detail = detail(ctx, graph, problem, &edge);
                verdicts.violations.push(Violation {
                    problem,
                    edge,
                    detail,
                });
            }
        }
        verdicts
    }

    /// Whether the edge `(from, to)` has `problem`.
    fn has(&self, problem: Problem, key: &(String, String)) -> bool {
        self.violations
            .iter()
            .any(|violation| violation.problem == problem && violation.edge.key() == *key)
    }

    /// The violations `problem`'s baselined check lists, by `(from, to)`.
    fn of(&self, problem: Problem) -> BTreeMap<(String, String), Crossing> {
        self.violations
            .iter()
            .filter(|violation| violation.problem == problem)
            .map(|violation| {
                let (from_layer, to_layer) = layers_of(&violation.edge);
                let crossing = Crossing {
                    location: Location {
                        path: violation.edge.from.krate.manifest(),
                        line: violation.edge.dependency.line,
                    },
                    detail: violation.detail.clone(),
                    summary: format!(
                        "{} ({from_layer}) -> {} ({to_layer})",
                        violation.edge.from.name(),
                        violation.edge.to.name()
                    ),
                };
                (violation.edge.key(), crossing)
            })
            .collect()
    }
}

/// Every check's verdicts.
struct Checked<'a> {
    graph: &'a Graph<'a>,
    verdicts: Verdicts<'a>,
    storage: Storage<'a>,
    domain: Domain,
}

impl<'a> Checked<'a> {
    fn new(ctx: &Context, config: &LayersConfig, graph: &'a Graph<'a>) -> Self {
        let storage = Storage::new(ctx, config, graph);
        Self {
            graph,
            verdicts: Verdicts::new(ctx, config, graph),
            domain: Domain::new(ctx, config, graph, &storage),
            storage,
        }
    }

    /// The violations a baselined check's table lists, by `(from, to)`.
    fn crossings(&self, check: &str) -> BTreeMap<(String, String), Crossing> {
        match check {
            DIRECTION => self.verdicts.of(Problem::Direction),
            DEV_DIRECTION => self.verdicts.of(Problem::DevDirection),
            STORAGE_ACCESS => self.storage.access.clone(),
            CODEC_FEATURES => self.storage.codecs.clone(),
            DOMAIN_DEPENDENCIES => self.domain.dependencies.clone(),
            _ => self.domain.model_protocol.clone(),
        }
    }
}

impl Rule for Layers {
    fn check(&self, ctx: &Context) -> Vec<Finding> {
        let Some(config) = &ctx.config.rules.layers else {
            return policy::rule_removed(ctx);
        };
        let graph = Graph::new(ctx, config);
        let mut findings = placement(ctx, config, &graph);
        findings.extend(layout(config, &graph));
        let checked = Checked::new(ctx, config, &graph);
        for check in CHECKS {
            findings.extend(baselined(ctx, config, &checked, check));
        }
        let verdicts = &checked.verdicts;
        for (problem, behaviour, mode) in [
            (Problem::TestOnly, "test_only", config.test_only.mode),
            (Problem::Clients, "clients", config.clients.mode),
            (Problem::StoreEdges, "store_edges", config.store_edges.mode),
        ] {
            findings.extend(
                verdicts
                    .violations
                    .iter()
                    .filter(|violation| violation.problem == problem)
                    .map(|violation| edge_finding(None, behaviour, mode, violation)),
            );
        }
        findings.extend(policy::stale_entries(ctx, config, &graph, verdicts));
        findings.extend(storage::stale_entries(
            ctx,
            config,
            &graph,
            &checked.storage,
        ));
        findings.extend(domain::stale_entries(
            ctx,
            config,
            &graph,
            &checked.domain,
            &checked.storage,
        ));
        findings.extend(policy::policy_changed(ctx, config, &graph));
        findings
    }

    /// Not called for a base branch's copy that is not TOML at all.
    fn skipped(&self, ctx: &Context) -> Vec<String> {
        if ctx.config.rules.layers.is_none() {
            return Vec::new();
        }
        if let BaseBaseline::NotGiven = ctx.base_baseline {
            return vec![BaseSkip::NotGiven.message(&format!("{} skipped", comparisons()))];
        }
        CHECKS
            .iter()
            .filter_map(|check| {
                let skip = ctx.base_baseline.layers_table(check).err()?;
                Some(skip.message(&format!("{RULE}.{check}.no_new_entries skipped")))
            })
            .collect()
    }

    fn base_comparisons(&self, ctx: &Context) -> Vec<String> {
        if ctx.config.rules.layers.is_none() {
            return Vec::new();
        }
        vec![comparisons()]
    }

    fn tables(&self, config: &RulesConfig) -> Vec<String> {
        if config.layers.is_none() {
            return Vec::new();
        }
        Self::table_names()
    }

    fn baseline_rule(&self) -> Option<&'static str> {
        Some(RULE)
    }

    fn other_skips(&self, ctx: &Context) -> Vec<String> {
        if ctx.config.rules.layers.is_none() {
            // The only comparison left is whether the base has the rule,
            // which an unreadable base table can't say.
            return match &ctx.base_architecture {
                BaseArchitecture::Loaded(base) => match &base.layers {
                    Err(unusable) => vec![BaseSkip::Unusable(unusable).message(&format!(
                        "{RULE}.policy_changed's check for the rule's removal skipped"
                    ))],
                    Ok(_) => Vec::new(),
                },
                BaseArchitecture::NotGiven => Vec::new(),
            };
        }
        let skipped = format!("{POLICY_COMPARISONS} skipped");
        match &ctx.base_architecture {
            BaseArchitecture::NotGiven => {
                vec![format!("{skipped} (no --base-architecture given)")]
            }
            BaseArchitecture::Loaded(base) => match &base.layers {
                Ok(_) => Vec::new(),
                Err(unusable) => vec![BaseSkip::Unusable(unusable).message(&skipped)],
            },
        }
    }

    /// Keeps each entry whose edge still has the check's problem, removes
    /// the rest, and adds none: an edge without an entry is listed, to fix.
    /// A hand-added entry fails `no_new_entries`; a reviewed, permanent
    /// exception to `direction` goes in `allowed_edges` instead. Every table
    /// is always written.
    fn update_baseline(&self, ctx: &Context, baseline: &mut Baseline) -> Vec<NotAdded> {
        let Some(config) = &ctx.config.rules.layers else {
            return Vec::new();
        };
        let graph = Graph::new(ctx, config);
        let checked = Checked::new(ctx, config, &graph);
        let mut not_added = Vec::new();
        for check in CHECKS {
            let existing = ctx.baseline.layers.get(check);
            let mut table = EdgeTable::default();
            for ((from, to), crossing) in checked.crossings(check) {
                if existing.is_some_and(|existing| existing.contains(&from, &to)) {
                    table.insert(&from, &to);
                    continue;
                }
                let exception = match check {
                    DIRECTION => format!(
                        "a reviewed, permanent exception goes in [rules.{RULE}] allowed_edges in \
                         {CONFIG_FILE}, which {RULE}.policy_changed reports"
                    ),
                    DEV_DIRECTION | MODEL_PROTOCOL => format!("{RULE}.{check} has no exceptions"),
                    STORAGE_ACCESS | CODEC_FEATURES => format!(
                        "a crate that has to reach storage for good becomes a store or a storage \
                         helper; listing one in [rules.{RULE}.storage] helpers is a policy change, \
                         which {RULE}.policy_changed reports, and a helper derived from a required \
                         driver dependency is reviewed in the manifest diff"
                    ),
                    _ => "a Domain crate that needs the package for good is not Domain code and \
                          moves to its layer's directory, a move reviewed in the diff"
                        .to_owned(),
                };
                not_added.push(NotAdded {
                    table: format!("{RULE}.{check}"),
                    entry: None,
                    line: format!(
                        "not added: {} in [{RULE}.{check}]; fix the dependency (cargo arch says \
                         how): an entry added by hand fails {RULE}.{check}.no_new_entries, and \
                         {exception}",
                        crossing.summary
                    ),
                });
            }
            baseline.layers.insert(check.to_owned(), table);
        }
        not_added
    }
}

/// `layers.direction.no_new_entries, …, and layers.codec_features.no_new_entries`.
fn comparisons() -> String {
    let all: Vec<String> = CHECKS
        .iter()
        .map(|check| format!("{RULE}.{check}.no_new_entries"))
        .collect();
    and_list(&all)
}

/// `items` for a sentence: `a`, `a and b`, or `a, b and c`.
fn and_list(items: &[String]) -> String {
    match items.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        _ => items.join(""),
    }
}

/// Whether the base branch's `ARCHITECTURE.toml` has `[rules.layers]`;
/// `None` when that copy was not given. A copy whose table can't be read,
/// as after a change to its schema, has the rule: only a base without it
/// seeds the baseline tables.
fn rule_on_base(ctx: &Context) -> Option<bool> {
    match &ctx.base_architecture {
        BaseArchitecture::Loaded(base) => Some(!matches!(base.layers, Ok(None))),
        BaseArchitecture::NotGiven => None,
    }
}

/// The layers of an edge's two ends, for a sentence.
fn layers_of(edge: &Edge<'_>) -> (Layer, Layer) {
    let layer = |member: &Member<'_>| member.layer.unwrap_or(Layer::Tools);
    (layer(edge.from), layer(edge.to))
}

/// Members without a layer, and `unmoved` entries the base branch's copy
/// lacks: a new crate goes in a layer directory.
fn placement(ctx: &Context, config: &LayersConfig, graph: &Graph<'_>) -> Vec<Finding> {
    let mode = config.placement.mode;
    let mut findings: Vec<Finding> = graph
        .members
        .values()
        .filter(|member| member.layer.is_none())
        .map(|member| {
            let manifest = member.krate.manifest();
            Finding {
                rule: RULE,
                instance: None,
                behaviour: "placement",
                detail: format!(
                    ": {} is at {}, outside the layer directories, and [rules.{RULE}] unmoved in \
                     {CONFIG_FILE} has no entry for it. Put the crate in its layer's directory, \
                     crates/<layer>/{}, where <layer> is l10_foundation, l20_domain, \
                     l30_storage, l40_api, l50_app, test_support or tools. See {GUIDE}.",
                    member.name(),
                    member.krate.member,
                    name_segments(member.name()).join("/"),
                ),
                path: manifest.clone(),
                mode,
                location: Location {
                    path: manifest,
                    line: member.krate.name_line,
                },
            }
        })
        .collect();
    let BaseArchitecture::Loaded(base) = &ctx.base_architecture else {
        return findings;
    };
    let Ok(Some(base)) = &base.layers else {
        return findings;
    };
    for (package, (layer, line)) in &ctx.layers_policy.unmoved.entries {
        if base.unmoved.entries.contains_key(package) {
            continue;
        }
        findings.push(Finding {
            rule: RULE,
            instance: None,
            behaviour: "placement",
            path: package.clone(),
            detail: format!(
                ": was added to [rules.{RULE}] unmoved in {CONFIG_FILE}, which lists only crates \
                 that predate the layer directories and is not to grow. Put the crate in \
                 crates/{layer}/{} instead. See {GUIDE}.",
                name_segments(package).join("/"),
            ),
            mode,
            location: Location {
                path: CONFIG_FILE.to_owned(),
                line: *line,
            },
        });
    }
    findings
}

/// Crates in a layer directory at a path other than their name's, or with
/// another crate inside them.
fn layout(config: &LayersConfig, graph: &Graph<'_>) -> Vec<Finding> {
    let mode = config.layout.mode;
    let finding = |member: &Member<'_>, detail: String| {
        let manifest = member.krate.manifest();
        Finding {
            rule: RULE,
            instance: None,
            behaviour: "layout",
            path: manifest.clone(),
            detail,
            mode,
            location: Location {
                path: manifest,
                line: member.krate.name_line,
            },
        }
    };
    let mut findings = Vec::new();
    for member in graph.members.values() {
        let (Some(layer), Some(path)) = (member.layer, member.layer_path) else {
            continue;
        };
        let expected = name_segments(member.name());
        if layer_segments(path) != expected && leaf(member).is_none() {
            let expected = expected.join("/");
            findings.push(finding(
                member,
                format!(
                    ": {} is at {}, but below crates/{layer} a crate's directory is its package \
                     name without `{}`, split on `-`: crates/{layer}/{expected}, or one \
                     directory below that (such as crates/{layer}/{expected}/server) where \
                     another crate's name in the layer would nest inside it. Move it there. See \
                     {GUIDE}.",
                    member.name(),
                    member.krate.member,
                    graph::PACKAGE_PREFIX,
                ),
            ));
        }
    }
    for outer in graph.members.values() {
        if outer.krate.member.is_empty() {
            continue;
        }
        let prefix = format!("{}/", outer.krate.member);
        for inner in graph.members.values() {
            let nested = inner.krate.member.starts_with(&prefix)
                && (outer.layer_path.is_some() || inner.layer_path.is_some());
            if !nested {
                continue;
            }
            let (outer_name, inner_name) = (outer.name(), inner.name());
            let fix = match (outer.layer, leaf(outer)) {
                // The outer crate already takes a leaf: another leaf, or the
                // inner crate elsewhere.
                (Some(layer), Some(_)) => format!(
                    "{outer_name} already sits in a leaf directory, so move one of them: \
                     {outer_name} to another leaf, crates/{layer}/{}/<leaf>, or {inner_name} \
                     out",
                    name_segments(outer_name).join("/"),
                ),
                _ => format!(
                    "Move {outer_name} one directory down, to {}/<leaf> (`server` for a \
                     service), or move {inner_name} out",
                    outer.krate.member,
                ),
            };
            findings.push(finding(
                outer,
                format!(
                    ": {outer_name} at {} contains the crate {inner_name} at {}, but no crate \
                     goes inside another crate's directory. {fix}. See {GUIDE}.",
                    outer.krate.member, inner.krate.member,
                ),
            ));
        }
    }
    findings
}

/// The parts of a path below a layer directory.
fn layer_segments(path: &str) -> Vec<&str> {
    path.split('/').filter(|part| !part.is_empty()).collect()
}

/// The leaf directory a crate in a layer directory sits in below its
/// name's path, if it takes one: one more directory, without `-`.
fn leaf<'a>(member: &Member<'a>) -> Option<&'a str> {
    let segments = layer_segments(member.layer_path?);
    let (leaf, parent) = segments.split_last()?;
    (parent == name_segments(member.name()).as_slice() && !leaf.contains('-')).then_some(*leaf)
}

/// A finding about an edge, shown on the manifest line that declares it.
fn edge_finding(
    instance: Option<&str>,
    behaviour: &'static str,
    mode: Mode,
    violation: &Violation<'_>,
) -> Finding {
    let manifest = violation.edge.from.krate.manifest();
    Finding {
        rule: RULE,
        instance: instance.map(str::to_owned),
        behaviour,
        path: manifest.clone(),
        detail: violation.detail.clone(),
        mode,
        location: Location {
            path: manifest,
            line: violation.edge.dependency.line,
        },
    }
}

/// What the base branch's copy of a baselined check's table is, for
/// `no_new_entries`.
#[derive(Clone, Copy)]
enum BaseEdges<'a> {
    Compare(&'a BTreeMap<String, BTreeSet<String>>),
    /// The base branch has no layers rule, or no such check, yet: every
    /// entry seeds the table.
    Seeding,
    /// The comparison is skipped, with a skip line.
    Skipped,
}

fn base_edges<'a>(ctx: &'a Context, check: &str) -> BaseEdges<'a> {
    static NONE: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let table = match ctx.base_baseline.layers_table(check) {
        Err(_) => return BaseEdges::Skipped,
        Ok(BaseLayersTable::Table(table)) => return BaseEdges::Compare(table),
        Ok(table) => table,
    };
    // A base with the check but without its table tolerates nothing; one
    // without the check, or the rule, is seeded. Without the base's
    // ARCHITECTURE.toml, a `[layers]` key in its baseline stands for the
    // rule.
    match (check_on_base(ctx, check), table) {
        (Some(true), _) | (None, BaseLayersTable::Missing) => BaseEdges::Compare(&NONE),
        _ => BaseEdges::Seeding,
    }
}

/// Whether the base branch's `[rules.layers]` configures `check`; `None`
/// when that copy was not given. A copy whose table can't be read has the
/// rule, as for [`rule_on_base`], and is taken to have the check.
fn check_on_base(ctx: &Context, check: &str) -> Option<bool> {
    let BaseArchitecture::Loaded(base) = &ctx.base_architecture else {
        return None;
    };
    let Ok(base) = &base.layers else {
        return Some(true);
    };
    let prefix = format!("{check}.");
    Some(base.as_ref().is_some_and(|policy| {
        policy
            .modes
            .keys()
            .any(|behaviour| behaviour.starts_with(&prefix))
    }))
}

/// `no_crossing`, `no_new_entries` and `stale_entries` of `check`.
fn baselined(
    ctx: &Context,
    config: &LayersConfig,
    checked: &Checked<'_>,
    check: &'static str,
) -> Vec<Finding> {
    let behaviours: &EdgeBehaviours = match check {
        DIRECTION => &config.direction,
        DEV_DIRECTION => &config.dev_direction,
        STORAGE_ACCESS => &config.storage_access,
        CODEC_FEATURES => &config.codec_features,
        DOMAIN_DEPENDENCIES => &config.domain_dependencies,
        _ => &config.model_protocol,
    };
    static EMPTY: EdgeTable = EdgeTable::new();
    let table = ctx.baseline.layers.get(check).unwrap_or(&EMPTY);
    let crossings = checked.crossings(check);
    let mut findings: Vec<Finding> = crossings
        .iter()
        .filter(|((from, to), _)| !table.contains(from, to))
        .map(|(_, crossing)| Finding {
            rule: RULE,
            instance: Some(check.to_owned()),
            behaviour: "no_crossing",
            path: crossing.location.path.clone(),
            detail: crossing.detail.clone(),
            mode: behaviours.no_crossing.mode,
            location: crossing.location.clone(),
        })
        .collect();
    let entry =
        |behaviour: &'static str, mode: Mode, from: &str, to: &str, detail: String| Finding {
            rule: RULE,
            instance: Some(check.to_owned()),
            behaviour,
            path: format!("{from} -> {to}"),
            detail,
            mode,
            location: Location {
                path: BASELINE_FILE.to_owned(),
                line: table.line(from, to).unwrap_or(1),
            },
        };
    let base = base_edges(ctx, check);
    let seeded = if rule_on_base(ctx) == Some(true) {
        format!("the base branch's layers rule has no {check} check yet")
    } else {
        "the base branch has no layers rule yet".to_owned()
    };
    for (from, to) in table.iter() {
        let listed = format!("[{RULE}.{check}] in {BASELINE_FILE}");
        match base {
            BaseEdges::Compare(base) if !base.get(from).is_some_and(|base| base.contains(to)) => {
                findings.push(entry(
                    "no_new_entries",
                    behaviours.no_new_entries.mode,
                    from,
                    to,
                    format!(
                        ": is in {listed} but not in the base branch's copy, and layer \
                         violations can't be added to the baseline. Remove the entry and fix the \
                         dependency instead; without the entry, {RULE}.{check}.no_crossing says \
                         how."
                    ),
                ));
            }
            BaseEdges::Seeding => findings.push(entry(
                "no_new_entries",
                Mode::Advisory,
                from,
                to,
                format!(
                    ": is in {listed}, and {seeded}, so this pull request seeds the table. \
                     Review each entry."
                ),
            )),
            BaseEdges::Compare(_) | BaseEdges::Skipped => {}
        }
    }
    for (from, to) in table.iter() {
        let key = (from.to_owned(), to.to_owned());
        if crossings.contains_key(&key) {
            continue;
        }
        let reason = match check {
            DIRECTION | DEV_DIRECTION => edge_stale_reason(checked, check, &key),
            STORAGE_ACCESS | CODEC_FEATURES => {
                checked
                    .storage
                    .stale_reason(config, checked.graph, check, from, to)
            }
            _ => checked.domain.stale_reason(
                config,
                checked.graph,
                &checked.storage,
                check,
                from,
                to,
            ),
        };
        findings.push(entry(
            "stale_entries",
            behaviours.stale_entries.mode,
            from,
            to,
            format!(
                ": is in [{RULE}.{check}] in {BASELINE_FILE}, but {reason}. Remove the entry \
                 (cargo arch --write-baseline={RULE}.{check})."
            ),
        ));
    }
    findings
}

/// Why `(from, to)` in the table of `direction` or `dev_direction` is no
/// violation.
fn edge_stale_reason(checked: &Checked<'_>, check: &str, key: &(String, String)) -> String {
    let (graph, verdicts) = (checked.graph, &checked.verdicts);
    let (from, to) = (key.0.as_str(), key.1.as_str());
    let edges = if check == DEV_DIRECTION {
        &verdicts.dev
    } else {
        &verdicts.production
    };
    match (graph.member(from), graph.member(to)) {
        (None, _) => format!("no workspace member is named {from}"),
        (_, None) => format!("no workspace member is named {to}"),
        (Some(member), _) | (_, Some(member)) if member.layer.is_none() => format!(
            "{} has no layer, so no layers check sees the edge (see its {RULE}.placement \
             finding)",
            member.name()
        ),
        _ if check == DEV_DIRECTION && verdicts.production.contains(key) => format!(
            "{from}'s dev-dependency on {to} duplicates a normal or build one, which \
             [{RULE}.{DIRECTION}] covers"
        ),
        _ if !edges.contains(key) => format!(
            "{from} has no {} on {to}",
            if check == DEV_DIRECTION {
                "dev-dependency"
            } else {
                "normal or build dependency"
            }
        ),
        _ => "the layer order allows that edge now, or another layers check reports it".to_owned(),
    }
}

/// The finding text for an edge with `problem`, after the path: the edge,
/// what the layer order allows, and the mechanical fixes.
fn detail(ctx: &Context, graph: &Graph<'_>, problem: Problem, edge: &Edge<'_>) -> String {
    let (from_layer, to_layer) = layers_of(edge);
    let (from, to) = (edge.from.name(), edge.to.name());
    let declared = edge.dependency.describe();
    let items = || {
        let lib = edge.to.krate.lib.as_deref().unwrap_or_default();
        let items = used_items(&ctx.files, edge.from.krate, edge.dependency, lib);
        if items.is_empty() {
            String::new()
        } else {
            format!(" ({})", list(&items, 5))
        }
    };
    let text = match problem {
        Problem::Direction => {
            let allowed = allowed_targets(from_layer);
            let fixes = match (from_layer, to_layer) {
                (Layer::App, Layer::App) => format!(
                    "An app composes API crates but never another app: move what {from} uses \
                     from {to}{} into an l40_api crate both can depend on.",
                    items()
                ),
                (Layer::App, Layer::Storage) => format!(
                    "An app reaches state only through API crates: use {to} through the l40_api \
                     crate that owns it. A declared exception, such as the database migrator's, \
                     goes in [rules.{RULE}] allowed_edges in {CONFIG_FILE}, which \
                     {RULE}.policy_changed reports."
                ),
                _ => {
                    // What the target layer's own rules would then ask of
                    // the crate; the whole move is not simulated.
                    let then = match to_layer {
                        Layer::Storage => format!(
                            ", and as a store, each store it uses must then list it in \
                             [rules.{RULE}] store_users"
                        ),
                        Layer::Api if from.ends_with("-client") => {
                            ", and as a client, it may then depend only on l20_domain, \
                             l10_foundation and other clients"
                                .to_owned()
                        }
                        _ => String::new(),
                    };
                    // Moving up to the target's layer helps only if a crate
                    // there may depend on the target: never for an app.
                    let move_up = if may_depend(to_layer, to_layer) {
                        format!(
                            "move {from} up to {to_layer}{}{then}; ",
                            blockers(graph, from, to_layer)
                        )
                    } else {
                        String::new()
                    };
                    format!(
                        "Fix one of: move the items it uses{} down into {from_layer} or below; \
                         {move_up}or invert the edge, with a trait that {from} defines and {to} \
                         implements.",
                        items()
                    )
                }
            };
            format!(
                ": {from} ({from_layer}) depends on {to} ({to_layer}), {declared}, but \
                 {from_layer} may depend only on {allowed}. {fixes}"
            )
        }
        Problem::DevDirection => format!(
            ": {from} ({from_layer}) dev-depends on {to} ({to_layer}), {declared}, but a \
             dev-dependency may point only at the crate's own or a lower layer, or at \
             test_support. Fix one of: move the fixtures its tests use into a test_support crate, \
             or down into {from_layer} or below; or move the tests to a crate in {to_layer} or \
             above."
        ),
        Problem::TestOnly if to_layer == Layer::Tools => format!(
            ": {from} depends on {to}, a tools crate, {declared}, but nothing depends on tools, \
             which are never shipped. Move what it uses{} out of {to} into a layer crate both can \
             depend on.",
            items()
        ),
        Problem::TestOnly => format!(
            ": {from} ({from_layer}) depends on {to}, a test_support crate, {declared}, but test \
             support is reachable only through [dev-dependencies]. Make it a dev-dependency; if \
             production code needs it, move what it uses{} out of {to} into a layer crate.",
            items()
        ),
        Problem::Clients => format!(
            ": {from} is a client (an l40_api crate named *-client), which depends only on \
             l20_domain, l10_foundation and other clients, but it depends on {to} ({to_layer}), \
             {declared}. Move the types it uses{} into a protocol crate in l20_domain, such as \
             the service's *-protocol crate, which both can depend on.",
            items()
        ),
        Problem::StoreEdges => {
            let unlisted = format!(
                ": {from} ({from_layer}) depends on the store {to}, {declared}, but {to} does not \
                 list it in [rules.{RULE}] store_users in {CONFIG_FILE}."
            );
            let users = format!("add it to {to}'s users, which {RULE}.policy_changed reports");
            if from_layer == Layer::Storage {
                // A store can't reach an API crate, so the read stays in a
                // store.
                format!(
                    "{unlisted} A store reads another only as one of its users: if {from} owns \
                     this state, {users}; otherwise move the read into {to}, the store that \
                     owns the state."
                )
            } else {
                format!(
                    "{unlisted} A store is private to the crates that own its state: reach the \
                     state through its owner's API (the crate root or its client), or, if \
                     {from} owns this state, {users}."
                )
            }
        }
    };
    format!("{text} See {GUIDE}.")
}

/// What keeps `from` from moving up to `layer`, after "move it up": its
/// dependents whose layer could not then depend on it, by a normal or build
/// edge the layer matrix forbids or a dev edge from a lower layer. The client
/// and store rules are not checked.
fn blockers(graph: &Graph<'_>, from: &str, layer: Layer) -> String {
    let blockers: Vec<String> = graph
        .dependents(from)
        .into_iter()
        .filter(|(dependent, production)| {
            dependent.layer.is_some_and(|dependent| {
                if *production {
                    !may_depend(dependent, layer)
                } else {
                    dependent
                        .rank()
                        .zip(layer.rank())
                        .is_some_and(|(dependent, layer)| dependent < layer)
                }
            })
        })
        .map(|(dependent, _)| dependent.name().to_owned())
        .collect();
    if blockers.is_empty() {
        "; no dependent's layer blocks that".to_owned()
    } else {
        format!(
            ", once its dependents whose layer can't depend on {layer} have moved too ({})",
            list(&blockers, 5)
        )
    }
}

/// A finding about an entry of `[rules.layers]`, shown on its line in
/// `ARCHITECTURE.toml`.
fn config_finding(
    behaviour: &'static str,
    config: &BehaviourConfig,
    path: &str,
    line: usize,
    detail: String,
) -> Finding {
    Finding {
        rule: RULE,
        instance: None,
        behaviour,
        path: path.to_owned(),
        detail,
        mode: config.mode,
        location: Location {
            path: CONFIG_FILE.to_owned(),
            line,
        },
    }
}
