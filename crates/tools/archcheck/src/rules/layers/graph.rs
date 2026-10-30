//! Each workspace member's layer, the edges between members, and what the
//! layer order says about each edge.

use std::collections::{BTreeMap, BTreeSet};

use regex::Regex;

use crate::{
    config::{Layer, LayersConfig},
    context::{Context, Crate, Dependency, ProductionFile},
};

/// The prefix every SPACE package name has; a crate's path under its layer
/// directory is the rest of the name.
pub(super) const PACKAGE_PREFIX: &str = "pplx-space-";

/// Where the layer directories are.
const CRATES_DIR: &str = "crates";

/// A workspace member and its layer.
pub(super) struct Member<'a> {
    pub krate: &'a Crate,
    /// From its directory, or else from its `unmoved` entry; `None` when
    /// neither gives one.
    pub layer: Option<Layer>,
    /// The member's path below its layer directory, when it is in one.
    pub layer_path: Option<&'a str>,
}

impl Member<'_> {
    pub fn name(&self) -> &str {
        &self.krate.name
    }

    /// A service's client: an API crate named `*-client`.
    pub fn is_client(&self) -> bool {
        self.layer == Some(Layer::Api) && self.krate.name.ends_with("-client")
    }
}

/// The layer directory `member` is in, and its path below it (empty for the
/// directory itself).
pub(super) fn layer_dir(member: &str) -> Option<(Layer, &str)> {
    let rest = member.strip_prefix(CRATES_DIR)?.strip_prefix('/')?;
    let (dir, path) = rest.split_once('/').unwrap_or((rest, ""));
    Some((Layer::from_dir(dir)?, path))
}

/// `krate`'s layer, from its directory or else from its `unmoved` entry, and
/// its path below its layer directory when it is in one.
pub(super) fn member_layer<'a>(
    config: &LayersConfig,
    krate: &'a Crate,
) -> (Option<Layer>, Option<&'a str>) {
    match layer_dir(&krate.member) {
        Some((layer, path)) => (Some(layer), Some(path)),
        None => (config.unmoved.get(&krate.name), None),
    }
}

/// The crate whose production file is at `path`: the one with the longest
/// member path whose `src/` holds it, so that a member nested in another's
/// `src/` owns the files below it.
pub(super) fn file_owner<'a>(crates: &'a [Crate], path: &str) -> Option<&'a Crate> {
    crates
        .iter()
        .filter(|krate| {
            let src = if krate.member.is_empty() {
                "src/".to_owned()
            } else {
                format!("{}/src/", krate.member)
            };
            path.starts_with(&src)
        })
        .max_by_key(|krate| krate.member.len())
}

/// The directories a package's path below its layer directory is made of:
/// its name without [`PACKAGE_PREFIX`], split on `-`.
pub(super) fn name_segments(package: &str) -> Vec<&str> {
    package
        .strip_prefix(PACKAGE_PREFIX)
        .unwrap_or(package)
        .split('-')
        .collect()
}

/// A dependency of one member on another, both with a layer.
#[derive(Clone, Copy)]
pub(super) struct Edge<'a> {
    pub from: &'a Member<'a>,
    pub to: &'a Member<'a>,
    /// The first manifest record that declares it.
    pub dependency: &'a Dependency,
}

impl Edge<'_> {
    pub fn key(&self) -> (String, String) {
        (self.from.name().to_owned(), self.to.name().to_owned())
    }
}

/// What the layers rule finds wrong with an edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Problem {
    /// A normal or build edge against the layer order.
    Direction,
    /// A dev edge that points at a higher layer.
    DevDirection,
    /// A normal or build edge into test support, or any edge into tools.
    TestOnly,
    /// A client's normal or build edge to anything but Domain, Foundation
    /// and other clients.
    Clients,
    /// An API or Storage crate's normal or build edge to a store that does
    /// not list it.
    StoreEdges,
}

/// Every member by package name, with its edges.
pub(super) struct Graph<'a> {
    pub members: BTreeMap<&'a str, Member<'a>>,
}

impl<'a> Graph<'a> {
    pub fn new(ctx: &'a Context, config: &LayersConfig) -> Self {
        let members = ctx
            .crates
            .iter()
            .map(|krate| {
                let (layer, layer_path) = member_layer(config, krate);
                let member = Member {
                    krate,
                    layer,
                    layer_path,
                };
                (krate.name.as_str(), member)
            })
            .collect();
        Self { members }
    }

    pub fn member(&self, name: &str) -> Option<&Member<'a>> {
        self.members.get(name)
    }

    /// The normal and build edges, and the dev edges, between members that
    /// both have a layer, one per pair: a dev edge that duplicates a normal
    /// or build one adds nothing and is left out.
    pub fn edges(&self) -> (Vec<Edge<'_>>, Vec<Edge<'_>>) {
        let mut production = Vec::new();
        let mut dev = Vec::new();
        for from in self
            .members
            .values()
            .filter(|member| member.layer.is_some())
        {
            let mut seen = BTreeSet::new();
            let mut seen_dev = BTreeMap::new();
            for dependency in &from.krate.dependencies {
                let Some(to) = self.member(&dependency.package) else {
                    continue;
                };
                if to.layer.is_none() {
                    continue;
                }
                let edge = Edge {
                    from,
                    to,
                    dependency,
                };
                if dependency.is_dev() {
                    seen_dev.entry(to.name()).or_insert(edge);
                } else if seen.insert(to.name()) {
                    production.push(edge);
                }
            }
            dev.extend(
                seen_dev
                    .into_iter()
                    .filter(|(to, _)| !seen.contains(to))
                    .map(|(_, edge)| edge),
            );
        }
        (production, dev)
    }

    /// The members with an edge to `name`, each once, and whether one of
    /// its edges is a normal or build one rather than only dev ones.
    pub fn dependents(&self, name: &str) -> Vec<(&Member<'a>, bool)> {
        self.members
            .values()
            .filter_map(|member| {
                let mut edges = member
                    .krate
                    .dependencies
                    .iter()
                    .filter(|dependency| dependency.package == name)
                    .peekable();
                edges.peek()?;
                Some((member, edges.any(|dependency| !dependency.is_dev())))
            })
            .collect()
    }
}

/// Whether a crate in `from` may have a normal or build edge to one in `to`
/// by the layer order alone, before the client and store rules.
pub(super) fn may_depend(from: Layer, to: Layer) -> bool {
    use Layer::{Api, App, Domain, Foundation, Storage, TestSupport, Tools};
    match (from, to) {
        (TestSupport | Tools, _) => true,
        (_, TestSupport | Tools) => false,
        (Foundation, Foundation)
        | (Domain, Domain | Foundation)
        | (Storage, Storage | Domain | Foundation)
        | (Api, Api | Storage | Domain | Foundation)
        | (App, Api | Domain | Foundation) => true,
        _ => false,
    }
}

/// What the layers that `from` may depend on are, in words.
pub(super) fn allowed_targets(from: Layer) -> &'static str {
    match from {
        Layer::Foundation => "l10_foundation",
        Layer::Domain => "l20_domain and l10_foundation",
        Layer::Storage => "the stores that list it, l20_domain and l10_foundation",
        Layer::Api => "l40_api, the stores that list it, l20_domain and l10_foundation",
        Layer::App => "l40_api, l20_domain and l10_foundation",
        Layer::TestSupport | Layer::Tools => "any layer",
    }
}

/// The problem with a normal or build edge, if any, and whether an
/// `allowed_edges` entry is what allows it.
pub(super) fn production_problem(
    config: &LayersConfig,
    edge: &Edge<'_>,
) -> (Option<Problem>, bool) {
    let (Some(from), Some(to)) = (edge.from.layer, edge.to.layer) else {
        return (None, false);
    };
    let test_only_from = matches!(from, Layer::TestSupport | Layer::Tools);
    if to == Layer::Tools || (to == Layer::TestSupport && !test_only_from) {
        return (Some(Problem::TestOnly), false);
    }
    if test_only_from {
        return (None, false);
    }
    let problem = if edge.from.is_client() {
        let allowed = matches!(to, Layer::Domain | Layer::Foundation) || edge.to.is_client();
        (!allowed).then_some(Problem::Clients)
    } else if to == Layer::Storage && matches!(from, Layer::Api | Layer::Storage) {
        let listed = config
            .store_users
            .get(edge.to.name())
            .is_some_and(|users| users.contains(edge.from.name()));
        (!listed).then_some(Problem::StoreEdges)
    } else {
        (!may_depend(from, to)).then_some(Problem::Direction)
    };
    let excepted = problem.is_some()
        && config
            .allowed_edges
            .get(edge.from.name())
            .is_some_and(|allowed| allowed.contains(edge.to.name()));
    if excepted {
        (None, true)
    } else {
        (problem, false)
    }
}

/// The problem with a dev edge, if any: it may point only at the same or a
/// lower layer, or at test support, unless it comes from test support or
/// tools; nothing depends on tools.
pub(super) fn dev_problem(edge: &Edge<'_>) -> Option<Problem> {
    let (from, to) = (edge.from.layer?, edge.to.layer?);
    if to == Layer::Tools {
        return Some(Problem::TestOnly);
    }
    if matches!(from, Layer::TestSupport | Layer::Tools) || to == Layer::TestSupport {
        return None;
    }
    (to.rank() > from.rank()).then_some(Problem::DevDirection)
}

/// The items `krate`'s production files name through `dependency`, such as
/// `pplx_space_api_route::token::RouteToken`, for a finding to say what
/// crosses an edge: every path that starts with the name the crate imports
/// the dependency by, with `use` groups expanded. A path after another
/// path's `::`, such as `other::b::x` for `b`, is another crate's module. A
/// name imported under another name, or used through a macro, is not seen;
/// nor is anything when the target has no library and the dependency no
/// other name.
pub(super) fn used_items(
    files: &[ProductionFile],
    krate: &Crate,
    dependency: &Dependency,
    lib: &str,
) -> Vec<String> {
    let name = dependency
        .rename
        .as_deref()
        .unwrap_or(lib)
        .replace('-', "_");
    if name.is_empty() {
        return Vec::new();
    }
    let Ok(pattern) = Regex::new(&format!(r"\b{}\s*::", regex::escape(&name))) else {
        return Vec::new();
    };
    let prefix = if krate.member.is_empty() {
        "src/".to_owned()
    } else {
        format!("{}/src/", krate.member)
    };
    let mut items = BTreeSet::new();
    for file in files.iter().filter(|file| file.path.starts_with(&prefix)) {
        for found in pattern.find_iter(&file.text) {
            let before = file.text.get(..found.start()).unwrap_or_default();
            if before.trim_end().ends_with("::") {
                continue;
            }
            let mut rest = file.text.get(found.end()..).unwrap_or_default();
            let mut paths = Vec::new();
            use_tree(&mut rest, "", &mut paths);
            items.extend(paths.into_iter().map(|path| format!("{name}::{path}")));
        }
    }
    items.into_iter().collect()
}

/// Adds to `paths` each path that the use tree at the start of `text` names
/// below `prefix`, and moves `text` past the tree: `a::B` and `a::c::D` for
/// `a::{B, c::D}`. `self` and a glob name `prefix` itself, and `as` and its
/// alias are skipped; anything else that is not a path segment, such as the
/// `<` of `a::B::<T>` or a trailing comma's `}`, ends the tree.
pub(super) fn use_tree(text: &mut &str, prefix: &str, paths: &mut Vec<String>) {
    let mut path = prefix.to_owned();
    loop {
        let rest = text.trim_start();
        if let Some(group) = rest.strip_prefix('{') {
            *text = group;
            loop {
                use_tree(text, &path, paths);
                match text.trim_start().strip_prefix(',') {
                    Some(next) => *text = next,
                    None => break,
                }
            }
            let rest = text.trim_start();
            *text = rest.strip_prefix('}').unwrap_or(rest);
            return;
        }
        let (segment, after) = rest.split_at(ident_len(rest));
        if segment.is_empty() || segment == "self" {
            let glob = after.strip_prefix('*');
            *text = glob.unwrap_or(after);
            let names_path = segment == "self" || glob.is_some() || path != prefix;
            if names_path && !path.is_empty() {
                paths.push(path);
            }
            return;
        }
        path = if path.is_empty() {
            segment.to_owned()
        } else {
            format!("{path}::{segment}")
        };
        match after.trim_start().strip_prefix("::") {
            Some(next) => *text = next,
            None => {
                paths.push(path);
                *text = after;
                let alias = after
                    .trim_start()
                    .strip_prefix("as")
                    .filter(|alias| alias.starts_with(char::is_whitespace));
                if let Some(alias) = alias {
                    let alias = alias.trim_start();
                    *text = alias.split_at(ident_len(alias)).1;
                }
                return;
            }
        }
    }
}

/// The length of the identifier at the start of `text`, 0 if none is.
fn ident_len(text: &str) -> usize {
    text.find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(text.len())
}

/// `names` as a list for a sentence, at most `limit` of them.
pub(super) fn list(names: &[String], limit: usize) -> String {
    match names.get(..limit) {
        Some(shown) if names.len() > limit => {
            format!("{} and {} more", shown.join(", "), names.len() - limit)
        }
        _ => names.join(", "),
    }
}
