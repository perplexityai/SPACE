//! `layers.facades`: an API crate uses another one only through its crate
//! root, the items it re-exports there, or through a client.
//!
//! Each production file of a member outside `l50_app` and `tools` is read
//! for paths into a module of an API crate that is not a client, such as
//! `pplx_space_api::controllers::Substrate` (see [`reaches`]). An app
//! composes its children at any path, and a client is the way in, so
//! neither is counted. The unit is the item: what a pair of packages
//! reaches is the set of distinct items one names inside the other's
//! modules, and is what the fix re-exports or moves. Formatting, repeated
//! mentions, the import style and file splits into the crate root and its
//! file modules (the root's names reached through `crate::`, a parent's
//! only through its children's `super::` and `use super::*;`) leave it
//! alone, but for three cases: a glob of a module is one `m::*` item; a
//! glob of the root used only for root items is `*`, until the glob is
//! replaced; and the `super::` paths of a module declared with `#[path]`,
//! or inside an inline module, are not seen.
//! `[layers.facades]` lists each pair's items, keyed `<from>.<to>` by
//! package names, and the three behaviours ratchet it as the edge tables
//! are: an item not listed, a listed item the base branch's copy lacks,
//! and a listed item no longer reached.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use super::{
    BaseEdges, GUIDE, RULE, base_edges,
    graph::{Graph, Member, file_owner, list},
    scan::{Exports, File, Names, ROOT, Reach, Target, reaches, root_modules},
    seeded,
};
use crate::{
    baseline::{BASELINE_FILE, Baseline, EdgeTable},
    config::{Layer, LayersConfig, Mode},
    context::{Context, ProductionFile},
    finding::{Finding, Location},
    rules::NotAdded,
};

/// The check's name, as in its finding codes and its baseline table.
pub(super) const FACADES: &str = "facades";

/// A module whose items are generated wire types; a client or protocol
/// crate holds them better than a crate root, so the finding counts them
/// apart and adds that fix to the others. They are counted like any other
/// module.
const PROTO: &str = "proto";

/// The items one package reaches inside another's modules.
#[derive(Debug, Default)]
pub(super) struct Reached {
    /// Each item, as [`scan::Reach::item`], with the first file and line
    /// that names it.
    ///
    /// [`scan::Reach::item`]: super::scan::Reach::item
    items: BTreeMap<String, (String, usize)>,
}

impl Reached {
    fn count(&self) -> usize {
        self.items.len()
    }

    /// The modules reached, most items first, each as `<module> <items>`;
    /// a glob of the root, which reaches all of them, is `* (a root glob)`.
    fn modules(&self) -> Vec<String> {
        let mut modules: BTreeMap<&str, usize> = BTreeMap::new();
        for item in self.items.keys() {
            let module = item.split("::").next().unwrap_or_default();
            *modules.entry(module).or_default() += 1;
        }
        let mut modules: Vec<(&str, usize)> = modules.into_iter().collect();
        modules.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        modules
            .into_iter()
            .map(|(module, count)| match module {
                "*" => format!("* (a root glob) {count}"),
                _ => format!("{module} {count}"),
            })
            .collect()
    }

    /// The number of items under [`PROTO`].
    fn proto(&self) -> usize {
        self.items
            .keys()
            .filter(|item| item.split("::").next() == Some(PROTO))
            .count()
    }
}

/// Whether paths into `member`'s modules are counted: an API crate with a
/// library that is not a client.
fn is_target(member: &Member<'_>) -> bool {
    member.layer == Some(Layer::Api) && !member.is_client() && member.krate.lib.is_some()
}

/// Whether `member`'s sources are read: any crate with a layer but an app,
/// which composes its children at any path, or a tool.
fn is_consumer(member: &Member<'_>) -> bool {
    member
        .layer
        .is_some_and(|layer| !matches!(layer, Layer::App | Layer::Tools))
}

/// `member`'s root file: its library's `src/lib.rs`, or else its
/// `src/main.rs`.
fn root_file(member: &Member<'_>) -> String {
    let sources = if member.krate.member.is_empty() {
        "src/".to_owned()
    } else {
        format!("{}/src/", member.krate.member)
    };
    let root = if member.krate.lib.is_some() {
        "lib.rs"
    } else {
        "main.rs"
    };
    format!("{sources}{root}")
}

/// What each `(from, to)` pair reaches, for every pair that reaches
/// something.
pub(super) fn measure(ctx: &Context, graph: &Graph<'_>) -> BTreeMap<(String, String), Reached> {
    let modules: BTreeMap<&str, BTreeSet<String>> = graph
        .members
        .values()
        .filter(|member| is_target(member))
        .map(|member| {
            let root = ctx.file(&root_file(member));
            let modules = root.map_or_else(BTreeSet::new, |file| root_modules(&file.text));
            (member.name(), modules)
        })
        .collect();
    // Each consumer's production files, in order.
    let mut files: BTreeMap<&str, Vec<&ProductionFile>> = BTreeMap::new();
    for file in &ctx.files {
        let Some(owner) =
            file_owner(&ctx.crates, &file.path).and_then(|krate| graph.member(&krate.name))
        else {
            continue;
        };
        if is_consumer(owner) {
            files.entry(owner.name()).or_default().push(file);
        }
    }
    let mut measured: BTreeMap<(String, String), Reached> = BTreeMap::new();
    for (from, files) in files {
        let Some(owner) = graph.member(from) else {
            continue;
        };
        let root = ctx.file(&root_file(owner)).map(|root| root.text.as_str());
        let names = Names::new(anchors(graph, owner, &modules), root);
        if names.is_empty() {
            continue;
        }
        for (path, reach) in crate_reaches(owner, &names, &files) {
            measured
                .entry((from.to_owned(), reach.package.to_owned()))
                .or_default()
                .items
                .entry(reach.item)
                .or_insert_with(|| (path.to_owned(), reach.line));
        }
    }
    measured
}

/// What a file module binds at its top level, the module paths of its
/// ancestors, parent first, and the modules it declares as files.
type Read<'t> = (Exports<'t>, Vec<String>, BTreeSet<String>);

/// What `files`, `member`'s, reach by `names`, in their order, but for the
/// imports that a path of another file goes on through. A file module is
/// read after its parent, whose top-level names its `super::` paths see,
/// if the parent declares it as `mod <name>;` without a `#[path]`.
fn crate_reaches<'f, 't>(
    member: &Member<'_>,
    names: &Names<'t>,
    files: &[&'f ProductionFile],
) -> Vec<(&'f str, Reach<'t>)> {
    let mut order: Vec<(usize, String)> = files
        .iter()
        .enumerate()
        .map(|(index, file)| (index, module_path(member, &file.path)))
        .collect();
    order.sort_by_key(|(index, module)| (module.matches("::").count(), *index));
    let mut read: BTreeMap<String, Read<'t>> = BTreeMap::new();
    let mut found = Vec::new();
    let mut through = BTreeSet::new();
    for (index, module) in order {
        let Some(file) = files.get(index) else {
            continue;
        };
        let chain: Vec<String> = module
            .rsplit_once("::")
            .and_then(|(parent, name)| {
                let (_, chain, children) = read.get(parent)?;
                let chain = std::iter::once(parent.to_owned()).chain(chain.iter().cloned());
                children.contains(name).then(|| chain.collect())
            })
            .unwrap_or_default();
        let ancestors: Vec<&Exports<'t>> = chain
            .iter()
            .filter_map(|ancestor| read.get(ancestor).map(|(exports, ..)| exports))
            .collect();
        let file_module = File {
            module: &module,
            ancestors: &ancestors,
        };
        let scanned = reaches(&file.text, names, file_module);
        found.extend(scanned.reaches.into_iter().map(|reach| (index, reach)));
        through.extend(scanned.through);
        read.insert(module, (scanned.exports, chain, scanned.children));
    }
    found.sort_by_key(|(index, _)| *index);
    found
        .into_iter()
        .filter(|(_, reach)| !reach.unless.iter().any(|key| through.contains(key)))
        .filter_map(|(index, reach)| Some((files.get(index)?.path.as_str(), reach)))
        .collect()
}

/// The module path of `member`'s file at `path`, as `crate::a::b` for
/// `src/a/b.rs` or `src/a/b/mod.rs`, and [`ROOT`] for its root file; a file
/// outside `src/` keeps its path, which no module has.
fn module_path(member: &Member<'_>, path: &str) -> String {
    let root = root_file(member);
    if path == root {
        return ROOT.to_owned();
    }
    let sources = root
        .strip_suffix(".rs")
        .and_then(|root| root.rsplit_once('/'));
    let module = sources.and_then(|(sources, _)| {
        let module = path.strip_prefix(sources)?.strip_prefix('/')?;
        let module = module.strip_suffix(".rs")?;
        Some(
            module
                .strip_suffix("/mod")
                .unwrap_or(module)
                .replace('/', "::"),
        )
    });
    match module {
        Some(module) => format!("{ROOT}::{module}"),
        None => path.to_owned(),
    }
}

/// The names `from` imports each target it depends on by: the library's
/// name, or the manifest's rename, with `-` as `_`. A dependency of any
/// kind counts, since a production file under `#[cfg(test)] mod x;` may use
/// a dev-dependency.
fn anchors<'t>(
    graph: &'t Graph<'_>,
    from: &Member<'_>,
    modules: &'t BTreeMap<&str, BTreeSet<String>>,
) -> BTreeMap<String, Target<'t>> {
    let mut names = BTreeMap::new();
    for dependency in &from.krate.dependencies {
        let (Some(to), Some(modules)) = (
            graph.member(&dependency.package),
            modules.get(dependency.package.as_str()),
        ) else {
            continue;
        };
        if to.name() == from.name() {
            continue;
        }
        let lib = to.krate.lib.as_deref().unwrap_or_default();
        let name = dependency
            .rename
            .as_deref()
            .unwrap_or(lib)
            .replace('-', "_");
        let target = Target {
            package: to.name(),
            modules,
        };
        names.insert(name, target);
    }
    names
}

/// The key of a pair's entry in `[layers.facades]`.
fn pair_key(from: &str, to: &str) -> String {
    format!("{from}.{to}")
}

/// `count` items, for a sentence.
fn items(count: usize) -> String {
    if count == 1 {
        "1 item".to_owned()
    } else {
        format!("{count} items")
    }
}

/// What a pair reaches, for a sentence: `{n} items inside {to}'s modules
/// (<module> <items>, …)`, and how many are under `proto`.
fn reached_text(to: &str, reached: &Reached) -> String {
    let proto = match reached.proto() {
        0 => String::new(),
        count => format!(", {count} of them under {PROTO}"),
    };
    format!(
        "{} inside {to}'s modules ({}){proto}",
        items(reached.count()),
        list(&reached.modules(), 5),
    )
}

/// The fixes, after `Fix one of: `. A target that several crates already
/// share is the shared library, so the code moves to where it belongs
/// rather than into another crate both depend on.
fn fixes(graph: &Graph<'_>, from: &str, to: &str, reached: &Reached) -> String {
    let proto = if reached.proto() > 0 {
        format!(
            "; for the {PROTO} items, a client or protocol crate that both use, which this check \
             does not count"
        )
    } else {
        String::new()
    };
    let sharing = graph
        .dependents(to)
        .into_iter()
        .filter(|(member, normal)| *normal && is_consumer(member))
        .count();
    let reexport = format!(
        "re-export the items {from} uses at {to}'s crate root (`pub use`) and import them from \
         there"
    );
    if sharing > 1 {
        format!(
            "{reexport}; or, since {to} is already a library {sharing} crates share, move the \
             code to where it belongs: a crate of its own, or {from} if only it uses \
             it{proto}"
        )
    } else {
        format!(
            "{reexport}; or move the code they belong to out of {to}, into a crate both can \
             depend on, such as a shared l40_api crate for operations both services run{proto}"
        )
    }
}

/// The library name of `package`, as a path starts with it.
fn lib_of<'a>(graph: &'a Graph<'_>, package: &str) -> &'a str {
    graph
        .member(package)
        .and_then(|member| member.krate.lib.as_deref())
        .unwrap_or_default()
}

/// `layers.facades`' findings.
pub(super) fn check(ctx: &Context, config: &LayersConfig, graph: &Graph<'_>) -> Vec<Finding> {
    static EMPTY: EdgeTable = EdgeTable::new();
    let modes = &config.facades;
    let measured = measure(ctx, graph);
    let table = ctx.baseline.layers.get(FACADES).unwrap_or(&EMPTY);
    let listed = format!("[{RULE}.{FACADES}] in {BASELINE_FILE}");
    let finding = |behaviour: &'static str, mode: Mode, pair: &str, location, detail| Finding {
        rule: RULE,
        instance: Some(FACADES.to_owned()),
        behaviour,
        path: pair.to_owned(),
        detail,
        mode,
        location,
    };
    let entry = |key: &str, item: &str| Location {
        path: BASELINE_FILE.to_owned(),
        line: table.line(key, item).unwrap_or(1),
    };
    let mut findings = Vec::new();
    for ((from, to), reached) in &measured {
        let key = pair_key(from, to);
        let lib = lib_of(graph, to);
        for (item, (path, line)) in &reached.items {
            if table.contains(&key, item) {
                continue;
            }
            findings.push(finding(
                "no_crossing",
                modes.no_crossing.mode,
                &format!("{from} -> {to}"),
                Location {
                    path: path.clone(),
                    line: *line,
                },
                format!(
                    ": {from} reaches {lib}::{item}, which {listed} does not list, rather than \
                     through {to}'s crate root; in all it reaches {}. An API crate uses another \
                     only through its root, the items it re-exports there, or a client. Fix one \
                     of: {}. See {GUIDE}.",
                    reached_text(to, reached),
                    fixes(graph, from, to, reached),
                ),
            ));
        }
    }
    match base_edges(ctx, FACADES) {
        BaseEdges::Compare(base) => {
            for (key, item) in table.iter() {
                if base.get(key).is_some_and(|base| base.contains(item)) {
                    continue;
                }
                let (pair, to) = describe(key);
                findings.push(finding(
                    "no_new_entries",
                    modes.no_new_entries.mode,
                    &pair,
                    entry(key, item),
                    format!(
                        ": lists {item} in {listed}, and the base branch's copy does not. A new \
                         path into another API crate's modules is fixed, not baselined: \
                         re-export the item at {to}'s root or move the code, and remove the \
                         entry; without it, {RULE}.{FACADES}.no_crossing says how. Keep it only \
                         as a reviewed exception, saying why in the pull request."
                    ),
                ));
            }
        }
        BaseEdges::Seeding => {
            // One finding per pair, at its first item, rather than one per
            // item seeded.
            let seeded = seeded(ctx, FACADES);
            for (key, entries) in &table.edges {
                let Some(first) = entries.first() else {
                    continue;
                };
                let (pair, _) = describe(key);
                findings.push(finding(
                    "no_new_entries",
                    Mode::Advisory,
                    &pair,
                    entry(key, first),
                    format!(
                        ": lists {} in {listed}, and {seeded}, so this pull request seeds the \
                         table. Review each entry.",
                        items(entries.len())
                    ),
                ));
            }
        }
        BaseEdges::Skipped => {}
    }
    let write = format!("cargo arch --write-baseline={RULE}.{FACADES}");
    for (key, item) in table.iter() {
        let (pair, _) = describe(key);
        let reason = match split_key(key) {
            None => format!("its key, {key}, is not <from>.<to>"),
            Some((from, to)) => match measured.get(&(from.to_owned(), to.to_owned())) {
                Some(reached) if reached.items.contains_key(item) => continue,
                Some(_) => format!(
                    "{from}'s production code no longer reaches {}::{item}",
                    lib_of(graph, to)
                ),
                None => gone(graph, from, to),
            },
        };
        findings.push(finding(
            "stale_entries",
            modes.stale_entries.mode,
            &pair,
            entry(key, item),
            format!(": lists {item} in {listed}, but {reason}. Remove the entry ({write})."),
        ));
    }
    findings
}

/// A pair's key split into `(from, to)`, if it is `<from>.<to>`.
fn split_key(key: &str) -> Option<(&str, &str)> {
    key.split_once('.')
        .filter(|(from, to)| !from.is_empty() && !to.is_empty())
}

/// A pair's key as a finding's path, `<from> -> <to>`, and its `<to>`.
fn describe(key: &str) -> (String, &str) {
    match split_key(key) {
        Some((from, to)) => (format!("{from} -> {to}"), to),
        None => (key.to_owned(), key),
    }
}

/// Why a listed pair reaches nothing that the check counts.
fn gone(graph: &Graph<'_>, from: &str, to: &str) -> String {
    let (Some(from_member), Some(to_member)) = (graph.member(from), graph.member(to)) else {
        let missing = if graph.member(from).is_none() {
            from
        } else {
            to
        };
        return format!("no workspace member is named {missing}");
    };
    match (from_member.layer, to_member.layer) {
        (None, _) | (_, None) => {
            let member = if from_member.layer.is_none() {
                from
            } else {
                to
            };
            format!(
                "{member} has no layer, so no layers check sees it (see its {RULE}.placement finding)"
            )
        }
        (Some(layer), _) if !is_consumer(from_member) => format!(
            "{from} is in {layer}, whose sources this check does not read: an app composes its \
             children at any path, and tools are never shipped"
        ),
        _ if to_member.is_client() => {
            format!("{to} is a client, the way into its service, so paths into it are not counted")
        }
        (_, Some(layer)) if !is_target(to_member) => format!(
            "{to} is in {layer}, not an l40_api crate with a library, so paths into it are not \
             counted"
        ),
        _ => format!("{from}'s production code no longer reaches inside {to}'s modules"),
    }
}

/// Keeps each listed item still reached, removes the rest, and adds none:
/// an item without an entry is listed, to fix or, for a reviewed
/// exception, to add by hand, which `no_new_entries` reports. The table is
/// always written.
pub(super) fn update_baseline(
    ctx: &Context,
    graph: &Graph<'_>,
    baseline: &mut Baseline,
) -> Vec<NotAdded> {
    let existing = ctx.baseline.layers.get(FACADES);
    let mut table = EdgeTable::default();
    let mut not_added = Vec::new();
    for ((from, to), reached) in measure(ctx, graph) {
        let key = pair_key(&from, &to);
        let mut unlisted = Vec::new();
        for item in reached.items.keys() {
            if existing.is_some_and(|existing| existing.contains(&key, item)) {
                table.insert(&key, item);
            } else {
                unlisted.push(item.clone());
            }
        }
        if unlisted.is_empty() {
            continue;
        }
        not_added.push(NotAdded {
            table: format!("{RULE}.{FACADES}"),
            entry: None,
            line: format!(
                "not added: {from} -> {to}, {} inside its modules ({}), in [{RULE}.{FACADES}]; \
                 re-export them at its root or move the code (cargo arch says how), or, for a \
                 reviewed exception, add each to \"{key}\" inside [{RULE}.{FACADES}] by hand, \
                 which {RULE}.{FACADES}.no_new_entries reports",
                items(unlisted.len()),
                list(&unlisted, 5),
            ),
        });
    }
    baseline.layers.insert(FACADES.to_owned(), table);
    not_added
}
