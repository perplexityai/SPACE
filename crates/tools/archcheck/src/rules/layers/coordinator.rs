//! What the layers rule asks of coordinators, beside the edge checks in
//! [`super::graph`], and of every Storage crate's library root.
//!
//! A coordinator is a Storage crate named `*-coordinator` that runs one
//! transaction across two or more stores. It owns no tables and holds no
//! SQL, so it is no store: `storage_access` and `codec_features` check it as
//! they check an API crate, and `state.declarations` does not ask it for a
//! state table.
//!
//! - `coordinators`: a coordinator with a `[package.metadata.space.state]`
//!   ([`declared_state`]); a Storage crate in a coordinator's directory whose
//!   name doesn't end in `-coordinator` ([`misnamed`]), which `layout`'s
//!   leaf rule would accept for `pplx-space-<area>`; and, from
//!   [`super::graph`], a coordinator's edge to another coordinator.
//! - `driver_reexports`: a `pub use` or `pub extern crate` of a driver in a
//!   Storage crate's library root, at its top level or in a `pub mod` block
//!   there ([`driver_reexports`]), which would hand the crate's users the
//!   driver. Items only tests compile, under `cfg(test)` or an `all(…)`
//!   that includes `test`, are skipped. Read with syn, as codec
//!   facades are (see [`super::facade`]), so a finding is on line 1 of the
//!   file and names the re-exported path. A root syn can't parse is skipped:
//!   rustc reports it, and syn may lag the language. So is a root outside
//!   the crate's `src/`, which archcheck doesn't read.

use std::collections::BTreeMap;

use syn::{Attribute, Item, Meta, Token, UseTree, Visibility, punctuated::Punctuated};

use super::{GUIDE, RULE, graph::Graph, storage::production};
use crate::{
    config::{Layer, LayersConfig},
    context::Context,
    finding::{Finding, Location},
};

/// `coordinators` findings for each coordinator that declares state, shown
/// on its state table's line.
pub(super) fn declared_state(config: &LayersConfig, graph: &Graph<'_>) -> Vec<Finding> {
    graph
        .members
        .values()
        .filter(|member| member.is_coordinator())
        .filter_map(|member| {
            let declarations = member.krate.state.as_ref()?;
            let manifest = member.krate.manifest();
            Some(Finding {
                rule: RULE,
                instance: None,
                behaviour: "coordinators",
                path: manifest.clone(),
                detail: format!(
                    ": {} is a coordinator (an l30_storage crate named *-coordinator), which owns \
                     no tables, but its Cargo.toml has a [package.metadata.space.state]. Remove \
                     it: each table the coordinator's transaction changes belongs to the store \
                     that owns it, whose verb the coordinator calls. See {GUIDE}.",
                    member.name()
                ),
                mode: config.coordinators.mode,
                location: Location {
                    path: manifest,
                    line: declarations.line,
                },
            })
        })
        .collect()
}

/// `coordinators` findings for each Storage crate whose path below the
/// layer ends in `coordinator` but whose name doesn't end in
/// `-coordinator`, shown on its `name =` line: archcheck would check it as a
/// store.
pub(super) fn misnamed(config: &LayersConfig, graph: &Graph<'_>) -> Vec<Finding> {
    graph
        .members
        .values()
        .filter(|member| {
            member.layer == Some(Layer::Storage)
                && !member.is_coordinator()
                && member
                    .layer_path
                    .is_some_and(|path| path.rsplit('/').next() == Some("coordinator"))
        })
        .map(|member| {
            let manifest = member.krate.manifest();
            Finding {
                rule: RULE,
                instance: None,
                behaviour: "coordinators",
                path: manifest.clone(),
                detail: format!(
                    ": {} is at {}, a coordinator's directory, but its name doesn't end in \
                     -coordinator, so archcheck checks it as a store. Rename it \
                     pplx-space-<area>-coordinator, or move it out of the coordinator directory. \
                     See {GUIDE}.",
                    member.name(),
                    member.krate.member,
                ),
                mode: config.coordinators.mode,
                location: Location {
                    path: manifest,
                    line: member.krate.name_line,
                },
            }
        })
        .collect()
}

/// `driver_reexports` findings for each Storage crate whose library root
/// re-exports one of its driver dependencies.
pub(super) fn driver_reexports(
    ctx: &Context,
    config: &LayersConfig,
    graph: &Graph<'_>,
) -> Vec<Finding> {
    let mut findings = Vec::new();
    for member in graph.members.values() {
        if member.layer != Some(Layer::Storage) {
            continue;
        }
        let Some(root) = &member.krate.lib_path else {
            continue;
        };
        // The name the crate's code gives each driver it depends on.
        let drivers: BTreeMap<String, &str> = production(member)
            .filter(|dependency| config.storage.drivers.contains(&dependency.package))
            .map(|dependency| {
                (
                    dependency.key().replace('-', "_"),
                    dependency.package.as_str(),
                )
            })
            .collect();
        if drivers.is_empty() {
            continue;
        }
        let Some(file) = ctx.files.iter().find(|file| file.path == *root) else {
            continue;
        };
        let Ok(parsed) = syn::parse_file(&file.text) else {
            continue;
        };
        let mut found = Vec::new();
        reexports(&parsed.items, "", &mut found);
        for (place, item, name) in found {
            let Some(driver) = drivers.get(&name) else {
                continue;
            };
            let kind = if member.is_coordinator() {
                "coordinator"
            } else {
                "store"
            };
            findings.push(Finding {
                rule: RULE,
                instance: None,
                behaviour: "driver_reexports",
                path: root.clone(),
                detail: format!(
                    ": `{item}` {place} re-exports the driver {driver} from the library root of \
                     {} (l30_storage), which hands the {kind}'s users a driver. Users reach \
                     storage only through a store's verbs and the types it defines, and a \
                     coordinator holds no SQL. Remove the re-export, and give callers a type or \
                     verb of the {kind}'s own instead. See {GUIDE}.",
                    member.name()
                ),
                mode: config.driver_reexports.mode,
                location: Location {
                    path: root.clone(),
                    line: 1,
                },
            });
        }
    }
    findings
}

/// Adds to `found` each public re-export among `items`, in `module` (a path
/// of `pub mod` names, empty for the top level), as where it is, the item
/// as written with the full path it re-exports, and that path's first name.
/// Items only tests compile are skipped.
fn reexports(items: &[Item], module: &str, found: &mut Vec<(String, String, String)>) {
    let place = if module.is_empty() {
        "at the file's top level".to_owned()
    } else {
        format!("in `{module}`")
    };
    for item in items {
        match item {
            Item::Use(inner) if is_pub(&inner.vis) && !is_cfg_test(&inner.attrs) => {
                let colon = if inner.leading_colon.is_some() {
                    "::"
                } else {
                    ""
                };
                let mut paths = Vec::new();
                tree_paths(&inner.tree, "", &mut paths);
                for (root, path) in paths {
                    found.push((place.clone(), format!("pub use {colon}{path}"), root));
                }
            }
            Item::ExternCrate(inner) if is_pub(&inner.vis) && !is_cfg_test(&inner.attrs) => {
                let name = inner.ident.to_string();
                found.push((place.clone(), format!("pub extern crate {name}"), name));
            }
            Item::Mod(inner) if is_pub(&inner.vis) && !is_cfg_test(&inner.attrs) => {
                if let Some((_, content)) = &inner.content {
                    let name = if module.is_empty() {
                        inner.ident.to_string()
                    } else {
                        format!("{module}::{}", inner.ident)
                    };
                    reexports(content, &name, found);
                }
            }
            _ => {}
        }
    }
}

/// Adds to `paths` each path `tree` re-exports below `prefix`, with its first
/// name: `("sqlx", "sqlx::PgPool")`, `("sqlx", "sqlx as db")` or
/// `("sqlx", "sqlx::*")`.
fn tree_paths(tree: &UseTree, prefix: &str, paths: &mut Vec<(String, String)>) {
    let join = |name: &str| {
        if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}::{name}")
        }
    };
    let root = |path: &str| path.split("::").next().unwrap_or_default().to_owned();
    match tree {
        UseTree::Path(inner) => tree_paths(&inner.tree, &join(&inner.ident.to_string()), paths),
        UseTree::Name(inner) => {
            let path = join(&inner.ident.to_string());
            paths.push((root(&path), path));
        }
        UseTree::Rename(inner) => {
            let path = join(&inner.ident.to_string());
            paths.push((root(&path), format!("{path} as {}", inner.rename)));
        }
        UseTree::Glob(_) => {
            let path = join("*");
            paths.push((root(&path), path));
        }
        UseTree::Group(group) => {
            for tree in &group.items {
                tree_paths(tree, prefix, paths);
            }
        }
    }
}

/// Whether `attrs` hold a `#[cfg(…)]` that only tests satisfy.
fn is_cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg") && attr.parse_args::<Meta>().is_ok_and(|meta| test_only(&meta))
    })
}

/// Whether the cfg predicate `meta` holds only in tests: `test`, or `all(…)`
/// with a member that does. `any(…)`, `not(…)` and anything else can hold
/// outside tests.
fn test_only(meta: &Meta) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident("test"),
        Meta::List(list) if list.path.is_ident("all") => list
            .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            .is_ok_and(|members| members.iter().any(test_only)),
        _ => false,
    }
}

fn is_pub(vis: &Visibility) -> bool {
    matches!(vis, Visibility::Public(_))
}
