//! `layers.stale_entries` for `[rules.layers]` entries that no longer apply,
//! and `layers.policy_changed` for every change to `[rules.layers]` from the
//! base branch's `ARCHITECTURE.toml`, including its removal.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    RULE, Verdicts, config_finding,
    graph::{Graph, Problem},
};
use crate::{
    config::{BaseArchitecture, CONFIG_FILE, Entries, Layer, LayersConfig, LayersPolicy, Mode},
    context::Context,
    finding::{Finding, Location},
};

/// `unmoved` entries for crates that are gone or in their layer directory,
/// `store_users` entries that allow nothing, and `allowed_edges` entries
/// for edges that don't exist or need no exception. An entry naming a crate
/// without a layer is left to that crate's `placement` finding.
pub(super) fn stale_entries(
    ctx: &Context,
    config: &LayersConfig,
    graph: &Graph<'_>,
    verdicts: &Verdicts<'_>,
) -> Vec<Finding> {
    let policy = &ctx.layers_policy;
    let behaviour = &config.stale_entries;
    let finding = |path: &str, line: Option<usize>, detail: String| {
        config_finding(
            "stale_entries",
            behaviour,
            path,
            line.unwrap_or(policy.line),
            detail,
        )
    };
    let mut findings = Vec::new();
    for package in config.unmoved.packages() {
        let line = policy.unmoved.entries.get(package).map(|(_, line)| *line);
        let reason = match graph.member(package) {
            None => "no workspace member has this name".to_owned(),
            Some(member) => match member.layer_path {
                Some(_) => format!(
                    "the crate is at {}, whose directory gives its layer",
                    member.krate.member
                ),
                None => continue,
            },
        };
        findings.push(finding(
            package,
            line,
            format!(
                ": is in [rules.{RULE}] unmoved in {CONFIG_FILE}, but {reason}. Remove the entry."
            ),
        ));
    }
    for (store, users) in &config.store_users {
        let (store_line, user_lines) = lines_of(&policy.store_users, store);
        let reason = match graph.member(store).map(|member| member.layer) {
            None => Some("no workspace member has this name".to_owned()),
            Some(Some(layer)) if layer != Layer::Storage => Some(format!(
                "it is not a store: its layer is {layer}, not l30_storage"
            )),
            Some(_) => None,
        };
        if let Some(reason) = reason {
            findings.push(finding(
                store,
                store_line,
                format!(
                    ": is in [rules.{RULE}] store_users in {CONFIG_FILE}, but {reason}. Remove \
                     the entry."
                ),
            ));
            continue;
        }
        for user in users.iter() {
            let reason = match graph.member(user) {
                None => "no workspace member has this name".to_owned(),
                Some(member) if member.is_client() => {
                    "it is a client, which never depends on a store".to_owned()
                }
                Some(member) => match member.layer {
                    None | Some(Layer::Api | Layer::Storage) => continue,
                    Some(layer) => format!(
                        "only l40_api and l30_storage crates use stores by their users lists, \
                         and its layer is {layer}"
                    ),
                },
            };
            findings.push(finding(
                store,
                user_lines.get(user.as_str()).copied().or(store_line),
                format!(
                    ": lists {user} in [rules.{RULE}] store_users in {CONFIG_FILE}, but {reason}, \
                     so the entry allows nothing. Remove it from the list."
                ),
            ));
        }
    }
    for (from, allowed) in &config.allowed_edges {
        let (from_line, to_lines) = lines_of(&policy.allowed_edges, from);
        for to in allowed.iter() {
            let key = (from.clone(), to.clone());
            let reason = match (graph.member(from), graph.member(to)) {
                (None, _) => format!("no workspace member is named {from}"),
                (_, None) => format!("no workspace member is named {to}"),
                (Some(from), Some(to)) if from.layer.is_none() || to.layer.is_none() => continue,
                _ if !verdicts.production.contains(&key) => {
                    format!("{from} has no normal or build dependency on {to}")
                }
                _ if verdicts.has(Problem::TestOnly, &key) => format!(
                    "allowed_edges does not exempt an edge from {RULE}.test_only, which reports \
                     it"
                ),
                _ if !verdicts.used_exceptions.contains(&key) => {
                    "the layer order already allows that edge".to_owned()
                }
                _ => continue,
            };
            findings.push(finding(
                from,
                to_lines.get(to.as_str()).copied().or(from_line),
                format!(
                    ": [rules.{RULE}] allowed_edges in {CONFIG_FILE} lets it depend on {to}, but \
                     {reason}. Remove {to} from the list."
                ),
            ));
        }
    }
    findings
}

/// The line of `key`'s entry in `entries`, and the line of each item it
/// lists.
fn lines_of<'a>(
    entries: &'a Entries<BTreeMap<String, usize>>,
    key: &str,
) -> (Option<usize>, &'a BTreeMap<String, usize>) {
    static NONE: BTreeMap<String, usize> = BTreeMap::new();
    match entries.entries.get(key) {
        Some((items, line)) => (Some(*line), items),
        None => (entries.line, &NONE),
    }
}

/// The advisory `policy_changed` finding for a `[rules.layers]` that the
/// base branch's copy has and this branch's lacks, when no layers check runs.
pub(super) fn rule_removed(ctx: &Context) -> Vec<Finding> {
    let BaseArchitecture::Loaded(base) = &ctx.base_architecture else {
        return Vec::new();
    };
    let Ok(Some(_)) = &base.layers else {
        return Vec::new();
    };
    vec![Finding {
        rule: RULE,
        instance: None,
        behaviour: "policy_changed",
        path: CONFIG_FILE.to_owned(),
        detail: format!(
            ": the base branch's copy has [rules.{RULE}] and this one doesn't: the whole layers \
             rule was removed and no longer runs."
        ),
        mode: Mode::Advisory,
        location: Location {
            path: CONFIG_FILE.to_owned(),
            line: 1,
        },
    }]
}

/// Every difference of `[rules.layers]` from the base branch's copy. A
/// removed `unmoved` entry is one only when its crate moved into the
/// directory of another layer than the entry's; otherwise the crate either
/// moved where the entry said, was deleted, or is a `placement` finding.
pub(super) fn policy_changed(
    ctx: &Context,
    config: &LayersConfig,
    graph: &Graph<'_>,
) -> Vec<Finding> {
    let BaseArchitecture::Loaded(base) = &ctx.base_architecture else {
        return Vec::new();
    };
    let Ok(base) = &base.layers else {
        return Vec::new();
    };
    let mut changes = changes(base.as_ref(), &ctx.layers_policy);
    if let Some(base) = base {
        changes.extend(reclassified(base, &ctx.layers_policy, graph));
    }
    changes
        .into_iter()
        .map(|(line, detail)| {
            config_finding(
                "policy_changed",
                &config.policy_changed,
                CONFIG_FILE,
                line,
                detail,
            )
        })
        .collect()
}

/// Each `unmoved` entry of `base` that `policy` lacks because its crate
/// moved into the directory of another layer, which changes the crate's
/// layer as editing the entry would.
fn reclassified(
    base: &LayersPolicy,
    policy: &LayersPolicy,
    graph: &Graph<'_>,
) -> Vec<(usize, String)> {
    let line = policy.unmoved.line.unwrap_or(policy.line);
    base.unmoved
        .entries
        .iter()
        .filter(|(package, _)| !policy.unmoved.entries.contains_key(*package))
        .filter_map(|(package, (old, _))| {
            let member = graph.member(package)?;
            member.layer_path?;
            let layer = member.layer?;
            (layer.dir() != old).then(|| {
                (
                    line,
                    format!(
                        ": `{package}` moved from {old} ([rules.{RULE}] unmoved) to {layer}, at \
                         {}.",
                        member.krate.member
                    ),
                )
            })
        })
        .collect()
}

/// Each difference of `policy` from `base` (`None`: the base has no
/// `[rules.layers]`), as the line it is shown on and the finding's detail.
fn changes(base: Option<&LayersPolicy>, policy: &LayersPolicy) -> Vec<(usize, String)> {
    let table = format!("[rules.{RULE}]");
    let Some(base) = base else {
        return vec![(
            policy.line,
            format!(": {table} is not in the base branch's copy; the whole policy is new."),
        )];
    };
    let mut changes = Vec::new();
    for (package, (layer, line)) in &policy.unmoved.entries {
        match base.unmoved.entries.get(package) {
            None => changes.push((
                *line,
                format!(": `{package}` was added to {table} unmoved, in {layer}."),
            )),
            Some((old, _)) if old != layer => changes.push((
                *line,
                format!(": `{package}` moved from {old} to {layer} in {table} unmoved."),
            )),
            Some(_) => {}
        }
    }
    for (key, what) in [("store_users", "users"), ("allowed_edges", "allowed edges")] {
        let (old, new) = match key {
            "store_users" => (&base.store_users, &policy.store_users),
            _ => (&base.allowed_edges, &policy.allowed_edges),
        };
        let packages: BTreeSet<&String> = old.entries.keys().chain(new.entries.keys()).collect();
        for package in packages {
            let old_items = old.entries.get(package).map(|(items, _)| items);
            let new_entry = new.entries.get(package);
            let entry_line = new_entry.map_or(new.line.unwrap_or(policy.line), |(_, line)| *line);
            for (item, line) in new_entry.map(|(items, _)| items).into_iter().flatten() {
                if !old_items.is_some_and(|old| old.contains_key(item)) {
                    changes.push((
                        *line,
                        format!(
                            ": `{item}` was added to the {what} of `{package}` in {table} {key}."
                        ),
                    ));
                }
            }
            for item in old_items.into_iter().flat_map(BTreeMap::keys) {
                if !new_entry.is_some_and(|(items, _)| items.contains_key(item)) {
                    changes.push((
                        entry_line,
                        format!(
                            ": `{item}` was removed from the {what} of `{package}` in {table} \
                             {key}."
                        ),
                    ));
                }
            }
        }
    }
    let behaviours: BTreeSet<&String> = base.modes.keys().chain(policy.modes.keys()).collect();
    for behaviour in behaviours {
        let old = base.modes.get(behaviour).map(|(mode, _)| mode.as_str());
        let new = policy.modes.get(behaviour);
        if old != new.map(|(mode, _)| mode.as_str()) {
            changes.push((
                new.map_or(policy.line, |(_, line)| *line),
                format!(
                    ": {RULE}.{behaviour} mode changed from {} to {}.",
                    old.unwrap_or("unset"),
                    new.map_or("unset", |(mode, _)| mode.as_str())
                ),
            ));
        }
    }
    changes
}
