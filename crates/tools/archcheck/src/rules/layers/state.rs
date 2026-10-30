//! The state checks: which store owns each table of each database.
//!
//! A store declares the tables it owns in its `[package.metadata.space.state]`,
//! by database namespace: `postgres` for `db/postgres`, `sqlite.<database>`
//! for `db/<database>`. A shared catalog, such as `db/common`, is applied to
//! the databases `[rules.layers.state.shared_catalogs]` lists, before their
//! own. `[rules.layers.state.co_writers]` grants another store a table's
//! writes, in `ARCHITECTURE.toml`, where `layers.policy_changed` sees it.
//!
//! - `state.declarations`: every store declares its databases, only stores
//!   declare state, and each namespace is a database.
//! - `state.owners`: no table has two owners.
//! - `state.writes`: only a table's owner and its co-writers among the
//!   stores have a write statement naming it.
//! - `state.pending` (advisory): a declared table no migration creates yet.
//! - `unowned`: every live table is declared, or listed in the
//!   `[layers.unowned]` baseline table.
//! - `stale_entries`, of the layers rule: a co-writer grant that allows
//!   nothing.

use std::collections::BTreeMap;

use super::{
    BaseEdges, GUIDE, RULE, UNOWNED, base_edges, config_finding,
    graph::{Graph, Member},
    list,
    sql::{self, Created},
};
use crate::{
    baseline::{BASELINE_FILE, EdgeTable},
    config::{CONFIG_FILE, EdgeBehaviours, Layer, LayersConfig, Mode, StateConfig},
    context::{CATALOGS_DIR, Context, Dialect, POSTGRES, TableName, declared_table, namespace},
    finding::{Finding, Location},
    rules::NotAdded,
};

/// A `(namespace, table)` pair.
type Key = (String, String);

/// The crates that declare a table, each with its declaration's line.
type Declarers<'a> = BTreeMap<Key, Vec<(&'a Member<'a>, usize)>>;

/// The databases' live tables, every crate's declarations, and the
/// co-writer grants.
pub(super) struct State<'a> {
    /// Namespace to each table its catalogs leave, and where it is created.
    live: BTreeMap<String, BTreeMap<String, Created>>,
    owners: Declarers<'a>,
    /// Each granted table to the packages `[rules.layers.state.co_writers]`
    /// lets write it.
    co_writers: BTreeMap<Key, Vec<String>>,
}

impl<'a> State<'a> {
    pub fn new(ctx: &Context, config: &StateConfig, graph: &'a Graph<'a>) -> Self {
        let mut live = BTreeMap::new();
        for catalog in &ctx.catalogs {
            if config.shared_catalogs.contains_key(&catalog.name) {
                continue;
            }
            let namespace = namespace(&catalog.name);
            let dialect = Dialect::of(&namespace);
            let mut tables = BTreeMap::new();
            for (shared, databases) in &config.shared_catalogs {
                if !databases.contains(&catalog.name) {
                    continue;
                }
                if let Some(shared) = ctx.catalogs.iter().find(|other| other.name == *shared) {
                    sql::replay(&shared.migrations, dialect, &mut tables);
                }
            }
            sql::replay(&catalog.migrations, dialect, &mut tables);
            live.insert(namespace, tables);
        }
        let mut owners: Declarers<'a> = BTreeMap::new();
        for member in graph.members.values() {
            let Some(declarations) = &member.krate.state else {
                continue;
            };
            for (namespace, tables) in &declarations.owns {
                for (table, line) in tables {
                    owners
                        .entry((namespace.clone(), table.clone()))
                        .or_default()
                        .push((member, *line));
                }
            }
        }
        let mut co_writers: BTreeMap<Key, Vec<String>> = BTreeMap::new();
        for (namespace, table, stores) in config.co_writers.grants() {
            let table = declared_table(Dialect::of(&namespace), table);
            co_writers
                .entry((namespace, table))
                .or_default()
                .extend(stores.iter().cloned());
        }
        Self {
            live,
            owners,
            co_writers,
        }
    }

    fn is_database(&self, namespace: &str) -> bool {
        self.live.contains_key(namespace)
    }

    fn is_live(&self, key: &Key) -> bool {
        self.live
            .get(&key.0)
            .is_some_and(|tables| tables.contains_key(&key.1))
    }

    /// Whether `key` is a table of its database: live, or declared ahead of
    /// its migration.
    fn has(&self, key: &Key) -> bool {
        self.is_live(key) || self.owners.contains_key(key)
    }

    /// Whether `package` may write `key`: no store owns it, or `package`
    /// owns it or is granted it.
    fn may_write(&self, key: &Key, package: &str) -> bool {
        let Some(owners) = self.owners.get(key) else {
            return true;
        };
        owners.iter().any(|(member, _)| member.name() == package)
            || self
                .co_writers
                .get(key)
                .is_some_and(|stores| stores.iter().any(|store| store == package))
    }

    /// The package names that own `key`.
    fn owner_names(&self, key: &Key) -> Vec<String> {
        self.owners
            .get(key)
            .map(|owners| {
                owners
                    .iter()
                    .map(|(member, _)| member.name().to_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every live table without an owner, sorted.
    fn ownerless(&self) -> Vec<(Key, &Created)> {
        self.live
            .iter()
            .flat_map(|(namespace, tables)| {
                tables
                    .iter()
                    .map(move |(table, created)| ((namespace.clone(), table.clone()), created))
            })
            .filter(|(key, _)| !self.owners.contains_key(key))
            .collect()
    }

    /// The namespaces, for a sentence.
    fn namespaces(&self) -> String {
        let names: Vec<String> = self.live.keys().cloned().collect();
        list(&names, names.len())
    }
}

/// A write statement in a store's production code.
struct StoreWrite<'a> {
    member: &'a Member<'a>,
    path: &'a str,
    line: usize,
    head: String,
    /// The table it names, as each database kind that reads the head
    /// there reads the name.
    tables: Vec<(Dialect, TableName)>,
}

impl StoreWrite<'_> {
    /// The table the statement names, keyed for a database of `namespace`;
    /// `None` if such a database reads no write there.
    fn table_in(&self, namespace: &str) -> Option<String> {
        let dialect = Dialect::of(namespace);
        self.tables
            .iter()
            .find(|(read_as, _)| *read_as == dialect)
            .map(|(_, table)| table.key(dialect))
    }
}

/// Every write head in each store's production Rust files and the `.sql`
/// files under its `src/`, whether or not the store declares a database.
/// Each file is read as each database kind reads SQL, since they quote and
/// bracket text differently; a head both read is one write.
fn store_writes<'a>(ctx: &'a Context, graph: &'a Graph<'a>) -> Vec<StoreWrite<'a>> {
    let mut writes = Vec::new();
    for member in graph.members.values() {
        if member.layer != Some(Layer::Storage) {
            continue;
        }
        let prefix = if member.krate.member.is_empty() {
            "src/".to_owned()
        } else {
            format!("{}/src/", member.krate.member)
        };
        let rust = ctx
            .files
            .iter()
            .filter(|file| file.path.starts_with(&prefix))
            .map(|file| (file.path.as_str(), file.text.as_str(), true));
        let sql_files = ctx
            .sql_files
            .iter()
            .filter(|file| file.path.starts_with(&prefix))
            .map(|file| (file.path.as_str(), file.text.as_str(), false));
        for (path, text, is_rust) in rust.chain(sql_files) {
            // By the head's offset, then its place among the tables a
            // `TRUNCATE` lists.
            let mut found: BTreeMap<(usize, usize), StoreWrite<'a>> = BTreeMap::new();
            for dialect in [Dialect::Postgres, Dialect::Sqlite] {
                let read = if is_rust {
                    sql::rust_writes(text, dialect)
                } else {
                    sql::sql_writes(text, dialect)
                };
                let mut previous = None;
                let mut place = 0;
                for (write, line) in read {
                    place = if previous == Some(write.at) {
                        place + 1
                    } else {
                        0
                    };
                    previous = Some(write.at);
                    found
                        .entry((write.at, place))
                        .or_insert_with(|| StoreWrite {
                            member,
                            path,
                            line,
                            head: write.head,
                            tables: Vec::new(),
                        })
                        .tables
                        .push((dialect, write.table));
                }
            }
            writes.extend(found.into_values());
        }
    }
    writes
}

/// The state checks' findings, after the edge checks'.
pub(super) fn check(
    ctx: &Context,
    layers: &LayersConfig,
    config: &StateConfig,
    unowned: &EdgeBehaviours,
    graph: &Graph<'_>,
) -> Vec<Finding> {
    let state = State::new(ctx, config, graph);
    let writes = store_writes(ctx, graph);
    let mut findings = declarations(config.declarations.mode, graph, &state);
    findings.extend(owners(config.owners.mode, &state));
    findings.extend(store_write_findings(config.writes.mode, &state, &writes));
    findings.extend(pending(config.pending.mode, &state));
    findings.extend(unowned_findings(ctx, unowned, &state));
    findings.extend(stale_grants(ctx, layers, config, graph, &state, &writes));
    findings
}

/// A finding on a line of a member's manifest.
fn manifest_finding(
    behaviour: &'static str,
    mode: Mode,
    member: &Member<'_>,
    path: Option<String>,
    line: usize,
    detail: String,
) -> Finding {
    let manifest = member.krate.manifest();
    Finding {
        rule: RULE,
        instance: Some("state".to_owned()),
        behaviour,
        path: path.unwrap_or_else(|| manifest.clone()),
        detail: format!("{detail} See {GUIDE}."),
        mode,
        location: Location {
            path: manifest,
            line,
        },
    }
}

/// `namespace.table`, as findings name a table.
fn qualified((namespace, table): &Key) -> String {
    format!("{namespace}.{table}")
}

/// `db/<catalog>`, the catalog directory of `namespace`.
fn catalog_dir(namespace: &str) -> String {
    let catalog = namespace.strip_prefix("sqlite.").unwrap_or(namespace);
    format!("{CATALOGS_DIR}/{catalog}")
}

fn declarations(mode: Mode, graph: &Graph<'_>, state: &State<'_>) -> Vec<Finding> {
    let finding = |member: &Member<'_>, line: usize, detail: String| {
        manifest_finding("declarations", mode, member, None, line, detail)
    };
    let mut findings = Vec::new();
    for member in graph.members.values() {
        let is_store = member.layer == Some(Layer::Storage);
        let Some(declarations) = &member.krate.state else {
            if is_store {
                findings.push(finding(
                    member,
                    member.krate.name_line,
                    format!(
                        ": {} is a store (l30_storage), but its Cargo.toml has no \
                         [package.metadata.space.state]. Add one naming each database the store \
                         reaches and the tables it owns there, as `{POSTGRES} = [\"<table>\"]` \
                         or `sqlite.<database> = [\"<table>\"]`: an empty list for a database \
                         whose tables it doesn't own, or an empty table for a store without a \
                         database.",
                        member.name()
                    ),
                ));
            }
            continue;
        };
        if !is_store {
            let layer = member
                .layer
                .map_or("no layer".to_owned(), |layer| layer.to_string());
            findings.push(finding(
                member,
                declarations.line,
                format!(
                    ": {} ({layer}) has a [package.metadata.space.state], but only l30_storage \
                     crates own tables. Move the declaration, with the SQL that \
                     writes those tables, into the store that owns them, or remove it.",
                    member.name()
                ),
            ));
            continue;
        }
        for namespace in declarations.namespaces() {
            if state.is_database(namespace) {
                continue;
            }
            findings.push(finding(
                member,
                declarations
                    .namespace_lines
                    .get(namespace)
                    .copied()
                    .unwrap_or(declarations.line),
                format!(
                    ": {} declares `{namespace}`, which is no database. The namespaces are {}: \
                     {POSTGRES} for {CATALOGS_DIR}/{POSTGRES}, and sqlite.<database> for \
                     {CATALOGS_DIR}/<database>; a shared catalog's tables are declared in each \
                     database it is applied to. Fix or remove the key.",
                    member.name(),
                    state.namespaces()
                ),
            ));
        }
    }
    findings
}

/// Tables two or more crates declare, one finding per declaration.
fn owners(mode: Mode, state: &State<'_>) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (key, owners) in &state.owners {
        if owners.len() < 2 {
            continue;
        }
        for (member, line) in owners {
            let others: Vec<String> = owners
                .iter()
                .map(|(other, _)| other.name().to_owned())
                .filter(|other| other != member.name())
                .collect();
            findings.push(manifest_finding(
                "owners",
                mode,
                member,
                None,
                *line,
                format!(
                    ": {} declares {}, which {} also declare{}, but a table has one owner. Keep \
                     the declaration in the store whose state the table is. In the others, \
                     remove it and move their writes into the owner, or, for an approved \
                     co-writer, grant it the table in [rules.{RULE}.state.co_writers.{}] in \
                     {CONFIG_FILE} instead.",
                    member.name(),
                    qualified(key),
                    list(&others, 5),
                    if others.len() == 1 { "s" } else { "" },
                    key.0,
                ),
            ));
        }
    }
    findings
}

/// Writes by a store that may not write the table they name. The table is
/// looked up in the databases the store declares; one it may write in any
/// of them is allowed, since the scan can't tell which database a statement
/// runs against. A table in none of them is checked against its owners in
/// every database, so a store can't write another's table by leaving its
/// database undeclared.
fn store_write_findings(mode: Mode, state: &State<'_>, writes: &[StoreWrite<'_>]) -> Vec<Finding> {
    let mut findings = Vec::new();
    for write in writes {
        let name = write.member.name();
        let declared: Vec<Key> = write
            .member
            .krate
            .state
            .iter()
            .flat_map(|declarations| declarations.namespaces())
            .filter_map(|namespace| Some((namespace.clone(), write.table_in(namespace)?)))
            .filter(|key| state.has(key))
            .collect();
        let undeclared = declared.is_empty();
        let candidates: Vec<Key> = if undeclared {
            state
                .owners
                .keys()
                .filter(|(namespace, table)| write.table_in(namespace).as_ref() == Some(table))
                .cloned()
                .collect()
        } else {
            declared
        };
        if candidates.iter().any(|key| state.may_write(key, name)) {
            continue;
        }
        let Some(key) = candidates.first() else {
            continue;
        };
        let owners = list(&state.owner_names(key), 5);
        let whose = if undeclared {
            format!(
                "a table {owners} owns in a database {name} doesn't declare, and {name} is not \
                 its co-writer"
            )
        } else {
            format!("but {owners} owns that table, and {name} is neither its owner nor a co-writer")
        };
        findings.push(Finding {
            rule: RULE,
            instance: Some("state".to_owned()),
            behaviour: "writes",
            path: write.path.to_owned(),
            detail: format!(
                ": {name} writes {} here ({} {}), {whose}. Move the statement into {owners} and \
                 call it from there; or, for an approved co-writer, add \"{name}\" to {} in \
                 [rules.{RULE}.state.co_writers.{}] in {CONFIG_FILE}, a policy change that \
                 {RULE}.policy_changed reports for review. See {GUIDE}.",
                qualified(key),
                write.head,
                key.1,
                key.1,
                key.0,
            ),
            mode,
            location: Location {
                path: write.path.to_owned(),
                line: write.line,
            },
        });
    }
    findings
}

/// `layers.stale_entries` for each `[rules.layers.state.co_writers]` grant
/// that allows nothing: in no database, to a package that is not a store,
/// for a table no store or the package itself owns, or that no write
/// statement of the package names.
fn stale_grants(
    ctx: &Context,
    layers: &LayersConfig,
    config: &StateConfig,
    graph: &Graph<'_>,
    state: &State<'_>,
    writes: &[StoreWrite<'_>],
) -> Vec<Finding> {
    let policy = &ctx.layers_policy;
    let mut findings = Vec::new();
    for (namespace, table, stores) in config.co_writers.grants() {
        let table = declared_table(Dialect::of(&namespace), table);
        let key = (namespace, table);
        let entry = policy.co_writers.entries.get(&qualified(&key));
        let entry_line = entry
            .map(|(_, line)| *line)
            .or(policy.co_writers.line)
            .unwrap_or(policy.line);
        for store in stores.iter() {
            let reason = if !state.is_database(&key.0) {
                format!(
                    "{} is no database; the namespaces are {}",
                    key.0,
                    state.namespaces()
                )
            } else {
                match graph.member(store) {
                    None => "no workspace member has this name".to_owned(),
                    Some(member) if member.layer != Some(Layer::Storage) => {
                        let layer = member
                            .layer
                            .map_or("no layer".to_owned(), |layer| layer.to_string());
                        format!("it is not a store: its layer is {layer}, not l30_storage")
                    }
                    Some(_) if !state.owners.contains_key(&key) => {
                        format!("no store owns {}", qualified(&key))
                    }
                    Some(_) if state.owner_names(&key).contains(store) => {
                        format!("{store} owns that table")
                    }
                    Some(member)
                        if !writes.iter().any(|write| {
                            write.member.name() == member.name()
                                && write.table_in(&key.0).as_ref() == Some(&key.1)
                        }) =>
                    {
                        format!(
                            "no write statement in its production code names {}, so it needs \
                             no grant",
                            key.1
                        )
                    }
                    Some(_) => continue,
                }
            };
            let line = entry
                .and_then(|(items, _)| items.get(store.as_str()).copied())
                .unwrap_or(entry_line);
            findings.push(config_finding(
                "stale_entries",
                &layers.stale_entries,
                &qualified(&key),
                line,
                format!(
                    ": lists {store} in [rules.{RULE}.state.co_writers.{}] in {CONFIG_FILE}, but \
                     {reason}, so the grant allows nothing. Remove it from the list.",
                    key.0
                ),
            ));
        }
    }
    findings
}

/// Declared tables that no migration creates yet, in a database that
/// exists.
fn pending(mode: Mode, state: &State<'_>) -> Vec<Finding> {
    let mut findings = Vec::new();
    for (key, owners) in &state.owners {
        if !state.is_database(&key.0) || state.is_live(key) {
            continue;
        }
        for (member, line) in owners {
            findings.push(manifest_finding(
                "pending",
                mode,
                member,
                Some(qualified(key)),
                *line,
                format!(
                    ": {} declares it in [package.metadata.space.state], but no migration in {} \
                     creates it yet. A new table lands that way: the declaration first, then \
                     the migration in a pull request of its own. If the migration has landed, \
                     check the table's name.",
                    member.name(),
                    catalog_dir(&key.0),
                ),
            ));
        }
    }
    findings
}

/// `unowned`'s `no_crossing`, `no_new_entries` and `stale_entries`.
fn unowned_findings(ctx: &Context, behaviours: &EdgeBehaviours, state: &State<'_>) -> Vec<Finding> {
    static EMPTY: EdgeTable = EdgeTable::new();
    let table = ctx.baseline.layers.get(UNOWNED).unwrap_or(&EMPTY);
    let listed = format!("[{RULE}.{UNOWNED}] in {BASELINE_FILE}");
    let finding = |behaviour: &'static str, mode: Mode, key: &Key, at: Location, detail| Finding {
        rule: RULE,
        instance: Some(UNOWNED.to_owned()),
        behaviour,
        path: qualified(key),
        detail,
        mode,
        location: at,
    };
    let entry_at = |key: &Key| Location {
        path: BASELINE_FILE.to_owned(),
        line: table.line(&key.0, &key.1).unwrap_or(1),
    };
    let mut findings = Vec::new();
    for (key, created) in state.ownerless() {
        if table.contains(&key.0, &key.1) {
            continue;
        }
        findings.push(finding(
            "no_crossing",
            behaviours.no_crossing.mode,
            &key,
            Location {
                path: created.path.clone(),
                line: created.line,
            },
            format!(
                ": is created by {}, but no store declares it in [package.metadata.space.state], \
                 and {listed} doesn't list it. Declare it in the l30_storage crate that owns it, \
                 as `{} = [\"{}\"]` in that store's state table. See {GUIDE}.",
                created.path, key.0, key.1
            ),
        ));
    }
    let base = base_edges(ctx, UNOWNED);
    for (namespace, name) in table.iter() {
        let key = (namespace.to_owned(), name.to_owned());
        match base {
            BaseEdges::Compare(base)
                if !base.get(namespace).is_some_and(|base| base.contains(name)) =>
            {
                findings.push(finding(
                    "no_new_entries",
                    behaviours.no_new_entries.mode,
                    &key,
                    entry_at(&key),
                    format!(
                        ": is in {listed} but not in the base branch's copy, and a table can't \
                         be added to it. Remove the entry and declare the table in the store \
                         that owns it; without the entry, {RULE}.{UNOWNED}.no_crossing says how."
                    ),
                ));
            }
            BaseEdges::Seeding => findings.push(finding(
                "no_new_entries",
                Mode::Advisory,
                &key,
                entry_at(&key),
                format!(
                    ": is in {listed}, and the base branch has no state checks yet, so this pull \
                     request seeds the table. Review each entry."
                ),
            )),
            BaseEdges::Compare(_) | BaseEdges::Skipped => {}
        }
    }
    for (namespace, name) in table.iter() {
        let key = (namespace.to_owned(), name.to_owned());
        let reason = if !state.is_database(namespace) {
            format!(
                "{namespace} is no database; the namespaces are {}",
                state.namespaces()
            )
        } else if !state.is_live(&key) {
            format!(
                "no migration in {} leaves a table {name}",
                catalog_dir(namespace)
            )
        } else if state.owners.contains_key(&key) {
            format!("{} declares it now", list(&state.owner_names(&key), 5))
        } else {
            continue;
        };
        findings.push(finding(
            "stale_entries",
            behaviours.stale_entries.mode,
            &key,
            entry_at(&key),
            format!(
                ": is in {listed}, but {reason}. Remove the entry (cargo arch \
                 --write-baseline={RULE}.{UNOWNED})."
            ),
        ));
    }
    findings
}

/// `[layers.unowned]` for `--write-baseline`: each entry whose table is
/// still live without an owner, and none added; each such table without an
/// entry is listed instead.
pub(super) fn update_unowned(
    ctx: &Context,
    config: &StateConfig,
    graph: &Graph<'_>,
) -> (EdgeTable, Vec<NotAdded>) {
    let state = State::new(ctx, config, graph);
    let existing = ctx.baseline.layers.get(UNOWNED);
    let mut table = EdgeTable::default();
    let mut not_added = Vec::new();
    for (key, created) in state.ownerless() {
        if existing.is_some_and(|existing| existing.contains(&key.0, &key.1)) {
            table.insert(&key.0, &key.1);
            continue;
        }
        not_added.push(NotAdded {
            table: format!("{RULE}.{UNOWNED}"),
            entry: None,
            line: format!(
                "not added: {} (created by {}) in [{RULE}.{UNOWNED}], which only shrinks \
                 ({RULE}.{UNOWNED}.no_new_entries); declare it in the store that owns it, \
                 before its migration lands ({RULE}.state.pending), as cargo arch says",
                qualified(&key),
                created.path,
            ),
        });
    }
    (table, not_added)
}
