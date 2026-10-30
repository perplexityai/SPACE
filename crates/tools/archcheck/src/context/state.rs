//! What the layers rule's state checks read besides Rust files: each store's
//! `[package.metadata.space.state]`, the migration catalogs under `db/`, and
//! the `.sql` files under workspace members' `src/`.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use toml::Spanned;

use super::{Crate, LoadError, is_test_directory, read, relative_path};
use crate::{baseline::line_of, config::StateConfig};

/// Where the migration catalogs are: one directory per catalog.
pub const CATALOGS_DIR: &str = "db";

/// The header a crate declares its state under.
const STATE_HEADER: &str = "[package.metadata.space.state]";

/// A file read as text, with its repository-relative, `/`-separated path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextFile {
    pub path: String,
    pub text: String,
}

/// One directory under `db/`: a database's catalog, or a shared catalog
/// applied to several databases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    /// The directory's name, such as `postgres` or `sandbox-manager`.
    pub name: String,
    /// Its top-level `*.up.sql` files, in version order; `history/` and
    /// `*.down.sql` are not among them.
    pub migrations: Vec<TextFile>,
}

/// A crate's `[package.metadata.space.state]`: the tables it owns, by
/// database namespace (`postgres`, or `sqlite.<database>`). A namespace with
/// an empty list puts the crate in that database without owning a table
/// there.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateDeclarations {
    /// The 1-based line of the table's header, or of its first key.
    pub line: usize,
    /// Namespace to each table the crate owns there, named as
    /// [`declared_table`] keys it, with its line.
    pub owns: BTreeMap<String, BTreeMap<String, usize>>,
    /// The line of each namespace's key.
    pub namespace_lines: BTreeMap<String, usize>,
}

impl StateDeclarations {
    /// Every namespace the crate declares, sorted.
    pub fn namespaces(&self) -> impl Iterator<Item = &String> {
        self.owns.keys()
    }
}

/// `[package.metadata]` down to the state table; other keys are ignored.
#[derive(Deserialize)]
pub(super) struct PackageMetadata {
    space: Option<SpaceMetadata>,
}

#[derive(Deserialize)]
struct SpaceMetadata {
    state: Option<StateTable>,
}

/// A list of tables in one namespace.
type Tables = Spanned<Vec<Spanned<String>>>;

/// The state table as written: unknown keys are an error, so a misspelled
/// namespace cannot take a store out of the checks. Co-writers are granted
/// in `ARCHITECTURE.toml`, not here.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateTable {
    postgres: Option<Tables>,
    sqlite: Option<BTreeMap<Spanned<String>, Tables>>,
}

impl PackageMetadata {
    /// The crate's declarations, with lines in `manifest`, its text at
    /// `path`; `None` without a state table. Two names of one table in a
    /// namespace are an error, so neither hides the other.
    pub(super) fn state(
        self,
        path: &Path,
        manifest: &str,
    ) -> Result<Option<StateDeclarations>, LoadError> {
        let Some(state) = self.space.and_then(|space| space.state) else {
            return Ok(None);
        };
        let mut declarations = StateDeclarations::default();
        for (namespace, (line, tables)) in namespaces(path, manifest, state.postgres, state.sqlite)?
        {
            declarations.namespace_lines.insert(namespace.clone(), line);
            declarations.owns.insert(namespace, tables);
        }
        declarations.line = manifest
            .lines()
            .position(|line| line.trim_start().starts_with(STATE_HEADER))
            .map(|index| index + 1)
            .or_else(|| declarations.namespace_lines.values().min().copied())
            .unwrap_or(1);
        Ok(Some(declarations))
    }
}

/// The tables a crate owns in one namespace, each with its line.
type Owned = BTreeMap<String, usize>;

/// Each namespace's key line and tables, each table with its line.
fn namespaces(
    path: &Path,
    manifest: &str,
    postgres: Option<Tables>,
    sqlite: Option<BTreeMap<Spanned<String>, Tables>>,
) -> Result<BTreeMap<String, (usize, Owned)>, LoadError> {
    let line = |span: std::ops::Range<usize>| line_of(manifest, span.start);
    let tables = |namespace: &str, list: Tables| {
        let at = line(list.span());
        let dialect = Dialect::of(namespace);
        let mut tables = BTreeMap::new();
        let mut written: BTreeMap<String, String> = BTreeMap::new();
        for table in list.into_inner() {
            let key = declared_table(dialect, table.get_ref());
            if let Some(first) = written.get(&key) {
                return Err(LoadError::DuplicateDeclaration {
                    path: path.to_path_buf(),
                    namespace: namespace.to_owned(),
                    table: key,
                    first: first.clone(),
                    second: table.into_inner(),
                });
            }
            tables.insert(key.clone(), line(table.span()));
            written.insert(key, table.into_inner());
        }
        Ok((at, tables))
    };
    let mut namespaces = BTreeMap::new();
    if let Some(list) = postgres {
        namespaces.insert(POSTGRES.to_owned(), tables(POSTGRES, list)?);
    }
    for (database, list) in sqlite.into_iter().flatten() {
        let key_line = line(database.span());
        let namespace = sqlite_namespace(database.get_ref());
        let (_, listed) = tables(&namespace, list)?;
        namespaces.insert(namespace, (key_line, listed));
    }
    Ok(namespaces)
}

/// How a database kind names its tables: the schema an unqualified name is
/// in, and how an identifier's case is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// Unqualified names are in `public`; an unquoted identifier folds to
    /// lower case and a quoted one is kept as written.
    Postgres,
    /// Unqualified names are in `main`, or in `temp`, which is searched
    /// first; every identifier, quoted or not, is compared without ASCII
    /// case.
    Sqlite,
}

impl Dialect {
    /// The dialect of the databases of `namespace`: `postgres`, or
    /// `sqlite.<database>`.
    pub fn of(namespace: &str) -> Self {
        if namespace == POSTGRES {
            Self::Postgres
        } else {
            Self::Sqlite
        }
    }

    /// An identifier as this kind of database compares it: `quoted` says
    /// whether it was written in quotes. Both kinds fold ASCII only.
    pub fn fold(self, name: &str, quoted: bool) -> String {
        match (self, quoted) {
            (Self::Postgres, true) => name.to_owned(),
            _ => name.to_ascii_lowercase(),
        }
    }

    /// Whether `[…]` quotes an identifier, as in SQLite. In Postgres, `[`
    /// is an array's or a subscript's, and what is inside it is read as
    /// code.
    pub fn has_bracket_identifiers(self) -> bool {
        self == Self::Sqlite
    }

    /// Whether `schema`, folded, is one an unqualified name is in.
    fn is_default_schema(self, schema: &str) -> bool {
        match self {
            Self::Postgres => schema == "public",
            Self::Sqlite => schema == "main" || schema == "temp",
        }
    }
}

/// A table as the state checks key it, in declarations, live tables and
/// write statements alike, from its schema and table, each folded as
/// [`Dialect::fold`] says: `<table>` in a default schema (Postgres's
/// `public`, SQLite's `main` or `temp`) or without one, and
/// `<schema>.<table>` in any other.
pub fn table_key(dialect: Dialect, schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) if !dialect.is_default_schema(schema) => format!("{schema}.{table}"),
        _ => table.to_owned(),
    }
}

/// The key of a table named in a declaration or a co-writer grant: the name
/// the database stores, so case matters in Postgres and not in SQLite. Its
/// parts are read as SQL reads a name, as [`TableName::parse`] says, so
/// `audit."events.v1"` is the table `events.v1` in the schema `audit`; but
/// an unquoted part keeps its case, as written. Of
/// `<catalog>.<schema>.<table>`, the last two parts are read. A name SQL
/// doesn't read whole, such as `a-b`, is split at each `.`.
pub fn declared_table(dialect: Dialect, name: &str) -> String {
    let fold = |part: &str| dialect.fold(part, true);
    if let Some((parsed, len)) = TableName::parse(name, dialect)
        && len == name.len()
    {
        let schema = parsed.schema.map(|schema| fold(&schema.text));
        return table_key(dialect, schema.as_deref(), &fold(&parsed.table.text));
    }
    let name = fold(name);
    let mut parts = name.rsplit('.');
    let table = parts.next().unwrap_or_default();
    table_key(dialect, parts.next(), table)
}

/// An identifier as written, without its quotes, and whether it had them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub text: String,
    pub quoted: bool,
}

/// A table's name as written: its schema, if it is qualified, and the
/// table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableName {
    pub schema: Option<Identifier>,
    pub table: Identifier,
}

impl TableName {
    /// The name of the parts of a dotted name: the last two, so a Postgres
    /// `<database>.<schema>.<table>` is read as its schema and table.
    pub fn of(mut parts: Vec<Identifier>) -> Option<Self> {
        let table = parts.pop()?;
        Some(Self {
            schema: parts.pop(),
            table,
        })
    }

    /// The name `text` starts with, as a database of `dialect` reads it:
    /// identifiers, each unquoted or quoted as [`quoted_identifier`] says,
    /// joined by `.`s, with any whitespace around them, and read as
    /// [`TableName::of`] says; and its length. A `.` inside quotes is part
    /// of its identifier.
    pub fn parse(text: &str, dialect: Dialect) -> Option<(Self, usize)> {
        let gap = |text: &str| text.len() - text.trim_start().len();
        let (first, mut len) = identifier(text, dialect)?;
        let mut parts = vec![first];
        loop {
            let after = text.get(len..)?;
            let Some(dotted) = after.trim_start().strip_prefix('.') else {
                break;
            };
            let (part, part_len) = identifier(dotted.trim_start(), dialect)?;
            len += gap(after) + 1 + gap(dotted) + part_len;
            parts.push(part);
        }
        Some((Self::of(parts)?, len))
    }

    /// The name's key in a database of `dialect`: each part folded as
    /// [`Dialect::fold`] says, then keyed as [`table_key`] says.
    pub fn key(&self, dialect: Dialect) -> String {
        let fold = |part: &Identifier| dialect.fold(&part.text, part.quoted);
        let schema = self.schema.as_ref().map(fold);
        table_key(dialect, schema.as_deref(), &fold(&self.table))
    }
}

/// The quoted identifier `text` starts with in a database of `dialect`,
/// without its quotes, and its length: `"…"` or `` `…` ``, each with its
/// quote doubled inside, or, in SQLite, `[…]`, up to the first `]`. `None`
/// if it is not closed.
pub fn quoted_identifier(text: &str, dialect: Dialect) -> Option<(String, usize)> {
    let mut chars = text.char_indices();
    let close = match chars.next()?.1 {
        '"' => '"',
        '`' => '`',
        '[' if dialect.has_bracket_identifiers() => ']',
        _ => return None,
    };
    let mut name = String::new();
    let mut chars = chars.peekable();
    while let Some((at, c)) = chars.next() {
        if c == close && (close == ']' || chars.next_if(|(_, c)| *c == close).is_none()) {
            return Some((name, at + c.len_utf8()));
        }
        name.push(c);
    }
    None
}

/// The identifier `text` starts with, as written, and its length.
fn identifier(text: &str, dialect: Dialect) -> Option<(Identifier, usize)> {
    let first = text.chars().next()?;
    if first.is_alphabetic() || first == '_' {
        let len = text
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(text.len());
        let text = text.get(..len)?.to_owned();
        return Some((
            Identifier {
                text,
                quoted: false,
            },
            len,
        ));
    }
    let (text, len) = quoted_identifier(text, dialect)?;
    Some((Identifier { text, quoted: true }, len))
}

/// The namespace of the control-plane database, whose catalog is
/// `db/postgres`.
pub const POSTGRES: &str = "postgres";

/// The namespace of the SQLite database whose catalog is `db/<database>`.
pub fn sqlite_namespace(database: &str) -> String {
    format!("sqlite.{database}")
}

/// The namespace of the database whose catalog is `db/<catalog>`.
pub fn namespace(catalog: &str) -> String {
    if catalog == POSTGRES {
        POSTGRES.to_owned()
    } else {
        sqlite_namespace(catalog)
    }
}

/// Every catalog under `root/db/`, sorted by name; none without `db/`. A
/// shared catalog and each database it lists must be a catalog, and a
/// shared catalog is not listed as a database.
pub(super) fn catalogs(root: &Path, state: &StateConfig) -> Result<Vec<Catalog>, LoadError> {
    let dir = root.join(CATALOGS_DIR);
    let mut catalogs = Vec::new();
    if dir.is_dir() {
        for (name, path) in entries(&dir)? {
            if !path.is_dir() {
                continue;
            }
            let mut migrations = Vec::new();
            for (file, path) in entries(&path)? {
                if path.is_file() && file.ends_with(".up.sql") {
                    migrations.push((version(&file), file, path));
                }
            }
            migrations.sort();
            let migrations = migrations
                .into_iter()
                .map(|(_, _, path)| {
                    Ok(TextFile {
                        text: read(&path)?,
                        path: relative_path(root, &path)?,
                    })
                })
                .collect::<Result<_, LoadError>>()?;
            catalogs.push(Catalog { name, migrations });
        }
    }
    catalogs.sort_by(|a, b| a.name.cmp(&b.name));
    let known = |name: &str| catalogs.iter().any(|catalog| catalog.name == name);
    for (shared, databases) in &state.shared_catalogs {
        let unknown = std::iter::once(shared)
            .chain(databases.iter())
            .find(|name| !known(name));
        if let Some(name) = unknown {
            return Err(LoadError::UnknownCatalog {
                shared: shared.clone(),
                name: name.clone(),
            });
        }
        if let Some(name) = databases
            .iter()
            .find(|name| state.shared_catalogs.contains_key(*name))
        {
            return Err(LoadError::SharedCatalogAsDatabase {
                shared: shared.clone(),
                name: name.clone(),
            });
        }
    }
    Ok(catalogs)
}

/// SQLx's version of a migration file: the number its name starts with;
/// `u128::MAX` without one, after every numbered file.
fn version(file: &str) -> u128 {
    let digits = file
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(file.len());
    file.get(..digits)
        .and_then(|digits| digits.parse().ok())
        .unwrap_or(u128::MAX)
}

/// Every `.sql` file under a member's `src/`, outside `tests/` directories,
/// sorted by path. Symlinks are skipped, as for Rust files.
pub(super) fn sql_files(root: &Path, crates: &[Crate]) -> Result<Vec<TextFile>, LoadError> {
    let mut paths = Vec::new();
    for krate in crates {
        let src = root.join(&krate.member).join("src");
        if src.is_dir() {
            collect_sql_files(&src, &mut paths)?;
        }
    }
    let mut files = paths
        .into_iter()
        .map(|path| {
            Ok(TextFile {
                text: read(&path)?,
                path: relative_path(root, &path)?,
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files.dedup_by(|a, b| a.path == b.path);
    Ok(files)
}

fn collect_sql_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), LoadError> {
    let read_error = |source| LoadError::Read {
        path: dir.to_path_buf(),
        source,
    };
    for entry in std::fs::read_dir(dir).map_err(read_error)? {
        let entry = entry.map_err(read_error)?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(read_error)?;
        if file_type.is_dir() {
            if !is_test_directory(&entry.file_name()) {
                collect_sql_files(&path, out)?;
            }
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "sql") {
            out.push(path);
        }
    }
    Ok(())
}

/// The entries of `dir` with UTF-8 names, as name and path; the others are
/// skipped.
fn entries(dir: &Path) -> Result<Vec<(String, PathBuf)>, LoadError> {
    let read_error = |source| LoadError::Read {
        path: dir.to_path_buf(),
        source,
    };
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(read_error)? {
        let entry = entry.map_err(read_error)?;
        // SQLx reads only UTF-8 names, so no catalog or migration has another.
        if let Some(name) = entry.file_name().to_str() {
            entries.push((name.to_owned(), entry.path()));
        }
    }
    Ok(entries)
}
