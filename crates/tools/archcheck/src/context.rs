//! Everything a rule may inspect, loaded once before any rule runs.

use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};

use serde::Deserialize;
use thiserror::Error;
use toml::Spanned;

mod state;

pub use state::{
    CATALOGS_DIR, Catalog, Dialect, Identifier, POSTGRES, StateDeclarations, TableName, TextFile,
    declared_table, namespace, quoted_identifier, sqlite_namespace,
};

use crate::{
    baseline::{BASELINE_FILE, BaseBaseline, Baseline, line_of, rule_of},
    config::{ArchitectureConfig, BaseArchitecture, CONFIG_FILE, CrateNamesPolicy, LayersPolicy},
    renames::{Renames, RenamesError},
    rules::{self, Layers},
};

/// The root manifest, the workspace `cargo metadata` reads.
const ROOT_MANIFEST: &str = "Cargo.toml";

/// Cargo target kinds that build a library crate, whose name code imports.
const LIB_KINDS: &[&str] = &["lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"];

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("could not read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("could not parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("could not run `cargo metadata` in {root}: {source}")]
    CargoMetadataSpawn {
        root: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("`cargo metadata` failed in {root} ({status}):\n{stderr}")]
    CargoMetadataFailed {
        root: PathBuf,
        status: ExitStatus,
        stderr: String,
    },
    #[error("could not read the output of `cargo metadata` in {root}: {source}")]
    CargoMetadataOutput {
        root: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("workspace member {manifest} is not under the workspace root {root}")]
    MemberOutsideRoot { manifest: PathBuf, root: PathBuf },
    #[error(
        "{name} is in both [rules.crate_names] allow in {CONFIG_FILE} and [crate_names] in \
         {BASELINE_FILE}; an allowed crate needs no entry, so remove one of them"
    )]
    AllowedAndListed { name: String },
    /// Debug-formatted, so the invalid bytes are shown escaped.
    #[error("{path:?} is not valid UTF-8")]
    NonUtf8Path { path: PathBuf },
    /// A glob that matches no member checks nothing, so it is a typo or a
    /// member that moved.
    #[error(
        "[rules.dependency_fence.{instance}] {key} in {CONFIG_FILE} has {glob:?}, which matches \
         no workspace member; fix or remove it"
    )]
    UnmatchedMemberGlob {
        instance: String,
        key: &'static str,
        glob: String,
    },
    #[error("no Cargo.toml with a [workspace] table found at or above {start}")]
    NoWorkspaceRoot { start: PathBuf },
    /// The wording is for a rule with instances: every other rule's
    /// configuration is required, so only a table of a rule with instances
    /// can name none.
    #[error(
        "{path}: [{table}] names no instance in {CONFIG_FILE}; remove that table, or rename it if \
         the instance was renamed"
    )]
    UnknownBaselineTable { path: PathBuf, table: String },
    /// A `[layers.<check>]` table is checked only with `[rules.layers]`, and
    /// only for a check with a baseline.
    #[error(
        "{path}: [{table}] is not a table of the layers rule: with [rules.layers] in {CONFIG_FILE}, \
         its tables are {known}, and without it there are none ([layers.unowned] \
         also needs [rules.layers.state]); remove that table"
    )]
    UnknownLayersTable {
        path: PathBuf,
        table: String,
        known: String,
    },
    #[error("could not read {path} as renames: {source}")]
    Renames {
        path: PathBuf,
        #[source]
        source: RenamesError,
    },
    /// A rule's tables must be read from a base baseline, or every entry of
    /// them would compare as new.
    #[error(
        "{path}: archcheck does not read the [{rule}] tables of a base baseline; read them in \
         Baseline::load_base"
    )]
    UnreadBaseTables { path: PathBuf, rule: &'static str },
    #[error(
        "[rules.layers.state] and [rules.layers.unowned] in {CONFIG_FILE} go together: the state \
         checks need both; add the missing one or remove both"
    )]
    StateWithoutUnowned,
    #[error(
        "[rules.layers.state.shared_catalogs] {shared} in {CONFIG_FILE} names {name:?}, which is \
         not a directory under {CATALOGS_DIR}/; fix or remove it"
    )]
    UnknownCatalog { shared: String, name: String },
    #[error(
        "[rules.layers.state.shared_catalogs] {shared} in {CONFIG_FILE} lists {name:?}, which is \
         a shared catalog, not a database; remove it"
    )]
    SharedCatalogAsDatabase { shared: String, name: String },
    /// Two names of one table would hide that the table is declared twice.
    #[error(
        "{path}: [package.metadata.space.state] {namespace} names the table {table} twice, as \
         {first:?} and {second:?}; keep one"
    )]
    DuplicateDeclaration {
        path: PathBuf,
        namespace: String,
        table: String,
        first: String,
        second: String,
    },
}

/// A production Rust file, its text and its code-line count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionFile {
    /// Repository-relative, `/`-separated.
    pub path: String,
    pub text: String,
    pub code_lines: usize,
}

/// A workspace member's package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Crate {
    /// `[package] name`.
    pub name: String,
    /// The name of its library target, which Cargo derives from the package
    /// name unless `[lib] name` sets it; `None` without a library.
    pub lib: Option<String>,
    /// The member's directory relative to the workspace root, `/`-separated;
    /// empty for a root package.
    pub member: String,
    /// The 1-based line of `name` in the member's `Cargo.toml`.
    pub name_line: usize,
    /// The 1-based line of `[lib] name`, when the manifest sets it.
    pub lib_name_line: Option<usize>,
    /// Every dependency its manifest declares, of any kind and target.
    pub dependencies: Vec<Dependency>,
    /// Its manifest's `[features]`: each feature to what it enables, such
    /// as `dep:sqlx` or `other/sqlx`, each with the 1-based line it is
    /// written on. Cargo's implicit features for optional dependencies are
    /// not among them.
    pub features: BTreeMap<String, Vec<(String, usize)>>,
    /// Its `[package.metadata.space.state]`, if the manifest has one.
    pub state: Option<StateDeclarations>,
}

/// A dependency a member's manifest declares, as `cargo metadata` reports
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependency {
    /// The depended-on package's name, whatever the manifest calls it.
    pub package: String,
    /// The name the manifest gives it with `package = "…"`, if it renames it.
    pub rename: Option<String>,
    /// `None` for `[dependencies]`; `dev` or `build` otherwise.
    pub kind: Option<String>,
    /// The `cfg(…)` or target triple of a `[target.<target>.…]` table.
    pub target: Option<String>,
    /// The 1-based line that names it in the manifest, or 1 if none does.
    pub line: usize,
    /// Whether it is `optional = true`, built only when a feature enables
    /// it.
    pub optional: bool,
    /// The features it enables on the depended-on package, `features =
    /// […]`, including those the workspace's entry adds.
    pub features: Vec<String>,
    /// Whether it enables the depended-on package's `default` feature.
    pub uses_default_features: bool,
}

impl Dependency {
    /// How the manifest declares it, such as `a normal dependency` or
    /// `renamed sqlite, a dev-dependency, for target cfg(unix)`.
    pub fn describe(&self) -> String {
        let mut declared = Vec::new();
        if let Some(rename) = &self.rename {
            declared.push(format!("renamed {rename}"));
        }
        declared.push(match self.kind.as_deref() {
            None => "a normal dependency".to_owned(),
            Some(kind) => format!("a {kind}-dependency"),
        });
        if let Some(target) = &self.target {
            declared.push(format!("for target {target}"));
        }
        declared.join(", ")
    }

    /// Whether it is a `[dev-dependencies]` entry, which only tests,
    /// examples and benchmarks build with.
    pub fn is_dev(&self) -> bool {
        self.kind.as_deref() == Some("dev")
    }

    /// The name the manifest and its `[features]` call it by: its
    /// `package = "…"` rename, or the package name.
    pub fn key(&self) -> &str {
        self.rename.as_deref().unwrap_or(&self.package)
    }
}

impl Crate {
    /// Repository-relative, `/`-separated.
    pub fn manifest(&self) -> String {
        if self.member.is_empty() {
            ROOT_MANIFEST.to_owned()
        } else {
            format!("{}/{ROOT_MANIFEST}", self.member)
        }
    }
}

#[derive(Debug)]
pub struct Context {
    pub config: ArchitectureConfig,
    pub baseline: Baseline,
    /// Behaviours that compare with the base branch's baseline are skipped
    /// unless it is [`BaseBaseline::Loaded`]; `crate_names.no_new` is limited
    /// instead.
    pub base_baseline: BaseBaseline,
    /// The files the pull request renamed, given with `--renames`: an entry
    /// for one is compared with its old path's entry in the base branch's
    /// baseline. `None` when not given, when any file may have been renamed.
    pub renames: Option<Renames>,
    /// `[rules.crate_names]` of `ARCHITECTURE.toml`, with the line of each
    /// part.
    pub crate_names_policy: CrateNamesPolicy,
    /// `[rules.layers]` of `ARCHITECTURE.toml`, with the line of each entry;
    /// empty without one.
    pub layers_policy: LayersPolicy,
    /// The base branch's `ARCHITECTURE.toml`, given with
    /// `--base-architecture`.
    pub base_architecture: BaseArchitecture,
    /// Every production Rust file of the workspace members, sorted by path.
    pub files: Vec<ProductionFile>,
    /// The package of each workspace member, sorted by member path.
    pub crates: Vec<Crate>,
    /// The catalogs under `db/`, sorted by name; read only with
    /// `[rules.layers.state]`.
    pub catalogs: Vec<Catalog>,
    /// Every `.sql` file under a member's `src/`, outside `tests/`
    /// directories, sorted by path; read only with `[rules.layers.state]`.
    pub sql_files: Vec<TextFile>,
}

impl Context {
    pub fn load(root: &Path) -> Result<Self, LoadError> {
        let baseline = Baseline::load(&root.join(BASELINE_FILE))?;
        Self::load_with_baseline(root, baseline)
    }

    pub fn load_with_baseline(root: &Path, baseline: Baseline) -> Result<Self, LoadError> {
        let config_path = root.join(CONFIG_FILE);
        let (config, config_text): (ArchitectureConfig, String) =
            parse_toml_with_text(&config_path)?;
        if let Some(layers) = &config.rules.layers
            && layers.state.is_some() != layers.unowned.is_some()
        {
            return Err(LoadError::StateWithoutUnowned);
        }
        // A table for an instance that no longer exists would never be
        // checked, so it cannot silently outlive its configuration.
        let configured = rules::tables(&config.rules);
        if let Some(table) = baseline
            .table_names()
            .into_iter()
            .find(|table| !configured.contains(table))
        {
            let path = root.join(BASELINE_FILE);
            if rule_of(&table) == Layers::NAME {
                return Err(LoadError::UnknownLayersTable {
                    path,
                    table,
                    known: Layers::table_list(),
                });
            }
            return Err(LoadError::UnknownBaselineTable { path, table });
        }
        let parse_error = |source| LoadError::Parse {
            path: config_path.clone(),
            source,
        };
        let crate_names_policy = CrateNamesPolicy::parse(&config_text)
            .map_err(parse_error)?
            .unwrap_or_default();
        let layers_policy = LayersPolicy::parse(&config_text)
            .map_err(parse_error)?
            .unwrap_or_default();
        if let Some(name) = baseline
            .crate_names
            .keys()
            .find(|name| config.rules.crate_names.allow.contains(name))
        {
            return Err(LoadError::AllowedAndListed { name: name.clone() });
        }
        let crates = workspace_crates(root)?;
        let members = crates.iter().map(|krate| krate.member.as_str());
        for (instance, fence) in &config.rules.dependency_fence {
            for (key, globs) in [("members", fence.members.globs()), ("allow", &fence.allow)] {
                if let Some(glob) = globs.unmatched(members.clone()) {
                    return Err(LoadError::UnmatchedMemberGlob {
                        instance: instance.to_string(),
                        key,
                        glob: glob.to_owned(),
                    });
                }
            }
        }
        let files = production_files(root, &crates)?;
        let layers = config.rules.layers.as_ref();
        let (catalogs, sql_files) = match layers.and_then(|layers| layers.state.as_ref()) {
            Some(state_config) => (
                state::catalogs(root, state_config)?,
                state::sql_files(root, &crates)?,
            ),
            None => (Vec::new(), Vec::new()),
        };
        Ok(Self {
            config,
            baseline,
            base_baseline: BaseBaseline::default(),
            renames: None,
            crate_names_policy,
            layers_policy,
            base_architecture: BaseArchitecture::NotGiven,
            files,
            crates,
            catalogs,
            sql_files,
        })
    }

    pub fn file(&self, path: &str) -> Option<&ProductionFile> {
        self.files
            .binary_search_by(|file| file.path.as_str().cmp(path))
            .ok()
            .and_then(|index| self.files.get(index))
    }
}

/// The nearest ancestor of `start` (inclusive) whose `Cargo.toml` declares a
/// `[workspace]`.
pub fn find_workspace_root(start: &Path) -> Result<PathBuf, LoadError> {
    for dir in start.ancestors() {
        let manifest = dir.join(ROOT_MANIFEST);
        if manifest.is_file() && parse_toml::<toml::Table>(&manifest)?.contains_key("workspace") {
            return Ok(dir.to_path_buf());
        }
    }
    Err(LoadError::NoWorkspaceRoot {
        start: start.to_path_buf(),
    })
}

/// A sibling test module (`tests.rs`, `<name>_tests.rs`), not production code.
pub fn is_test_file(name: &str) -> bool {
    name == "tests.rs" || name.ends_with("_tests.rs")
}

/// A `tests/` directory under `src/` holds test modules (`#[cfg(test)] mod
/// tests;` with a `tests/mod.rs`), so nothing below it is production code.
/// `scripts/check-rust-error-policy.sh` excludes `*/tests/*` the same way.
pub fn is_test_directory(name: &OsStr) -> bool {
    name == "tests"
}

/// The number of [`code_lines`] in `text`.
pub fn count_code_lines(text: &str) -> usize {
    code_lines(text).count()
}

/// Each code line of `text`, untrimmed and without its line ending, with its
/// 1-based line number. A code line is non-empty after trimming and does not
/// start with `//`: line, doc and inner-doc comments are free; block comments
/// and attributes count. Lines end at LF or CRLF, where rustc ends a `//`
/// comment and counts line numbers; a lone CR, U+2028 or U+2029 ends nothing,
/// so a CR-only file is one line. A leading byte order mark is not part of the
/// first line.
pub fn code_lines(text: &str) -> impl Iterator<Item = (usize, &str)> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    text.lines()
        .enumerate()
        .map(|(index, line)| (index + 1, line))
        .filter(|(_, line)| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with("//")
        })
}

pub(crate) fn read(path: &Path) -> Result<String, LoadError> {
    std::fs::read_to_string(path).map_err(|source| LoadError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn parse_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, LoadError> {
    parse_toml_with_text(path).map(|(value, _)| value)
}

/// [`parse_toml`], also returning the text it parsed.
pub(crate) fn parse_toml_with_text<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<(T, String), LoadError> {
    let text = read(path)?;
    match toml::from_str(&text) {
        Ok(value) => Ok((value, text)),
        Err(source) => Err(LoadError::Parse {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// The part of `cargo metadata --format-version 1` the inventory needs.
#[derive(Deserialize)]
struct Metadata {
    packages: Vec<MetadataPackage>,
    workspace_members: Vec<String>,
    workspace_root: PathBuf,
}

#[derive(Deserialize)]
struct MetadataPackage {
    id: String,
    name: String,
    manifest_path: PathBuf,
    targets: Vec<MetadataTarget>,
    dependencies: Vec<MetadataDependency>,
}

#[derive(Deserialize)]
struct MetadataDependency {
    name: String,
    rename: Option<String>,
    kind: Option<String>,
    target: Option<String>,
    #[serde(default)]
    optional: bool,
    #[serde(default)]
    features: Vec<String>,
    #[serde(default = "enabled")]
    uses_default_features: bool,
}

/// Cargo's default for `default-features`.
fn enabled() -> bool {
    true
}

#[derive(Deserialize)]
struct MetadataTarget {
    name: String,
    kind: Vec<String>,
}

/// Every workspace member as Cargo sees it: the root manifest's `members`,
/// expanded, and every path dependency inside the workspace, which Cargo makes
/// a member too. `cargo` is the one running archcheck under `cargo run`.
fn workspace_crates(root: &Path) -> Result<Vec<Crate>, LoadError> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--offline",
        ])
        .current_dir(root)
        .output()
        .map_err(|source| LoadError::CargoMetadataSpawn {
            root: root.to_path_buf(),
            source,
        })?;
    if !output.status.success() {
        return Err(LoadError::CargoMetadataFailed {
            root: root.to_path_buf(),
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr)
                .trim_end()
                .to_owned(),
        });
    }
    let metadata: Metadata = serde_json::from_slice(&output.stdout).map_err(|source| {
        LoadError::CargoMetadataOutput {
            root: root.to_path_buf(),
            source,
        }
    })?;
    let mut crates = metadata
        .packages
        .into_iter()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| load_crate(&metadata.workspace_root, package))
        .collect::<Result<Vec<_>, _>>()?;
    crates.sort_by(|a, b| a.member.cmp(&b.member));
    Ok(crates)
}

/// Only the names of a member's manifest, for their lines, and its
/// `[features]`, whose lines `cargo metadata` does not give; Cargo has read
/// the rest.
#[derive(Deserialize)]
struct MemberManifest {
    package: Option<PackageTable>,
    lib: Option<NameTable>,
    #[serde(default)]
    features: BTreeMap<String, Vec<Spanned<String>>>,
}

#[derive(Deserialize)]
struct PackageTable {
    name: Option<Spanned<String>>,
    metadata: Option<state::PackageMetadata>,
}

#[derive(Deserialize)]
struct NameTable {
    name: Option<Spanned<String>>,
}

fn load_crate(workspace_root: &Path, package: MetadataPackage) -> Result<Crate, LoadError> {
    let manifest = package.manifest_path;
    let dir = manifest.parent().unwrap_or(&manifest);
    let Ok(member) = dir.strip_prefix(workspace_root) else {
        return Err(LoadError::MemberOutsideRoot {
            manifest,
            root: workspace_root.to_path_buf(),
        });
    };
    let member = relative_path(Path::new(""), member)?;
    let (parsed, text): (MemberManifest, String) = parse_toml_with_text(&manifest)?;
    let features = parsed
        .features
        .into_iter()
        .map(|(feature, enables)| {
            let enables = enables
                .into_iter()
                .map(|item| {
                    let at = line_of(&text, item.span().start);
                    (item.into_inner(), at)
                })
                .collect();
            (feature, enables)
        })
        .collect();
    let line = |name: Option<Spanned<String>>| name.map(|name| line_of(&text, name.span().start));
    let (name, metadata) = parsed
        .package
        .map_or((None, None), |package| (package.name, package.metadata));
    let lib = package
        .targets
        .into_iter()
        .find(|target| {
            target
                .kind
                .iter()
                .any(|kind| LIB_KINDS.contains(&kind.as_str()))
        })
        .map(|target| target.name);
    let dependencies = package
        .dependencies
        .into_iter()
        .map(|dependency| Dependency {
            line: dependency_line(
                &text,
                dependency.rename.as_deref().unwrap_or(&dependency.name),
            ),
            package: dependency.name,
            rename: dependency.rename,
            kind: dependency.kind,
            target: dependency.target,
            optional: dependency.optional,
            features: dependency.features,
            uses_default_features: dependency.uses_default_features,
        })
        .collect();
    Ok(Crate {
        name: package.name,
        lib,
        member,
        name_line: line(name).unwrap_or(1),
        lib_name_line: line(parsed.lib.and_then(|lib| lib.name)),
        dependencies,
        features,
        state: match metadata {
            Some(metadata) => metadata.state(&manifest, &text)?,
            None => None,
        },
    })
}

/// The first line of `manifest` that declares a dependency called `key`:
/// `key = …` or `key.workspace = …` in a dependency table, or a
/// `[….dependencies.key]` header, with the key bare or quoted. Other tables,
/// such as `[features]`, whose keys may share the name, and the root
/// manifest's `[workspace.dependencies]` are skipped. 1 if none does, since
/// the line only places the finding.
fn dependency_line(manifest: &str, key: &str) -> usize {
    let quoted = format!("\"{key}\"");
    let names_key = |rest: &str| {
        [key, quoted.as_str()].iter().any(|name| {
            rest.strip_prefix(name)
                .is_some_and(|after| after.trim_start().starts_with(['=', '.', ']']))
        })
    };
    let mut in_dependencies = false;
    for (index, line) in manifest.lines().enumerate() {
        let line = line.trim_start();
        if let Some(header) = line.strip_prefix('[') {
            let header = header.split(']').next().unwrap_or_default().trim();
            if header.starts_with("workspace") {
                in_dependencies = false;
                continue;
            }
            in_dependencies = header.ends_with("dependencies");
            if header
                .split_once("dependencies.")
                .is_some_and(|(_, rest)| names_key(&format!("{rest}]")))
            {
                return index + 1;
            }
        } else if in_dependencies && names_key(line) {
            return index + 1;
        }
    }
    1
}

fn production_files(root: &Path, crates: &[Crate]) -> Result<Vec<ProductionFile>, LoadError> {
    let mut paths = Vec::new();
    for krate in crates {
        // A member directory or `src` that is itself a symlink is followed;
        // symlinks below `src/` are not.
        let src = root.join(&krate.member).join("src");
        if src.is_dir() {
            collect_rust_files(&src, &mut paths)?;
        }
    }
    let mut files = paths
        .into_iter()
        .map(|path| {
            let text = read(&path)?;
            Ok(ProductionFile {
                code_lines: count_code_lines(&text),
                text,
                path: relative_path(root, &path)?,
            })
        })
        .collect::<Result<Vec<_>, LoadError>>()?;
    // Sort by the string form: `Path` ordering is per component and would not
    // match the baseline's key order or `Context::file`'s binary search.
    files.sort_by(|a, b| a.path.cmp(&b.path));
    // A member nested inside another member's `src/` is walked twice.
    files.dedup_by(|a, b| a.path == b.path);
    Ok(files)
}

/// Symlinks are neither files nor directories here, so they are skipped.
fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), LoadError> {
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
                collect_rust_files(&path, out)?;
            }
            continue;
        }
        // Only `.rs` names have to be UTF-8, so the extension is checked on
        // the raw name first.
        if !file_type.is_file() || path.extension() != Some(OsStr::new("rs")) {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return Err(LoadError::NonUtf8Path { path });
        };
        if !is_test_file(name) {
            out.push(path);
        }
    }
    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String, LoadError> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let parts: Option<Vec<&str>> = relative
        .components()
        .map(|component| component.as_os_str().to_str())
        .collect();
    match parts {
        Some(parts) => Ok(parts.join("/")),
        None => Err(LoadError::NonUtf8Path {
            path: path.to_path_buf(),
        }),
    }
}
