//! `[rules.layers]`: the layer of each workspace member that is not yet in its
//! layer's directory, which crates may use each store, the declared
//! exceptions to the layer order, and the mode of each of the layers rule's
//! checks.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::Deserialize;
use thiserror::Error;
use toml::Spanned;

use super::BehaviourConfig;
use crate::baseline::line_of;

/// A layer of the stack, bottom-up, or one of the two sets of crates beside
/// it. Its name is its directory under `crates/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(try_from = "String")]
pub enum Layer {
    Foundation,
    Domain,
    Storage,
    Api,
    App,
    /// Test doubles and fixtures, reachable only through dev-dependencies.
    TestSupport,
    /// Never shipped, and nothing depends on them.
    Tools,
}

#[derive(Debug, Error)]
#[error(
    "unknown layer {0:?}; expected one of l10_foundation, l20_domain, l30_storage, l40_api, \
     l50_app, test_support, tools"
)]
pub struct UnknownLayerError(String);

impl Layer {
    pub const ALL: [Self; 7] = [
        Self::Foundation,
        Self::Domain,
        Self::Storage,
        Self::Api,
        Self::App,
        Self::TestSupport,
        Self::Tools,
    ];

    /// The directory under `crates/`, which is also the layer's name in
    /// `ARCHITECTURE.toml`.
    pub fn dir(self) -> &'static str {
        match self {
            Self::Foundation => "l10_foundation",
            Self::Domain => "l20_domain",
            Self::Storage => "l30_storage",
            Self::Api => "l40_api",
            Self::App => "l50_app",
            Self::TestSupport => "test_support",
            Self::Tools => "tools",
        }
    }

    /// The layer whose directory is `dir`.
    pub fn from_dir(dir: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|layer| layer.dir() == dir)
    }

    /// The position in the stack, bottom-up; `None` for `test_support` and
    /// `tools`, which are outside it.
    pub fn rank(self) -> Option<u8> {
        match self {
            Self::Foundation => Some(10),
            Self::Domain => Some(20),
            Self::Storage => Some(30),
            Self::Api => Some(40),
            Self::App => Some(50),
            Self::TestSupport | Self::Tools => None,
        }
    }
}

impl TryFrom<String> for Layer {
    type Error = UnknownLayerError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::from_dir(&name).ok_or(UnknownLayerError(name))
    }
}

impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.dir())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayersConfig {
    /// Package name to its layer, for each crate not yet in its layer's
    /// directory, written as each layer's list of packages. Only shrinks: a
    /// new crate goes in a layer directory.
    #[serde(default)]
    pub unmoved: Unmoved,
    /// Each store's package name to the packages that may depend on it.
    #[serde(default)]
    pub store_users: BTreeMap<String, PackageSet>,
    /// Package name to the packages it may depend on although the layer
    /// order forbids it.
    #[serde(default)]
    pub allowed_edges: BTreeMap<String, PackageSet>,
    /// What `storage_access` and `codec_features` treat as storage.
    pub storage: StorageConfig,
    /// Crates that predate the layers and are being dissolved. A legacy
    /// Foundation crate is no storage helper even with a required driver
    /// dependency; every check covers legacy crates, with today's edges
    /// baselined.
    #[serde(default)]
    pub legacy: PackageSet,
    pub placement: BehaviourConfig,
    pub layout: BehaviourConfig,
    pub direction: EdgeBehaviours,
    pub dev_direction: EdgeBehaviours,
    pub storage_access: EdgeBehaviours,
    pub codec_features: EdgeBehaviours,
    pub test_only: BehaviourConfig,
    pub clients: BehaviourConfig,
    pub store_edges: BehaviourConfig,
    pub stale_entries: BehaviourConfig,
    pub policy_changed: BehaviourConfig,
}

/// `[rules.layers.unmoved]`: each layer's packages, read as package to
/// layer, each package in one layer.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(try_from = "BTreeMap<Layer, PackageSet>")]
pub struct Unmoved(BTreeMap<String, Layer>);

#[derive(Debug, Error)]
#[error("unmoved lists {package:?} in both {first} and {second}")]
pub struct UnmovedError {
    package: String,
    first: Layer,
    second: Layer,
}

impl Unmoved {
    pub fn get(&self, package: &str) -> Option<Layer> {
        self.0.get(package).copied()
    }

    /// Every package, sorted.
    pub fn packages(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }
}

impl TryFrom<BTreeMap<Layer, PackageSet>> for Unmoved {
    type Error = UnmovedError;

    fn try_from(layers: BTreeMap<Layer, PackageSet>) -> Result<Self, Self::Error> {
        let mut unmoved = BTreeMap::new();
        for (layer, packages) in layers {
            for package in packages.0 {
                if let Some(first) = unmoved.insert(package.clone(), layer) {
                    return Err(UnmovedError {
                        package,
                        first,
                        second: layer,
                    });
                }
            }
        }
        Ok(Self(unmoved))
    }
}

/// Layer names, each listed once, as a `pattern_ratchet` instance's
/// `allow_layers` takes them; empty when absent.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(try_from = "Vec<Layer>")]
pub struct LayerSet(BTreeSet<Layer>);

#[derive(Debug, Error)]
#[error("layer {0} is listed twice")]
pub struct LayerSetError(Layer);

impl LayerSet {
    pub fn contains(&self, layer: Layer) -> bool {
        self.0.contains(&layer)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The layers as a list for a sentence, bottom-up.
    pub fn names(&self) -> String {
        let names: Vec<&str> = self.0.iter().map(|layer| layer.dir()).collect();
        names.join(", ")
    }
}

impl TryFrom<Vec<Layer>> for LayerSet {
    type Error = LayerSetError;

    fn try_from(layers: Vec<Layer>) -> Result<Self, Self::Error> {
        let mut set = BTreeSet::new();
        for layer in layers {
            if !set.insert(layer) {
                return Err(LayerSetError(layer));
            }
        }
        Ok(Self(set))
    }
}

/// A `pattern_ratchet` instance's `include_layers`: a [`LayerSet`] with at
/// least one layer, since an empty list would check nothing.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Vec<Layer>")]
pub struct IncludeLayers(LayerSet);

#[derive(Debug, Error)]
pub enum IncludeLayersError {
    #[error("include_layers must list at least one layer; omit it to check every layer")]
    Empty,
    #[error(transparent)]
    Set(#[from] LayerSetError),
}

impl IncludeLayers {
    pub fn layers(&self) -> &LayerSet {
        &self.0
    }
}

impl TryFrom<Vec<Layer>> for IncludeLayers {
    type Error = IncludeLayersError;

    fn try_from(layers: Vec<Layer>) -> Result<Self, Self::Error> {
        if layers.is_empty() {
            return Err(IncludeLayersError::Empty);
        }
        Ok(Self(LayerSet::try_from(layers)?))
    }
}

/// `[rules.layers.storage]`: the packages that reach a database or object
/// storage, and the storage helpers without a dependency on one.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Packages outside the workspace that talk to a database or object
    /// storage, such as `sqlx`.
    pub drivers: Drivers,
    /// Foundation crates that are storage helpers without a required
    /// dependency on a driver, which would make them one.
    #[serde(default)]
    pub helpers: PackageSet,
}

/// A [`PackageSet`] with at least one package, since without a driver the
/// storage checks would check nothing.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct Drivers(PackageSet);

#[derive(Debug, Error)]
pub enum DriversError {
    #[error("drivers must list at least one package")]
    Empty,
    #[error(transparent)]
    Duplicate(#[from] DuplicatePackageError),
}

impl Drivers {
    pub fn contains(&self, package: &str) -> bool {
        self.0.contains(package)
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }
}

impl TryFrom<Vec<String>> for Drivers {
    type Error = DriversError;

    fn try_from(packages: Vec<String>) -> Result<Self, Self::Error> {
        if packages.is_empty() {
            return Err(DriversError::Empty);
        }
        Ok(Self(PackageSet::try_from(packages)?))
    }
}

/// The behaviours of a layers check with a baseline table of edges.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeBehaviours {
    pub no_crossing: BehaviourConfig,
    pub no_new_entries: BehaviourConfig,
    pub stale_entries: BehaviourConfig,
}

/// Package names, each listed once.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct PackageSet(BTreeSet<String>);

#[derive(Debug, Error)]
#[error("{0:?} is listed twice")]
pub struct DuplicatePackageError(String);

impl PackageSet {
    pub fn contains(&self, package: &str) -> bool {
        self.0.contains(package)
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }
}

impl TryFrom<Vec<String>> for PackageSet {
    type Error = DuplicatePackageError;

    fn try_from(packages: Vec<String>) -> Result<Self, Self::Error> {
        let mut set = BTreeSet::new();
        for package in packages {
            if set.contains(&package) {
                return Err(DuplicatePackageError(package));
            }
            set.insert(package);
        }
        Ok(Self(set))
    }
}

/// `[rules.layers]` as `layers.policy_changed` compares it and
/// `layers.stale_entries` places its findings: each entry with the 1-based
/// line it is written on. Read leniently, so that the base branch's copy can
/// have another schema.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LayersPolicy {
    /// The line of the first `[rules.layers…]` header.
    pub line: usize,
    /// Package name to its layer as written.
    pub unmoved: Entries<String>,
    pub store_users: Entries<BTreeMap<String, usize>>,
    pub allowed_edges: Entries<BTreeMap<String, usize>>,
    pub storage: StoragePolicy,
    /// `legacy`, each package with the line it is written on.
    pub legacy: BTreeMap<String, usize>,
    /// The line of the `legacy` key, or without one, of the
    /// `[rules.layers]` header, if there is one.
    pub legacy_line: Option<usize>,
    /// Behaviour name, such as `placement` or `direction.no_crossing`, to
    /// its mode as written and the mode's line.
    pub modes: BTreeMap<String, (String, usize)>,
}

/// `[rules.layers.storage]`'s lists, each package with the line it is
/// written on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoragePolicy {
    /// The line of the table's header, if it has one.
    pub line: Option<usize>,
    pub drivers: BTreeMap<String, usize>,
    pub helpers: BTreeMap<String, usize>,
}

/// One table of `[rules.layers]` keyed by package name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entries<V> {
    /// The line of the table's header, if it has one.
    pub line: Option<usize>,
    /// Package name to its value and the line of its key.
    pub entries: BTreeMap<String, (V, usize)>,
}

#[derive(Deserialize)]
struct PolicyFile {
    #[serde(default)]
    rules: PolicyRules,
}

#[derive(Default, Deserialize)]
struct PolicyRules {
    layers: Option<PolicyTable>,
}

/// `flatten` and `untagged` would lose the spans, so each behaviour is a
/// field.
#[derive(Deserialize)]
struct PolicyTable {
    unmoved: Option<BTreeMap<String, Vec<Spanned<String>>>>,
    store_users: Option<BTreeMap<Spanned<String>, Vec<Spanned<String>>>>,
    allowed_edges: Option<BTreeMap<Spanned<String>, Vec<Spanned<String>>>>,
    storage: Option<PolicyStorage>,
    legacy: Option<Spanned<Vec<Spanned<String>>>>,
    placement: Option<PolicyBehaviour>,
    layout: Option<PolicyBehaviour>,
    direction: Option<PolicyEdgeBehaviours>,
    dev_direction: Option<PolicyEdgeBehaviours>,
    storage_access: Option<PolicyEdgeBehaviours>,
    codec_features: Option<PolicyEdgeBehaviours>,
    test_only: Option<PolicyBehaviour>,
    clients: Option<PolicyBehaviour>,
    store_edges: Option<PolicyBehaviour>,
    stale_entries: Option<PolicyBehaviour>,
    policy_changed: Option<PolicyBehaviour>,
}

#[derive(Deserialize)]
struct PolicyStorage {
    drivers: Option<Vec<Spanned<String>>>,
    helpers: Option<Vec<Spanned<String>>>,
}

#[derive(Deserialize)]
struct PolicyEdgeBehaviours {
    no_crossing: Option<PolicyBehaviour>,
    no_new_entries: Option<PolicyBehaviour>,
    stale_entries: Option<PolicyBehaviour>,
}

#[derive(Deserialize)]
struct PolicyBehaviour {
    mode: Option<Spanned<String>>,
}

impl LayersPolicy {
    /// `[rules.layers]` of `text`, or `None` without one. Other tables and
    /// unknown keys are ignored.
    pub fn parse(text: &str) -> Result<Option<Self>, toml::de::Error> {
        let file: PolicyFile = toml::from_str(text)?;
        let Some(table) = file.rules.layers else {
            return Ok(None);
        };
        let line = |span: std::ops::Range<usize>| line_of(text, span.start);
        let header = |name: &str| header_line(text, &format!("[rules.layers.{name}]"));
        let unmoved = Entries {
            line: header("unmoved"),
            entries: table
                .unmoved
                .unwrap_or_default()
                .into_iter()
                .flat_map(|(layer, packages)| {
                    packages.into_iter().map(move |package| {
                        let at = line(package.span());
                        (package.into_inner(), (layer.clone(), at))
                    })
                })
                .collect(),
        };
        let lists =
            |name: &str, lists: Option<BTreeMap<Spanned<String>, Vec<Spanned<String>>>>| Entries {
                line: header(name),
                entries: lists
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(package, listed)| {
                        let at = line(package.span());
                        let listed = listed
                            .into_iter()
                            .map(|item| {
                                let at = line(item.span());
                                (item.into_inner(), at)
                            })
                            .collect();
                        (package.into_inner(), (listed, at))
                    })
                    .collect(),
            };
        let store_users = lists("store_users", table.store_users);
        let allowed_edges = lists("allowed_edges", table.allowed_edges);
        let packages = |list: Option<Vec<Spanned<String>>>| {
            list.unwrap_or_default()
                .into_iter()
                .map(|item| {
                    let at = line(item.span());
                    (item.into_inner(), at)
                })
                .collect()
        };
        let storage = match table.storage {
            None => StoragePolicy::default(),
            Some(storage) => StoragePolicy {
                line: header("storage"),
                drivers: packages(storage.drivers),
                helpers: packages(storage.helpers),
            },
        };
        let legacy_line = match &table.legacy {
            Some(legacy) => Some(line(legacy.span())),
            None => header_line(text, "[rules.layers]"),
        };
        let legacy = packages(table.legacy.map(Spanned::into_inner));
        let mut modes = BTreeMap::new();
        let mut mode = |behaviour: String, entry: Option<PolicyBehaviour>| {
            if let Some(mode) = entry.and_then(|entry| entry.mode) {
                modes.insert(behaviour, (mode.get_ref().clone(), line(mode.span())));
            }
        };
        for (behaviour, entry) in [
            ("placement", table.placement),
            ("layout", table.layout),
            ("test_only", table.test_only),
            ("clients", table.clients),
            ("store_edges", table.store_edges),
            ("stale_entries", table.stale_entries),
            ("policy_changed", table.policy_changed),
        ] {
            mode(behaviour.to_owned(), entry);
        }
        for (check, entry) in [
            ("direction", table.direction),
            ("dev_direction", table.dev_direction),
            ("storage_access", table.storage_access),
            ("codec_features", table.codec_features),
        ] {
            let Some(entry) = entry else { continue };
            for (behaviour, entry) in [
                ("no_crossing", entry.no_crossing),
                ("no_new_entries", entry.no_new_entries),
                ("stale_entries", entry.stale_entries),
            ] {
                mode(format!("{check}.{behaviour}"), entry);
            }
        }
        let line = text
            .lines()
            .position(|line| {
                let line = line.trim_start();
                line.starts_with("[rules.layers]") || line.starts_with("[rules.layers.")
            })
            .map_or(1, |index| index + 1);
        Ok(Some(Self {
            line,
            unmoved,
            store_users,
            allowed_edges,
            storage,
            legacy,
            legacy_line,
            modes,
        }))
    }
}

/// The 1-based line of the first line of `text` that starts with `header`,
/// after leading whitespace.
fn header_line(text: &str, header: &str) -> Option<usize> {
    text.lines()
        .position(|line| line.trim_start().starts_with(header))
        .map(|index| index + 1)
}
