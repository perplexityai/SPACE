//! A codec facade's source, read strictly for `storage_access` with syn, a
//! Rust parser: the only items are `pub mod` blocks that hold such items and
//! `pub use` items that start with a driver the facade depends on, with
//! `cfg(…)` and `doc = "…"` attributes, which doc comments are. Anything
//! else, such as a `fn`, a `macro_rules!`, `mod x;`, a `pub(crate)` item, a
//! `use` that renames, globs or names `self`, or a file syn can't parse, is a
//! problem, not something to skip: the facade's exports are what every codec
//! may use.
//!
//! archcheck takes syn 2 with only its `full` and `parsing` features. A
//! `cargo build --workspace` compiles that syn already, but `cargo arch` and
//! `run-archcheck.sh`, which build only archcheck's graph, compile a
//! target-side syn and proc-macro2 of their own. proc-macro2's
//! `span-locations` would also, in a workspace build, build proc-macro2 a
//! second time and rebuild every proc macro once, so syn reads no line
//! numbers: a finding is on line 1 of the file, naming the item, or, for a
//! file syn can't parse, giving syn's error without a position.

use std::collections::BTreeMap;

use syn::{Attribute, Expr, Item, Lit, Meta, UseTree, Visibility};

/// What one `pub use` item re-exports.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Export {
    /// The driver's package.
    pub driver: String,
    /// The name the source starts the path with, the driver's.
    pub root: String,
    /// The path below the driver's root, such as `postgres::PgTypeInfo`.
    pub path: String,
}

/// What a facade's source file re-exports, and why it is not one a facade
/// may have, if it isn't.
#[derive(Default)]
pub(super) struct Source {
    pub exports: Vec<Export>,
    pub problems: Vec<String>,
}

/// `text`, a facade's source file, whose code names each driver it depends
/// on by a key of `drivers`, the driver's package its value.
pub(super) fn read(text: &str, drivers: &BTreeMap<String, String>) -> Source {
    let mut reader = Reader {
        drivers,
        source: Source::default(),
    };
    match syn::parse_file(text) {
        Err(error) => reader
            .source
            .problems
            .push(format!("it can't be read as Rust: {error}")),
        Ok(file) => {
            reader.attributes(&file.attrs, "the file");
            reader.items(&file.items, "");
        }
    }
    reader.source
}

struct Reader<'d> {
    drivers: &'d BTreeMap<String, String>,
    source: Source,
}

impl Reader<'_> {
    fn problem(&mut self, reason: String) {
        self.source.problems.push(reason);
    }

    /// The items of `module`, a path of `pub mod` names, empty for the
    /// file's top level.
    fn items(&mut self, items: &[Item], module: &str) {
        let place = if module.is_empty() {
            "at the file's top level".to_owned()
        } else {
            format!("in `{module}`")
        };
        for item in items {
            let owner = format!("`{}` {place}", head(item));
            match item {
                Item::Mod(inner) if is_pub(&inner.vis) => {
                    self.attributes(&inner.attrs, &owner);
                    if let Some((_, content)) = &inner.content {
                        let name = if module.is_empty() {
                            inner.ident.to_string()
                        } else {
                            format!("{module}::{}", inner.ident)
                        };
                        self.items(content, &name);
                    } else {
                        self.problem(format!("{owner} is no `pub mod` block"));
                    }
                }
                Item::Use(inner) if is_pub(&inner.vis) => {
                    self.attributes(&inner.attrs, &owner);
                    match &inner.tree {
                        UseTree::Path(path) if inner.leading_colon.is_none() => self.use_item(path),
                        _ => self.problem(format!("{owner} does not start with `<driver>::`")),
                    }
                }
                _ => self.problem(format!(
                    "{owner} is neither a `pub mod` block nor a `pub use` item"
                )),
            }
        }
    }

    /// Only `cfg(…)` and `doc = "…"`, on `owner`.
    fn attributes(&mut self, attrs: &[Attribute], owner: &str) {
        for attr in attrs {
            let allowed = match &attr.meta {
                Meta::List(list) => list.path.is_ident("cfg"),
                Meta::NameValue(pair) => {
                    pair.path.is_ident("doc")
                        && matches!(&pair.value, Expr::Lit(lit) if matches!(lit.lit, Lit::Str(_)))
                }
                Meta::Path(_) => false,
            };
            if !allowed {
                self.problem(format!(
                    "{owner} has a `{}` attribute that is neither `cfg(…)` nor `doc = \"…\"`",
                    path(attr.path())
                ));
            }
        }
    }

    /// `pub use <root>::<tree>;`.
    fn use_item(&mut self, root: &syn::UsePath) {
        let name = root.ident.to_string();
        let Some(driver) = self.drivers.get(&name) else {
            return self.problem(format!(
                "it re-exports from `{name}`, which is no driver it depends on"
            ));
        };
        self.tree(&root.tree, driver, &name, "");
    }

    /// A use tree below `prefix`: a path, a name, or a `{…}` group of trees.
    fn tree(&mut self, tree: &UseTree, driver: &str, root: &str, prefix: &str) {
        let below = |ident: &syn::Ident| {
            if prefix.is_empty() {
                ident.to_string()
            } else {
                format!("{prefix}::{ident}")
            }
        };
        let (ident, path) = match tree {
            UseTree::Path(inner) => (&inner.ident, below(&inner.ident)),
            UseTree::Name(inner) => (&inner.ident, below(&inner.ident)),
            UseTree::Rename(inner) => {
                return self.problem(format!(
                    "it re-exports {root}::{} as `{}`, and a re-export doesn't rename",
                    below(&inner.ident),
                    inner.rename
                ));
            }
            UseTree::Glob(_) => {
                let glob = if prefix.is_empty() {
                    "*".to_owned()
                } else {
                    format!("{prefix}::*")
                };
                return self.problem(format!(
                    "it re-exports {root}::{glob}, and a re-export doesn't glob"
                ));
            }
            UseTree::Group(group) => {
                for tree in &group.items {
                    self.tree(tree, driver, root, prefix);
                }
                return;
            }
        };
        if ["self", "super", "crate", "Self"].contains(&ident.to_string().as_str()) {
            return self.problem(format!(
                "it re-exports {root}::{path}, and a re-export names an item, not `{ident}`"
            ));
        }
        match tree {
            UseTree::Path(inner) => self.tree(&inner.tree, driver, root, &path),
            _ => self.source.exports.push(Export {
                driver: driver.to_owned(),
                root: root.to_owned(),
                path,
            }),
        }
    }
}

fn is_pub(vis: &Visibility) -> bool {
    matches!(vis, Visibility::Public(_))
}

fn path(path: &syn::Path) -> String {
    let segments: Vec<String> = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();
    segments.join("::")
}

/// How `item` starts, such as `pub(crate) fn pool`, `mod x;` or
/// `pub use ::sqlx::…`, to name it
/// in a problem.
fn head(item: &Item) -> String {
    let (vis, what) = match item {
        Item::Const(item) => (Some(&item.vis), format!("const {}", item.ident)),
        Item::Enum(item) => (Some(&item.vis), format!("enum {}", item.ident)),
        Item::ExternCrate(item) => (Some(&item.vis), format!("extern crate {}", item.ident)),
        Item::Fn(item) => (Some(&item.vis), format!("fn {}", item.sig.ident)),
        Item::ForeignMod(_) => (None, "extern { … }".to_owned()),
        Item::Impl(_) => (None, "impl".to_owned()),
        Item::Macro(item) => (
            None,
            match &item.ident {
                Some(ident) => format!("{}! {ident}", path(&item.mac.path)),
                None => format!("{}!", path(&item.mac.path)),
            },
        ),
        Item::Mod(item) => {
            let body = if item.content.is_some() {
                " { … }"
            } else {
                ";"
            };
            (Some(&item.vis), format!("mod {}{body}", item.ident))
        }
        Item::Static(item) => (Some(&item.vis), format!("static {}", item.ident)),
        Item::Struct(item) => (Some(&item.vis), format!("struct {}", item.ident)),
        Item::Trait(item) => (Some(&item.vis), format!("trait {}", item.ident)),
        Item::TraitAlias(item) => (Some(&item.vis), format!("trait {}", item.ident)),
        Item::Type(item) => (Some(&item.vis), format!("type {}", item.ident)),
        Item::Union(item) => (Some(&item.vis), format!("union {}", item.ident)),
        Item::Use(item) => {
            let colon = if item.leading_colon.is_some() {
                "::"
            } else {
                ""
            };
            let tree = match &item.tree {
                UseTree::Path(path) => format!("{}::…", path.ident),
                UseTree::Name(name) => name.ident.to_string(),
                UseTree::Rename(rename) => format!("{} as {}", rename.ident, rename.rename),
                UseTree::Glob(_) => "*".to_owned(),
                UseTree::Group(_) => "{…}".to_owned(),
            };
            (Some(&item.vis), format!("use {colon}{tree}"))
        }
        _ => (None, "…".to_owned()),
    };
    let vis = match vis {
        Some(Visibility::Public(_)) => "pub ".to_owned(),
        Some(Visibility::Restricted(restricted)) => format!(
            "pub({}{}) ",
            if restricted.in_token.is_some() {
                "in "
            } else {
                ""
            },
            path(&restricted.path)
        ),
        Some(Visibility::Inherited) | None => String::new(),
    };
    format!("{vis}{what}")
}
