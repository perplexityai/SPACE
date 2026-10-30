//! The paths a Rust file names into other crates, read from its tokens:
//! comments, string, character and byte literals are skipped, so text
//! inside them never matches; items under `#[cfg(test)]` are left out;
//! `use` trees are expanded, groups, `self`, globs, aliases and a leading
//! `::` included; expression and type paths are read where they start; and
//! the names `use` binds are followed through, in the scope they are
//! declared in, and from other files of the crate as what the crate root
//! binds, through `crate::`, or a parent file module, through `super::`
//! and `use super::*;`. [`reaches`] reads the paths into other crates'
//! modules for `facades`, and [`paths`] what a file uses through a
//! dependency, for the other layers checks' findings.

mod lex;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

#[cfg(test)]
use lex::Kind;
use lex::{Token, closers, lex};

/// A file's tokens, with what [`reaches`] needs to read them.
struct Tokens<'a> {
    tokens: Vec<Token<'a>>,
    close: Vec<usize>,
    /// Whether each token is in an item under `#[cfg(test)]`.
    test: Vec<bool>,
    /// The `{` of the innermost brace group around each token, `None` at
    /// the file's top level: the scope its names are looked up from.
    scope: Vec<Option<usize>>,
    /// The `{` of each inline module's body, which does not see the names
    /// the scopes around it bind.
    module_bodies: BTreeSet<usize>,
}

impl<'a> Tokens<'a> {
    fn new(text: &'a str) -> Self {
        let tokens = lex(text);
        let close = closers(&tokens);
        let mut scope = Vec::with_capacity(tokens.len());
        let mut module_bodies = BTreeSet::new();
        let mut open = Vec::new();
        for (index, token) in tokens.iter().enumerate() {
            if token.is("}") {
                open.pop();
            }
            scope.push(open.last().copied());
            if token.is("{") {
                open.push(index);
                let before = |back: usize| {
                    index
                        .checked_sub(back)
                        .and_then(|index| tokens.get(index))
                        .and_then(Token::ident)
                };
                if before(1).is_some() && before(2) == Some("mod") {
                    module_bodies.insert(index);
                }
            }
        }
        let mut file = Self {
            test: vec![false; tokens.len()],
            tokens,
            close,
            scope,
            module_bodies,
        };
        file.mark_tests();
        file
    }

    fn get(&self, index: usize) -> Option<&Token<'a>> {
        self.tokens.get(index)
    }

    fn is(&self, index: usize, punct: &str) -> bool {
        self.get(index).is_some_and(|token| token.is(punct))
    }

    fn ident(&self, index: usize) -> Option<&'a str> {
        self.get(index).and_then(Token::ident)
    }

    /// The index of the bracket that closes the one at `index`.
    fn closer(&self, index: usize) -> usize {
        self.close.get(index).copied().unwrap_or(index)
    }

    fn in_test(&self, index: usize) -> bool {
        self.test.get(index).copied().unwrap_or_default()
    }

    /// The scope of the token at `index`.
    fn scope(&self, index: usize) -> Option<usize> {
        self.scope.get(index).copied().flatten()
    }

    /// The scope whose names `scope` sees next: the one around a block;
    /// none around the file's top level or a module's body.
    fn parent(&self, scope: Option<usize>) -> Option<Option<usize>> {
        let brace = scope?;
        (!self.module_bodies.contains(&brace)).then(|| self.scope(brace))
    }

    /// The scope of the module `scope` is in: the module body or the file's
    /// top level around it, past its blocks.
    fn module_scope(&self, mut scope: Option<usize>) -> Option<usize> {
        while let Some(brace) = scope
            && !self.module_bodies.contains(&brace)
        {
            scope = self.scope(brace);
        }
        scope
    }

    /// The module `super` names from `module`.
    fn up(&self, module: Module) -> Module {
        match module {
            Module::Here(Some(brace)) => Module::Here(self.module_scope(self.scope(brace))),
            Module::Here(None) => Module::Up(0),
            Module::Up(ancestor) => Module::Up(ancestor + 1),
        }
    }

    /// Each module the file declares outside test code, `mod <name>;` or
    /// `mod <name> {…}`, with the scope it is declared in; and the names of
    /// those declared at its top level as `mod <name>;` without a
    /// `#[path]`, whose files are its children.
    fn modules(&self) -> (Vec<(Option<usize>, &'a str)>, BTreeSet<String>) {
        let mut declared = Vec::new();
        let mut children = BTreeSet::new();
        let mut path = false;
        let mut index = 0;
        while let Some(token) = self.get(index) {
            if token.is("#") && self.is(index + 1, "[") {
                let end = self.closer(index + 1);
                let attribute = self.tokens.get(index + 2..end).unwrap_or_default();
                path |= attribute.windows(2).any(
                    |pair| matches!(pair, [name, eq] if name.ident() == Some("path") && eq.is("=")),
                );
                index = end + 1;
                continue;
            }
            if token.ident() == Some("mod")
                && let Some(name) = self.ident(index + 1)
                && (self.is(index + 2, ";") || self.is(index + 2, "{"))
            {
                let scope = self.scope(index);
                if !self.in_test(index) {
                    declared.push((scope, name));
                }
                if scope.is_none() && self.is(index + 2, ";") && !path {
                    children.insert(name.to_owned());
                }
                path = false;
            } else if token.is(";") || token.is("{") || token.is("}") {
                path = false;
            }
            index += 1;
        }
        (declared, children)
    }

    /// Marks each item under a `cfg` that requires `test` as test code: an
    /// outer attribute's item, up to its `;`, or its first `{…}` group, or,
    /// for a field, an arm or a statement, its `,`; an inner attribute's
    /// whole module or file.
    fn mark_tests(&mut self) {
        let mut braces = Vec::new();
        let mut index = 0;
        while let Some(token) = self.get(index) {
            if token.is("{") {
                braces.push(index);
            } else if token.is("}") {
                braces.pop();
            }
            if !token.is("#") {
                index += 1;
                continue;
            }
            let inner = self.is(index + 1, "!");
            let open = index + 1 + usize::from(inner);
            if !self.is(open, "[") {
                index += 1;
                continue;
            }
            let end = self.closer(open);
            let attribute = self.tokens.get(open + 1..end).unwrap_or_default();
            if cfg_requires_test(attribute) {
                let range = match (inner, braces.last()) {
                    (false, _) => self.item(end + 1),
                    (true, Some(&brace)) => brace + 1..self.closer(brace),
                    (true, None) => 0..self.tokens.len(),
                };
                let end = range.end.min(self.test.len());
                if let Some(marked) = self.test.get_mut(range.start..end) {
                    marked.fill(true);
                }
            }
            index = end + 1;
        }
    }

    /// The tokens of the item, field, arm or statement at `start`, after
    /// its attributes.
    fn item(&self, mut start: usize) -> Range<usize> {
        while self.is(start, "#") && self.is(start + 1, "[") {
            start = self.closer(start + 1) + 1;
        }
        let mut first = start;
        if self.ident(first) == Some("pub") {
            first += 1;
            if self.is(first, "(") {
                first = self.closer(first) + 1;
            }
        }
        // An item ends at its body or `;`; a comma inside its generics does
        // not end it, nor one in a `let` statement's type, nor one inside a
        // field's type's `<…>`, which an `=>`, or an `=` outside it, ends the
        // search for: an arm's `<` is a comparison, and an `=` inside `<…>`
        // binds an associated type (`Iterator<Item = A, …>`).
        let keyword = self.ident(first);
        let is_item = keyword.is_some_and(|word| ITEM_KEYWORDS.contains(&word));
        let is_let = keyword == Some("let");
        let mut angles = Some(0usize);
        let mut index = start;
        while let Some(token) = self.get(index) {
            if token.is("{") && !is_let {
                return start..self.closer(index) + 1;
            }
            if token.is("(") || token.is("[") || token.is("{") {
                index = self.closer(index) + 1;
                continue;
            }
            let arrow = index
                .checked_sub(1)
                .is_some_and(|previous| self.is(previous, "-"));
            let ends_angles = angles == Some(0) || self.is(index + 1, ">");
            angles = match angles {
                _ if token.is("=") && ends_angles => None,
                Some(depth) if token.is("<") => Some(depth + 1),
                Some(depth) if token.is(">") && !arrow => Some(depth.saturating_sub(1)),
                depth => depth,
            };
            let in_angles = angles.is_some_and(|depth| depth > 0);
            if token.is(";") || (token.is(",") && !is_item && !is_let && !in_angles) {
                return start..index + 1;
            }
            if token.is(")") || token.is("]") || token.is("}") {
                return start..index;
            }
            index += 1;
        }
        start..self.tokens.len()
    }

    /// The use tree at `index`, below `prefix`, appended to `out` as one
    /// leaf per path it names; returns the index after it.
    fn use_tree(&self, mut index: usize, prefix: &[&'a str], out: &mut Vec<UseLeaf<'a>>) -> usize {
        let mut segments = prefix.to_vec();
        if self.is(index, "::") {
            index += 1;
        }
        while let Some(token) = self.get(index) {
            if token.is("{") {
                let end = self.closer(index);
                let mut item = index + 1;
                while item < end {
                    let next = self.use_tree(item, &segments, out);
                    item = if self.is(next, ",") {
                        next + 1
                    } else {
                        next.max(item + 1)
                    };
                }
                return end + 1;
            }
            if token.is("*") {
                out.push(UseLeaf {
                    segments,
                    glob: true,
                    line: token.line,
                    ..UseLeaf::default()
                });
                return index + 1;
            }
            let Some(name) = token.ident() else {
                return index;
            };
            segments.push(name);
            index += 1;
            if self.is(index, "::") {
                index += 1;
                continue;
            }
            let mut alias = None;
            if self.ident(index) == Some("as") {
                alias = self.ident(index + 1);
                index += 2;
            }
            out.push(UseLeaf {
                segments,
                alias,
                line: token.line,
                ..UseLeaf::default()
            });
            return index;
        }
        index
    }

    /// Whether the identifier at `index` starts a path: it is not a field
    /// or method after `.`, a macro variable after `$`, or a segment after
    /// another path's `::`, such as `crate::x`, `a::x` or `<T>::x` for `x`.
    /// A leading `::`, as in `(::x::y)` or `-> ::x::Y`, starts one, and so
    /// does `..`, as in `..x::Y::default()`.
    fn starts_path(&self, index: usize) -> bool {
        let before = |back: usize| index.checked_sub(back).and_then(|index| self.get(index));
        let Some(previous) = before(1) else {
            return true;
        };
        if previous.is(".") {
            return before(2).is_some_and(|token| token.is("."));
        }
        if previous.is("$") {
            return false;
        }
        if !previous.is("::") {
            return true;
        }
        match before(2) {
            None => true,
            Some(token) if token.is(">") => {
                before(3).is_some_and(|token| token.is("-") || token.is("="))
            }
            Some(token) => token
                .ident()
                .is_none_or(|word| KEYWORDS_BEFORE_A_PATH.contains(&word)),
        }
    }

    /// The identifier at `index` and each `::<identifier>` after it.
    fn path_at(&self, index: usize) -> Vec<&'a str> {
        let Some(head) = self.ident(index) else {
            return Vec::new();
        };
        let mut segments = vec![head];
        let mut next = index + 1;
        while self.is(next, "::")
            && let Some(segment) = self.ident(next + 1)
        {
            segments.push(segment);
            next += 2;
        }
        segments
    }
}

/// The keywords an item starts with after its visibility, for
/// [`Tokens::item`].
const ITEM_KEYWORDS: &[&str] = &[
    "async",
    "const",
    "enum",
    "extern",
    "fn",
    "impl",
    "macro_rules",
    "mod",
    "static",
    "struct",
    "trait",
    "type",
    "union",
    "unsafe",
    "use",
];

/// The keywords a path with a leading `::` may follow.
const KEYWORDS_BEFORE_A_PATH: &[&str] = &[
    "as", "async", "await", "break", "const", "dyn", "else", "enum", "fn", "for", "if", "impl",
    "in", "let", "match", "mod", "move", "mut", "pub", "ref", "return", "static", "struct", "type",
    "unsafe", "use", "where", "while",
];

/// Whether the attribute `tokens` (inside `#[…]`) is a `cfg` that holds
/// only in tests: `cfg(test)`, or an `all(…)` with such a predicate.
/// `cfg(any(test, …))` and `cfg(not(test))` do not.
fn cfg_requires_test(tokens: &[Token<'_>]) -> bool {
    match tokens {
        [cfg, open, predicate @ .., close]
            if cfg.ident() == Some("cfg") && open.is("(") && close.is(")") =>
        {
            requires_test(predicate)
        }
        _ => false,
    }
}

fn requires_test(predicate: &[Token<'_>]) -> bool {
    match predicate {
        [only] => only.ident() == Some("test"),
        [all, open, arguments @ .., close]
            if all.ident() == Some("all") && open.is("(") && close.is(")") =>
        {
            let mut depth = 0usize;
            let mut start = 0;
            let mut found = false;
            for (index, token) in arguments.iter().enumerate() {
                if token.is("(") {
                    depth += 1;
                } else if token.is(")") {
                    depth = depth.saturating_sub(1);
                } else if token.is(",") && depth == 0 {
                    found |= requires_test(arguments.get(start..index).unwrap_or_default());
                    start = index + 1;
                }
            }
            found || requires_test(arguments.get(start..).unwrap_or_default())
        }
        _ => false,
    }
}

/// One path a `use` declaration or an `extern crate` names, without a
/// leading `::`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UseLeaf<'a> {
    segments: Vec<&'a str>,
    alias: Option<&'a str>,
    /// `…::*`.
    glob: bool,
    /// Whether the path starts with `::`, which names a crate, as an
    /// `extern crate` does.
    absolute: bool,
    extern_crate: bool,
    /// The line of its last segment.
    line: usize,
}

impl<'a> UseLeaf<'a> {
    /// Its segments without a trailing `self`.
    fn path(&self) -> &[&'a str] {
        match self.segments.split_last() {
            Some((&"self", path)) => path,
            _ => &self.segments,
        }
    }

    /// The name it binds, if any: its alias, or its last segment.
    fn name(&self) -> Option<&'a str> {
        if self.glob {
            return None;
        }
        self.alias
            .or_else(|| self.path().last().copied())
            .filter(|name| *name != "_")
    }
}

/// A crate other crates may reach into, as [`reaches`] reads paths into it.
#[derive(Debug, Clone, Copy)]
pub(super) struct Target<'t> {
    pub package: &'t str,
    /// The `pub mod`s at its crate root.
    pub modules: &'t BTreeSet<String>,
}

/// A target and a path inside it, empty for its crate root.
type Inside<'t> = (Target<'t>, Vec<String>);

/// The crate root's module path, as [`File::module`] gives it.
pub(super) const ROOT: &str = "crate";

/// A name a file module binds at its top level: the module's path, as
/// [`File::module`], and the name.
pub(super) type Key = (String, String);

/// What a name a file module binds at its top level is bound to, as the
/// crate's other files see it, through `crate::` or `super::`.
#[derive(Debug, Clone)]
pub(super) struct Exported<'t> {
    /// `None` for anything but a target or a path inside one, which hides a
    /// target of the same name from a `use super::*;`.
    to: Option<Inside<'t>>,
    /// The name, and each name it binds again, in that module or another:
    /// what a path through it goes on through.
    keys: Vec<Key>,
}

/// What a file module binds at its top level, by name.
pub(super) type Exports<'t> = BTreeMap<String, Exported<'t>>;

/// The file module [`reaches`] reads.
#[derive(Debug, Clone, Copy)]
pub(super) struct File<'m, 't> {
    /// Its module path, as `crate::a::b`, [`ROOT`] for the crate root, or
    /// anything else no other file of the crate has.
    pub module: &'m str,
    /// What its parent file module binds at its top level, then its
    /// grandparent, and so on, for its `super::` paths; as many as are
    /// known.
    pub ancestors: &'m [&'m Exports<'t>],
}

impl File<'_, '_> {
    /// A file whose ancestors are not known, outside the crate root.
    #[cfg(test)]
    pub fn other() -> Self {
        File {
            module: "crate::other",
            ancestors: &[],
        }
    }

    /// The crate root.
    #[cfg(test)]
    pub fn root() -> Self {
        File {
            module: ROOT,
            ancestors: &[],
        }
    }
}

/// A module a `super::` path names: one of the file's, by the scope its
/// names are bound in, or the file module's ancestor, 0 for its parent.
#[derive(Debug, Clone, Copy)]
enum Module {
    Here(Option<usize>),
    Up(usize),
}

/// What a name the file binds is bound to.
#[derive(Debug, Clone)]
struct Binding<'t> {
    /// `None` for anything but a target or a path inside one, which hides a
    /// target of the same name, and until the leaf is resolved.
    to: Option<Inside<'t>>,
    /// The use leaf that binds it, by its index: an import's, or a root
    /// glob's; `None` for a module the file declares.
    leaf: Option<usize>,
    /// The name a path through it goes on through.
    via: Option<Via>,
}

/// The names a crate's files may reach targets by.
#[derive(Debug, Clone, Default)]
pub(super) struct Names<'t> {
    /// Each name any file of the crate imports a target by: its
    /// dependency's, or an `extern crate … as` alias at the crate root.
    anchors: BTreeMap<String, Target<'t>>,
    /// What the crate root binds at its top level, for `crate::<name>…` in
    /// any of its files.
    root: Exports<'t>,
}

impl<'t> Names<'t> {
    /// `anchors`, the names a crate's dependencies give their targets, and
    /// what its root file, `root`, binds at its top level. A crate with a
    /// library and binaries is read through the library's root only.
    pub fn new(anchors: BTreeMap<String, Target<'t>>, root: Option<&str>) -> Self {
        let mut names = Self {
            anchors,
            root: Exports::new(),
        };
        let Some(text) = root.filter(|_| !names.is_empty()) else {
            return names;
        };
        let file = Tokens::new(text);
        let module = File {
            module: ROOT,
            ancestors: &[],
        };
        let scan = Scan::new(&file, &names, module);
        let mut anchors = names.anchors.clone();
        for (used, resolved) in scan.uses.iter().zip(&scan.resolved) {
            if let (None, true, Some(alias), Some((target, path))) = (
                used.scope,
                used.leaf.extern_crate,
                used.leaf.alias,
                resolved,
            ) && path.is_empty()
            {
                anchors.insert(alias.to_owned(), *target);
            }
        }
        let root = scan.exports();
        names.anchors = anchors;
        names.root = root;
        names
    }

    /// Whether the crate imports no target, so that no file of it reaches
    /// one.
    pub fn is_empty(&self) -> bool {
        self.anchors.is_empty()
    }
}

/// A use leaf, with the scope it is declared in.
struct Use<'a> {
    leaf: UseLeaf<'a>,
    scope: Option<usize>,
}

/// The name a path goes through to reach a target.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Via {
    /// A use leaf's, by its index, and the name it binds that the path
    /// starts with.
    Leaf(usize, String),
    /// Names other file modules bind: the crate root, as `crate::<name>`,
    /// or an ancestor, as `super::<name>`; each as [`Exported::keys`].
    Names(Vec<Key>),
}

/// A path resolved to a target.
struct Resolved<'t> {
    inside: Inside<'t>,
    /// The name it goes through, unless it is a target's.
    through: Option<Via>,
    /// The number of segments after the name it starts with.
    rest: usize,
}

/// A file's `use` declarations and `extern crate`s, and what each name
/// they bind in each scope is bound to. Bindings are order-independent, as
/// in Rust: each leaf is resolved once the names its path starts with are.
struct Scan<'f, 'a, 't> {
    file: &'f Tokens<'a>,
    names: &'f Names<'t>,
    module: File<'f, 't>,
    uses: Vec<Use<'a>>,
    /// Whether each token is in a `use` declaration or an `extern crate`.
    in_use: Vec<bool>,
    /// The names each scope binds, by its imports, its root globs and its
    /// `mod`s.
    bindings: BTreeMap<Option<usize>, BTreeMap<String, Binding<'t>>>,
    /// The modules each scope's `use super::*;` globs, whose names it sees
    /// after its own.
    globs: BTreeMap<Option<usize>, Vec<Module>>,
    /// The modules declared at the top level as files, by name.
    children: BTreeSet<String>,
    /// What each use leaf names, once resolved.
    resolved: Vec<Option<Inside<'t>>>,
    /// Whether a path goes on through each use leaf's name, as `m::X` does
    /// through `use lib::m;`.
    through: Vec<bool>,
    /// For each use leaf that only binds another name again, as `use
    /// crate::m;` or `use m as n;` do, that name, which a path through the
    /// leaf goes on through too.
    reimports: Vec<Option<Via>>,
    /// The names other file modules bind that a path goes on through, here
    /// or through a leaf resolved through one.
    through_names: BTreeSet<Key>,
}

impl<'f, 'a, 't> Scan<'f, 'a, 't> {
    fn new(file: &'f Tokens<'a>, names: &'f Names<'t>, module: File<'f, 't>) -> Self {
        let mut uses = Vec::new();
        let mut in_use = vec![false; file.tokens.len()];
        let mut index = 0;
        while let Some(token) = file.get(index) {
            if file.in_test(index) || !file.starts_path(index) {
                index += 1;
                continue;
            }
            let scope = file.scope(index);
            let mut leaves = Vec::new();
            let end = match token.ident() {
                Some("use") => {
                    let end = file.use_tree(index + 1, &[], &mut leaves);
                    let absolute = file.is(index + 1, "::");
                    for leaf in &mut leaves {
                        leaf.absolute = absolute;
                    }
                    end
                }
                Some("extern") if file.ident(index + 1) == Some("crate") => {
                    let name = file.ident(index + 2);
                    let alias = (file.ident(index + 3) == Some("as"))
                        .then(|| file.ident(index + 4))
                        .flatten();
                    leaves.push(UseLeaf {
                        segments: name.into_iter().collect(),
                        alias,
                        absolute: true,
                        extern_crate: true,
                        line: token.line,
                        ..UseLeaf::default()
                    });
                    index + if alias.is_some() { 5 } else { 3 }
                }
                _ => {
                    index += 1;
                    continue;
                }
            };
            if let Some(used) = in_use.get_mut(index..end.min(file.tokens.len())) {
                used.fill(true);
            }
            uses.extend(leaves.into_iter().map(|leaf| Use { leaf, scope }));
            index = end.max(index + 1);
        }
        let (declared, children) = file.modules();
        let mut scan = Self {
            file,
            names,
            module,
            resolved: vec![None; uses.len()],
            through: vec![false; uses.len()],
            reimports: vec![None; uses.len()],
            through_names: BTreeSet::new(),
            uses,
            in_use,
            bindings: BTreeMap::new(),
            globs: BTreeMap::new(),
            children,
        };
        // Each name is bound, to nothing until its leaf is resolved, so
        // that it hides a target of the same name; so is each module the
        // file declares outside test code, which a glob does not bind again.
        for (leaf, used) in scan.uses.iter().enumerate() {
            let path = used.leaf.path();
            if let Some(name) = used.leaf.name() {
                scan.bindings
                    .entry(used.scope)
                    .or_default()
                    .entry(name.to_owned())
                    .or_insert(Binding {
                        to: None,
                        leaf: Some(leaf),
                        via: Some(Via::Leaf(leaf, name.to_owned())),
                    });
            } else if used.leaf.glob
                && !path.is_empty()
                && path.iter().all(|segment| *segment == "super")
            {
                let mut source = Module::Here(file.module_scope(used.scope));
                for _ in path {
                    source = file.up(source);
                }
                scan.globs.entry(used.scope).or_default().push(source);
            }
        }
        for (scope, name) in declared {
            scan.bindings
                .entry(scope)
                .or_default()
                .entry(name.to_owned())
                .or_insert(Binding {
                    to: None,
                    leaf: None,
                    via: None,
                });
        }
        scan.resolve_uses();
        scan
    }

    /// Resolves every leaf that can be, until none more can.
    fn resolve_uses(&mut self) {
        loop {
            let mut changed = false;
            for leaf in 0..self.uses.len() {
                if self.resolved.get(leaf).is_some_and(Option::is_some) {
                    continue;
                }
                let Some(used) = self.uses.get(leaf) else {
                    continue;
                };
                let (scope, glob) = (used.scope, used.leaf.glob);
                let path = used.leaf.path();
                let Some(resolved) = self.resolve(scope, path, used.leaf.absolute, Some(leaf))
                else {
                    continue;
                };
                changed = true;
                self.mark_through(&resolved, glob);
                if resolved.rest == 0
                    && !glob
                    && let Some(reimport) = self.reimports.get_mut(leaf)
                {
                    *reimport = resolved.through.clone();
                }
                let (target, path) = resolved.inside;
                let names = self.bindings.entry(scope).or_default();
                if glob {
                    // A glob of a root brings its modules into scope, unless
                    // another import or a `mod` binds the name.
                    if path.is_empty() {
                        for module in target.modules {
                            names.entry(module.clone()).or_insert(Binding {
                                to: Some((target, vec![module.clone()])),
                                leaf: Some(leaf),
                                via: Some(Via::Leaf(leaf, module.clone())),
                            });
                        }
                    }
                } else if let Some(name) = self.uses.get(leaf).and_then(|used| used.leaf.name())
                    && let Some(binding) = names.get_mut(name)
                    && binding.leaf == Some(leaf)
                {
                    binding.to = Some((target, path.clone()));
                }
                if let Some(resolved) = self.resolved.get_mut(leaf) {
                    *resolved = Some((target, path));
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// Marks the name a path goes on through, if it goes on, or is a glob,
    /// and each name that one binds again.
    fn mark_through(&mut self, resolved: &Resolved<'_>, glob: bool) {
        if resolved.rest == 0 && !glob {
            return;
        }
        let mut via = resolved.through.clone();
        // Each step marks a leaf, so a cycle ends once each is marked.
        for _ in 0..=self.uses.len() {
            match via {
                None => return,
                Some(Via::Names(keys)) => {
                    self.through_names.extend(keys);
                    return;
                }
                Some(Via::Leaf(leaf, _)) => {
                    if let Some(through) = self.through.get_mut(leaf) {
                        *through = true;
                    }
                    via = self.reimports.get(leaf).cloned().flatten();
                }
            }
        }
    }

    /// What the file binds at its top level, itself or by a `use super::*;`,
    /// as [`Exports`].
    fn exports(&self) -> Exports<'t> {
        let mut exports = Exports::new();
        for (name, binding) in self.bindings.get(&None).into_iter().flatten() {
            let mut keys = vec![(self.module.module.to_owned(), name.clone())];
            let mut via = binding.via.clone();
            for _ in 0..=self.uses.len() {
                match via {
                    None => break,
                    Some(Via::Names(names)) => {
                        keys.extend(names);
                        break;
                    }
                    Some(Via::Leaf(leaf, name)) => {
                        let key = (self.module.module.to_owned(), name);
                        if !keys.contains(&key) {
                            keys.push(key);
                        }
                        via = self.reimports.get(leaf).cloned().flatten();
                    }
                }
            }
            let to = binding.to.clone();
            exports.insert(name.clone(), Exported { to, keys });
        }
        for module in self.globs.get(&None).into_iter().flatten() {
            if let Module::Up(ancestor) = module
                && let Some(ancestor) = self.module.ancestors.get(*ancestor)
            {
                for (name, exported) in *ancestor {
                    exports
                        .entry(name.clone())
                        .or_insert_with(|| exported.clone());
                }
            }
        }
        exports
    }

    /// What `segments`, seen from `scope`, name inside a target: the first
    /// segment is a name bound there or around it, or a target's; `crate::x`
    /// is what the crate root binds `x` to, `self::x` what this module does,
    /// and `super::x` what the module around it does, inside the file or,
    /// past its top level, in its ancestors. `except`, a leaf being
    /// resolved, does not see its own name.
    fn resolve(
        &self,
        scope: Option<usize>,
        segments: &[&str],
        absolute: bool,
        except: Option<usize>,
    ) -> Option<Resolved<'t>> {
        let (head, rest) = segments.split_first()?;
        let ((target, mut path), through, rest) = match *head {
            _ if absolute => ((*self.names.anchors.get(*head)?, Vec::new()), None, rest),
            "crate" if self.module.module == ROOT => {
                let (name, rest) = rest.split_first()?;
                let (inside, through) = self.bound(Module::Here(None), name, except)?;
                (inside?, through, rest)
            }
            "crate" => {
                let (name, rest) = rest.split_first()?;
                let exported = self.names.root.get(*name)?;
                let through = Some(Via::Names(exported.keys.clone()));
                (exported.to.clone()?, through, rest)
            }
            "self" | "super" => {
                let mut module = Module::Here(self.file.module_scope(scope));
                let mut rest = if *head == "self" { rest } else { segments };
                while let Some((&"super", tail)) = rest.split_first() {
                    module = self.file.up(module);
                    rest = tail;
                }
                let (name, rest) = rest.split_first()?;
                let (inside, through) = self.bound(module, name, except)?;
                (inside?, through, rest)
            }
            _ => {
                let (inside, through) = self.lookup(scope, head, except, true)?;
                (inside, through, rest)
            }
        };
        path.extend(rest.iter().map(|segment| (*segment).to_owned()));
        Some(Resolved {
            inside: (target, path),
            through,
            rest: rest.len(),
        })
    }

    /// What `name` is bound to from `scope`: by the innermost scope around
    /// it that binds it, up to its module's body, or else, with `anchors`,
    /// as a target's name.
    fn lookup(
        &self,
        scope: Option<usize>,
        name: &str,
        except: Option<usize>,
        anchors: bool,
    ) -> Option<(Inside<'t>, Option<Via>)> {
        let mut scope = Some(scope);
        while let Some(current) = scope {
            if let Some((inside, through)) = self.bound(Module::Here(current), name, except) {
                return Some((inside?, through));
            }
            scope = self.file.parent(current);
        }
        let target = self.names.anchors.get(name).filter(|_| anchors)?;
        Some(((*target, Vec::new()), None))
    }

    /// Whether `module` binds `name`, itself or by its `use super::*;`, and
    /// if so to what, and through what.
    fn bound(
        &self,
        module: Module,
        name: &str,
        except: Option<usize>,
    ) -> Option<(Option<Inside<'t>>, Option<Via>)> {
        match module {
            Module::Up(ancestor) => {
                let exported = self.module.ancestors.get(ancestor)?.get(name)?;
                let through = Some(Via::Names(exported.keys.clone()));
                Some((exported.to.clone(), through))
            }
            Module::Here(scope) => {
                let binding = self
                    .bindings
                    .get(&scope)
                    .and_then(|names| names.get(name))
                    .filter(|binding| except.is_none() || binding.leaf != except);
                if let Some(binding) = binding {
                    return Some((binding.to.clone(), binding.via.clone()));
                }
                // A glob's source is around the scope, so this ends.
                self.globs
                    .get(&scope)
                    .into_iter()
                    .flatten()
                    .find_map(|source| self.bound(*source, name, None))
            }
        }
    }
}

/// A path into one of a target's modules: an item below one, or the module
/// itself, imported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Reach<'t> {
    pub package: &'t str,
    /// The path after the crate name, cut after its first segment that is
    /// not a module, such as `controllers::substrate::Substrate`; a module
    /// imported is its own path (`launch`), and a glob ends in `*`.
    pub item: String,
    /// The 1-based line that names it.
    pub line: usize,
    /// For an import at its file's top level that counts because no path
    /// of its file goes on through it, the names it binds there: if a path
    /// of another file goes on through one, as `crate::<name>::…` or
    /// `super::<name>::…`, the import does not count.
    pub unless: Vec<Key>,
}

/// What [`reaches`] reads from a file.
#[derive(Debug, Default)]
pub(super) struct Scanned<'t> {
    pub reaches: Vec<Reach<'t>>,
    /// The names other file modules bind that a path of the file goes on
    /// through.
    pub through: BTreeSet<Key>,
    /// What the file binds at its top level, for its children's `super::`
    /// paths.
    pub exports: Exports<'t>,
    /// The modules it declares at its top level as files, `mod <name>;`
    /// without a `#[path]`, by name.
    pub children: BTreeSet<String>,
}

/// Whether `segment` names a module, by Rust's naming conventions: snake
/// case, unlike a type, trait, variant or constant.
fn is_module_like(segment: &str) -> bool {
    segment
        .chars()
        .next()
        .is_some_and(|c| c == '_' || c.is_lowercase())
        && !matches!(segment, "crate" | "self" | "super")
}

/// `segments` cut after the first that is not a module, or after the last.
fn item_of(segments: &[String]) -> String {
    let end = segments
        .iter()
        .position(|segment| !is_module_like(segment))
        .map_or(segments.len(), |index| index + 1);
    segments.get(..end).unwrap_or_default().join("::")
}

/// The item a path inside a target names, if it is inside a module: its
/// first segment is snake case and another follows.
fn module_item(path: &[String]) -> Option<String> {
    match path {
        [first, _, ..] if is_module_like(first) => Some(item_of(path)),
        _ => None,
    }
}

/// Every path `text`, a production file, names into a module of a target,
/// in order, by the names of `names` and those the file binds, in the file
/// module `file`. Code under `#[cfg(test)]` is left out.
///
/// A name is bound by a `use` or an `extern crate` for the block or module
/// it is declared in, and blocks inside it, as in Rust; a glob of a root
/// binds each of its `pub mod`s, and `use super::*;` each name the module
/// around it binds, in the file or, at its top level, in its parent file
/// module, as [`File::ancestors`] has them. A `mod` the file declares hides
/// a target of its name. A path reaches a module when, with the path its
/// first segment is bound to in place of it, the path inside the target
/// starts with a snake-case segment and has another after it
/// (`lib::m::Item`, or `m::Item` after `use lib::m;`). Each path is keyed
/// by the item it names inside the target, however it is written.
///
/// An import counts as the item it names, `use lib::m::Item;` as
/// `m::Item`; one of a module only if no path goes on through its name:
/// `use lib::m::n;` as `m::n`, and `use lib::m;` or `use lib::m::{self}` as
/// `m` when `m` is a module, as a root `pub mod` or by the `self`. One that
/// binds another name again, as `use crate::m;` or `use m as n;` do, is not
/// counted itself, and a path through it goes on through that name. A glob
/// of a module counts as `m::*`, and one of the root as `*` if the target
/// has modules and no path goes on through one it binds. Root items
/// (`lib::Item`, `lib::Enum::Variant`, `lib::function`, `lib::f::<T>()`)
/// are not reaches. An import at the file's top level is gone on through by
/// the crate's other files too, as `crate::<name>` from any of them in the
/// crate root, or as `super::<name>` from its children, which
/// [`Reach::unless`] leaves to the caller.
pub(super) fn reaches<'t>(text: &str, names: &Names<'t>, module: File<'_, 't>) -> Scanned<'t> {
    let file = Tokens::new(text);
    let mut scan = Scan::new(&file, names, module);
    let mut found = Vec::new();
    for index in 0..file.tokens.len() {
        let used = scan.in_use.get(index).copied().unwrap_or_default();
        if used || file.in_test(index) || !file.starts_path(index) {
            continue;
        }
        let segments = file.path_at(index);
        if segments.len() < 2 {
            continue;
        }
        let absolute = index
            .checked_sub(1)
            .is_some_and(|previous| file.is(previous, "::"));
        let Some(resolved) = scan.resolve(file.scope(index), &segments, absolute, None) else {
            continue;
        };
        scan.mark_through(&resolved, false);
        let (target, path) = &resolved.inside;
        if resolved.rest > 0
            && let Some(item) = module_item(path)
        {
            found.push(Reach {
                package: target.package,
                item,
                line: file.get(index).map_or(1, |token| token.line),
                unless: Vec::new(),
            });
        }
    }
    let key = |name: &str| (module.module.to_owned(), name.to_owned());
    let top_level = scan.bindings.get(&None);
    let leaves = scan.uses.iter().zip(&scan.resolved).zip(&scan.through);
    for (index, (((used, resolved), through), reimport)) in leaves.zip(&scan.reimports).enumerate()
    {
        let Some((target, path)) = resolved else {
            continue;
        };
        let leaf = &used.leaf;
        let module_itself = leaf.segments.last() == Some(&"self");
        let whole = path.iter().all(|segment| is_module_like(segment));
        let item = match path.as_slice() {
            [] if leaf.glob => (!target.modules.is_empty() && !through).then(|| "*".to_owned()),
            [] => None,
            [first, ..] if !is_module_like(first) => None,
            _ if leaf.glob && whole => Some(format!("{}::*", path.join("::"))),
            _ if !whole => Some(item_of(path)),
            // An import alone counts once: a leaf that binds another name
            // again leaves it to that one's import.
            _ if *through || reimport.is_some() => None,
            [_, _, ..] => Some(path.join("::")),
            [module] => (module_itself || target.modules.contains(module)).then(|| module.clone()),
        };
        let Some(item) = item else {
            continue;
        };
        // Only an import counted for want of a path through it can be gone
        // through from elsewhere: a root glob by the names it bound itself.
        let unless = match (used.scope, leaf.name()) {
            (Some(_), _) => Vec::new(),
            (None, _) if leaf.glob => top_level
                .into_iter()
                .flatten()
                .filter(|(_, binding)| binding.leaf == Some(index))
                .map(|(name, _)| key(name))
                .collect(),
            (None, Some(name)) if whole => vec![key(name)],
            (None, _) => Vec::new(),
        };
        found.push(Reach {
            package: target.package,
            item,
            line: leaf.line,
            unless,
        });
    }
    found.sort_by_key(|reach| reach.line);
    Scanned {
        reaches: found,
        exports: scan.exports(),
        through: scan.through_names,
        children: scan.children,
    }
}

/// Every path `text` names that starts with `name`, outside
/// `#[cfg(test)]`, as `name::a::B`: each leaf of a `use` tree, without a
/// trailing `self` or `*`, and each other path where it starts, up to its
/// last segment (`name::T` for `name::T::<u8>::new()`). The name alone, as
/// in `use name;` or `use name::*;`, is not one, nor a path after another
/// path's `::`.
pub(super) fn paths(text: &str, name: &str) -> Vec<String> {
    let file = Tokens::new(text);
    let names = Names::default();
    let module = File {
        module: "",
        ancestors: &[],
    };
    let scan = Scan::new(&file, &names, module);
    let mut paths: Vec<String> = scan
        .uses
        .iter()
        .map(|used| used.leaf.path())
        .filter(|path| path.len() > 1 && path.first() == Some(&name))
        .map(|path| path.join("::"))
        .collect();
    for index in 0..file.tokens.len() {
        let used = scan.in_use.get(index).copied().unwrap_or_default();
        if used || file.in_test(index) || file.ident(index) != Some(name) {
            continue;
        }
        let segments = file.path_at(index);
        if segments.len() > 1 && file.starts_path(index) {
            paths.push(segments.join("::"));
        }
    }
    paths
}

/// The `pub mod`s at the root of a crate whose root file is `text`, outside
/// `#[cfg(test)]`. A module that is `pub use`d from elsewhere is not one.
pub(super) fn root_modules(text: &str) -> BTreeSet<String> {
    let file = Tokens::new(text);
    let mut modules = BTreeSet::new();
    let mut depth = 0usize;
    for (index, token) in file.tokens.iter().enumerate() {
        if token.is("{") {
            depth += 1;
        } else if token.is("}") {
            depth = depth.saturating_sub(1);
        }
        let public = depth == 0 && !file.in_test(index) && token.ident() == Some("pub");
        if public
            && file.ident(index + 1) == Some("mod")
            && let Some(name) = file.ident(index + 2)
        {
            modules.insert(name.to_owned());
        }
    }
    modules
}

#[cfg(test)]
fn tokens(text: &str) -> Vec<(Kind, &str, usize)> {
    lex(text)
        .into_iter()
        .map(|token| (token.kind, token.text, token.line))
        .collect()
}
