//! The scanner's pieces that the command-line tests reach only through
//! whole workspaces: the lexer, what counts as a path into a module, the
//! scope of a name, test code, and a crate root's modules.

use std::collections::{BTreeMap, BTreeSet};

use super::{File, Key, Kind, Names, Scanned, Target, paths, reaches, root_modules, tokens};

/// Comments, literals and lifetimes never lex as identifiers, and each
/// token keeps the line it starts on.
#[test]
fn the_lexer() {
    let text = "\u{feff}a // lib::m::X\n/* lib /* nested */ m::X */ b\n\
                \"lib::m::X\" r\"lib\" r#\"a \"quoted\" lib\"# br##\"x\"## b\"y\" c\"z\"\n\
                'c' '\\'' '\\u{1F600}' b'q' 'é' 'life &'static loop_label: r#type x::y 1.5e3 \
                \"multi\nline\" after";
    let found = tokens(text);
    let idents: Vec<(&str, usize)> = found
        .iter()
        .filter(|(kind, _, _)| *kind == Kind::Ident)
        .map(|(_, text, line)| (*text, *line))
        .collect();
    assert_eq!(
        idents,
        [
            ("a", 1),
            ("b", 2),
            ("loop_label", 4),
            ("type", 4),
            ("x", 4),
            ("y", 4),
            ("after", 5),
        ]
    );
    let literals = found
        .iter()
        .filter(|(kind, _, _)| *kind == Kind::Literal)
        .count();
    // Six strings on line 3, four characters and a byte, two numbers and
    // the multi-line string.
    assert_eq!(literals, 6 + 5 + 2 + 1, "{found:#?}");
    let lifetimes: Vec<&str> = found
        .iter()
        .filter(|(kind, _, _)| *kind == Kind::Lifetime)
        .map(|(_, text, _)| *text)
        .collect();
    assert_eq!(lifetimes, ["'life", "'static"]);
    assert!(found.contains(&(Kind::Punct, "::", 4)), "{found:#?}");
    // An unterminated literal or comment ends the file without a panic.
    for text in [
        "\"open",
        "r#\"open",
        "/* open",
        "'",
        "b'",
        "x::",
        "#[cfg(test)",
    ] {
        tokens(text);
        reaches(text, &Names::default(), File::root());
        paths(text, "x");
    }
}

/// The items `text` reaches in a target imported as `lib`, whose root
/// declares `pub mod root_mod` and `pub mod other_mod`, in a crate whose
/// root file is `root`.
fn reached_in(root: Option<&str>, text: &str) -> Vec<(String, usize)> {
    let modules = BTreeSet::from(["root_mod".to_owned(), "other_mod".to_owned()]);
    let anchors = BTreeMap::from([(
        "lib".to_owned(),
        Target {
            package: "pplx-space-lib",
            modules: &modules,
        },
    )]);
    reaches(text, &Names::new(anchors, root), File::other())
        .reaches
        .into_iter()
        .map(|reach| {
            assert_eq!(reach.package, "pplx-space-lib");
            (reach.item, reach.line)
        })
        .collect()
}

/// [`reached_in`] a file of a crate whose root binds nothing.
fn reached(text: &str) -> Vec<(String, usize)> {
    reached_in(None, text)
}

/// The items alone of [`reached_in`].
fn items_in(root: Option<&str>, text: &str) -> Vec<String> {
    reached_in(root, text)
        .into_iter()
        .map(|(item, _)| item)
        .collect()
}

#[test]
fn paths_into_modules() {
    let cases: &[(&str, &[&str])] = &[
        // Use trees, of every form.
        ("use lib::m::Item;", &["m::Item"]),
        ("use ::lib::m::Item;", &["m::Item"]),
        ("pub(crate) use lib::m::n::Item as Other;", &["m::n::Item"]),
        ("use lib::m::Enum::Variant;", &["m::Enum"]),
        ("use lib::m::function;", &["m::function"]),
        (
            "use lib::{\n    m::{A, n::B},\n    Root,\n    o::*,\n};",
            &["m::A", "m::n::B", "o::*"],
        ),
        ("use lib::m::Enum::*;", &["m::Enum"]),
        // A module imported that no path goes on through counts as itself,
        // when it is one: a root `pub mod`, or by its `self`.
        ("use lib::{m::{self, A}};", &["m", "m::A"]),
        ("use lib::m::{self as alias};", &["m"]),
        ("use lib::root_mod;", &["root_mod"]),
        ("use lib::{root_mod, function};", &["root_mod"]),
        ("use lib::m::n;", &["m::n"]),
        // A path through an imported module, by its name or an alias,
        // counts as the item it reaches, as the path in full would, and the
        // import does not.
        (
            "use lib::m;\nfn f() { m::X::new(); m::n::Y; }",
            &["m::X", "m::n::Y"],
        ),
        (
            "use lib::reexported as r;\nfn f() { r::run(); }",
            &["reexported::run"],
        ),
        (
            "use lib::{self, m::{self, n}};\nfn f() -> n::X { m::Y }",
            &["m::n::X", "m::Y"],
        ),
        ("use lib::root_mod;\nuse root_mod::X;", &["root_mod::X"]),
        ("use lib::m;\nuse m::n;\nfn f() { n::X; }", &["m::n::X"]),
        ("use lib::m;\nuse m as k;\nfn f() { k::X; }", &["m::X"]),
        ("use lib::root_mod;\nuse root_mod as k;", &["root_mod"]),
        ("use lib::m;\nfn f() { self::m::X; }", &["m::X"]),
        // A glob of the root brings its modules into scope: it counts once
        // if no path goes on through one, and an explicit import of the
        // name hides the glob's.
        ("pub use lib::*;", &["*"]),
        (
            "use lib::*;\nfn f() { root_mod::X; other_mod::n::Y; m::Z; }",
            &["root_mod::X", "other_mod::n::Y"],
        ),
        (
            "use lib::*;\nuse other::root_mod;\nfn f() { root_mod::X; }",
            &["*"],
        ),
        // So does a module the file declares, and it hides a target too.
        (
            "use lib::*;\nmod root_mod;\nfn f() { root_mod::X; }",
            &["*"],
        ),
        (
            "use lib::*;\nfn f() { other_mod::A; }\npub(crate) mod other_mod {}",
            &["*"],
        ),
        ("mod lib;\nfn f() { lib::m::A; }", &[]),
        // One declared only for tests hides neither: production code does
        // not see it.
        (
            "use lib::*;\n#[cfg(test)]\nmod root_mod;\nfn f() { root_mod::X; }",
            &["root_mod::X"],
        ),
        (
            "#[cfg(all(test, unix))]\nmod lib;\nfn f() { lib::m::A; }",
            &["m::A"],
        ),
        // Expression and type paths.
        (
            "fn f() -> lib::m::Item { lib::m::Item::new() }",
            &["m::Item", "m::Item"],
        ),
        ("let x = ::lib::m::f(1);", &["m::f"]),
        ("let x = lib::m::f::<u8>();", &["m::f"]),
        (
            "let x: Vec<lib::m::T> = lib::m::T::<u8>::new();",
            &["m::T", "m::T"],
        ),
        ("#[source] error: lib::m::Error,", &["m::Error"]),
        ("let s = S { a: 1, ..lib::m::S::default() };", &["m::S"]),
        ("let r = 0..lib::m::MAX;", &["m::MAX"]),
        ("fn f() -> ::lib::m::X { todo!() }", &["m::X"]),
        ("match x { _ => ::lib::m::Y }", &["m::Y"]),
        ("lib::m::mac!(x);", &["m::mac"]),
        ("call!(lib::proto::Request);", &["proto::Request"]),
        // Aliases of the crate, declared before or after their use.
        ("use lib as alias;\nuse alias::m::X;", &["m::X"]),
        (
            "fn f() -> other::m::X {}\nuse lib::{self as other};",
            &["m::X"],
        ),
        ("extern crate lib as ext;\nfn f(_: ext::m::X) {}", &["m::X"]),
    ];
    for (text, expected) in cases {
        assert_eq!(items_in(None, text), *expected, "{text}");
    }
}

/// A name a `use` or an `extern crate` binds is seen in the block or
/// module it is declared in, before or after the declaration, and in the
/// blocks inside it, but not in a module inside it, which an inner
/// declaration of the name hides it from too.
#[test]
fn the_scope_of_a_name() {
    let cases: &[(&str, &[&str])] = &[
        (
            "fn f() { use lib as x; x::m::A; { x::m::B; } }\nfn g() { x::m::C; }",
            &["m::A", "m::B"],
        ),
        ("fn f() { x::m::A; use lib as x; }", &["m::A"]),
        ("mod inner { use lib as x; }\nfn g() { x::m::C; }", &[]),
        ("use lib as x;\nmod inner { fn f() { x::m::C; } }", &[]),
        ("use lib as x;\nfn f() { use other as x; x::m::A; }", &[]),
        // `self::` names the module's names, past the blocks around the
        // path.
        ("fn f() { use lib as x; self::x::m::A; }", &[]),
        (
            "use lib as x;\nfn f() { use other as x; { self::x::m::A; } }",
            &["m::A"],
        ),
        (
            "mod inner { use lib as x; fn f() { self::x::m::A; } }",
            &["m::A"],
        ),
        (
            "fn f() { extern crate lib as ext; ext::m::A; }\nfn g() { ext::m::B; }",
            &["m::A"],
        ),
        (
            "fn f() { use lib::root_mod; }\nfn g() { root_mod::X; }",
            &["root_mod"],
        ),
        // A target's own name is seen everywhere, unless a `use` binds the
        // name to something else.
        ("mod inner { fn f() { lib::m::A; } }", &["m::A"]),
        ("use other::lib;\nfn f() { lib::m::A; }", &[]),
    ];
    for (text, expected) in cases {
        assert_eq!(items_in(None, text), *expected, "{text}");
    }
}

/// What the crate root binds at its top level, to a target or a path
/// inside one, is reached through `crate::<name>` from any file of the
/// crate; an `extern crate … as` alias there is a name of the target in
/// every file.
#[test]
fn names_the_crate_root_binds() {
    let root = "pub use lib as api;\nuse lib::{m, root_mod};\nextern crate lib as ext;\n\
                fn f() { use lib as local; }\nmod inner;\nuse crate::api as again;\n\
                pub use crate::again::m as cm;\n";
    let cases: &[(&str, &[&str])] = &[
        ("use crate::api::m::X;", &["m::X"]),
        (
            "fn f() -> crate::api::m::Y { crate::m::Z }",
            &["m::Y", "m::Z"],
        ),
        ("use crate::api;\nfn f() { api::m::B; }", &["m::B"]),
        // Importing the root's module again is not a reach of its own.
        ("use crate::root_mod;", &[]),
        (
            "use crate::root_mod;\nfn f() { root_mod::C; }",
            &["root_mod::C"],
        ),
        ("fn f() { ext::m::A; }", &["m::A"]),
        // An alias the root binds through `crate::`, at any length.
        (
            "fn f() { crate::again::m::A; crate::cm::B; }",
            &["m::A", "m::B"],
        ),
        (
            "fn f() { api::m::A; crate::local::m::B; crate::inner::m::C; }",
            &[],
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(items_in(Some(root), text), *expected, "{text}");
    }
}

#[test]
fn root_items_and_other_text_are_not_reaches() {
    let cases = [
        "use lib::Root;",
        "use lib::{Root, Enum::Variant};",
        "use lib::function;",
        "use lib::Enum::*;",
        "use lib;",
        "use lib as alias;",
        "use lib::{self};",
        "use lib::Trait as _;",
        // A turbofish on a root function imported by name is no module.
        "use lib::f;\nfn g() { f::<u8>(); }",
        "fn f() { lib::function(); lib::Root::new(); lib::f::<T>(); lib::CONST; }",
        "lib::root_macro!();",
        // Another crate's module of the same name, or a field or method.
        "use other::lib::m::X;",
        "use crate::lib::m::X;",
        "fn f() { self::lib::m::X; x.lib::m(); <T>::lib::m::X; }",
        "macro_rules! m { ($lib:ident) => { $lib::m::X }; }",
        // Text that only names a path.
        "// lib::m::X\n/// lib::m::X\n/* lib::m::X */\nlet s = \"lib::m::X\";",
        "let s = r#\"lib::m::X\"#; let c = 'l'; let b = b\"lib::m::X\";",
        // A name used as a path that the file did not import from the
        // target, and a single-segment import that is not a module.
        "use lib::run;\nfn f() { other::run(); }",
    ];
    for text in cases {
        assert_eq!(reached(text), [], "{text}");
    }
}

/// The modules a target imported as `lib` declares at its root.
fn lib_modules() -> BTreeSet<String> {
    BTreeSet::from(["root_mod".to_owned(), "other_mod".to_owned()])
}

/// The names of a crate that imports a target as `lib`, whose modules are
/// `modules`, and whose root file is `root`.
fn names_of<'t>(modules: &'t BTreeSet<String>, root: Option<&str>) -> Names<'t> {
    let anchors = BTreeMap::from([(
        "lib".to_owned(),
        Target {
            package: "pplx-space-lib",
            modules,
        },
    )]);
    Names::new(anchors, root)
}

/// Each reach's item and the names that cancel it, as `module.name`.
fn with_unless(scanned: &Scanned<'_>) -> Vec<(String, Vec<String>)> {
    scanned
        .reaches
        .iter()
        .map(|reach| {
            let unless = reach
                .unless
                .iter()
                .map(|(module, name)| format!("{module}.{name}"))
                .collect();
            (reach.item.clone(), unless)
        })
        .collect()
}

/// The items of `reached` that the names `through` does not cancel.
fn kept(reached: &[(String, Vec<String>)], through: &BTreeSet<Key>) -> Vec<String> {
    reached
        .iter()
        .filter(|(_, unless)| {
            !unless.iter().any(|key| {
                key.split_once('.').is_some_and(|(module, name)| {
                    through.contains(&(module.to_owned(), name.to_owned()))
                })
            })
        })
        .map(|(item, _)| item.clone())
        .collect()
}

/// In the crate root, an import at the top level that its file does not go
/// on through counts, but names what it binds there, so that a path of
/// another file through `crate::<name>` can cancel it; each file says which
/// of those names it goes on through, itself or by a leaf. A root glob is
/// cancelled only by the names it bound itself, not one another import
/// binds.
#[test]
fn imports_at_the_crate_root() {
    let modules = lib_modules();
    let root = "pub use lib::{m::n, root_mod, o::Item};\npub use lib::*;\n\
                fn f() { use lib::other_mod; }\n";
    let names = names_of(&modules, Some(root));
    let in_root = with_unless(&reaches(root, &names, File::root()));
    let expected: &[(&str, &[&str])] = &[
        ("m::n", &["crate.n"]),
        ("root_mod", &["crate.root_mod"]),
        ("o::Item", &[]),
        ("*", &["crate.other_mod"]),
        ("other_mod", &[]),
    ];
    let expected: Vec<(String, Vec<String>)> = expected
        .iter()
        .map(|(item, unless)| {
            let unless = unless.iter().map(|key| (*key).to_owned()).collect();
            ((*item).to_owned(), unless)
        })
        .collect();
    assert_eq!(in_root, expected);
    let other = "use crate::n;\nfn f() { n::X; crate::root_mod::Y; crate::o::Z; }\n";
    let scanned = reaches(other, &names, File::other());
    let items: Vec<&str> = scanned
        .reaches
        .iter()
        .map(|reach| reach.item.as_str())
        .collect();
    assert_eq!(items, ["m::n::X", "root_mod::Y"]);
    assert!(scanned.reaches.iter().all(|reach| reach.unless.is_empty()));
    let through: Vec<String> = scanned
        .through
        .iter()
        .map(|(module, name)| format!("{module}.{name}"))
        .collect();
    assert_eq!(through, ["crate.n", "crate.root_mod"]);
    // The explicit `root_mod` import is gone through, but the glob is not.
    assert_eq!(
        kept(&in_root, &scanned.through),
        ["o::Item", "*", "other_mod"]
    );
    let glob_only = reaches("fn f() { crate::other_mod::W; }", &names, File::other());
    assert_eq!(
        kept(&in_root, &glob_only.through),
        ["m::n", "root_mod", "o::Item", "other_mod"]
    );
}

/// A child file module sees what its parent binds at its top level, as
/// `super::<name>` or by `use super::*;`, and its paths through those names
/// cancel the parent's import as `crate::<name>` paths do the root's; a
/// grandchild sees the grandparent through `super::super::`. In a file,
/// `super::` names the module around an inline one.
#[test]
fn super_paths() {
    let modules = lib_modules();
    let root = "pub use lib::root_mod as rm;\nmod service;\n";
    let names = names_of(&modules, Some(root));
    let root_scan = reaches(root, &names, File::root());
    let parent = "use lib::m::{self};\nuse crate::rm;\nuse lib::*;\nmod child;\n\
                  #[path = \"elsewhere.rs\"]\nmod moved;\nmod inline {}\n";
    let parent_file = File {
        module: "crate::service",
        ancestors: &[&root_scan.exports],
    };
    let parent_scan = reaches(parent, &names, parent_file);
    assert_eq!(
        with_unless(&parent_scan),
        [
            ("m".to_owned(), vec!["crate::service.m".to_owned()]),
            (
                "*".to_owned(),
                vec![
                    "crate::service.other_mod".to_owned(),
                    "crate::service.root_mod".to_owned()
                ]
            ),
        ]
    );
    assert_eq!(
        parent_scan.children.iter().collect::<Vec<_>>(),
        ["child"],
        "neither a #[path] module nor an inline one is a child file"
    );
    let ancestors = [&parent_scan.exports, &root_scan.exports];
    let child_file = File {
        module: "crate::service::child",
        ancestors: &ancestors,
    };
    let cases: &[(&str, &[&str], &[&str])] = &[
        (
            "use super::*;\nfn f() { m::A; rm::B; other_mod::C; }",
            &["m::A", "root_mod::B", "other_mod::C"],
            &[
                "crate.rm",
                "crate::service.m",
                "crate::service.other_mod",
                "crate::service.rm",
            ],
        ),
        (
            "fn f() { super::m::A; super::super::rm::B; }",
            &["m::A", "root_mod::B"],
            &["crate.rm", "crate::service.m"],
        ),
        ("use super::m::A;", &["m::A"], &["crate::service.m"]),
        // Only `super::` reaches the parent's names, not `crate::`.
        ("fn f() { crate::service::m::A; }", &[], &[]),
        // An explicit import hides the parent's name.
        ("use super::*;\nuse other::m;\nfn f() { m::A; }", &[], &[]),
        (
            "fn f() { super::child::m::A; super::inline::m::B; }",
            &[],
            &[],
        ),
        (
            "mod inner {\n    use super::super::m;\n    fn f() { m::A; super::super::rm::B; }\n}",
            &["m::A", "root_mod::B"],
            &["crate.rm", "crate::service.m", "crate::service.rm"],
        ),
    ];
    for (text, items, through) in cases {
        let scanned = reaches(text, &names, child_file);
        let found: Vec<&str> = scanned
            .reaches
            .iter()
            .map(|reach| reach.item.as_str())
            .collect();
        assert_eq!(found, *items, "{text}");
        let gone: Vec<String> = scanned
            .through
            .iter()
            .map(|(module, name)| format!("{module}.{name}"))
            .collect();
        assert_eq!(gone, *through, "{text}");
    }
    // A name the parent binds to something else, by a `mod` or an import,
    // hides a target of that name from its children's `use super::*;`.
    for parent in [
        "mod lib;\nmod child;\n",
        "use other::thing as lib;\nmod child;\n",
    ] {
        let parent_scan = reaches(parent, &names, parent_file);
        let ancestors = [&parent_scan.exports, &root_scan.exports];
        let child_file = File {
            module: "crate::service::child",
            ancestors: &ancestors,
        };
        let text = "use super::*;\nfn f() { lib::m::A; }";
        assert_eq!(reaches(text, &names, child_file).reaches, [], "{parent}");
    }
    // Without its ancestors, as for a `#[path]` module, a child's
    // `super::` paths reach nothing.
    let unknown = File {
        module: "crate::moved",
        ancestors: &[],
    };
    for text in ["use super::*;\nfn f() { m::A; }", "fn f() { super::m::A; }"] {
        assert_eq!(reaches(text, &names, unknown).reaches, [], "{text}");
    }
    // A grandchild sees what the child binds by its own `use super::*;`.
    let child = reaches("use super::*;\nmod grandchild;\n", &names, child_file);
    let ancestors = [&child.exports, &parent_scan.exports, &root_scan.exports];
    let grandchild = File {
        module: "crate::service::child::grandchild",
        ancestors: &ancestors,
    };
    let scanned = reaches("use super::*;\nfn f() { m::A; }", &names, grandchild);
    assert_eq!(scanned.reaches.len(), 1);
    assert!(
        scanned
            .through
            .contains(&("crate::service".to_owned(), "m".to_owned()))
    );
    // Inside a file, `super::` from an inline module is the module around it.
    let inline =
        "use lib::m;\nmod inner {\n    use super::*;\n    fn f() { m::A; super::m::B; }\n}";
    assert_eq!(
        items_in(None, inline),
        ["m::A", "m::B"],
        "the import is gone on through, so it does not count"
    );
}

/// Code under a `cfg` that holds only in tests is left out: an item, up to
/// its body or `;`, a field or arm up to its `,`, and a whole file or module
/// under an inner attribute. A `cfg` that also holds outside tests is
/// production code.
#[test]
fn test_code() {
    let skipped = [
        "#[cfg(test)]\nmod tests { use lib::m::X; }",
        "#[cfg(test)]\nuse lib::m::X;",
        "#[cfg(all(test, unix))]\nfn f<A, B>() -> lib::m::X {}",
        "#[cfg(all(unix, all(test)))]\n#[allow(unused)]\npub(crate) fn f() { lib::m::X; }",
        "#![cfg(test)]\nuse lib::m::X;",
        "mod m {\n    #![cfg(test)]\n    use lib::m::X;\n}",
        "struct S {\n    #[cfg(test)]\n    field: lib::m::X,\n}",
        "struct S {\n    #[cfg(test)]\n    field: Map<A, lib::m::X>,\n}",
        "struct S(#[cfg(test)] Map<A, lib::m::X>, u8);",
        "struct S {\n    #[cfg(test)]\n    field: Box<dyn Iterator<Item = A, Extra = lib::m::X>>,\n}",
        "struct S {\n    #[cfg(test)]\n    field: Map<Item = Vec<A>, lib::m::X>,\n}",
        "fn f() {\n    #[cfg(test)]\n    let x: Map<A, lib::m::X> = make();\n}",
    ];
    for text in skipped {
        assert_eq!(reached(text), [], "{text}");
    }
    let counted = [
        ("#[cfg(test)]\nmod tests;\nuse lib::m::X;", 3),
        ("#[cfg(any(test, feature = \"x\"))]\nuse lib::m::X;", 2),
        ("#[cfg(not(test))]\nuse lib::m::X;", 2),
        ("#[cfg(feature = \"test\")]\nuse lib::m::X;", 2),
        ("#[cfg_attr(test, allow(unused))]\nuse lib::m::X;", 2),
        (
            "struct S {\n    #[cfg(test)]\n    field: u8,\n    other: lib::m::X,\n}",
            4,
        ),
        (
            "struct S {\n    #[cfg(test)]\n    field: Map<A, B>,\n    other: lib::m::X,\n}",
            4,
        ),
        (
            "struct S {\n    #[cfg(test)]\n    field: Foo<Item = A, B>,\n    other: lib::m::X,\n}",
            4,
        ),
        (
            "match x {\n    #[cfg(test)]\n    A if a < b => 1,\n    _ => lib::m::X,\n}",
            4,
        ),
        (
            "match x {\n    #[cfg(test)]\n    A if a <= b => 1,\n    _ => lib::m::X,\n}",
            4,
        ),
        ("mod m {\n    #![cfg(test)]\n}\nuse lib::m::X;", 4),
    ];
    for (text, line) in counted {
        assert_eq!(reached(text), [("m::X".to_owned(), line)], "{text}");
    }
}

/// Each reach is on the line that names it, in order, inside a multi-line
/// group too.
#[test]
fn lines() {
    let text = "fn f() { lib::a::X; }\nuse lib::{\n    b::Y,\n    c::{\n        Z,\n    },\n};";
    assert_eq!(
        reached(text),
        [
            ("a::X".to_owned(), 1),
            ("b::Y".to_owned(), 3),
            ("c::Z".to_owned(), 5),
        ]
    );
}

#[test]
fn a_crate_roots_modules() {
    let text = "pub mod a;\npub mod b { pub mod nested; }\nmod private;\npub(crate) mod \
                internal;\n#[cfg(test)]\npub mod test_only;\npub use other::reexported;\n\
                // pub mod commented;\nconst S: &str = \"pub mod quoted;\";\npub mod c {}\n";
    let modules: Vec<String> = root_modules(text).into_iter().collect();
    assert_eq!(modules, ["a", "b", "c"]);
}
