//! The layers rule's pieces that the command-line tests reach only through
//! whole workspaces: the layer matrix, paths below a layer directory, the
//! items a finding names, the lenient reading of `[rules.layers]`, and the
//! SQL the state checks read.

use std::collections::BTreeMap;

use super::{
    and_list,
    domain::protocol_suffix,
    graph::{Member, Problem, layer_dir, list, may_depend, name_segments, use_tree, used_items},
    sql::{self, Created},
    storage::features_of,
};
use crate::{
    config::{CoWriters, DomainFences, Layer, LayersConfig, LayersPolicy},
    context::{Crate, Dependency, Dialect, ProductionFile, TextFile, declared_table},
};

/// Every behaviour of `[rules.layers]`, as its key in `LayersPolicy.modes`.
/// `LayersConfig` requires each, but the state checks' (`state.*` and
/// `unowned.*`), which come together, and denies others, so this list is
/// its list.
const BEHAVIOURS: [&str; 32] = [
    "placement",
    "layout",
    "direction.no_crossing",
    "direction.no_new_entries",
    "direction.stale_entries",
    "dev_direction.no_crossing",
    "dev_direction.no_new_entries",
    "dev_direction.stale_entries",
    "storage_access.no_crossing",
    "storage_access.no_new_entries",
    "storage_access.stale_entries",
    "codec_features.no_crossing",
    "codec_features.no_new_entries",
    "codec_features.stale_entries",
    "domain_dependencies.no_crossing",
    "domain_dependencies.no_new_entries",
    "domain_dependencies.stale_entries",
    "model_protocol.no_crossing",
    "model_protocol.no_new_entries",
    "model_protocol.stale_entries",
    "state.owners",
    "state.declarations",
    "state.writes",
    "state.pending",
    "unowned.no_crossing",
    "unowned.no_new_entries",
    "unowned.stale_entries",
    "test_only",
    "clients",
    "store_edges",
    "stale_entries",
    "policy_changed",
];

/// Each behaviour as `[<prefix><behaviour>]` with an advisory mode, and
/// the `storage` table `LayersConfig` requires.
fn behaviour_tables(prefix: &str) -> String {
    BEHAVIOURS
        .iter()
        .map(|behaviour| format!("[{prefix}{behaviour}]\nmode = \"advisory\"\n"))
        .chain([format!("[{prefix}storage]\ndrivers = [\"sqlx\"]\n")])
        .collect()
}

/// Each layer that may have a normal or build edge to each other one,
/// before the client and store rules.
#[test]
fn the_layer_matrix() {
    use Layer::{Api, App, Domain, Foundation, Storage, TestSupport, Tools};
    let allowed: &[(Layer, &[Layer])] = &[
        (Foundation, &[Foundation]),
        (Domain, &[Domain, Foundation]),
        (Storage, &[Storage, Domain, Foundation]),
        (Api, &[Api, Storage, Domain, Foundation]),
        (App, &[Api, Domain, Foundation]),
        (TestSupport, &Layer::ALL),
        (Tools, &Layer::ALL),
    ];
    for (from, targets) in allowed {
        for to in Layer::ALL {
            assert_eq!(
                may_depend(*from, to),
                targets.contains(&to),
                "{from} -> {to}"
            );
        }
    }
    // The stack is ranked bottom-up; test support and tools are beside it.
    let ranks: Vec<Option<u8>> = Layer::ALL.iter().map(|layer| layer.rank()).collect();
    assert_eq!(
        ranks,
        [Some(10), Some(20), Some(30), Some(40), Some(50), None, None]
    );
    assert!(Problem::Direction < Problem::DevDirection);
}

#[test]
fn layer_directories() {
    let cases: &[(&str, Option<(Layer, &str)>)] = &[
        ("crates/l10_foundation/x", Some((Layer::Foundation, "x"))),
        (
            "crates/l40_api/sbx/mgr/server",
            Some((Layer::Api, "sbx/mgr/server")),
        ),
        (
            "crates/test_support/fixtures",
            Some((Layer::TestSupport, "fixtures")),
        ),
        ("crates/tools", Some((Layer::Tools, ""))),
        ("crates/libs/x", None),
        ("crates", None),
        ("other/l10_foundation/x", None),
        ("", None),
    ];
    for (member, expected) in cases {
        assert_eq!(layer_dir(member), *expected, "{member}");
    }
    for layer in Layer::ALL {
        assert_eq!(Layer::from_dir(layer.dir()), Some(layer));
        assert_eq!(layer.to_string(), layer.dir());
    }
    assert_eq!(
        name_segments("pplx-space-sbx-mgr-client"),
        ["sbx", "mgr", "client"]
    );
    assert_eq!(name_segments("other-name"), ["other", "name"]);
}

/// A crate's features for the storage checks: its `[features]`, and Cargo's
/// implicit feature for each optional normal or build dependency that no
/// `dep:` item names, under the name the manifest gives it. A required
/// dependency has none. (Cargo rejects an optional dev-dependency.)
#[test]
fn features_with_implicit_ones() {
    let dependency =
        |package: &str, rename: Option<&str>, kind: Option<&str>, optional| Dependency {
            package: package.to_owned(),
            rename: rename.map(str::to_owned),
            kind: kind.map(str::to_owned),
            target: None,
            line: 1,
            optional,
            features: Vec::new(),
            uses_default_features: true,
        };
    let krate = Crate {
        name: "x".to_owned(),
        lib: None,
        member: "crates/x".to_owned(),
        name_line: 1,
        lib_name_line: None,
        dependencies: vec![
            dependency("sqlx", None, None, true),
            dependency("redis", Some("kv"), None, true),
            dependency("rusqlite", None, None, true),
            dependency("libsqlite3-sys", None, Some("build"), true),
            dependency("aws-sdk-s3", None, None, false),
        ],
        features: BTreeMap::from([
            ("db".to_owned(), vec![("dep:sqlx".to_owned(), 3)]),
            ("default".to_owned(), vec![("db".to_owned(), 2)]),
        ]),
        state: None,
    };
    let member = Member {
        krate: &krate,
        layer: Some(Layer::Foundation),
        layer_path: Some("x"),
    };
    let features = features_of(&member);
    let expected = BTreeMap::from([
        ("db".to_owned(), vec!["dep:sqlx".to_owned()]),
        ("default".to_owned(), vec!["db".to_owned()]),
        ("kv".to_owned(), vec!["dep:kv".to_owned()]),
        (
            "libsqlite3-sys".to_owned(),
            vec!["dep:libsqlite3-sys".to_owned()],
        ),
        ("rusqlite".to_owned(), vec!["dep:rusqlite".to_owned()]),
    ]);
    assert_eq!(features, expected);
}

#[test]
fn and_lists() {
    let items =
        |items: &[&str]| -> Vec<String> { items.iter().map(|&item| item.to_owned()).collect() };
    assert_eq!(and_list(&items(&[])), "");
    assert_eq!(and_list(&items(&["a"])), "a");
    assert_eq!(and_list(&items(&["a", "b"])), "a and b");
    assert_eq!(and_list(&items(&["a", "b", "c"])), "a, b and c");
}

/// The paths a use tree or a path in code names, as a finding lists them.
#[test]
fn use_trees() {
    let cases: &[(&str, &str, &[&str])] = &[
        ("Item;", "", &["Item"]),
        ("module::Item;", "", &["module::Item"]),
        ("module :: Item ;", "", &["module::Item"]),
        ("{A, b::C};", "", &["A", "b::C"]),
        (
            "{\n    a::{B, c::{D, self}},\n    E as F,\n    G,\n};",
            "",
            &["a::B", "a::c::D", "a::c", "E", "G"],
        ),
        ("{self, A};", "", &["A"]),
        ("a::{self};", "", &["a"]),
        ("a::*;", "", &["a"]),
        ("*;", "", &[]),
        ("{A,};", "", &["A"]),
        ("Type::<u8>::new()", "", &["Type"]),
        ("f(x)", "", &["f"]),
        ("", "", &[]),
        ("B, C", "a", &["a::B"]),
    ];
    for (text, prefix, expected) in cases {
        let mut rest = *text;
        let mut paths = Vec::new();
        use_tree(&mut rest, prefix, &mut paths);
        assert_eq!(paths, *expected, "{text:?}");
    }
    // The tree is consumed, up to what ends it.
    let mut rest = "{A as B, C}; rest";
    use_tree(&mut rest, "", &mut Vec::new());
    assert_eq!(rest, "; rest");
}

/// The items a crate's files name through a dependency; none when the
/// dependency has no name to find, as for a target without a library that
/// the manifest doesn't rename.
#[test]
fn items_used_through_a_dependency() {
    let text = "use dep::{A, b::C};\nfn f() { other::dep::x(); d::e(); }\n";
    let files = [ProductionFile {
        path: "member/src/lib.rs".to_owned(),
        text: text.to_owned(),
    }];
    let krate = Crate {
        name: "pplx-space-member".to_owned(),
        lib: Some("pplx_space_member".to_owned()),
        member: "member".to_owned(),
        name_line: 2,
        lib_name_line: None,
        dependencies: Vec::new(),
        features: BTreeMap::new(),
        state: None,
    };
    let dependency = |rename: Option<&str>| Dependency {
        package: "pplx-space-dep".to_owned(),
        rename: rename.map(str::to_owned),
        kind: None,
        target: None,
        line: 6,
        optional: false,
        features: Vec::new(),
        uses_default_features: true,
    };
    let used =
        |rename: Option<&str>, lib: &str| used_items(&files, &krate, &dependency(rename), lib);
    assert_eq!(used(None, "dep"), ["dep::A", "dep::b::C"]);
    assert_eq!(used(Some("d"), "dep"), ["d::e"]);
    assert_eq!(used(None, ""), Vec::<String>::new());
}

#[test]
fn lists_for_a_sentence() {
    let names: Vec<String> = ["a", "b", "c"].map(str::to_owned).to_vec();
    assert_eq!(list(&names, 5), "a, b, c");
    assert_eq!(list(&names, 3), "a, b, c");
    assert_eq!(list(&names, 2), "a, b and 1 more");
    assert_eq!(list(&[], 2), "");
}

/// `[rules.layers]` read for its lines: each entry, list item and mode on
/// its own line, including the state checks', and the table's first header.
#[test]
fn the_policy_and_its_lines() {
    let text = "[rules.crate_names]\nallow = []\n\
                [rules.layers.unmoved]\n\
                l20_domain = [\n  \"pplx-space-b\",\n  \"pplx-space-a\",\n]\n\
                [rules.layers.store_users]\n\
                pplx-space-s = [\"pplx-space-a\"]\n\
                [rules.layers.allowed_edges]\n\
                pplx-space-p = [\n  \"pplx-space-s\",\n]\n\
                [rules.layers.direction.no_crossing]\nmode = \"required\"\n\
                [rules.layers.policy_changed]\nmode = \"advisory\"\n\
                [rules.layers.retired]\nmode = \"advisory\"\n\
                [rules.layers.state.shared_catalogs]\ncommon = [\"x\"]\n\
                [rules.layers.state.writes]\nmode = \"required\"\n\
                [rules.layers.unowned.no_crossing]\nmode = \"required\"\n\
                [rules.layers.state.co_writers.postgres]\n\"public.t\" = [\"p\"]\nt = [\"r\"]\n\
                [rules.layers.state.co_writers.sqlite.db]\nu = [\n  \"q\",\n]\n";
    let policy = LayersPolicy::parse(text)
        .expect("the table parses")
        .expect("the text has the table");
    assert_eq!(policy.line, 3);
    assert_eq!(policy.unmoved.line, Some(3));
    let unmoved: Vec<(&str, &str, usize)> = policy
        .unmoved
        .entries
        .iter()
        .map(|(package, (layer, line))| (package.as_str(), layer.as_str(), *line))
        .collect();
    assert_eq!(
        unmoved,
        [
            ("pplx-space-a", "l20_domain", 6),
            ("pplx-space-b", "l20_domain", 5)
        ]
    );
    let (users, line) = &policy.store_users.entries["pplx-space-s"];
    assert_eq!((*line, users["pplx-space-a"]), (9, 9));
    let (allowed, line) = &policy.allowed_edges.entries["pplx-space-p"];
    assert_eq!((*line, allowed["pplx-space-s"]), (11, 12));
    let modes: Vec<(&str, &str, usize)> = policy
        .modes
        .iter()
        .map(|(behaviour, (mode, line))| (behaviour.as_str(), mode.as_str(), *line))
        .collect();
    assert_eq!(
        modes,
        [
            ("direction.no_crossing", "required", 15),
            ("policy_changed", "advisory", 17),
            ("state.writes", "required", 23),
            ("unowned.no_crossing", "required", 25),
        ]
    );
    let (databases, line) = &policy.shared_catalogs.entries["common"];
    assert_eq!(
        (policy.shared_catalogs.line, *line, databases["x"]),
        (Some(20), 21, 21)
    );
    /// A granted table, its stores with their lines, and its line.
    type Grant<'a> = (&'a str, Vec<(&'a str, usize)>, usize);
    let grants: Vec<Grant> = policy
        .co_writers
        .entries
        .iter()
        .map(|(table, (stores, line))| {
            let stores = stores
                .iter()
                .map(|(store, line)| (store.as_str(), *line))
                .collect();
            (table.as_str(), stores, *line)
        })
        .collect();
    assert_eq!(policy.co_writers.line, Some(26));
    assert_eq!(
        grants,
        [
            // A base branch's copy may name a table twice; it is granted
            // the stores of both.
            ("postgres.t", vec![("p", 27), ("r", 28)], 27),
            ("sqlite.db.u", vec![("q", 31)], 30),
        ]
    );
    assert!(policy.has_check("unowned") && policy.has_check("state"));
    assert!(!policy.has_check("dev_direction"));

    assert_eq!(
        LayersPolicy::parse("[rules.crate_names]\nallow = []\n").ok(),
        Some(None)
    );
    LayersPolicy::parse("[rules.layers]\nunmoved = 5\n").expect_err("unmoved is not a table");
}

/// The lenient reading of `[rules.layers]` has a mode for every behaviour
/// `LayersConfig` reads, so that `policy_changed` sees each one change.
#[test]
fn every_behaviour_has_a_policy_mode() {
    toml::from_str::<LayersConfig>(&behaviour_tables(""))
        .expect("BEHAVIOURS are LayersConfig's behaviours");
    let policy = LayersPolicy::parse(&behaviour_tables("rules.layers."))
        .expect("the table parses")
        .expect("the text has the table");
    let modes: Vec<&str> = policy.modes.keys().map(String::as_str).collect();
    let mut expected = BEHAVIOURS.to_vec();
    expected.sort_unstable();
    assert_eq!(modes, expected);
}

/// `unmoved` is read as package to layer, each package in one layer.
#[test]
fn unmoved_entries() {
    let behaviours = behaviour_tables("");
    let parse = |unmoved: &str| {
        toml::from_str::<LayersConfig>(&format!("[unmoved]\n{unmoved}{behaviours}"))
    };
    let config = parse("l10_foundation = [\"x\"]\ntools = [\"y\"]\n").expect("it parses");
    assert_eq!(config.unmoved.get("x"), Some(Layer::Foundation));
    assert_eq!(config.unmoved.get("y"), Some(Layer::Tools));
    assert_eq!(config.unmoved.get("z"), None);
    let packages: Vec<&String> = config.unmoved.packages().collect();
    assert_eq!(packages, ["x", "y"]);
    for (unmoved, error) in [
        ("l60_ui = [\"x\"]\n", "unknown layer \"l60_ui\""),
        (
            "l10_foundation = [\"x\"]\nl20_domain = [\"x\"]\n",
            "unmoved lists \"x\" in both l10_foundation and l20_domain",
        ),
        ("l10_foundation = [\"x\", \"x\"]\n", "\"x\" is listed twice"),
    ] {
        let found = parse(unmoved).expect_err("it is rejected").to_string();
        assert!(found.contains(error), "{unmoved}: {found}");
    }
}

/// A Domain crate is a protocol by name when the last part of its name is
/// `protocol` or `proto`, not when a part only starts or ends with it.
#[test]
fn protocol_names() {
    for (name, suffix) in [
        ("pplx-space-node-mgr-protocol", Some("-protocol")),
        ("pplx-space-credential-provider-proto", Some("-proto")),
        ("pplx-space-protocols", None),
        ("pplx-space-prototype", None),
        ("pplx-space-x-proto-types", None),
        ("pplx-space-rpc-contracts", None),
    ] {
        assert_eq!(protocol_suffix(name), suffix, "{name}");
    }
}

/// `protocols` and `[rules.layers.domain_fences]` read for their lines, the
/// `protocols` key's too, each fenced package with its family.
#[test]
fn the_domain_policy_and_its_lines() {
    let text = "[rules.layers]\nprotocols = [\n  \"pplx-space-w\",\n]\n\
                [rules.layers.domain_fences]\n\
                runtime = [\"tokio\", \"smol\"]\n\
                http = [\n  \"hyper\",\n]\n\
                [rules.layers.model_protocol.stale_entries]\nmode = \"required\"\n";
    let policy = LayersPolicy::parse(text)
        .expect("the table parses")
        .expect("the text has the table");
    assert_eq!(
        policy.protocols,
        BTreeMap::from([("pplx-space-w".to_owned(), 3)])
    );
    assert_eq!(policy.protocols_line, Some(2));
    assert_eq!(policy.domain_fences.line, Some(5));
    let fences: Vec<(&str, &str, usize)> = policy
        .domain_fences
        .entries
        .iter()
        .map(|(package, (family, line))| (package.as_str(), family.as_str(), *line))
        .collect();
    assert_eq!(
        fences,
        [
            ("hyper", "http", 8),
            ("smol", "runtime", 6),
            ("tokio", "runtime", 6)
        ]
    );
    let modes: Vec<&str> = policy.modes.keys().map(String::as_str).collect();
    assert_eq!(modes, ["model_protocol.stale_entries"]);
}

/// `domain_fences` is read as package to family, each package in one family.
#[test]
fn domain_fences() {
    let fences: DomainFences = toml::from_str::<BTreeMap<String, DomainFences>>(
        "fences = { runtime = [\"tokio\"], kube = [\"kube\", \"k8s-openapi\"] }",
    )
    .expect("it parses")
    .remove("fences")
    .expect("the key is there");
    assert_eq!(fences.family("tokio"), Some("runtime"));
    assert_eq!(fences.family("k8s-openapi"), Some("kube"));
    assert_eq!(fences.family("tonic"), None);
    let packages: Vec<&String> = fences.packages().collect();
    assert_eq!(packages, ["k8s-openapi", "kube", "tokio"]);
    let error = toml::from_str::<BTreeMap<String, DomainFences>>(
        "fences = { runtime = [\"tokio\"], http = [\"tokio\"] }",
    )
    .expect_err("a package in two families is rejected")
    .to_string();
    assert!(
        error.contains("domain_fences lists \"tokio\" in both http and runtime"),
        "{error}"
    );
}

/// The live tables of migrations `(name, text)` applied in order to a
/// database of `dialect`, as `table -> (file, line)`.
fn live(dialect: Dialect, migrations: &[(&str, &str)]) -> BTreeMap<String, (String, usize)> {
    let files: Vec<TextFile> = migrations
        .iter()
        .map(|(path, text)| TextFile {
            path: (*path).to_owned(),
            text: (*text).to_owned(),
        })
        .collect();
    let mut tables = BTreeMap::new();
    sql::replay(&files, dialect, &mut tables);
    tables
        .into_iter()
        .map(|(table, Created { path, line })| (table, (path, line)))
        .collect()
}

/// Creates, drops and renames in file order; names folded and keyed as
/// each database kind reads them, without its default schema and with any
/// other; temporary tables not live; DDL in comments, strings and bodies
/// not read.
#[test]
fn live_tables_of_a_catalog() {
    use Dialect::{Postgres, Sqlite};
    /// Name, dialect, migrations as `(name, text)`, and each live table
    /// with its line.
    type Case<'a> = (
        &'a str,
        Dialect,
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, usize)],
    );
    let cases: &[Case] = &[
        (
            "one table",
            Postgres,
            &[("1.sql", "CREATE TABLE a (id int);")],
            &[("a", 1)],
        ),
        (
            "Postgres: public is the default schema, quoted names keep their case",
            Postgres,
            &[(
                "1.sql",
                "create table public.a ();\nCREATE TABLE IF NOT EXISTS \"B\" ();\n\
                 CREATE UNLOGGED TABLE C ();\nCREATE TABLE \"public\".\"d\" ();\n\
                 CREATE TABLE other.e ();\nCREATE TABLE IF NOT EXISTS a ();\n\
                 CREATE TABLE main.f ();\nCREATE TABLE \"Public\".g ();\n\
                 CREATE TABLE space.public.h ();\n",
            )],
            &[
                ("B", 2),
                ("Public.g", 8),
                ("a", 1),
                ("c", 3),
                ("d", 4),
                ("h", 9),
                ("main.f", 7),
                ("other.e", 5),
            ],
        ),
        (
            "SQLite: main is the default schema, no name keeps its case",
            Sqlite,
            &[
                (
                    "1.sql",
                    "CREATE TABLE \"B\" ();\nCREATE TABLE Main.c ();\nCREATE TABLE [D] ();\n\
                     CREATE TABLE public.e ();\nCREATE TABLE `F`.g ();\n",
                ),
                (
                    "2.sql",
                    "ALTER TABLE b RENAME TO \"Bee\";\nDROP TABLE \"d\";\n",
                ),
            ],
            &[("bee", 1), ("c", 2), ("f.g", 5), ("public.e", 4)],
        ),
        (
            "SQLite's backtick and bracket quotes, in CREATE, DROP and RENAME",
            Sqlite,
            &[
                (
                    "1.sql",
                    "CREATE TABLE `a` ();\nCREATE TABLE [b c] ();\nCREATE TABLE `d``e` ();\n\
                     CREATE TABLE main.[f] ();\nCREATE TABLE \"g\" ();\nCREATE TABLE h ();\n\
                     CREATE TABLE `-- not a comment` ();\nCREATE TABLE [i--j] ();\n\
                     CREATE TABLE [k/*l] ();\nCREATE TABLE m (); -- */\n",
                ),
                (
                    "2.sql",
                    "DROP TABLE [a];\nALTER TABLE `b c` RENAME TO [x];\n\
                     ALTER TABLE [g] RENAME TO `y`;\nDROP TABLE `h`;\n",
                ),
            ],
            &[
                ("-- not a comment", 7),
                ("d`e", 3),
                ("f", 4),
                ("i--j", 8),
                ("k/*l", 9),
                ("m", 10),
                ("x", 2),
                ("y", 5),
            ],
        ),
        (
            "a rename keeps the table's schema",
            Postgres,
            &[
                (
                    "1.sql",
                    "CREATE TABLE audit.a ();\nCREATE TABLE public.b ();\n",
                ),
                (
                    "2.sql",
                    "ALTER TABLE audit.a RENAME TO z;\nALTER TABLE public.b RENAME TO y;\n",
                ),
            ],
            &[("audit.z", 1), ("y", 2)],
        ),
        (
            "Postgres's temporary tables are not live; its temp is a schema",
            Postgres,
            &[(
                "1.sql",
                "CREATE TEMP TABLE a ();\nCREATE TEMPORARY TABLE IF NOT EXISTS b ();\n\
                 CREATE GLOBAL TEMPORARY TABLE c ();\nCREATE LOCAL TEMP TABLE d ();\n\
                 CREATE TABLE temp.e ();\nCREATE TABLE pg_temp.f ();\n\
                 CREATE UNLOGGED TABLE g ();\nCREATE TABLE pg_temp_3.h ();\n",
            )],
            &[("g", 7), ("temp.e", 5)],
        ),
        (
            "SQLite's temporary tables are not live, nor changed by DROP or RENAME",
            Sqlite,
            &[
                (
                    "1.sql",
                    "CREATE TEMP TABLE a ();\nCREATE TABLE temp.b ();\nCREATE TABLE \"TEMP\".c ();\n\
                     CREATE TABLE x ();\nCREATE TABLE pg_temp.d ();\n",
                ),
                (
                    "2.sql",
                    "DROP TABLE temp.x;\nALTER TABLE temp.x RENAME TO y;\n",
                ),
            ],
            &[("pg_temp.d", 5), ("x", 4)],
        ),
        (
            "drops, lists and IF EXISTS on a name never created",
            Postgres,
            &[
                (
                    "1.sql",
                    "CREATE TABLE a ();\nCREATE TABLE b ();\nCREATE TABLE c ();",
                ),
                ("2.sql", "DROP TABLE IF EXISTS a, public.b, never CASCADE;"),
            ],
            &[("c", 3)],
        ),
        (
            "a rename keeps where the table was created; a column rename is not one",
            Postgres,
            &[
                ("1.sql", "CREATE TABLE a ();\nCREATE TABLE b ();"),
                (
                    "2.sql",
                    "ALTER TABLE IF EXISTS ONLY a RENAME TO z;\n\
                     ALTER TABLE b RENAME COLUMN x TO y;\nALTER TABLE b ADD COLUMN q int;",
                ),
            ],
            &[("b", 2), ("z", 1)],
        ),
        (
            "comments, strings and bodies hide DDL",
            Postgres,
            &[(
                "1.sql",
                "-- CREATE TABLE a ();\n/* CREATE TABLE b (); /* nested */ DROP TABLE c; */\n\
                 COMMENT ON x IS 'CREATE TABLE d (); it''s';\n\
                 CREATE FUNCTION f() AS $body$ CREATE TABLE e (); $body$;\n\
                 CREATE FUNCTION g() AS $$ CREATE TABLE f (); $$;\n\
                 SELECT $1; CREATE TABLE \"with \"\"quote\" ();\n\
                 CREATE VIEW v AS SELECT 1; CREATE INDEX i ON t (x);\n\
                 COMMENT ON y IS E'it\\'s; CREATE TABLE g ();';\n\
                 CREATE TABLE h (tags text[] DEFAULT ARRAY['it''s', '-- x']);\n",
            )],
            &[("h", 9), ("with \"quote", 6)],
        ),
        (
            "Postgres: a `[` is an array's, and what is in it is read as code",
            Postgres,
            &[(
                "1.sql",
                "CREATE TABLE a (x int[] DEFAULT ARRAY[1 /* ] ' */]);\n\
                 SELECT ARRAY[1 /* CREATE TABLE b (); */];\nCREATE TABLE c ();\n",
            )],
            &[("a", 1), ("c", 3)],
        ),
        (
            "SQLite: a `[…]` identifier runs to its first `]`, `'`s included",
            Sqlite,
            &[("1.sql", "CREATE TABLE [a'--b] ();\nCREATE TABLE c ();\n")],
            &[("a'--b", 1), ("c", 2)],
        ),
    ];
    for (name, dialect, migrations, expected) in cases {
        let found = live(*dialect, migrations);
        let found: Vec<(&str, usize)> = found
            .iter()
            .map(|(table, (_, line))| (table.as_str(), *line))
            .collect();
        assert_eq!(found, *expected, "{name}");
    }
    let found = live(
        Postgres,
        &[
            ("1.sql", "CREATE TABLE a ();"),
            ("2.sql", "\nCREATE TABLE b ();"),
        ],
    );
    assert_eq!(found["b"], ("2.sql".to_owned(), 2));
}

/// A table named in a declaration or a grant is the name its database
/// stores: case kept in Postgres and not in SQLite, the default schema
/// dropped, and of three parts the last two read. Its parts are read as SQL
/// reads them, so a `.` inside quotes is the table's.
#[test]
fn declared_tables() {
    let cases = [
        (Dialect::Postgres, "audit.\"events.v1\"", "audit.events.v1"),
        (Dialect::Postgres, "\"public\".\"Mixed\"", "Mixed"),
        (Dialect::Postgres, "\"a.b\"", "a.b"),
        (Dialect::Postgres, "a-b.c", "a-b.c"),
        (Dialect::Sqlite, "Audit.[Events.V1]", "audit.events.v1"),
        (Dialect::Sqlite, "main.\"x.y\"", "x.y"),
        (Dialect::Postgres, "public.a", "a"),
        (Dialect::Postgres, "Mixed", "Mixed"),
        (Dialect::Postgres, "main.a", "main.a"),
        (Dialect::Postgres, "space.public.a", "a"),
        (Dialect::Postgres, "space.audit.a", "audit.a"),
        (Dialect::Sqlite, "Main.Mixed", "mixed"),
        (Dialect::Sqlite, "temp.a", "a"),
        (Dialect::Sqlite, "public.a", "public.a"),
    ];
    for (dialect, name, key) in cases {
        assert_eq!(declared_table(dialect, name), key, "{dialect:?} {name}");
    }
}

/// Two keys of one namespace's co-writers that name one table are an
/// error, as each database kind names it; other keys are not.
#[test]
fn co_writer_keys_name_distinct_tables() {
    let parse = |text: &str| toml::from_str::<CoWriters>(text).map_err(|error| error.to_string());
    let errors = [
        (
            "postgres = { a = [\"x\"], \"public.a\" = [\"y\"] }",
            "[rules.layers.state.co_writers.postgres] names the table a twice, as \"a\" and \
             \"public.a\"; merge their stores into one key",
        ),
        (
            "sqlite.db = { Items = [\"x\"], \"main.items\" = [\"y\"] }",
            "[rules.layers.state.co_writers.sqlite.db] names the table items twice, as \"Items\" \
             and \"main.items\"",
        ),
    ];
    for (text, error) in errors {
        let found = parse(text).expect_err(text);
        assert!(found.contains(error), "{text}: {found}");
    }
    let distinct = "postgres = { a = [\"x\"], A = [\"y\"], \"main.a\" = [\"z\"] }\n\
                    sqlite.db = { a = [\"x\"] }\nsqlite.other = { a = [\"y\"] }";
    let grants = parse(distinct).expect("distinct tables");
    assert_eq!(grants.grants().count(), 5);
}

/// The heads a write scan finds in SQL text read as `dialect`, as `(head,
/// table)`, with the table keyed for a database of `dialect`.
fn heads(text: &str, dialect: Dialect) -> Vec<(String, String)> {
    sql::writes(&sql::blank(text, false, dialect), dialect)
        .into_iter()
        .map(|write| (write.head, write.table.key(dialect)))
        .collect()
}

#[test]
fn write_heads() {
    use Dialect::{Postgres, Sqlite};
    /// SQL, the dialect its tables are keyed for, and its heads.
    type Case<'a> = (&'a str, Dialect, &'a [(&'a str, &'a str)]);
    let cases: &[Case] = &[
        (
            "INSERT INTO a (x) VALUES (1)",
            Postgres,
            &[("INSERT INTO", "a")],
        ),
        ("DELETE\n  FROM\tb", Postgres, &[("DELETE FROM", "b")]),
        ("MERGE INTO c USING d", Postgres, &[("MERGE INTO", "c")]),
        ("UPDATE ONLY e SET", Postgres, &[("UPDATE", "e")]),
        (
            "TRUNCATE TABLE f, public.g ,h; TRUNCATE i",
            Postgres,
            &[
                ("TRUNCATE TABLE", "f"),
                ("TRUNCATE TABLE", "g"),
                ("TRUNCATE TABLE", "h"),
                ("TRUNCATE", "i"),
            ],
        ),
        (
            "INSERT OR IGNORE INTO j; REPLACE INTO k; UPDATE OR REPLACE l SET",
            Sqlite,
            &[
                ("INSERT OR IGNORE INTO", "j"),
                ("REPLACE INTO", "k"),
                ("UPDATE OR REPLACE", "l"),
            ],
        ),
        // Not heads, or heads of no table this check reads.
        (
            "ON CONFLICT DO UPDATE SET x = 1",
            Postgres,
            &[("UPDATE", "set")],
        ),
        ("SELECT 1 FOR UPDATE OF m", Postgres, &[("UPDATE", "of")]),
        ("x.update n; updated_at; my_update o", Postgres, &[]),
        ("UPDATE {table} SET", Postgres, &[]),
        (
            "-- UPDATE q\n/* DELETE FROM r */ SELECT '-- not a comment'",
            Postgres,
            &[],
        ),
        // Strings are values, and bodies code.
        (
            "SELECT 'UPDATE s', 'it''s DELETE FROM s2'; INSERT INTO log VALUES ('DELETE FROM s3')",
            Postgres,
            &[("INSERT INTO", "log")],
        ),
        (
            "CREATE FUNCTION f() AS $$ BEGIN UPDATE t SET x = 'DELETE FROM u'; END $$; \
             DO $body$ BEGIN DELETE FROM v; -- UPDATE w\n END $body$",
            Postgres,
            &[("UPDATE", "t"), ("DELETE FROM", "v")],
        ),
        // An escape string ends at no `\'`; a standard one at the first `'`.
        (
            "SELECT E'it\\'s UPDATE g'; DELETE FROM h; SELECT e'\\\\'; UPDATE i; \
             SELECT x LIKE'a\\'; UPDATE j; SELECT type'k\\'; UPDATE l",
            Postgres,
            &[
                ("DELETE FROM", "h"),
                ("UPDATE", "i"),
                ("UPDATE", "j"),
                ("UPDATE", "l"),
            ],
        ),
        // In SQLite, a `[…]` identifier holds no comment or string, up to
        // its first `]`.
        (
            "UPDATE [a--b] SET x = 1; DELETE FROM [c/*d] WHERE 1; INSERT INTO e -- */",
            Sqlite,
            &[
                ("UPDATE", "a--b"),
                ("DELETE FROM", "c/*d"),
                ("INSERT INTO", "e"),
            ],
        ),
        (
            "UPDATE [a'--b] SET x = 1; DELETE FROM c -- '",
            Sqlite,
            &[("UPDATE", "a'--b"), ("DELETE FROM", "c")],
        ),
        // In Postgres, `[` is an array's or a subscript's, and the comments
        // and strings inside it are blanked.
        (
            "SELECT ARRAY['UPDATE d', 'x]']; DELETE FROM e; SELECT y[1] -- UPDATE f\n",
            Postgres,
            &[("DELETE FROM", "e")],
        ),
        (
            "SELECT ARRAY[1 /* ] ' */]; UPDATE sandboxes SET id = id;",
            Postgres,
            &[("UPDATE", "sandboxes")],
        ),
        ("SELECT ARRAY[1 /* UPDATE sandboxes */];", Postgres, &[]),
        ("UPDATE [a] SET", Postgres, &[]),
        // Postgres: public is the default schema, any other is kept, of
        // three parts the last two are read, and a quoted name keeps its
        // case.
        (
            "UPDATE other.p SET; DELETE FROM \"Audit\" . \"Log\"; UPDATE \"public\".q; \
             UPDATE a.b.c SET; UPDATE space.public.sandboxes SET; UPDATE main.m; \
             insert into public.A; Update \"Mixed\" SET; UPDATE \"Public\".n",
            Postgres,
            &[
                ("UPDATE", "other.p"),
                ("DELETE FROM", "Audit.Log"),
                ("UPDATE", "q"),
                ("UPDATE", "b.c"),
                ("UPDATE", "sandboxes"),
                ("UPDATE", "main.m"),
                ("INSERT INTO", "a"),
                ("UPDATE", "Mixed"),
                ("UPDATE", "Public.n"),
            ],
        ),
        // SQLite: main and temp are default schemas, no name keeps its
        // case, and its quotes.
        (
            "INSERT INTO `x` VALUES; UPDATE [y z] SET; DELETE FROM main.`w`; \
             REPLACE INTO \"v\"\"u\"; UPDATE public.p; Update \"Mixed\" SET; \
             DELETE FROM Temp.T; UPDATE [Main].[Q]",
            Sqlite,
            &[
                ("INSERT INTO", "x"),
                ("UPDATE", "y z"),
                ("DELETE FROM", "w"),
                ("REPLACE INTO", "v\"u"),
                ("UPDATE", "public.p"),
                ("UPDATE", "mixed"),
                ("DELETE FROM", "t"),
                ("UPDATE", "q"),
            ],
        ),
    ];
    for (text, dialect, expected) in cases {
        let expected: Vec<(String, String)> = expected
            .iter()
            .map(|(head, table)| ((*head).to_owned(), (*table).to_owned()))
            .collect();
        assert_eq!(heads(text, *dialect), expected, "{text:?}");
    }
}

/// Rust string literals, decoded, with each byte's offset in the source;
/// comments, chars and lifetimes skipped.
#[test]
fn rust_string_literals() {
    let source = "let a = \"x\\ny\\u{41}\\x42\\\"\";\n\
                  let b = r#\"raw \"quoted\" \\n\"#; let c = br\"bytes\"; let d = c\"c\";\n\
                  // \"in a comment\"\n/* \"in /* a nested */ block\" */\n\
                  let e: &'static str = \"after a lifetime\"; let f = '\"'; let g = b'\\'';\n\
                  let h = \"one \\\n    line\"; let i = for_r\"no\";\n\
                  q('\\'','\"', \"after a quote char\");\n";
    let texts: Vec<String> = sql::literals(source)
        .into_iter()
        .map(|literal| literal.text)
        .collect();
    assert_eq!(
        texts,
        [
            "x\nyAB\"",
            "raw \"quoted\" \\n",
            "bytes",
            "c",
            "after a lifetime",
            "one line",
            "no",
            "after a quote char",
        ]
    );
    let literal = &sql::literals("x(\"ab\\ncd\")")[0];
    assert_eq!(literal.offsets, [3, 4, 5, 7, 8]);
}

/// A head is placed on its line in the file: inside a multi-line literal,
/// after a continuation, and in a `.sql` file.
#[test]
fn write_lines() {
    let source = "fn f() {\n    q(\"SELECT 1\");\n    q(r#\"\n        UPDATE a\n           SET x = 1\"#);\n\
                  q(\"DELETE \\\n     FROM b -- UPDATE c\");\n    // q(\"UPDATE d\")\n}\n";
    let found: Vec<(String, usize)> = sql::rust_writes(source, Dialect::Postgres)
        .into_iter()
        .map(|(write, line)| (write.table.key(Dialect::Postgres), line))
        .collect();
    assert_eq!(found, [("a".to_owned(), 4), ("b".to_owned(), 6)]);
    let found: Vec<(String, usize)> = sql::sql_writes(
        "-- UPDATE x\n\nWITH y AS (\n  UPDATE z SET",
        Dialect::Postgres,
    )
    .into_iter()
    .map(|(write, line)| (write.table.key(Dialect::Postgres), line))
    .collect();
    assert_eq!(found, [("z".to_owned(), 4)]);
    // Both read `[` as the dialect does: an array's in Postgres, so the
    // comment in it ends before the `UPDATE`; an identifier in SQLite, so
    // the `'` after it opens a string that runs to the end.
    let sql = "SELECT ARRAY[1 /* ] ' */]; UPDATE sandboxes SET id = id;";
    let source = format!("q(\"{sql}\");");
    for (dialect, expected) in [(Dialect::Postgres, 1), (Dialect::Sqlite, 0)] {
        assert_eq!(
            sql::rust_writes(&source, dialect).len(),
            expected,
            "{dialect:?}"
        );
        assert_eq!(sql::sql_writes(sql, dialect).len(), expected, "{dialect:?}");
    }
}
