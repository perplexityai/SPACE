//! The layers rule's pieces that the command-line tests reach only through
//! whole workspaces: the layer matrix, paths below a layer directory, the
//! items a finding names, the top level of a library root, and the lenient
//! reading of `[rules.layers]`.

use std::collections::BTreeMap;

use super::{
    and_list,
    domain::{protocol_suffix, top_level},
    graph::{Member, Problem, file_owner, layer_dir, list, may_depend, name_segments, used_items},
    storage::features_of,
};
use crate::{
    config::{DomainFences, Layer, LayersConfig, LayersPolicy},
    context::{Crate, Dependency, ProductionFile, count_code_lines},
};

/// Every behaviour of `[rules.layers]`, as its key in `LayersPolicy.modes`.
/// `LayersConfig` requires each and denies others, so this list is its
/// list.
const BEHAVIOURS: [&str; 29] = [
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
    "facades.no_crossing",
    "facades.no_new_entries",
    "facades.stale_entries",
    "test_only",
    "clients",
    "store_edges",
    "single_user",
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

/// A production file belongs to the member whose `src/` holds it, the
/// innermost one when a member is nested in another's `src/`.
#[test]
fn file_owners() {
    let krate = |name: &str, member: &str| Crate {
        name: name.to_owned(),
        lib: None,
        member: member.to_owned(),
        name_line: 1,
        lib_name_line: None,
        dependencies: Vec::new(),
        features: BTreeMap::new(),
    };
    let crates = [
        krate("root", ""),
        krate("outer", "crates/outer"),
        krate("inner", "crates/outer/src/inner"),
        krate("outer-two", "crates/outer-two"),
    ];
    let cases = [
        ("src/main.rs", Some("root")),
        ("crates/outer/src/lib.rs", Some("outer")),
        ("crates/outer/src/inner/src/lib.rs", Some("inner")),
        ("crates/outer/src/inner/lib.rs", Some("outer")),
        ("crates/outer-two/src/lib.rs", Some("outer-two")),
        ("crates/outer/build.rs", None),
    ];
    for (path, owner) in cases {
        let found = file_owner(&crates, path).map(|krate| krate.name.as_str());
        assert_eq!(found, owner, "{path}");
    }
    assert!(file_owner(&crates[1..], "src/main.rs").is_none());
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

/// The items a crate's files name through a dependency, from use trees of
/// any shape and from paths in code, but not from comments, literals or
/// test code; none when the dependency has no name to find, as for a target
/// without a library that the manifest doesn't rename.
#[test]
fn items_used_through_a_dependency() {
    let text = "use dep::{A, b::{C, self}, e::*, F as G};\nuse dep::{self, *};\n\
                fn f() { other::dep::x(); d::e(); dep::T::<u8>::new(); }\n\
                // dep::Commented\nconst S: &str = \"dep::Quoted\";\n\
                #[cfg(test)]\nfn t() { dep::InTests; }\n";
    let files = [ProductionFile {
        path: "member/src/lib.rs".to_owned(),
        text: text.to_owned(),
        code_lines: count_code_lines(text),
    }];
    let krate = Crate {
        name: "pplx-space-member".to_owned(),
        lib: Some("pplx_space_member".to_owned()),
        member: "member".to_owned(),
        name_line: 2,
        lib_name_line: None,
        dependencies: Vec::new(),
        features: BTreeMap::new(),
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
    assert_eq!(
        used(None, "dep"),
        [
            "dep::A",
            "dep::F",
            "dep::T",
            "dep::b",
            "dep::b::C",
            "dep::e"
        ]
    );
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
/// its own line, and the table's first header.
#[test]
fn the_policy_and_its_lines() {
    let text = "[rules.file_size]\nlimit = 5\n\
                [rules.layers.unmoved]\n\
                l20_domain = [\n  \"pplx-space-b\",\n  \"pplx-space-a\",\n]\n\
                [rules.layers.store_users]\n\
                pplx-space-s = [\"pplx-space-a\"]\n\
                [rules.layers.allowed_edges]\n\
                pplx-space-p = [\n  \"pplx-space-s\",\n]\n\
                [rules.layers.direction.no_crossing]\nmode = \"required\"\n\
                [rules.layers.policy_changed]\nmode = \"advisory\"\n\
                [rules.layers.retired]\nmode = \"advisory\"\n";
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
            ("policy_changed", "advisory", 17)
        ]
    );

    assert_eq!(
        LayersPolicy::parse("[rules.file_size]\nlimit = 5\n").ok(),
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
                [rules.layers.single_user]\nmode = \"advisory\"\n\
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
    assert_eq!(modes, ["model_protocol.stale_entries", "single_user"]);
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

/// Only a library root's top-level code is left for `single_user`'s
/// re-exports: literals of every kind, comments and delimited code are
/// blanked, a lifetime and a delimiter in a literal end nothing, and the
/// outermost delimiters stay.
#[test]
fn top_level_code() {
    let kept = |text: &str| {
        top_level(text)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };
    let text = "pub use a;\n\
                const R: &str = r#\"\"}\npub use b;\"#;\n\
                const B: &[u8] = br\"{\";\n\
                const C: char = '{';\n\
                fn f<'x>(s: &'x str) -> &'x str { s }\n\
                pub(crate) use c;\n\
                /* /* */ pub use d; */\n\
                pub use e::{F, G};\n";
    assert_eq!(
        kept(text),
        "pub use a; const R: &str = ; const B: &[ ] = ; const C: char = ; \
         fn f<'x>( ) -> &'x str { } pub( ) use c; pub use e::{ };"
    );
}
