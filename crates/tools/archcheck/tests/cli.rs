//! archcheck over temporary workspaces: the library for counting, the
//! inventory, findings and the baseline; the binary for modes, exit codes,
//! formats and arguments.

use std::{fs, path::Path, process::Command};

use pplx_space_archcheck::{
    BaseBaseline, Baseline, Context, check,
    config::CrateNamesPolicy,
    context::count_code_lines,
    report::{Format, render},
    skipped, write_baseline,
};
use serde_json::{Value, json};
use tempfile::TempDir;

const LIMIT: usize = 5;

/// A file of `lines` code lines.
fn code(lines: usize) -> String {
    "let x = 1;\n".repeat(lines)
}

const ADVISORY: [&str; 5] = ["advisory"; 5];

/// The summary's line when no base architecture is given.
const POLICY_SKIPPED: &str =
    "archcheck: crate_names.policy_changed skipped (no --base-architecture given)\n";

/// The summary's lines when neither base is given.
const SKIPPED: &str = "archcheck: file_size.no_new_entries and file_size.no_raised_caps skipped \
                       (no --base-baseline given)\n\
                       archcheck: crate_names.no_new allows only the 16 crates listed when the \
                       rule was added in [crate_names] (no --base-baseline given)\n\
                       archcheck: crate_names.policy_changed skipped (no --base-architecture \
                       given)\n";

/// `crate_names`' lines after the other skip lines, for a base baseline
/// without `[crate_names]` and no base architecture.
const CRATE_NAMES_SKIPPED: [&str; 2] = [
    "the base branch's architecture-baseline.toml has no [crate_names] table; crate_names.no_new \
     allows only the 16 crates listed when the rule was added in [crate_names]",
    "crate_names.policy_changed skipped (no --base-architecture given)",
];

/// `[rules.crate_names]` as in the repository's `ARCHITECTURE.toml`.
const CRATE_NAMES: &str = "[rules.crate_names]\n\
                           banned_tokens = [\"common\", \"shared\", \"util\", \"utils\", \
                           \"helper\", \"helpers\", \"misc\", \"core\"]\n\
                           allow = []\n\
                           [rules.crate_names.no_new]\nmode = \"required\"\n\
                           [rules.crate_names.stale_entries]\nmode = \"advisory\"\n\
                           [rules.crate_names.policy_changed]\nmode = \"advisory\"\n";

/// `modes` of file_size's no_crossing, no_new_entries, no_raised_caps,
/// listed_no_growth and stale_entries, followed by [`CRATE_NAMES`].
fn config(modes: [&str; 5]) -> String {
    let [
        no_crossing,
        no_new_entries,
        no_raised_caps,
        listed_no_growth,
        stale_entries,
    ] = modes;
    format!(
        "[rules.file_size]\nlimit = {LIMIT}\n\
         [rules.file_size.no_crossing]\nmode = \"{no_crossing}\"\n\
         [rules.file_size.no_new_entries]\nmode = \"{no_new_entries}\"\n\
         [rules.file_size.no_raised_caps]\nmode = \"{no_raised_caps}\"\n\
         [rules.file_size.listed_no_growth]\nmode = \"{listed_no_growth}\"\n\
         [rules.file_size.stale_entries]\nmode = \"{stale_entries}\"\n\
         {CRATE_NAMES}"
    )
}

/// [`config`] with file_size's behaviours advisory, and the `key` line of
/// `[rules.crate_names]` replaced with `key = <value>`.
fn with_crate_names(key: &str, value: &str) -> String {
    let advisory = config(ADVISORY);
    let line = advisory
        .lines()
        .find(|line| line.starts_with(&format!("{key} = ")))
        .expect("CRATE_NAMES has the key");
    advisory.replace(line, &format!("{key} = {value}"))
}

/// `[file_size]` with `entries` and an empty `[crate_names]`.
fn baseline(entries: &[(&str, usize)]) -> String {
    tables(entries, &[])
}

/// `[file_size]` and `[crate_names]` with these entries.
fn tables(file_size: &[(&str, usize)], crate_names: &[(&str, &str)]) -> String {
    let mut text = String::from("[file_size]\n");
    for (path, cap) in file_size {
        text.push_str(&format!("{path:?} = {cap}\n"));
    }
    text.push_str("[crate_names]\n");
    for (name, member) in crate_names {
        text.push_str(&format!("{name:?} = {member:?}\n"));
    }
    text
}

/// A member manifest for package `name`. Its library's source is outside
/// `src/`, so that it is not a production file, and need not exist.
fn package(name: &str) -> String {
    format!("[package]\nname = {name:?}\n[lib]\npath = \"lib.rs\"\n")
}

/// A workspace with members `crates/a` and `crates/b`, every behaviour
/// advisory and an empty baseline, with `files` written over those defaults.
fn workspace(files: &[(&str, String)]) -> TempDir {
    let dir = tempfile::tempdir().expect("create a temporary workspace");
    let defaults = [
        (
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/a\", \"crates/b\"]\n".to_owned(),
        ),
        ("crates/a/Cargo.toml", package("a")),
        ("crates/b/Cargo.toml", package("b")),
        ("ARCHITECTURE.toml", config(ADVISORY)),
        ("architecture-baseline.toml", baseline(&[])),
    ];
    for (path, contents) in defaults.iter().chain(files) {
        let path = dir.path().join(path);
        fs::create_dir_all(path.parent().expect("a parent directory")).expect("create it");
        fs::write(&path, contents).expect("write a workspace file");
    }
    dir
}

/// A workspace with members `crates/a`, `crates/b` and one per
/// `(member, package name)` in `packages`, and `files` written over the
/// defaults.
fn crate_workspace(packages: &[(&str, &str)], files: &[(&str, String)]) -> TempDir {
    let mut members = vec!["crates/a", "crates/b"];
    members.extend(packages.iter().map(|(member, _)| *member));
    let manifests: Vec<(String, String)> = packages
        .iter()
        .map(|(member, name)| (format!("{member}/Cargo.toml"), package(name)))
        .collect();
    let mut all: Vec<(&str, String)> = vec![(
        "Cargo.toml",
        format!("[workspace]\nmembers = {members:?}\n"),
    )];
    all.extend(
        manifests
            .iter()
            .map(|(path, text)| (path.as_str(), text.clone())),
    );
    all.extend(files.iter().cloned());
    workspace(&all)
}

/// The workspace's context, with `base` written to `base-baseline.toml` and
/// read as the base branch's baseline.
fn load(root: &Path, base: Option<&str>) -> Context {
    load_with(root, base, None)
}

/// [`load`], also with `architecture` written to `base-architecture.toml`
/// and read as the base branch's `ARCHITECTURE.toml`.
fn load_with(root: &Path, base: Option<&str>, architecture: Option<&str>) -> Context {
    let mut ctx = Context::load(root).expect("the workspace loads");
    if let Some(text) = base {
        let path = root.join("base-baseline.toml");
        fs::write(&path, text).expect("write the base baseline");
        ctx.base_baseline = Baseline::load_base(&path).expect("the base baseline is readable");
    }
    if let Some(text) = architecture {
        let path = root.join("base-architecture.toml");
        fs::write(&path, text).expect("write the base architecture");
        ctx.base_architecture =
            CrateNamesPolicy::load_base(&path).expect("the base architecture is readable");
    }
    ctx
}

/// `(behaviour, message)` of every finding, in reporting order.
fn findings(root: &Path) -> Vec<(&'static str, String)> {
    findings_against(root, None)
}

fn findings_against(root: &Path, base: Option<&str>) -> Vec<(&'static str, String)> {
    check(&load(root, base))
        .into_iter()
        .map(|finding| (finding.behaviour, finding.message()))
        .collect()
}

fn rdjson(root: &Path, base: Option<&str>) -> Value {
    let ctx = load(root, base);
    let text = render(Format::Rdjson, &check(&ctx), ctx.files.len());
    serde_json::from_str(&text).expect("one JSON document")
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(root: &Path, args: &[&str]) -> Run {
    let output = Command::new(env!("CARGO_BIN_EXE_archcheck"))
        .arg("--root")
        .arg(root)
        .args(args)
        .output()
        .expect("run archcheck");
    Run {
        code: output.status.code().expect("an exit status"),
        stdout: String::from_utf8(output.stdout).expect("UTF-8 stdout"),
        stderr: String::from_utf8(output.stderr).expect("UTF-8 stderr"),
    }
}

#[test]
fn counting() {
    let cases: &[(&str, &str, usize)] = &[
        ("statements", "let a = 1;\nlet b = 2;\n", 2),
        (
            "comments and blank lines are free",
            "// c\n/// d\n//! e\n\n   \n\tfn f() {}\n",
            1,
        ),
        (
            "block comments and attributes count",
            "/* c */\n#[test]\nfn f() {}\n",
            3,
        ),
        ("a byte order mark", "\u{feff}// c\nfn f() {}\n", 1),
        ("CRLF", "// c\r\nfn f() {}\r\n", 1),
        ("a lone CR ends no comment", "// c\rfn f() {}", 0),
        ("a CR-only file is one line", "fn f() {}\rfn g() {}\r", 1),
        ("U+2028 ends no comment", "// c\u{2028}fn f() {}", 0),
        ("U+2029 ends no comment", "// c\u{2029}fn f() {}", 0),
    ];
    for (name, text, expected) in cases {
        assert_eq!(count_code_lines(text), *expected, "{name}");
    }
}

#[test]
fn inventory() {
    let cases: &[(&str, bool)] = &[
        ("crates/a/src/lib.rs", true),
        ("crates/a/src/attests.rs", true),
        ("crates/a/src/testsuite/mod.rs", true),
        ("crates/b/src/deep/nested.rs", true),
        ("crates/a/src/tests.rs", false),
        ("crates/a/src/foo_tests.rs", false),
        ("crates/a/src/m/tests/mod.rs", false),
        ("crates/a/src/m/tests/deep/x.rs", false),
        ("crates/a/src/notes.txt", false),
        ("crates/a/tests/it.rs", false),
        ("crates/a/build.rs", false),
        ("crates/c/src/lib.rs", false),
        // A member inside another member's `src/` is listed once.
        ("crates/a/src/inner/src/lib.rs", true),
    ];
    let mut files: Vec<(&str, String)> = cases.iter().map(|(path, _)| (*path, code(1))).collect();
    files.extend([
        ("crates/a/src/inner/Cargo.toml", package("inner")),
        ("crates/c/Cargo.toml", package("c")),
    ]);
    let mut members = vec!["crates/a", "crates/b", "crates/a/src/inner"];
    let mut expected: Vec<&str> = cases
        .iter()
        .filter(|(_, checked)| *checked)
        .map(|(path, _)| *path)
        .collect();
    #[cfg(unix)]
    {
        // `crates/s/src` is a symlink to `shared/src`, made below.
        files.extend([
            ("crates/s/Cargo.toml", package("s")),
            ("shared/src/lib.rs", code(1)),
        ]);
        members.push("crates/s");
        expected.push("crates/s/src/lib.rs");
    }
    files.push((
        "Cargo.toml",
        format!("[workspace]\nmembers = {members:?}\n"),
    ));
    let ws = workspace(&files);
    #[cfg(unix)]
    {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt, os::unix::fs::symlink};

        // Symlinks below `src/` are skipped, to files and to directories; a
        // member's `src` that is a symlink is followed.
        symlink("lib.rs", ws.path().join("crates/a/src/link.rs")).unwrap();
        symlink("deep", ws.path().join("crates/b/src/linked")).unwrap();
        symlink("../../shared/src", ws.path().join("crates/s/src")).unwrap();
        // Only `.rs` names must be UTF-8.
        let src = ws.path().join("crates/b/src");
        fs::write(src.join(OsStr::from_bytes(b"\xff.txt")), "").unwrap();
    }

    let ctx = Context::load(ws.path()).unwrap();

    let checked: Vec<&str> = ctx.files.iter().map(|file| file.path.as_str()).collect();
    expected.sort_unstable();
    assert_eq!(checked, expected);
}

/// One row per situation: the base branch's baseline entries (`None`
/// without `--base-baseline`), baseline entries, file sizes, and the expected
/// `(behaviour, start of message)` of each finding.
#[test]
fn behaviours() {
    const LISTED: &str = "crates/a/src/listed.rs";
    const NEW: &str = "crates/b/src/new.rs";
    type Sizes<'a> = &'a [(&'a str, usize)];
    type Case<'a> = (
        &'a str,
        Option<Sizes<'a>>,
        Sizes<'a>,
        Sizes<'a>,
        &'a [(&'a str, &'a str)],
    );
    let cases: &[Case] = &[
        (
            "a new file over the limit",
            None,
            &[],
            &[(NEW, LIMIT + 1)],
            &[(
                "no_crossing",
                "crates/b/src/new.rs: 6 code lines, over the 5-line limit (comments and blank \
                 lines are not counted). Split it into submodules by responsibility.",
            )],
        ),
        ("a new file at the limit", None, &[], &[(NEW, LIMIT)], &[]),
        (
            "a listed file grew",
            None,
            &[(LISTED, 6)],
            &[(LISTED, 7)],
            &[(
                "listed_no_growth",
                "crates/a/src/listed.rs grew from 6 to 7 code lines. Once this rule is \
                 required, split the file before adding to it.",
            )],
        ),
        (
            "a listed file at its cap",
            None,
            &[(LISTED, 6)],
            &[(LISTED, 6)],
            &[],
        ),
        (
            "a listed file shrank",
            None,
            &[(LISTED, 8)],
            &[(LISTED, 7)],
            &[(
                "stale_entries",
                "crates/a/src/listed.rs shrank from 8 to 7 code lines. Lower its cap in \
                 architecture-baseline.toml to 7.",
            )],
        ),
        (
            "a listed file fell to the limit",
            None,
            &[(LISTED, 8)],
            &[(LISTED, LIMIT)],
            &[(
                "stale_entries",
                "crates/a/src/listed.rs is down to 5 code lines, within the 5-line limit. \
                 Remove it from architecture-baseline.toml.",
            )],
        ),
        (
            "a listed file is gone",
            None,
            &[(LISTED, 8)],
            &[],
            &[(
                "stale_entries",
                "crates/a/src/listed.rs: listed in architecture-baseline.toml but no longer a \
                 production Rust file. Remove the entry; a renamed file is checked under its new \
                 path.",
            )],
        ),
        (
            "a listed file was renamed",
            None,
            &[(LISTED, 8)],
            &[(NEW, 8)],
            &[
                ("no_crossing", "crates/b/src/new.rs: 8 code lines, over"),
                ("stale_entries", "crates/a/src/listed.rs: listed in"),
            ],
        ),
        (
            "a key that names no production file",
            None,
            &[("./crates/a/src/listed.rs", 8)],
            &[(LISTED, 8)],
            &[
                ("no_crossing", "crates/a/src/listed.rs: 8 code lines, over"),
                ("stale_entries", "./crates/a/src/listed.rs: listed in"),
            ],
        ),
        (
            "an entry was added",
            Some(&[]),
            &[(NEW, 6)],
            &[(NEW, 6)],
            &[(
                "no_new_entries",
                "crates/b/src/new.rs is in architecture-baseline.toml but not in the base branch's \
                 copy; files over the 5-line limit can't be added (a renamed file counts as \
                 added). Split the file instead.",
            )],
        ),
        (
            "an entry was added without --base-baseline",
            None,
            &[(NEW, 6)],
            &[(NEW, 6)],
            &[],
        ),
        (
            "a listed file was renamed and its entry with it",
            Some(&[(LISTED, 8)]),
            &[(NEW, 8)],
            &[(NEW, 8)],
            &[(
                "no_new_entries",
                "crates/b/src/new.rs is in architecture-baseline.toml but",
            )],
        ),
        (
            "an entry was removed",
            Some(&[(LISTED, 8), (NEW, 6)]),
            &[(NEW, 6)],
            &[(LISTED, LIMIT), (NEW, 6)],
            &[],
        ),
        (
            "a cap was lowered",
            Some(&[(LISTED, 8)]),
            &[(LISTED, 7)],
            &[(LISTED, 7)],
            &[],
        ),
        (
            "a cap was raised",
            Some(&[(LISTED, 6)]),
            &[(LISTED, 7)],
            &[(LISTED, 7)],
            &[(
                "no_raised_caps",
                "crates/a/src/listed.rs: cap raised from 6 to 7 in architecture-baseline.toml; \
                 caps should only go down. Split the file instead.",
            )],
        ),
        (
            "an entry was added and a cap raised",
            Some(&[(LISTED, 6)]),
            &[(LISTED, 7), (NEW, 6)],
            &[(LISTED, 7), (NEW, 6)],
            &[
                ("no_new_entries", "crates/b/src/new.rs is in"),
                (
                    "no_raised_caps",
                    "crates/a/src/listed.rs: cap raised from 6 to 7",
                ),
            ],
        ),
        (
            "a cap was raised without --base-baseline",
            None,
            &[(LISTED, 7)],
            &[(LISTED, 7)],
            &[],
        ),
        (
            "a cap was lowered and another entry removed",
            Some(&[(LISTED, 8), (NEW, 9)]),
            &[(LISTED, 7)],
            &[(LISTED, 7), (NEW, LIMIT)],
            &[],
        ),
    ];
    for (name, base, entries, sizes, expected) in cases {
        let mut files: Vec<(&str, String)> = sizes
            .iter()
            .map(|(path, lines)| (*path, code(*lines)))
            .collect();
        files.push(("architecture-baseline.toml", baseline(entries)));
        let ws = workspace(&files);

        let found = findings_against(ws.path(), base.map(baseline).as_deref());

        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for ((behaviour, message), (want_behaviour, want_message)) in found.iter().zip(*expected) {
            assert_eq!(behaviour, want_behaviour, "{name}");
            assert!(message.starts_with(want_message), "{name}: {message}");
        }
    }
}

/// The base branch's copy is read leniently: only `[file_size]` matters, and
/// a copy that does not parse skips the comparison with a one-line warning.
/// The baseline lists `crates/a/src/listed.rs` (cap 8) and `crates/b/src/new.rs`.
#[test]
fn lenient_base_baseline() {
    let cases: &[(&str, &str, &[&str], Option<&str>)] = &[
        (
            "an unknown table and key",
            "version = 2\n[file_size]\n\"crates/a/src/listed.rs\" = 8\n[crate_names]\nx = \"y\"\n",
            &["no_new_entries"],
            None,
        ),
        (
            "no [file_size] table",
            "[crate_names]\n",
            &["no_new_entries", "no_new_entries"],
            None,
        ),
        (
            "an empty file",
            "",
            &["no_new_entries", "no_new_entries"],
            None,
        ),
        ("invalid TOML", "[file_size\n", &[], Some("at line 1")),
        (
            "a non-integer cap",
            "[file_size]\n\"crates/a/src/listed.rs\" = 8\n\"crates/b/src/new.rs\" = \"9\"\n",
            &[],
            Some("at line 3"),
        ),
        (
            "a negative cap",
            "[file_size]\n\"x\" = -1\n",
            &[],
            Some("at line 2"),
        ),
        (
            "[file_size] is not a table",
            "file_size = 3\n",
            &[],
            Some("at line 1"),
        ),
    ];
    for (name, base, expected, problem) in cases {
        let ws = workspace(&[
            (
                "architecture-baseline.toml",
                baseline(&[("crates/a/src/listed.rs", 8), ("crates/b/src/new.rs", 6)]),
            ),
            ("crates/a/src/listed.rs", code(8)),
            ("crates/b/src/new.rs", code(6)),
        ]);

        let ctx = load(ws.path(), Some(base));

        let found: Vec<_> = check(&ctx)
            .iter()
            .map(|finding| finding.behaviour)
            .collect();
        assert_eq!(found, *expected, "{name}");
        let skipped: Vec<String> = skipped(&ctx)
            .into_iter()
            .filter(|line| line.contains("file_size."))
            .collect();
        match problem {
            None => {
                assert!(
                    matches!(ctx.base_baseline, BaseBaseline::Loaded(_)),
                    "{name}"
                );
                assert_eq!(skipped, Vec::<String>::new(), "{name}");
            }
            Some(at) => {
                let path = ws.path().join("base-baseline.toml");
                let [line] = skipped.as_slice() else {
                    panic!("{name}: {skipped:?}");
                };
                let start = format!("warning: could not parse {} {at}: ", path.display());
                let end = "; file_size.no_new_entries and file_size.no_raised_caps skipped";
                assert!(
                    line.starts_with(&start) && line.ends_with(end) && !line.contains('\n'),
                    "{name}: {line}"
                );
            }
        }
    }
}

/// Pairs such as `(member, package name)` or `(behaviour, message)`.
type Pairs<'a> = &'a [(&'a str, &'a str)];

/// The base branch's `[crate_names]`, as a column of the tables below.
#[derive(Clone, Copy)]
enum Base<'a> {
    /// No `--base-baseline`, as when the base can't be resolved.
    Unresolved,
    /// A copy without the table, from before the rule.
    NoTable,
    Names(&'a [&'a str]),
}

impl Base<'_> {
    fn text(self) -> Option<String> {
        match self {
            Self::Unresolved => None,
            Self::NoTable => Some("[file_size]\n".to_owned()),
            Self::Names(names) => {
                let entries: Vec<(&str, &str)> =
                    names.iter().map(|name| (*name, "crates/x")).collect();
                Some(tables(&[], &entries))
            }
        }
    }
}

/// `(behaviour, message)` of each `crate_names` finding.
fn crate_findings(ctx: &Context) -> Vec<(&'static str, String)> {
    check(ctx)
        .into_iter()
        .filter(|finding| finding.rule == "crate_names")
        .map(|finding| (finding.behaviour, finding.message()))
        .collect()
}

fn assert_findings(name: &str, found: &[(&str, String)], expected: &[(&str, &str)]) {
    assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
    for ((behaviour, message), (want_behaviour, want_message)) in found.iter().zip(expected) {
        assert_eq!(behaviour, want_behaviour, "{name}");
        assert!(message.starts_with(want_message), "{name}: {message}");
    }
}

/// One row per situation: the base branch's `[crate_names]`, the baseline's
/// entries, the extra members as `(member, package name)`, and the expected
/// `(behaviour, start of message)` of each `crate_names` finding.
#[test]
fn crate_names_behaviours() {
    const LISTED: &str = "pplx-space-common-io";
    type Case<'a> = (
        &'a str,
        Base<'a>,
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str)],
    );
    let new = |member: &str, name: &str, token: &str| {
        format!("{member}/Cargo.toml: `{name}` uses `{token}`, a catch-all name.")
    };
    let underscores = new("crates/x", "pplx_space_foo_utils", "utils");
    let mixed_case = new("crates/x", "Foo-Utils", "utils");
    let mixed_separators = new("crates/x", "foo_Shared-io", "shared");
    let unprefixed = new("crates/x", "foo-core", "core");
    let first = new("crates/x", "pplx-space-common-core", "common");
    let renamed_to_utils = new("crates/x", "pplx-space-io-utils", "utils");
    let not_bootstrap = "pplx-space-foo-utils is in [crate_names] in architecture-baseline.toml \
                         but not one of the 16 crates listed when the rule was added, and there \
                         is no base branch copy of [crate_names] to compare with; catch-all crate \
                         names can't be added. Name the crate for what it does instead.";
    let elsewhere = "crates/new/Cargo.toml: `pplx-space-common-io` is listed in \
                     architecture-baseline.toml at crates/old, but this crate is at crates/new. \
                     An entry only covers the crate at its path: if the listed crate moved here, \
                     update the entry's path; otherwise name this crate for what it does.";
    let foo_utils = [("crates/libs/foo-utils", "pplx-space-foo-utils")];
    let foo_utils_entry = [("pplx-space-foo-utils", "crates/libs/foo-utils")];
    let cases: &[Case] = &[
        (
            "a new catch-all crate",
            Base::Unresolved,
            &[],
            &foo_utils,
            &[(
                "no_new",
                "crates/libs/foo-utils/Cargo.toml: `pplx-space-foo-utils` uses `utils`, a \
                 catch-all name. Name the crate for what it does, and split it if it holds \
                 unrelated things. Existing catch-all crates are listed in \
                 architecture-baseline.toml; new ones can't be added.",
            )],
        ),
        (
            "underscores",
            Base::Unresolved,
            &[],
            &[("crates/x", "pplx_space_foo_utils")],
            &[("no_new", &underscores)],
        ),
        (
            "mixed case",
            Base::Unresolved,
            &[],
            &[("crates/x", "Foo-Utils")],
            &[("no_new", &mixed_case)],
        ),
        (
            "mixed separators",
            Base::Unresolved,
            &[],
            &[("crates/x", "foo_Shared-io")],
            &[("no_new", &mixed_separators)],
        ),
        (
            "a name without the pplx-space- prefix",
            Base::Unresolved,
            &[],
            &[("crates/x", "foo-core")],
            &[("no_new", &unprefixed)],
        ),
        (
            "the first matching part is named",
            Base::Unresolved,
            &[],
            &[("crates/x", "pplx-space-common-core")],
            &[("no_new", &first)],
        ),
        (
            "whole parts only",
            Base::Unresolved,
            &[],
            &[
                ("crates/p0", "pplx-space-database"),
                ("crates/p1", "pplx-space-coreutils"),
                ("crates/p2", "encore"),
                ("crates/p3", "pplx-space-uncommon"),
                ("crates/p4", "utility_helpersets"),
            ],
            &[],
        ),
        (
            "a listed crate",
            Base::Names(&[LISTED]),
            &[(LISTED, "crates/x")],
            &[("crates/x", LISTED)],
            &[],
        ),
        (
            "an entry was added",
            Base::Names(&[]),
            &[(LISTED, "crates/x")],
            &[("crates/x", LISTED)],
            &[(
                "no_new",
                "pplx-space-common-io is in [crate_names] in architecture-baseline.toml but not \
                 in the base branch's copy; catch-all crate names can't be added. Name the crate \
                 for what it does instead.",
            )],
        ),
        (
            "a new crate with an entry of its own",
            Base::Names(&[]),
            &foo_utils_entry,
            &foo_utils,
            &[("no_new", "pplx-space-foo-utils is in [crate_names]")],
        ),
        (
            "an entry in the base, though not a bootstrap crate",
            Base::Names(&["pplx-space-foo-utils"]),
            &foo_utils_entry,
            &foo_utils,
            &[],
        ),
        (
            "no base table: a bootstrap entry",
            Base::NoTable,
            &[(LISTED, "crates/x")],
            &[("crates/x", LISTED)],
            &[],
        ),
        (
            "no base table: any other entry",
            Base::NoTable,
            &foo_utils_entry,
            &foo_utils,
            &[("no_new", not_bootstrap)],
        ),
        (
            "an unresolved base: a bootstrap entry",
            Base::Unresolved,
            &[(LISTED, "crates/x")],
            &[("crates/x", LISTED)],
            &[],
        ),
        (
            "an unresolved base: any other entry",
            Base::Unresolved,
            &foo_utils_entry,
            &foo_utils,
            &[("no_new", not_bootstrap)],
        ),
        (
            "an entry was removed",
            Base::Names(&[LISTED, "pplx-space-misc"]),
            &[(LISTED, "crates/x")],
            &[("crates/x", LISTED)],
            &[],
        ),
        (
            "a listed name at another path",
            Base::Names(&[LISTED]),
            &[(LISTED, "crates/old")],
            &[("crates/new", LISTED)],
            &[("no_new", elsewhere)],
        ),
        (
            "a listed crate moved, and its entry's path with it",
            Base::Names(&[LISTED]),
            &[(LISTED, "crates/new")],
            &[("crates/new", LISTED)],
            &[],
        ),
        (
            "a listed crate was renamed to a clean name",
            Base::Names(&[LISTED]),
            &[(LISTED, "crates/x")],
            &[("crates/x", "pplx-space-io-buffers")],
            &[(
                "stale_entries",
                "pplx-space-common-io: listed in architecture-baseline.toml but no workspace \
                 member has this name. Remove the entry; a renamed crate is checked under its new \
                 name.",
            )],
        ),
        (
            "a listed crate was removed",
            Base::Names(&[LISTED]),
            &[(LISTED, "crates/x")],
            &[],
            &[("stale_entries", "pplx-space-common-io: listed in")],
        ),
        (
            "a listed crate was renamed to another catch-all name",
            Base::Names(&[LISTED]),
            &[(LISTED, "crates/x")],
            &[("crates/x", "pplx-space-io-utils")],
            &[
                ("no_new", &renamed_to_utils),
                ("stale_entries", "pplx-space-common-io: listed in"),
            ],
        ),
        (
            "a listed name that is not a catch-all name",
            Base::Names(&["pplx-space-io-buffers"]),
            &[("pplx-space-io-buffers", "crates/x")],
            &[("crates/x", "pplx-space-io-buffers")],
            &[(
                "stale_entries",
                "pplx-space-io-buffers: listed in architecture-baseline.toml but no longer a \
                 catch-all name. Remove the entry.",
            )],
        ),
    ];
    for (name, base, entries, packages, expected) in cases {
        let ws = crate_workspace(
            packages,
            &[("architecture-baseline.toml", tables(&[], entries))],
        );

        let found = crate_findings(&load(ws.path(), base.text().as_deref()));

        assert_findings(name, &found, expected);
    }
}

/// Cargo makes a path dependency inside the workspace a member, though the
/// root manifest's `members` does not list it: both rules check it.
#[test]
fn implicit_members() {
    let ws = workspace(&[
        (
            "crates/a/Cargo.toml",
            format!(
                "{}[dependencies]\nfoo = {{ path = \"../../libs/foo-utils\", package = \
                 \"pplx-space-foo-utils\" }}\n",
                package("a")
            ),
        ),
        ("libs/foo-utils/Cargo.toml", package("pplx-space-foo-utils")),
        ("libs/foo-utils/src/big.rs", code(LIMIT + 1)),
        ("libs/foo-utils/src/ok.rs", code(LIMIT)),
    ]);

    let ctx = load(ws.path(), None);

    let files: Vec<&str> = ctx.files.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(
        files,
        ["libs/foo-utils/src/big.rs", "libs/foo-utils/src/ok.rs"]
    );
    let found: Vec<_> = check(&ctx)
        .into_iter()
        .map(|finding| (finding.behaviour, finding.location.path))
        .collect();
    assert_eq!(
        found,
        [
            ("no_crossing", "libs/foo-utils/src/big.rs".to_owned()),
            ("no_new", "libs/foo-utils/Cargo.toml".to_owned()),
        ]
    );
    assert_eq!(run(ws.path(), &[]).code, 1);
}

/// The library's name is checked too, as written in `[lib] name` or derived
/// from the package name; the package name is reported first.
#[test]
fn crate_names_library_names() {
    let cases: &[(&str, &str, Pairs)] = &[
        (
            "pplx-space-io",
            "name = \"utils\"\n",
            &[(
                "no_new",
                "crates/x/Cargo.toml: the library of `pplx-space-io` is named `utils`, which uses \
                 `utils`, a catch-all name. Name the crate",
            )],
        ),
        (
            "pplx-space-io",
            "name = \"io_Helpers\"\n",
            &[(
                "no_new",
                "crates/x/Cargo.toml: the library of `pplx-space-io` is named",
            )],
        ),
        ("pplx-space-io", "name = \"io_buffers\"\n", &[]),
        ("pplx-space-io", "", &[]),
        (
            "pplx-space-misc-io",
            "",
            &[(
                "no_new",
                "crates/x/Cargo.toml: `pplx-space-misc-io` uses `misc`",
            )],
        ),
        (
            "pplx-space-misc-io",
            "name = \"utils\"\n",
            &[(
                "no_new",
                "crates/x/Cargo.toml: `pplx-space-misc-io` uses `misc`",
            )],
        ),
    ];
    for (package, lib, expected) in cases {
        let manifest = format!("[package]\nname = {package:?}\n[lib]\npath = \"lib.rs\"\n{lib}");
        let ws = crate_workspace(
            &[("crates/x", package)],
            &[("crates/x/Cargo.toml", manifest)],
        );

        let found = crate_findings(&load(ws.path(), None));

        assert_findings(&format!("{package} {lib}"), &found, expected);
    }

    // Shown on the `[lib] name` line.
    let ws = crate_workspace(
        &[("crates/x", "pplx-space-io")],
        &[(
            "crates/x/Cargo.toml",
            "[package]\nname = \"pplx-space-io\"\n\n[lib]\npath = \"lib.rs\"\nname = \"utils\"\n"
                .to_owned(),
        )],
    );
    let document = rdjson(ws.path(), None);
    assert_eq!(
        document["diagnostics"][0]["location"]["range"]["start"]["line"],
        6
    );
}

/// `allow` exempts exact package names; one that is also in the baseline is
/// a configuration error, and one that no longer applies is stale.
#[test]
fn crate_names_allow() {
    let shared = "pplx-space-shared-memory";
    let cases: &[(&str, &str, Pairs, Pairs)] = &[
        ("[\"pplx-space-shared-memory\"]", shared, &[], &[]),
        (
            "[\"pplx-space-shared\"]",
            shared,
            &[],
            &[
                (
                    "no_new",
                    "crates/x/Cargo.toml: `pplx-space-shared-memory` uses `shared`",
                ),
                (
                    "stale_entries",
                    "pplx-space-shared: in [rules.crate_names] allow in ARCHITECTURE.toml but no \
                     workspace member has this name. Remove it from allow.",
                ),
            ],
        ),
        (
            "[\"pplx-space-memory\"]",
            "pplx-space-memory",
            &[],
            &[(
                "stale_entries",
                "pplx-space-memory: in [rules.crate_names] allow in ARCHITECTURE.toml but not a \
                 catch-all name. Remove it from allow.",
            )],
        ),
    ];
    for (allow, package, entries, expected) in cases {
        let ws = crate_workspace(
            &[("crates/x", package)],
            &[
                ("ARCHITECTURE.toml", with_crate_names("allow", allow)),
                ("architecture-baseline.toml", tables(&[], entries)),
            ],
        );

        let found = crate_findings(&load(ws.path(), None));

        assert_findings(allow, &found, expected);
    }

    let ws = crate_workspace(
        &[("crates/x", shared)],
        &[
            (
                "ARCHITECTURE.toml",
                with_crate_names("allow", "[\"pplx-space-shared-memory\"]"),
            ),
            (
                "architecture-baseline.toml",
                tables(&[], &[(shared, "crates/x")]),
            ),
        ],
    );
    let error = Context::load(ws.path()).unwrap_err().to_string();
    assert!(
        error.starts_with(
            "pplx-space-shared-memory is in both [rules.crate_names] allow in ARCHITECTURE.toml \
             and [crate_names] in architecture-baseline.toml"
        ),
        "{error}"
    );
    assert_eq!(run(ws.path(), &[]).code, 2);
}

/// `policy_changed` compares `[rules.crate_names]` with the base branch's
/// `ARCHITECTURE.toml` and shows each difference on its line there; the
/// base copy is read leniently.
#[test]
fn crate_names_policy_changed() {
    let base = config(ADVISORY);
    let without_misc = base.replace(", \"misc\"", "");
    let allowing = base.replace(
        "allow = []",
        "allow = [\n  \"pplx-space-shared-memory\",\n]",
    );
    let advisory_no_new = base.replace(
        "[rules.crate_names.no_new]\nmode = \"required\"",
        "[rules.crate_names.no_new]\nmode = \"advisory\"",
    );
    let line_of =
        |text: &str, needle: &str| text.lines().position(|line| line.contains(needle)).unwrap() + 1;
    let tokens = line_of(&base, "banned_tokens");
    type Case<'a> = (&'a str, &'a str, String, &'a [(usize, &'a str)]);
    let cases: &[Case] = &[
        ("unchanged", &base, base.clone(), &[]),
        (
            "a token removed",
            &base,
            without_misc.clone(),
            &[(
                tokens,
                "ARCHITECTURE.toml: `misc` was removed from [rules.crate_names] banned_tokens, so \
                 crate names using it are no longer checked.",
            )],
        ),
        (
            "a token added",
            &without_misc,
            base.clone(),
            &[(
                tokens,
                "ARCHITECTURE.toml: `misc` was added to [rules.crate_names] banned_tokens.",
            )],
        ),
        (
            "an allow entry added",
            &base,
            allowing.clone(),
            &[(
                line_of(&allowing, "\"pplx-space-shared-memory\""),
                "ARCHITECTURE.toml: `pplx-space-shared-memory` was added to [rules.crate_names] \
                 allow, exempting it from the rule.",
            )],
        ),
        (
            "an allow entry removed",
            &allowing,
            base.clone(),
            &[(
                line_of(&base, "allow"),
                "ARCHITECTURE.toml: `pplx-space-shared-memory` was removed from \
                 [rules.crate_names] allow.",
            )],
        ),
        (
            "a mode changed",
            &base,
            advisory_no_new.clone(),
            &[(
                line_of(&advisory_no_new, "[rules.crate_names.no_new]") + 1,
                "ARCHITECTURE.toml: crate_names.no_new mode changed from required to advisory.",
            )],
        ),
        (
            "a base from before the rule",
            "[rules.file_size]\nlimit = 5\n",
            base.clone(),
            &[(
                tokens,
                "ARCHITECTURE.toml: [rules.crate_names] is not in the base branch's copy; the \
                 whole policy is new.",
            )],
        ),
        (
            "unknown keys and behaviours in the base",
            &format!("{base}extra = 1\n[rules.crate_names.retired]\nmode = \"advisory\"\n"),
            base.clone(),
            &[],
        ),
    ];
    for (name, base, policy, expected) in cases {
        let ws = crate_workspace(
            &[("crates/x", "pplx-space-shared-memory")],
            &[("ARCHITECTURE.toml", policy.clone())],
        );

        let ctx = load_with(ws.path(), None, Some(base));

        let found: Vec<_> = check(&ctx)
            .into_iter()
            .filter(|finding| finding.behaviour == "policy_changed")
            .map(|finding| (finding.location.line, finding.message(), finding.mode))
            .collect();
        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for ((line, message, mode), (want_line, want)) in found.iter().zip(*expected) {
            assert_eq!(line, want_line, "{name}");
            assert_eq!(message, want, "{name}");
            assert_eq!(*mode, pplx_space_archcheck::Mode::Advisory, "{name}");
        }
        assert!(
            !skipped(&ctx)
                .iter()
                .any(|line| line.contains("policy_changed")),
            "{name}"
        );
    }

    // A copy that does not parse skips the comparison with a warning, and so
    // does the lack of one; neither fails the check.
    let ws = workspace(&[("base.toml", "[rules\n".to_owned())]);
    let base = ws.path().join("base.toml");
    let run_unusable = run(ws.path(), &["--base-architecture", base.to_str().unwrap()]);
    assert_eq!(run_unusable.code, 0, "{}", run_unusable.stderr);
    let warning = format!(
        "archcheck: warning: could not parse {} at line 1: ",
        base.display()
    );
    let last = run_unusable.stdout.lines().last().unwrap();
    assert!(
        last.starts_with(&warning) && last.ends_with("; crate_names.policy_changed skipped"),
        "{}",
        run_unusable.stdout
    );
    assert!(run(ws.path(), &[]).stdout.ends_with(POLICY_SKIPPED));
    let missing = ws.path().join("missing.toml");
    let run_missing = run(
        ws.path(),
        &["--base-architecture", missing.to_str().unwrap()],
    );
    assert_eq!(run_missing.code, 2);
}

/// The base branch's `[crate_names]` is read on its own: other tables and the
/// values are ignored. Without the table, only the bootstrap crates may be
/// listed; a table that does not parse counts as empty, with a warning. The
/// baseline lists `pplx-space-common-io`, a bootstrap crate.
#[test]
fn crate_names_lenient_base_baseline() {
    /// Name, the base's copy, the `crate_names` behaviours found, the
    /// summary's line, and the exit status.
    type Case<'a> = (&'a str, String, &'a [&'a str], Option<&'a str>, i32);
    let listed = "[crate_names]\n\"pplx-space-common-io\" = \"crates/x\"\n";
    let missing = CRATE_NAMES_SKIPPED[0];
    let cases: &[Case] = &[
        ("the table", listed.to_owned(), &[], None, 0),
        (
            "other tables and keys",
            format!("version = 2\n[other]\nx = 1\n{listed}"),
            &[],
            None,
            0,
        ),
        (
            "values of another shape",
            "[crate_names]\n\"pplx-space-common-io\" = { path = 1 }\n".to_owned(),
            &[],
            None,
            0,
        ),
        (
            "a [file_size] that does not parse",
            format!("[file_size]\n\"x\" = \"9\"\n{listed}"),
            &[],
            None,
            0,
        ),
        ("an empty file", String::new(), &[], Some(missing), 0),
        (
            "no [crate_names] table",
            "[file_size]\n".to_owned(),
            &[],
            Some(missing),
            0,
        ),
        (
            "[crate_names] is not a table",
            "crate_names = 3\n".to_owned(),
            &["no_new"],
            Some("at line 1: "),
            1,
        ),
        (
            "invalid TOML",
            format!("{listed}[file_size\n"),
            &["no_new"],
            Some("at line 3: "),
            1,
        ),
    ];
    for (name, base, expected, line, code) in cases {
        let ws = crate_workspace(
            &[("crates/x", "pplx-space-common-io")],
            &[
                (
                    "architecture-baseline.toml",
                    format!("[file_size]\n{listed}"),
                ),
                ("base-baseline.toml", base.clone()),
            ],
        );

        let ctx = load(ws.path(), Some(base));

        let found: Vec<_> = check(&ctx)
            .iter()
            .filter(|finding| finding.rule == "crate_names")
            .map(|finding| finding.behaviour)
            .collect();
        assert_eq!(found, *expected, "{name}");
        // However the copy reads, policy_changed's skip is reported once.
        let policy: Vec<String> = skipped(&ctx)
            .into_iter()
            .filter(|line| line.contains("crate_names.policy_changed"))
            .collect();
        assert_eq!(
            policy,
            [POLICY_SKIPPED.trim_start_matches("archcheck: ").trim_end()],
            "{name}"
        );
        let skipped: Vec<String> = skipped(&ctx)
            .into_iter()
            .filter(|line| {
                line.contains("crate_names.no_new")
                    || matches!(ctx.base_baseline, BaseBaseline::Unusable(_))
                        && line.starts_with("warning: ")
            })
            .collect();
        match line {
            None => assert_eq!(skipped, Vec::<String>::new(), "{name}"),
            Some(at) if at.starts_with("at line") => {
                let path = ws.path().join("base-baseline.toml");
                let [line] = skipped.as_slice() else {
                    panic!("{name}: {skipped:?}");
                };
                let start = format!("warning: could not parse {} {at}", path.display());
                // A copy that is not TOML at all gets the one warning for
                // every comparison skipped; no_new's findings say why.
                let end = if matches!(ctx.base_baseline, BaseBaseline::Unusable(_)) {
                    "; file_size.no_new_entries and file_size.no_raised_caps skipped"
                } else {
                    "; crate_names.no_new counts every [crate_names] entry as added"
                };
                assert!(
                    line.starts_with(&start) && line.ends_with(end) && !line.contains('\n'),
                    "{name}: {line}"
                );
            }
            Some(line) => assert_eq!(skipped, [*line], "{name}"),
        }
        let base = ws.path().join("base-baseline.toml");
        let run = run(ws.path(), &["--base-baseline", base.to_str().unwrap()]);
        assert_eq!(run.code, *code, "{name}: {}{}", run.stdout, run.stderr);
    }
}

/// `--write-baseline` keeps each entry whose crate still matches, removes
/// the others, adds none, and leaves an entry whose crate is at another path
/// as it is. `[crate_names]` is written even when empty.
#[test]
fn crate_names_write_baseline() {
    let rows: &[(Pairs, Pairs, &str, &[&str])] = &[
        (
            &[
                ("crates/x", "pplx-space-common-io"),
                ("crates/new", "pplx-space-common-jobs"),
                ("crates/y", "pplx-space-io-buffers"),
                ("crates/libs/foo-utils", "pplx-space-foo-utils"),
            ],
            &[
                ("pplx-space-common-io", "crates/x"),
                ("pplx-space-common-jobs", "crates/old"),
                ("pplx-space-io-buffers", "crates/y"),
                ("pplx-space-gone-core", "crates/gone"),
            ],
            "[file_size]\n[crate_names]\npplx-space-common-io = \"crates/x\"\n\
             pplx-space-common-jobs = \"crates/old\"\n",
            &[
                "not added: pplx-space-foo-utils (crates/libs/foo-utils/Cargo.toml) uses \
                 `utils`, a catch-all name; name the crate for what it does",
                "not updated: pplx-space-common-jobs is listed at crates/old but is at \
                 crates/new; update the entry's path by hand if it moved",
            ],
        ),
        (
            &[("crates/y", "pplx-space-io-buffers")],
            &[("pplx-space-gone-core", "crates/gone")],
            "[file_size]\n[crate_names]\n",
            &[],
        ),
    ];
    for (packages, entries, expected, expected_not_added) in rows {
        let ws = crate_workspace(
            packages,
            &[("architecture-baseline.toml", tables(&[], entries))],
        );

        let written = write_baseline(ws.path()).unwrap();

        let mut not_added = written.not_added.clone();
        not_added.sort();
        assert_eq!(not_added, *expected_not_added);
        let text = fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap();
        assert!(
            text.contains("\n\n# [crate_names] maps each workspace package"),
            "{text}"
        );
        let body: String = text
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
            .map(|line| format!("{line}\n"))
            .collect();
        assert_eq!(body, *expected);

        let run = run(ws.path(), &["--write-baseline"]);
        assert_eq!(run.code, 0, "{}", run.stderr);
        assert_eq!(
            fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap(),
            text,
            "a generated baseline is written back unchanged"
        );
    }
}

/// `--write-baseline=crate_names` updates only `[crate_names]`, and lists no
/// entries to paste for a crate it did not add; `--write-baseline=file_size`
/// keeps `[crate_names]` as it is.
#[test]
fn crate_names_scoped_write_baseline() {
    const BASELINE: &str = "architecture-baseline.toml";
    let ws = crate_workspace(
        &[
            ("crates/x", "pplx-space-common-io"),
            ("crates/libs/foo-utils", "pplx-space-foo-utils"),
        ],
        &[
            (
                BASELINE,
                "[file_size]\n\"crates/a/src/big.rs\" = 9\n\n[crate_names]\n\
                 pplx-space-common-io = \"crates/x\"\npplx-space-gone-core = \"crates/gone\"\n"
                    .to_owned(),
            ),
            ("crates/a/src/big.rs", code(LIMIT + 1)),
        ],
    );

    let wrote = run(ws.path(), &["--write-baseline=crate_names"]);

    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    let output = write_output(&wrote.stdout);
    assert_eq!(
        output.not_added,
        [
            "not added: pplx-space-foo-utils (crates/libs/foo-utils/Cargo.toml) uses `utils`, a \
          catch-all name; name the crate for what it does"
        ]
    );
    assert!(
        output
            .summary
            .starts_with("archcheck: wrote 1 crate_names entries to ")
            && output.summary.ends_with("; other tables kept"),
        "{}",
        wrote.stdout
    );
    assert_eq!(output.entries, "");
    let after = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert!(
        after.contains("[file_size]\n\"crates/a/src/big.rs\" = 9\n"),
        "{after}"
    );
    assert!(
        after.ends_with("[crate_names]\npplx-space-common-io = \"crates/x\"\n"),
        "{after}"
    );

    let wrote = run(ws.path(), &["--write-baseline=file_size"]);

    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    let output = write_output(&wrote.stdout);
    assert!(
        output
            .summary
            .starts_with("archcheck: wrote 1 file_size entries to "),
        "{}",
        wrote.stdout
    );
    let kept = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert!(
        kept.contains("[file_size]\n\"crates/a/src/big.rs\" = 6\n"),
        "{kept}"
    );
    assert!(
        kept.ends_with("[crate_names]\npplx-space-common-io = \"crates/x\"\n"),
        "{kept}"
    );
}

/// A new catch-all crate is an `ERROR` on its manifest's `name =` line and
/// exits 1; a new baseline entry is one on the entry's line; a stale entry is
/// a `WARNING` there.
#[test]
fn crate_names_rdjson_and_exit_codes() {
    let packages = [
        ("crates/x", "pplx-space-common-io"),
        ("crates/libs/foo-utils", "pplx-space-foo-utils"),
    ];
    let baseline = "[file_size]\n\n[crate_names]\n# A comment.\n\"pplx-space-common-io\" = \
                    \"crates/x\"\n'pplx-space-gone-utils' = \"crates/gone\"\n";
    let ws = crate_workspace(
        &packages,
        &[
            (
                "crates/libs/foo-utils/Cargo.toml",
                format!("# A comment.\n\n{}", package("pplx-space-foo-utils")),
            ),
            ("architecture-baseline.toml", baseline.to_owned()),
        ],
    );
    let diagnostic = |path: &str, line: u64, severity: &str, behaviour: &str| {
        json!({
            "path": path,
            "line": line,
            "severity": severity,
            "code": format!("crate_names.{behaviour}"),
        })
    };

    let document = rdjson(
        ws.path(),
        Base::Names(&["pplx-space-gone-utils"]).text().as_deref(),
    );

    let diagnostics: Vec<Value> = document["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            json!({
                "path": d["location"]["path"],
                "line": d["location"]["range"]["start"]["line"],
                "severity": d["severity"],
                "code": d["code"]["value"],
            })
        })
        .collect();
    assert_eq!(
        diagnostics,
        [
            diagnostic("crates/libs/foo-utils/Cargo.toml", 4, "ERROR", "no_new"),
            diagnostic("architecture-baseline.toml", 5, "ERROR", "no_new"),
            diagnostic("architecture-baseline.toml", 6, "WARNING", "stale_entries"),
        ]
    );

    let run_human = run(ws.path(), &[]);
    assert_eq!(run_human.code, 1, "{}", run_human.stderr);
    assert!(
        run_human.stdout.contains(
            "crate_names.no_new (required):\n  crates/libs/foo-utils/Cargo.toml: \
             `pplx-space-foo-utils` uses `utils`"
        ),
        "{}",
        run_human.stdout
    );

    // Only the advisory stale entry left.
    let ws = crate_workspace(
        &packages[..1],
        &[
            ("architecture-baseline.toml", baseline.to_owned()),
            (
                "base.toml",
                tables(
                    &[],
                    &[
                        ("pplx-space-common-io", "crates/x"),
                        ("pplx-space-gone-utils", "crates/gone"),
                    ],
                ),
            ),
        ],
    );
    let base = ws.path().join("base.toml");
    let run_stale = run(
        ws.path(),
        &[
            "--format",
            "rdjson",
            "--base-baseline",
            base.to_str().unwrap(),
        ],
    );
    assert_eq!(run_stale.code, 0, "{}", run_stale.stderr);
    let document: Value = serde_json::from_str(&run_stale.stdout).unwrap();
    assert_eq!(document["diagnostics"][0]["severity"], "WARNING");
}

/// `paths` limits an instance to the files it matches: others are neither
/// counted nor kept in its table, and `allow` still applies inside them.
#[test]
fn pattern_ratchet_paths() {
    let config = format!(
        "{}{}",
        config(ADVISORY),
        ratchet(
            "old",
            &[r"\bold_table\b"],
            &["crates/a/src/allowed.rs"],
            ADVISORY
        ),
    )
    .replace(
        "allow = ['crates/a/src/allowed.rs']\n",
        "allow = ['crates/a/src/allowed.rs']\npaths = ['crates/a/**']\n",
    );
    type Case<'a> = (
        &'a str,
        &'a [(&'a str, usize)],
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str)],
    );
    let cases: &[Case] = &[
        (
            "a file inside the paths is checked",
            &[],
            &[("crates/a/src/x.rs", "old_table\n")],
            &[("no_crossing", "crates/a/src/x.rs: 1 code line matches")],
        ),
        (
            "a file outside the paths is not",
            &[],
            &[("crates/b/src/x.rs", "old_table\n")],
            &[],
        ),
        (
            "allowed paths inside the paths may match",
            &[],
            &[("crates/a/src/allowed.rs", "old_table\n")],
            &[],
        ),
        (
            "an entry for a file outside the paths is stale",
            &[("crates/b/src/x.rs", 1)],
            &[("crates/b/src/x.rs", "old_table\n")],
            &[(
                "stale_entries",
                "crates/b/src/x.rs is outside the paths [rules.pattern_ratchet.old] in \
                 ARCHITECTURE.toml checks. Remove it from [pattern_ratchet.old] in \
                 architecture-baseline.toml.",
            )],
        ),
    ];
    for (name, entries, files, expected) in cases {
        let mut files: Vec<(&str, String)> = files
            .iter()
            .map(|(path, text)| (*path, (*text).to_owned()))
            .collect();
        files.push(("ARCHITECTURE.toml", config.clone()));
        files.push((
            "architecture-baseline.toml",
            ratchet_baseline(&[("old", entries)]),
        ));
        let ws = workspace(&files);

        let found = ratchet_findings(ws.path());

        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for ((code, message), (want_behaviour, want_message)) in found.iter().zip(*expected) {
            assert_eq!(
                code,
                &format!("pattern_ratchet.old.{want_behaviour}"),
                "{name}"
            );
            assert!(message.starts_with(want_message), "{name}: {message}");
        }
    }

    // An empty `paths` would check nothing, so it is rejected; an empty
    // `allow` is still legal.
    let no_allow = config.replace("allow = ['crates/a/src/allowed.rs']", "allow = []");
    let baseline = ratchet_baseline(&[("old", &[])]);
    let ws = workspace(&[
        ("ARCHITECTURE.toml", no_allow.clone()),
        ("architecture-baseline.toml", baseline.clone()),
    ]);
    Context::load(ws.path()).expect("an empty allow list loads");
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            no_allow.replace("paths = ['crates/a/**']", "paths = []"),
        ),
        ("architecture-baseline.toml", baseline),
    ]);
    let error = Context::load(ws.path()).expect_err("an empty paths list is rejected");
    assert!(
        error
            .to_string()
            .contains("paths must list at least one glob; omit it to check every production file"),
        "{error}"
    );
}

/// A fenced member may not depend on a listed package by any name, of any
/// kind or for any target; an allowed or unchecked member may, and a package
/// whose name only starts with a listed one is not listed.
#[test]
fn dependency_fence() {
    let fence = "[rules.dependency_fence.driver]\n\
                 packages = [\"rusqlite\"]\n\
                 members = [\"crates/a\", \"crates/store\"]\n\
                 allow = [\"crates/store\"]\n\
                 [rules.dependency_fence.driver.no_crossing]\nmode = \"required\"\n";
    let manifest = |name: &str, dependencies: &str| format!("{}{dependencies}", package(name));
    type Case<'a> = (&'a str, &'a str, &'a [(&'a str, usize)]);
    let cases: &[Case] = &[
        (
            "a normal dependency",
            "[dependencies]\nrusqlite = \"0.39\"\n",
            &[("a depends on rusqlite (a normal dependency), which only", 6)],
        ),
        (
            "a renamed dependency",
            "[dependencies]\nsqlite = { package = \"rusqlite\", version = \"0.39\" }\n",
            &[(
                "a depends on rusqlite (renamed sqlite, a normal dependency)",
                6,
            )],
        ),
        (
            "a dev-dependency in a table of its own",
            "[dev-dependencies.rusqlite]\nversion = \"0.39\"\n",
            &[("a depends on rusqlite (a dev-dependency)", 5)],
        ),
        (
            "a build-dependency",
            "[build-dependencies]\nrusqlite = \"0.39\"\n",
            &[("a depends on rusqlite (a build-dependency)", 6)],
        ),
        (
            "a target's dependency",
            "[target.'cfg(unix)'.dependencies]\nrusqlite = \"0.39\"\n",
            &[(
                "a depends on rusqlite (a normal dependency, for target cfg(unix))",
                6,
            )],
        ),
        (
            "a renamed dev-dependency for one target",
            "[target.'cfg(unix)'.dev-dependencies]\nsqlite = { package = \"rusqlite\", version = \"0.39\" }\n",
            &[(
                "a depends on rusqlite (renamed sqlite, a dev-dependency, for target cfg(unix))",
                6,
            )],
        ),
        (
            "another package with a similar name",
            "[dependencies]\nrusqlite-macros = \"0.1\"\n",
            &[],
        ),
    ];
    for (name, dependencies, expected) in cases {
        let ws = crate_workspace(
            &[("crates/store", "store")],
            &[
                ("crates/a/Cargo.toml", manifest("a", dependencies)),
                ("crates/b/Cargo.toml", manifest("b", dependencies)),
                ("crates/store/Cargo.toml", manifest("store", dependencies)),
                ("ARCHITECTURE.toml", format!("{}{fence}", config(ADVISORY))),
            ],
        );
        let found: Vec<_> = check(&load(ws.path(), None))
            .into_iter()
            .filter(|finding| finding.rule == "dependency_fence")
            .collect();
        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for (finding, (message, line)) in found.iter().zip(*expected) {
            assert_eq!(
                finding.code(),
                "dependency_fence.driver.no_crossing",
                "{name}"
            );
            assert!(finding.mode.is_required(), "{name}");
            assert!(
                finding
                    .message()
                    .starts_with(&format!("crates/a/Cargo.toml: {message}")),
                "{name}: {}",
                finding.message()
            );
            assert_eq!(finding.location.path, "crates/a/Cargo.toml", "{name}");
            assert_eq!(finding.location.line, *line, "{name}");
        }
    }

    // Each list must name something, or the instance would check nothing.
    for (key, error) in [
        ("packages", "packages must list at least one package name"),
        ("members", "members must list at least one glob"),
    ] {
        let line = fence
            .lines()
            .find(|line| line.starts_with(&format!("{key} = ")))
            .expect("the fence has the key");
        let ws = workspace(&[(
            "ARCHITECTURE.toml",
            format!(
                "{}{}",
                config(ADVISORY),
                fence.replace(line, &format!("{key} = []"))
            ),
        )]);
        let found = Context::load(ws.path()).expect_err("an empty list is rejected");
        assert!(found.to_string().contains(error), "{key}: {found}");
    }

    // A glob that matches no workspace member checks or allows nothing, so it
    // is rejected; here `crates/store` is not a member.
    let matching = fence
        .replace(
            "members = [\"crates/a\", \"crates/store\"]",
            "members = [\"crates/a\"]",
        )
        .replace("allow = [\"crates/store\"]", "allow = [\"crates/a\"]");
    for key in ["members", "allow"] {
        let line = matching
            .lines()
            .find(|line| line.starts_with(&format!("{key} = ")))
            .expect("the fence has the key");
        let ws = workspace(&[(
            "ARCHITECTURE.toml",
            format!(
                "{}{}",
                config(ADVISORY),
                matching.replace(line, &format!("{key} = [\"crates/a\", \"crates/store\"]"))
            ),
        )]);
        let found = Context::load(ws.path()).expect_err("an unmatched glob is rejected");
        assert!(
            found.to_string().contains(&format!(
                "[rules.dependency_fence.driver] {key} in ARCHITECTURE.toml has \"crates/store\", \
                 which matches no workspace member"
            )),
            "{key}: {found}"
        );
    }
}

/// One finding per behaviour, each reported in its own mode; only required
/// findings exit 1. Without `--base-baseline`, no_new_entries is skipped and
/// the summary says so.
#[test]
fn modes_and_exit_codes() {
    let files = |modes: [&str; 5]| {
        vec![
            ("ARCHITECTURE.toml", config(modes)),
            (
                "architecture-baseline.toml",
                baseline(&[
                    ("crates/a/src/added.rs", 6),
                    ("crates/a/src/gone.rs", 9),
                    ("crates/a/src/grew.rs", 6),
                    ("crates/a/src/raised.rs", 7),
                ]),
            ),
            (
                "base-baseline.toml",
                baseline(&[
                    ("crates/a/src/gone.rs", 9),
                    ("crates/a/src/grew.rs", 6),
                    ("crates/a/src/raised.rs", 6),
                ]),
            ),
            ("crates/a/src/added.rs", code(6)),
            ("crates/a/src/grew.rs", code(7)),
            ("crates/a/src/raised.rs", code(7)),
            ("crates/b/src/new.rs", code(6)),
        ]
    };
    let cases: &[([&str; 5], i32)] = &[(ADVISORY, 0)];
    let required = (0..5).map(|index| {
        let mut modes = ADVISORY;
        modes[index] = "required";
        (modes, 1)
    });
    let cases: Vec<_> = cases.iter().copied().chain(required).collect();
    for (modes, expected) in &cases {
        let ws = workspace(&files(*modes));
        let base = ws.path().join("base-baseline.toml");

        let run = run(ws.path(), &["--base-baseline", base.to_str().unwrap()]);

        assert_eq!(
            run.code, *expected,
            "{modes:?}: {}{}",
            run.stdout, run.stderr
        );
        let headings: Vec<&str> = run
            .stdout
            .lines()
            .filter(|line| line.starts_with("file_size."))
            .collect();
        assert_eq!(
            headings,
            [
                format!("file_size.no_crossing ({}):", modes[0]),
                format!("file_size.no_new_entries ({}):", modes[1]),
                format!("file_size.no_raised_caps ({}):", modes[2]),
                format!("file_size.listed_no_growth ({}):", modes[3]),
                format!("file_size.stale_entries ({}):", modes[4]),
            ],
        );
    }

    let ws = workspace(&files([
        "advisory", "advisory", "advisory", "required", "advisory",
    ]));
    let growth = "crates/a/src/grew.rs grew from 6 to 7 code lines. Split the file before adding \
                  to it.";
    assert!(
        findings(ws.path())
            .iter()
            .any(|(_, message)| message == growth)
    );

    // Without --base-baseline, both are skipped even when required.
    let ws = workspace(&files([
        "advisory", "required", "required", "advisory", "advisory",
    ]));
    let run_without_base = run(ws.path(), &[]);
    assert_eq!(run_without_base.code, 0, "{}", run_without_base.stderr);
    assert!(!run_without_base.stdout.contains("no_new_entries ("));
    assert!(!run_without_base.stdout.contains("no_raised_caps ("));
    assert!(run_without_base.stdout.ends_with(SKIPPED));

    let clean = workspace(&[("ARCHITECTURE.toml", config(["required"; 5]))]);
    let skipped = SKIPPED;
    let summary = "archcheck: no findings (0 production Rust files checked)\n";
    let base = clean.path().join("architecture-baseline.toml");
    let architecture = clean.path().join("ARCHITECTURE.toml");
    for (args, stdout) in [
        (vec![], format!("{summary}{skipped}")),
        (
            vec!["--base-baseline", base.to_str().unwrap()],
            format!("{summary}{POLICY_SKIPPED}"),
        ),
        (
            vec![
                "--base-baseline",
                base.to_str().unwrap(),
                "--base-architecture",
                architecture.to_str().unwrap(),
            ],
            summary.to_owned(),
        ),
    ] {
        let run = run(clean.path(), &args);
        assert_eq!(run.code, 0, "{}", run.stderr);
        assert_eq!(run.stdout, stdout);
    }
}

#[test]
fn human_report() {
    let ws = workspace(&[
        (
            "architecture-baseline.toml",
            baseline(&[("crates/a/src/grew.rs", 6)]),
        ),
        ("crates/a/src/grew.rs", code(7)),
        ("crates/b/src/new.rs", code(6)),
    ]);

    let run = run(ws.path(), &[]);

    assert_eq!(run.code, 0, "{}", run.stderr);
    assert_eq!(
        run.stdout,
        "file_size.no_crossing (advisory):\n  \
         crates/b/src/new.rs: 6 code lines, over the 5-line limit (comments and blank lines are \
         not counted). Split it into submodules by responsibility.\n\
         file_size.listed_no_growth (advisory):\n  \
         crates/a/src/grew.rs grew from 6 to 7 code lines. Once this rule is required, split \
         the file before adding to it.\n\
         \n\
         archcheck: 2 findings (2 advisory, 0 required; 2 production Rust files checked)\n"
            .to_owned()
            + SKIPPED
    );
}

/// Limits are written with digit grouping; counts are not.
#[test]
fn messages_group_the_limit() {
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            config(ADVISORY).replace("limit = 5", "limit = 1000"),
        ),
        (
            "architecture-baseline.toml",
            baseline(&[("crates/a/src/listed.rs", 1002)]),
        ),
        ("crates/a/src/listed.rs", code(999)),
        ("crates/b/src/new.rs", code(1001)),
    ]);

    assert_eq!(
        findings(ws.path()),
        [
            (
                "no_crossing",
                "crates/b/src/new.rs: 1001 code lines, over the 1,000-line limit (comments and \
                 blank lines are not counted). Split it into submodules by responsibility."
                    .to_owned()
            ),
            (
                "stale_entries",
                "crates/a/src/listed.rs is down to 999 code lines, within the 1,000-line limit. \
                 Remove it from architecture-baseline.toml."
                    .to_owned()
            ),
        ]
    );
}

/// Each row replaces one file, or removes it with `None`; loading fails with
/// an error that names the file and the problem.
#[test]
fn configuration_errors() {
    const CONFIG: &str = "ARCHITECTURE.toml";
    const BASELINE: &str = "architecture-baseline.toml";
    const MANIFEST: &str = "Cargo.toml";
    let advisory = config(ADVISORY);
    let text = |value: &str| Some(value.to_owned());
    let members = |member: &str| {
        text(&format!(
            "[workspace]\nmembers = [\"crates/a\", {member:?}]\n"
        ))
    };
    let instance = |patterns: &[&str], allow: &[&str]| {
        format!("{advisory}{}", ratchet("old", patterns, allow, ADVISORY))
    };
    let valid_instance = instance(&[r"\bold_table\b"], &["crates/a/**"]);
    let parse = "could not parse";
    let cargo = "`cargo metadata` failed";
    let crate_names = |key: &str, value: &str| Some(with_crate_names(key, value));
    let tokens = |list: &str| crate_names("banned_tokens", list);
    let cases: &[(&str, &str, Option<String>, &str)] = &[
        (
            "unknown behaviour key",
            CONFIG,
            Some(format!("{advisory}[rules.file_size.no_crossing.extra]\n")),
            parse,
        ),
        (
            "unknown rule key",
            CONFIG,
            Some(advisory.replace("limit = 5", "limit = 5\nlimt = 5")),
            parse,
        ),
        (
            "unknown rule",
            CONFIG,
            Some(format!("{advisory}[rules.module_names]\n")),
            parse,
        ),
        (
            "unknown mode",
            CONFIG,
            Some(advisory.replacen("advisory", "warn", 1)),
            parse,
        ),
        (
            "missing behaviour",
            CONFIG,
            Some(advisory.replace("[rules.file_size.stale_entries]\nmode = \"advisory\"\n", "")),
            parse,
        ),
        (
            "empty banned tokens",
            CONFIG,
            tokens("[]"),
            "banned_tokens is empty",
        ),
        (
            "banned token with -",
            CONFIG,
            tokens("[\"util\", \"foo-bar\"]"),
            "banned token \"foo-bar\" can never match",
        ),
        (
            "banned token with _",
            CONFIG,
            tokens("[\"foo_bar\"]"),
            "banned token \"foo_bar\" can never match",
        ),
        (
            "uppercase banned token",
            CONFIG,
            tokens("[\"Common\"]"),
            "banned token \"Common\" can never match",
        ),
        (
            "empty banned token",
            CONFIG,
            tokens("[\"\"]"),
            "banned token \"\" can never match",
        ),
        (
            "strip_prefix",
            CONFIG,
            Some(advisory.replace("allow = []", "allow = []\nstrip_prefix = \"x-\"")),
            "unknown field `strip_prefix`",
        ),
        (
            "missing allow",
            CONFIG,
            Some(advisory.replace("allow = []\n", "")),
            "missing field `allow`",
        ),
        (
            "uppercase allow entry",
            CONFIG,
            crate_names("allow", "[\"pplx-space-Shared\"]"),
            "allow entry \"pplx-space-Shared\" is not a lowercase package name",
        ),
        (
            "duplicate allow entry",
            CONFIG,
            crate_names("allow", "[\"x-shared\", \"y\", \"x-shared\"]"),
            "allow lists \"x-shared\" twice",
        ),
        (
            "missing policy_changed",
            CONFIG,
            Some(advisory.replace(
                "[rules.crate_names.policy_changed]\nmode = \"advisory\"\n",
                "",
            )),
            "missing field `policy_changed`",
        ),
        ("unparseable", CONFIG, text("[rules.file_size\n"), parse),
        (
            "bad regex",
            CONFIG,
            Some(instance(&["(old_table"], &[])),
            "regex parse error",
        ),
        (
            "no patterns",
            CONFIG,
            Some(instance(&[], &[])),
            "patterns must list at least one regex",
        ),
        (
            "bad glob",
            CONFIG,
            Some(instance(&["old_table"], &["crates/{a"])),
            "error parsing glob 'crates/{a'",
        ),
        (
            "unknown instance key",
            CONFIG,
            Some(valid_instance.replace("allow = [", "alow = []\nallow = [")),
            "unknown field `alow`",
        ),
        (
            "unknown instance behaviour key",
            CONFIG,
            Some(format!(
                "{valid_instance}[rules.pattern_ratchet.old.no_crossing.extra]\n"
            )),
            parse,
        ),
        (
            "missing instance behaviour",
            CONFIG,
            Some(valid_instance.replace(
                "[rules.pattern_ratchet.old.stale_entries]\nmode = \"advisory\"\n",
                "",
            )),
            "missing field `stale_entries`",
        ),
        (
            "missing patterns",
            CONFIG,
            Some(valid_instance.replace("patterns = ['\\bold_table\\b']\n", "")),
            "missing field `patterns`",
        ),
        (
            "instance name",
            CONFIG,
            Some(valid_instance.replace("pattern_ratchet.old", "pattern_ratchet.Old")),
            "instance name \"Old\" must be",
        ),
        ("missing", CONFIG, None, "could not read"),
        (
            "unknown baseline section",
            BASELINE,
            text("[file_size]\n[file_sizes]\n"),
            parse,
        ),
        (
            "non-integer cap",
            BASELINE,
            text("[file_size]\n\"crates/a/src/lib.rs\" = \"9\"\n"),
            parse,
        ),
        ("missing baseline", BASELINE, None, "could not read"),
        ("missing member", MANIFEST, members("crates/c"), cargo),
        (
            "baseline table for no instance",
            BASELINE,
            text("[file_size]\n[pattern_ratchet.gone]\n"),
            "[pattern_ratchet.gone] names no instance in ARCHITECTURE.toml",
        ),
        (
            "non-integer pattern_ratchet cap",
            BASELINE,
            text("[file_size]\n[pattern_ratchet.old]\n\"crates/a/src/lib.rs\" = \"1\"\n"),
            parse,
        ),
        (
            "unparseable root manifest",
            MANIFEST,
            text("[workspace\n"),
            cargo,
        ),
        (
            "root manifest without a workspace",
            MANIFEST,
            text("[x]\n"),
            cargo,
        ),
        (
            "member without a package name",
            "crates/a/Cargo.toml",
            text("[package]\nversion = \"0.1.0\"\n"),
            cargo,
        ),
        (
            "member without [package]",
            "crates/b/Cargo.toml",
            text("[lib]\n"),
            cargo,
        ),
        (
            "member without a target",
            "crates/b/Cargo.toml",
            text("[package]\nname = \"b\"\n"),
            "no targets specified",
        ),
    ];
    for (name, file, contents, problem) in cases {
        let ws = workspace(&[]);
        let path = ws.path().join(file);
        match contents {
            Some(contents) => fs::write(&path, contents).unwrap(),
            None => fs::remove_file(&path).unwrap(),
        }

        let error = Context::load(ws.path()).expect_err(name).to_string();

        assert!(error.contains(file), "{name}: {error}");
        assert!(error.contains(problem), "{name}: {error}");
    }

    let cases = [
        (
            "ARCHITECTURE.toml",
            "[rules.file_size\n",
            "could not parse ",
        ),
        (
            "ARCHITECTURE.toml",
            &instance(&["(old_table"], &[]),
            "regex parse error",
        ),
        (
            "ARCHITECTURE.toml",
            &instance(&["old_table"], &["crates/{a"]),
            "error parsing glob",
        ),
        (
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/zz\"]\n",
            "`cargo metadata` failed in ",
        ),
    ];
    for (file, contents, problem) in cases {
        let ws = workspace(&[(file, contents.to_owned())]);
        for format in ["human", "rdjson"] {
            let run = run(ws.path(), &["--format", format]);
            assert_eq!(run.code, 2, "{file} {format}");
            assert_eq!(run.stdout, "", "{file} {format}");
            assert!(
                run.stderr.starts_with("archcheck: error: ") && run.stderr.contains(problem),
                "{}",
                run.stderr
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn a_rust_file_name_that_is_not_utf8_exits_2() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    let ws = workspace(&[]);
    let src = ws.path().join("crates/a/src");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join(OsStr::from_bytes(b"\xff.rs")), "").unwrap();

    let run = run(ws.path(), &[]);

    assert_eq!(run.code, 2, "{}", run.stdout);
    assert!(
        run.stderr.contains("\\xFF.rs\" is not valid UTF-8"),
        "{}",
        run.stderr
    );
}

/// `--write-baseline` keeps the entries still over the limit at their
/// current size and removes the rest; a file without an entry is listed, not
/// added.
#[test]
fn write_baseline_output() {
    type Sizes<'a> = &'a [(&'a str, usize)];
    let not_added = |path: &str, lines: usize| {
        format!(
            "not added: {path} ({lines} code lines); split it, or add an entry by hand if this \
             is a deliberate exception"
        )
    };
    let cases: &[(Sizes, Sizes, &str, Vec<String>)] = &[
        (
            &[
                ("crates/a/src/grew.rs", LIMIT + 1),
                ("crates/a/src/shrank.rs", LIMIT + 4),
                ("crates/a/src/same.rs", LIMIT + 2),
                ("crates/a/src/fell.rs", LIMIT + 3),
                ("crates/a/src/gone.rs", LIMIT + 3),
                ("./crates/a/src/same.rs", LIMIT + 2),
            ],
            &[
                ("crates/a/src/grew.rs", LIMIT + 3),
                ("crates/a/src/shrank.rs", LIMIT + 1),
                ("crates/a/src/same.rs", LIMIT + 2),
                ("crates/a/src/fell.rs", LIMIT),
                ("crates/b/src/new.rs", LIMIT + 7),
                ("crates/a/src/ok.rs", LIMIT),
                ("crates/a/src/tests.rs", LIMIT + 9),
            ],
            "[file_size]\n\"crates/a/src/grew.rs\" = 8\n\"crates/a/src/same.rs\" = 7\n\
             \"crates/a/src/shrank.rs\" = 6\n[crate_names]\n",
            vec![not_added("crates/b/src/new.rs", 12)],
        ),
        (
            &[],
            &[("crates/a/src/ok.rs", LIMIT)],
            "[file_size]\n[crate_names]\n",
            vec![],
        ),
    ];
    for (entries, sizes, expected, expected_not_added) in cases {
        let mut files: Vec<(&str, String)> = sizes
            .iter()
            .map(|(path, lines)| (*path, code(*lines)))
            .collect();
        files.push(("architecture-baseline.toml", baseline(entries)));
        let ws = workspace(&files);

        let written = write_baseline(ws.path()).unwrap();

        assert_eq!(written.not_added, *expected_not_added);
        let written = fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap();
        assert!(written.starts_with("# Existing violations"), "{written}");
        let body: String = written
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
            .map(|line| format!("{line}\n"))
            .collect();
        assert_eq!(body, *expected);
        let left: Vec<_> = findings(ws.path())
            .into_iter()
            .map(|(behaviour, _)| behaviour)
            .collect();
        let expected_left = vec!["no_crossing"; expected_not_added.len()];
        assert_eq!(left, expected_left);
    }

    // The old baseline is read, so one that does not parse is left alone.
    let ws = workspace(&[("architecture-baseline.toml", "not toml".to_owned())]);
    let error = write_baseline(ws.path()).unwrap_err().to_string();
    assert!(error.contains("could not parse"), "{error}");
    let left = fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap();
    assert_eq!(left, "not toml");

    let ws = workspace(&[
        (
            "architecture-baseline.toml",
            baseline(&[("crates/a/src/listed.rs", LIMIT + 1)]),
        ),
        ("crates/a/src/listed.rs", code(LIMIT + 2)),
        ("crates/a/src/big.rs", code(LIMIT + 1)),
    ]);
    let run = run(ws.path(), &["--write-baseline"]);
    assert_eq!(run.code, 0, "{}", run.stderr);
    let (listed, wrote) = run.stdout.split_once('\n').unwrap();
    assert_eq!(listed, not_added("crates/a/src/big.rs", 6));
    assert!(
        wrote.starts_with("archcheck: wrote 1 file_size, 0 crate_names entries to "),
        "{}",
        run.stdout
    );
}

/// A `--base-baseline` that cannot be read is an error; one that does not
/// parse is a warning after the summary, with the comparison skipped.
#[test]
fn base_baseline_problems() {
    let cases: &[(&str, &str, Option<&str>, i32, &str)] = &[
        ("missing", "base.toml", None, 2, "could not read"),
        ("a directory", "crates", None, 2, "could not read"),
        (
            "unparseable",
            "base.toml",
            Some("[file_size\n"),
            0,
            "could not parse",
        ),
        (
            "non-integer cap",
            "base.toml",
            Some("[file_size]\n\"crates/a/src/lib.rs\" = \"9\"\n"),
            0,
            "could not parse",
        ),
    ];
    for (name, file, contents, code, problem) in cases {
        let ws = workspace(&[]);
        let path = ws.path().join(file);
        if let Some(contents) = contents {
            fs::write(&path, contents).unwrap();
        }
        let path = path.to_str().unwrap();
        for format in ["human", "rdjson"] {
            let run = run(ws.path(), &["--format", format, "--base-baseline", path]);

            assert_eq!(run.code, *code, "{name} {format}: {}", run.stderr);
            if *code == 2 {
                assert_eq!(run.stdout, "", "{name} {format}");
                assert!(
                    run.stderr.starts_with("archcheck: error: ")
                        && run.stderr.contains(path)
                        && run.stderr.contains(problem),
                    "{name}: {}",
                    run.stderr
                );
                continue;
            }
            let summary = if format == "human" {
                run.stdout
            } else {
                serde_json::from_str::<Value>(&run.stdout).expect("one JSON document");
                run.stderr
            };
            let warning = summary
                .lines()
                .find(|line| line.contains("file_size."))
                .unwrap_or_default();
            assert!(
                warning.starts_with(&format!("archcheck: warning: {problem} {path} at line "))
                    && warning.ends_with("file_size.no_raised_caps skipped"),
                "{name} {format}: {summary}"
            );
        }
    }
}

/// A finding is an `ERROR` in required mode and a `WARNING` in advisory
/// mode, on line 1 of its file; a new entry, a raised cap, and a stale entry
/// whose file is gone point at the entry's line in the baseline.
#[test]
fn rdjson_document() {
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            config(["required", "advisory", "advisory", "advisory", "advisory"]),
        ),
        (
            "architecture-baseline.toml",
            "# A comment.\n\n[file_size]\n\"crates/a/src/grew.rs\" = 6\n\n\
             'crates/a/src/gone.rs' = 9\n\"crates/a/src/added.rs\" = 6\n\
             \"crates/a/src/raised.rs\" = 7\n"
                .to_owned(),
        ),
        ("crates/a/src/added.rs", code(6)),
        ("crates/a/src/raised.rs", code(7)),
        ("crates/a/src/grew.rs", code(7)),
        ("crates/b/src/new.rs", code(6)),
    ]);
    let diagnostic = |message: &str, path: &str, line: u64, severity: &str, behaviour: &str| {
        json!({
            "message": message,
            "location": {"path": path, "range": {"start": {"line": line}}},
            "severity": severity,
            "code": {"value": format!("file_size.{behaviour}")},
        })
    };

    let base = baseline(&[
        ("crates/a/src/gone.rs", 9),
        ("crates/a/src/grew.rs", 6),
        ("crates/a/src/raised.rs", 6),
    ]);
    assert_eq!(
        rdjson(ws.path(), Some(&base)),
        json!({
            "source": {"name": "archcheck"},
            "diagnostics": [
                diagnostic(
                    "crates/b/src/new.rs: 6 code lines, over the 5-line limit (comments and blank \
                     lines are not counted). Split it into submodules by responsibility.",
                    "crates/b/src/new.rs", 1, "ERROR", "no_crossing",
                ),
                diagnostic(
                    "crates/a/src/added.rs is in architecture-baseline.toml but not in the base \
                     branch's copy; files over the 5-line limit can't be added (a renamed file \
                     counts as added). Split the file instead.",
                    "architecture-baseline.toml", 7, "WARNING", "no_new_entries",
                ),
                diagnostic(
                    "crates/a/src/raised.rs: cap raised from 6 to 7 in architecture-baseline.toml; \
                     caps should only go down. Split the file instead.",
                    "architecture-baseline.toml", 8, "WARNING", "no_raised_caps",
                ),
                diagnostic(
                    "crates/a/src/grew.rs grew from 6 to 7 code lines. Once this rule is \
                     required, split the file before adding to it.",
                    "crates/a/src/grew.rs", 1, "WARNING", "listed_no_growth",
                ),
                diagnostic(
                    "crates/a/src/gone.rs: listed in architecture-baseline.toml but no longer a \
                     production Rust file. Remove the entry; a renamed file is checked under its \
                     new path.",
                    "architecture-baseline.toml", 6, "WARNING", "stale_entries",
                ),
            ],
        })
    );
    assert_eq!(
        render(Format::Rdjson, &[], 0),
        "{\"source\":{\"name\":\"archcheck\"},\"diagnostics\":[]}\n"
    );
}

/// reviewdog matches each path against the diff, so paths are exact.
#[cfg(unix)]
#[test]
fn rdjson_paths_are_exact() {
    let names = [
        "crates/a/src/q\"uote.rs",
        "crates/a/src/back\\slash.rs",
        "crates/a/src/tab\t.rs",
        "crates/a/src/new\nline.rs",
    ];
    let ws = workspace(&names.map(|name| (name, code(LIMIT + 1))));

    let document = rdjson(ws.path(), None);

    let mut paths: Vec<&str> = document["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|diagnostic| diagnostic["location"]["path"].as_str().unwrap())
        .collect();
    paths.sort_unstable();
    let mut expected = names.to_vec();
    expected.sort_unstable();
    assert_eq!(paths, expected);
}

#[test]
fn rdjson_on_stdout_and_the_summary_on_stderr() {
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            config(["required", "advisory", "advisory", "advisory", "advisory"]),
        ),
        ("crates/b/src/new.rs", code(6)),
    ]);

    let run = run(ws.path(), &["--format=rdjson"]);

    assert_eq!(run.code, 1, "{}", run.stderr);
    let document: Value = serde_json::from_str(&run.stdout).unwrap();
    assert_eq!(document["diagnostics"][0]["severity"], "ERROR");
    assert_eq!(
        run.stderr,
        "archcheck: 1 finding (0 advisory, 1 required; 1 production Rust files checked)\n"
            .to_owned()
            + SKIPPED
    );
}

#[test]
fn arguments() {
    let ws = workspace(&[]);
    // An empty expectation means nothing on stderr.
    let cases: &[(&[&str], i32, &str)] = &[
        (&["--help"], 0, ""),
        (&["-h"], 0, ""),
        (&["--format", "human"], 0, ""),
        (
            &["--format", "json"],
            2,
            "archcheck: unknown format \"json\"; expected human or rdjson",
        ),
        (&["--format", "github"], 2, "archcheck: unknown format"),
        (&["--format"], 2, "archcheck: --format needs a value"),
        (
            &["--base-baseline"],
            2,
            "archcheck: --base-baseline needs a value",
        ),
        (
            &["--write-baseline", "--base-baseline", "base.toml"],
            2,
            "archcheck: --base-baseline cannot be combined with --write-baseline",
        ),
        (
            &["--base-baseline=base.toml", "--write-baseline"],
            2,
            "archcheck: --base-baseline cannot be combined",
        ),
        (
            &["--write-baseline", "--base-architecture", "base.toml"],
            2,
            "archcheck: --base-architecture cannot be combined with --write-baseline",
        ),
        (
            &["--base-architecture"],
            2,
            "archcheck: --base-architecture needs a value",
        ),
        (
            &["--bogus"],
            2,
            "archcheck: unexpected argument \"--bogus\"",
        ),
        (
            &["--changed-files", "list"],
            2,
            "archcheck: unexpected argument",
        ),
        (
            &["--summary", "summary.md"],
            2,
            "archcheck: unexpected argument",
        ),
    ];
    for (args, code, stderr) in cases {
        let run = run(ws.path(), args);
        assert_eq!(run.code, *code, "{args:?}: {}", run.stderr);
        if stderr.is_empty() {
            assert_eq!(run.stderr, "", "{args:?}");
        } else {
            assert!(run.stderr.starts_with(stderr), "{args:?}: {}", run.stderr);
        }
        if run.code == 2 {
            assert!(run.stderr.contains("\nusage: archcheck "), "{args:?}");
        }
        if matches!(args[0], "-h" | "--help") {
            assert!(
                run.stdout.starts_with("usage: archcheck [--root <path>]")
                    && run.stdout.ends_with("never adds one\n"),
                "{args:?}: {}",
                run.stdout
            );
        }
    }

    // Without --root: the workspace containing the current directory.
    let in_dir = |dir: &Path| {
        Command::new(env!("CARGO_BIN_EXE_archcheck"))
            .current_dir(dir)
            .output()
            .expect("run archcheck")
            .status
            .code()
    };
    let nested = ws.path().join("crates/a/nested");
    fs::create_dir_all(&nested).unwrap();
    assert_eq!(in_dir(&nested), Some(0));
    assert_eq!(in_dir(tempfile::tempdir().unwrap().path()), Some(2));
}

/// A write to a full device exits 2 instead of panicking.
#[cfg(target_os = "linux")]
#[test]
fn output_write_errors_exit_2() {
    let full = || {
        fs::OpenOptions::new()
            .write(true)
            .open("/dev/full")
            .expect("open /dev/full")
    };
    let ws = workspace(&[("crates/b/src/new.rs", code(LIMIT + 1))]);
    let cases: &[&[&str]] = &[
        &[],
        &["--format", "rdjson"],
        &["--help"],
        &["--write-baseline"],
    ];
    for args in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_archcheck"))
            .arg("--root")
            .arg(ws.path())
            .args(*args)
            .stdout(full())
            .output()
            .expect("run archcheck");

        assert_eq!(output.status.code(), Some(2), "{args:?}");
        let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
        assert!(
            stderr.starts_with("archcheck: error: could not write the output: "),
            "{args:?}: {stderr}"
        );
    }

    // The rdjson summary goes to stderr; with nowhere to report, the exit
    // status still says so.
    let status = Command::new(env!("CARGO_BIN_EXE_archcheck"))
        .arg("--root")
        .arg(ws.path())
        .args(["--format", "rdjson"])
        .stdout(std::process::Stdio::null())
        .stderr(full())
        .status()
        .expect("run archcheck");
    assert_eq!(status.code(), Some(2));
}

/// The behaviours of `file_size` and of each `pattern_ratchet` instance, in
/// reporting order.
const BEHAVIOURS: [&str; 5] = [
    "no_crossing",
    "no_new_entries",
    "no_raised_caps",
    "listed_no_growth",
    "stale_entries",
];

const REQUIRED: [&str; 5] = ["required"; 5];

/// A `[rules.pattern_ratchet.<instance>]` table, with the mode of each of
/// [`BEHAVIOURS`] in `modes`.
fn ratchet(instance: &str, patterns: &[&str], allow: &[&str], modes: [&str; 5]) -> String {
    let list = |items: &[&str]| {
        items
            .iter()
            .map(|item| format!("'{item}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let table = format!("[rules.pattern_ratchet.{instance}");
    let behaviours: String = BEHAVIOURS
        .iter()
        .zip(modes)
        .map(|(behaviour, mode)| format!("{table}.{behaviour}]\nmode = \"{mode}\"\n"))
        .collect();
    format!(
        "{table}]\npatterns = [{}]\nallow = [{}]\n{behaviours}",
        list(patterns),
        list(allow),
    )
}

/// `ARCHITECTURE.toml` with advisory `file_size` and one instance, `old`,
/// restricting `old_table` and, in any case, `old_log` to
/// `crates/a/src/allowed.rs`, `crates/a/src/allowed/**` and
/// `crates/b/src/*.rs`.
fn old_tables_config(modes: [&str; 5]) -> String {
    format!(
        "{}{}",
        config(ADVISORY),
        ratchet(
            "old",
            &[r"\bold_table\b", r"(?i)\bold_log\b"],
            &[
                "crates/a/src/allowed.rs",
                "crates/a/src/allowed/**",
                "crates/b/src/*.rs"
            ],
            modes,
        ),
    )
}

/// An empty `[file_size]` table and one `[pattern_ratchet.<instance>]` table
/// per entry of `tables`.
fn ratchet_baseline(tables: &[(&str, &[(&str, usize)])]) -> String {
    let mut text = baseline(&[]);
    for (instance, entries) in tables {
        text.push_str(&format!("[pattern_ratchet.{instance}]\n"));
        for (path, cap) in *entries {
            text.push_str(&format!("{path:?} = {cap}\n"));
        }
    }
    text
}

/// `(code, message)` of every `pattern_ratchet` finding, in reporting order.
fn ratchet_findings(root: &Path) -> Vec<(String, String)> {
    let ctx = Context::load(root).expect("the workspace loads");
    check(&ctx)
        .into_iter()
        .filter(|finding| finding.rule == "pattern_ratchet")
        .map(|finding| (finding.code(), finding.message()))
        .collect()
}

/// What a `--write-baseline` run printed.
struct WriteOutput<'a> {
    not_added: Vec<&'a str>,
    summary: &'a str,
    /// The TOML of the entries not added, after its heading line.
    entries: String,
}

fn write_output(stdout: &str) -> WriteOutput<'_> {
    let mut lines = stdout.lines().peekable();
    let mut not_added = Vec::new();
    while let Some(line) = lines.next_if(|line| !line.starts_with("archcheck: ")) {
        not_added.push(line);
    }
    let summary = lines.next().expect("a summary line");
    let entries = match lines.next() {
        None => String::new(),
        Some(heading) => {
            assert_eq!(
                heading,
                "archcheck: the entries not added, for a deliberate exception added by hand \
                 (no_new_entries reports it):"
            );
            lines.map(|line| format!("{line}\n")).collect()
        }
    };
    WriteOutput {
        not_added,
        summary,
        entries,
    }
}

/// The comment `--write-baseline` prints before entries for `table`, which
/// the baseline has.
fn inside(table: &str) -> String {
    format!(
        "# add inside the existing [{table}] table, below its header; not at the end of the \
         file, which may be another table\n"
    )
}

/// `baseline` with `entries`, as `--write-baseline` printed them, pasted
/// where each block's comment says: inside the existing table it names,
/// below its header, or at the end of the file for a new table.
fn paste_entries(baseline: &str, entries: &str) -> String {
    const INSIDE: &str = "# add inside the existing [";
    const NEW: &str = "# add as a new table, header included, at the end of the file\n";
    let mut text = baseline.to_owned();
    let mut lines = entries.split_inclusive('\n').peekable();
    while let Some(comment) = lines.next() {
        let mut block = String::new();
        while let Some(line) = lines.next_if(|line| !line.starts_with('#')) {
            block.push_str(line);
        }
        if comment == NEW {
            text.push_str(&block);
            continue;
        }
        let (table, _) = comment
            .strip_prefix(INSIDE)
            .and_then(|rest| rest.split_once(']'))
            .expect("each block starts with a comment saying where it goes");
        let header = format!("[{table}]\n");
        let at = text
            .find(&header)
            .expect("the baseline has the table the comment names");
        text.insert_str(at + header.len(), &block);
    }
    text
}

/// One row per situation for the `old` instance: baseline entries, files,
/// and the expected `(behaviour, start of message)` of each finding.
#[test]
fn pattern_ratchet_behaviours() {
    const LISTED: &str = "crates/a/src/listed.rs";
    const NEW: &str = "crates/b/src/sub/new.rs";
    type Entries<'a> = &'a [(&'a str, usize)];
    type Files<'a> = &'a [(&'a str, &'a str)];
    type Case<'a> = (&'a str, Entries<'a>, Files<'a>, &'a [(&'a str, &'a str)]);
    let cases: &[Case] = &[
        (
            "a new file with matching lines",
            &[],
            &[(
                NEW,
                "fn f() {}\nlet t = \"old_table\";\n\nlet l = old_log;\n",
            )],
            &[(
                "no_crossing",
                "crates/b/src/sub/new.rs: 2 code lines match the old patterns, first on line 2, \
                 outside the paths [rules.pattern_ratchet.old] in ARCHITECTURE.toml allows. Move \
                 that code behind an allowed path. If the file was renamed, move its entry in \
                 [pattern_ratchet.old] in architecture-baseline.toml to the new path; \
                 no_new_entries reports the moved entry as new, for a reviewer to accept.",
            )],
        ),
        (
            "a line matching several patterns, several times, counts once",
            &[],
            &[(NEW, "old_table old_log old_table\n")],
            &[(
                "no_crossing",
                "crates/b/src/sub/new.rs: 1 code line matches the old",
            )],
        ),
        (
            "only whole identifiers match, case-sensitively by default",
            &[],
            &[(
                NEW,
                "old_table_id\nmy_old_table\nold_tables\nOLD_TABLE\nOLD_LOGS\n",
            )],
            &[],
        ),
        (
            "a (?i) pattern matches in any case",
            &[],
            &[(NEW, "OLD_LOG\nOld_Log\n")],
            &[(
                "no_crossing",
                "crates/b/src/sub/new.rs: 2 code lines match the old patterns, first on line 1,",
            )],
        ),
        (
            "qualified and quoted names are whole identifiers",
            &[],
            &[(NEW, "\"SELECT * FROM public.old_table\"\n")],
            &[(
                "no_crossing",
                "crates/b/src/sub/new.rs: 1 code line matches",
            )],
        ),
        (
            "comment lines do not count",
            &[],
            &[(NEW, "// old_table\n/// old_table\n  //! old_log\n")],
            &[],
        ),
        (
            "a trailing comment is part of its code line",
            &[],
            &[(NEW, "// old_table\nlet x = 1; // old_table\n")],
            &[(
                "no_crossing",
                "crates/b/src/sub/new.rs: 1 code line matches the old patterns, first on line 2,",
            )],
        ),
        (
            "allowed paths may match",
            &[],
            &[
                ("crates/a/src/allowed.rs", "old_table\n"),
                ("crates/a/src/allowed/deep/x.rs", "old_table\n"),
                ("crates/b/src/top.rs", "old_table\n"),
            ],
            &[],
        ),
        (
            "near misses of the allowed paths may not",
            &[],
            &[
                ("crates/a/src/allowed_more.rs", "old_table\n"),
                ("crates/b/src/sub/x.rs", "old_table\n"),
            ],
            &[
                ("no_crossing", "crates/a/src/allowed_more.rs: 1 code line"),
                ("no_crossing", "crates/b/src/sub/x.rs: 1 code line"),
            ],
        ),
        (
            "test files are not production files",
            &[],
            &[
                ("crates/a/src/tests.rs", "old_table\n"),
                ("crates/a/src/x_tests.rs", "old_table\n"),
                ("crates/a/src/m/tests/mod.rs", "old_table\n"),
                ("crates/a/tests/it.rs", "old_table\n"),
            ],
            &[],
        ),
        (
            "a listed file gained a matching line",
            &[(LISTED, 1)],
            &[(LISTED, "old_table\nold_log\n")],
            &[(
                "listed_no_growth",
                "crates/a/src/listed.rs grew from 1 to 2 code lines matching the old patterns. \
                 Once this rule is required, move new matches behind an allowed path.",
            )],
        ),
        (
            "a listed file at its cap",
            &[(LISTED, 2)],
            &[(LISTED, "old_table\nold_log\n")],
            &[],
        ),
        (
            "a listed file lost a matching line",
            &[(LISTED, 2)],
            &[(LISTED, "old_table\n")],
            &[(
                "stale_entries",
                "crates/a/src/listed.rs shrank from 2 to 1 code lines matching the old patterns. \
                 Lower its cap in [pattern_ratchet.old] in architecture-baseline.toml to 1.",
            )],
        ),
        (
            "a listed file has no matching lines left",
            &[(LISTED, 2)],
            &[(LISTED, "// old_table\n")],
            &[(
                "stale_entries",
                "crates/a/src/listed.rs has no code lines left matching the old patterns outside \
                 its allowed paths. Remove it from [pattern_ratchet.old] in \
                 architecture-baseline.toml.",
            )],
        ),
        (
            "a listed file is now allowed",
            &[("crates/a/src/allowed.rs", 1)],
            &[("crates/a/src/allowed.rs", "old_table\n")],
            &[(
                "stale_entries",
                "crates/a/src/allowed.rs has no code lines left",
            )],
        ),
        (
            "a listed file is gone",
            &[(LISTED, 1)],
            &[],
            &[(
                "stale_entries",
                "crates/a/src/listed.rs: listed in [pattern_ratchet.old] in \
                 architecture-baseline.toml but no longer a production Rust file. Remove the \
                 entry; a renamed file is checked under its new path.",
            )],
        ),
        (
            "a listed file became a test file",
            &[(LISTED, 1)],
            &[("crates/a/src/listed_tests.rs", "old_table\n")],
            &[("stale_entries", "crates/a/src/listed.rs: listed in")],
        ),
        (
            "a listed file was renamed",
            &[(LISTED, 1)],
            &[(NEW, "old_table\n")],
            &[
                (
                    "no_crossing",
                    "crates/b/src/sub/new.rs: 1 code line matches",
                ),
                ("stale_entries", "crates/a/src/listed.rs: listed in"),
            ],
        ),
    ];
    for (name, entries, files, expected) in cases {
        let mut files: Vec<(&str, String)> = files
            .iter()
            .map(|(path, text)| (*path, (*text).to_owned()))
            .collect();
        files.push(("ARCHITECTURE.toml", old_tables_config(ADVISORY)));
        files.push((
            "architecture-baseline.toml",
            ratchet_baseline(&[("old", entries)]),
        ));
        let ws = workspace(&files);

        let found = ratchet_findings(ws.path());

        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for ((code, message), (want_behaviour, want_message)) in found.iter().zip(*expected) {
            assert_eq!(
                code,
                &format!("pattern_ratchet.old.{want_behaviour}"),
                "{name}"
            );
            assert!(message.starts_with(want_message), "{name}: {message}");
        }
    }
}

/// One finding per behaviour, each reported in its own mode; only required
/// findings exit 1. Against the base branch's copy, gone.rs is a new entry
/// and grew.rs's cap was raised from 1 to 2.
#[test]
fn pattern_ratchet_modes_and_exit_codes() {
    let files = |modes: [&str; 5]| {
        vec![
            ("ARCHITECTURE.toml", old_tables_config(modes)),
            (
                "architecture-baseline.toml",
                ratchet_baseline(&[(
                    "old",
                    &[("crates/a/src/gone.rs", 1), ("crates/a/src/grew.rs", 2)],
                )]),
            ),
            (
                "base.toml",
                ratchet_baseline(&[("old", &[("crates/a/src/grew.rs", 1)])]),
            ),
            (
                "crates/a/src/grew.rs",
                "old_table\nold_log\nold_table\n".to_owned(),
            ),
            ("crates/b/src/sub/new.rs", "old_table\n".to_owned()),
        ]
    };
    let mut cases = vec![(ADVISORY, 0)];
    for required in 0..BEHAVIOURS.len() {
        let mut modes = ADVISORY;
        modes[required] = "required";
        cases.push((modes, 1));
    }
    for (modes, expected) in cases {
        let ws = workspace(&files(modes));
        let base = ws.path().join("base.toml");

        let run = run(ws.path(), &["--base-baseline", base.to_str().unwrap()]);

        assert_eq!(
            run.code, expected,
            "{modes:?}: {}{}",
            run.stdout, run.stderr
        );
        let headings: Vec<&str> = run
            .stdout
            .lines()
            .filter(|line| line.starts_with("pattern_ratchet."))
            .collect();
        let expected: Vec<String> = BEHAVIOURS
            .iter()
            .zip(modes)
            .map(|(behaviour, mode)| format!("pattern_ratchet.old.{behaviour} ({mode}):"))
            .collect();
        assert_eq!(headings, expected);
    }

    let mut modes = ADVISORY;
    modes[3] = "required";
    let ws = workspace(&files(modes));
    let growth = "crates/a/src/grew.rs grew from 2 to 3 code lines matching the old patterns. \
                  Move new matches behind an allowed path.";
    assert!(
        ratchet_findings(ws.path())
            .iter()
            .any(|(_, message)| message == growth)
    );
}

/// `no_new_entries` and `no_raised_caps` compare each instance's table with
/// the base branch's copy of it, read leniently and on its own: a table for
/// another instance is ignored, and one that does not parse skips only that
/// instance's comparisons, with a warning. The baseline lists
/// `crates/a/src/listed.rs` (cap 2) and `crates/a/src/kept.rs` (cap 1), at
/// their counts.
#[test]
fn pattern_ratchet_base_baseline() {
    const LISTED: &str = "crates/a/src/listed.rs";
    let new = "pattern_ratchet.old.no_new_entries";
    let raised = "pattern_ratchet.old.no_raised_caps";
    // The line's skipped comparisons, for `codes` separated by ", ".
    let both = |codes: &str| {
        let comparisons: Vec<String> = codes
            .split(", ")
            .map(|code| format!("{code}.no_new_entries and {code}.no_raised_caps"))
            .collect();
        format!("{} skipped", comparisons.join(", "))
    };
    // Name, base copy, finding codes, (code, position) of each skip, and
    // the base's tables for instances that are not configured.
    type Case<'a> = (
        &'a str,
        Option<&'a str>,
        &'a [&'a str],
        &'a [(&'a str, &'a str)],
        &'a [&'a str],
    );
    let cases: &[Case] = &[
        (
            "the same entries and caps",
            Some(
                "[pattern_ratchet.old]\n\"crates/a/src/listed.rs\" = 2\n\"crates/a/src/kept.rs\" = 1\n",
            ),
            &[],
            &[],
            &[],
        ),
        (
            "a new entry",
            Some("[pattern_ratchet.old]\n\"crates/a/src/kept.rs\" = 1\n"),
            &[new],
            &[],
            &[],
        ),
        (
            "a raised cap",
            Some(
                "[pattern_ratchet.old]\n\"crates/a/src/listed.rs\" = 1\n\"crates/a/src/kept.rs\" = 1\n",
            ),
            &[raised],
            &[],
            &[],
        ),
        (
            "a lowered cap and a removed entry",
            Some(
                "[pattern_ratchet.old]\n\"crates/a/src/listed.rs\" = 3\n\"crates/a/src/kept.rs\" = 1\n\
                 \"crates/a/src/gone.rs\" = 4\n",
            ),
            &[],
            &[],
            &[],
        ),
        (
            "no table for the instance",
            Some("[file_size]\n"),
            &[new, new],
            &[],
            &[],
        ),
        (
            "the instance renamed: the old name's table is ignored",
            Some(
                "[pattern_ratchet.older]\n\"crates/a/src/listed.rs\" = 2\n\"crates/a/src/kept.rs\" = 1\n",
            ),
            &[new, new],
            &[],
            &["older"],
        ),
        (
            "a table that does not parse, for an instance not configured",
            Some(
                "[pattern_ratchet.gone]\n\"x\" = \"1\"\n[pattern_ratchet.old]\n\
                 \"crates/a/src/listed.rs\" = 2\n\"crates/a/src/kept.rs\" = 1\n",
            ),
            &[],
            &[],
            &["gone"],
        ),
        (
            "the instance's table does not parse",
            Some(
                "[pattern_ratchet.old]\n\"crates/a/src/kept.rs\" = 1\n\"crates/a/src/listed.rs\" = \"2\"\n",
            ),
            &[],
            &[("pattern_ratchet.old", "at line 3")],
            &[],
        ),
        (
            "[file_size] does not parse: only file_size is skipped",
            Some("[file_size]\n\"x\" = -1\n[pattern_ratchet.old]\n\"crates/a/src/kept.rs\" = 1\n"),
            &[new],
            &[("file_size", "at line 2")],
            &[],
        ),
        (
            "pattern_ratchet is not a table",
            Some("pattern_ratchet = 3\n"),
            &[],
            &[("pattern_ratchet.old", "at line 1")],
            &[],
        ),
        (
            "the instance is not a table",
            Some("[pattern_ratchet]\nold = 3\n"),
            &[],
            &[("pattern_ratchet.old", "at line 2")],
            &[],
        ),
        (
            "not TOML",
            Some("[pattern_ratchet\n"),
            &[],
            // One line for both tables, not one each.
            &[("file_size, pattern_ratchet.old", "at line 1")],
            &[],
        ),
        (
            "no --base-baseline",
            None,
            &[],
            &[("file_size", ""), ("pattern_ratchet.old", "")],
            &[],
        ),
    ];
    for (name, base, expected, problems, ignored) in cases {
        let ws = workspace(&[
            ("ARCHITECTURE.toml", old_tables_config(ADVISORY)),
            (
                "architecture-baseline.toml",
                ratchet_baseline(&[("old", &[(LISTED, 2), ("crates/a/src/kept.rs", 1)])]),
            ),
            (LISTED, "old_table\nold_log\n".to_owned()),
            ("crates/a/src/kept.rs", "old_table\n".to_owned()),
        ]);

        let ctx = load(ws.path(), *base);

        let found: Vec<String> = check(&ctx).iter().map(|finding| finding.code()).collect();
        assert_eq!(found, *expected, "{name}");
        let path = ws.path().join("base-baseline.toml");
        let (info, skipped): (Vec<String>, Vec<String>) = skipped(&ctx)
            .into_iter()
            .filter(|line| !line.contains("crate_names."))
            .partition(|line| line.starts_with("base baseline has "));
        let expected_info: Vec<String> = ignored
            .iter()
            .map(|instance| {
                format!(
                    "base baseline has [pattern_ratchet.{instance}], which is not configured; \
                     skipped"
                )
            })
            .collect();
        assert_eq!(info, expected_info, "{name}");
        assert_eq!(skipped.len(), problems.len(), "{name}: {skipped:?}");
        for (line, (code, at)) in skipped.iter().zip(*problems) {
            match base {
                None => assert_eq!(
                    *line,
                    format!("{} (no --base-baseline given)", both(code)),
                    "{name}"
                ),
                Some(_) => assert!(
                    line.starts_with(&format!(
                        "warning: could not parse {} {at}: ",
                        path.display()
                    )) && line.ends_with(&format!("; {}", both(code)))
                        && !line.contains('\n'),
                    "{name}: {line}"
                ),
            }
        }
    }

    // The messages, for a new entry and a raised cap.
    let ws = workspace(&[
        ("ARCHITECTURE.toml", old_tables_config(ADVISORY)),
        (
            "architecture-baseline.toml",
            ratchet_baseline(&[("old", &[(LISTED, 2), ("crates/a/src/kept.rs", 1)])]),
        ),
        (LISTED, "old_table\nold_log\n".to_owned()),
        ("crates/a/src/kept.rs", "old_table\n".to_owned()),
    ]);
    let found = findings_against(
        ws.path(),
        Some("[pattern_ratchet.old]\n\"crates/a/src/listed.rs\" = 1\n"),
    );
    assert_eq!(
        found,
        [
            (
                "no_new_entries",
                "crates/a/src/kept.rs is in [pattern_ratchet.old] in architecture-baseline.toml \
                 but not in the base branch's copy; files outside the paths \
                 [rules.pattern_ratchet.old] in ARCHITECTURE.toml allows can't be added (a \
                 renamed file counts as added). Move that code behind an allowed path instead."
                    .to_owned()
            ),
            (
                "no_raised_caps",
                "crates/a/src/listed.rs: cap raised from 1 to 2 in [pattern_ratchet.old] in \
                 architecture-baseline.toml; caps should only go down. Move that code behind an \
                 allowed path instead."
                    .to_owned()
            ),
        ]
    );
}

/// `ARCHITECTURE.toml` with advisory `file_size` and instances restricting
/// `<instance>_table`, with no allowed paths.
fn instances_config(instances: &[&str]) -> String {
    let mut text = config(ADVISORY);
    for instance in instances {
        text.push_str(&ratchet(
            instance,
            &[&format!(r"\b{instance}_table\b")],
            &[],
            ADVISORY,
        ));
    }
    text
}

/// A base table that does not parse skips its own instance's comparisons
/// only: another configured instance is still compared, and reports both a
/// new entry and a raised cap.
#[test]
fn pattern_ratchet_base_table_that_does_not_parse_skips_only_its_instance() {
    let ws = workspace(&[
        ("ARCHITECTURE.toml", instances_config(&["alpha", "beta"])),
        (
            "architecture-baseline.toml",
            ratchet_baseline(&[
                ("alpha", &[("crates/a/src/a.rs", 1)]),
                (
                    "beta",
                    &[("crates/a/src/b.rs", 2), ("crates/a/src/c.rs", 1)],
                ),
            ]),
        ),
        ("crates/a/src/a.rs", "alpha_table\n".to_owned()),
        ("crates/a/src/b.rs", "beta_table\nbeta_table\n".to_owned()),
        ("crates/a/src/c.rs", "beta_table\n".to_owned()),
    ]);
    let base = "[pattern_ratchet.alpha]\n\"crates/a/src/a.rs\" = \"1\"\n\
                [pattern_ratchet.beta]\n\"crates/a/src/b.rs\" = 1\n";

    let ctx = load(ws.path(), Some(base));

    let found: Vec<(String, String)> = check(&ctx)
        .iter()
        .map(|finding| (finding.code(), finding.path.clone()))
        .collect();
    assert_eq!(
        found,
        [
            (
                "pattern_ratchet.beta.no_new_entries".to_owned(),
                "crates/a/src/c.rs".to_owned()
            ),
            (
                "pattern_ratchet.beta.no_raised_caps".to_owned(),
                "crates/a/src/b.rs".to_owned()
            ),
        ]
    );
    let path = ws.path().join("base-baseline.toml");
    assert_eq!(
        skipped(&ctx),
        [
            format!(
                "warning: could not parse {} at line 2: invalid type: string \"1\", expected \
                 usize; pattern_ratchet.alpha.no_new_entries and \
                 pattern_ratchet.alpha.no_raised_caps skipped",
                path.display()
            ),
            CRATE_NAMES_SKIPPED[0].to_owned(),
            CRATE_NAMES_SKIPPED[1].to_owned(),
        ]
    );
}

/// The skip lines, in order: `file_size`'s, then each configured instance's,
/// in configuration order, then one per table of an instance that is not
/// configured, in name order, whatever the order of the base copy.
#[test]
fn skip_lines_come_in_rule_then_instance_order() {
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            instances_config(&["alpha", "beta", "gamma"]),
        ),
        (
            "architecture-baseline.toml",
            ratchet_baseline(&[("alpha", &[]), ("beta", &[]), ("gamma", &[])]),
        ),
    ]);
    let base = "[pattern_ratchet.zeta]\n\
                [pattern_ratchet.gamma]\n\"a\" = -2\n\
                [pattern_ratchet.beta]\n\
                [pattern_ratchet.delta]\nx = \"y\"\n\
                [pattern_ratchet.alpha]\n\"a\" = \"1\"\n\
                [file_size]\n\"x\" = -1\n";

    let ctx = load(ws.path(), Some(base));

    let path = ws.path().join("base-baseline.toml");
    let warning = |line: usize, error: &str, code: &str| {
        format!(
            "warning: could not parse {} at line {line}: {error}; {code}.no_new_entries and \
             {code}.no_raised_caps skipped",
            path.display()
        )
    };
    assert_eq!(
        skipped(&ctx),
        [
            warning(
                10,
                "invalid value: integer `-1`, expected usize",
                "file_size"
            ),
            warning(
                8,
                "invalid type: string \"1\", expected usize",
                "pattern_ratchet.alpha"
            ),
            warning(
                3,
                "invalid value: integer `-2`, expected usize",
                "pattern_ratchet.gamma"
            ),
            "base baseline has [pattern_ratchet.delta], which is not configured; skipped"
                .to_owned(),
            "base baseline has [pattern_ratchet.zeta], which is not configured; skipped".to_owned(),
            CRATE_NAMES_SKIPPED[0].to_owned(),
            CRATE_NAMES_SKIPPED[1].to_owned(),
        ]
    );
}

/// Each instance reads only its own patterns, allowed paths, modes and
/// baseline table, and reports under its own code.
#[test]
fn pattern_ratchet_instances_are_independent() {
    let files = |w: &str| {
        vec![
            (
                "ARCHITECTURE.toml",
                format!(
                    "{}{}{}",
                    config(ADVISORY),
                    ratchet("alpha", &[r"\balpha_table\b"], &["crates/a/**"], ADVISORY,),
                    ratchet(
                        "beta",
                        &[r"\bbeta_table\b"],
                        &["crates/a/src/beta_home.rs"],
                        ["advisory", "advisory", "advisory", "required", "advisory"],
                    ),
                ),
            ),
            (
                "architecture-baseline.toml",
                ratchet_baseline(&[
                    ("alpha", &[("crates/b/src/y.rs", 3)]),
                    ("beta", &[("crates/b/src/w.rs", 1)]),
                ]),
            ),
            // alpha allows it; beta does not.
            (
                "crates/a/src/x.rs",
                "alpha_table\nbeta_table\nbeta_table\n".to_owned(),
            ),
            // At alpha's cap; alpha's entry does not cover beta.
            (
                "crates/b/src/y.rs",
                "alpha_table\nalpha_table\nalpha_table\nbeta_table\n".to_owned(),
            ),
            ("crates/b/src/z.rs", "alpha_table\n".to_owned()),
            ("crates/b/src/w.rs", w.to_owned()),
        ]
    };
    let ws = workspace(&files("beta_table\nbeta_table\n"));

    let run_grown = run(ws.path(), &[]);

    assert_eq!(
        run_grown.code, 1,
        "{}{}",
        run_grown.stdout, run_grown.stderr
    );
    let headings: Vec<&str> = run_grown
        .stdout
        .lines()
        .filter(|line| line.starts_with("pattern_ratchet."))
        .collect();
    assert_eq!(
        headings,
        [
            "pattern_ratchet.alpha.no_crossing (advisory):",
            "pattern_ratchet.beta.no_crossing (advisory):",
            "pattern_ratchet.beta.listed_no_growth (required):",
        ]
    );
    let found: Vec<(String, String)> = ratchet_findings(ws.path())
        .into_iter()
        .map(|(code, message)| (code, message.split(" outside").next().unwrap().to_owned()))
        .collect();
    let finding = |code: &str, message: &str| (code.to_owned(), message.to_owned());
    assert_eq!(
        found,
        [
            finding(
                "pattern_ratchet.alpha.no_crossing",
                "crates/b/src/z.rs: 1 code line matches the alpha patterns, first on line 1,",
            ),
            finding(
                "pattern_ratchet.beta.no_crossing",
                "crates/a/src/x.rs: 2 code lines match the beta patterns, first on line 2,",
            ),
            finding(
                "pattern_ratchet.beta.no_crossing",
                "crates/b/src/y.rs: 1 code line matches the beta patterns, first on line 4,",
            ),
            finding(
                "pattern_ratchet.beta.listed_no_growth",
                "crates/b/src/w.rs grew from 1 to 2 code lines matching the beta patterns. Move \
                 new matches behind an allowed path.",
            ),
        ]
    );

    // Without beta's required finding, alpha's advisory one does not fail.
    let ws = workspace(&files("beta_table\n"));
    let run_at_cap = run(ws.path(), &[]);
    assert_eq!(
        run_at_cap.code, 0,
        "{}{}",
        run_at_cap.stdout, run_at_cap.stderr
    );
    assert!(
        run_at_cap
            .stdout
            .contains("pattern_ratchet.alpha.no_crossing (advisory):"),
        "{}",
        run_at_cap.stdout
    );
}

/// `--write-baseline` updates every existing table of both rules, empty or
/// not: an entry still matching gets its current count, an entry at zero,
/// now allowed or whose file is gone is removed, and a matching file without
/// an entry is listed, not added.
#[test]
fn pattern_ratchet_write_baseline() {
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            format!(
                "{}{}",
                old_tables_config(REQUIRED),
                ratchet("unused", &[r"\bnowhere\b"], &[], REQUIRED,),
            ),
        ),
        (
            "architecture-baseline.toml",
            "[file_size]\n\"crates/b/src/sub/big.rs\" = 6\n\
             [pattern_ratchet.old]\n\"crates/a/src/z.rs\" = 1\n\"crates/a/src/one.rs\" = 3\n\
             \"crates/a/src/none.rs\" = 1\n\"crates/a/src/allowed.rs\" = 1\n\
             \"crates/a/src/gone.rs\" = 1\n\"crates/b/src/sub/big.rs\" = 2\n\
             [pattern_ratchet.unused]\n"
                .to_owned(),
        ),
        (
            "crates/a/src/z.rs",
            "old_table\n// old_log\nold_log\n".to_owned(),
        ),
        ("crates/a/src/one.rs", "old_log\n".to_owned()),
        ("crates/a/src/none.rs", "old_tables\n".to_owned()),
        ("crates/a/src/allowed.rs", "old_table\n".to_owned()),
        ("crates/a/src/tests.rs", "old_table\n".to_owned()),
        // Over the file_size limit, with matching lines.
        (
            "crates/b/src/sub/big.rs",
            format!("old_table\nold_log\n{}", code(LIMIT)),
        ),
        ("crates/a/src/new.rs", "old_table\nOLD_LOG\n".to_owned()),
    ]);

    let wrote = run(ws.path(), &["--write-baseline"]);

    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    let output = write_output(&wrote.stdout);
    assert_eq!(
        output.not_added,
        [
            "not added: crates/a/src/new.rs (2 code lines matching the old patterns); move them \
          behind an allowed path, or add an entry by hand if this is a deliberate exception"
        ]
    );
    assert!(
        output.summary.starts_with(
            "archcheck: wrote 1 file_size, 3 pattern_ratchet.old, 0 pattern_ratchet.unused, \
             0 crate_names entries to "
        ),
        "{}",
        wrote.stdout
    );
    assert_eq!(
        output.entries,
        format!(
            "{}\"crates/a/src/new.rs\" = 2\n",
            inside("pattern_ratchet.old")
        )
    );
    let written = fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap();
    let body: String = written
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| format!("{line}\n"))
        .collect();
    assert_eq!(
        body,
        "[file_size]\n\"crates/b/src/sub/big.rs\" = 7\n[pattern_ratchet.old]\n\
         \"crates/a/src/one.rs\" = 1\n\"crates/a/src/z.rs\" = 2\n\
         \"crates/b/src/sub/big.rs\" = 2\n[pattern_ratchet.unused]\n[crate_names]\n"
    );
    // Only the file left out is still a finding.
    let checked = run(ws.path(), &[]);
    assert_eq!(checked.code, 1, "{}{}", checked.stdout, checked.stderr);
    assert_eq!(codes(ws.path()), ["pattern_ratchet.old.no_crossing"]);
}

/// Neither form of `--write-baseline` creates the table of an instance the
/// baseline has none for: its matching files are listed, with the table to
/// add by hand, and the scoped form leaves the file alone.
#[test]
fn pattern_ratchet_write_creates_no_table() {
    const BASELINE: &str = "architecture-baseline.toml";
    let generated = "[file_size]\n[pattern_ratchet.old]\n\"crates/a/src/o.rs\" = 1\n";
    let hint = "architecture-baseline.toml has no [pattern_ratchet.fresh] table; for a new \
                instance, add the table and its entries by hand";
    let not_added = [
        format!("not added: crates/a/src/f.rs (1 code line matching the fresh patterns); {hint}"),
        format!("not added: crates/b/src/g.rs (2 code lines matching the fresh patterns); {hint}"),
    ];
    let entries = "# add as a new table, header included, at the end of the file\n\
                   [pattern_ratchet.fresh]\n\"crates/a/src/f.rs\" = 1\n\"crates/b/src/g.rs\" = 2\n";
    for scoped in [false, true] {
        let ws = workspace(&[
            (
                "ARCHITECTURE.toml",
                format!(
                    "{}{}",
                    old_tables_config(ADVISORY),
                    ratchet("fresh", &[r"\bfresh_table\b"], &[], ADVISORY),
                ),
            ),
            (BASELINE, generated.to_owned()),
            ("crates/a/src/o.rs", "old_table\n".to_owned()),
            ("crates/a/src/f.rs", "fresh_table\n".to_owned()),
            ("crates/b/src/g.rs", "fresh_table\nfresh_table\n".to_owned()),
        ]);
        let args: &[&str] = if scoped {
            &["--write-baseline=pattern_ratchet.fresh"]
        } else {
            &["--write-baseline"]
        };

        let wrote = run(ws.path(), args);

        assert_eq!(wrote.code, 0, "{args:?}: {}", wrote.stderr);
        let output = write_output(&wrote.stdout);
        assert_eq!(output.not_added, not_added, "{args:?}");
        let summary = if scoped {
            format!(
                "archcheck: {} has no [pattern_ratchet.fresh] table, which only a hand edit \
                 adds; left it alone",
                ws.path().join(BASELINE).display()
            )
        } else {
            format!(
                "archcheck: wrote 0 file_size, 1 pattern_ratchet.old, 0 crate_names entries to {}",
                ws.path().join(BASELINE).display()
            )
        };
        assert_eq!(output.summary, summary, "{args:?}");
        assert_eq!(output.entries, entries, "{args:?}");
        let written = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
        assert!(
            !written.contains("[pattern_ratchet.fresh]"),
            "{args:?}: {written}"
        );
        if scoped {
            assert_eq!(written, generated);
        }
        assert_eq!(
            codes(ws.path()),
            [
                "pattern_ratchet.fresh.no_crossing",
                "pattern_ratchet.fresh.no_crossing"
            ],
            "{args:?}"
        );

        // The listed entries, pasted as a new table, are what the instance
        // then tolerates.
        fs::write(ws.path().join(BASELINE), paste_entries(&written, entries)).unwrap();
        assert_eq!(codes(ws.path()), Vec::<String>::new(), "{args:?}");
    }
}

/// `no_crossing`, `listed_no_growth` and a shrank `stale_entries` point at
/// the file's first matching line, and a `stale_entries` for a file without
/// matches at its line 1, so that a pull request changing only the file sees
/// them; `no_new_entries`, `no_raised_caps` and a `stale_entries` for a file
/// that is gone point at the entry's line in the baseline.
#[test]
fn pattern_ratchet_rdjson() {
    let mut modes = ADVISORY;
    modes[0] = "required";
    let ws = workspace(&[
        ("ARCHITECTURE.toml", old_tables_config(modes)),
        (
            "architecture-baseline.toml",
            "[file_size]\n\n[pattern_ratchet.old]\n\"crates/a/src/grew.rs\" = 1\n\
             'crates/a/src/gone.rs' = 3\n\"crates/a/src/shrank.rs\" = 3\n\
             \"crates/a/src/none.rs\" = 1\n"
                .to_owned(),
        ),
        (
            "crates/a/src/grew.rs",
            "fn f() {}\nold_table\nold_log\n".to_owned(),
        ),
        ("crates/a/src/shrank.rs", "\nold_table\n".to_owned()),
        ("crates/a/src/none.rs", "old_tables\n".to_owned()),
        (
            "crates/b/src/sub/new.rs",
            "fn f() {}\n// old_table\nold_log\n".to_owned(),
        ),
    ]);
    let base = "[pattern_ratchet.old]\n\"crates/a/src/grew.rs\" = 1\n\
                \"crates/a/src/gone.rs\" = 3\n\"crates/a/src/shrank.rs\" = 2\n";

    let document = rdjson(ws.path(), Some(base));

    let found: Vec<(&str, &str, u64, &str)> = document["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|diagnostic| {
            (
                diagnostic["code"]["value"].as_str().unwrap(),
                diagnostic["location"]["path"].as_str().unwrap(),
                diagnostic["location"]["range"]["start"]["line"]
                    .as_u64()
                    .unwrap(),
                diagnostic["severity"].as_str().unwrap(),
            )
        })
        .collect();
    const BASELINE: &str = "architecture-baseline.toml";
    assert_eq!(
        found,
        [
            (
                "pattern_ratchet.old.no_crossing",
                "crates/b/src/sub/new.rs",
                3,
                "ERROR"
            ),
            ("pattern_ratchet.old.no_new_entries", BASELINE, 7, "WARNING"),
            ("pattern_ratchet.old.no_raised_caps", BASELINE, 6, "WARNING"),
            (
                "pattern_ratchet.old.listed_no_growth",
                "crates/a/src/grew.rs",
                2,
                "WARNING"
            ),
            ("pattern_ratchet.old.stale_entries", BASELINE, 5, "WARNING"),
            (
                "pattern_ratchet.old.stale_entries",
                "crates/a/src/none.rs",
                1,
                "WARNING"
            ),
            (
                "pattern_ratchet.old.stale_entries",
                "crates/a/src/shrank.rs",
                2,
                "WARNING"
            ),
        ]
    );
}

/// The `[name]` table of a baseline file, from its header line up to the
/// next table's.
fn table_section(text: &str, name: &str) -> String {
    let header = format!("[{name}]");
    let mut lines = text
        .split_inclusive('\n')
        .skip_while(|line| line.trim_end() != header);
    let first = lines.next().expect("the baseline has the table");
    let rest = lines.take_while(|line| !line.starts_with('['));
    std::iter::once(first).chain(rest).collect()
}

/// The codes of every finding, in reporting order.
fn codes(root: &Path) -> Vec<String> {
    check(&Context::load(root).expect("the workspace loads"))
        .iter()
        .map(|finding| finding.code())
        .collect()
}

/// `--write-baseline=<table>` updates only that table, listing only its
/// files left out; the others stay byte for byte as they were, drifted caps
/// included.
#[test]
fn pattern_ratchet_scoped_write_baseline() {
    const BASELINE: &str = "architecture-baseline.toml";
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            format!(
                "{}{}",
                old_tables_config(ADVISORY),
                ratchet("other", &[r"\bother_table\b"], &[], ADVISORY,),
            ),
        ),
        (
            BASELINE,
            "[file_size]\n\"crates/a/src/big.rs\" = 6\n\
             [pattern_ratchet.old]\n\"crates/a/src/shrinks.rs\" = 2\n\
             [pattern_ratchet.other]\n\"crates/b/src/sub/o.rs\" = 1\n"
                .to_owned(),
        ),
        ("crates/a/src/big.rs", code(LIMIT + 1)),
        ("crates/a/src/shrinks.rs", "old_table\nold_log\n".to_owned()),
        ("crates/b/src/sub/o.rs", "other_table\n".to_owned()),
    ]);
    // In generated form, so that kept tables can be compared byte for byte.
    let generated = run(ws.path(), &["--write-baseline"]);
    assert_eq!(generated.code, 0, "{}", generated.stderr);
    assert!(
        generated.stdout.starts_with(
            "archcheck: wrote 1 file_size, 1 pattern_ratchet.old, 1 pattern_ratchet.other, \
             0 crate_names entries to "
        ),
        "{}",
        generated.stdout
    );
    // Every table drifts: big.rs grows past its cap, shrinks.rs loses a
    // matching line, a new file matches `old`, o.rs gains an `other` line,
    // and a new file is over the limit.
    fs::write(ws.path().join("crates/a/src/big.rs"), code(LIMIT + 3)).unwrap();
    fs::write(ws.path().join("crates/a/src/shrinks.rs"), "old_table\n").unwrap();
    fs::write(ws.path().join("crates/b/src/sub/new.rs"), "old_log\n").unwrap();
    fs::write(
        ws.path().join("crates/b/src/sub/o.rs"),
        "other_table\nother_table\n",
    )
    .unwrap();
    fs::write(ws.path().join("crates/a/src/huge.rs"), code(LIMIT + 2)).unwrap();
    let before = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert_eq!(
        table_section(&before, "file_size"),
        "[file_size]\n\"crates/a/src/big.rs\" = 6\n\n"
    );

    let wrote = run(ws.path(), &["--write-baseline=pattern_ratchet.old"]);

    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    let output = write_output(&wrote.stdout);
    assert_eq!(
        output.not_added,
        [
            "not added: crates/b/src/sub/new.rs (1 code line matching the old patterns); move \
          them behind an allowed path, or add an entry by hand if this is a deliberate exception"
        ]
    );
    assert!(
        output
            .summary
            .starts_with("archcheck: wrote 1 pattern_ratchet.old entries to ")
            && output.summary.ends_with("; other tables kept"),
        "{}",
        wrote.stdout
    );
    assert_eq!(
        output.entries,
        format!(
            "{}\"crates/b/src/sub/new.rs\" = 1\n",
            inside("pattern_ratchet.old")
        )
    );
    let after = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert_eq!(
        table_section(&after, "pattern_ratchet.old"),
        "[pattern_ratchet.old]\n\"crates/a/src/shrinks.rs\" = 1\n\n"
    );
    for kept in ["file_size", "pattern_ratchet.other"] {
        assert_eq!(table_section(&after, kept), table_section(&before, kept));
    }
    assert_eq!(
        codes(ws.path()),
        [
            "file_size.no_crossing",
            "file_size.listed_no_growth",
            "pattern_ratchet.old.no_crossing",
            "pattern_ratchet.other.listed_no_growth"
        ]
    );

    // The other way round, with `old` drifted again: file_size is
    // updated and both instance tables are kept.
    fs::write(
        ws.path().join("crates/a/src/shrinks.rs"),
        "old_table\nold_log\n",
    )
    .unwrap();
    let wrote = run(ws.path(), &["--write-baseline=file_size"]);
    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    let output = write_output(&wrote.stdout);
    assert_eq!(
        output.not_added,
        [
            "not added: crates/a/src/huge.rs (7 code lines); split it, or add an entry by hand if \
          this is a deliberate exception"
        ]
    );
    assert!(
        output
            .summary
            .starts_with("archcheck: wrote 1 file_size entries to "),
        "{}",
        wrote.stdout
    );
    assert_eq!(
        output.entries,
        format!("{}\"crates/a/src/huge.rs\" = 7\n", inside("file_size"))
    );
    let after_file_size = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert_eq!(
        table_section(&after_file_size, "file_size"),
        "[file_size]\n\"crates/a/src/big.rs\" = 8\n\n"
    );
    for kept in ["pattern_ratchet.old", "pattern_ratchet.other"] {
        assert_eq!(
            table_section(&after_file_size, kept),
            table_section(&after, kept)
        );
    }
    assert_eq!(
        codes(ws.path()),
        [
            "file_size.no_crossing",
            "pattern_ratchet.old.no_crossing",
            "pattern_ratchet.old.listed_no_growth",
            "pattern_ratchet.other.listed_no_growth"
        ]
    );

    // An unknown table, or a baseline that does not parse, exits 2 and
    // leaves the file alone.
    for table in ["pattern_ratchet.nope", "file_sizes", "pattern_ratchet", ""] {
        let failed = run(ws.path(), &[&format!("--write-baseline={table}")]);
        assert_eq!(failed.code, 2, "{table}: {}", failed.stdout);
        assert!(
            failed.stderr.starts_with(&format!(
                "archcheck: error: no baseline table {table:?}; expected one of: file_size, \
                 pattern_ratchet.old, pattern_ratchet.other"
            )),
            "{table}: {}",
            failed.stderr
        );
        assert_eq!(
            fs::read_to_string(ws.path().join(BASELINE)).unwrap(),
            after_file_size
        );
    }
    fs::write(ws.path().join(BASELINE), "not toml").unwrap();
    let failed = run(ws.path(), &["--write-baseline=file_size"]);
    assert_eq!(failed.code, 2, "{}", failed.stdout);
    assert!(
        failed.stderr.contains("could not parse"),
        "{}",
        failed.stderr
    );
    assert_eq!(
        fs::read_to_string(ws.path().join(BASELINE)).unwrap(),
        "not toml"
    );
}

/// A scoped write of an existing but empty table updates it like any other,
/// so it stays empty and a matching file is listed, not added, with entries
/// to paste inside that table. It is not the last table, so entries pasted
/// at the end of the file would land in another one.
#[test]
fn pattern_ratchet_scoped_write_keeps_an_empty_table() {
    const BASELINE: &str = "architecture-baseline.toml";
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            format!(
                "{}{}",
                old_tables_config(REQUIRED),
                ratchet("other", &[r"\bother_table\b"], &[], REQUIRED),
            ),
        ),
        (
            BASELINE,
            "[file_size]\n[pattern_ratchet.old]\n[pattern_ratchet.other]\n".to_owned(),
        ),
        ("crates/a/src/n.rs", "old_table\n".to_owned()),
    ]);

    let wrote = run(ws.path(), &["--write-baseline=pattern_ratchet.old"]);

    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    let output = write_output(&wrote.stdout);
    assert_eq!(
        output.not_added,
        [
            "not added: crates/a/src/n.rs (1 code line matching the old patterns); move them \
          behind an allowed path, or add an entry by hand if this is a deliberate exception"
        ]
    );
    assert!(
        output
            .summary
            .starts_with("archcheck: wrote 0 pattern_ratchet.old entries to ")
            && output.summary.ends_with("; other tables kept"),
        "{}",
        wrote.stdout
    );
    assert_eq!(
        output.entries,
        format!(
            "{}\"crates/a/src/n.rs\" = 1\n",
            inside("pattern_ratchet.old")
        )
    );
    let after = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert_eq!(
        table_section(&after, "pattern_ratchet.old"),
        "[pattern_ratchet.old]\n\n"
    );
    // Only [crate_names], always written last, follows it.
    assert!(
        after.contains("[pattern_ratchet.other]\n\n# [crate_names] ")
            && after.ends_with("\n[crate_names]\n"),
        "{after}"
    );
    assert_eq!(codes(ws.path()), ["pattern_ratchet.old.no_crossing"]);

    // Pasted inside the existing table, as the comment says, the entries
    // parse and are what that table then tolerates.
    fs::write(
        ws.path().join(BASELINE),
        paste_entries(&after, &output.entries),
    )
    .unwrap();
    assert_eq!(codes(ws.path()), Vec::<String>::new());
}

/// The unscoped write lists the entries of every table, each block saying
/// where it goes; pasted there, each lands in its own table, though the
/// tables they name are first, in the middle and missing, not last.
#[test]
fn write_baseline_entries_paste_into_their_tables() {
    const BASELINE: &str = "architecture-baseline.toml";
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            format!(
                "{}{}{}",
                old_tables_config(REQUIRED),
                ratchet("fresh", &[r"\bfresh_table\b"], &[], REQUIRED),
                ratchet("zz", &[r"\bzz_table\b"], &[], REQUIRED),
            ),
        ),
        (
            BASELINE,
            "[file_size]\n\"crates/a/src/big.rs\" = 6\n[pattern_ratchet.old]\n\
             \"crates/a/src/o.rs\" = 1\n[pattern_ratchet.zz]\n\"crates/a/src/z.rs\" = 1\n"
                .to_owned(),
        ),
        ("crates/a/src/big.rs", code(6)),
        ("crates/a/src/huge.rs", code(7)),
        ("crates/a/src/o.rs", "old_table\n".to_owned()),
        ("crates/a/src/n.rs", "old_table\n".to_owned()),
        ("crates/a/src/z.rs", "zz_table\n".to_owned()),
        ("crates/a/src/f.rs", "fresh_table\n".to_owned()),
    ]);

    let wrote = run(ws.path(), &["--write-baseline"]);

    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    let output = write_output(&wrote.stdout);
    assert_eq!(output.not_added.len(), 3, "{}", wrote.stdout);
    assert_eq!(
        output.entries,
        format!(
            "{}\"crates/a/src/huge.rs\" = 7\n\
             # add as a new table, header included, at the end of the file\n\
             [pattern_ratchet.fresh]\n\"crates/a/src/f.rs\" = 1\n\
             {}\"crates/a/src/n.rs\" = 1\n",
            inside("file_size"),
            inside("pattern_ratchet.old"),
        )
    );
    let written = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert_eq!(
        codes(ws.path()),
        [
            "file_size.no_crossing",
            "pattern_ratchet.fresh.no_crossing",
            "pattern_ratchet.old.no_crossing"
        ]
    );

    fs::write(
        ws.path().join(BASELINE),
        paste_entries(&written, &output.entries),
    )
    .unwrap();
    assert_eq!(codes(ws.path()), Vec::<String>::new());
    let pasted = Baseline::load(&ws.path().join(BASELINE)).expect("the pasted baseline loads");
    assert_eq!(
        pasted.table_names(),
        [
            "file_size",
            "pattern_ratchet.fresh",
            "pattern_ratchet.old",
            "pattern_ratchet.zz",
            "crate_names"
        ]
    );
    assert_eq!(
        pasted
            .table("pattern_ratchet.zz")
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["crates/a/src/z.rs"]
    );
}

/// Either form of `--write-baseline` refuses, exits 2 and leaves the file
/// alone when the existing baseline has a table for an instance that is no
/// longer configured, or does not exist.
#[test]
fn pattern_ratchet_write_baseline_errors() {
    const BASELINE: &str = "architecture-baseline.toml";
    let stale = "[file_size]\n[pattern_ratchet.foo]\n\"crates/a/src/x.rs\" = 1\n";
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            format!(
                "{}{}",
                config(ADVISORY),
                ratchet("bar", &[r"\bold_table\b"], &[], ADVISORY,),
            ),
        ),
        (BASELINE, stale.to_owned()),
        ("crates/a/src/x.rs", "old_table\n".to_owned()),
    ]);
    let forms: [&[&str]; 2] = [
        &["--write-baseline=pattern_ratchet.bar"],
        &["--write-baseline"],
    ];

    for args in forms {
        let renamed = run(ws.path(), args);

        assert_eq!(renamed.code, 2, "{args:?}: {}", renamed.stdout);
        assert!(
            renamed.stderr.contains(
                "[pattern_ratchet.foo] names no instance in ARCHITECTURE.toml; remove that \
                 table, or rename it if the instance was renamed"
            ),
            "{args:?}: {}",
            renamed.stderr
        );
        assert_eq!(fs::read_to_string(ws.path().join(BASELINE)).unwrap(), stale);
    }

    fs::remove_file(ws.path().join(BASELINE)).unwrap();
    for args in forms {
        let missing = run(ws.path(), args);
        assert_eq!(missing.code, 2, "{args:?}: {}", missing.stdout);
        assert!(
            missing
                .stderr
                .starts_with("archcheck: error: could not read ")
                && missing.stderr.contains("architecture-baseline.toml"),
            "{args:?}: {}",
            missing.stderr
        );
        assert!(!ws.path().join(BASELINE).exists(), "{args:?}");
    }
}

/// A table is only taken as `--write-baseline=<table>`: `--write-baseline`
/// alone is the unscoped write, whatever follows it, and an argument after it
/// that is not a flag is an error rather than a table, with a hint unless
/// `=` already named one.
#[test]
fn write_baseline_takes_a_table_only_after_equals() {
    let ws = workspace(&[("ARCHITECTURE.toml", old_tables_config(ADVISORY))]);
    let baseline = ws.path().join("architecture-baseline.toml");
    let archcheck = |args: &[&std::ffi::OsStr]| {
        let output = Command::new(env!("CARGO_BIN_EXE_archcheck"))
            .args(args)
            .output()
            .expect("run archcheck");
        Run {
            code: output.status.code().expect("an exit status"),
            stdout: String::from_utf8(output.stdout).expect("UTF-8 stdout"),
            stderr: String::from_utf8(output.stderr).expect("UTF-8 stderr"),
        }
    };

    for table in ["file_size", "pattern_ratchet.old"] {
        let before = fs::read_to_string(&baseline).unwrap();
        let failed = run(ws.path(), &["--write-baseline", table]);
        assert_eq!(failed.code, 2, "{table}: {}", failed.stdout);
        assert!(
            failed.stderr.starts_with(&format!(
                "archcheck: unexpected argument {table:?}; name a table as \
                 --write-baseline={table}\nusage: archcheck "
            )),
            "{table}: {}",
            failed.stderr
        );
        assert_eq!(failed.stdout, "", "{table}");
        assert_eq!(fs::read_to_string(&baseline).unwrap(), before, "{table}");
    }
    let before = fs::read_to_string(&baseline).unwrap();
    let failed = run(ws.path(), &["--write-baseline=file_size", "extra"]);
    assert_eq!(failed.code, 2, "{}", failed.stdout);
    assert!(
        failed
            .stderr
            .starts_with("archcheck: unexpected argument \"extra\"\nusage: archcheck "),
        "{}",
        failed.stderr
    );
    assert_eq!(fs::read_to_string(&baseline).unwrap(), before);

    let unscoped = [
        archcheck(&[
            "--root".as_ref(),
            ws.path().as_os_str(),
            "--write-baseline".as_ref(),
            "--format".as_ref(),
            "human".as_ref(),
        ]),
        archcheck(&[
            "--write-baseline".as_ref(),
            "--root".as_ref(),
            ws.path().as_os_str(),
        ]),
    ];
    for run in unscoped {
        assert_eq!(run.code, 0, "{}", run.stderr);
        assert!(
            run.stdout
                .starts_with("archcheck: wrote 0 file_size, 0 crate_names entries to ")
                && !run.stdout.contains("other tables kept"),
            "{}",
            run.stdout
        );
    }
}

/// A configured instance without a baseline table tolerates nothing, as with
/// an empty table.
#[test]
fn pattern_ratchet_without_a_baseline_table() {
    for (text, expected) in [("old_table\n", 1), ("old_tables\n", 0)] {
        let found = |baseline: String| {
            let ws = workspace(&[
                ("ARCHITECTURE.toml", old_tables_config(ADVISORY)),
                ("architecture-baseline.toml", baseline),
                ("crates/b/src/sub/new.rs", text.to_owned()),
            ]);
            ratchet_findings(ws.path())
        };

        let without_table = found(baseline(&[]));

        assert_eq!(
            without_table.len(),
            expected,
            "{text:?}: {without_table:#?}"
        );
        assert!(
            without_table
                .iter()
                .all(|(code, _)| code == "pattern_ratchet.old.no_crossing")
        );
        assert_eq!(without_table, found(ratchet_baseline(&[("old", &[])])));
    }
}

/// Instance names are lowercase ASCII letters, digits and underscores,
/// starting with a letter.
#[test]
fn pattern_ratchet_instance_names() {
    let cases = [
        ("a", true),
        ("a_2", true),
        ("old_tables", true),
        ("a-b", false),
        ("2a", false),
        ("", false),
        ("A", false),
        ("a.b", false),
        ("_a", false),
    ];
    for (name, accepted) in cases {
        let ws = workspace(&[(
            "ARCHITECTURE.toml",
            format!(
                "{}{}",
                config(ADVISORY),
                ratchet(&format!("{name:?}"), &["x"], &[], ADVISORY,),
            ),
        )]);

        let loaded = Context::load(ws.path());

        match loaded {
            Ok(_) => assert!(accepted, "{name:?} was accepted"),
            Err(error) => {
                let error = error.to_string();
                assert!(!accepted, "{name:?}: {error}");
                assert!(
                    error.contains(&format!("instance name {name:?} must be")),
                    "{name:?}: {error}"
                );
            }
        }
    }
}

/// An invalid regex or glob is reported at its line and column in
/// `ARCHITECTURE.toml`.
#[test]
fn pattern_ratchet_errors_point_at_the_line() {
    let cases = [
        (
            &["(old_table"][..],
            &[][..],
            "patterns",
            "regex parse error",
        ),
        (
            &["old_table"][..],
            &["crates/{a"][..],
            "allow",
            "error parsing glob",
        ),
    ];
    for (patterns, allow, key, problem) in cases {
        let text = format!(
            "{}{}",
            config(ADVISORY),
            ratchet("old", patterns, allow, ADVISORY),
        );
        // The last: [rules.crate_names], before the instance, has an allow too.
        let lines: Vec<&str> = text.lines().collect();
        let line = 1 + lines
            .iter()
            .rposition(|line| line.starts_with(&format!("{key} = ")))
            .unwrap();
        let column = key.len() + " = ".len() + 1;
        let ws = workspace(&[("ARCHITECTURE.toml", text)]);

        let error = Context::load(ws.path()).unwrap_err().to_string();

        assert!(
            error.contains(&format!("at line {line}, column {column}")),
            "{key}: {error}"
        );
        assert!(error.contains(problem), "{key}: {error}");
    }
}
