//! archcheck over temporary workspaces: the library for counting, the
//! inventory, findings and the baseline; the binary for modes, exit codes,
//! formats and arguments.

use std::{collections::BTreeMap, fs, path::Path, process::Command};

use pplx_space_archcheck::{
    BaseBaseline, Baseline, Context, Renames, check,
    config::BaseArchitecture,
    context::count_code_lines,
    finding::Finding,
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
            BaseArchitecture::load(&path).expect("the base architecture is readable");
    }
    ctx
}

/// `(behaviour, message)` of every finding, in reporting order.
fn findings(root: &Path) -> Vec<(&'static str, String)> {
    findings_against(root, None)
}

fn findings_against(root: &Path, base: Option<&str>) -> Vec<(&'static str, String)> {
    findings_with_renames(root, base, None)
}

/// [`findings_against`], with `renames` as `--renames` gives them.
fn findings_with_renames(
    root: &Path,
    base: Option<&str>,
    renames: Option<&[u8]>,
) -> Vec<(&'static str, String)> {
    let mut ctx = load(root, base);
    ctx.renames = renames.map(|renames| Renames::parse(renames).expect("the renames parse"));
    check(&ctx)
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
                 added unless --renames pairs it with an old path the base branch's copy lists). \
                 Split the file instead.",
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

/// An entry moved with a file `--renames` says was renamed is compared with
/// the old path's entry in the base branch's copy: at or below its cap it is
/// neither new nor raised. Each row changes one thing from the first: the
/// base lists `old.rs` (cap 8), which became `new.rs` (8 code lines), and the
/// baseline lists `new.rs` (cap 8).
#[test]
fn renames() {
    const OLD: &str = "crates/a/src/old.rs";
    const NEW: &str = "crates/b/src/new.rs";
    const OLD2: &str = "crates/a/src/old2.rs";
    const NEW2: &str = "crates/b/src/new2.rs";
    const RENAMED: &[u8] = b"R097\0crates/a/src/old.rs\0crates/b/src/new.rs\0";
    let crossing = |path: &str, lines: usize, advice: &str| {
        (
            "no_crossing",
            format!(
                "{path}: {lines} code lines, over the 5-line limit (comments and blank lines are \
                 not counted). Split it into submodules by responsibility.{advice}"
            ),
        )
    };
    let new_entry = |path: &str| {
        (
            "no_new_entries",
            format!(
                "{path} is in architecture-baseline.toml but not in the base branch's copy; files \
                 over the 5-line limit can't be added (a renamed file counts as added unless \
                 --renames pairs it with an old path the base branch's copy lists). Split the \
                 file instead."
            ),
        )
    };
    let raised = |path: &str, renamed: &str, from: usize, to: usize| {
        (
            "no_raised_caps",
            format!(
                "{path}:{renamed} cap raised from {from} to {to} in architecture-baseline.toml; \
                 caps should only go down. Split the file instead."
            ),
        )
    };
    type Sizes<'a> = &'a [(&'a str, usize)];
    // Name, base entries, entries, file sizes, renames and the expected
    // `(behaviour, message)` of each finding.
    type Case<'a> = (
        &'a str,
        Sizes<'a>,
        Sizes<'a>,
        Sizes<'a>,
        Option<&'a [u8]>,
        Vec<(&'a str, String)>,
    );
    let cases: Vec<Case> = vec![
        (
            "renamed at the same cap",
            &[(OLD, 8)],
            &[(NEW, 8)],
            &[(NEW, 8)],
            Some(RENAMED),
            vec![],
        ),
        (
            "renamed with a lower cap",
            &[(OLD, 8)],
            &[(NEW, 7)],
            &[(NEW, 7)],
            Some(RENAMED),
            vec![],
        ),
        (
            "renamed with a raised cap",
            &[(OLD, 8)],
            &[(NEW, 9)],
            &[(NEW, 9)],
            Some(RENAMED),
            vec![raised(NEW, " renamed from crates/a/src/old.rs, with", 8, 9)],
        ),
        (
            "renamed without --renames",
            &[(OLD, 8)],
            &[(NEW, 8)],
            &[(NEW, 8)],
            None,
            vec![new_entry(NEW)],
        ),
        (
            "a deleted file and an unrelated new file",
            &[(OLD, 8)],
            &[(NEW, 8)],
            &[(NEW, 8)],
            Some(b"D\0crates/a/src/old.rs\0A\0crates/b/src/new.rs\0"),
            vec![new_entry(NEW)],
        ),
        (
            "a copy, not a rename",
            &[(OLD, 8)],
            &[(OLD, 8), (NEW, 8)],
            &[(OLD, 8), (NEW, 8)],
            Some(b"C100\0crates/a/src/old.rs\0crates/b/src/new.rs\0"),
            vec![new_entry(NEW)],
        ),
        (
            "renamed from a file the base does not list",
            &[],
            &[(NEW, 8)],
            &[(NEW, 8)],
            Some(RENAMED),
            vec![new_entry(NEW)],
        ),
        // A path the base lists is compared with its own entry, renamed or
        // not, as without --renames.
        (
            "the base lists the new path too",
            &[(OLD, 8), (NEW, 5)],
            &[(NEW, 8)],
            &[(NEW, 8)],
            Some(RENAMED),
            vec![raised(NEW, "", 5, 8)],
        ),
        (
            "renamed, and the entry not moved",
            &[(OLD, 8)],
            &[(OLD, 8)],
            &[(NEW, 8)],
            Some(RENAMED),
            vec![
                crossing(
                    NEW,
                    8,
                    " It was renamed from crates/a/src/old.rs: move that entry in \
                     architecture-baseline.toml to the new path, at or below \
                     crates/a/src/old.rs's cap of 8 in the base branch's copy.",
                ),
                (
                    "stale_entries",
                    "crates/a/src/old.rs: listed in architecture-baseline.toml but no longer a \
                     production Rust file. Remove the entry; a renamed file is checked under its \
                     new path."
                        .to_owned(),
                ),
            ],
        ),
        (
            "renamed, and the entry removed",
            &[(OLD, 8)],
            &[],
            &[(NEW, 8)],
            Some(RENAMED),
            vec![crossing(
                NEW,
                8,
                " It was renamed from crates/a/src/old.rs: give it an entry in \
                 architecture-baseline.toml at or below crates/a/src/old.rs's cap of 8 in the base \
                 branch's copy.",
            )],
        ),
        // The advice names the cap the new entry would be compared with.
        (
            "renamed, the entry removed, and the base lists the new path",
            &[(OLD, 8), (NEW, 5)],
            &[],
            &[(NEW, 8)],
            Some(RENAMED),
            vec![crossing(
                NEW,
                8,
                " It was renamed from crates/a/src/old.rs: give it an entry in \
                 architecture-baseline.toml at or below its cap of 5 in the base branch's copy.",
            )],
        ),
        (
            "renamed from a file neither lists",
            &[],
            &[],
            &[(NEW, 8)],
            Some(RENAMED),
            vec![crossing(NEW, 8, "")],
        ),
        (
            "a file over the limit that was not renamed",
            &[(OLD, 8)],
            &[(NEW, 8)],
            &[(NEW, 8), ("crates/a/src/big.rs", 6)],
            Some(RENAMED),
            vec![crossing("crates/a/src/big.rs", 6, "")],
        ),
        // Git, not the caps, pairs old paths with new ones.
        (
            "two renames",
            &[(OLD, 8), (OLD2, 6)],
            &[(NEW, 8), (NEW2, 6)],
            &[(NEW, 8), (NEW2, 6)],
            Some(
                b"R090\0crates/a/src/old.rs\0crates/b/src/new.rs\0\
                  R080\0crates/a/src/old2.rs\0crates/b/src/new2.rs\0",
            ),
            vec![],
        ),
        (
            "two renames, paired the other way",
            &[(OLD, 8), (OLD2, 6)],
            &[(NEW, 8), (NEW2, 6)],
            &[(NEW, 8), (NEW2, 6)],
            Some(
                b"R090\0crates/a/src/old.rs\0crates/b/src/new2.rs\0\
                  R080\0crates/a/src/old2.rs\0crates/b/src/new.rs\0",
            ),
            vec![raised(
                NEW,
                " renamed from crates/a/src/old2.rs, with",
                6,
                8,
            )],
        ),
    ];
    for (name, base, entries, sizes, renames, expected) in cases {
        let mut files: Vec<(&str, String)> = sizes
            .iter()
            .map(|(path, lines)| (*path, code(*lines)))
            .collect();
        files.push(("architecture-baseline.toml", baseline(entries)));
        let ws = workspace(&files);

        let found = findings_with_renames(ws.path(), Some(&baseline(base)), renames);

        assert_eq!(found, expected, "{name}");
    }
}

/// From the binary, `--renames` takes git's own output, and a file that
/// can't be read or isn't that output exits 2.
#[test]
fn renames_from_git() {
    const OLD: &str = "crates/a/src/old.rs";
    let ws = workspace(&[
        ("architecture-baseline.toml", baseline(&[(OLD, 8)])),
        (OLD, code(8)),
    ]);
    // Git acts on the temporary repository alone, even when the test runs
    // under a git hook or `git rebase -x`, which set GIT_DIR, GIT_INDEX_FILE
    // and the like for the outer checkout: no GIT_* variable is inherited, no
    // configuration but the test's own is read, and discovery stops at the
    // workspace.
    let git = |args: &[&str]| {
        let mut command = Command::new("git");
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("GIT_") {
                command.env_remove(name);
            }
        }
        let output = command
            .current_dir(ws.path())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CEILING_DIRECTORIES",
                ws.path().parent().expect("a parent directory"),
            )
            .args([
                "-c",
                "user.name=archcheck",
                "-c",
                "user.email=archcheck@example.com",
            ])
            .args(args)
            .output()
            .expect("run git");
        assert!(output.status.success(), "git {args:?}: {output:?}");
        output.stdout
    };
    let base = ws.path().join("base-baseline.toml");
    fs::write(&base, baseline(&[(OLD, 8)])).unwrap();
    // An empty template copies no hooks.
    git(&["init", "-q", "--template="]);
    git(&["add", "-A"]);
    git(&["commit", "-q", "-m", "base"]);
    git(&["mv", OLD, "crates/a/src/new.rs"]);
    fs::write(
        ws.path().join("architecture-baseline.toml"),
        baseline(&[("crates/a/src/new.rs", 8)]),
    )
    .unwrap();
    let renames = ws.path().join("renames");
    fs::write(
        &renames,
        git(&["diff", "-z", "--name-status", "--find-renames", "HEAD"]),
    )
    .unwrap();
    let base = base.to_str().unwrap();
    let run_with = |renames: &Path| {
        let renames = renames.to_str().unwrap();
        run(ws.path(), &["--base-baseline", base, "--renames", renames])
    };

    let renamed = run_with(&renames);
    assert_eq!(renamed.code, 0, "{}", renamed.stderr);
    assert!(
        renamed.stdout.starts_with("archcheck: no findings"),
        "{}",
        renamed.stdout
    );
    let unpaired = run(ws.path(), &["--base-baseline", base]);
    assert!(
        unpaired
            .stdout
            .starts_with("file_size.no_new_entries (advisory):\n  crates/a/src/new.rs is in"),
        "{}",
        unpaired.stdout
    );

    let missing = run_with(&ws.path().join("missing"));
    assert_eq!(missing.code, 2);
    assert!(
        missing
            .stderr
            .starts_with("archcheck: error: could not read ")
            && missing.stderr.contains("missing"),
        "{}",
        missing.stderr
    );

    // Without -z, git quotes and separates paths differently, and ends
    // records with a newline, not a NUL.
    let text = ws.path().join("renames.txt");
    fs::write(
        &text,
        git(&["diff", "--name-status", "--find-renames", "HEAD"]),
    )
    .unwrap();
    let not_z = run_with(&text);
    assert_eq!(not_z.code, 2);
    assert!(
        not_z.stderr.contains(
            "renames.txt as renames: the last record does not end with a NUL; give the complete \
             output of git diff -z --name-status --find-renames"
        ),
        "{}",
        not_z.stderr
    );
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
    // Without [rules.layers] here, whether the base has it is unknown too.
    let last: Vec<&str> = run_unusable.stdout.lines().rev().take(2).collect();
    for (line, skipped) in last.iter().rev().zip([
        "; crate_names.policy_changed skipped",
        "; layers.policy_changed's check for the rule's removal skipped",
    ]) {
        assert!(
            line.starts_with(&warning) && line.ends_with(skipped),
            "{}",
            run_unusable.stdout
        );
    }
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

/// `include` limits an instance to the files it matches, and an instance
/// without it checks every production file: others are neither counted nor
/// kept in its table, and `allow` still applies inside the scope. An empty
/// `include` would check nothing, so it is rejected.
#[test]
fn pattern_ratchet_include_scopes_measurement_without_changing_default() {
    for (include, expected) in [
        (None, vec!["crates/a/src/lib.rs", "crates/b/src/lib.rs"]),
        (Some("['crates/a/src/**']"), vec!["crates/a/src/lib.rs"]),
    ] {
        let mut rule = ratchet(
            "scoped",
            &[r"\bold_table\b"],
            &["crates/a/src/owner.rs"],
            REQUIRED,
        );
        if let Some(include) = include {
            rule = rule.replace("patterns =", &format!("include = {include}\npatterns ="));
        }
        let ws = workspace(&[
            ("ARCHITECTURE.toml", format!("{}{rule}", config(ADVISORY))),
            (
                "architecture-baseline.toml",
                ratchet_baseline(&[("scoped", &[])]),
            ),
            ("crates/a/src/lib.rs", "old_table\n".to_owned()),
            ("crates/a/src/owner.rs", "old_table\n".to_owned()),
            ("crates/a/src/tests.rs", "old_table\n".to_owned()),
            ("crates/b/src/lib.rs", "old_table\n".to_owned()),
        ]);
        let ctx = Context::load(ws.path()).unwrap();
        let paths: Vec<_> = check(&ctx)
            .into_iter()
            .filter(|finding| finding.code() == "pattern_ratchet.scoped.no_crossing")
            .map(|finding| finding.path)
            .collect();
        assert_eq!(paths, expected, "include={include:?}");
    }

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
        "allow = ['crates/a/src/allowed.rs']\ninclude = ['crates/a/**']\n",
    );
    type Case<'a> = (
        &'a str,
        &'a [(&'a str, usize)],
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str)],
    );
    let cases: &[Case] = &[
        (
            "a file inside the include is checked",
            &[],
            &[("crates/a/src/x.rs", "old_table\n")],
            &[("no_crossing", "crates/a/src/x.rs: 1 code line matches")],
        ),
        (
            "a file outside the include is not",
            &[],
            &[("crates/b/src/x.rs", "old_table\n")],
            &[],
        ),
        (
            "allowed paths inside the include may match",
            &[],
            &[("crates/a/src/allowed.rs", "old_table\n")],
            &[],
        ),
        (
            "an entry for a file outside the include is stale",
            &[("crates/b/src/x.rs", 1)],
            &[("crates/b/src/x.rs", "old_table\n")],
            &[(
                "stale_entries",
                "crates/b/src/x.rs is outside the paths [rules.pattern_ratchet.old] in \
                 ARCHITECTURE.toml includes. Remove it from [pattern_ratchet.old] in \
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

    // An empty `include` would check nothing, so it is rejected; an empty
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
            no_allow.replace("include = ['crates/a/**']", "include = []"),
        ),
        ("architecture-baseline.toml", baseline),
    ]);
    let error = Context::load(ws.path()).expect_err("an empty include list is rejected");
    assert!(
        error.to_string().contains(
            "include must list at least one glob; omit it to check every production file"
        ),
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
         not counted). Split it into submodules by responsibility. If the file was renamed, \
         move its entry in architecture-baseline.toml to the new path; with --renames, \
         no_new_entries accepts the moved entry at or below its old cap.\n\
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
                 blank lines are not counted). Split it into submodules by responsibility. If \
                 the file was renamed, move its entry in architecture-baseline.toml to the new \
                 path; with --renames, no_new_entries accepts the moved entry at or below its old \
                 cap."
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
                     lines are not counted). Split it into submodules by responsibility. If the \
                     file was renamed, move its entry in architecture-baseline.toml to the new \
                     path; with --renames, no_new_entries accepts the moved entry at or below its \
                     old cap.",
                    "crates/b/src/new.rs", 1, "ERROR", "no_crossing",
                ),
                diagnostic(
                    "crates/a/src/added.rs is in architecture-baseline.toml but not in the base \
                     branch's copy; files over the 5-line limit can't be added (a renamed file \
                     counts as added unless --renames pairs it with an old path the base \
                     branch's copy lists). Split the file instead.",
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
        (&["--renames"], 2, "archcheck: --renames needs a value"),
        (
            &["--renames", "renames"],
            2,
            "archcheck: --renames needs --base-baseline, whose entries it pairs renamed files \
             with",
        ),
        (
            &["--write-baseline", "--renames", "renames"],
            2,
            "archcheck: --renames cannot be combined with --write-baseline",
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
                 [pattern_ratchet.old] in architecture-baseline.toml to the new path; with \
                 --renames, no_new_entries accepts the moved entry at or below its old cap.",
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
                 renamed file counts as added unless --renames pairs it with an old path the base \
                 branch's copy lists). Move that code behind an allowed path instead."
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

/// An instance's table gets the same rename handling as `[file_size]`
/// (see `renames`). The base's `[pattern_ratchet.old]` lists `old.rs` (cap
/// 2), which became `new.rs`; the baseline lists `new.rs`, at its count.
#[test]
fn pattern_ratchet_renames() {
    const NEW: &str = "crates/b/src/sub/new.rs";
    const RENAMED: &[u8] = b"R100\0crates/a/src/old.rs\0crates/b/src/sub/new.rs\0";
    // Name, the text of new.rs, renames and the expected `(behaviour,
    // message)` of each finding.
    type Case<'a> = (&'a str, &'a str, &'a [u8], &'a [(&'a str, &'a str)]);
    let cases: &[Case] = &[
        (
            "renamed at the same cap",
            "old_table\nold_log\n",
            RENAMED,
            &[],
        ),
        (
            "renamed with a raised cap",
            "old_table\nold_log\nold_table\n",
            RENAMED,
            &[(
                "no_raised_caps",
                "crates/b/src/sub/new.rs: renamed from crates/a/src/old.rs, with cap raised from \
                 2 to 3 in [pattern_ratchet.old] in architecture-baseline.toml; caps should only \
                 go down. Move that code behind an allowed path instead.",
            )],
        ),
        (
            "a deleted file and an unrelated new file",
            "old_table\nold_log\n",
            b"D\0crates/a/src/old.rs\0A\0crates/b/src/sub/new.rs\0",
            &[(
                "no_new_entries",
                "crates/b/src/sub/new.rs is in [pattern_ratchet.old] in",
            )],
        ),
    ];
    for (name, text, renames, expected) in cases {
        let count = text.lines().count();
        let ws = workspace(&[
            ("ARCHITECTURE.toml", old_tables_config(ADVISORY)),
            (
                "architecture-baseline.toml",
                ratchet_baseline(&[("old", &[(NEW, count)])]),
            ),
            (NEW, (*text).to_owned()),
        ]);

        let found = findings_with_renames(
            ws.path(),
            Some("[pattern_ratchet.old]\n\"crates/a/src/old.rs\" = 2\n"),
            Some(renames),
        );

        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for ((behaviour, message), (want_behaviour, want_message)) in found.iter().zip(*expected) {
            assert_eq!(behaviour, want_behaviour, "{name}");
            assert!(message.starts_with(want_message), "{name}: {message}");
        }
    }
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

/// Every behaviour of the layers rule, as `[rules.layers.<behaviour>]`.
const LAYERS_BEHAVIOURS: [&str; 25] = [
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
    "test_only",
    "clients",
    "store_edges",
    "stale_entries",
    "policy_changed",
];

/// The workspace members of a layered workspace, as `(member, package)`: one
/// or two per layer, a service with a server and a client, and two stores.
const LAYERED: [(&str, &str); 12] = [
    ("crates/l10_foundation/f", "pplx-space-f"),
    ("crates/l10_foundation/g", "pplx-space-g"),
    ("crates/l20_domain/d", "pplx-space-d"),
    ("crates/l30_storage/s/store", "pplx-space-s-store"),
    ("crates/l30_storage/t/store", "pplx-space-t-store"),
    ("crates/l40_api/a/server", "pplx-space-a"),
    ("crates/l40_api/a/client", "pplx-space-a-client"),
    ("crates/l40_api/b/client", "pplx-space-b-client"),
    ("crates/l50_app/p", "pplx-space-p"),
    ("crates/l50_app/q", "pplx-space-q"),
    ("crates/test_support/fixtures", "pplx-space-fixtures"),
    ("crates/tools/bench", "pplx-space-bench"),
];

/// The `[rules.layers.storage]` [`layers_config`] adds when its tables lack
/// one.
const STORAGE: &str = "[rules.layers.storage]\ndrivers = [\"redis\", \"sqlx\"]\n";

/// [`config`] with file_size's behaviours advisory, followed by
/// `[rules.layers]`: its `tables` (`unmoved`, `store_users`,
/// `allowed_edges` and `storage`, as TOML; [`STORAGE`] without `storage`)
/// and every check in `mode`, except `policy_changed`, which is advisory.
fn layers_config(tables: &str, mode: &str) -> String {
    let mut text = format!("{}{tables}", config(ADVISORY));
    if !tables.contains("[rules.layers.storage]") {
        text.push_str(STORAGE);
    }
    for behaviour in LAYERS_BEHAVIOURS {
        let mode = if behaviour == "policy_changed" {
            "advisory"
        } else {
            mode
        };
        text.push_str(&format!("[rules.layers.{behaviour}]\nmode = \"{mode}\"\n"));
    }
    text
}

/// A baseline with an empty `[file_size]`, the two layers tables with these
/// edges as `(from, to)`, and an empty `[crate_names]`.
fn layers_baseline(direction: &[(&str, &str)], dev_direction: &[(&str, &str)]) -> String {
    let table = |name: &str, edges: &[(&str, &str)]| {
        let mut text = format!("[layers.{name}]\n");
        for (from, to) in edges {
            text.push_str(&format!("{from:?} = [{to:?}]\n"));
        }
        text
    };
    format!(
        "[file_size]\n{}{}[crate_names]\n",
        table("dev_direction", dev_direction),
        table("direction", direction)
    )
}

/// A workspace of [`LAYERED`]'s members and `extra` ones, as `(member,
/// package)`, where each `(from, section, to)` of `dependencies` is a path
/// dependency of `from` on `to` in the manifest `section`, such as
/// `dev-dependencies`, under `to`'s name or, as `<key>=<package>`, renamed
/// `<key>`. `ARCHITECTURE.toml` is [`layers_config`] of `tables` with every
/// check required, the baseline [`layers_baseline`] without edges, and
/// `files` are written over all of that.
#[derive(Default)]
struct Layered<'a> {
    extra: &'a [(&'a str, &'a str)],
    dependencies: &'a [(&'a str, &'a str, &'a str)],
    tables: &'a str,
    files: Vec<(&'a str, String)>,
}

impl Layered<'_> {
    fn members(&self) -> Vec<(&str, &str)> {
        LAYERED.iter().chain(self.extra).copied().collect()
    }

    /// The manifest of member `member`, package `name`.
    fn manifest(&self, member: &str, name: &str) -> String {
        let mut sections: BTreeMap<&str, String> = BTreeMap::new();
        let up = "../".repeat(member.split('/').count());
        for (from, section, to) in self.dependencies {
            if *from != name {
                continue;
            }
            let (key, to) = to.split_once('=').unwrap_or((to, to));
            let (target, _) = self
                .members()
                .into_iter()
                .find(|(_, package)| *package == to)
                .expect("a dependency on a member");
            let rename = if key == to {
                String::new()
            } else {
                format!(", package = {to:?}")
            };
            sections
                .entry(section)
                .or_default()
                .push_str(&format!("{key} = {{ path = \"{up}{target}\"{rename} }}\n"));
        }
        let mut text = package(name);
        for (section, entries) in sections {
            text.push_str(&format!("[{section}]\n{entries}"));
        }
        text
    }

    fn build(&self) -> TempDir {
        let members = self.members();
        let names: Vec<&str> = members.iter().map(|(member, _)| *member).collect();
        let mut files: Vec<(String, String)> = vec![
            (
                "Cargo.toml".to_owned(),
                format!("[workspace]\nmembers = {names:?}\n"),
            ),
            (
                "ARCHITECTURE.toml".to_owned(),
                layers_config(self.tables, "required"),
            ),
            (
                "architecture-baseline.toml".to_owned(),
                layers_baseline(&[], &[]),
            ),
        ];
        files.extend(
            members.iter().map(|(member, name)| {
                (format!("{member}/Cargo.toml"), self.manifest(member, name))
            }),
        );
        files.extend(
            self.files
                .iter()
                .map(|(path, text)| ((*path).to_owned(), text.clone())),
        );
        let files: Vec<(&str, String)> = files
            .iter()
            .map(|(path, text)| (path.as_str(), text.clone()))
            .collect();
        workspace(&files)
    }
}

/// `(code, message, location)` of each layers finding, in reporting order.
fn layer_findings(ctx: &Context) -> Vec<(String, String, String)> {
    check(ctx)
        .into_iter()
        .filter(|finding| finding.rule == "layers")
        .map(|finding| {
            let at = format!("{}:{}", finding.location.path, finding.location.line);
            (finding.code(), finding.message(), at)
        })
        .collect()
}

/// Asserts that `found` has one finding per `(code, start of message,
/// location)` of `expected`, in order.
fn assert_layer_findings(
    name: &str,
    found: &[(String, String, String)],
    expected: &[(&str, &str, &str)],
) {
    assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
    for ((code, message, at), (want_code, want_message, want_at)) in found.iter().zip(expected) {
        assert_eq!(code, want_code, "{name}: {message}");
        assert!(message.starts_with(want_message), "{name}: {message}");
        assert_eq!(at, want_at, "{name}: {message}");
    }
}

/// The 1-based line of the first line of `text` containing `needle`.
fn line_with(text: &str, needle: &str) -> usize {
    text.lines()
        .position(|line| line.contains(needle))
        .expect("the text has the line")
        + 1
}

/// A member takes its layer from its directory, or else from its `unmoved`
/// entry; one with neither is reported on its `name =` line. An `unmoved`
/// entry the base branch's `[rules.layers]` lacks is reported on its line,
/// but not when the base has no such table, and without
/// `--base-architecture` the check is skipped with a line saying so.
#[test]
fn layers_placement() {
    const OLD: [(&str, &str); 1] = [("crates/libs/old", "pplx-space-old")];
    let unresolved = Layered {
        extra: &OLD,
        ..Layered::default()
    }
    .build();
    assert_layer_findings(
        "unresolved",
        &layer_findings(&load(unresolved.path(), None)),
        &[(
            "layers.placement",
            "crates/libs/old/Cargo.toml: pplx-space-old is at crates/libs/old, outside the layer \
             directories, and [rules.layers] unmoved in ARCHITECTURE.toml has no entry for it. \
             Put the crate in its layer's directory, crates/<layer>/old,",
            "crates/libs/old/Cargo.toml:2",
        )],
    );

    // The entry gives the crate its layer, which its edges are checked by.
    let unmoved = "[rules.layers.unmoved]\nl20_domain = [\"pplx-space-old\"]\n";
    let ws = Layered {
        extra: &OLD,
        dependencies: &[("pplx-space-old", "dependencies", "pplx-space-a")],
        tables: unmoved,
        ..Layered::default()
    }
    .build();
    assert_layer_findings(
        "unmoved",
        &layer_findings(&load(ws.path(), None)),
        &[(
            "layers.direction.no_crossing",
            "crates/libs/old/Cargo.toml: pplx-space-old (l20_domain) depends on pplx-space-a \
             (l40_api)",
            "crates/libs/old/Cargo.toml:6",
        )],
    );
    let skip = "layers.policy_changed and layers.placement's check for new unmoved entries \
                skipped (no --base-architecture given)";
    assert!(
        skipped(&load(ws.path(), None))
            .iter()
            .any(|line| line == skip),
        "{:?}",
        skipped(&load(ws.path(), None))
    );

    // Against a base whose [rules.layers] lacks the entry, it is new; against
    // one without the rule, the pull request seeds the table.
    let architecture = layers_config(unmoved, "required");
    let entry = format!(
        "ARCHITECTURE.toml:{}",
        line_with(&architecture, "\"pplx-space-old\"")
    );
    type Case<'a> = (&'a str, String, &'a [(&'a str, &'a str, &'a str)]);
    let cases: &[Case] = &[
        (
            "a base with the rule",
            layers_config("", "required"),
            &[(
                "layers.placement",
                "pplx-space-old: was added to [rules.layers] unmoved in ARCHITECTURE.toml, which \
                 lists only crates that predate the layer directories and is not to grow. Put the \
                 crate in crates/l20_domain/old instead.",
                &entry,
            )],
        ),
        ("a base with the entry", architecture.clone(), &[]),
        ("a base without the rule", config(ADVISORY), &[]),
    ];
    let ws = Layered {
        extra: &OLD,
        tables: unmoved,
        ..Layered::default()
    }
    .build();
    for (name, base, expected) in cases {
        let ctx = load_with(ws.path(), None, Some(base));
        let found: Vec<_> = layer_findings(&ctx)
            .into_iter()
            .filter(|(code, _, _)| code != "layers.policy_changed")
            .collect();
        assert_layer_findings(name, &found, expected);
        assert!(
            !skipped(&ctx).iter().any(|line| line.contains("placement")),
            "{name}: {:?}",
            skipped(&ctx)
        );
    }
}

/// Below its layer directory, a crate's path is its package name without
/// `pplx-space-`, split on `-`, optionally with one more directory without
/// `-`; and no crate is inside another, which is reported on the outer one.
#[test]
fn layers_layout() {
    type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a [(&'a str, &'a str)]);
    let cases: &[Case] = &[
        (
            "the exact path",
            &[("crates/l20_domain/x/y", "pplx-space-x-y")],
            &[],
        ),
        (
            "the path and a leaf",
            &[("crates/l20_domain/x/y/server", "pplx-space-x-y")],
            &[],
        ),
        (
            "a wrong path",
            &[("crates/l20_domain/x/z", "pplx-space-x-y")],
            &[(
                "crates/l20_domain/x/z/Cargo.toml: pplx-space-x-y is at crates/l20_domain/x/z, but \
                 below crates/l20_domain a crate's directory is its package name without \
                 `pplx-space-`, split on `-`: crates/l20_domain/x/y, or one directory below that",
                "crates/l20_domain/x/z/Cargo.toml:2",
            )],
        ),
        (
            "a directory with a `-`",
            &[("crates/l20_domain/x-y", "pplx-space-x-y")],
            &[(
                "crates/l20_domain/x-y/Cargo.toml: pplx-space-x-y is at crates/l20_domain/x-y",
                "crates/l20_domain/x-y/Cargo.toml:2",
            )],
        ),
        (
            "a leaf with a `-`",
            &[("crates/l20_domain/x/y/grpc-server", "pplx-space-x-y")],
            &[(
                "crates/l20_domain/x/y/grpc-server/Cargo.toml: pplx-space-x-y is at",
                "crates/l20_domain/x/y/grpc-server/Cargo.toml:2",
            )],
        ),
        (
            "a crate inside another",
            &[
                ("crates/l20_domain/x", "pplx-space-x"),
                ("crates/l20_domain/x/y", "pplx-space-x-y"),
            ],
            &[(
                "crates/l20_domain/x/Cargo.toml: pplx-space-x at crates/l20_domain/x contains the \
                 crate pplx-space-x-y at crates/l20_domain/x/y, but no crate goes inside another \
                 crate's directory. Move pplx-space-x one directory down, to \
                 crates/l20_domain/x/<leaf>",
                "crates/l20_domain/x/Cargo.toml:2",
            )],
        ),
        (
            "a crate inside one that already has a leaf",
            &[
                ("crates/l20_domain/x/server", "pplx-space-x"),
                ("crates/l20_domain/x/server/y", "pplx-space-x-server-y"),
            ],
            &[(
                "crates/l20_domain/x/server/Cargo.toml: pplx-space-x at crates/l20_domain/x/server \
                 contains the crate pplx-space-x-server-y at crates/l20_domain/x/server/y, but no \
                 crate goes inside another crate's directory. pplx-space-x already sits in a leaf \
                 directory, so move one of them: pplx-space-x to another leaf, \
                 crates/l20_domain/x/<leaf>, or pplx-space-x-server-y out. See \
                 crates/tools/archcheck/README.md#layers.",
                "crates/l20_domain/x/server/Cargo.toml:2",
            )],
        ),
    ];
    for (name, extra, expected) in cases {
        let ws = Layered {
            extra,
            ..Layered::default()
        }
        .build();
        let expected: Vec<(&str, &str, &str)> = expected
            .iter()
            .map(|(message, at)| ("layers.layout", *message, *at))
            .collect();
        assert_layer_findings(name, &layer_findings(&load(ws.path(), None)), &expected);
    }
}

/// Normal and build edges follow the layer matrix, whatever the manifest
/// calls the dependency and for whatever target; a dev-dependency that
/// duplicates a normal one is not counted again. The message names the
/// items the source uses through the edge and the dependents that keep its
/// crate from moving up.
#[test]
fn layers_direction() {
    type Case<'a> = (
        &'a str,
        &'a [(&'a str, &'a str, &'a str)],
        &'a str,
        Vec<(&'a str, String)>,
        &'a [(&'a str, &'a str, &'a str)],
    );
    let crossing = "layers.direction.no_crossing";
    let cases: &[Case] = &[
        (
            "Foundation to Domain",
            &[("pplx-space-f", "dependencies", "pplx-space-d")],
            "",
            vec![],
            &[(
                crossing,
                "crates/l10_foundation/f/Cargo.toml: pplx-space-f (l10_foundation) depends on \
                 pplx-space-d (l20_domain), a normal dependency, but l10_foundation may depend \
                 only on l10_foundation. Fix one of: move the items it uses down into \
                 l10_foundation or below; move pplx-space-f up to l20_domain; no dependent's layer \
                 blocks that; or invert the edge, with a trait that pplx-space-f defines and \
                 pplx-space-d implements. See crates/tools/archcheck/README.md#layers.",
                "crates/l10_foundation/f/Cargo.toml:6",
            )],
        ),
        (
            "Foundation to Foundation",
            &[("pplx-space-f", "dependencies", "pplx-space-g")],
            "",
            vec![],
            &[],
        ),
        (
            "Storage to API",
            &[("pplx-space-s-store", "dependencies", "pplx-space-a")],
            "",
            vec![],
            &[(
                crossing,
                "crates/l30_storage/s/store/Cargo.toml: pplx-space-s-store (l30_storage) depends \
                 on pplx-space-a (l40_api), a normal dependency, but l30_storage may depend only \
                 on the stores that list it, l20_domain and l10_foundation.",
                "crates/l30_storage/s/store/Cargo.toml:6",
            )],
        ),
        (
            "API to Domain",
            &[("pplx-space-a", "dependencies", "pplx-space-d")],
            "",
            vec![],
            &[],
        ),
        (
            "App to App, naming what it uses",
            &[("pplx-space-p", "dependencies", "pplx-space-q")],
            "",
            vec![(
                "crates/l50_app/p/src/lib.rs",
                "use pplx_space_q::{\n    config::{Config, self},\n    Run as Start,\n    \
                 serve,\n};\nfn f() { pplx_space_q::util::helper::<u8>(); other::pplx_space_q::x(); }\n"
                    .to_owned(),
            )],
            &[(
                crossing,
                "crates/l50_app/p/Cargo.toml: pplx-space-p (l50_app) depends on pplx-space-q \
                 (l50_app), a normal dependency, but l50_app may depend only on l40_api, \
                 l20_domain and l10_foundation. An app composes API crates but never another \
                 app: move what pplx-space-p uses from pplx-space-q (pplx_space_q::Run, \
                 pplx_space_q::config, pplx_space_q::config::Config, pplx_space_q::serve, \
                 pplx_space_q::util::helper) into an l40_api crate both can depend on.",
                "crates/l50_app/p/Cargo.toml:6",
            )],
        ),
        (
            "App to Storage",
            &[("pplx-space-p", "dependencies", "pplx-space-s-store")],
            "",
            vec![],
            &[(
                crossing,
                "crates/l50_app/p/Cargo.toml: pplx-space-p (l50_app) depends on \
                 pplx-space-s-store (l30_storage), a normal dependency, but l50_app may depend \
                 only on l40_api, l20_domain and l10_foundation. An app reaches state only \
                 through API crates",
                "crates/l50_app/p/Cargo.toml:6",
            )],
        ),
        (
            "App to Storage, allowed by name",
            &[("pplx-space-p", "dependencies", "pplx-space-s-store")],
            "[rules.layers.allowed_edges]\npplx-space-p = [\"pplx-space-s-store\"]\n",
            vec![],
            &[],
        ),
        (
            "a build-dependency",
            &[("pplx-space-f", "build-dependencies", "pplx-space-d")],
            "",
            vec![],
            &[(
                crossing,
                "crates/l10_foundation/f/Cargo.toml: pplx-space-f (l10_foundation) depends on \
                 pplx-space-d (l20_domain), a build-dependency,",
                "crates/l10_foundation/f/Cargo.toml:6",
            )],
        ),
        (
            "a renamed dependency",
            &[("pplx-space-f", "dependencies", "dom=pplx-space-d")],
            "",
            vec![(
                "crates/l10_foundation/f/src/lib.rs",
                "use dom::Id;\nuse pplx_space_d::Unseen;\n".to_owned(),
            )],
            &[(
                crossing,
                "crates/l10_foundation/f/Cargo.toml: pplx-space-f (l10_foundation) depends on \
                 pplx-space-d (l20_domain), renamed dom, a normal dependency, but l10_foundation \
                 may depend only on l10_foundation. Fix one of: move the items it uses (dom::Id) \
                 down",
                "crates/l10_foundation/f/Cargo.toml:6",
            )],
        ),
        (
            "a target's dependency",
            &[("pplx-space-f", "target.'cfg(unix)'.dependencies", "pplx-space-d")],
            "",
            vec![],
            &[(
                crossing,
                "crates/l10_foundation/f/Cargo.toml: pplx-space-f (l10_foundation) depends on \
                 pplx-space-d (l20_domain), a normal dependency, for target cfg(unix),",
                "crates/l10_foundation/f/Cargo.toml:6",
            )],
        ),
        (
            "a dev-dependency duplicating a normal one",
            &[
                ("pplx-space-f", "dependencies", "pplx-space-d"),
                ("pplx-space-f", "dev-dependencies", "pplx-space-d"),
            ],
            "",
            vec![],
            &[(
                crossing,
                "crates/l10_foundation/f/Cargo.toml: pplx-space-f (l10_foundation) depends on \
                 pplx-space-d (l20_domain), a normal dependency,",
                "crates/l10_foundation/f/Cargo.toml:6",
            )],
        ),
        (
            "blocking dependents",
            &[
                ("pplx-space-f", "dependencies", "pplx-space-d"),
                ("pplx-space-g", "dependencies", "pplx-space-f"),
                ("pplx-space-d", "dependencies", "pplx-space-f"),
                ("pplx-space-a", "dev-dependencies", "pplx-space-f"),
            ],
            "",
            vec![],
            &[(
                crossing,
                "crates/l10_foundation/f/Cargo.toml: pplx-space-f (l10_foundation) depends on \
                 pplx-space-d (l20_domain), a normal dependency, but l10_foundation may depend \
                 only on l10_foundation. Fix one of: move the items it uses down into \
                 l10_foundation or below; move pplx-space-f up to l20_domain, once its \
                 dependents whose layer can't depend on l20_domain have moved too \
                 (pplx-space-g); or invert the edge",
                "crates/l10_foundation/f/Cargo.toml:6",
            )],
        ),
        (
            "a dev dependent below the target's layer",
            &[
                ("pplx-space-f", "dependencies", "pplx-space-d"),
                ("pplx-space-g", "dev-dependencies", "pplx-space-f"),
            ],
            "",
            vec![],
            &[(
                crossing,
                "crates/l10_foundation/f/Cargo.toml: pplx-space-f (l10_foundation) depends on \
                 pplx-space-d (l20_domain), a normal dependency, but l10_foundation may depend \
                 only on l10_foundation. Fix one of: move the items it uses down into \
                 l10_foundation or below; move pplx-space-f up to l20_domain, once its \
                 dependents whose layer can't depend on l20_domain have moved too \
                 (pplx-space-g); or invert the edge",
                "crates/l10_foundation/f/Cargo.toml:6",
            )],
        ),
        (
            "Domain to Storage, where moving up makes it a store's user",
            &[("pplx-space-d", "dependencies", "pplx-space-s-store")],
            "",
            vec![],
            &[(
                crossing,
                "crates/l20_domain/d/Cargo.toml: pplx-space-d (l20_domain) depends on \
                 pplx-space-s-store (l30_storage), a normal dependency, but l20_domain may depend \
                 only on l20_domain and l10_foundation. Fix one of: move the items it uses down \
                 into l20_domain or below; move pplx-space-d up to l30_storage; no dependent's \
                 layer blocks that, and as a store, each store it uses must then list it in \
                 [rules.layers] store_users; or invert the edge",
                "crates/l20_domain/d/Cargo.toml:6",
            )],
        ),
        (
            "Domain to App, where moving up can't help",
            &[("pplx-space-d", "dependencies", "pplx-space-p")],
            "",
            vec![],
            &[(
                crossing,
                "crates/l20_domain/d/Cargo.toml: pplx-space-d (l20_domain) depends on \
                 pplx-space-p (l50_app), a normal dependency, but l20_domain may depend only on \
                 l20_domain and l10_foundation. Fix one of: move the items it uses down into \
                 l20_domain or below; or invert the edge, with a trait that pplx-space-d defines \
                 and pplx-space-p implements. See crates/tools/archcheck/README.md#layers.",
                "crates/l20_domain/d/Cargo.toml:6",
            )],
        ),
    ];
    for (name, dependencies, tables, files, expected) in cases {
        let ws = Layered {
            dependencies,
            tables,
            files: files.clone(),
            ..Layered::default()
        }
        .build();
        assert_layer_findings(name, &layer_findings(&load(ws.path(), None)), expected);
    }
}

/// `[layers.direction]`'s edges are tolerated while they cross, reported once
/// they don't, and compared with the base branch's copy: an entry its table
/// lacks is new, unless the base has no layers rule yet, when the pull
/// request seeds the table and each entry is advisory. A base whose
/// `ARCHITECTURE.toml` has the rule, even one this branch can't read, but
/// whose baseline has no layers tables tolerates nothing.
#[test]
fn layers_baselined_checks() {
    const FD: (&str, &str) = ("pplx-space-f", "pplx-space-d");
    const CROSSING: [(&str, &str, &str); 1] = [("pplx-space-f", "dependencies", "pplx-space-d")];
    let with_rule = layers_config("", "required");
    let without_rule = config(ADVISORY);
    // Another schema of the rule, as a pull request that changes it sees the
    // base's: the rule is there, so it does not seed the tables.
    let other_shape = format!("{without_rule}[rules.layers]\nunmoved = 5\n");
    let no_tables = baseline(&[]);
    let empty_tables = layers_baseline(&[], &[]);
    let dev_only = "[file_size]\n[layers.dev_direction]\n[crate_names]\n".to_owned();
    let listed = layers_baseline(&[FD], &[]);
    let entry_line = "architecture-baseline.toml:4";
    let new = "pplx-space-f -> pplx-space-d: is in [layers.direction] in architecture-baseline.toml \
               but not in the base branch's copy, and layer violations can't be added to the \
               baseline.";
    let seeds = "pplx-space-f -> pplx-space-d: is in [layers.direction] in \
                 architecture-baseline.toml, and the base branch has no layers rule yet, so this \
                 pull request seeds the table. Review each entry.";
    let stale = |to: &str, reason: &str| {
        format!(
            "pplx-space-f -> {to}: is in [layers.direction] in architecture-baseline.toml, but \
             {reason}. Remove the entry (cargo arch --write-baseline=layers.direction)."
        )
    };
    /// Name, baseline edges, dependencies, base baseline, base architecture,
    /// and `(code, start of message, required)` of each finding, all at the
    /// entry's line.
    type Case<'a> = (
        &'a str,
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str, &'a str)],
        Option<&'a str>,
        Option<&'a str>,
        Vec<(&'a str, String, bool)>,
    );
    let cases: Vec<Case> = vec![
        ("a listed edge", &[FD], &CROSSING, None, None, vec![]),
        (
            "a listed edge whose dependency is gone",
            &[FD],
            &[],
            None,
            None,
            vec![(
                "layers.direction.stale_entries",
                stale(
                    "pplx-space-d",
                    "pplx-space-f has no normal or build dependency on pplx-space-d",
                ),
                true,
            )],
        ),
        (
            "a listed edge the layer order allows",
            &[("pplx-space-f", "pplx-space-g")],
            &[("pplx-space-f", "dependencies", "pplx-space-g")],
            None,
            None,
            vec![(
                "layers.direction.stale_entries",
                stale(
                    "pplx-space-g",
                    "the layer order allows that edge now, or another layers check reports it",
                ),
                true,
            )],
        ),
        (
            "a listed edge of a deleted package",
            &[("pplx-space-gone", "pplx-space-d")],
            &[],
            None,
            None,
            vec![(
                "layers.direction.stale_entries",
                "pplx-space-gone -> pplx-space-d: is in [layers.direction] in \
                 architecture-baseline.toml, but no workspace member is named pplx-space-gone."
                    .to_owned(),
                true,
            )],
        ),
        (
            "an entry the base's table has",
            &[FD],
            &CROSSING,
            Some(&listed),
            Some(&with_rule),
            vec![],
        ),
        (
            "an entry the base's table lacks",
            &[FD],
            &CROSSING,
            Some(&empty_tables),
            Some(&with_rule),
            vec![("layers.direction.no_new_entries", new.to_owned(), true)],
        ),
        (
            "a base without the table but with another layers table",
            &[FD],
            &CROSSING,
            Some(&dev_only),
            None,
            vec![("layers.direction.no_new_entries", new.to_owned(), true)],
        ),
        (
            "a base without the rule seeds the table",
            &[FD],
            &CROSSING,
            Some(&no_tables),
            Some(&without_rule),
            vec![("layers.direction.no_new_entries", seeds.to_owned(), false)],
        ),
        (
            "a base baseline without the rule, and no base architecture",
            &[FD],
            &CROSSING,
            Some(&no_tables),
            None,
            vec![("layers.direction.no_new_entries", seeds.to_owned(), false)],
        ),
        (
            "a base with the rule but no layers tables",
            &[FD],
            &CROSSING,
            Some(&no_tables),
            Some(&with_rule),
            vec![("layers.direction.no_new_entries", new.to_owned(), true)],
        ),
        (
            "a base whose rule can't be read, and no layers tables",
            &[FD],
            &CROSSING,
            Some(&no_tables),
            Some(&other_shape),
            vec![("layers.direction.no_new_entries", new.to_owned(), true)],
        ),
        (
            "a base architecture that is not TOML, and no layers tables",
            &[FD],
            &CROSSING,
            Some(&no_tables),
            Some("[rules\n"),
            vec![("layers.direction.no_new_entries", new.to_owned(), true)],
        ),
    ];
    for (name, edges, dependencies, base, architecture, expected) in cases {
        let ws = Layered {
            dependencies,
            files: vec![("architecture-baseline.toml", layers_baseline(edges, &[]))],
            ..Layered::default()
        }
        .build();
        let ctx = load_with(ws.path(), base, architecture);
        let found: Vec<_> = check(&ctx)
            .into_iter()
            .filter(|finding| finding.rule == "layers" && finding.behaviour != "policy_changed")
            .collect();
        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for (finding, (code, message, required)) in found.iter().zip(&expected) {
            assert_eq!(finding.code(), *code, "{name}");
            assert!(
                finding.message().starts_with(message.as_str()),
                "{name}: {}",
                finding.message()
            );
            assert_eq!(finding.mode.is_required(), *required, "{name}");
            let at = format!("{}:{}", finding.location.path, finding.location.line);
            assert_eq!(at, entry_line, "{name}");
        }
        let skips = skipped(&ctx);
        assert_eq!(
            skips
                .iter()
                .any(|line| line.contains("layers.direction.no_new_entries")),
            base.is_none(),
            "{name}: {skips:?}"
        );
    }
}

/// A crate that moves into its layer directory leaves its `unmoved` entry
/// stale, and its baseline entries, keyed by package name, keep matching.
#[test]
fn layers_a_crate_moving_into_its_layer_directory() {
    let unmoved = "[rules.layers.unmoved]\nl10_foundation = [\"pplx-space-old\"]\n";
    let edges = [("pplx-space-old", "pplx-space-d")];
    let baseline = layers_baseline(&edges, &[]);
    let base_architecture = layers_config(unmoved, "required");
    for (member, expected) in [
        ("crates/libs/old", &[][..]),
        (
            "crates/l10_foundation/old",
            &[(
                "layers.stale_entries",
                "pplx-space-old: is in [rules.layers] unmoved in ARCHITECTURE.toml, but the crate \
                 is at crates/l10_foundation/old, whose directory gives its layer. Remove the \
                 entry.",
                "ARCHITECTURE.toml:",
            )][..],
        ),
    ] {
        let extra = [(member, "pplx-space-old")];
        let ws = Layered {
            extra: &extra,
            dependencies: &[("pplx-space-old", "dependencies", "pplx-space-d")],
            tables: unmoved,
            files: vec![("architecture-baseline.toml", baseline.clone())],
        }
        .build();
        let ctx = load_with(ws.path(), Some(&baseline), Some(&base_architecture));
        let found = layer_findings(&ctx);
        let line = format!(
            "ARCHITECTURE.toml:{}",
            line_with(&base_architecture, "\"pplx-space-old\"")
        );
        let expected: Vec<(&str, &str, &str)> = expected
            .iter()
            .map(|(code, message, _)| (*code, *message, line.as_str()))
            .collect();
        assert_layer_findings(member, &found, &expected);
    }
}

/// One row per edge: the dependencies, `[rules.layers]`' tables, the baseline's
/// `[layers.dev_direction]` edges, and each layers finding. Dev edges point
/// at the crate's own layer or a lower one, or at test support; normal and
/// build edges into test support, and any edge into tools, are `test_only`;
/// a client depends only on Domain, Foundation and other clients; an API or
/// Storage crate depends on a store only if the store lists it.
#[test]
fn layers_edge_checks() {
    const S: &str = "crates/l30_storage/s/store/Cargo.toml";
    const D: &str = "crates/l20_domain/d/Cargo.toml";
    const A: &str = "crates/l40_api/a/server/Cargo.toml";
    const CLIENT: &str = "crates/l40_api/a/client/Cargo.toml";
    type Case<'a> = (
        &'a str,
        &'a [(&'a str, &'a str, &'a str)],
        &'a str,
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str, &'a str)],
    );
    let at = |manifest: &str| format!("{manifest}:6");
    let (s, d, a, client) = (at(S), at(D), at(A), at(CLIENT));
    fn dev<'a>(from: &'a str, to: &'a str) -> [(&'a str, &'a str, &'a str); 1] {
        [(from, "dev-dependencies", to)]
    }
    fn normal<'a>(from: &'a str, to: &'a str) -> [(&'a str, &'a str, &'a str); 1] {
        [(from, "dependencies", to)]
    }
    let s_dev_a = dev("pplx-space-s-store", "pplx-space-a");
    let s_dev_fixtures = dev("pplx-space-s-store", "pplx-space-fixtures");
    let f_dev_g = dev("pplx-space-f", "pplx-space-g");
    let a_dev_d = dev("pplx-space-a", "pplx-space-d");
    let d_fixtures = normal("pplx-space-d", "pplx-space-fixtures");
    let d_dev_fixtures = dev("pplx-space-d", "pplx-space-fixtures");
    let d_bench = normal("pplx-space-d", "pplx-space-bench");
    let d_dev_bench = dev("pplx-space-d", "pplx-space-bench");
    let bench_fixtures = normal("pplx-space-bench", "pplx-space-fixtures");
    let fixtures_a = normal("pplx-space-fixtures", "pplx-space-a");
    let client_a = normal("pplx-space-a-client", "pplx-space-a");
    let client_store = normal("pplx-space-a-client", "pplx-space-s-store");
    let client_client = normal("pplx-space-a-client", "pplx-space-b-client");
    let client_d = normal("pplx-space-a-client", "pplx-space-d");
    let a_store = normal("pplx-space-a", "pplx-space-s-store");
    let s_store = normal("pplx-space-s-store", "pplx-space-t-store");
    let users = "[rules.layers.store_users]\npplx-space-s-store = [\"pplx-space-a\"]\n\
                 pplx-space-t-store = [\"pplx-space-s-store\"]\n";
    let dev_crossing = "crates/l30_storage/s/store/Cargo.toml: pplx-space-s-store (l30_storage) \
                        dev-depends on pplx-space-a (l40_api), a dev-dependency, but a \
                        dev-dependency may point only at the crate's own or a lower layer, or at \
                        test_support.";
    let fixtures = "crates/l20_domain/d/Cargo.toml: pplx-space-d (l20_domain) depends on \
                    pplx-space-fixtures, a test_support crate, a normal dependency, but test \
                    support is reachable only through [dev-dependencies].";
    let tools = "crates/l20_domain/d/Cargo.toml: pplx-space-d depends on pplx-space-bench, a \
                 tools crate,";
    let store = |from: &str, layer: &str, to: &str| {
        format!(
            "{}: {from} ({layer}) depends on the store {to}, a normal dependency, but {to} does \
             not list it in [rules.layers] store_users in ARCHITECTURE.toml.",
            if from == "pplx-space-a" { A } else { S }
        )
    };
    let a_unlisted = format!(
        "{} A store is private to the crates that own its state: reach the state through its \
         owner's API (the crate root or its client), or, if pplx-space-a owns this state, add it \
         to pplx-space-s-store's users, which layers.policy_changed reports. See \
         crates/tools/archcheck/README.md#layers.",
        store("pplx-space-a", "l40_api", "pplx-space-s-store")
    );
    // A store can't go through an API crate.
    let s_unlisted = format!(
        "{} A store reads another only as one of its users: if pplx-space-s-store owns this \
         state, add it to pplx-space-t-store's users, which layers.policy_changed reports; \
         otherwise move the read into pplx-space-t-store, the store that owns the state. See \
         crates/tools/archcheck/README.md#layers.",
        store("pplx-space-s-store", "l30_storage", "pplx-space-t-store")
    );
    let clients = |to: &str| {
        format!(
            "crates/l40_api/a/client/Cargo.toml: pplx-space-a-client is a client (an l40_api crate \
             named *-client), which depends only on l20_domain, l10_foundation and other \
             clients, but it depends on {to},"
        )
    };
    let (client_api, client_storage) = (
        clients("pplx-space-a (l40_api)"),
        clients("pplx-space-s-store (l30_storage)"),
    );
    let dev_stale = "pplx-space-s-store -> pplx-space-a: is in [layers.dev_direction] in \
                     architecture-baseline.toml, but pplx-space-s-store has no dev-dependency on \
                     pplx-space-a.";
    let s_a_normal = normal("pplx-space-s-store", "pplx-space-a");
    let both: Vec<_> = s_a_normal.iter().chain(&s_dev_a).copied().collect();
    let duplicate = "pplx-space-s-store -> pplx-space-a: is in [layers.dev_direction] in \
                     architecture-baseline.toml, but pplx-space-s-store's dev-dependency on \
                     pplx-space-a duplicates a normal or build one, which [layers.direction] \
                     covers.";
    let listed = [("pplx-space-s-store", "pplx-space-a")];
    let dev_entry = "architecture-baseline.toml:3";
    let cases: &[Case] = &[
        (
            "Storage dev to API",
            &s_dev_a,
            "",
            &[],
            &[("layers.dev_direction.no_crossing", dev_crossing, &s)],
        ),
        ("Storage dev to API, listed", &s_dev_a, "", &listed, &[]),
        (
            "Storage dev to API, listed but gone",
            &[],
            "",
            &listed,
            &[("layers.dev_direction.stale_entries", dev_stale, dev_entry)],
        ),
        (
            "listed dev edge duplicating a normal one",
            &both,
            "",
            &listed,
            &[
                (
                    "layers.direction.no_crossing",
                    "crates/l30_storage/s/store/Cargo.toml: pplx-space-s-store (l30_storage) \
                     depends on pplx-space-a (l40_api), a normal dependency,",
                    &s,
                ),
                ("layers.dev_direction.stale_entries", duplicate, dev_entry),
            ],
        ),
        ("dev to test support", &s_dev_fixtures, "", &[], &[]),
        ("dev within a layer", &f_dev_g, "", &[], &[]),
        ("dev to a lower layer", &a_dev_d, "", &[], &[]),
        (
            "normal into test support",
            &d_fixtures,
            "",
            &[],
            &[("layers.test_only", fixtures, &d)],
        ),
        ("dev into test support", &d_dev_fixtures, "", &[], &[]),
        (
            "normal into tools",
            &d_bench,
            "",
            &[],
            &[("layers.test_only", tools, &d)],
        ),
        (
            "dev into tools",
            &d_dev_bench,
            "",
            &[],
            &[("layers.test_only", tools, &d)],
        ),
        ("tools into test support", &bench_fixtures, "", &[], &[]),
        ("test support into API", &fixtures_a, "", &[], &[]),
        (
            "a client to an API crate",
            &client_a,
            "",
            &[],
            &[("layers.clients", &client_api, &client)],
        ),
        (
            "a client to a store",
            &client_store,
            users,
            &[],
            &[("layers.clients", &client_storage, &client)],
        ),
        ("a client to a client", &client_client, "", &[], &[]),
        ("a client to Domain", &client_d, "", &[], &[]),
        (
            "API to an unlisted store",
            &a_store,
            "",
            &[],
            &[("layers.store_edges", &a_unlisted, &a)],
        ),
        ("API to a store listing it", &a_store, users, &[], &[]),
        (
            "Storage to an unlisted store",
            &s_store,
            "",
            &[],
            &[("layers.store_edges", &s_unlisted, &s)],
        ),
        ("Storage to a store listing it", &s_store, users, &[], &[]),
    ];
    for (name, dependencies, tables, edges, expected) in cases {
        let ws = Layered {
            dependencies,
            tables,
            files: vec![("architecture-baseline.toml", layers_baseline(&[], edges))],
            ..Layered::default()
        }
        .build();
        assert_layer_findings(name, &layer_findings(&load(ws.path(), None)), expected);
    }
}

/// `[rules.layers]` entries that no longer apply are stale, each shown on its
/// line in `ARCHITECTURE.toml`: an `unmoved` crate that is gone or in a layer
/// directory, a `store_users` store that is not a Storage member or a user
/// that can't use a store by its list, and an `allowed_edges` edge that does
/// not exist or needs no exception.
#[test]
fn layers_stale_config_entries() {
    /// Name, the text of the finding's line, `[rules.layers]`' tables, the
    /// dependencies, extra members, and the finding's message.
    type Case<'a> = (
        &'a str,
        &'a str,
        &'a str,
        &'a [(&'a str, &'a str, &'a str)],
        &'a [(&'a str, &'a str)],
        &'a str,
    );
    let d_fixtures = [("pplx-space-d", "dependencies", "pplx-space-fixtures")];
    let p_a = [("pplx-space-p", "dependencies", "pplx-space-a")];
    let cases: &[Case] = &[
        (
            "an unmoved entry for no member",
            "\"pplx-space-nobody\"",
            "[rules.layers.unmoved]\nl20_domain = [\"pplx-space-nobody\"]\n",
            &[],
            &[],
            "pplx-space-nobody: is in [rules.layers] unmoved in ARCHITECTURE.toml, but no \
             workspace member has this name. Remove the entry.",
        ),
        (
            "an unmoved entry for a crate in a layer directory",
            "\"pplx-space-f\"",
            "[rules.layers.unmoved]\nl20_domain = [\"pplx-space-f\"]\n",
            &[],
            &[],
            "pplx-space-f: is in [rules.layers] unmoved in ARCHITECTURE.toml, but the crate is \
             at crates/l10_foundation/f, whose directory gives its layer.",
        ),
        (
            "a store that is no member",
            "pplx-space-nobody = ",
            "[rules.layers.store_users]\npplx-space-nobody = [\"pplx-space-a\"]\n",
            &[],
            &[],
            "pplx-space-nobody: is in [rules.layers] store_users in ARCHITECTURE.toml, but no \
             workspace member has this name.",
        ),
        (
            "a store that is not a Storage crate",
            "pplx-space-d = ",
            "[rules.layers.store_users]\npplx-space-d = [\"pplx-space-a\"]\n",
            &[],
            &[],
            "pplx-space-d: is in [rules.layers] store_users in ARCHITECTURE.toml, but it is not \
             a store: its layer is l20_domain, not l30_storage.",
        ),
        (
            "a user that is no member",
            "\"pplx-space-nobody\",",
            "[rules.layers.store_users]\npplx-space-s-store = [\n  \"pplx-space-a\",\n  \
             \"pplx-space-nobody\",\n]\n",
            &[],
            &[],
            "pplx-space-s-store: lists pplx-space-nobody in [rules.layers] store_users in \
             ARCHITECTURE.toml, but no workspace member has this name, so the entry allows \
             nothing.",
        ),
        (
            "a user that is an app",
            "[\"pplx-space-p\"]",
            "[rules.layers.store_users]\npplx-space-s-store = [\"pplx-space-p\"]\n",
            &[],
            &[],
            "pplx-space-s-store: lists pplx-space-p in [rules.layers] store_users in \
             ARCHITECTURE.toml, but only l40_api and l30_storage crates use stores by their \
             users lists, and its layer is l50_app,",
        ),
        (
            "a user that is a client",
            "[\"pplx-space-a-client\"]",
            "[rules.layers.store_users]\npplx-space-s-store = [\"pplx-space-a-client\"]\n",
            &[],
            &[],
            "pplx-space-s-store: lists pplx-space-a-client in [rules.layers] store_users in \
             ARCHITECTURE.toml, but it is a client, which never depends on a store,",
        ),
        (
            "an allowed edge that does not exist",
            "pplx-space-p = ",
            "[rules.layers.allowed_edges]\npplx-space-p = [\"pplx-space-s-store\"]\n",
            &[],
            &[],
            "pplx-space-p: [rules.layers] allowed_edges in ARCHITECTURE.toml lets it depend on \
             pplx-space-s-store, but pplx-space-p has no normal or build dependency on \
             pplx-space-s-store. Remove pplx-space-s-store from the list.",
        ),
        (
            "an allowed edge the layer order allows",
            "pplx-space-p = ",
            "[rules.layers.allowed_edges]\npplx-space-p = [\"pplx-space-a\"]\n",
            &p_a,
            &[],
            "pplx-space-p: [rules.layers] allowed_edges in ARCHITECTURE.toml lets it depend on \
             pplx-space-a, but the layer order already allows that edge.",
        ),
        (
            "an allowed edge into test support",
            "pplx-space-d = ",
            "[rules.layers.allowed_edges]\npplx-space-d = [\"pplx-space-fixtures\"]\n",
            &d_fixtures,
            &[],
            "pplx-space-d: [rules.layers] allowed_edges in ARCHITECTURE.toml lets it depend on \
             pplx-space-fixtures, but allowed_edges does not exempt an edge from \
             layers.test_only, which reports it.",
        ),
    ];
    for (name, at, tables, dependencies, extra, expected) in cases {
        let ws = Layered {
            extra,
            dependencies,
            tables,
            ..Layered::default()
        }
        .build();
        let line = line_with(&layers_config(tables, "required"), at);
        let found: Vec<_> = layer_findings(&load(ws.path(), None))
            .into_iter()
            .filter(|(code, _, _)| code == "layers.stale_entries")
            .collect();
        assert_layer_findings(
            name,
            &found,
            &[(
                "layers.stale_entries",
                expected,
                &format!("ARCHITECTURE.toml:{line}"),
            )],
        );
    }

    // An entry naming a crate without a layer is not stale: the crate's
    // placement finding covers it.
    let tables = "[rules.layers.store_users]\npplx-space-old = [\"pplx-space-a\"]\n\
                  pplx-space-s-store = [\"pplx-space-old\"]\n\
                  [rules.layers.allowed_edges]\npplx-space-old = [\"pplx-space-d\"]\n\
                  pplx-space-p = [\"pplx-space-old\"]\n";
    let ws = Layered {
        extra: &[("crates/libs/old", "pplx-space-old")],
        dependencies: &[
            ("pplx-space-old", "dependencies", "pplx-space-d"),
            ("pplx-space-p", "dependencies", "pplx-space-old"),
        ],
        tables,
        ..Layered::default()
    }
    .build();
    let codes: Vec<String> = layer_findings(&load(ws.path(), None))
        .into_iter()
        .map(|(code, _, _)| code)
        .collect();
    assert_eq!(codes, ["layers.placement"]);
}

/// Every change to `[rules.layers]` from the base branch's copy is shown on
/// its line, except a removed `unmoved` entry. A base table of another shape
/// skips the comparison with a warning, and `crate_names.policy_changed`
/// still compares its own table.
#[test]
fn layers_policy_changed() {
    let unmoved =
        |layer: &str, packages: &str| format!("[rules.layers.unmoved]\n{layer} = [\n{packages}]\n");
    let old = "  \"pplx-space-old\",\n";
    let new = "  \"pplx-space-new\",\n";
    let users =
        |list: &str| format!("[rules.layers.store_users]\npplx-space-s-store = [\n{list}]\n");
    let a = "  \"pplx-space-a\",\n";
    let t = "  \"pplx-space-t-store\",\n";
    let edges = |list: &str| format!("[rules.layers.allowed_edges]\npplx-space-p = [\n{list}]\n");
    let storage = |drivers: &str, helpers: &str| {
        format!("[rules.layers.storage]\ndrivers = [\n  {drivers},\n]\nhelpers = [{helpers}]\n")
    };
    let legacy = |list: &str| {
        format!("[rules.layers.unmoved]\nl20_domain = []\n[rules.layers]\nlegacy = [{list}]\n")
    };
    // After another `[rules.layers…]` header, as in the repository's copy.
    let protocols = |list: &str| {
        format!("[rules.layers.unmoved]\nl20_domain = []\n[rules.layers]\nprotocols = [{list}]\n")
    };
    let fences = |runtime: &str, http: &str, kube: &str| {
        format!(
            "[rules.layers.domain_fences]\nruntime = [{runtime}]\nhttp = [\n  {http},\n]\n\
             kube = [{kube}]\n"
        )
    };
    let required = layers_config("", "required");
    let advisory_crossing = required.replace(
        "[rules.layers.direction.no_crossing]\nmode = \"required\"",
        "[rules.layers.direction.no_crossing]\nmode = \"advisory\"",
    );
    /// Name, the base's `ARCHITECTURE.toml`, this branch's, and each finding
    /// as the text of its line and its message.
    type Case<'a> = (&'a str, String, String, &'a [(&'a str, &'a str)]);
    let both = format!("{old}{new}");
    let cases: Vec<Case> = vec![
        ("unchanged", required.clone(), required.clone(), &[]),
        (
            "the whole table new",
            config(ADVISORY),
            required.clone(),
            &[(
                "[rules.layers.storage]",
                "ARCHITECTURE.toml: [rules.layers] is not in the base branch's copy; the whole \
                 policy is new.",
            )],
        ),
        (
            "an unmoved entry added",
            layers_config(&unmoved("l20_domain", old), "required"),
            layers_config(&unmoved("l20_domain", &both), "required"),
            &[(
                "\"pplx-space-new\"",
                "ARCHITECTURE.toml: `pplx-space-new` was added to [rules.layers] unmoved, in \
                 l20_domain.",
            )],
        ),
        (
            "an unmoved entry's layer changed",
            layers_config(&unmoved("l20_domain", old), "required"),
            layers_config(&unmoved("l10_foundation", old), "required"),
            &[(
                "\"pplx-space-old\"",
                "ARCHITECTURE.toml: `pplx-space-old` moved from l20_domain to l10_foundation in \
                 [rules.layers] unmoved.",
            )],
        ),
        (
            "an unmoved entry removed",
            layers_config(&unmoved("l20_domain", &both), "required"),
            layers_config(&unmoved("l20_domain", old), "required"),
            &[],
        ),
        (
            "a store user added",
            layers_config(&users(a), "required"),
            layers_config(&users(&format!("{a}{t}")), "required"),
            &[(
                "\"pplx-space-t-store\"",
                "ARCHITECTURE.toml: `pplx-space-t-store` was added to the users of \
                 `pplx-space-s-store` in [rules.layers] store_users.",
            )],
        ),
        (
            "a store user removed",
            layers_config(&users(&format!("{a}{t}")), "required"),
            layers_config(&users(a), "required"),
            &[(
                "pplx-space-s-store = [",
                "ARCHITECTURE.toml: `pplx-space-t-store` was removed from the users of \
                 `pplx-space-s-store` in [rules.layers] store_users.",
            )],
        ),
        (
            "an allowed edge added",
            required.clone(),
            layers_config(&edges("  \"pplx-space-s-store\",\n"), "required"),
            &[(
                "\"pplx-space-s-store\"",
                "ARCHITECTURE.toml: `pplx-space-s-store` was added to the allowed edges of \
                 `pplx-space-p` in [rules.layers] allowed_edges.",
            )],
        ),
        (
            "storage lists changed",
            layers_config(&storage("\"sqlx\"", "\"pplx-space-g\""), "required"),
            layers_config(&storage("\"sqlx\",\n  \"redis\"", ""), "required"),
            &[
                (
                    "  \"redis\"",
                    "ARCHITECTURE.toml: `redis` was added to [rules.layers.storage] drivers.",
                ),
                (
                    "[rules.layers.storage]",
                    "ARCHITECTURE.toml: `pplx-space-g` was removed from [rules.layers.storage] \
                     helpers.",
                ),
            ],
        ),
        (
            "legacy changed",
            layers_config(&legacy("\"pplx-space-old\""), "required"),
            layers_config(&legacy("\"pplx-space-new\""), "required"),
            &[
                (
                    "legacy = ",
                    "ARCHITECTURE.toml: `pplx-space-new` was added to [rules.layers] legacy.",
                ),
                (
                    "legacy = ",
                    "ARCHITECTURE.toml: `pplx-space-old` was removed from [rules.layers] legacy.",
                ),
            ],
        ),
        (
            "legacy removed",
            layers_config(&legacy("\"pplx-space-old\""), "required"),
            layers_config(
                "[rules.layers.unmoved]\nl20_domain = []\n[rules.layers]\n",
                "required",
            ),
            &[(
                "[rules.layers]",
                "ARCHITECTURE.toml: `pplx-space-old` was removed from [rules.layers] legacy.",
            )],
        ),
        (
            "protocols changed",
            layers_config(&protocols("\"pplx-space-old\""), "required"),
            layers_config(&protocols("\"pplx-space-new\""), "required"),
            &[
                (
                    "protocols = ",
                    "ARCHITECTURE.toml: `pplx-space-new` was added to [rules.layers] protocols.",
                ),
                (
                    "protocols = ",
                    "ARCHITECTURE.toml: `pplx-space-old` was removed from [rules.layers] \
                     protocols.",
                ),
            ],
        ),
        (
            "protocols removed",
            layers_config(&protocols("\"pplx-space-old\""), "required"),
            layers_config(
                "[rules.layers.unmoved]\nl20_domain = []\n[rules.layers]\n",
                "required",
            ),
            &[(
                "[rules.layers]",
                "ARCHITECTURE.toml: `pplx-space-old` was removed from [rules.layers] protocols.",
            )],
        ),
        (
            "domain_fences changed",
            layers_config(
                &fences("\"async-std\", \"smol\", \"tokio\"", "\"hyper\"", ""),
                "required",
            ),
            layers_config(
                &fences("\"tokio\"", "\"hyper\",\n  \"reqwest\"", "\"smol\""),
                "required",
            ),
            &[
                (
                    "  \"reqwest\"",
                    "ARCHITECTURE.toml: `reqwest` was added to [rules.layers.domain_fences], in \
                     http.",
                ),
                (
                    "kube = ",
                    "ARCHITECTURE.toml: `smol` moved from runtime to kube in \
                     [rules.layers.domain_fences].",
                ),
                (
                    "[rules.layers.domain_fences]",
                    "ARCHITECTURE.toml: `async-std` was removed from [rules.layers.domain_fences], \
                     where it was in runtime.",
                ),
            ],
        ),
        (
            "a Domain check's mode changed",
            required.clone(),
            required.replace(
                "[rules.layers.model_protocol.no_crossing]\nmode = \"required\"",
                "[rules.layers.model_protocol.no_crossing]\nmode = \"advisory\"",
            ),
            &[(
                "[rules.layers.model_protocol.no_crossing]",
                "ARCHITECTURE.toml: layers.model_protocol.no_crossing mode changed from \
                 required to advisory.",
            )],
        ),
        (
            "a storage check's mode changed",
            required.clone(),
            required.replace(
                "[rules.layers.codec_features.stale_entries]\nmode = \"required\"",
                "[rules.layers.codec_features.stale_entries]\nmode = \"advisory\"",
            ),
            &[(
                "[rules.layers.codec_features.stale_entries]",
                "ARCHITECTURE.toml: layers.codec_features.stale_entries mode changed from \
                 required to advisory.",
            )],
        ),
        (
            "a mode changed",
            required.clone(),
            advisory_crossing.clone(),
            &[(
                "[rules.layers.direction.no_crossing]",
                "ARCHITECTURE.toml: layers.direction.no_crossing mode changed from required to \
                 advisory.",
            )],
        ),
    ];
    for (name, base, policy, expected) in &cases {
        let ws = Layered {
            files: vec![("ARCHITECTURE.toml", policy.clone())],
            ..Layered::default()
        }
        .build();
        let ctx = load_with(ws.path(), None, Some(base));
        let found: Vec<_> = check(&ctx)
            .into_iter()
            .filter(|finding| finding.code() == "layers.policy_changed")
            .collect();
        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for (finding, (line, message)) in found.iter().zip(*expected) {
            // A mode is on the line after its table's header.
            let mut want = line_with(policy, line);
            if line.starts_with("[rules.layers.direction")
                || line.starts_with("[rules.layers.codec_features")
                || line.starts_with("[rules.layers.model_protocol")
            {
                want += 1;
            }
            assert_eq!(finding.message(), *message, "{name}");
            assert_eq!(finding.location.path, "ARCHITECTURE.toml", "{name}");
            assert_eq!(finding.location.line, want, "{name}");
            assert!(!finding.mode.is_required(), "{name}");
        }
        assert!(
            !skipped(&ctx)
                .iter()
                .any(|line| line.contains("layers.policy_changed")),
            "{name}: {:?}",
            skipped(&ctx)
        );
    }

    // A crate whose unmoved entry is removed as it moves into another
    // layer's directory changes layer; one moving into its entry's layer
    // doesn't.
    let base = layers_config(&unmoved("l30_storage", old), "required");
    for (member, expected) in [
        (
            "crates/l40_api/old",
            &[
                "ARCHITECTURE.toml: `pplx-space-old` moved from l30_storage ([rules.layers] \
               unmoved) to l40_api, at crates/l40_api/old.",
            ][..],
        ),
        ("crates/l30_storage/old", &[][..]),
    ] {
        let extra = [(member, "pplx-space-old")];
        let ws = Layered {
            extra: &extra,
            ..Layered::default()
        }
        .build();
        let found: Vec<Finding> = check(&load_with(ws.path(), None, Some(&base)))
            .into_iter()
            .filter(|finding| finding.code() == "layers.policy_changed")
            .collect();
        let messages: Vec<String> = found.iter().map(Finding::message).collect();
        assert_eq!(messages, expected, "{member}");
        for finding in found {
            assert_eq!(
                finding.location.line,
                line_with(&required, "[rules.layers.storage]")
            );
            assert!(!finding.mode.is_required());
        }
    }

    // Removing the whole table is advisory, on the file's first line.
    let ws = Layered {
        files: vec![
            ("ARCHITECTURE.toml", config(ADVISORY)),
            (
                "architecture-baseline.toml",
                "[file_size]\n[crate_names]\n".to_owned(),
            ),
        ],
        ..Layered::default()
    }
    .build();
    let found: Vec<Finding> = check(&load_with(ws.path(), None, Some(&required)))
        .into_iter()
        .filter(|finding| finding.rule == "layers")
        .collect();
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(found[0].code(), "layers.policy_changed");
    assert_eq!(
        found[0].message(),
        "ARCHITECTURE.toml: the base branch's copy has [rules.layers] and this one doesn't: the \
         whole layers rule was removed and no longer runs."
    );
    assert_eq!(
        (found[0].location.path.as_str(), found[0].location.line),
        ("ARCHITECTURE.toml", 1)
    );
    assert!(!found[0].mode.is_required());
    // Without the base, or against one without the rule, nothing is said.
    for base in [None, Some(config(ADVISORY))] {
        let ctx = load_with(ws.path(), None, base.as_deref());
        assert!(
            !check(&ctx).iter().any(|finding| finding.rule == "layers"),
            "{base:?}"
        );
        assert!(
            !skipped(&ctx).iter().any(|line| line.contains("layers.")),
            "{base:?}"
        );
    }
    // Against a base whose [rules.layers] can't be read, no finding says
    // whether the rule was removed, and a skip line says so.
    let base = format!("{}[rules.layers]\nunmoved = 5\n", config(ADVISORY));
    let ctx = load_with(ws.path(), None, Some(&base));
    assert!(!check(&ctx).iter().any(|finding| finding.rule == "layers"));
    let skips: Vec<String> = skipped(&ctx)
        .into_iter()
        .filter(|line| line.contains("layers."))
        .collect();
    assert_eq!(skips.len(), 1, "{skips:?}");
    assert!(
        skips[0].starts_with("warning: could not parse ")
            && skips[0].ends_with("; layers.policy_changed's check for the rule's removal skipped"),
        "{skips:?}"
    );

    // A base [rules.layers] of another shape: the layers comparisons are
    // skipped with a warning, and crate_names still compares its table.
    let ws = Layered::default().build();
    let base = format!(
        "{}[rules.layers]\nunmoved = 5\n",
        config(ADVISORY).replace(", \"misc\"", "")
    );
    let ctx = load_with(ws.path(), None, Some(&base));
    let codes: Vec<String> = check(&ctx)
        .iter()
        .filter(|finding| finding.behaviour == "policy_changed")
        .map(|finding| format!("{}: {}", finding.code(), finding.message()))
        .collect();
    assert_eq!(
        codes,
        [
            "crate_names.policy_changed: ARCHITECTURE.toml: `misc` was added to [rules.crate_names] \
          banned_tokens."
        ]
    );
    let skips = skipped(&ctx);
    let warning = skips
        .iter()
        .find(|line| line.contains("layers.policy_changed"))
        .expect("a skip line for the layers comparisons");
    assert!(
        warning.starts_with("warning: could not parse ")
            && warning.ends_with(
                "; layers.policy_changed and layers.placement's check for new unmoved entries \
                 skipped"
            ),
        "{warning}"
    );
    assert!(
        !skips
            .iter()
            .any(|line| line.contains("crate_names.policy_changed")),
        "{skips:?}"
    );
}

/// A `[rules.layers]` that can't be read as one layer per package, and a
/// baseline layers table that no layers check has, exit 2.
#[test]
fn layers_configuration_errors() {
    let tables = |text: &str| layers_config(text, "required");
    let unknown_table = "is not a table of the layers rule: with [rules.layers] in \
                         ARCHITECTURE.toml, its tables are layers.direction, \
                         layers.dev_direction, layers.storage_access, layers.codec_features, \
                         layers.domain_dependencies and layers.model_protocol, and without it \
                         there are none";
    let cases: &[(&str, &str, String, &str)] = &[
        (
            "an unknown layer",
            "ARCHITECTURE.toml",
            tables("[rules.layers.unmoved]\nl60_ui = [\"pplx-space-x\"]\n"),
            "unknown layer \"l60_ui\"; expected one of l10_foundation, l20_domain",
        ),
        (
            "a package in two layers",
            "ARCHITECTURE.toml",
            tables(
                "[rules.layers.unmoved]\nl10_foundation = [\"pplx-space-x\"]\n\
                 l20_domain = [\"pplx-space-x\"]\n",
            ),
            "unmoved lists \"pplx-space-x\" in both l10_foundation and l20_domain",
        ),
        (
            "a package twice in one layer",
            "ARCHITECTURE.toml",
            tables(
                "[rules.layers.unmoved]\nl10_foundation = [\"pplx-space-x\", \"pplx-space-x\"]\n",
            ),
            "\"pplx-space-x\" is listed twice",
        ),
        (
            "a package in two domain_fences families",
            "ARCHITECTURE.toml",
            tables("[rules.layers.domain_fences]\nhttp = [\"hyper\"]\nruntime = [\"hyper\"]\n"),
            "domain_fences lists \"hyper\" in both http and runtime",
        ),
        (
            "a package twice in one domain_fences family",
            "ARCHITECTURE.toml",
            tables("[rules.layers.domain_fences]\nhttp = [\"hyper\", \"hyper\"]\n"),
            "\"hyper\" is listed twice",
        ),
        (
            "a protocol listed twice",
            "ARCHITECTURE.toml",
            tables("[rules.layers]\nprotocols = [\"pplx-space-d\", \"pplx-space-d\"]\n"),
            "\"pplx-space-d\" is listed twice",
        ),
        (
            "a store user listed twice",
            "ARCHITECTURE.toml",
            tables(
                "[rules.layers.store_users]\npplx-space-s-store = [\"pplx-space-a\", \
                 \"pplx-space-a\"]\n",
            ),
            "\"pplx-space-a\" is listed twice",
        ),
        (
            "an unknown layers behaviour",
            "ARCHITECTURE.toml",
            format!(
                "{}[rules.layers.module_names]\nmode = \"required\"\n",
                tables("")
            ),
            "could not parse",
        ),
        (
            "a baseline table of no check",
            "architecture-baseline.toml",
            format!("{}[layers.foo]\n", layers_baseline(&[], &[])),
            unknown_table,
        ),
        (
            "a baseline table without the rule",
            "ARCHITECTURE.toml",
            config(ADVISORY),
            unknown_table,
        ),
        (
            "a baseline table of edges that is not a list",
            "architecture-baseline.toml",
            "[file_size]\n[layers.direction]\npplx-space-f = \"pplx-space-d\"\n[crate_names]\n"
                .to_owned(),
            "could not parse",
        ),
    ];
    for (name, path, text, error) in cases {
        let ws = Layered {
            files: vec![(path, text.clone())],
            ..Layered::default()
        }
        .build();
        let checked = run(ws.path(), &[]);
        assert_eq!(checked.code, 2, "{name}: {}", checked.stdout);
        assert!(checked.stderr.contains(error), "{name}: {}", checked.stderr);
        let write = run(ws.path(), &["--write-baseline"]);
        assert_eq!(write.code, 2, "{name}: {}", write.stdout);
        assert_eq!(
            fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap(),
            if *path == "architecture-baseline.toml" {
                text.clone()
            } else {
                layers_baseline(&[], &[])
            },
            "{name}: the baseline is left alone"
        );
    }
}

/// The `[<name>]` table of a generated baseline, up to the blank line
/// after it.
fn generated_table(text: &str, name: &str) -> String {
    let section = table_section(text, name);
    match section.split_once("\n\n") {
        Some((table, _)) => format!("{table}\n"),
        None => section,
    }
}

/// `--write-baseline` keeps each layers entry whose edge still has its
/// check's problem, removes the others, adds none, listing each as `not
/// added`, and writes both layers tables, even when empty; with
/// `=layers.direction`, it updates that table alone. The generated form is
/// written back byte-identical.
#[test]
fn layers_write_baseline() {
    const BASELINE: &str = "architecture-baseline.toml";
    let dependencies = [
        ("pplx-space-f", "dependencies", "pplx-space-d"),
        ("pplx-space-s-store", "dependencies", "pplx-space-a"),
        ("pplx-space-s-store", "dev-dependencies", "pplx-space-p"),
    ];
    let hand_written = "[file_size]\n\"crates/gone.rs\" = 9\n\
                        [layers.direction]\n\"pplx-space-f\" = [\"pplx-space-d\", \
                        \"pplx-space-g\"]\npplx-space-gone = [\"pplx-space-d\"]\n\
                        [crate_names]\n";
    let ws = Layered {
        dependencies: &dependencies,
        files: vec![(BASELINE, hand_written.to_owned())],
        ..Layered::default()
    }
    .build();

    // Scoped: only [layers.direction] changes, and the stale file_size entry
    // stays; the table the file lacks is not created.
    let scoped = run(ws.path(), &["--write-baseline=layers.direction"]);
    assert_eq!(scoped.code, 0, "{}", scoped.stderr);
    assert_eq!(
        scoped.stdout.lines().next(),
        Some(
            "not added: pplx-space-s-store (l30_storage) -> pplx-space-a (l40_api) in \
             [layers.direction]; fix the dependency (cargo arch says how): an entry added by \
             hand fails layers.direction.no_new_entries, and a reviewed, permanent exception \
             goes in [rules.layers] allowed_edges in ARCHITECTURE.toml, which \
             layers.policy_changed reports"
        ),
        "{}",
        scoped.stdout
    );
    assert!(
        !scoped.stdout.contains("dev_direction"),
        "{}",
        scoped.stdout
    );
    assert!(
        scoped
            .stdout
            .contains("archcheck: wrote 1 layers.direction entries to "),
        "{}",
        scoped.stdout
    );
    let text = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert!(text.contains("\"crates/gone.rs\" = 9"), "{text}");
    assert_eq!(
        generated_table(&text, "layers.direction"),
        "[layers.direction]\npplx-space-f = [\n  \"pplx-space-d\",\n]\n",
        "{text}"
    );
    assert!(!text.contains("[layers.dev_direction]"), "{text}");

    // Unscoped: both tables, the dev one empty although its edge is not
    // added.
    let all = run(ws.path(), &["--write-baseline"]);
    assert_eq!(all.code, 0, "{}", all.stderr);
    let mut not_added: Vec<&str> = all
        .stdout
        .lines()
        .filter(|line| line.starts_with("not added: "))
        .collect();
    not_added.sort_unstable();
    assert_eq!(not_added.len(), 2, "{}", all.stdout);
    assert!(
        not_added[0].starts_with(
            "not added: pplx-space-s-store (l30_storage) -> pplx-space-a (l40_api) in \
             [layers.direction];"
        ) && not_added[1]
            == "not added: pplx-space-s-store (l30_storage) -> pplx-space-p (l50_app) in \
                [layers.dev_direction]; fix the dependency (cargo arch says how): an entry added \
                by hand fails layers.dev_direction.no_new_entries, and \
                layers.dev_direction has no exceptions",
        "{not_added:?}"
    );
    assert!(
        all.stdout
            .contains("0 layers.dev_direction, 1 layers.direction"),
        "{}",
        all.stdout
    );
    assert!(
        !all.stdout.contains("the entries not added"),
        "no entries to paste: {}",
        all.stdout
    );
    let generated = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert!(!generated.contains("crates/gone.rs"), "{generated}");
    assert_eq!(
        generated_table(&generated, "layers.dev_direction"),
        "[layers.dev_direction]\n",
        "{generated}"
    );
    assert!(
        generated.contains(
            "\n# [layers.<check>] maps each workspace package to what it depends on against\n"
        ),
        "{generated}"
    );
    let layers_at = generated.find("[layers.dev_direction]").unwrap();
    assert!(
        generated.find("[file_size]").unwrap() < layers_at
            && layers_at < generated.find("[crate_names]").unwrap(),
        "{generated}"
    );

    // The generated form loads and is written back byte-identical, by
    // either form of the write; and it checks clean of stale entries.
    for args in [
        &["--write-baseline"][..],
        &["--write-baseline=layers.direction"],
    ] {
        assert_eq!(run(ws.path(), args).code, 0);
        assert_eq!(
            fs::read_to_string(ws.path().join(BASELINE)).unwrap(),
            generated,
            "{args:?}"
        );
    }
    let codes: Vec<String> = layer_findings(&load(ws.path(), None))
        .into_iter()
        .map(|(code, _, _)| code)
        .collect();
    assert_eq!(
        codes,
        [
            "layers.direction.no_crossing",
            "layers.dev_direction.no_crossing"
        ]
    );

    // A scoped write creates its table when the baseline lacks it, and only
    // that one.
    fs::write(ws.path().join(BASELINE), "[file_size]\n[crate_names]\n").unwrap();
    let created = run(ws.path(), &["--write-baseline=layers.direction"]);
    assert_eq!(created.code, 0, "{}", created.stderr);
    let text = fs::read_to_string(ws.path().join(BASELINE)).unwrap();
    assert_eq!(
        generated_table(&text, "layers.direction"),
        "[layers.direction]\n",
        "{text}"
    );
    assert!(!text.contains("[layers.dev_direction]"), "{text}");
}

/// A required crossing is an `ERROR` at the manifest line that declares the
/// dependency and exits 1; advisory findings alone exit 0. Without the base
/// flags, a line after the summary says what is skipped; with them, nothing
/// is, and a base baseline that is not TOML gets one warning line naming
/// the layers comparisons with the others.
#[test]
fn layers_rdjson_and_exit_codes() {
    const MANIFEST: &str = "crates/l10_foundation/f/Cargo.toml";
    let dependencies = [("pplx-space-f", "dependencies", "pplx-space-d")];
    let skips = [
        "archcheck: layers.direction.no_new_entries, layers.dev_direction.no_new_entries, \
         layers.storage_access.no_new_entries, layers.codec_features.no_new_entries, \
         layers.domain_dependencies.no_new_entries and layers.model_protocol.no_new_entries \
         skipped (no --base-baseline given)",
        "archcheck: layers.policy_changed and layers.placement's check for new unmoved entries \
         skipped (no --base-architecture given)",
    ];
    for (mode, code, severity) in [("required", 1, "ERROR"), ("advisory", 0, "WARNING")] {
        let ws = Layered {
            dependencies: &dependencies,
            files: vec![("ARCHITECTURE.toml", layers_config("", mode))],
            ..Layered::default()
        }
        .build();

        let found = run(ws.path(), &["--format", "rdjson"]);

        assert_eq!(found.code, code, "{mode}: {}", found.stderr);
        let document: Value = serde_json::from_str(&found.stdout).unwrap();
        let diagnostics: Vec<Value> = document["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| {
                json!([
                    d["code"]["value"],
                    d["severity"],
                    d["location"]["path"],
                    d["location"]["range"]["start"]["line"],
                ])
            })
            .collect();
        assert_eq!(
            diagnostics,
            [json!([
                "layers.direction.no_crossing",
                severity,
                MANIFEST,
                6
            ])],
            "{mode}"
        );
        for skip in skips {
            assert!(
                found.stderr.lines().any(|line| line == skip),
                "{mode}: {}",
                found.stderr
            );
        }
    }

    // With both bases, nothing of the rule is skipped.
    let ws = Layered {
        dependencies: &dependencies,
        files: vec![
            (
                "architecture-baseline.toml",
                layers_baseline(&[("pplx-space-f", "pplx-space-d")], &[]),
            ),
            (
                "base-baseline.toml",
                layers_baseline(&[("pplx-space-f", "pplx-space-d")], &[]),
            ),
            ("base-architecture.toml", layers_config("", "required")),
            ("not-toml.toml", "[file_size\n".to_owned()),
            (
                "layers-not-a-table.toml",
                "layers = 5\n[file_size]\n".to_owned(),
            ),
        ],
        ..Layered::default()
    }
    .build();
    let path = |name: &str| ws.path().join(name).to_str().unwrap().to_owned();
    let base_architecture = path("base-architecture.toml");
    let both = run(
        ws.path(),
        &[
            "--base-baseline",
            &path("base-baseline.toml"),
            "--base-architecture",
            &base_architecture,
        ],
    );
    assert_eq!(both.code, 0, "{}{}", both.stdout, both.stderr);
    assert!(
        !both.stdout.contains("archcheck: layers"),
        "{}",
        both.stdout
    );

    // A base baseline that is not TOML: one warning, naming every comparison.
    let not_toml = run(
        ws.path(),
        &[
            "--base-baseline",
            &path("not-toml.toml"),
            "--base-architecture",
            &base_architecture,
        ],
    );
    assert_eq!(not_toml.code, 0, "{}", not_toml.stderr);
    let warnings: Vec<&str> = not_toml
        .stdout
        .lines()
        .filter(|line| line.starts_with("archcheck: warning:"))
        .collect();
    assert_eq!(warnings.len(), 1, "{}", not_toml.stdout);
    assert!(
        warnings[0].starts_with(&format!(
            "archcheck: warning: could not parse {} at line 1: ",
            path("not-toml.toml")
        )) && warnings[0].ends_with(
            "file_size.no_new_entries and file_size.no_raised_caps, \
             layers.direction.no_new_entries, layers.dev_direction.no_new_entries, \
             layers.storage_access.no_new_entries, layers.codec_features.no_new_entries, \
             layers.domain_dependencies.no_new_entries and \
             layers.model_protocol.no_new_entries skipped"
        ),
        "{}",
        warnings[0]
    );

    // A base `layers` that is not a table of tables skips each check's
    // comparison, with a warning.
    let not_a_table = run(
        ws.path(),
        &[
            "--base-baseline",
            &path("layers-not-a-table.toml"),
            "--base-architecture",
            &base_architecture,
        ],
    );
    assert_eq!(not_a_table.code, 0, "{}", not_a_table.stdout);
    for check in [
        "direction",
        "dev_direction",
        "storage_access",
        "codec_features",
    ] {
        assert!(
            not_a_table.stdout.lines().any(|line| line
                .starts_with("archcheck: warning: could not parse ")
                && line.ends_with(&format!("; layers.{check}.no_new_entries skipped"))),
            "{check}: {}",
            not_a_table.stdout
        );
    }
}

/// The members, beyond [`LAYERED`]'s, of [`storage_workspace`]: a helper
/// that `helpers` lists, a codec crate whose default feature is a driver
/// feature, one that reaches drivers only through another codec crate, and
/// a legacy crate.
const STORAGE_EXTRA: [(&str, &str); 4] = [
    ("crates/l10_foundation/conn", "pplx-space-conn"),
    ("crates/l10_foundation/h", "pplx-space-h"),
    ("crates/l10_foundation/k", "pplx-space-k"),
    ("crates/libs/legacy", "pplx-space-legacy"),
];

/// `[rules.layers]` tables for [`storage_workspace`].
const STORAGE_TABLES: &str = "[rules.layers.unmoved]\nl10_foundation = [\"pplx-space-legacy\"]\n\
                              [rules.layers.storage]\ndrivers = [\"redis\", \"sqlx\"]\n\
                              helpers = [\"pplx-space-conn\"]\n\
                              [rules.layers]\nlegacy = [\"pplx-space-legacy\"]\n";

/// A layered workspace whose manifests reach drivers in every way the
/// storage checks tell apart, with `tables` as `[rules.layers]`'s and
/// `files` written over the rest:
///
/// - `f` is a helper by its required `sqlx`, `conn` by `helpers`;
/// - `g` and `h` are Foundation codec crates: optional `sqlx` behind a
///   `sqlx` feature (`h`'s default), and `g`'s optional `redis` behind
///   Cargo's implicit feature;
/// - `k`, a Foundation codec crate too, reaches drivers only through `g`:
///   its implicit `kv` turns on an optional dependency on `g` that requests
///   `redis`, its `pg` forwards to `kv/sqlx`, and its `json` forwards to
///   `kv/serde`, no driver feature, which reaches `redis` only by turning
///   on that dependency;
/// - the legacy crate (Foundation) has a required `sqlx`, but `legacy` lists
///   it, which keeps it from being a helper, and it forwards its `codec`
///   feature to `g`'s `sqlx`;
/// - `d` (Domain) has an optional `sqlx`, enables `g`'s `sqlx`, and its
///   `hdb` turns on an optional dependency on `h` that requests `sqlx`;
/// - `a` (API) depends on `sqlx`, `f`, `conn` and the legacy crate, enables
///   the legacy crate's `codec`, and forwards its own `pg` to `g?/sqlx`;
/// - `p` (App) depends on `redis` and enables `a`'s `pg`, `q` takes `h`'s
///   default features and enables `d`'s `hdb`, and `b-client` depends on
///   `f` and enables `g`'s implicit `redis` and `k`'s `kv` and `json`;
/// - the store, test support and tool depend on drivers and helpers freely,
///   and dev-dependencies are not checked.
fn storage_workspace(tables: &str, files: Vec<(&str, String)>) -> TempDir {
    let manifest = |name: &str, body: &str| format!("{}{body}", package(name));
    let mut all: Vec<(&str, String)> = vec![
        ("ARCHITECTURE.toml", layers_config(tables, "required")),
        (
            "crates/l10_foundation/f/Cargo.toml",
            manifest("pplx-space-f", "[dependencies]\nsqlx = \"1\"\n"),
        ),
        (
            "crates/l10_foundation/g/Cargo.toml",
            manifest(
                "pplx-space-g",
                "[features]\nserde = []\nsqlx = [\"dep:sqlx\"]\n[dependencies]\n\
                 sqlx = { version = \"1\", optional = true }\n\
                 redis = { version = \"1\", optional = true }\n",
            ),
        ),
        (
            "crates/l10_foundation/h/Cargo.toml",
            manifest(
                "pplx-space-h",
                "[features]\ndefault = [\"sqlx\"]\nsqlx = [\"dep:sqlx\"]\n[dependencies]\n\
                 sqlx = { version = \"1\", optional = true }\n",
            ),
        ),
        (
            "crates/l10_foundation/k/Cargo.toml",
            manifest(
                "pplx-space-k",
                "[features]\njson = [\"kv/serde\"]\npg = [\"kv/sqlx\"]\n[dependencies]\n\
                 kv = { package = \"pplx-space-g\", path = \"../g\", optional = true, \
                 features = [\"redis\"] }\n",
            ),
        ),
        (
            "crates/libs/legacy/Cargo.toml",
            manifest(
                "pplx-space-legacy",
                "[features]\ncodec = [\"pplx-space-g/sqlx\"]\n[dependencies]\nsqlx = \"1\"\n\
                 pplx-space-g = { path = \"../../l10_foundation/g\" }\n",
            ),
        ),
        (
            "crates/l20_domain/d/Cargo.toml",
            manifest(
                "pplx-space-d",
                "[features]\nsqlx = [\"dep:sqlx\"]\nhdb = [\"dep:pplx-space-h\"]\n\
                 [dependencies]\nsqlx = { version = \"1\", optional = true }\n\
                 pplx-space-g = { path = \"../../l10_foundation/g\", features = [\"sqlx\"] }\n\
                 pplx-space-h = { path = \"../../l10_foundation/h\", optional = true, \
                 default-features = false, features = [\"sqlx\"] }\n\
                 [dev-dependencies]\nredis = \"1\"\n",
            ),
        ),
        (
            "crates/l30_storage/s/store/Cargo.toml",
            manifest(
                "pplx-space-s-store",
                "[features]\nkubernetes = [\"dep:redis\"]\ndefault = [\"kubernetes\"]\n\
                 [dependencies]\nsqlx = \"1\"\nredis = { version = \"1\", optional = true }\n\
                 pplx-space-f = { path = \"../../../l10_foundation/f\" }\n\
                 pplx-space-g = { path = \"../../../l10_foundation/g\", features = [\"sqlx\"] }\n",
            ),
        ),
        (
            "crates/l40_api/a/server/Cargo.toml",
            manifest(
                "pplx-space-a",
                "[features]\npg = [\"pplx-space-g?/sqlx\"]\n[dependencies]\nsqlx = \"1\"\n\
                 pplx-space-f = { path = \"../../../l10_foundation/f\" }\n\
                 pplx-space-conn = { path = \"../../../l10_foundation/conn\" }\n\
                 pplx-space-g = { path = \"../../../l10_foundation/g\", optional = true }\n\
                 pplx-space-legacy = { path = \"../../../libs/legacy\", features = [\"codec\"] }\n\
                 [dev-dependencies]\n\
                 pplx-space-h = { path = \"../../../l10_foundation/h\" }\n",
            ),
        ),
        (
            "crates/l40_api/a/server/src/lib.rs",
            "use sqlx::PgPool;\n".to_owned(),
        ),
        (
            "crates/l40_api/b/client/Cargo.toml",
            manifest(
                "pplx-space-b-client",
                "[dependencies]\n\
                 pplx-space-f = { path = \"../../../l10_foundation/f\" }\n\
                 pplx-space-g = { path = \"../../../l10_foundation/g\", features = [\"redis\"] }\n\
                 pplx-space-k = { path = \"../../../l10_foundation/k\", features = [\"kv\", \
                 \"json\"] }\n",
            ),
        ),
        (
            "crates/l50_app/p/Cargo.toml",
            manifest(
                "pplx-space-p",
                "[dependencies]\nredis = \"1\"\n\
                 pplx-space-a = { path = \"../../l40_api/a/server\", features = [\"pg\"] }\n",
            ),
        ),
        (
            "crates/l50_app/q/Cargo.toml",
            manifest(
                "pplx-space-q",
                "[dependencies]\npplx-space-h = { path = \"../../l10_foundation/h\" }\n\
                 pplx-space-d = { path = \"../../l20_domain/d\", features = [\"hdb\"] }\n",
            ),
        ),
        (
            "crates/test_support/fixtures/Cargo.toml",
            manifest("pplx-space-fixtures", "[dependencies]\nsqlx = \"1\"\n"),
        ),
        (
            "crates/tools/bench/Cargo.toml",
            manifest(
                "pplx-space-bench",
                "[dependencies]\nredis = \"1\"\n\
                 pplx-space-h = { path = \"../../l10_foundation/h\" }\n",
            ),
        ),
    ];
    all.extend(files);
    Layered {
        extra: &STORAGE_EXTRA,
        tables,
        files: all,
        ..Layered::default()
    }
    .build()
}

/// A baseline of [`layers_baseline`] without edges, plus the storage
/// checks' tables with these `(from, to)` entries, each package's on one
/// line.
fn storage_baseline(access: &[(&str, &str)], codecs: &[(&str, &str)]) -> String {
    let table = |name: &str, edges: &[(&str, &str)]| {
        let mut grouped: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for (from, to) in edges {
            grouped.entry(from).or_default().push(format!("{to:?}"));
        }
        let mut text = format!("[layers.{name}]\n");
        for (from, to) in grouped {
            text.push_str(&format!("{from:?} = [{}]\n", to.join(", ")));
        }
        text
    };
    layers_baseline(&[], &[]).replace(
        "[crate_names]\n",
        &format!(
            "{}{}[crate_names]\n",
            table("storage_access", access),
            table("codec_features", codecs)
        ),
    )
}

/// The `storage_access` and `codec_features` findings, as `(code, message,
/// location)`.
fn storage_findings(ctx: &Context) -> Vec<(String, String, String)> {
    layer_findings(ctx)
        .into_iter()
        .filter(|(code, _, _)| {
            code.starts_with("layers.storage_access.") || code.starts_with("layers.codec_features.")
        })
        .collect()
}

/// Only stores and storage helpers depend on a driver or helper, or turn on
/// a driver feature; each finding is on the manifest line that declares it,
/// says what it reaches and how to fix it.
#[test]
fn layers_storage_checks() {
    let ws = storage_workspace(STORAGE_TABLES, Vec::new());
    let ctx = load(ws.path(), None);
    let a = "crates/l40_api/a/server/Cargo.toml";
    let manifest = |path: &str| fs::read_to_string(ws.path().join(path)).unwrap();
    let at = |path: &str, needle: &str| format!("{path}:{}", line_with(&manifest(path), needle));
    let d = "crates/l20_domain/d/Cargo.toml";
    let b = "crates/l40_api/b/client/Cargo.toml";
    let p = "crates/l50_app/p/Cargo.toml";
    let q = "crates/l50_app/q/Cargo.toml";
    let legacy = "crates/libs/legacy/Cargo.toml";
    let expected = [
        (
            "layers.storage_access.no_crossing",
            format!(
                "{a}: pplx-space-a (l40_api) depends on pplx-space-conn, a storage helper \
                 ([rules.layers.storage] helpers lists it), a normal dependency, but only \
                 l30_storage crates and storage helpers may"
            ),
            at(a, "pplx-space-conn ="),
        ),
        (
            "layers.storage_access.no_crossing",
            format!(
                "{a}: pplx-space-a (l40_api) depends on pplx-space-f, a storage helper (it has a \
                 required dependency on sqlx), a normal dependency"
            ),
            at(a, "pplx-space-f ="),
        ),
        (
            "layers.storage_access.no_crossing",
            format!(
                "{a}: pplx-space-a (l40_api) depends on sqlx, a driver ([rules.layers.storage] \
                 drivers lists it), a normal dependency, but only l30_storage crates and storage \
                 helpers may: nothing above l30_storage reaches a database or object storage \
                 directly, l20_domain never does, and l10_foundation does only in a helper that \
                 names nothing SPACE-specific. Fix one of: move the code that uses it \
                 (sqlx::PgPool) into a store in l30_storage that lists pplx-space-a in \
                 [rules.layers] store_users, and call the store; or, for a generic mechanism \
                 that names nothing SPACE-specific, into a storage helper in l10_foundation that \
                 such a store calls. Moving it into a new storage helper does not let \
                 pplx-space-a depend on that helper: layers.storage_access checks that edge too. \
                 See crates/tools/archcheck/README.md#layers."
            ),
            at(a, "sqlx = \"1\""),
        ),
        // A client never depends on a store, so it is not told to use one.
        (
            "layers.storage_access.no_crossing",
            format!(
                "{b}: pplx-space-b-client (l40_api) depends on pplx-space-f, a storage helper (it \
                 has a required dependency on sqlx), a normal dependency, but only l30_storage \
                 crates and storage helpers may: nothing above l30_storage reaches a database or \
                 object storage directly, l20_domain never does, and l10_foundation does only in \
                 a helper that names nothing SPACE-specific. Fix: a client reaches state only \
                 through its server's protocol, and never depends on a store: move the code that \
                 uses it into the server, behind a store, and add a call for it to the protocol. \
                 Moving it into a new storage helper does not let pplx-space-b-client depend on \
                 that helper: layers.storage_access checks that edge too. See \
                 crates/tools/archcheck/README.md#layers."
            ),
            at(b, "pplx-space-f ="),
        ),
        (
            "layers.storage_access.no_crossing",
            format!(
                "{d}: pplx-space-d (l20_domain) depends on sqlx, a driver ([rules.layers.storage] \
                 drivers lists it), a normal dependency (optional, so a feature turns it on), but \
                 only l30_storage crates and storage helpers may: nothing above l30_storage \
                 reaches a database or object storage directly, l20_domain never does, and \
                 l10_foundation does only in a helper that names nothing SPACE-specific. Fix: \
                 move the code that uses it, such as a codec's impls, into the store that \
                 persists these types"
            ),
            at(d, "sqlx = {"),
        ),
        (
            "layers.storage_access.no_crossing",
            format!(
                "{legacy}: pplx-space-legacy (l10_foundation) depends on sqlx, a driver \
                 ([rules.layers.storage] drivers lists it), a normal dependency, but only \
                 l30_storage crates and storage helpers may: nothing above l30_storage reaches \
                 a database or object storage directly, l20_domain never does, and \
                 l10_foundation does only in a helper that names nothing SPACE-specific. Fix: \
                 move the code that uses it into a store or a storage helper, as splitting \
                 pplx-space-legacy up would: [rules.layers] legacy lists it, so it is \
                 no storage helper itself. See crates/tools/archcheck/README.md#layers."
            ),
            at(legacy, "sqlx = \"1\""),
        ),
        // Nor does an app, which goes through an API crate.
        (
            "layers.storage_access.no_crossing",
            format!(
                "{p}: pplx-space-p (l50_app) depends on redis, a driver ([rules.layers.storage] \
                 drivers lists it), a normal dependency, but only l30_storage crates and storage \
                 helpers may: nothing above l30_storage reaches a database or object storage \
                 directly, l20_domain never does, and l10_foundation does only in a helper that \
                 names nothing SPACE-specific. Fix: an app reaches state only through the \
                 l40_api crate that owns it: move the code that uses it into that crate, or a \
                 store it uses, and call the API crate. Moving it into a new storage helper does \
                 not let pplx-space-p depend on that helper: layers.storage_access checks that \
                 edge too. See crates/tools/archcheck/README.md#layers."
            ),
            at(p, "redis = \"1\""),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{a}: pplx-space-a (l40_api) enables the `sqlx` feature of pplx-space-g, by \
                 forwarding \"pplx-space-g?/sqlx\" from its own feature `pg`, which turns on its \
                 optional dependency on sqlx, but only l30_storage crates and storage helpers may \
                 turn on a driver feature. Fix: drop the feature, move the code that needs it, \
                 such as a codec or a client, into a store in l30_storage (which may enable the \
                 feature itself) or a storage helper, and reach the data through that. See \
                 crates/tools/archcheck/README.md#layers."
            ),
            at(a, "pg = ["),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{a}: pplx-space-a (l40_api) enables the `codec` feature of pplx-space-legacy, \
                 with features = [\"codec\"] on a normal dependency, which reaches sqlx through \
                 pplx-space-g/sqlx"
            ),
            at(a, "pplx-space-legacy ="),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{b}: pplx-space-b-client (l40_api) enables the `redis` feature of pplx-space-g, \
                 with features = [\"redis\"] on a normal dependency, which turns on its optional \
                 dependency on redis"
            ),
            at(b, "pplx-space-g ="),
        ),
        // `json` is a driver feature only because turning on `kv` turns on
        // the `redis` it requests.
        (
            "layers.codec_features.no_crossing",
            format!(
                "{b}: pplx-space-b-client (l40_api) enables the `json` feature of pplx-space-k, \
                 with features = [\"json\"] on a normal dependency, which reaches redis through \
                 pplx-space-g/redis"
            ),
            at(b, "pplx-space-k ="),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{b}: pplx-space-b-client (l40_api) enables the `kv` feature of pplx-space-k, \
                 with features = [\"kv\"] on a normal dependency, which reaches redis through \
                 pplx-space-g/redis"
            ),
            at(b, "pplx-space-k ="),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{d}: pplx-space-d (l20_domain) enables the `sqlx` feature of pplx-space-g, with \
                 features = [\"sqlx\"]"
            ),
            at(d, "pplx-space-g ="),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{d}: pplx-space-d (l20_domain) enables the `sqlx` feature of pplx-space-h, with \
                 features = [\"sqlx\"] on a normal dependency, which turns on its optional \
                 dependency on sqlx"
            ),
            at(d, "pplx-space-h ="),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{p}: pplx-space-p (l50_app) enables the `pg` feature of pplx-space-a, with \
                 features = [\"pg\"] on a normal dependency, which reaches sqlx through \
                 pplx-space-g/sqlx"
            ),
            at(p, "pplx-space-a ="),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{q}: pplx-space-q (l50_app) enables the `hdb` feature of pplx-space-d, with \
                 features = [\"hdb\"] on a normal dependency, which reaches sqlx through \
                 pplx-space-h/sqlx"
            ),
            at(q, "pplx-space-d ="),
        ),
        (
            "layers.codec_features.no_crossing",
            format!(
                "{q}: pplx-space-q (l50_app) enables the `default` feature of pplx-space-h, \
                 through the default features of a normal dependency, which reaches sqlx through \
                 pplx-space-h/sqlx"
            ),
            at(q, "pplx-space-h ="),
        ),
    ];
    let found = storage_findings(&ctx);
    let expected: Vec<(&str, &str, &str)> = expected
        .iter()
        .map(|(code, message, at)| (*code, message.as_str(), at.as_str()))
        .collect();
    assert_layer_findings("storage", &found, &expected);
    assert!(
        layer_findings(&ctx)
            .iter()
            .all(|(code, _, _)| !code.ends_with("stale_entries")),
        "{:#?}",
        layer_findings(&ctx)
    );

    // Listed, they are tolerated.
    let access = [
        ("pplx-space-a", "pplx-space-conn"),
        ("pplx-space-a", "pplx-space-f"),
        ("pplx-space-a", "sqlx"),
        ("pplx-space-b-client", "pplx-space-f"),
        ("pplx-space-d", "sqlx"),
        ("pplx-space-legacy", "sqlx"),
        ("pplx-space-p", "redis"),
    ];
    let codecs = [
        ("pplx-space-a", "pplx-space-g/sqlx"),
        ("pplx-space-a", "pplx-space-legacy/codec"),
        ("pplx-space-b-client", "pplx-space-g/redis"),
        ("pplx-space-b-client", "pplx-space-k/json"),
        ("pplx-space-b-client", "pplx-space-k/kv"),
        ("pplx-space-d", "pplx-space-g/sqlx"),
        ("pplx-space-d", "pplx-space-h/sqlx"),
        ("pplx-space-p", "pplx-space-a/pg"),
        ("pplx-space-q", "pplx-space-d/hdb"),
        ("pplx-space-q", "pplx-space-h/default"),
    ];
    let ws = storage_workspace(
        STORAGE_TABLES,
        vec![(
            "architecture-baseline.toml",
            storage_baseline(&access, &codecs),
        )],
    );
    let found = storage_findings(&load(ws.path(), None));
    assert!(found.is_empty(), "{found:#?}");

    // Without `default-features` and `hdb`, `q` enables nothing; `d`'s
    // optional dependency on `h` requests no driver feature without
    // `features`, so `hdb` is none. Test support and tools are left out.
    let kept: Vec<(&str, &str)> = codecs
        .iter()
        .copied()
        .filter(|&(from, to)| from != "pplx-space-q" && to != "pplx-space-h/sqlx")
        .collect();
    let ws = storage_workspace(
        STORAGE_TABLES,
        vec![
            (
                "architecture-baseline.toml",
                storage_baseline(&access, &kept),
            ),
            (
                "crates/l50_app/q/Cargo.toml",
                format!(
                    "{}[dependencies]\npplx-space-h = {{ path = \"../../l10_foundation/h\", \
                     default-features = false }}\n\
                     pplx-space-d = {{ path = \"../../l20_domain/d\", features = [\"hdb\"] }}\n",
                    package("pplx-space-q")
                ),
            ),
            (
                "crates/l20_domain/d/Cargo.toml",
                format!(
                    "{}[features]\nsqlx = [\"dep:sqlx\"]\nhdb = [\"dep:pplx-space-h\"]\n\
                     [dependencies]\nsqlx = {{ version = \"1\", optional = true }}\n\
                     pplx-space-g = {{ path = \"../../l10_foundation/g\", features = \
                     [\"sqlx\"] }}\n\
                     pplx-space-h = {{ path = \"../../l10_foundation/h\", optional = true, \
                     default-features = false }}\n",
                    package("pplx-space-d")
                ),
            ),
        ],
    );
    let found = storage_findings(&load(ws.path(), None));
    assert!(found.is_empty(), "{found:#?}");
}

/// An entry that no longer applies is stale, with the reason; so is a
/// `helpers` entry of `[rules.layers.storage]` that does nothing, and a
/// `[rules.layers] legacy` entry for a crate that is gone, but not one
/// outside `l10_foundation`. Neither is for a crate without a layer, which
/// `placement` reports.
#[test]
fn layers_storage_stale_entries() {
    let tables = "[rules.layers.unmoved]\nl10_foundation = [\"pplx-space-legacy\"]\n\
                  [rules.layers.storage]\ndrivers = [\"redis\", \"sqlx\"]\n\
                  helpers = [\n  \"pplx-space-conn\",\n  \"pplx-space-d\",\n  \"pplx-space-f\",\n  \
                  \"pplx-space-gone\",\n  \"pplx-space-legacy\",\n]\n\
                  [rules.layers]\n\
                  legacy = [\n  \"pplx-space-d\",\n  \"pplx-space-legacy\",\n  \"pplx-space-old\",\n]\n";
    let access = [
        ("pplx-space-a", "pplx-space-conn"),
        ("pplx-space-a", "pplx-space-f"),
        ("pplx-space-a", "pplx-space-x"),
        ("pplx-space-a", "redis"),
        ("pplx-space-a", "sqlx"),
        ("pplx-space-b-client", "pplx-space-f"),
        ("pplx-space-d", "sqlx"),
        ("pplx-space-g", "sqlx"),
        ("pplx-space-gone", "sqlx"),
        ("pplx-space-legacy", "sqlx"),
        ("pplx-space-p", "redis"),
        ("pplx-space-s-store", "sqlx"),
    ];
    let codecs = [
        ("pplx-space-a", "nofeature"),
        ("pplx-space-a", "pplx-space-g/sqlx"),
        ("pplx-space-a", "pplx-space-legacy/codec"),
        ("pplx-space-a", "pplx-space-s-store/kubernetes"),
        ("pplx-space-b-client", "pplx-space-g/redis"),
        ("pplx-space-b-client", "pplx-space-k/json"),
        ("pplx-space-b-client", "pplx-space-k/kv"),
        ("pplx-space-d", "pplx-space-g/serde"),
        ("pplx-space-d", "pplx-space-g/sqlx"),
        ("pplx-space-d", "pplx-space-h/sqlx"),
        ("pplx-space-k", "pplx-space-g/redis"),
        ("pplx-space-p", "pplx-space-a/pg"),
        ("pplx-space-p", "pplx-space-g/sqlx"),
        ("pplx-space-q", "pplx-space-d/hdb"),
        ("pplx-space-q", "pplx-space-h/default"),
    ];
    let baseline = storage_baseline(&access, &codecs);
    let ws = storage_workspace(
        tables,
        vec![("architecture-baseline.toml", baseline.clone())],
    );
    let ctx = load(ws.path(), None);
    let config = fs::read_to_string(ws.path().join("ARCHITECTURE.toml")).unwrap();
    let entry = |from: &str, to: &str| {
        let line = baseline
            .lines()
            .position(|line| {
                line.starts_with(&format!("{from:?} = ")) && line.contains(&format!("{to:?}"))
            })
            .expect("the entry is in the baseline");
        format!("architecture-baseline.toml:{}", line + 1)
    };
    let stale = |table: &str, from: &str, to: &str, reason: &str| {
        (
            format!("layers.{table}.stale_entries"),
            format!(
                "{from} -> {to}: is in [layers.{table}] in architecture-baseline.toml, but \
                 {reason}. Remove the entry (cargo arch --write-baseline=layers.{table})."
            ),
            entry(from, to),
        )
    };
    let config_at = |needle: &str| format!("ARCHITECTURE.toml:{}", line_with(&config, needle));
    let expected = vec![
        stale(
            "storage_access",
            "pplx-space-a",
            "pplx-space-x",
            "pplx-space-x is neither a driver in [rules.layers.storage] nor a storage helper",
        ),
        stale(
            "storage_access",
            "pplx-space-a",
            "redis",
            "pplx-space-a has no normal or build dependency on redis",
        ),
        stale(
            "storage_access",
            "pplx-space-g",
            "sqlx",
            "pplx-space-g's dependency on sqlx is optional in an l10_foundation crate, a codec \
             behind a feature, whose enabling layers.codec_features checks",
        ),
        stale(
            "storage_access",
            "pplx-space-gone",
            "sqlx",
            "no workspace member is named pplx-space-gone",
        ),
        stale(
            "storage_access",
            "pplx-space-s-store",
            "sqlx",
            "pplx-space-s-store is a store, which may reach storage",
        ),
        stale(
            "codec_features",
            "pplx-space-a",
            "nofeature",
            "nofeature is not <package>/<feature>",
        ),
        stale(
            "codec_features",
            "pplx-space-a",
            "pplx-space-s-store/kubernetes",
            "pplx-space-s-store is a store, whose features are not checked",
        ),
        stale(
            "codec_features",
            "pplx-space-d",
            "pplx-space-g/serde",
            "`serde` of pplx-space-g does not turn on a driver or storage helper",
        ),
        stale(
            "codec_features",
            "pplx-space-k",
            "pplx-space-g/redis",
            "pplx-space-k enables it only behind a feature of its own in an l10_foundation \
             crate, a codec, whose enabling layers.codec_features checks",
        ),
        stale(
            "codec_features",
            "pplx-space-p",
            "pplx-space-g/sqlx",
            "pplx-space-p does not enable it",
        ),
    ];
    let expected: Vec<(&str, &str, &str)> = expected
        .iter()
        .map(|(code, message, at)| (code.as_str(), message.as_str(), at.as_str()))
        .collect();
    assert_layer_findings("stale", &storage_findings(&ctx), &expected);

    let config_stale: Vec<(String, String, String)> = layer_findings(&ctx)
        .into_iter()
        .filter(|(code, _, _)| code == "layers.stale_entries")
        .collect();
    let helper = |package: &str, reason: &str| {
        (
            format!(
                "{package}: is in [rules.layers.storage] helpers in ARCHITECTURE.toml, but \
                 {reason}. Remove it."
            ),
            config_at(&format!("  \"{package}\",")),
        )
    };
    // The second line naming a package in the config is its `legacy` entry.
    let legacy = |package: &str, reason: &str| {
        let line = config
            .lines()
            .enumerate()
            .filter(|(_, line)| *line == format!("  \"{package}\","))
            .map(|(index, _)| index + 1)
            .last()
            .expect("the package is in legacy");
        (
            format!(
                "{package}: is in [rules.layers] legacy in ARCHITECTURE.toml, but {reason}. \
                 Remove it."
            ),
            format!("ARCHITECTURE.toml:{line}"),
        )
    };
    // `legacy` is [rules.layers]'s own, whose entries are checked first.
    let expected = [
        legacy("pplx-space-old", "no workspace member has this name"),
        helper(
            "pplx-space-d",
            "a storage helper is an l10_foundation crate, and its layer is l20_domain",
        ),
        helper(
            "pplx-space-f",
            "its required dependency on sqlx already makes it a storage helper",
        ),
        helper("pplx-space-gone", "no workspace member has this name"),
        helper(
            "pplx-space-legacy",
            "[rules.layers] legacy lists it too, which keeps a crate from being a storage helper",
        ),
    ];
    let found: Vec<(&str, &str)> = config_stale
        .iter()
        .map(|(_, message, at)| (message.as_str(), at.as_str()))
        .collect();
    let expected: Vec<(&str, &str)> = expected
        .iter()
        .map(|(message, at)| (message.as_str(), at.as_str()))
        .collect();
    assert_eq!(found, expected);

    // Without its `unmoved` entry the legacy crate has no layer: its
    // `helpers` and `legacy` entries are left to `placement`.
    let tables = "[rules.layers.storage]\ndrivers = [\"sqlx\"]\n\
                  helpers = [\"pplx-space-legacy\"]\n\
                  [rules.layers]\nlegacy = [\"pplx-space-legacy\"]\n";
    let ctx = load(storage_workspace(tables, Vec::new()).path(), None);
    let codes: Vec<String> = layer_findings(&ctx)
        .into_iter()
        .filter(|(code, _, _)| code == "layers.placement" || code == "layers.stale_entries")
        .map(|(code, _, _)| code)
        .collect();
    assert_eq!(codes, ["layers.placement"], "{:#?}", layer_findings(&ctx));
}

/// The storage tables compare with the base branch's: a base whose layers
/// rule lacks the check is seeded, with advisory findings; one with the
/// check but without the table tolerates nothing; and an entry in the base's
/// table is no new one. `--write-baseline` keeps the entries still in
/// violation and lists the others.
#[test]
fn layers_storage_base_comparison_and_write_baseline() {
    let access = [("pplx-space-a", "sqlx"), ("pplx-space-d", "sqlx")];
    let ws = storage_workspace(
        STORAGE_TABLES,
        vec![("architecture-baseline.toml", storage_baseline(&access, &[]))],
    );
    let config = fs::read_to_string(ws.path().join("ARCHITECTURE.toml")).unwrap();
    let without_storage: String = {
        let mut kept = String::new();
        let mut skipping = false;
        for line in config.lines() {
            if line.starts_with('[') {
                skipping = line.starts_with("[rules.layers.storage")
                    || line.starts_with("[rules.layers.codec_features");
            }
            if !skipping {
                kept.push_str(line);
                kept.push('\n');
            }
        }
        kept
    };
    let base = layers_baseline(&[], &[]);
    let new_entries = |architecture: &str, base: &str| {
        let ctx = load_with(ws.path(), Some(base), Some(architecture));
        layer_findings(&ctx)
            .into_iter()
            .filter(|(code, _, _)| code == "layers.storage_access.no_new_entries")
            .map(|(_, message, _)| message)
            .zip(
                check(&ctx)
                    .into_iter()
                    .filter(|finding| finding.code() == "layers.storage_access.no_new_entries")
                    .map(|finding| finding.mode.is_required()),
            )
            .collect::<Vec<_>>()
    };

    let seeded = new_entries(&without_storage, &base);
    assert_eq!(seeded.len(), 2, "{seeded:#?}");
    assert_eq!(
        seeded[0].0,
        "pplx-space-a -> sqlx: is in [layers.storage_access] in architecture-baseline.toml, and \
         the base branch's layers rule has no storage_access check yet, so this pull request \
         seeds the table. Review each entry."
    );
    assert!(seeded.iter().all(|(_, required)| !required));

    let compared = new_entries(&config, &base);
    assert_eq!(compared.len(), 2, "{compared:#?}");
    assert!(
        compared[0].0.starts_with(
            "pplx-space-a -> sqlx: is in [layers.storage_access] in architecture-baseline.toml \
             but not in the base branch's copy"
        ),
        "{compared:#?}"
    );
    assert!(compared.iter().all(|(_, required)| *required));

    let listed = new_entries(&config, &storage_baseline(&access[..1], &[]));
    assert_eq!(listed.len(), 1, "{listed:#?}");
    assert!(
        listed[0].0.starts_with("pplx-space-d -> sqlx: "),
        "{listed:#?}"
    );

    let write = run(ws.path(), &["--write-baseline=layers.storage_access"]);
    assert_eq!(write.code, 0, "{}", write.stderr);
    let not_added: Vec<&str> = write
        .stdout
        .lines()
        .filter(|line| line.starts_with("not added: "))
        .collect();
    let line = |edge: &str| {
        format!(
            "not added: {edge} in [layers.storage_access]; fix the dependency (cargo arch says \
             how): an entry added by hand fails layers.storage_access.no_new_entries, and a \
             crate that has to reach storage for good becomes a store or a storage helper; \
             listing one in [rules.layers.storage] helpers is a policy change, which \
             layers.policy_changed reports, and a helper derived from a required driver \
             dependency is reviewed in the manifest diff"
        )
    };
    assert_eq!(
        not_added,
        [
            line("pplx-space-a (l40_api) -> pplx-space-conn"),
            line("pplx-space-a (l40_api) -> pplx-space-f"),
            line("pplx-space-b-client (l40_api) -> pplx-space-f"),
            line("pplx-space-legacy (l10_foundation) -> sqlx"),
            line("pplx-space-p (l50_app) -> redis"),
        ],
        "{}",
        write.stdout
    );
    let text = fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap();
    assert_eq!(
        generated_table(&text, "layers.storage_access"),
        "[layers.storage_access]\npplx-space-a = [\n  \"sqlx\",\n]\npplx-space-d = [\n  \
         \"sqlx\",\n]\n",
        "{text}"
    );
    assert_eq!(
        generated_table(&text, "layers.codec_features"),
        "[layers.codec_features]\n",
        "{text}"
    );
}

/// `[rules.layers.storage]` needs at least one driver, and lists each
/// package once.
#[test]
fn layers_storage_configuration_errors() {
    for (storage, error) in [
        (
            "[rules.layers.storage]\ndrivers = []\n",
            "drivers must list at least one package",
        ),
        (
            "[rules.layers.storage]\ndrivers = [\"sqlx\", \"sqlx\"]\n",
            "\"sqlx\" is listed twice",
        ),
        (
            "[rules.layers.storage]\ndrivers = [\"sqlx\"]\nhelpers = [\"x\", \"x\"]\n",
            "\"x\" is listed twice",
        ),
        (
            "[rules.layers.storage]\ndrivers = [\"sqlx\"]\nowners = []\n",
            "unknown field `owners`",
        ),
    ] {
        let ws = Layered {
            files: vec![("ARCHITECTURE.toml", layers_config(storage, "required"))],
            ..Layered::default()
        }
        .build();
        let checked = run(ws.path(), &[]);
        assert_eq!(checked.code, 2, "{storage}: {}", checked.stdout);
        assert!(
            checked.stderr.contains(error),
            "{storage}: {}",
            checked.stderr
        );
    }
    // Without the table, the layers rule's configuration is incomplete.
    let ws = Layered {
        files: vec![(
            "ARCHITECTURE.toml",
            layers_config("", "required").replace(STORAGE, ""),
        )],
        ..Layered::default()
    }
    .build();
    let checked = run(ws.path(), &[]);
    assert_eq!(checked.code, 2, "{}", checked.stdout);
    assert!(
        checked.stderr.contains("missing field `storage`"),
        "{}",
        checked.stderr
    );
}

const DOMAIN_EXTRA: [(&str, &str); 4] = [
    ("crates/l20_domain/m", "pplx-space-m"),
    ("crates/l20_domain/w/protocol", "pplx-space-w-protocol"),
    ("crates/l20_domain/wire", "pplx-space-wire"),
    ("crates/libs/legacy", "pplx-space-legacy"),
];

/// `[rules.layers]` tables for [`domain_workspace`].
const DOMAIN_TABLES: &str = "[rules.layers]\nlegacy = [\"pplx-space-legacy\"]\n\
                             protocols = [\"pplx-space-wire\"]\n\
                             [rules.layers.unmoved]\nl20_domain = [\"pplx-space-legacy\"]\n\
                             [rules.layers.storage]\ndrivers = [\"redis\", \"sqlx\"]\n\
                             [rules.layers.domain_fences]\nruntime = [\"tokio\"]\n\
                             http = [\"hyper\", \"reqwest\"]\nkube = [\"kube\"]\n";

/// A layered workspace whose Domain manifests reach fenced packages and
/// protocols in every way the Domain checks tell apart, with `tables` as
/// `[rules.layers]`'s and `files` written over the rest:
///
/// - `d`, a model, depends on `tokio`, on `hyper` (optional) and `reqwest`
///   (a build-dependency), and on `sqlx`, a driver, and `aws-lc-rs`, which
///   no family lists; and on the protocols `w-protocol`, by its name, and
///   `wire` (a build-dependency), which `protocols` lists;
/// - `m`, a model, dev-depends on `tokio` and `w-protocol`, and depends on
///   the model `d`;
/// - `w-protocol` depends on `tokio` for one target, and on `d` and `wire`;
/// - the legacy crate, a model, depends on `kube` (optional) and `wire`;
/// - `a` (API) depends on `tokio` and `w-protocol`.
fn domain_workspace(tables: &str, files: Vec<(&str, String)>) -> TempDir {
    let manifest = |name: &str, body: &str| format!("{}{body}", package(name));
    let mut all: Vec<(&str, String)> = vec![
        ("ARCHITECTURE.toml", layers_config(tables, "required")),
        (
            "crates/l20_domain/d/Cargo.toml",
            manifest(
                "pplx-space-d",
                "[dependencies]\ntokio = \"1\"\nhyper = { version = \"1\", optional = true }\n\
                 sqlx = \"1\"\naws-lc-rs = \"1\"\n\
                 pplx-space-w-protocol = { path = \"../w/protocol\" }\n\
                 [build-dependencies]\nreqwest = \"1\"\n\
                 pplx-space-wire = { path = \"../wire\" }\n",
            ),
        ),
        (
            "crates/l20_domain/d/src/lib.rs",
            "use tokio::sync::Mutex;\nuse pplx_space_w_protocol::v1::Thing;\n".to_owned(),
        ),
        (
            "crates/l20_domain/m/Cargo.toml",
            manifest(
                "pplx-space-m",
                "[dependencies]\npplx-space-d = { path = \"../d\" }\n\
                 [dev-dependencies]\ntokio = \"1\"\n\
                 pplx-space-w-protocol = { path = \"../w/protocol\" }\n",
            ),
        ),
        (
            "crates/l20_domain/w/protocol/Cargo.toml",
            manifest(
                "pplx-space-w-protocol",
                "[dependencies]\npplx-space-d = { path = \"../../d\" }\n\
                 pplx-space-wire = { path = \"../../wire\" }\n\
                 [target.'cfg(unix)'.dependencies]\ntokio = \"1\"\n",
            ),
        ),
        (
            "crates/libs/legacy/Cargo.toml",
            manifest(
                "pplx-space-legacy",
                "[dependencies]\nkube = { version = \"1\", optional = true }\n\
                 pplx-space-wire = { path = \"../../l20_domain/wire\" }\n",
            ),
        ),
        (
            "crates/l40_api/a/server/Cargo.toml",
            manifest(
                "pplx-space-a",
                "[dependencies]\ntokio = \"1\"\n\
                 pplx-space-w-protocol = { path = \"../../../l20_domain/w/protocol\" }\n",
            ),
        ),
    ];
    all.extend(files);
    Layered {
        extra: &DOMAIN_EXTRA,
        tables,
        files: all,
        ..Layered::default()
    }
    .build()
}

/// A baseline of [`layers_baseline`] without edges, plus the Domain checks'
/// tables with these `(from, to)` entries, each package's on one line.
fn domain_baseline(dependencies: &[(&str, &str)], model_protocol: &[(&str, &str)]) -> String {
    let table = |name: &str, edges: &[(&str, &str)]| {
        let mut grouped: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for (from, to) in edges {
            grouped.entry(from).or_default().push(format!("{to:?}"));
        }
        let mut text = format!("[layers.{name}]\n");
        for (from, to) in grouped {
            text.push_str(&format!("{from:?} = [{}]\n", to.join(", ")));
        }
        text
    };
    layers_baseline(&[], &[]).replace(
        "[crate_names]\n",
        &format!(
            "{}{}[crate_names]\n",
            table("domain_dependencies", dependencies),
            table("model_protocol", model_protocol)
        ),
    )
}

/// The `domain_dependencies` and `model_protocol` findings, as `(code,
/// message, location)`.
fn domain_findings(ctx: &Context) -> Vec<(String, String, String)> {
    layer_findings(ctx)
        .into_iter()
        .filter(|(code, _, _)| {
            code.starts_with("layers.domain_dependencies.")
                || code.starts_with("layers.model_protocol.")
        })
        .collect()
}

/// The edges [`domain_workspace`] has against the Domain checks.
const DOMAIN_DEPENDENCIES: [(&str, &str); 5] = [
    ("pplx-space-d", "hyper"),
    ("pplx-space-d", "reqwest"),
    ("pplx-space-d", "tokio"),
    ("pplx-space-legacy", "kube"),
    ("pplx-space-w-protocol", "tokio"),
];
const MODEL_PROTOCOL: [(&str, &str); 3] = [
    ("pplx-space-d", "pplx-space-w-protocol"),
    ("pplx-space-d", "pplx-space-wire"),
    ("pplx-space-legacy", "pplx-space-wire"),
];

/// No Domain crate has a normal or build dependency on a fenced package,
/// and no model one on a protocol; each finding is on the manifest line
/// that declares it, says why and how to fix it. Drivers are left to
/// storage_access, legacy crates are checked, and dev-dependencies and
/// crates outside Domain are not.
#[test]
fn layers_domain_checks() {
    let ws = domain_workspace(DOMAIN_TABLES, Vec::new());
    let ctx = load(ws.path(), None);
    let manifest = |path: &str| fs::read_to_string(ws.path().join(path)).unwrap();
    let at = |path: &str, needle: &str| format!("{path}:{}", line_with(&manifest(path), needle));
    let d = "crates/l20_domain/d/Cargo.toml";
    let legacy = "crates/libs/legacy/Cargo.toml";
    let w = "crates/l20_domain/w/protocol/Cargo.toml";
    let expected = [
        (
            "layers.domain_dependencies.no_crossing",
            format!(
                "{d}: pplx-space-d (l20_domain) depends on hyper, which \
                 [rules.layers.domain_fences] http lists, a normal dependency (optional, so a \
                 feature turns it on), but l20_domain crates do no I/O"
            ),
            at(d, "hyper ="),
        ),
        (
            "layers.domain_dependencies.no_crossing",
            format!(
                "{d}: pplx-space-d (l20_domain) depends on reqwest, which \
                 [rules.layers.domain_fences] http lists, a build-dependency, but"
            ),
            at(d, "reqwest ="),
        ),
        (
            "layers.domain_dependencies.no_crossing",
            format!(
                "{d}: pplx-space-d (l20_domain) depends on tokio, which \
                 [rules.layers.domain_fences] runtime lists, a normal dependency, but l20_domain \
                 crates do no I/O, and domain_fences lists the packages that do it, such as async \
                 runtimes, HTTP clients, cloud SDKs and Kubernetes clients. Fix one of: move the \
                 code that uses it (tokio::sync::Mutex) into the l40_api crate that does the \
                 I/O, or into an l10_foundation mechanism that names nothing SPACE-specific, and \
                 have pplx-space-d take plain values or define a trait for it; or, if \
                 pplx-space-d is not Domain code, move it to its layer. See \
                 crates/tools/archcheck/README.md#layers."
            ),
            at(d, "tokio ="),
        ),
        (
            "layers.domain_dependencies.no_crossing",
            format!(
                "{legacy}: pplx-space-legacy (l20_domain) depends on kube, which \
                 [rules.layers.domain_fences] kube lists, a normal dependency (optional"
            ),
            at(legacy, "kube ="),
        ),
        (
            "layers.domain_dependencies.no_crossing",
            format!(
                "{w}: pplx-space-w-protocol (l20_domain) depends on tokio, which \
                 [rules.layers.domain_fences] runtime lists, a normal dependency, for target \
                 cfg(unix), but"
            ),
            at(w, "tokio ="),
        ),
        (
            "layers.model_protocol.no_crossing",
            format!(
                "{d}: pplx-space-d (a model in l20_domain) depends on pplx-space-w-protocol (a \
                 protocol: its name ends in -protocol), a normal dependency, but a model never \
                 depends on a protocol: protocols hold wire schemas and convert between them and \
                 the models, so the edge goes the other way. Fix one of: move what pplx-space-d \
                 uses (pplx_space_w_protocol::v1::Thing) into a model, and have \
                 pplx-space-w-protocol convert to it; or, if pplx-space-d holds wire code too, \
                 split that into a protocol crate (named *-protocol), which may depend on both. \
                 See crates/tools/archcheck/README.md#layers."
            ),
            at(d, "pplx-space-w-protocol ="),
        ),
        (
            "layers.model_protocol.no_crossing",
            format!(
                "{d}: pplx-space-d (a model in l20_domain) depends on pplx-space-wire (a \
                 protocol: [rules.layers] protocols lists it), a build-dependency, but"
            ),
            at(d, "pplx-space-wire ="),
        ),
        (
            "layers.model_protocol.no_crossing",
            format!(
                "{legacy}: pplx-space-legacy (a model in l20_domain) depends on pplx-space-wire"
            ),
            at(legacy, "pplx-space-wire ="),
        ),
    ];
    let expected: Vec<(&str, &str, &str)> = expected
        .iter()
        .map(|(code, message, at)| (*code, message.as_str(), at.as_str()))
        .collect();
    assert_layer_findings("domain", &domain_findings(&ctx), &expected);
    // The driver is storage_access's, not domain_dependencies'.
    assert!(
        layer_findings(&ctx).iter().any(|(code, message, _)| {
            code == "layers.storage_access.no_crossing" && message.contains("depends on sqlx")
        }),
        "{:#?}",
        layer_findings(&ctx)
    );

    // Listed, they are tolerated.
    let ws = domain_workspace(
        DOMAIN_TABLES,
        vec![(
            "architecture-baseline.toml",
            domain_baseline(&DOMAIN_DEPENDENCIES, &MODEL_PROTOCOL),
        )],
    );
    let found = domain_findings(&load(ws.path(), None));
    assert!(found.is_empty(), "{found:#?}");

    // Without its `protocols` entry, `wire` is a model, and depending on it
    // is no finding.
    let ws = domain_workspace(
        &DOMAIN_TABLES.replace("[\"pplx-space-wire\"]", "[]"),
        vec![(
            "architecture-baseline.toml",
            domain_baseline(&DOMAIN_DEPENDENCIES, &[]),
        )],
    );
    let found: Vec<String> = domain_findings(&load(ws.path(), None))
        .into_iter()
        .map(|(code, _, _)| code)
        .collect();
    assert_eq!(found, ["layers.model_protocol.no_crossing"], "{found:#?}");
}

/// A crate whose name ends in `-proto` is a protocol.
#[test]
fn layers_domain_proto_names() {
    let extra = [
        ("crates/l20_domain/m", "pplx-space-m"),
        ("crates/l20_domain/x/proto", "pplx-space-x-proto"),
    ];
    let dependencies = [
        ("pplx-space-m", "dependencies", "pplx-space-x-proto"),
        ("pplx-space-x-proto", "dependencies", "pplx-space-d"),
    ];
    let ws = Layered {
        extra: &extra,
        dependencies: &dependencies,
        ..Layered::default()
    }
    .build();
    let found = domain_findings(&load(ws.path(), None));
    assert_layer_findings(
        "proto",
        &found,
        &[(
            "layers.model_protocol.no_crossing",
            "crates/l20_domain/m/Cargo.toml: pplx-space-m (a model in l20_domain) depends on \
             pplx-space-x-proto (a protocol: its name ends in -proto), a normal dependency",
            "crates/l20_domain/m/Cargo.toml:6",
        )],
    );
}

/// An entry that no longer applies is stale, with the reason; so is a
/// `protocols` entry that is no Domain crate or a protocol by its name, and
/// a `domain_fences` package that storage_access checks.
#[test]
fn layers_domain_stale_entries() {
    let tables = "[rules.layers]\nlegacy = [\"pplx-space-legacy\"]\n\
                  protocols = [\n  \"pplx-space-a\",\n  \"pplx-space-gone\",\n  \
                  \"pplx-space-w-protocol\",\n  \"pplx-space-wire\",\n]\n\
                  [rules.layers.unmoved]\nl20_domain = [\"pplx-space-legacy\"]\n\
                  [rules.layers.storage]\ndrivers = [\"redis\", \"sqlx\"]\n\
                  [rules.layers.domain_fences]\nruntime = [\"tokio\"]\n\
                  http = [\"hyper\", \"reqwest\"]\nkube = [\"kube\"]\ndb = [\"sqlx\"]\n";
    let dependencies = [
        ("pplx-space-a", "tokio"),
        ("pplx-space-d", "aws-lc-rs"),
        ("pplx-space-d", "sqlx"),
        ("pplx-space-gone", "tokio"),
        ("pplx-space-m", "tokio"),
    ];
    let model_protocol = [
        ("pplx-space-m", "pplx-space-d"),
        ("pplx-space-m", "pplx-space-gone"),
        ("pplx-space-m", "pplx-space-w-protocol"),
        ("pplx-space-w-protocol", "pplx-space-wire"),
    ];
    let baseline = domain_baseline(&dependencies, &model_protocol);
    let ws = domain_workspace(
        tables,
        vec![("architecture-baseline.toml", baseline.clone())],
    );
    let ctx = load(ws.path(), None);
    let config = fs::read_to_string(ws.path().join("ARCHITECTURE.toml")).unwrap();
    let entry = |from: &str, to: &str| {
        let line = baseline
            .lines()
            .position(|line| {
                line.starts_with(&format!("{from:?} = ")) && line.contains(&format!("{to:?}"))
            })
            .expect("the entry is in the baseline");
        format!("architecture-baseline.toml:{}", line + 1)
    };
    let stale = |table: &str, from: &str, to: &str, reason: &str| {
        (
            format!("layers.{table}.stale_entries"),
            format!(
                "{from} -> {to}: is in [layers.{table}] in architecture-baseline.toml, but \
                 {reason}. Remove the entry (cargo arch --write-baseline=layers.{table})."
            ),
            entry(from, to),
        )
    };
    let expected = vec![
        stale(
            "domain_dependencies",
            "pplx-space-a",
            "tokio",
            "pplx-space-a is not in l20_domain",
        ),
        stale(
            "domain_dependencies",
            "pplx-space-d",
            "aws-lc-rs",
            "aws-lc-rs is not fenced: no [rules.layers.domain_fences] family lists it, and it \
             is no l10_foundation crate with a required dependency on a fenced package",
        ),
        stale(
            "domain_dependencies",
            "pplx-space-d",
            "sqlx",
            "sqlx is a driver or a storage helper, which layers.storage_access checks instead",
        ),
        stale(
            "domain_dependencies",
            "pplx-space-gone",
            "tokio",
            "no workspace member is named pplx-space-gone",
        ),
        stale(
            "domain_dependencies",
            "pplx-space-m",
            "tokio",
            "pplx-space-m has no normal or build dependency on tokio",
        ),
        stale(
            "model_protocol",
            "pplx-space-m",
            "pplx-space-d",
            "pplx-space-d is not a protocol",
        ),
        stale(
            "model_protocol",
            "pplx-space-m",
            "pplx-space-gone",
            "no workspace member is named pplx-space-gone",
        ),
        stale(
            "model_protocol",
            "pplx-space-m",
            "pplx-space-w-protocol",
            "pplx-space-m has no normal or build dependency on pplx-space-w-protocol",
        ),
        stale(
            "model_protocol",
            "pplx-space-w-protocol",
            "pplx-space-wire",
            "pplx-space-w-protocol is a protocol, which may depend on protocols",
        ),
    ];
    let found: Vec<_> = domain_findings(&ctx)
        .into_iter()
        .filter(|(code, _, _)| code.ends_with(".stale_entries"))
        .collect();
    let expected: Vec<(&str, &str, &str)> = expected
        .iter()
        .map(|(code, message, at)| (code.as_str(), message.as_str(), at.as_str()))
        .collect();
    assert_layer_findings("stale", &found, &expected);

    let config_at = |needle: &str| format!("ARCHITECTURE.toml:{}", line_with(&config, needle));
    let protocol = |package: &str, reason: &str| {
        (
            format!(
                "{package}: is in [rules.layers] protocols in ARCHITECTURE.toml, but {reason}. \
                 Remove it."
            ),
            config_at(&format!("  \"{package}\",")),
        )
    };
    let expected = [
        protocol(
            "pplx-space-a",
            "a protocol is an l20_domain crate, and its layer is l40_api",
        ),
        protocol("pplx-space-gone", "no workspace member has this name"),
        protocol(
            "pplx-space-w-protocol",
            "its name ends in -protocol, which makes it a protocol",
        ),
        (
            "sqlx: is in [rules.layers.domain_fences] in ARCHITECTURE.toml, but it is a driver \
             or a storage helper, whose dependents in l20_domain layers.storage_access reports. \
             Remove it."
                .to_owned(),
            config_at("db = "),
        ),
    ];
    let found: Vec<(String, String)> = layer_findings(&ctx)
        .into_iter()
        .filter(|(code, _, _)| code == "layers.stale_entries")
        .map(|(_, message, at)| (message, at))
        .collect();
    assert_eq!(found, expected);
}

/// An l10_foundation crate with a required dependency on a fenced package,
/// legacy or not, is fenced under its family, to a fixpoint, and the
/// finding names the dependency; one with an optional dependency only is
/// not, but can be listed. A listed crate that its dependencies fence is a
/// stale entry; a `protocols` entry for a crate without a layer is not,
/// since placement reports the crate.
#[test]
fn layers_domain_fences_derive_foundation_crates() {
    let extra = [
        ("crates/l10_foundation/o", "pplx-space-o"),
        ("crates/libs/old", "pplx-space-old"),
        ("crates/libs/stray", "pplx-space-stray"),
    ];
    let tables = "[rules.layers]\nlegacy = [\"pplx-space-old\"]\n\
                  protocols = [\"pplx-space-stray\"]\n\
                  [rules.layers.unmoved]\nl10_foundation = [\"pplx-space-old\"]\n\
                  [rules.layers.storage]\ndrivers = [\"sqlx\"]\n\
                  [rules.layers.domain_fences]\nruntime = [\"tokio\"]\n\
                  http = [\"hyper\", \"pplx-space-g\", \"pplx-space-o\", \"reqwest\"]\n";
    let manifest = |name: &str, body: &str| format!("{}{body}", package(name));
    let ws = Layered {
        extra: &extra,
        tables,
        files: vec![
            (
                "crates/l10_foundation/f/Cargo.toml",
                manifest(
                    "pplx-space-f",
                    "[dependencies]\ntokio = \"1\"\nhyper = \"1\"\n",
                ),
            ),
            (
                "crates/l10_foundation/g/Cargo.toml",
                manifest(
                    "pplx-space-g",
                    "[dependencies]\nreqwest = { version = \"1\", optional = true }\n\
                     pplx-space-f = { path = \"../f\" }\n",
                ),
            ),
            (
                "crates/l10_foundation/o/Cargo.toml",
                manifest(
                    "pplx-space-o",
                    "[dependencies]\ntokio = { version = \"1\", optional = true }\n",
                ),
            ),
            (
                "crates/libs/old/Cargo.toml",
                manifest("pplx-space-old", "[dependencies]\ntokio = \"1\"\n"),
            ),
            (
                "crates/l20_domain/d/Cargo.toml",
                manifest(
                    "pplx-space-d",
                    "[dependencies]\npplx-space-f = { path = \"../../l10_foundation/f\" }\n\
                     pplx-space-g = { path = \"../../l10_foundation/g\" }\n\
                     pplx-space-o = { path = \"../../l10_foundation/o\" }\n\
                     pplx-space-old = { path = \"../../libs/old\" }\n",
                ),
            ),
            (
                "crates/l20_domain/d/src/lib.rs",
                "use pplx_space_g::Pool;\n".to_owned(),
            ),
        ],
        ..Layered::default()
    }
    .build();
    let ctx = load(ws.path(), None);
    let d = "crates/l20_domain/d/Cargo.toml";
    let text = fs::read_to_string(ws.path().join(d)).unwrap();
    let at = |needle: &str| format!("{d}:{}", line_with(&text, needle));
    let fenced = |to: &str, why: &str| {
        (
            "layers.domain_dependencies.no_crossing",
            format!(
                "{d}: pplx-space-d (l20_domain) depends on {to}, {why}, a normal dependency, but"
            ),
            at(&format!("{to} =")),
        )
    };
    let expected = [
        fenced(
            "pplx-space-f",
            "an l10_foundation crate whose required dependency on tokio fences it under \
             [rules.layers.domain_fences] runtime",
        ),
        fenced(
            "pplx-space-g",
            "an l10_foundation crate whose required dependency on pplx-space-f fences it under \
             [rules.layers.domain_fences] runtime",
        ),
        fenced(
            "pplx-space-o",
            "which [rules.layers.domain_fences] http lists",
        ),
        fenced(
            "pplx-space-old",
            "an l10_foundation crate whose required dependency on tokio fences it under \
             [rules.layers.domain_fences] runtime",
        ),
    ];
    let expected: Vec<(&str, &str, &str)> = expected
        .iter()
        .map(|(code, message, at)| (*code, message.as_str(), at.as_str()))
        .collect();
    let found = domain_findings(&ctx);
    assert_layer_findings("derived", &found, &expected);
    assert!(found[1].1.contains("(pplx_space_g::Pool)"), "{found:#?}");

    let config = fs::read_to_string(ws.path().join("ARCHITECTURE.toml")).unwrap();
    let stale: Vec<(String, String)> = layer_findings(&ctx)
        .into_iter()
        .filter(|(code, _, _)| code == "layers.stale_entries")
        .map(|(_, message, at)| (message, at))
        .collect();
    assert_eq!(
        stale,
        [(
            "pplx-space-g: is in [rules.layers.domain_fences] in ARCHITECTURE.toml, but its \
             required dependency on pplx-space-f fences it already, under runtime. Remove it."
                .to_owned(),
            format!("ARCHITECTURE.toml:{}", line_with(&config, "http = ")),
        )]
    );
    assert!(
        layer_findings(&ctx).iter().any(|(code, message, _)| {
            code == "layers.placement" && message.contains("pplx-space-stray")
        }),
        "{:#?}",
        layer_findings(&ctx)
    );
}

/// The Domain tables compare with the base branch's: a base whose layers
/// rule lacks the check is seeded, with advisory findings, and one with the
/// check but without the table tolerates nothing. `--write-baseline` keeps
/// the entries still in violation and lists the others.
#[test]
fn layers_domain_base_comparison_and_write_baseline() {
    let listed = [("pplx-space-d", "tokio"), ("pplx-space-m", "tokio")];
    let ws = domain_workspace(
        DOMAIN_TABLES,
        vec![(
            "architecture-baseline.toml",
            domain_baseline(&listed, &MODEL_PROTOCOL[..1]),
        )],
    );
    let config = fs::read_to_string(ws.path().join("ARCHITECTURE.toml")).unwrap();
    let mut without_domain = String::new();
    let mut skipping = false;
    for line in config.lines() {
        if line.starts_with('[') {
            skipping = line.starts_with("[rules.layers.domain_dependencies")
                || line.starts_with("[rules.layers.model_protocol");
        }
        if !skipping {
            without_domain.push_str(line);
            without_domain.push('\n');
        }
    }
    let base = layers_baseline(&[], &[]);
    let new_entries = |architecture: &str| {
        let ctx = load_with(ws.path(), Some(&base), Some(architecture));
        check(&ctx)
            .into_iter()
            .filter(|finding| finding.behaviour == "no_new_entries")
            .map(|finding| (finding.message(), finding.mode.is_required()))
            .collect::<Vec<_>>()
    };

    let seeded = new_entries(&without_domain);
    assert_eq!(seeded.len(), 3, "{seeded:#?}");
    assert_eq!(
        seeded[0].0,
        "pplx-space-d -> tokio: is in [layers.domain_dependencies] in \
         architecture-baseline.toml, and the base branch's layers rule has no \
         domain_dependencies check yet, so this pull request seeds the table. Review each entry."
    );
    assert!(
        seeded[2].0.starts_with(
            "pplx-space-d -> pplx-space-w-protocol: is in [layers.model_protocol] in \
             architecture-baseline.toml, and the base branch's layers rule has no model_protocol \
             check yet"
        ),
        "{seeded:#?}"
    );
    assert!(seeded.iter().all(|(_, required)| !required));

    let compared = new_entries(&config);
    assert_eq!(compared.len(), 3, "{compared:#?}");
    assert!(
        compared[0].0.starts_with(
            "pplx-space-d -> tokio: is in [layers.domain_dependencies] in \
             architecture-baseline.toml but not in the base branch's copy"
        ),
        "{compared:#?}"
    );
    assert!(compared.iter().all(|(_, required)| *required));

    let write = run(ws.path(), &["--write-baseline=layers.domain_dependencies"]);
    assert_eq!(write.code, 0, "{}", write.stderr);
    let not_added: Vec<&str> = write
        .stdout
        .lines()
        .filter(|line| line.starts_with("not added: "))
        .collect();
    assert_eq!(
        not_added,
        [
            "not added: pplx-space-d (l20_domain) -> hyper (http) in \
             [layers.domain_dependencies]; fix the dependency (cargo arch says how): an entry \
             added by hand fails layers.domain_dependencies.no_new_entries, and a Domain crate \
             that needs the package for good is not Domain code and moves to its layer's \
             directory, a move reviewed in the diff",
            "not added: pplx-space-d (l20_domain) -> reqwest (http) in \
             [layers.domain_dependencies]; fix the dependency (cargo arch says how): an entry \
             added by hand fails layers.domain_dependencies.no_new_entries, and a Domain crate \
             that needs the package for good is not Domain code and moves to its layer's \
             directory, a move reviewed in the diff",
            "not added: pplx-space-legacy (l20_domain) -> kube (kube) in \
             [layers.domain_dependencies]; fix the dependency (cargo arch says how): an entry \
             added by hand fails layers.domain_dependencies.no_new_entries, and a Domain crate \
             that needs the package for good is not Domain code and moves to its layer's \
             directory, a move reviewed in the diff",
            "not added: pplx-space-w-protocol (l20_domain) -> tokio (runtime) in \
             [layers.domain_dependencies]; fix the dependency (cargo arch says how): an entry \
             added by hand fails layers.domain_dependencies.no_new_entries, and a Domain crate \
             that needs the package for good is not Domain code and moves to its layer's \
             directory, a move reviewed in the diff",
        ],
        "{}",
        write.stdout
    );
    let text = fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap();
    assert_eq!(
        generated_table(&text, "layers.domain_dependencies"),
        "[layers.domain_dependencies]\npplx-space-d = [\n  \"tokio\",\n]\n",
        "{text}"
    );
    assert_eq!(
        generated_table(&text, "layers.model_protocol"),
        "[layers.model_protocol]\npplx-space-d = [\n  \"pplx-space-w-protocol\",\n]\n",
        "{text}"
    );
}
