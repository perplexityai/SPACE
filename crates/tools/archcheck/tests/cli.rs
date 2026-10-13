//! archcheck over temporary workspaces: the library for counting, the
//! inventory, findings and the baseline; the binary for modes, exit codes,
//! formats and arguments.

use std::{fs, path::Path, process::Command};

use pplx_space_archcheck::{
    BaseBaseline, Baseline, Context, check,
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

/// The summary's line when no base baseline is given.
const SKIPPED: &str = "archcheck: file_size.no_new_entries and file_size.no_raised_caps skipped \
                       (no --base-baseline given)\n";

/// `modes` of no_crossing, no_new_entries, no_raised_caps, listed_no_growth
/// and stale_entries.
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
         [rules.file_size.stale_entries]\nmode = \"{stale_entries}\"\n"
    )
}

fn baseline(entries: &[(&str, usize)]) -> String {
    let mut text = String::from("[file_size]\n");
    for (path, cap) in entries {
        text.push_str(&format!("{path:?} = {cap}\n"));
    }
    text
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
        ("crates/a/Cargo.toml", "[package]\n".to_owned()),
        ("crates/b/Cargo.toml", "[package]\n".to_owned()),
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

/// The workspace's context, with `base` written to `base-baseline.toml` and
/// read as the base branch's baseline.
fn load(root: &Path, base: Option<&str>) -> Context {
    let mut ctx = Context::load(root).expect("the workspace loads");
    if let Some(text) = base {
        let path = root.join("base-baseline.toml");
        fs::write(&path, text).expect("write the base baseline");
        ctx.base_baseline = Baseline::load_base(&path).expect("the base baseline is readable");
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
        ("crates/a/src/inner/Cargo.toml", "[package]\n".to_owned()),
        ("crates/c/Cargo.toml", "[package]\n".to_owned()),
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
            ("crates/s/Cargo.toml", "[package]\n".to_owned()),
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
        let skipped = skipped(&ctx);
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
    for (args, stdout) in [
        (vec![], format!("{summary}{skipped}")),
        (
            vec!["--base-baseline", base.to_str().unwrap()],
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
    let parse = "could not parse";
    let glob = "contains glob characters";
    let unnormalized = "has an empty, `.` or `..` component";
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
            Some(format!("{advisory}[rules.crate_names]\n")),
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
        ("unparseable", CONFIG, text("[rules.file_size\n"), parse),
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
        (
            "glob member",
            MANIFEST,
            members("crates/*"),
            "member crates/* contains glob characters",
        ),
        ("glob ?", MANIFEST, members("crates/b?"), glob),
        ("glob [", MANIFEST, members("crates/[b"), glob),
        ("glob ]", MANIFEST, members("crates/b]"), glob),
        ("glob {", MANIFEST, members("crates/{b"), glob),
        ("glob }", MANIFEST, members("crates/b}"), glob),
        (
            "member with ..",
            MANIFEST,
            members("crates/b/../b"),
            unnormalized,
        ),
        (
            "member with .",
            MANIFEST,
            members("crates/./b"),
            unnormalized,
        ),
        (
            "member with //",
            MANIFEST,
            members("crates//b"),
            unnormalized,
        ),
        (
            "member ending in /",
            MANIFEST,
            members("crates/b/"),
            unnormalized,
        ),
        (
            "absolute member",
            MANIFEST,
            members("/crates/b"),
            unnormalized,
        ),
        ("empty member", MANIFEST, members(""), unnormalized),
        (
            "missing member",
            MANIFEST,
            members("crates/c"),
            "member crates/c has no Cargo.toml",
        ),
        (
            "no members",
            MANIFEST,
            text("[workspace]\n"),
            "has no [workspace] members list",
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
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/a/../a\"]\n",
            "Cargo.toml: workspace member \"crates/a/../a\" has",
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
             \"crates/a/src/shrank.rs\" = 6\n",
            vec![not_added("crates/b/src/new.rs", 12)],
        ),
        (
            &[],
            &[("crates/a/src/ok.rs", LIMIT)],
            "[file_size]\n",
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
        wrote.starts_with("archcheck: wrote 1 file_size entries to "),
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
            let warning = summary.lines().last().unwrap_or_default();
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
        (
            &["--write-baseline=yes"],
            2,
            "archcheck: --write-baseline takes no value",
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
