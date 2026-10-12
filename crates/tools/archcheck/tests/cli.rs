//! archcheck over temporary workspaces: the library for counting, the
//! inventory, findings and the baseline; the binary for modes, exit codes,
//! formats and arguments.

use std::{fs, path::Path, process::Command};

use pplx_space_archcheck::{
    Context, check,
    context::count_code_lines,
    report::{Format, render},
    write_baseline,
};
use serde_json::{Value, json};
use tempfile::TempDir;

const LIMIT: usize = 5;

/// A file of `lines` code lines.
fn code(lines: usize) -> String {
    "let x = 1;\n".repeat(lines)
}

fn config(no_crossing: &str, listed_no_growth: &str, stale_entries: &str) -> String {
    format!(
        "[rules.file_size]\nlimit = {LIMIT}\n\
         [rules.file_size.no_crossing]\nmode = \"{no_crossing}\"\n\
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
        (
            "ARCHITECTURE.toml",
            config("advisory", "advisory", "advisory"),
        ),
        ("architecture-baseline.toml", baseline(&[])),
    ];
    for (path, contents) in defaults.iter().chain(files) {
        let path = dir.path().join(path);
        fs::create_dir_all(path.parent().expect("a parent directory")).expect("create it");
        fs::write(&path, contents).expect("write a workspace file");
    }
    dir
}

/// `(behaviour, message)` of every finding, in reporting order.
fn findings(root: &Path) -> Vec<(&'static str, String)> {
    let ctx = Context::load(root).expect("the workspace loads");
    check(&ctx)
        .into_iter()
        .map(|finding| (finding.behaviour, finding.message()))
        .collect()
}

fn rdjson(root: &Path) -> Value {
    let ctx = Context::load(root).expect("the workspace loads");
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

/// One row per situation: baseline entries, file sizes, and the expected
/// `(behaviour, start of message)` of each finding.
#[test]
fn behaviours() {
    const LISTED: &str = "crates/a/src/listed.rs";
    const NEW: &str = "crates/b/src/new.rs";
    type Sizes<'a> = &'a [(&'a str, usize)];
    type Case<'a> = (&'a str, Sizes<'a>, Sizes<'a>, &'a [(&'a str, &'a str)]);
    let cases: &[Case] = &[
        (
            "a new file over the limit",
            &[],
            &[(NEW, LIMIT + 1)],
            &[(
                "no_crossing",
                "crates/b/src/new.rs: 6 code lines, over the 5-line limit (comments and blank \
                 lines are not counted). Split it into submodules by responsibility.",
            )],
        ),
        ("a new file at the limit", &[], &[(NEW, LIMIT)], &[]),
        (
            "a listed file grew",
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
            &[(LISTED, 6)],
            &[(LISTED, 6)],
            &[],
        ),
        (
            "a listed file shrank",
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
            &[(LISTED, 8)],
            &[(NEW, 8)],
            &[
                ("no_crossing", "crates/b/src/new.rs: 8 code lines, over"),
                ("stale_entries", "crates/a/src/listed.rs: listed in"),
            ],
        ),
        (
            "a key that names no production file",
            &[("./crates/a/src/listed.rs", 8)],
            &[(LISTED, 8)],
            &[
                ("no_crossing", "crates/a/src/listed.rs: 8 code lines, over"),
                ("stale_entries", "./crates/a/src/listed.rs: listed in"),
            ],
        ),
    ];
    for (name, entries, sizes, expected) in cases {
        let mut files: Vec<(&str, String)> = sizes
            .iter()
            .map(|(path, lines)| (*path, code(*lines)))
            .collect();
        files.push(("architecture-baseline.toml", baseline(entries)));
        let ws = workspace(&files);

        let found = findings(ws.path());

        assert_eq!(found.len(), expected.len(), "{name}: {found:#?}");
        for ((behaviour, message), (want_behaviour, want_message)) in found.iter().zip(*expected) {
            assert_eq!(behaviour, want_behaviour, "{name}");
            assert!(message.starts_with(want_message), "{name}: {message}");
        }
    }
}

/// One finding per behaviour, each reported in its own mode; only required
/// findings exit 1.
#[test]
fn modes_and_exit_codes() {
    let files = |modes: [&str; 3]| {
        vec![
            ("ARCHITECTURE.toml", config(modes[0], modes[1], modes[2])),
            (
                "architecture-baseline.toml",
                baseline(&[("crates/a/src/gone.rs", 9), ("crates/a/src/grew.rs", 6)]),
            ),
            ("crates/a/src/grew.rs", code(7)),
            ("crates/b/src/new.rs", code(6)),
        ]
    };
    let cases: &[([&str; 3], i32)] = &[
        (["advisory", "advisory", "advisory"], 0),
        (["required", "advisory", "advisory"], 1),
        (["advisory", "required", "advisory"], 1),
        (["advisory", "advisory", "required"], 1),
    ];
    for (modes, expected) in cases {
        let ws = workspace(&files(*modes));

        let run = run(ws.path(), &[]);

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
                format!("file_size.listed_no_growth ({}):", modes[1]),
                format!("file_size.stale_entries ({}):", modes[2]),
            ],
        );
    }

    let ws = workspace(&files(["advisory", "required", "advisory"]));
    let growth = "crates/a/src/grew.rs grew from 6 to 7 code lines. Split the file before adding \
                  to it.";
    assert!(
        findings(ws.path())
            .iter()
            .any(|(_, message)| message == growth)
    );

    let clean = workspace(&[(
        "ARCHITECTURE.toml",
        config("required", "required", "required"),
    )]);
    let run = run(clean.path(), &[]);
    assert_eq!(run.code, 0, "{}", run.stderr);
    assert_eq!(
        run.stdout,
        "archcheck: no findings (0 production Rust files checked)\n"
    );
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
    );
}

/// Limits are written with digit grouping; counts are not.
#[test]
fn messages_group_the_limit() {
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            config("advisory", "advisory", "advisory").replace("limit = 5", "limit = 1000"),
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
    let advisory = config("advisory", "advisory", "advisory");
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

/// The baseline is rewritten from the tree, whatever the old file held.
#[test]
fn write_baseline_output() {
    let cases: &[(&[(&str, usize)], &str)] = &[
        (
            &[
                ("crates/b/src/lib.rs", LIMIT + 3),
                ("crates/a/src/z.rs", LIMIT + 1),
                ("crates/a/src/a/b.rs", LIMIT + 2),
                ("crates/a/src/ok.rs", LIMIT),
                ("crates/a/src/tests.rs", LIMIT + 9),
            ],
            "[file_size]\n\"crates/a/src/a/b.rs\" = 7\n\"crates/a/src/z.rs\" = 6\n\
             \"crates/b/src/lib.rs\" = 8\n",
        ),
        (&[("crates/a/src/ok.rs", LIMIT)], "[file_size]\n"),
    ];
    for (sizes, expected) in cases {
        let mut files: Vec<(&str, String)> = sizes
            .iter()
            .map(|(path, lines)| (*path, code(*lines)))
            .collect();
        files.push(("architecture-baseline.toml", "not toml".to_owned()));
        let ws = workspace(&files);

        write_baseline(ws.path()).unwrap();

        let written = fs::read_to_string(ws.path().join("architecture-baseline.toml")).unwrap();
        assert!(written.starts_with("# Existing violations"), "{written}");
        let body: String = written
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
            .map(|line| format!("{line}\n"))
            .collect();
        assert_eq!(body, *expected);
        assert_eq!(findings(ws.path()), []);
    }

    let ws = workspace(&[("crates/a/src/big.rs", code(LIMIT + 1))]);
    let run = run(ws.path(), &["--write-baseline"]);
    assert_eq!(run.code, 0, "{}", run.stderr);
    assert!(
        run.stdout
            .starts_with("archcheck: wrote 1 file_size entries to "),
        "{}",
        run.stdout
    );
}

/// A finding is an `ERROR` in required mode and a `WARNING` in advisory
/// mode, on line 1 of its file; a stale entry whose file is gone points at
/// the entry's line in the baseline.
#[test]
fn rdjson_document() {
    let ws = workspace(&[
        (
            "ARCHITECTURE.toml",
            config("required", "advisory", "advisory"),
        ),
        (
            "architecture-baseline.toml",
            "# A comment.\n\n[file_size]\n\"crates/a/src/grew.rs\" = 6\n\n\
             'crates/a/src/gone.rs' = 9\n"
                .to_owned(),
        ),
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

    assert_eq!(
        rdjson(ws.path()),
        json!({
            "source": {"name": "archcheck"},
            "diagnostics": [
                diagnostic(
                    "crates/b/src/new.rs: 6 code lines, over the 5-line limit (comments and blank \
                     lines are not counted). Split it into submodules by responsibility.",
                    "crates/b/src/new.rs", 1, "ERROR", "no_crossing",
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

    let document = rdjson(ws.path());

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
            config("required", "advisory", "advisory"),
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
                    && run.stdout.ends_with("tree\n"),
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
