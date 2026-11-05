"""move_crates.py moves a workspace's crates into their layer directories.

Each test builds a small Cargo workspace in a temporary git repository: two
layers, a family of crates split across them where one would nest inside
another, a renamed library crate, a legacy crate that stays, and the kinds of
reference the move rewrites. `cargo check` on the moved workspace is the
oracle for the relative paths.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
import tomllib
from collections.abc import Callable
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

from required_test_deps import unavailable_dependency  # noqa: E402

TOOL = SCRIPTS_DIR / "move_crates.py"


def manifest(name: str, deps: str = "", build: bool = False) -> str:
    text = (
        f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"\n'
        f'description = "The {name} crate of a synthetic workspace."\n'
        'license = "Apache-2.0"\npublish = false\n'
    )
    if build:
        text += 'build = "build.rs"\n'
    return text + f"\n[lib]\ndoctest = false\n\n[dependencies]\n{deps}"


def library(what: str, body: str = "") -> str:
    """A library's source, long and distinct enough for git to pair it with
    itself once moved."""
    return (
        f"//! {what}.\n//!\n//! Part of a synthetic workspace that tests the crate move;\n"
        f"//! nothing here runs.\n\npub const ABOUT: &str = \"{what}\";\n{body}"
    )


FILES = {
    "Cargo.toml": """\
        [workspace]
        resolver = "2"
        members = [
            "crates/libs/model/widget",
            "crates/libs/gadget/server",
            "crates/libs/gadget/client",
            "crates/libs/gadget/protocol",
            "crates/libs/gadgetsvc",
            "crates/libs/legacy",
        ]

        [workspace.dependencies]
        pplx-space-widget = { path = "crates/libs/model/widget" }
        pplx-space-gadgetsvc = { path = "crates/libs/gadgetsvc" }
        """,
    "ARCHITECTURE.toml": """\
        [rules.pattern_ratchet.gadgets]
        patterns = ['\\bpplx_space_gadgetsvc\\b']
        allow = ["crates/libs/gadget/**", "crates/l*/widget/**", "crates/libs/model/widget/**"]

        [rules.layers.unmoved]
        l20_domain = [
          "pplx-space-gadget-protocol",
          "pplx-space-legacy",
          "pplx-space-widget",
        ]
        l40_api = [
          "pplx-space-gadget",
          "pplx-space-gadget-client",
          "pplx-space-gadgetsvc",
        ]

        [rules.layers]
        legacy = ["pplx-space-legacy"]
        """,
    # Archcheck's order, each comment over the entries after it.
    "architecture-baseline.toml": """\
        [file_size]
        # the gadget services
        "crates/libs/gadget/client/src/lib.rs" = 1300
        "crates/libs/gadget/server/src/lib.rs" = 1100
        # the widget
        "crates/libs/model/widget/src/lib.rs" = 1200
        """,
    "proto/widget.txt": "widget-proto\n",
    "data/blob.txt": "blob\n",
    "crates/libs/model/widget/Cargo.toml": manifest("pplx-space-widget", build=True),
    "crates/libs/model/widget/build.rs": """\
        fn main() {
            let dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            let proto = std::path::Path::new(&dir).join("../../../../proto/widget.txt");
            let text = std::fs::read_to_string(&proto).expect("proto root");
            println!("cargo:rustc-env=WIDGET_PROTO={}", text.trim());
        }
        """,
    "crates/libs/model/widget/src/lib.rs": library(
        "The widget model",
        'pub const PROTO: &str = env!("WIDGET_PROTO");\n'
        'pub const BLOB: &str = include_str!("../../../../../data/blob.txt");\n'
        "pub fn blob_path() -> String {\n"
        '    format!("{}/../../../../data/blob.txt", env!("CARGO_MANIFEST_DIR"))\n'
        "}\n",
    ),
    "crates/libs/model/widget/scripts/blob.sh": """\
        #!/bin/sh
        # Prints the shared blob, found from this script's own directory, so it
        # works from any working directory. A synthetic script of a synthetic
        # workspace that tests the crate move.
        set -eu
        cat "$(dirname "$0")/../../../../../data/blob.txt"
        """,
    "crates/libs/gadgetsvc/Cargo.toml": manifest("pplx-space-gadgetsvc"),
    "crates/libs/gadgetsvc/src/lib.rs": library("The gadget service's name", 'pub const NAME: &str = "gadget";\n'),
    "crates/libs/gadget/server/Cargo.toml": manifest(
        "pplx-space-gadget",
        'pplx-space-widget = { path = "../../model/widget" }\n'
        "pplx-space-gadgetsvc = { workspace = true }\n",
    ),
    "crates/libs/gadget/server/src/lib.rs": library(
        "The gadget server",
        "pub fn describe() -> String {\n"
        '    format!("{} {}", pplx_space_gadgetsvc::NAME, pplx_space_widget::BLOB)\n'
        "}\n",
    ),
    "crates/libs/gadget/client/Cargo.toml": manifest("pplx-space-gadget-client"),
    "crates/libs/gadget/client/src/lib.rs": library("The gadget client"),
    "crates/libs/gadget/protocol/Cargo.toml": manifest("pplx-space-gadget-protocol"),
    "crates/libs/gadget/protocol/src/lib.rs": library("The gadget wire protocol"),
    "crates/libs/gadget/AGENTS.md": "# The gadget crates\n",
    "crates/libs/legacy/Cargo.toml": manifest(
        "pplx-space-legacy", 'pplx-space-widget = { path = "../model/widget" }\n'
    ),
    "crates/libs/legacy/src/lib.rs": library("A legacy crate", "pub use pplx_space_widget::PROTO;\n"),
    "ci/paths.txt": "crates/libs/gadget/**\ncrates/libs/legacy/**\n",
    "docs/notes.md": """\
        The widget lives in `crates/libs/model/widget/src/lib.rs`, the gadget
        family in `crates/libs/gadget` ([its server](../crates/libs/gadget/server/src/lib.rs)),
        and `pplx-space-gadgetsvc` builds the gadget's name. Libraries are in
        `crates/libs/**`.
        """,
    ".agents/plugins/p/plugin.yaml": "name: p\nversion: '0.1.4'\n",
    ".agents/plugins/p/skills/s/SKILL.md": "Start at `crates/libs/gadget/server/src/lib.rs`.\n",
}

REVIEWED = """\
    [crates]
    "pplx-space-gadget" = { to = "crates/l40_api/gadget/server" }
    "pplx-space-gadgetsvc" = { to = "crates/l40_api/gadgets/svc", rename = "pplx-space-gadgets-svc" }

    [stale_paths]
    "scripts/move_crates.toml" = "the mapping names crates as they were"
    """


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.email=t@example.com", "-c", "user.name=t", *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def tool(repo: Path, *args: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "CARGO_TARGET_DIR": str(repo / "target")}
    return subprocess.run(
        [sys.executable, str(TOOL), "--repo", str(repo), *args],
        cwd=repo,
        capture_output=True,
        text=True,
        env=env,
    )


def cargo_check(repo: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["cargo", "check", "--offline", "--locked", "--quiet"],
        cwd=repo,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TARGET_DIR": str(repo / "target")},
    )


def write(repo: Path, path: str, text: str) -> None:
    (repo / path).parent.mkdir(parents=True, exist_ok=True)
    (repo / path).write_text(textwrap.dedent(text))


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    if shutil.which("cargo") is None:
        unavailable_dependency("cargo", "move_crates.py reads the workspace with cargo metadata")
    root = tmp_path / "repo"
    for path, text in FILES.items():
        write(root, path, text)
    (root / "crates/libs/gadget/CLAUDE.md").symlink_to("AGENTS.md")
    (root / "docs/widget-lib.rs").symlink_to("../crates/libs/model/widget/src/lib.rs")
    shutil.copy(SCRIPTS_DIR.parent / "rust-toolchain.toml", root)
    (root / ".gitignore").write_text("target/\n")
    subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=root, check=True, capture_output=True)
    git(root, "init", "-q")
    git(root, "add", "-A")
    git(root, "commit", "-qm", "base")
    return root


def reviewed(repo: Path, text: str = REVIEWED) -> None:
    write(repo, "scripts/move_crates.toml", text)


def generated(repo: Path) -> None:
    reviewed(repo)
    result = tool(repo, "--generate")
    assert result.returncode == 0, result.stderr
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "mapping")


def test_generate_rejects_a_nesting_without_a_leaf(repo: Path) -> None:
    result = tool(repo, "--generate")

    assert result.returncode == 1
    assert (
        "pplx-space-gadget at crates/l40_api/gadget would contain "
        "pplx-space-gadget-client at crates/l40_api/gadget/client" in result.stderr
    )


def test_generate_rejects_a_target_off_the_name_rule(repo: Path) -> None:
    reviewed(repo, REVIEWED.replace("crates/l40_api/gadget/server", "crates/l40_api/gadgets/server"))

    result = tool(repo, "--generate")

    assert result.returncode == 1
    assert "crates/l40_api/gadgets/server breaks the layout rule" in result.stderr


def test_generate_rejects_a_rename_the_baseline_names(repo: Path) -> None:
    write(
        repo,
        "architecture-baseline.toml",
        '[layers.codec_features]\n"pplx-space-gadget" = ["pplx-space-gadgetsvc/s3"]\n',
    )
    reviewed(repo)

    result = tool(repo, "--generate")

    assert result.returncode == 1
    assert "pplx-space-gadgetsvc: renaming it re-keys its entries in architecture-baseline.toml" in result.stderr


def test_generate_keeps_the_reviewed_leaf_and_rename(repo: Path) -> None:
    reviewed(repo)

    result = tool(repo, "--generate")

    assert result.returncode == 0, result.stderr
    mapping = tomllib.loads((repo / "scripts/move_crates.toml").read_text())
    assert mapping["crates"] == {
        "pplx-space-widget": {"to": "crates/l20_domain/widget"},
        "pplx-space-gadget": {"to": "crates/l40_api/gadget/server"},
        "pplx-space-gadget-client": {"to": "crates/l40_api/gadget/client"},
        "pplx-space-gadget-protocol": {"to": "crates/l20_domain/gadget/protocol"},
        "pplx-space-gadgetsvc": {"to": "crates/l40_api/gadgets/svc", "rename": "pplx-space-gadgets-svc"},
        "pplx-space-legacy": "stay",
    }
    # Most of the family goes to l40_api, so its AGENTS.md does too.
    assert mapping["files"] == {
        "crates/libs/gadget/AGENTS.md": "crates/l40_api/gadget/AGENTS.md",
        "crates/libs/gadget/CLAUDE.md": "crates/l40_api/gadget/CLAUDE.md",
    }


def test_apply_moves_crates_and_rewrites_what_names_them(repo: Path) -> None:
    generated(repo)

    result = tool(repo, "--apply")

    assert result.returncode == 0, result.stderr
    files = set(git(repo, "ls-files").split())
    assert {
        "crates/l20_domain/widget/build.rs",
        "crates/l20_domain/gadget/protocol/src/lib.rs",
        "crates/l40_api/gadget/server/src/lib.rs",
        "crates/l40_api/gadgets/svc/src/lib.rs",
        "crates/l40_api/gadget/AGENTS.md",
        "crates/libs/legacy/src/lib.rs",
    } <= files
    assert not any(f.startswith(("crates/libs/model", "crates/libs/gadget")) for f in files)
    # The relative paths out of moved crates, and into one from a crate that
    # stays, still resolve; the renamed library builds under its new name.
    check = cargo_check(repo)
    assert check.returncode == 0, check.stderr
    assert 'path = "../../l20_domain/widget"' in (repo / "crates/libs/legacy/Cargo.toml").read_text()
    assert "pplx_space_gadgets_svc::NAME" in (repo / "crates/l40_api/gadget/server/src/lib.rs").read_text()
    # A climb from the manifest directory in a format string, and one from a
    # script's own directory, which the script still finds once moved.
    widget = (repo / "crates/l20_domain/widget/src/lib.rs").read_text()
    assert 'format!("{}/../../../data/blob.txt", env!("CARGO_MANIFEST_DIR"))' in widget
    script = subprocess.run(["sh", str(repo / "crates/l20_domain/widget/scripts/blob.sh")], capture_output=True, text=True)
    assert script.stdout == "blob\n", script.stderr
    # A directory of crates split across layers: one list line per layer, a
    # brace glob in prose.
    assert (repo / "ci/paths.txt").read_text() == (
        "crates/l20_domain/gadget/**\ncrates/l40_api/gadget/**\ncrates/libs/legacy/**\n"
    )
    notes = (repo / "docs/notes.md").read_text()
    assert "`crates/l20_domain/widget/src/lib.rs`" in notes
    assert "`crates/{l20_domain,l40_api}/gadget`" in notes
    assert "(../crates/l40_api/gadget/server/src/lib.rs)" in notes
    assert "`pplx-space-gadgets-svc`" in notes
    assert "`crates/libs/**`" in notes
    assert "docs/notes.md:4: crates/libs/**" in result.stdout
    # In TOML the split path becomes several quoted ones, and the widget's
    # rewritten path, which `crates/l*/widget/**` now covers, is dropped. The
    # regex `\b` around the renamed library is not part of its name.
    architecture = (repo / "ARCHITECTURE.toml").read_text()
    assert (
        'allow = ["crates/l20_domain/gadget/**", "crates/l40_api/gadget/**", "crates/l*/widget/**"]'
        in architecture
    )
    assert "patterns = ['\\bpplx_space_gadgets_svc\\b']" in architecture
    assert "pplx-space-widget" not in architecture and "pplx-space-legacy" in architecture
    # Each comment keeps the entries it heads.
    assert (repo / "architecture-baseline.toml").read_text() == textwrap.dedent(
        """\
        [file_size]
        # the widget
        "crates/l20_domain/widget/src/lib.rs" = 1200
        # the gadget services
        "crates/l40_api/gadget/client/src/lib.rs" = 1300
        "crates/l40_api/gadget/server/src/lib.rs" = 1100
        """
    )
    assert (repo / "crates/l40_api/gadget/CLAUDE.md").read_text() == "# The gadget crates\n"
    assert os.readlink(repo / "docs/widget-lib.rs") == "../crates/l20_domain/widget/src/lib.rs"
    assert "version: '0.1.5'" in (repo / ".agents/plugins/p/plugin.yaml").read_text()

    verify = tool(repo, "--verify", "--skip-arch")

    assert verify.returncode == 0, verify.stderr
    assert "crates/libs/model/widget -> crates/l20_domain/widget" in verify.stdout
    assert "pplx-space-gadgetsvc -> pplx-space-gadgets-svc" in verify.stdout


def test_apply_names_each_crate_of_a_family_another_crate_joins(repo: Path) -> None:
    # Renamed, gadgetsvc lands in crates/l40_api/gadget, so that directory is
    # no longer only the old family's: the paths name its crates one by one.
    generated(repo)
    mapping = repo / "scripts/move_crates.toml"
    mapping.write_text(mapping.read_text().replace("gadgets/svc", "gadget/svc").replace("gadgets-svc", "gadget-svc"))
    git(repo, "commit", "-qam", "rename into the family")

    result = tool(repo, "--apply")

    assert (repo / "ci/paths.txt").read_text() == (
        "crates/l20_domain/gadget/**\n"
        "crates/l40_api/gadget/client/**\n"
        "crates/l40_api/gadget/server/**\n"
        "crates/l40_api/gadget/AGENTS.md\n"
        "crates/l40_api/gadget/CLAUDE.md\n"
        "crates/libs/legacy/**\n"
    )
    assert result.returncode == 1
    assert "docs/notes.md:2: crates/libs/gadget splits into" in result.stderr


def widen_a_ratchet(repo: Path) -> None:
    text = (repo / "ARCHITECTURE.toml").read_text()
    write(repo, "ARCHITECTURE.toml", text + '\n[rules.pattern_ratchet.w]\ninclude = ["crates/l20_domain/**"]\n')


def name_a_split_family_in_code(repo: Path) -> None:
    write(repo, "scripts/roots.py", 'ROOTS = ("crates/libs/gadget", "data")\n')


def name_a_missing_path_in_a_moved_crate(repo: Path) -> None:
    write(repo, "crates/libs/gadgetsvc/README.md", "See [it](../../../../crates/libs/gadget/server/missing).\n")


def climb_after_an_expansion(repo: Path) -> None:
    write(repo, "crates/libs/gadgetsvc/gen.sh", 'protoc -I "$CRATE_DIR/../../../proto" gadget.proto\n')


def name_a_moved_path_in_latin1(repo: Path) -> None:
    (repo / "docs/latin1.txt").write_bytes(b"caf\xe9: crates/libs/gadget/server/src/lib.rs\n")


def link_to_a_missing_file_in_a_moved_crate(repo: Path) -> None:
    (repo / "docs/gone.rs").symlink_to("../crates/libs/model/widget/src/missing.rs")


def dirty_the_tree(repo: Path) -> None:
    write(repo, "docs/draft.md", "draft\n")


def drop_a_row(repo: Path) -> None:
    text = (repo / "scripts/move_crates.toml").read_text()
    write(repo, "scripts/move_crates.toml", text.replace('"pplx-space-legacy" = "stay"\n', ""))
    git(repo, "commit", "-qam", "drop a row")


# Each setup, made before the mapping is generated or after it is committed,
# and what --apply says as it exits 1.
REFUSALS: list[tuple[str, Callable[[Path], None], bool, str]] = [
    ("dirty tree", dirty_the_tree, False, "the tree has uncommitted changes"),
    ("stale mapping", drop_a_row, False, "the mapping is out of date"),
    ("widened glob", widen_a_ratchet, True, "[rules.pattern_ratchet.w] include now also matches"),
    (
        "climb reaching nothing",
        name_a_missing_path_in_a_moved_crate,
        True,
        "crates/l40_api/gadgets/svc/README.md:1: ../../../../crates/libs/gadget/server/missing "
        "names a moved path but reaches nothing tracked",
    ),
    (
        "climb after an expansion",
        climb_after_an_expansion,
        True,
        "crates/l40_api/gadgets/svc/gen.sh:1: ../../../proto follows $CRATE_DIR, whose directory this tool can't tell",
    ),
    (
        "not UTF-8",
        name_a_moved_path_in_latin1,
        True,
        "docs/latin1.txt: not UTF-8, so not rewritten, but it names a moved path",
    ),
    (
        "dangling symlink",
        link_to_a_missing_file_in_a_moved_crate,
        True,
        "docs/gone.rs: a symlink to ../crates/libs/model/widget/src/missing.rs, which reaches nothing tracked",
    ),
    (
        "split path in code",
        name_a_split_family_in_code,
        True,
        "scripts/roots.py:1: crates/libs/gadget splits into crates/l20_domain/gadget, crates/l40_api/gadget",
    ),
]


@pytest.mark.parametrize(("setup", "before", "message"), [r[1:] for r in REFUSALS], ids=[r[0] for r in REFUSALS])
def test_apply_exits_1_on_what_it_cannot_do_exactly(
    repo: Path, setup: Callable[[Path], None], before: bool, message: str
) -> None:
    if before:
        setup(repo)
        git(repo, "add", "-A")
        git(repo, "commit", "-qm", "setup")
    generated(repo)
    if not before:
        setup(repo)

    result = tool(repo, "--apply")

    assert result.returncode == 1
    assert message in result.stderr


def test_apply_after_a_move_finds_renamed_crates_by_their_new_names(repo: Path) -> None:
    generated(repo)
    assert tool(repo, "--apply").returncode == 0
    git(repo, "commit", "-qm", "move")

    again = tool(repo, "--apply")

    assert again.returncode == 0, again.stderr
    assert "nothing to move" in again.stdout


def test_verify_reports_old_paths_and_names(repo: Path) -> None:
    generated(repo)
    nothing = tool(repo, "--verify", "--skip-arch")
    assert nothing.returncode == 1
    assert "no crate moved relative to HEAD" in nothing.stderr
    assert tool(repo, "--apply").returncode == 0
    git(repo, "commit", "-qm", "move")
    write(repo, "docs/stale.md", "See crates/libs/model/widget/build.rs.\n")
    write(repo, "docs/pattern.toml", "patterns = ['\\bpplx_space_gadgetsvc\\b']\n")
    write(repo, "crates/libs/legacy/build.rs", 'fn main() { let _ = "../model/widget/build.rs"; }\n')
    # A moved file's climbs, written as they were before the move.
    write(
        repo,
        "crates/l20_domain/widget/src/stale.rs",
        'pub fn p() -> String {\n    format!("{}/../../../../data/blob.txt", env!("CARGO_MANIFEST_DIR"))\n}\n',
    )
    write(repo, "crates/l20_domain/widget/gen.sh", 'protoc -I "$CRATE_DIR/../../../proto" widget.proto\n')
    git(repo, "add", "-A")

    stale = tool(repo, "--verify", "--skip-arch", "--base", "HEAD~1")

    assert stale.returncode == 1
    assert "docs/stale.md:1: crates/libs/model/widget/build.rs" in stale.stderr
    assert "docs/pattern.toml:1: pplx_space_gadgetsvc" in stale.stderr
    assert "crates/libs/legacy/build.rs:1: ../model/widget/build.rs reached a tracked path" in stale.stderr
    assert "crates/l20_domain/widget/src/stale.rs:2: ../../../../data/blob.txt reached a tracked path" in stale.stderr
    assert "crates/l20_domain/widget/gen.sh:1: ../../../proto follows an expansion" in stale.stderr

    mapping = repo / "scripts/move_crates.toml"
    allow = (
        '"docs/stale.md" = "a dated record"\n"docs/pattern.toml" = "x"\n"crates/libs/legacy/build.rs" = "x"\n'
        '"crates/l20_domain/widget/src/stale.rs" = "x"\n"crates/l20_domain/widget/gen.sh" = "x"\n'
    )
    mapping.write_text(mapping.read_text() + allow)

    allowed = tool(repo, "--verify", "--skip-arch", "--base", "HEAD~1")

    assert allowed.returncode == 0, allowed.stderr


def twin_build_scripts(repo: Path) -> None:
    # Once moved, each build script is the other's old one byte for byte, so
    # git pairs them crosswise and would carry an open edit into the other
    # crate.
    widget = (repo / "crates/libs/model/widget/build.rs").read_text()
    write(repo, "crates/libs/gadgetsvc/build.rs", widget.replace("../../../../proto", "../../../proto"))
    write(repo, "crates/libs/gadgetsvc/Cargo.toml", manifest("pplx-space-gadgetsvc", build=True))


def a_one_line_file_that_climbs(repo: Path) -> None:
    # Its only line changes with the move, so git sees a deletion and an
    # addition, and an open edit to it would not follow it.
    write(repo, "crates/libs/model/widget/src/blob.rs", 'pub const B: &str = include_str!("../../../../../data/blob.txt");\n')


@pytest.mark.parametrize(
    ("setup", "message"),
    [
        (
            twin_build_scripts,
            "crates/libs/model/widget/build.rs -> crates/l40_api/gadgets/svc/build.rs, "
            "but the move puts it at crates/l20_domain/widget/build.rs",
        ),
        (
            a_one_line_file_that_climbs,
            "moved file(s) git does not pair as renames:\n    "
            "crates/libs/model/widget/src/blob.rs -> crates/l20_domain/widget/src/blob.rs",
        ),
    ],
    ids=["mismatched pair", "unpaired"],
)
def test_verify_rejects_renames_git_does_not_pair_as_the_move(
    repo: Path, setup: Callable[[Path], None], message: str
) -> None:
    setup(repo)
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "setup")
    generated(repo)
    assert tool(repo, "--apply").returncode == 0

    result = tool(repo, "--verify", "--skip-arch")

    assert result.returncode == 1
    assert message in result.stderr
