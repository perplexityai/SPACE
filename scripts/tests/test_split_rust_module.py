"""split_rust_module.py moves items into child modules without changing what they mean.

Each test builds a small crate in a temporary git repository, runs `apply`, and checks
the generated text and that `verify` accepts the result. The visibility edits must keep
every item's reach exactly what it was in the parent.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS_DIR))

import split_rust_module as srm  # noqa: E402

TOOL = SCRIPTS_DIR / "split_rust_module.py"

PARENT = textwrap.dedent(
    """\
    //! Parent docs.

    use std::collections::HashMap;
    use std::fmt;

    mod existing;

    macro_rules! noop {
        () => {};
    }

    /// A config.
    #[derive(Debug)]
    pub struct Config {
        pub name: String,
        inner: u32,
        pub(super) shared: bool,
        routes: HashMap<String, Vec<u8>>,
    }

    struct Tuple(u32, pub String, #[allow(dead_code)] Box<dyn Fn(u8) -> u8>);

    impl Config {
        pub fn new() -> Self {
            noop!();
            Self { name: String::new(), inner: 0, shared: false, routes: HashMap::new() }
        }

        fn private_helper(&self) -> u32 {
            let s = "fn not_a_member() {}";
            self.inner
        }

        pub(super) const LIMIT: u32 = 5;
    }

    impl fmt::Display for Config {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl<F> Holder<F>
    where
        F: for<'a> Fn(&'a u8),
    {
        fn call(&self) {}
    }

    pub struct Holder<F> {
        f: F,
    }

    // Parsing.

    fn parse(x: &str) -> u32 {
        let _ = super::outer();
        let _ = include_str!("data.txt");
        x.len() as u32
    }

    pub(super) fn reach_up() {}

    #[cfg(feature = "k8s")]
    fn k8s_only() {}

    fn stays() {}

    #[cfg(test)]
    mod tests;
    """
)

SPEC = textwrap.dedent(
    """\
    file = "crate/src/a/foo.rs"
    [[module]]
    name = "config"
    doc = "The configuration type."
    items = ["struct Config", "impl Config", "impl fmt::Display for Config", "struct Tuple"]
    [[module]]
    name = "parsing"
    items = ["fn parse", "fn reach_up", "impl<F> Holder<F> *", "struct Holder"]
    [[module]]
    name = "k8s"
    items = ["fn k8s_only"]
    """
)


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True).stdout


def run(*args: str, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(TOOL), *args], cwd=cwd, capture_output=True, text=True)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    src = tmp_path / "crate/src/a"
    src.mkdir(parents=True)
    (src / "foo.rs").write_text(PARENT)
    (tmp_path / "spec.toml").write_text(SPEC)
    git(tmp_path, "init", "-q")
    git(tmp_path, "-c", "user.email=t@example.com", "-c", "user.name=t", "add", "crate")
    git(tmp_path, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qm", "base")
    return tmp_path


def apply_and_commit(repo: Path) -> None:
    r = run("apply", "spec.toml", cwd=repo)
    assert r.returncode == 0, r.stderr
    git(repo, "add", "crate")
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qm", "split")


@pytest.mark.parametrize(
    ("vis", "parent", "expected"),
    [
        ("", ["a", "foo"], "pub(super) "),
        ("pub(self) ", ["a", "foo"], "pub(super) "),
        ("pub(super) ", ["a", "foo"], "pub(in crate::a) "),
        ("pub(super) ", ["foo"], "pub(crate) "),
        ("pub(crate) ", ["a", "foo"], None),
        ("pub ", ["a", "foo"], None),
        ("pub(in crate::a) ", ["a", "foo"], None),
        ("", [], "pub(super) "),
    ],
)
def test_visibility_keeps_the_same_reach(vis, parent, expected):
    assert srm.map_vis(vis, parent) == expected


def test_relative_pub_in_is_refused():
    with pytest.raises(SystemExit):
        srm.map_vis("pub(in super::x) ", ["a", "foo"])


@pytest.mark.parametrize(
    ("path", "segments"),
    [
        ("crates/x/src/lib.rs", []),
        ("crates/x/src/a.rs", ["a"]),
        ("crates/x/src/a/mod.rs", ["a"]),
        ("crates/x/src/a/b.rs", ["a", "b"]),
    ],
)
def test_module_path_of(path, segments):
    assert srm.module_path_of(path) == segments


def test_apply_writes_children_and_keeps_parent_paths(repo):
    apply_and_commit(repo)
    parent = (repo / "crate/src/a/foo.rs").read_text()
    config = (repo / "crate/src/a/foo/config.rs").read_text()
    parsing = (repo / "crate/src/a/foo/parsing.rs").read_text()
    k8s = (repo / "crate/src/a/foo/k8s.rs").read_text()

    # Parent: declarations after the macro (textual scope), glob re-exports at each
    # module's widest visibility, cfg gates carried to the declaration.
    assert parent.index("macro_rules! noop") < parent.index("mod config;")
    assert "pub use config::*;" in parent
    assert "pub use parsing::*;" in parent
    assert '#[cfg(feature = "k8s")]\nmod k8s;' in parent
    assert '#[cfg(feature = "k8s")]\nuse k8s::*;' in parent
    assert "fn stays() {}" in parent and "#[cfg(test)]\nmod tests;" in parent
    assert "struct Config" not in parent

    assert config.startswith("//! The configuration type.\n\nuse super::*;\n")
    # Fields and inherent-impl members keep the parent's reach; trait impls are untouched.
    assert "    pub name: String,\n    pub(super) inner: u32,\n    pub(in crate::a) shared: bool," in config
    assert "    pub(super) routes: HashMap<String, Vec<u8>>," in config
    assert "pub(super) struct Tuple(pub(super) u32, pub String, #[allow(dead_code)] pub(super) Box<dyn Fn(u8) -> u8>);" in config
    assert "    pub(super) fn private_helper(&self) -> u32 {" in config
    assert '        let s = "fn not_a_member() {}";' in config
    assert "    pub(in crate::a) const LIMIT: u32 = 5;" in config
    assert "    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {" in config

    # A higher-ranked bound is not a trait impl; super:: and include paths go one level deeper.
    assert "    pub(super) fn call(&self) {}" in parsing
    assert "    pub(super) f: F," in parsing
    assert "// Parsing.\n\npub(super) fn parse(x: &str) -> u32 {" in parsing
    assert "super::super::outer()" in parsing
    assert 'include_str!("../data.txt")' in parsing
    assert "pub(in crate::a) fn reach_up() {}" in parsing
    assert '#[cfg(feature = "k8s")]\npub(super) fn k8s_only() {}' in k8s


def test_verify_accepts_the_move_and_rejects_an_edit(repo):
    apply_and_commit(repo)
    ok = run("verify", "HEAD~1", cwd=repo)
    assert ok.returncode == 0, ok.stdout
    assert "moved: 11   unmatched at base: 0   unmatched at head: 0" in ok.stdout

    config = repo / "crate/src/a/foo/config.rs"
    config.write_text(config.read_text().replace("        self.inner\n", "        self.inner + 1\n"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "edit")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1
    assert "impl Config" in bad.stdout


def test_verify_ignores_rustfmt_rewraps(repo):
    apply_and_commit(repo)
    parsing = repo / "crate/src/a/foo/parsing.rs"
    parsing.write_text(parsing.read_text().replace("pub(super) fn parse(x: &str) -> u32 {",
                                                   "pub(super) fn parse(\n    x: &str,\n) -> u32 {"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "fmt")
    assert run("verify", "HEAD~2", cwd=repo).returncode == 0


def test_verify_sees_edits_inside_string_literals(repo):
    apply_and_commit(repo)
    config = repo / "crate/src/a/foo/config.rs"
    config.write_text(config.read_text().replace('"fn not_a_member() {}"', '"fn not_a_member(){}"'))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "edit")
    assert run("verify", "HEAD~2", cwd=repo).returncode == 1


@pytest.mark.parametrize(
    ("items", "message"),
    [
        ('["fn missing"]', "matches no item"),
        ('["macro_rules! noop"]', "macro_rules"),
        ('["mod-decl existing"]', "stay in the parent"),
        ('["fn stays", "fn stay*"]', None),
    ],
)
def test_spec_errors(repo, items, message):
    (repo / "spec.toml").write_text(f'file = "crate/src/a/foo.rs"\n[[module]]\nname = "m"\nitems = {items}\n')
    r = run("apply", "spec.toml", "--dry-run", cwd=repo)
    if message is None:
        assert r.returncode == 0, r.stderr
    else:
        assert r.returncode != 0
        assert message in r.stderr


def test_an_item_cannot_go_to_two_modules(repo):
    (repo / "spec.toml").write_text(
        'file = "crate/src/a/foo.rs"\n'
        '[[module]]\nname = "one"\nitems = ["fn stays"]\n'
        '[[module]]\nname = "two"\nitems = ["fn stay*"]\n'
    )
    r = run("apply", "spec.toml", "--dry-run", cwd=repo)
    assert r.returncode != 0 and "matched by one and two" in r.stderr


def test_module_name_may_not_shadow_an_existing_module(repo):
    (repo / "spec.toml").write_text('file = "crate/src/a/foo.rs"\n[[module]]\nname = "existing"\nitems = ["fn stays"]\n')
    r = run("apply", "spec.toml", "--dry-run", cwd=repo)
    assert r.returncode != 0 and "collides" in r.stderr


MEMBER_SPEC = textwrap.dedent(
    """\
    file = "crate/src/a/foo.rs"
    [[module]]
    name = "helpers"
    doc = "Config helpers."
    items = ["impl Config :: fn private_helper", "impl Config :: const LIMIT"]
    """
)


def test_members_of_an_inherent_impl_move_into_their_own_impl_block(repo):
    (repo / "spec.toml").write_text(MEMBER_SPEC)
    apply_and_commit(repo)
    parent = (repo / "crate/src/a/foo.rs").read_text()
    helpers = (repo / "crate/src/a/foo/helpers.rs").read_text()
    assert "impl Config {\n    pub fn new() -> Self {" in parent
    assert "private_helper" not in parent and "LIMIT" not in parent
    assert helpers == textwrap.dedent(
        """\
        //! Config helpers.

        use super::*;

        impl Config {
            pub(super) fn private_helper(&self) -> u32 {
                let s = "fn not_a_member() {}";
                self.inner
            }

            pub(in crate::a) const LIMIT: u32 = 5;
        }
        """
    )
    # No re-export: an impl block has nothing to name.
    assert "use helpers::*;" not in parent and "mod helpers;" in parent
    ok = run("verify", "HEAD~1", cwd=repo)
    assert ok.returncode == 0, ok.stdout


def test_an_impl_split_across_modules_keeps_its_attributes_on_every_block(repo):
    (repo / "crate/src/a/foo.rs").write_text(PARENT.replace(
        "impl Config {", "/// Config methods.\n#[allow(dead_code)]\nimpl Config {"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "attrs")
    (repo / "spec.toml").write_text(textwrap.dedent(
        """\
        file = "crate/src/a/foo.rs"
        [[module]]
        name = "one"
        items = ["impl Config :: fn new"]
        [[module]]
        name = "two"
        items = ["impl Config :: *"]
        """
    ).replace('"impl Config :: *"', '"impl Config :: fn private_helper", "impl Config :: const LIMIT"'))
    apply_and_commit(repo)
    one = (repo / "crate/src/a/foo/one.rs").read_text()
    two = (repo / "crate/src/a/foo/two.rs").read_text()
    parent = (repo / "crate/src/a/foo.rs").read_text()
    assert "impl Config" not in parent.replace("impl fmt::Display for Config", "")
    assert "/// Config methods.\n#[allow(dead_code)]\nimpl Config {" in one
    assert "#[allow(dead_code)]\nimpl Config {" in two and "/// Config methods." not in two
    assert run("verify", "HEAD~1", cwd=repo).returncode == 0


def test_trait_impl_members_cannot_be_split(repo):
    (repo / "spec.toml").write_text(
        'file = "crate/src/a/foo.rs"\n[[module]]\nname = "m"\nitems = ["impl fmt::Display for Config :: fn fmt"]\n')
    r = run("apply", "spec.toml", "--dry-run", cwd=repo)
    assert r.returncode != 0 and "only inherent impls" in r.stderr


def test_verify_rejects_a_member_edited_in_its_new_block(repo):
    (repo / "spec.toml").write_text(MEMBER_SPEC)
    apply_and_commit(repo)
    helpers = repo / "crate/src/a/foo/helpers.rs"
    helpers.write_text(helpers.read_text().replace("const LIMIT: u32 = 5;", "const LIMIT: u32 = 6;"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "edit")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "LIMIT" in bad.stdout


def test_a_file_loaded_through_a_path_attribute_is_refused(repo):
    (repo / "crate/src/lib.rs").write_text('#[path = "a/foo.rs"]\nmod renamed;\n')
    (repo / "spec.toml").write_text('file = "crate/src/a/foo.rs"\n[[module]]\nname = "m"\nitems = ["fn stays"]\n')
    r = run("apply", "spec.toml", "--dry-run", cwd=repo)
    assert r.returncode != 0 and "#[path]" in r.stderr


def test_const_underscore_takes_no_visibility(repo):
    src = repo / "crate/src/a/foo.rs"
    src.write_text(src.read_text() + "\nconst _: () = assert!(1 < 2);\n")
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "assert")
    (repo / "spec.toml").write_text('file = "crate/src/a/foo.rs"\n[[module]]\nname = "m"\nitems = ["const _", "fn stays"]\n')
    apply_and_commit(repo)
    m = (repo / "crate/src/a/foo/m.rs").read_text()
    assert "\nconst _: () = assert!(1 < 2);" in m and "pub(super) fn stays" in m
    assert run("verify", "HEAD~1", cwd=repo).returncode == 0


def test_moved_callers_of_a_macro_that_emits_relative_visibility_are_refused(repo):
    src = repo / "crate/src/a/foo.rs"
    src.write_text(src.read_text().replace(
        "macro_rules! noop {\n    () => {};\n}",
        "macro_rules! noop {\n    () => {};\n}\n\nmacro_rules! helper {\n    ($n:ident) => {\n        pub(super) fn $n() {}\n    };\n}\n"
    ) + "\nimpl Holder<u8> {\n    helper!(made);\n}\n")
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "macro")
    (repo / "spec.toml").write_text('file = "crate/src/a/foo.rs"\n[[module]]\nname = "m"\nitems = ["impl Holder<u8>"]\n')
    r = run("apply", "spec.toml", "--dry-run", cwd=repo)
    assert r.returncode != 0 and "macro_rules! helper" in r.stderr


def test_verify_ignores_a_trailing_comma_rustfmt_adds_in_generics(repo):
    apply_and_commit(repo)
    parsing = repo / "crate/src/a/foo/parsing.rs"
    parsing.write_text(parsing.read_text().replace("impl<F> Holder<F>", "impl<\n    F,\n> Holder<F>"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "fmt")
    assert run("verify", "HEAD~2", cwd=repo).returncode == 0


@pytest.mark.parametrize(
    ("path", "old", "new"),
    [
        ("crate/src/a/foo.rs", "//! Parent docs.\n", "//! Parent docs.\n#![allow(dead_code)]\n"),
        ("crate/src/a/foo/config.rs", "//! The configuration type.\n", "#![cfg(test)]\n"),
    ],
)
def test_verify_rejects_header_edits(repo, path, old, new):
    apply_and_commit(repo)
    f = repo / path
    f.write_text(f.read_text().replace(old, new, 1))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "header")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "header" in bad.stdout
