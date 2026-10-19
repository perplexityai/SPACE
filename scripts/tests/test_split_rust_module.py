"""split_rust_module.py moves items into child modules without changing what they mean.

Each test builds a small crate in a temporary git repository, runs `apply`, and checks
the generated text and that `verify` accepts the result. The visibility edits must keep
every item's reach exactly what it was in the parent.
"""

from __future__ import annotations

import os
import re
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


def rustfmt_all(repo: Path) -> None:
    """What `cargo fmt` does to a real split before it is committed."""
    for f in sorted((repo / "crate").rglob("*.rs")):
        out = subprocess.run(["rustfmt", "--edition", "2024", "--emit", "stdout", "--quiet"],
                             input=f.read_text(), capture_output=True, text=True, check=True).stdout
        f.write_text(out)


def apply_and_commit(repo: Path) -> None:
    r = run("apply", "spec.toml", cwd=repo)
    assert r.returncode == 0, r.stderr
    rustfmt_all(repo)
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
    tuple = re.sub(r"\s+", " ", config[config.index("pub(super) struct Tuple("):])
    assert tuple.startswith("pub(super) struct Tuple( pub(super) u32, pub String, #[allow(dead_code)] pub(super) Box<dyn Fn(u8) -> u8>, );")
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
    assert "regenerated and compared 4 files from 1 split parents: identical" in ok.stdout

    config = repo / "crate/src/a/foo/config.rs"
    config.write_text(config.read_text().replace("        self.inner\n", "        self.inner + 1\n"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "edit")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1
    assert "foo/config.rs: differs from what apply generates" in bad.stdout and "self.inner + 1" in bad.stdout


def test_verify_rejects_a_head_that_is_not_rustfmt_formatted(repo):
    apply_and_commit(repo)
    parsing = repo / "crate/src/a/foo/parsing.rs"
    parsing.write_text(parsing.read_text().replace("pub(super) fn parse(x: &str) -> u32 {",
                                                   "pub(super) fn parse(\n    x: &str,\n) -> u32 {"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "fmt")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "parsing.rs: differs from what apply generates" in bad.stdout


def test_verify_rejects_a_token_edit_rustfmt_would_erase(repo):
    apply_and_commit(repo)
    parsing = repo / "crate/src/a/foo/parsing.rs"
    parsing.write_text(parsing.read_text().replace("x.len() as u32", "vec![x.len() as u32,][0]"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "edit")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "parsing.rs: differs from what apply generates" in bad.stdout


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
    assert bad.returncode == 1 and "const LIMIT: u32 = 6;" in bad.stdout


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
    assert bad.returncode == 1 and "differs from what apply generates" in bad.stdout


# --- Adversarial cases: verify must reject anything that is not what apply generates. ---


def commit_all(repo: Path, msg: str) -> None:
    git(repo, "add", "-A")
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qm", msg)


@pytest.mark.parametrize(
    ("path", "old", "new"),
    [
        # dropping a re-export or a module declaration silently drops items
        ("crate/src/a/foo.rs", "pub use config::*;\n", ""),
        ("crate/src/a/foo.rs", "mod parsing;\n", ""),
        # an unrelated import smuggled next to the generated lines
        ("crate/src/a/foo.rs", "pub use config::*;\n", "pub use config::*;\nuse std::process::exit; // use parsing::*;\n"),
        # syntax a whitespace-insensitive comparison would erase
        ("crate/src/a/foo/config.rs", 'let s = "fn not_a_member() {}";', 'let s = "fn not_a_member()\n{}";'),
        ("crate/src/a/foo/config.rs", "self.inner\n", "(self.inner,).0\n"),
        # a comment edited inside a moved item
        ("crate/src/a/foo/parsing.rs", "// Parsing.", "// Parsing, edited."),
    ],
)
def test_verify_rejects_edits_a_loose_comparison_would_miss(repo, path, old, new):
    apply_and_commit(repo)
    f = repo / path
    assert old in f.read_text()
    f.write_text(f.read_text().replace(old, new, 1))
    commit_all(repo, "edit")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and ("differs from what apply generates" in bad.stdout or "regenerating" in bad.stdout), bad.stdout + bad.stderr


def test_verify_rejects_doc_comments_swapped_between_impls(repo):
    (repo / "spec.toml").write_text(MEMBER_SPEC)
    src = repo / "crate/src/a/foo.rs"
    src.write_text(src.read_text().replace("impl Config {", "/// Config docs.\nimpl Config {")
                   .replace("impl<F> Holder<F>", "/// Holder docs.\nimpl<F> Holder<F>"))
    commit_all(repo, "docs")
    apply_and_commit(repo)
    text = src.read_text()
    src.write_text(text.replace("/// Config docs.", "/// TMP").replace("/// Holder docs.", "/// Config docs.")
                   .replace("/// TMP", "/// Holder docs."))
    commit_all(repo, "swap")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "differs from what apply generates" in bad.stdout


def write_parent(repo: Path, text: str, spec: str) -> subprocess.CompletedProcess:
    (repo / "crate/src/a/foo.rs").write_text(textwrap.dedent(text))
    (repo / "spec.toml").write_text(textwrap.dedent(spec))
    commit_all(repo, "parent")
    return run("apply", "spec.toml", cwd=repo)


def test_a_shadowed_macro_used_by_moved_code_is_refused(repo):
    r = write_parent(repo, """\
        macro_rules! m {
            () => {
                1
            };
        }

        fn f() -> u8 {
            m!()
        }

        macro_rules! m {
            () => {
                2
            };
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "defined more than once" in r.stderr


def test_a_macro_chain_that_emits_relative_visibility_is_refused(repo):
    r = write_parent(repo, """\
        macro_rules! helper {
            ($n:ident) => {
                pub(super) fn $n() {}
            };
        }

        macro_rules! outer {
            ($n:ident) => {
                helper!($n);
            };
        }

        struct S;

        impl S {
            outer!(made);
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["impl S"]\n')
    assert r.returncode != 0 and "macro_rules! outer" in r.stderr


def test_the_reexport_reaches_as_far_as_the_widest_item(repo):
    r = write_parent(repo, """\
        pub(in crate::a) struct Wide;

        pub(super) struct Narrow;
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["struct Wide", "struct Narrow"]\n')
    assert r.returncode == 0, r.stderr
    parent = (repo / "crate/src/a/foo.rs").read_text()
    child = (repo / "crate/src/a/foo/x.rs").read_text()
    assert "pub(in crate::a) use x::*;" in parent
    assert "pub(in crate::a) struct Wide;" in child and "pub(in crate::a) struct Narrow;" in child


def test_fields_after_const_expressions_and_where_clause_blocks_are_widened(repo):
    r = write_parent(repo, """\
        trait Tr<const N: usize> {}

        struct S {
            a: [u8; 1 << 3],
            b: u8,
        }

        struct G<T>
        where
            T: Tr<{ 1 + 1 }>,
        {
            t: T,
        }

        struct E;

        impl E {
            extern "C" fn ffi() {}
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["struct S", "struct G", "impl E"]\n')
    assert r.returncode == 0, r.stderr
    child = (repo / "crate/src/a/foo/x.rs").read_text()
    assert "    pub(super) a: [u8; 1 << 3],\n    pub(super) b: u8," in child
    assert "    T: Tr<{ 1 + 1 }>,\n{\n    pub(super) t: T," in child
    assert '    pub(super) extern "C" fn ffi() {}' in child


@pytest.mark.parametrize(
    ("rel", "segments", "beside"),
    [
        ("c/src/bin/tool.rs", [], True),
        ("c/src/bin/tool/main.rs", [], True),
        ("c/src/bin/tool/helper.rs", ["helper"], False),
        ("c/src/a/main.rs", ["a", "main"], False),
        ("c/src/a/lib.rs", ["a", "lib"], False),
        ("c/src/a/mod.rs", ["a"], True),
    ],
)
def test_file_layout(rel, segments, beside):
    assert srm.file_layout(rel) == (segments, beside)


def test_include_str_paths_are_rebased_and_include_is_refused(repo):
    ok = write_parent(repo, """\
        fn data() -> &'static str {
            include_str!("data.txt")
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn data"]\n')
    assert ok.returncode == 0, ok.stderr
    assert 'include_str!("../data.txt")' in (repo / "crate/src/a/foo/x.rs").read_text()
    for text, message in [
        ("fn items() {\n    include!(\"gen.rs\");\n}\n", "include!"),
        ("fn raw() -> &'static str {\n    include_str!(r\"data.txt\")\n}\n", "cannot rebase"),
    ]:
        git(repo, "reset", "-q", "--hard", "HEAD")
        git(repo, "clean", "-qfd")
        bad = write_parent(repo, text, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "y"\nitems = ["fn *"]\n')
        assert bad.returncode != 0 and message in bad.stderr, bad.stderr


def test_split_impl_attributes_follow_the_header_rewrite(repo):
    r = write_parent(repo, """\
        struct S;

        #[super::attr]
        impl S {
            fn a() {}

            fn b() {}
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["impl S :: fn b"]\n')
    assert r.returncode == 0, r.stderr
    assert "#[super::super::attr]\nimpl S {" in (repo / "crate/src/a/foo/x.rs").read_text()
    assert "#[super::attr]\nimpl S {" in (repo / "crate/src/a/foo.rs").read_text()


def test_cfg_gates_on_declarations_keep_their_exact_text(repo):
    r = write_parent(repo, """\
        #[cfg(custom = "a  b")]
        fn gated() {}
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn gated"]\n')
    assert r.returncode == 0, r.stderr
    assert '#[cfg(custom = "a  b")]\nmod x;' in (repo / "crate/src/a/foo.rs").read_text()


def test_module_name_may_not_shadow_an_import(repo):
    r = write_parent(repo, """\
        use crate::other::child;

        fn f() {}
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "child"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "imports" in r.stderr


def test_nested_relative_visibility_is_refused(repo):
    r = write_parent(repo, """\
        fn f() {
            mod inner {
                pub(super) fn g() {}
            }
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "nested" in r.stderr


# --- Compile oracle: the split crate must build and behave as the original did. ---

ORACLE = {
    "lib.rs": """\
        pub mod a;

        #[test]
        fn reach_is_unchanged() {
            assert_eq!(a::from_grandparent(), 7);
            assert_eq!(a::foo::from_parent(), 12);
        }
        """,
    "a.rs": """\
        pub mod foo;

        pub fn from_grandparent() -> u32 {
            // pub(super) items of foo, and pub(super) fields, reach here.
            foo::shared() + foo::Pair::new().wide
        }
        """,
    "a/foo.rs": """\
        mod sibling;

        pub struct Pair {
            narrow: u32,
            pub(super) wide: u32,
        }

        impl Pair {
            pub fn new() -> Self {
                Self { narrow: 2, wide: 3 }
            }

            fn narrow(&self) -> u32 {
                self.narrow
            }
        }

        pub(super) fn shared() -> u32 {
            4
        }

        fn private_helper() -> u32 {
            5
        }

        pub fn from_parent() -> u32 {
            private_helper() + Pair::new().narrow() + Pair::new().narrow + sibling::calls()
        }
        """,
    "a/foo/sibling.rs": """\
        use super::*;

        pub(super) fn calls() -> u32 {
            private_helper() - 2
        }
        """,
}


def rustc() -> str | None:
    import shutil

    found = shutil.which("rustc")
    if found is None and os.environ.get("SPACE_CI_REQUIRE_TEST_DEPS") == "1":
        pytest.fail("rustc is required for the compile oracle in CI")
    return found


def build_and_test(root: Path) -> subprocess.CompletedProcess:
    exe = root / "oracle-test"
    b = subprocess.run([rustc(), "--edition", "2021", "--test", "-D", "warnings", "-o", str(exe), str(root / "src/lib.rs")],
                       capture_output=True, text=True)
    if b.returncode:
        return b
    return subprocess.run([str(exe)], capture_output=True, text=True)


@pytest.mark.skipif(rustc() is None, reason="rustc not installed")
def test_split_crate_compiles_and_keeps_behavior(tmp_path):
    src = tmp_path / "oracle/src"
    for rel, text in ORACLE.items():
        (src / rel).parent.mkdir(parents=True, exist_ok=True)
        (src / rel).write_text(textwrap.dedent(text))
    assert build_and_test(tmp_path / "oracle").returncode == 0
    (tmp_path / "spec.toml").write_text(
        'file = "oracle/src/a/foo.rs"\n'
        '[[module]]\nname = "pair"\nallow_unused_glob = true\nitems = ["struct Pair", "impl Pair :: fn narrow"]\n'
        '[[module]]\nname = "helpers"\nallow_unused_glob = true\nitems = ["fn shared", "fn private_helper"]\n'
    )
    r = run("apply", "spec.toml", cwd=tmp_path)
    assert r.returncode == 0, r.stderr
    after = build_and_test(tmp_path / "oracle")
    assert after.returncode == 0, after.stdout + after.stderr


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ('#[derive(Default)]\nstruct D {\n    #[serde(default = "super::f")]\n    a: u8,\n}\n', "string literal"),
        ("fn f() {\n    self::super::g();\n}\n", "self::super"),
        ('fn f() -> &\'static str {\n    stringify!(super::g)\n}\n', "stringifies"),
    ],
)
def test_paths_the_tool_cannot_rewrite_are_refused(repo, text, message):
    name = "struct D" if "struct D" in text else "fn f"
    r = write_parent(repo, text, f'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["{name}"]\n')
    assert r.returncode != 0 and message in r.stderr


def test_keyword_module_names_are_refused(repo):
    (repo / "spec.toml").write_text('file = "crate/src/a/foo.rs"\n[[module]]\nname = "type"\nitems = ["fn stays"]\n')
    assert run("apply", "spec.toml", "--dry-run", cwd=repo).returncode != 0


def test_extern_blocks_parse_and_stay(repo):
    r = write_parent(repo, 'extern "C" {\n    fn abs(x: i32) -> i32;\n}\n\nfn f() {}\n',
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode == 0, r.stderr
    assert 'extern "C" {' in (repo / "crate/src/a/foo.rs").read_text()


def test_an_attribute_sharing_a_line_with_its_item_is_refused(repo):
    r = write_parent(repo, "#[rustfmt::skip] const T: u8 = 1;\n\nfn f() {}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "share a line" in r.stderr


def test_verify_rejects_a_symlinked_child_and_an_empty_change(repo):
    assert run("verify", "HEAD", cwd=repo).returncode == 1  # nothing split
    apply_and_commit(repo)
    child = repo / "crate/src/a/foo/k8s.rs"
    real = repo / "elsewhere.rs"
    real.write_text(child.read_text())
    child.unlink()
    child.symlink_to(real)
    commit_all(repo, "symlink")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "not a regular file" in bad.stdout


def test_the_path_attribute_scan_stays_inside_the_crate(tmp_path):
    (tmp_path / "other/src").mkdir(parents=True)
    (tmp_path / "other/src/lib.rs").write_text('#[path = "../../repo/c/src/a.rs"]\nmod x;\n')
    (tmp_path / "repo/c/src").mkdir(parents=True)
    (tmp_path / "repo/c/src/a.rs").write_text("fn f() {}\n")
    assert srm.path_attribute_users(str(tmp_path / "repo"), "c/src/a.rs") == ""


# --- Round-2 review cases. ---


def test_verify_rejects_a_removed_glob_import(repo):
    apply_and_commit(repo)
    child = repo / "crate/src/a/foo/k8s.rs"
    child.write_text(child.read_text().replace("use super::*;\n\n", ""))
    commit_all(repo, "drop glob")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "k8s.rs: differs from what apply generates" in bad.stdout


@pytest.mark.parametrize(
    ("text", "items", "message"),
    [
        # a parent macro whose expansion names a super:: path
        ("macro_rules! m {\n    () => {\n        super::V\n    };\n}\n\nfn f() -> u8 {\n    m!()\n}\n",
         '["fn f"]', "expansion names"),
        # an ancestor macro of the same name would be shadowed by the later local definition
        ("fn f() -> u8 {\n    m!()\n}\n\nmacro_rules! m {\n    () => {\n        2\n    };\n}\n",
         '["fn f"]', "before the parent defines it"),
        # a top-level macro invocation without opt-in
        ("make!();\n\nfn g() {}\n", '["macro make"]', "allow_macro_items"),
        # an inline module nested inside a moved function, with super
        ("fn f() -> u8 {\n    mod i {\n        pub fn g() -> u8 {\n            super::super::V\n        }\n    }\n    i::g()\n}\n",
         '["fn f"]', "inline module"),
        # stringify with braces
        ("fn f() -> &'static str {\n    stringify! {super::hello}\n}\n", '["fn f"]', "stringifies"),
        # an include in a split impl's header
        ("struct S<const N: usize>;\n\nimpl S<{ include_str!(\"size.txt\").len() }> {\n    fn a() {}\n\n    fn b() {}\n}\n",
         '["impl S<* :: fn b"]', "include"),
        # one-line attribute on an impl member
        ("struct S;\n\nimpl S {\n    #[inline] fn a() {}\n\n    fn b() {}\n}\n", '["impl S :: fn b"]', "share a line"),
    ],
)
def test_round_two_refusals(repo, text, items, message):
    r = write_parent(repo, text, f'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = {items}\n')
    assert r.returncode != 0 and message in r.stderr, r.stderr


def test_terminal_super_in_use_trees_is_deepened(repo):
    r = write_parent(repo, "fn f() -> u8 {\n    use super as outer;\n    outer::V\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode == 0, r.stderr
    assert "use super::super as outer;" in (repo / "crate/src/a/foo/x.rs").read_text()


def test_include_with_brace_delimiters_is_rebased(repo):
    r = write_parent(repo, "fn f() -> &'static str {\n    include_str! {\"data.txt\"}\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode == 0, r.stderr
    assert 'include_str! {"../data.txt"}' in (repo / "crate/src/a/foo/x.rs").read_text()


def test_macro_items_move_with_opt_in(repo):
    r = write_parent(repo, "make!();\n\nfn g() {}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nallow_macro_items = true\nitems = ["macro make"]\n')
    assert r.returncode == 0, r.stderr
    assert "pub use x::*;" in (repo / "crate/src/a/foo.rs").read_text()


def test_module_docs_wrap_at_100_columns_and_verify_round_trips(repo):
    long = "A long module description " * 8
    (repo / "spec.toml").write_text(SPEC.replace('doc = "The configuration type."', f'doc = "{long.strip()}"'))
    apply_and_commit(repo)
    config = (repo / "crate/src/a/foo/config.rs").read_text()
    header = [ln for ln in config.split("\n") if ln.startswith("//!")]
    assert len(header) > 1 and all(len(ln) <= 100 for ln in header)
    assert run("verify", "HEAD~1", cwd=repo).returncode == 0


def bin_repo(tmp_path: Path, rel: str) -> Path:
    root = tmp_path / "r"
    (root / rel).parent.mkdir(parents=True)
    (root / rel).write_text("fn helper() -> u8 {\n    1\n}\n\nfn main() {\n    helper();\n}\n")
    git(root, "init", "-q")
    commit_all(root, "base")
    (root / "spec.toml").write_text(f'file = "{rel}"\n[[module]]\nname = "helpers"\nitems = ["fn helper"]\n')
    return root


def test_a_src_bin_file_root_is_refused(tmp_path):
    root = bin_repo(tmp_path, "c/src/bin/tool.rs")
    r = run("apply", "spec.toml", cwd=root)
    assert r.returncode != 0 and "own binary" in r.stderr


def test_a_src_bin_directory_root_round_trips_through_verify(tmp_path):
    root = bin_repo(tmp_path, "c/src/bin/tool/main.rs")
    assert run("apply", "spec.toml", cwd=root).returncode == 0
    assert (root / "c/src/bin/tool/helpers.rs").exists()
    for f in (root / "c").rglob("*.rs"):
        f.write_text(subprocess.run(["rustfmt", "--edition", "2024", "--emit", "stdout", "--quiet"],
                                    input=f.read_text(), capture_output=True, text=True, check=True).stdout)
    commit_all(root, "split")
    v = run("verify", "HEAD~1", cwd=root)
    assert v.returncode == 0, v.stdout


def test_a_carriage_return_is_refused_and_rejected(repo):
    src = repo / "crate/src/a/foo.rs"
    src.write_bytes(src.read_bytes().replace(b"// Parsing.\n", b"// Parsing.\r\n"))
    commit_all(repo, "cr")
    r = run("apply", "spec.toml", cwd=repo)
    assert r.returncode != 0 and "carriage return" in r.stderr
    git(repo, "reset", "-q", "--hard", "HEAD~1")
    (repo / "spec.toml").write_text(SPEC)
    apply_and_commit(repo)
    child = repo / "crate/src/a/foo/parsing.rs"
    child.write_bytes(child.read_bytes().replace(b"// Parsing.\n", b"// Parsing.\r"))
    commit_all(repo, "cr edit")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "carriage return" in bad.stdout


def test_an_attribute_inside_a_block_comment_is_not_an_attribute(repo):
    r = write_parent(repo, "/*\n#[cfg(any())]\n*/\nfn f() {}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode == 0, r.stderr
    assert "#[cfg(any())]\nmod x;" not in (repo / "crate/src/a/foo.rs").read_text()


def test_a_module_name_already_used_as_a_path_is_refused(repo):
    r = write_parent(repo, "use crate::prelude::*;\n\nfn f() -> u8 {\n    util::one()\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "util"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "already used as a path" in r.stderr


def test_verify_round_trips_tricky_but_valid_splits(repo):
    (repo / "crate/src/a/foo.rs").write_text(textwrap.dedent("""\
        trait T {}

        struct S;

        impl super::Marker for S {}

        #[doc = include_str!("doc.md")]
        impl S {
            fn a() {}
        }

        const _: () = ();
        """))
    commit_all(repo, "tricky")
    (repo / "spec.toml").write_text(
        'file = "crate/src/a/foo.rs"\n'
        '[[module]]\nname = "impls"\ndoc = "Impls \u2014 with a non-ASCII dash."\n'
        'items = ["impl super::Marker for S", "impl S", "const _"]\n'
    )
    apply_and_commit(repo)
    parent = (repo / "crate/src/a/foo.rs").read_text()
    assert "mod impls;" in parent and "use impls::*;" not in parent  # nothing nameable to re-export
    assert "impl super::super::Marker for S {}" in (repo / "crate/src/a/foo/impls.rs").read_text()
    v = run("verify", "HEAD~1", cwd=repo)
    assert v.returncode == 0, v.stdout


# --- Round-3 review cases. ---


@pytest.mark.parametrize(
    ("text", "items", "message"),
    [
        # a moved function reaches a later-defined macro through another parent macro
        ("macro_rules! relay {\n    () => {\n        value!()\n    };\n}\n\npub fn answer() -> u8 {\n    relay!()\n}\n\n"
         "macro_rules! value {\n    () => {\n        2\n    };\n}\n", '["fn answer"]', "before the parent defines it"),
        # a parent macro whose body names a terminal `super`
        ("const V: u8 = 2;\n\nmacro_rules! value {\n    () => {{\n        use super as outer;\n        outer::V\n    }};\n}\n\n"
         "pub fn answer() -> u8 {\n    value!()\n}\n", '["fn answer"]', "expansion names"),
        # a multi-line member attribute ending on the member's line
        ("struct S;\n\n#[rustfmt::skip]\nimpl S {\n    #[\n        cfg(all())\n    ] fn answer() -> u8 { 1 }\n    fn stay() {}\n    fn kept() {}\n}\n",
         '["impl S :: fn stay"]', "share a line"),
    ],
)
def test_round_three_refusals(repo, text, items, message):
    r = write_parent(repo, text, f'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = {items}\n')
    assert r.returncode != 0 and message in r.stderr, r.stderr


def test_a_commented_attribute_is_not_copied_onto_split_impl_blocks(repo):
    r = write_parent(repo, "struct S;\n\n/*\n#[cfg(any())]\n*/\nimpl S {\n    fn answer() -> u8 {\n        1\n    }\n\n    fn stay() {}\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["impl S :: fn answer"]\n')
    assert r.returncode == 0, r.stderr
    child = (repo / "crate/src/a/foo/x.rs").read_text()
    assert "#[cfg(any())]" not in child.split("impl S {")[0].split("use super::*;")[1]


@pytest.mark.parametrize(
    ("text", "spec_extra", "items", "message"),
    [
        # an impl-member macro needs the opt-in
        ("struct S;\n\nimpl S {\n    describe!();\n\n    fn stay() {}\n}\n", "", '["impl S :: macro describe"]', "allow_macro_items"),
        # a raw-identifier inline module with super inside a moved fn
        ("fn f() -> u8 {\n    mod r#i {\n        pub fn g() -> u8 {\n            super::super::V\n        }\n    }\n    r#i::g()\n}\n",
         "", '["fn f"]', "inline module"),
        # a fenced code block in a module doc would become a doctest
        ("fn f() {}\n", 'doc = "Example:\\n\\n```\\nassert!(false);\\n```"\n', '["fn f"]', "code block"),
    ],
)
def test_round_three_claude_refusals(repo, text, spec_extra, items, message):
    r = write_parent(repo, text, f'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\n{spec_extra}items = {items}\n')
    assert r.returncode != 0 and message in r.stderr, r.stderr


def test_super_passed_to_a_user_macro_is_refused_and_to_format_is_deepened(repo):
    r = write_parent(repo, "fn f() -> u8 {\n    call!(super)\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "passes `super` to call!" in r.stderr, r.stderr
    git(repo, "reset", "-q", "--hard", "HEAD")
    git(repo, "clean", "-qfd")
    ok = write_parent(repo, "fn f() -> String {\n    format!(\"{}\", super::V)\n}\n",
                      'file = "crate/src/a/foo.rs"\n[[module]]\nname = "y"\nitems = ["fn f"]\n')
    assert ok.returncode == 0, ok.stderr
    assert 'format!("{}", super::super::V)' in (repo / "crate/src/a/foo/y.rs").read_text()


def test_a_crate_macro_whose_body_names_super_is_refused(repo):
    (repo / "crate/src/lib.rs").write_text("macro_rules! up {\n    () => {\n        super::which()\n    };\n}\n")
    r = write_parent(repo, "fn f() {\n    up!();\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "macro_rules! up" in r.stderr, r.stderr


def test_a_parent_with_macro_use_is_refused(repo):
    r = write_parent(repo, "fn early() {}\n\n#[macro_use]\nmod inner {\n    macro_rules! answer {\n        () => {\n            1\n        };\n    }\n}\n\nfn f() -> u8 {\n    answer!()\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "macro_use" in r.stderr, r.stderr


@pytest.mark.parametrize(
    ("root", "call"),
    [
        # a relay macro elsewhere in the crate that reaches a macro naming super
        ("macro_rules! up {\n    () => {\n        super::which()\n    };\n}\n\nmacro_rules! relay {\n    () => {\n        up!()\n    };\n}\n", "relay!()"),
        # a path-qualified call to a crate macro naming super
        ("macro_rules! up {\n    () => {\n        super::which()\n    };\n}\n\npub(crate) use up;\n", "crate::up!()"),
        # an alias, defined in another file, of a macro naming super
        ("macro_rules! up {\n    () => {\n        super::which()\n    };\n}\n\npub(crate) use up as lifted;\n", "lifted!()"),
    ],
)
def test_crate_macros_that_expand_relative_paths_are_refused(repo, root, call):
    (repo / "crate/src/lib.rs").write_text(root)
    r = write_parent(repo, f"fn f() {{\n    {call};\n}}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "expansion names" in r.stderr, r.stderr


def test_the_shadowing_scan_covers_nested_children(repo):
    (repo / "crate/src/a/foo/other").mkdir(parents=True, exist_ok=True)
    (repo / "crate/src/a/foo/other/mod.rs").write_text("use super::*;\n\nfn g() -> u8 {\n    util::one()\n}\n")
    r = write_parent(repo, "mod other;\n\nfn f() {}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "util"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "already used as a path" in r.stderr


def test_a_module_name_used_as_a_path_in_an_existing_child_is_refused(repo):
    (repo / "crate/src/a/foo").mkdir(parents=True, exist_ok=True)
    (repo / "crate/src/a/foo/existing.rs").write_text("use super::*;\n\nfn g() -> u8 {\n    util::one()\n}\n")
    r = write_parent(repo, "mod existing;\n\nfn f() {}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "util"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "already used as a path" in r.stderr


def test_an_unused_reexport_is_kept_under_an_allowance_and_round_trips(repo):
    (repo / "spec.toml").write_text(SPEC.replace('name = "k8s"\n', 'name = "k8s"\nallow_unused_reexport = true\n'))
    apply_and_commit(repo)
    parent = (repo / "crate/src/a/foo.rs").read_text()
    assert '#[allow(unused_imports)]\nuse k8s::*;' in parent
    assert run("verify", "HEAD~1", cwd=repo).returncode == 0


# --- Upstream review cases. ---


@pytest.mark.parametrize(
    ("root", "call", "name"),
    [
        # a path-qualified user macro
        ("macro_rules! call {\n    ($e:path) => {\n        $e()\n    };\n}\n\npub(crate) use call;\n", "crate::call!(super::g)", "call"),
        # a crate macro that shares a name with a known std/tracing/anyhow macro
        ("macro_rules! ensure {\n    ($e:expr) => {\n        assert!($e)\n    };\n}\n", "ensure!(super::ok())", "ensure"),
    ],
)
def test_super_passed_to_a_qualified_or_shadowing_macro_is_refused(repo, root, call, name):
    (repo / "crate/src/lib.rs").write_text(root)
    r = write_parent(repo, f"fn f() {{\n    {call};\n}}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and f"passes `super` to {name}!" in r.stderr, r.stderr


@pytest.mark.parametrize(
    "header",
    [
        "// SPDX-License-Identifier: Apache-2.0\n\n//! Docs.\n",
        "// Copyright notice.\n/*!\n * Block docs.\n */\n",
    ],
)
def test_comments_above_inner_docs_stay_in_the_parent_header(repo, header):
    r = write_parent(repo, header + "\nstruct S;\n\nfn stay() {}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["struct S"]\n')
    assert r.returncode == 0, r.stderr
    assert (repo / "crate/src/a/foo.rs").read_text().startswith(header)
    child = (repo / "crate/src/a/foo/x.rs").read_text()
    assert "Docs" not in child and "notice" not in child and "SPDX" not in child
    rustfmt_all(repo)
    commit_all(repo, "split")
    v = run("verify", "HEAD~1", cwd=repo)
    assert v.returncode == 0, v.stdout


@pytest.mark.parametrize(
    "text",
    [
        "/// See [`super::V`].\nfn f() {}\n",
        "/** Calls [`self::g`]. */\nfn f() {}\n\nfn g() {}\n",
        '#[doc = "Reads [`super::V`]."]\nfn f() {}\n',
    ],
)
def test_doc_links_through_relative_paths_are_refused(repo, text):
    r = write_parent(repo, text, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode != 0 and "doc comment" in r.stderr, r.stderr


def test_relative_paths_in_code_and_plain_comments_are_not_doc_links(repo):
    r = write_parent(repo, "fn f() -> u8 {\n    // Scales super::V.\n    3\n        * super::V\n}\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn f"]\n')
    assert r.returncode == 0, r.stderr
    assert "* super::super::V" in (repo / "crate/src/a/foo/x.rs").read_text()


def test_declarations_go_above_the_first_fn_when_the_parent_has_no_types(repo):
    r = write_parent(repo, "use std::fmt;\n\nfn a() {}\n\nfn b() {}\n\n#[cfg(test)]\nmod tests;\n",
                     'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn b"]\n')
    assert r.returncode == 0, r.stderr
    parent = (repo / "crate/src/a/foo.rs").read_text()
    assert parent.index("use std::fmt;") < parent.index("mod x;") < parent.index("fn a()") < parent.index("mod tests;")


def test_verify_rejects_non_rust_changes_inside_the_split_crate(repo):
    apply_and_commit(repo)
    (repo / "crate/Cargo.toml").write_text('[package]\nname = "c"\n')
    (repo / "NOTES.md").write_text("Outside the crate.\n")
    commit_all(repo, "manifest")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1 and "crate/Cargo.toml" in bad.stdout and "inside the split crate" in bad.stdout
    assert "NOTES.md" not in bad.stdout.split("not checked by verify")[0]
    ok = run("verify", "HEAD~2", "--allow-non-rust", cwd=repo)
    assert ok.returncode == 0, ok.stdout
    git(repo, "reset", "-q", "--hard", "HEAD~1")
    (repo / "NOTES.md").write_text("Outside the crate.\n")
    commit_all(repo, "notes")
    assert run("verify", "HEAD~2", cwd=repo).returncode == 0
