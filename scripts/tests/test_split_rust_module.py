"""split_rust_module.py moves items into child modules without changing what they mean.

Each test builds a small crate in a temporary git repository, runs `apply`, and checks
the generated text and that `verify` accepts the result. The visibility edits must keep
every item's reach exactly what it was in the parent.
"""

from __future__ import annotations

import os
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
    assert "regenerated and compared 4 files from 1 split parents: identical" in ok.stdout

    config = repo / "crate/src/a/foo/config.rs"
    config.write_text(config.read_text().replace("        self.inner\n", "        self.inner + 1\n"))
    git(repo, "-c", "user.email=t@example.com", "-c", "user.name=t", "commit", "-qam", "edit")
    bad = run("verify", "HEAD~2", cwd=repo)
    assert bad.returncode == 1
    assert "foo/config.rs: differs from what apply generates" in bad.stdout and "self.inner + 1" in bad.stdout


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
    assert run("verify", "HEAD~2", cwd=repo).returncode == 1


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
    assert run("verify", "HEAD~2", cwd=repo).returncode == 1


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


def test_include_paths_are_rebased_or_refused(repo):
    ok = write_parent(repo, """\
        fn data() -> &'static str {
            include_str!("data.txt")
        }

        fn items() {
            include!("gen.rs");
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "x"\nitems = ["fn data", "fn items"]\n')
    assert ok.returncode == 0, ok.stderr
    child = (repo / "crate/src/a/foo/x.rs").read_text()
    assert 'include_str!("../data.txt")' in child and 'include!("../gen.rs")' in child
    git(repo, "reset", "-q", "--hard", "HEAD")
    git(repo, "clean", "-qfd")
    bad = write_parent(repo, """\
        fn raw() -> &'static str {
            include_str!(r"data.txt")
        }
        """, 'file = "crate/src/a/foo.rs"\n[[module]]\nname = "y"\nitems = ["fn raw"]\n')
    assert bad.returncode != 0 and "cannot rebase" in bad.stderr


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
        '[[module]]\nname = "pair"\nglob_import = false\nitems = ["struct Pair", "impl Pair :: fn narrow"]\n'
        '[[module]]\nname = "helpers"\nglob_import = false\nitems = ["fn shared", "fn private_helper"]\n'
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
