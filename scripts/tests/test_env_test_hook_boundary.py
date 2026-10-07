"""The guard must catch a documented setting read through a test hook."""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "env_test_hook_boundary",
    ROOT / "scripts/source_guards/env_test_hook_boundary.py",
)
assert SPEC is not None and SPEC.loader is not None
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)

DISPATCHER = Path("crates/libs/cluster/controller/server/src/public_operation_dispatcher.rs")
ENV_CONFIG = Path("crates/libs/cluster/controller/server/src/env_config.rs")
DOC_ROW = "| `SPACE_EXAMPLE_LIMIT` | 8 | |\n"
LIB = "crates/libs/example/src/lib.rs"


def tree(root: Path, files: dict[str, str]) -> Path:
    (root / guard.DOCS).parent.mkdir(parents=True, exist_ok=True)
    (root / guard.DOCS).write_text(DOC_ROW)
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return root


def test_repository_reads_documented_settings_outside_test_hooks() -> None:
    assert guard.violations(ROOT) == []


def test_dispatcher_concurrency_moved_back_to_a_test_hook_fails(tmp_path: Path) -> None:
    # The real readers too, so the regression is the one this guard exists for.
    for relative in (guard.DOCS, DISPATCHER, ENV_CONFIG):
        (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, tmp_path / relative)
    source = tmp_path / DISPATCHER
    text = source.read_text()
    anchor = "config.bounded_usize(\n"
    assert anchor in text, "the dispatcher no longer reads its concurrency this way"
    source.write_text(text.replace(anchor, "config.test_bounded_u32(\n", 1))

    failures = guard.violations(tmp_path)
    assert len(failures) == 1
    assert "SPACE_PUBLIC_OPERATION_DISPATCHER_EXECUTION_CONCURRENCY" in failures[0]
    assert "test_bounded_u32" in failures[0]


WRAPPER = "fn test_raw(var: &str) -> Option<String> { test_hook_var(var) }\n"


@pytest.mark.parametrize(
    "files",
    [
        {LIB: 'let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
        {LIB: 'let n = test_hook_var(r#"SPACE_EXAMPLE_LIMIT"#);'},
        {LIB: 'const LIMIT_ENV: &str = "SPACE_EXAMPLE_LIMIT";\nlet n = test_hook_var(LIMIT_ENV);'},
        {LIB: 'static LIMIT_ENV: &str = "SPACE_EXAMPLE_LIMIT";\nlet n = test_hook_var(LIMIT_ENV);'},
        {LIB: 'const LIMIT_ENV: &str = r"SPACE_EXAMPLE_LIMIT";\nlet n = test_hook_var(LIMIT_ENV);'},
        {
            LIB: 'impl C { const ENV: &\'static str = "SPACE_EXAMPLE_LIMIT"; }\n'
            "let n = env::test_hook(\n    env,\n    Self::ENV,\n);"
        },
        # A constant defined in a sibling module of the same crate.
        {
            LIB: "impl Gc { fn from_env() { test_hook_var(Self::ENV); } }",
            "crates/libs/example/src/consts.rs": 'impl Gc { const ENV: &str = "SPACE_EXAMPLE_LIMIT"; }',
        },
        {
            LIB: 'const ENV: &str = "SPACE_EXAMPLE_TICK";\nlet n = test_hook_var(other::LIMIT);',
            "crates/libs/example/src/other.rs": 'pub const LIMIT: &str = "SPACE_EXAMPLE_LIMIT";',
        },
        # Every name-shaped argument counts, not only the first.
        {LIB: 'let n = test_hook(&*PROCESS_ENV, "SPACE_EXAMPLE_LIMIT");'},
        {LIB: 'let n = test_hook_var(if a { "SPACE_EXAMPLE_TICK" } else { "SPACE_EXAMPLE_LIMIT" });'},
        # Through a wrapper, found from its body rather than a list.
        {LIB: WRAPPER + 'let n = test_raw("SPACE_EXAMPLE_LIMIT");'},
        {LIB: "fn test_raw(var: &str) -> Option<String> { test_hook(&*PROCESS_ENV, var) }\n"
         'let n = test_raw("SPACE_EXAMPLE_LIMIT");'},
        {LIB: WRAPPER + "fn test_parsed<T>(v: &str) -> T { test_raw(v) }\n"
         'let n = config.test_parsed::<u32>("SPACE_EXAMPLE_LIMIT");'},
        # Through an import alias of a hook.
        {LIB: "use pplx_space_common_core::env::test_hook_var as hook;\n"
         'let n = hook("SPACE_EXAMPLE_LIMIT");'},
        # The reading file's constant wins over a same-named one elsewhere.
        {
            LIB: 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";\nlet n = test_hook_var(ENV);',
            "crates/libs/example/src/other.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";',
        },
        # A reader defined inside another function is still found.
        {LIB: "fn from_env() {\n    fn test_read(v: &str) -> Option<String> { test_hook_var(v) }\n"
         '    test_read("SPACE_EXAMPLE_LIMIT");\n}'},
        # A char literal holding `"` doesn't swallow the read after it.
        {LIB: "let q = '\"'; let n = test_hook_var(\"SPACE_EXAMPLE_LIMIT\");"},
        # A `//` inside a string doesn't hide the rest of the line.
        {LIB: 'let u = "http://x"; let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
        {LIB: 'let b = br#"a"b // x"#; let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
    ],
    ids=[
        "literal", "raw-literal", "constant", "static", "raw-constant", "associated-constant",
        "self-constant-in-sibling", "path-constant-over-local-name", "second-argument",
        "later-branch", "wrapper", "wrapper-with-env-argument", "turbofish-wrapper-of-wrapper",
        "import-alias", "local-constant-wins", "reader-inside-function", "char-quote-before-read",
        "url-in-string", "byte-raw-string",
    ],
)
def test_documented_setting_through_a_test_hook_fails(tmp_path: Path, files: dict[str, str]) -> None:
    failures = guard.violations(tree(tmp_path, files))
    assert len(failures) == 1, failures
    assert "SPACE_EXAMPLE_LIMIT" in failures[0]


def test_wrapper_not_named_test_fails(tmp_path: Path) -> None:
    source = (
        "fn hook_u32(name: &str) -> Option<String> { test_hook_var(name) }\n"
        'let n = hook_u32("SPACE_EXAMPLE_LIMIT");'
    )
    failures = guard.violations(tree(tmp_path, {LIB: source}))
    assert len(failures) == 1, failures
    assert "`hook_u32` passes its parameter `name`" in failures[0]


@pytest.mark.parametrize(
    "files",
    [
        # An undocumented variable is what test hooks are for.
        {LIB: 'let n = test_hook_var("SPACE_EXAMPLE_TICK_SECS");'},
        # A documented variable read from the environment.
        {LIB: 'let n = config.bounded_u32("SPACE_EXAMPLE_LIMIT", 8, 64);'},
        # A `test_*` function that isn't a reader may take any argument.
        {LIB: 'let image = test_image_ref("SPACE_EXAMPLE_LIMIT");'},
        # A wrapper may forward its parameter.
        {LIB: WRAPPER + 'let n = test_raw("SPACE_EXAMPLE_TICK_SECS");'},
        # Passing a non-string parameter along isn't forwarding a name.
        {LIB: "fn from_environment(env: &dyn Environment) { test_hook(env, Self::TICK); }"},
        # A nested helper inside a wrapper doesn't change the enclosing function.
        {LIB: "fn test_raw(var: &str) -> Option<String> { fn trim(s: String) -> String { s }\n"
         "    test_hook_var(var).map(trim) }"},
        # Imports, fields and mentions in strings aren't reads.
        {LIB: "use pplx_space_common_core::env::{\n    env_flag,\n    test_hook_var,\n};"},
        {LIB: "struct C { test_hook: bool }\nfn f(c: C) -> bool { c.test_hook }"},
        {LIB: 'fn f() { tracing::info!(r#"call test_hook_var("SPACE_EXAMPLE_LIMIT")"#); }'},
        # Comments aren't code, nested or not.
        {LIB: '// test_hook_var("SPACE_EXAMPLE_LIMIT")\n'
         '/* a /* b */ test_hook_var("SPACE_EXAMPLE_LIMIT") */'},
        {LIB: "let q = '\"'; // test_hook_var(\"SPACE_EXAMPLE_LIMIT\")"},
        # Test code may read anything.
        {LIB: '#[cfg(test)]\nmod tests {\n    fn reads() { test_hook_var("SPACE_EXAMPLE_LIMIT"); }\n}'},
        {"crates/libs/example/src/tests.rs": 'let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
        {"crates/libs/example/src/store_tests.rs": 'let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
        {"crates/libs/example/tests/it.rs": 'let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
        # The same constant name means another variable in another file; the
        # reading file's own definition wins.
        {
            "crates/libs/example/src/a.rs": 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";\n'
            "let n = std::env::var(ENV);",
            "crates/libs/example/src/b.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";\n'
            "let n = test_hook_var(ENV);",
        },
        # A name defined differently across the crate isn't guessed, and other
        # crates' constants don't count.
        {
            LIB: "let n = test_hook_var(gc::ENV);",
            "crates/libs/example/src/gc.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";',
            "crates/libs/example/src/evict.rs": 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";',
        },
        {
            "crates/libs/one/Cargo.toml": "",
            "crates/libs/one/src/lib.rs": "let n = test_hook_var(gc::ENV);",
            "crates/libs/one/src/gc.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";',
            "crates/libs/two/Cargo.toml": "",
            "crates/libs/two/src/lib.rs": 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";',
        },
    ],
    ids=[
        "undocumented", "real-read", "non-reader-test-fn", "wrapper-definition",
        "environment-argument", "nested-helper", "multi-line-import", "field", "string-mention",
        "comments", "char-quote", "inline-test-module", "tests-rs", "suffix-tests", "tests-dir",
        "per-file-constant", "ambiguous-constant", "other-crate-constant",
    ],
)
def test_reads_that_are_not_violations_pass(tmp_path: Path, files: dict[str, str]) -> None:
    assert guard.violations(tree(tmp_path, files)) == []
