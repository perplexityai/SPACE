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
        # Wrappers with `String`, generic or escaped-char surroundings.
        {LIB: "fn test_raw(var: String) -> Option<String> { test_hook_var(&var) }\n"
         'let n = test_raw("SPACE_EXAMPLE_LIMIT".to_owned());'},
        {LIB: "fn test_parse<T: FromStr>(var: &str) -> Option<T> { test_hook_var(var)?.parse().ok() }\n"
         'let n = test_parse::<u32>("SPACE_EXAMPLE_LIMIT");'},
        {LIB: "let q = '\\\"'; let n = test_hook_var(\"SPACE_EXAMPLE_LIMIT\");"},
        {LIB: "fn test_hook_fn<F: Fn(&str) -> u8>(var: &str) -> Option<String> { test_hook_var(var) }\n"
         'let n = test_hook_fn("SPACE_EXAMPLE_LIMIT");'},
        # `Self::` resolves in the reading file even when the crate is ambiguous.
        {
            LIB: 'impl C { const ENV: &str = "SPACE_EXAMPLE_LIMIT"; fn f() { test_hook_var(Self::ENV); } }',
            "crates/libs/example/src/other.rs": 'impl D { const ENV: &str = "SPACE_EXAMPLE_TICK_SECS"; }',
        },
        # Release code next to test-only fields, variants and expressions.
        {LIB: "struct S { #[cfg(test)] probe: bool, live: bool }\n"
         "impl S { fn new() { test_hook_var(\"SPACE_EXAMPLE_LIMIT\"); } }"},
        {LIB: "enum E { #[cfg(test)]\n Static(u8), Live }\nfn from_env() { test_hook_var(\"SPACE_EXAMPLE_LIMIT\"); }"},
        {LIB: "fn f() -> u8 { #[cfg(test)]\n return 1;\n 2 }\nfn g() { test_hook_var(\"SPACE_EXAMPLE_LIMIT\"); }"},
        # `not(test)` is release code, and `any(test, ...)` can be.
        {LIB: "#[cfg(not(test))]\nfn limit() -> Option<String> { test_hook_var(\"SPACE_EXAMPLE_LIMIT\") }"},
        {LIB: "#[cfg(any(test, feature = \"x\"))]\nfn limit() -> Option<String> { test_hook_var(\"SPACE_EXAMPLE_LIMIT\") }"},
        # A `pub` wrapper counts in every crate, not only its own.
        {
            "crates/libs/core2/Cargo.toml": "",
            "crates/libs/core2/src/lib.rs": "pub fn test_flag(name: &str) -> bool { test_hook_var(name).is_some() }",
            "crates/libs/user/Cargo.toml": "",
            "crates/libs/user/src/lib.rs": 'let on = core2::test_flag("SPACE_EXAMPLE_LIMIT");',
        },
        # A comparison or shift in a test-only item doesn't hide what follows.
        {LIB: "#[cfg(test)]\nconst SHORT: bool = MAX < 10;\nfn grace() -> Option<String> { test_hook_var(\"SPACE_EXAMPLE_LIMIT\") }"},
        {LIB: "#[cfg(test)]\nconst MASK: u32 = 1 << 4;\nfn grace() -> Option<String> { test_hook_var(\"SPACE_EXAMPLE_LIMIT\") }"},
        # A file compiled outside tests, whatever else its `cfg` mentions.
        {LIB: '#![cfg(all(target_os = "linux", not(test)))]\nfn grace() -> Option<String> { test_hook_var(\"SPACE_EXAMPLE_LIMIT\") }'},
        # A trait method wrapper counts in other crates.
        {
            "crates/libs/a/Cargo.toml": "",
            "crates/libs/a/src/lib.rs": "pub trait HookExt { fn test_read(&self, var: &str) -> Option<String> { test_hook_var(var) } }",
            "crates/libs/b/Cargo.toml": "",
            "crates/libs/b/src/lib.rs": 'let n = x.test_read("SPACE_EXAMPLE_LIMIT");',
        },
        # A test-only field named like a keyword doesn't hide the next item.
        {LIB: "pub struct Sets {\n    #[cfg(test)]\n    union: Vec<u8>,\n    other: u8,\n}\n"
         f"fn grace() -> Option<String> {{ test_hook_var(\"SPACE_EXAMPLE_LIMIT\") }}"},
        {LIB: "enum Op { Call(#[cfg(test)] fn() -> u8) }\n"
         f"fn grace() -> Option<String> {{ test_hook_var(\"SPACE_EXAMPLE_LIMIT\") }}"},
        # A trait impl's method wrapper counts in other crates.
        {
            "crates/libs/a/Cargo.toml": "",
            "crates/libs/a/src/lib.rs": "pub trait Env { fn test_raw(&self, n: &str) -> Option<String>; }\n"
            "impl Env for E { fn test_raw(&self, n: &str) -> Option<String> { test_hook_var(n) } }",
            "crates/libs/b/Cargo.toml": "",
            "crates/libs/b/src/lib.rs": 'let n = e.test_raw("SPACE_EXAMPLE_LIMIT");',
        },
        # A `;` inside the return type doesn't drop the wrapper.
        {LIB: "fn test_key(name: &str) -> Option<[u8; 4]> { test_hook_var(name).map(|_| [0; 4]) }\n"
         'let k = test_key("SPACE_EXAMPLE_LIMIT");'},
        # Test-only code inside a production file is checked like the rest.
        {LIB: "#[cfg(test)]\nmod tests {\n    fn reads() { test_hook_var(\"SPACE_EXAMPLE_LIMIT\"); }\n}"},
        {LIB: "#[cfg(test)]\nfn helper() { test_hook_var(\"SPACE_EXAMPLE_LIMIT\"); }"},
        {"crates/libs/example/src/fixtures.rs": "#![cfg(test)]\nfn r() { test_hook_var(\"SPACE_EXAMPLE_LIMIT\"); }"},
        # A same-named constant elsewhere can't make a documented one disappear.
        {
            LIB: "let n = test_hook_var(gc::ENV);",
            "crates/libs/example/src/gc.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";',
            "crates/libs/example/src/evict.rs": 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";',
        },
        {LIB: 'const GRACE: &str = "SPACE_EXAMPLE_LIMIT";\n'
         "fn grace() -> Option<String> { test_hook_var(GRACE) }\n"
         "#[cfg(test)]\nfn fixture() -> Result<(), String> {\n"
         '    const GRACE: &str = "SPACE_SYNTHETIC_FIXTURE";\n    Ok(())\n}'},
        # Generics holding `->` don't hide the real parameter list.
        {LIB: "fn test_knob<F: FnOnce() -> Result<(), E>, E>(var: &str, f: F) -> Option<String> {\n"
         "    let _ = f();\n    test_hook_var(var)\n}\n"
         'let n = test_knob("SPACE_EXAMPLE_LIMIT", || Ok(()));'},
        # A `pub` wrapper counts elsewhere even when a private one shares its name.
        {
            "crates/libs/a/Cargo.toml": "",
            "crates/libs/a/src/a1.rs": "fn test_raw(var: &str) -> Option<String> { test_hook_var(var) }",
            "crates/libs/a/src/b2.rs": "pub fn test_raw(var: &str) -> Option<String> { test_hook_var(var) }",
            "crates/libs/b/Cargo.toml": "",
            "crates/libs/b/src/lib.rs": 'let n = a::b2::test_raw("SPACE_EXAMPLE_LIMIT");',
        },
        # `-> impl Trait` isn't an impl block for `Self::` resolution.
        {LIB: 'impl C {\n    const ENV: &\'static str = "SPACE_EXAMPLE_LIMIT";\n'
         "    fn knobs() -> impl Iterator<Item = String> {\n"
         '        const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";\n'
         "        test_hook_var(Self::ENV).into_iter()\n    }\n}"},
        # Pipes elsewhere in a wrapper don't hide its forwarding.
        {LIB: "pub fn test_raw(var: &str) -> Option<String> {\n"
         "    if var.is_empty() || var.len() > 64 || var.contains(' ') { return None; }\n"
         "    test_hook_var(var)\n}\n"
         'let n = test_raw("SPACE_EXAMPLE_LIMIT");'},
        {LIB: "pub fn test_raw(var: &str) -> Option<String> {\n"
         "    let known = KNOBS.iter().filter(|k| **k == var).map(|k| k.len()).count();\n"
         "    test_hook_var(var)\n}\n"
         'let n = test_raw("SPACE_EXAMPLE_LIMIT");'},
        # A single-letter constant.
        {LIB: 'const X: &str = "SPACE_EXAMPLE_LIMIT";\nlet n = test_hook_var(X);'},
        # A `//` inside a string doesn't hide the rest of the line.
        {LIB: 'let u = "http://x"; let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
        {LIB: 'let b = br#"a"b // x"#; let n = test_hook_var("SPACE_EXAMPLE_LIMIT");'},
    ],
    ids=[
        "literal", "raw-literal", "constant", "static", "raw-constant", "associated-constant",
        "self-constant-in-sibling", "path-constant-over-local-name", "second-argument",
        "later-branch", "wrapper", "wrapper-with-env-argument", "turbofish-wrapper-of-wrapper",
        "import-alias", "local-constant-wins", "reader-inside-function", "char-quote-before-read",
        "string-parameter-wrapper", "generic-wrapper", "escaped-char-before-read",
        "generic-with-parens", "self-local-over-ambiguous-crate",
        "after-test-field", "after-test-variant", "after-test-expression", "cfg-not-test",
        "cfg-any-test", "pub-wrapper-other-crate", "after-test-comparison", "after-test-shift",
        "inner-cfg-not-test-file", "trait-wrapper-other-crate",
        "after-test-union-field", "after-test-fn-tuple-field", "trait-impl-wrapper-other-crate",
        "array-return-wrapper", "test-module-in-production-file", "test-function-in-production-file",
        "inner-cfg-test-file", "ambiguous-crate-constant", "shadowed-by-test-constant",
        "generics-with-arrow", "pub-wrapper-after-private-namesake", "return-impl-trait",
        "or-chain-in-wrapper", "ended-closure-in-wrapper", "single-letter-constant",
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
    "source",
    [
        "fn hook(var: &str) -> Option<String> { test_hook_var(var.trim()) }",
        "fn hook(var: &&str) -> Option<String> { test_hook_var(*var) }",
        "fn hook(var: Option<&str>) -> Option<String> { test_hook_var(var?) }",
        "fn hook(var: Cow<'_, str>) -> Option<String> { test_hook_var(&var) }",
        "fn hook<F: Fn(&str) -> u8>(f: F, var: &str) -> Option<String> { test_hook_var(var) }",
    ],
    ids=["method", "deref", "question-mark", "cow", "fn-bound-before"],
)
def test_parameter_shapes_forwarded_outside_test_function_fail(tmp_path: Path, source: str) -> None:
    failures = guard.violations(tree(tmp_path, {LIB: source}))
    assert len(failures) == 1, failures
    assert "`hook` passes its parameter `var`" in failures[0]


def test_attributed_parameter_forwarded_outside_test_function_fails(tmp_path: Path) -> None:
    source = "fn read(#[allow(unused)] var: &str) -> Option<String> { test_hook_var(var) }"
    failures = guard.violations(tree(tmp_path, {LIB: source}))
    assert len(failures) == 1, failures
    assert "`read` passes its parameter `var`" in failures[0]


def test_mut_parameter_forwarded_outside_test_function_fails(tmp_path: Path) -> None:
    source = "fn hook(mut name: &str) -> Option<String> { test_hook_var(name) }"
    failures = guard.violations(tree(tmp_path, {LIB: source}))
    assert len(failures) == 1, failures
    assert "`hook` passes its parameter `name`" in failures[0]


P = pytest.param
READ = 'test_hook_var("SPACE_EXAMPLE_LIMIT")'


@pytest.mark.parametrize(
    "files",
    [
        # Reads that are what test hooks are for, or not hook reads at all.
        P({LIB: 'let n = test_hook_var("SPACE_EXAMPLE_TICK_SECS");'}, id="undocumented"),
        P({LIB: 'let n = config.bounded_u32("SPACE_EXAMPLE_LIMIT", 8, 64);'}, id="real-read"),
        P({LIB: 'let image = test_image_ref("SPACE_EXAMPLE_LIMIT");'}, id="non-reader-test-fn"),
        P({LIB: WRAPPER + 'let n = test_raw("SPACE_EXAMPLE_TICK_SECS");'}, id="wrapper-definition"),
        # Passing along something that isn't a whole string parameter.
        P({LIB: "fn from_environment(env: &dyn Environment) { test_hook(env, Self::TICK); }"},
          id="environment-argument"),
        P({LIB: "fn test_raw(var: &str) -> Option<String> { fn trim(s: String) -> String { s }\n"
               "    test_hook_var(var).map(trim) }"}, id="nested-helper"),
        P({LIB: "fn tick(service: &str) -> Option<String> {\n"
               '    test_hook(&scoped_env(service), "SPACE_EXAMPLE_TICK_SECS")\n}'},
          id="parameter-inside-argument"),
        P({LIB: 'fn tick(prefix: &str) -> Option<String> { test_hook_var(&format!("{}_TICK", prefix)) }'},
          id="parameter-inside-format"),
        P({LIB: "impl C { fn f(&self, name: &str) -> Option<String> { test_hook_var(self.name) } }"},
          id="field-not-parameter"),
        P({LIB: "fn tick(service: &str, env: &dyn Environment) -> Option<String> {\n"
               '    test_hook(&service.env_for(env), "SPACE_EXAMPLE_TICK_SECS")\n}'},
          id="parameter-starts-argument"),
        # Imports, fields and mentions in strings and comments aren't reads.
        P({LIB: "use pplx_space_common_core::env::{\n    env_flag,\n    test_hook_var,\n};"},
          id="multi-line-import"),
        P({LIB: "struct C { test_hook: bool }\nfn f(c: C) -> bool { c.test_hook }"}, id="field"),
        P({LIB: 'fn f() { tracing::info!(r#"call test_hook_var("SPACE_EXAMPLE_LIMIT")"#); }'},
          id="string-mention"),
        P({LIB: f"// {READ}\n/* a /* b */ {READ} */"}, id="comments"),
        P({LIB: f"let q = '\"'; // {READ}"}, id="char-quote"),
        # Test files may read anything.
        P({"crates/libs/example/src/tests.rs": f"let n = {READ};"}, id="tests-rs"),
        P({"crates/libs/example/src/store_tests.rs": f"let n = {READ};"}, id="suffix-tests"),
        P({"crates/libs/example/tests/it.rs": f"let n = {READ};"}, id="tests-dir"),
        P({"crates/libs/example/benches/b.rs": f"let n = {READ};"}, id="benches"),
        # Constants: the narrowest defining scope wins, and other crates don't
        # count.
        P({
            "crates/libs/example/src/a.rs": 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";\n'
            "let n = std::env::var(ENV);",
            "crates/libs/example/src/b.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";\n'
            "let n = test_hook_var(ENV);",
        }, id="per-file-constant"),
        P({LIB: 'impl A { const ENV: &str = "SPACE_EXAMPLE_LIMIT"; fn f() { std::env::var(Self::ENV); } }\n'
               'impl B { const ENV: &str = "SPACE_EXAMPLE_TICK_SECS"; fn g() { test_hook_var(Self::ENV); } }'},
          id="same-file-constant-twice"),
        P({
            "crates/libs/one/Cargo.toml": "",
            "crates/libs/one/src/lib.rs": "let n = test_hook_var(gc::ENV);",
            "crates/libs/one/src/gc.rs": 'const ENV: &str = "SPACE_EXAMPLE_TICK_SECS";',
            "crates/libs/two/Cargo.toml": "",
            "crates/libs/two/src/lib.rs": 'const ENV: &str = "SPACE_EXAMPLE_LIMIT";',
        }, id="other-crate-constant"),
        # A `for<'a>` bound doesn't make an inherent impl a trait impl.
        P({
            "crates/libs/one/Cargo.toml": "",
            "crates/libs/one/src/lib.rs": "impl<F> W<F> where F: for<'a> Fn(&'a str) {\n"
            "    fn test_raw(&self, var: &str) -> Option<String> { test_hook_var(var) }\n}",
            "crates/libs/two/Cargo.toml": "",
            "crates/libs/two/src/lib.rs": 'let n = x.test_raw("SPACE_EXAMPLE_LIMIT");',
        }, id="hrtb-inherent-impl"),
        # A crate-private wrapper's name and an alias mean nothing in another crate.
        P({
            "crates/libs/one/Cargo.toml": "",
            "crates/libs/one/src/lib.rs": "use pplx_space_common_core::env::test_hook_var as var;\n" + WRAPPER,
            "crates/libs/two/Cargo.toml": "",
            "crates/libs/two/src/lib.rs": "fn read(name: &str) { std::env::var(name) }\n"
            'let n = std::env::var("SPACE_EXAMPLE_LIMIT");\n'
            "struct Probe; impl Probe { fn test_raw(&self, url: &str) {} }\n"
            "fn check(url: &str) { Probe.test_raw(url) }",
        }, id="other-crate-readers"),
    ],
)
def test_reads_that_are_not_violations_pass(tmp_path: Path, files: dict[str, str]) -> None:
    assert guard.violations(tree(tmp_path, files)) == []
