#!/usr/bin/env python3
"""Keep documented operator settings off the test-hook readers.

`docs/configuration.md` lists every `SPACE_*` variable a release binary reads.
Test hooks are the opposite: `pplx_space_common_core::env::test_hook` returns
`None` unless the `test-hooks` feature is compiled in, which release images
never do, so a variable read through one is inert in production. A documented
variable read through a hook is a setting operators are told they can change
and can't.

Unit tests can't catch that regression. Every crate that reads hooks enables
`test-hooks` through its dev-dependencies, so in a test build a hook reads the
environment exactly like a real read. This guard checks the source instead: it
resolves the variable name of every test-hook read in non-test Rust code, from
a string literal or a `&str` constant, and fails if a documented name is among
them.

A read counts as a test-hook read when it calls `test_hook`, `test_hook_var`,
or any function or method named `test_*`, which is how every wrapper over them
is named (`EnvConfig::test_duration_secs`, for example). A read whose name
isn't a literal or a known constant, such as a wrapper forwarding its own
parameter, isn't resolved; the wrapper's callers are checked instead.

Run standalone (`python scripts/source_guards/env_test_hook_boundary.py`).
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCS = pathlib.Path("docs/configuration.md")

DOCUMENTED = re.compile(r"`(SPACE_[A-Z0-9_]+)`")
# A call to a test-hook reader: `test_hook(`, `test_hook_var(`, or `test_*(`,
# as a free function, a path (`env::test_hook(`) or a method (`.test_raw(`).
HOOK_CALL = re.compile(r"\btest_[a-z0-9_]+\s*\(")
# The first variable-name candidate in a call's arguments: a SPACE_ string
# literal, or an upper-case constant, possibly path-qualified (`Self::ENV`).
NAME_ARG = re.compile(
    r'"(SPACE_[A-Z0-9_]+)"|(?<![A-Za-z0-9_"])((?:[A-Za-z_][A-Za-z0-9_]*::)*)([A-Z][A-Z0-9_]+)\b'
)
STR_CONST = re.compile(
    r'\bconst\s+([A-Z][A-Z0-9_]*)\s*:\s*&(?:\'static\s+)?str\s*=\s*"([^"]*)"'
)
LINE_COMMENT = re.compile(r"//[^\n]*")


def documented(root: pathlib.Path) -> set[str]:
    return set(DOCUMENTED.findall((root / DOCS).read_text()))


def is_test_source(relative: pathlib.Path) -> bool:
    return (
        relative.name == "tests.rs"
        or relative.name.endswith("_tests.rs")
        or "tests" in relative.parts[:-1]
        or "benches" in relative.parts[:-1]
    )


def sources(root: pathlib.Path) -> list[tuple[pathlib.Path, str]]:
    found = []
    for path in sorted(root.glob("crates/**/*.rs")):
        relative = path.relative_to(root)
        if "target" in relative.parts or is_test_source(relative):
            continue
        found.append((relative, LINE_COMMENT.sub("", path.read_text())))
    return found


def call_arguments(text: str, open_paren: int) -> str:
    """The text between `(` at `open_paren` and its matching `)`."""
    depth = 0
    for index in range(open_paren, len(text)):
        if text[index] == "(":
            depth += 1
        elif text[index] == ")":
            depth -= 1
            if depth == 0:
                return text[open_paren + 1 : index]
    return text[open_paren + 1 :]


def string_constants(text: str) -> dict[str, set[str]]:
    constants: dict[str, set[str]] = {}
    for name, value in STR_CONST.findall(text):
        constants.setdefault(name, set()).add(value)
    return constants


def violations(root: pathlib.Path) -> list[str]:
    settings = documented(root)
    files = sources(root)
    # Constant names repeat across crates (`ENV_INTERVAL_SECS` names a
    # different variable in each eviction loop), so a name resolves in its own
    # file first, and through other files only when the file doesn't define it.
    everywhere: dict[str, set[str]] = {}
    for _, text in files:
        for name, values in string_constants(text).items():
            everywhere.setdefault(name, set()).update(values)

    failures = []
    for relative, text in files:
        local = string_constants(text)
        for call in HOOK_CALL.finditer(text):
            # A definition (`fn test_raw(`) isn't a read.
            if re.search(r"\bfn\s+$", text[max(0, call.start() - 8) : call.start()]):
                continue
            reader = call.group(0).rstrip("( \t\n")
            arguments = call_arguments(text, call.end() - 1)
            candidate = NAME_ARG.search(arguments)
            if candidate is None:
                continue
            literal, path, constant = candidate.groups()
            if literal:
                names = {literal}
            elif constant in local or path == "Self::":
                names = local.get(constant, set())
            else:
                names = everywhere.get(constant, set())
            for name in sorted(names & settings):
                line = text.count("\n", 0, call.start()) + 1
                failures.append(
                    f"{relative}:{line}: {name} is documented in {DOCS} but read "
                    f"through test hook `{reader}`, which release builds ignore; "
                    "read it from the environment or drop it from the docs"
                )
    return failures


def main() -> int:
    failures = violations(REPO_ROOT)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print("Documented settings are read outside test hooks: passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
