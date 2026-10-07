#!/usr/bin/env python3
"""Keep documented operator settings off the test-hook readers.

`docs/configuration.md` lists every `SPACE_*` variable a release binary reads.
Test hooks are the opposite: `pplx_space_common_core::env::test_hook` returns
`None` unless the `test-hooks` feature is compiled in, which release images
never do, so a variable read through one is inert in production. A documented
variable read through a hook is a setting operators are told they can change
and can't.

Unit tests rarely catch that regression. Crates that read hooks generally
enable `test-hooks` for their own tests, so in a test build a hook reads the
environment exactly like a real read. This guard checks the source instead.

Two rules, over non-test Rust sources under `crates/`:

1. A *reader* is `test_hook`, `test_hook_var`, or a function named `test_*`
   that forwards a variable name into a reader (`EnvConfig::test_raw`, for
   example). Every argument of a reader call that names a variable, as a
   string literal or a `&str` constant, must not be a documented variable.
2. A reader call that names no variable forwards one from elsewhere, so it must
   sit inside a function named `test_*`. That keeps every forwarding wrapper
   visible as a reader under rule 1: a closure, macro, loop or differently
   named helper that forwards a hook read fails here instead of hiding it.
   Passing `test_hook` or `test_hook_var` as a value fails for the same reason.

Constants resolve in the reading file first, then across the tree; a
path-qualified constant other than `Self::` resolves across the tree. Names
built with `concat!` or held in constants defined by other constants aren't
resolved.

Run standalone (`python scripts/source_guards/env_test_hook_boundary.py`).
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCS = pathlib.Path("docs/configuration.md")
PRIMITIVES = frozenset({"test_hook", "test_hook_var"})

DOCUMENTED = re.compile(r"`(SPACE_[A-Z0-9_]+)`")
FN_DEF = re.compile(r"\bfn\s+([a-z_][a-z0-9_]*)\b")
# A call, with an optional turbofish: `name(`, `name::<T>(`.
CALL = re.compile(r"\b([a-z_][a-z0-9_]*)\s*(?:::\s*<[^()]*?>\s*)?\(")
PRIMITIVE_USE = re.compile(r"\b(test_hook_var|test_hook)\b")
# A variable-name candidate in a call's arguments: a string literal (plain or
# raw), or an upper-case constant, possibly path-qualified (`Self::ENV`).
STRING = r'r?#*"(SPACE_[A-Z0-9_]+)"#*'
NAME_ARG = re.compile(
    STRING + r'|(?<![A-Za-z0-9_"])((?:[A-Za-z_][A-Za-z0-9_]*::)*)([A-Z][A-Z0-9_]+)\b'
)
CHAR_LITERAL = re.compile(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'\n])'")
STR_CONST = re.compile(
    r"\b(?:const|static)\s+([A-Z][A-Z0-9_]*)\s*:\s*&(?:'static\s+)?str\s*=\s*r?#*\"([^\"]*)\""
)


def documented(root: pathlib.Path) -> set[str]:
    return set(DOCUMENTED.findall((root / DOCS).read_text()))


def is_test_source(relative: pathlib.Path) -> bool:
    return (
        relative.name == "tests.rs"
        or relative.name.endswith("_tests.rs")
        or "tests" in relative.parts[:-1]
        or "benches" in relative.parts[:-1]
    )


def strip_comments(text: str) -> str:
    """Blank out `//` and `/* */` comments, leaving string literals intact.

    Comment characters become spaces so offsets and line numbers still match
    the file. A char literal is skipped whole so `'"'` doesn't open a string;
    a lifetime (`'a`) has no closing quote and is left alone.
    """
    out = list(text)
    index, length = 0, len(text)
    while index < length:
        char = text[index]
        literal = CHAR_LITERAL.match(text, index) if char == "'" else None
        if literal:
            index = literal.end()
        elif char == '"' or (
            char == "r"
            and not re.match(r"[A-Za-z0-9_]", text[index - 1 : index] or " ")
            and re.match(r'r#*"', text[index:])
        ):
            hashes = 0
            if char == "r":
                index += 1
                while text[index] == "#":
                    hashes += 1
                    index += 1
            closing = '"' + "#" * hashes
            index += 1
            while index < length and not text.startswith(closing, index):
                index += 2 if text[index] == "\\" and not hashes else 1
            index += len(closing)
        elif text.startswith("//", index):
            end = text.find("\n", index)
            end = length if end == -1 else end
            out[index:end] = " " * (end - index)
            index = end
        elif text.startswith("/*", index):
            end = text.find("*/", index + 2)
            end = length if end == -1 else end + 2
            out[index:end] = [c if c == "\n" else " " for c in text[index:end]]
            index = end
        else:
            index += 1
    return "".join(out)


def sources(root: pathlib.Path) -> list[tuple[pathlib.Path, str]]:
    found = []
    for path in sorted(root.glob("crates/**/*.rs")):
        relative = path.relative_to(root)
        if "target" in relative.parts or is_test_source(relative):
            continue
        found.append((relative, strip_comments(path.read_text())))
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


def functions(text: str) -> list[tuple[str, int, int]]:
    """Each `fn` as (name, start, end), its body running to the next `fn`."""
    starts = [(m.group(1), m.start()) for m in FN_DEF.finditer(text)]
    return [
        (name, start, starts[i + 1][1] if i + 1 < len(starts) else len(text))
        for i, (name, start) in enumerate(starts)
    ]


def enclosing_function(text: str, offset: int) -> str | None:
    name = None
    for match in FN_DEF.finditer(text, 0, offset):
        name = match.group(1)
    return name


def calls(text: str, readers: set[str]):
    for match in CALL.finditer(text):
        if match.group(1) not in readers:
            continue
        if re.search(r"\bfn\s+$", text[max(0, match.start() - 8) : match.start()]):
            continue
        yield match, call_arguments(text, match.end() - 1)


def forwards(arguments: str) -> bool:
    """Whether a reader call names no variable itself."""
    return NAME_ARG.search(arguments) is None


def find_readers(files: list[tuple[pathlib.Path, str]]) -> set[str]:
    # Only `test_*` functions join: rule 2 already fails any other forwarder.
    readers = set(PRIMITIVES)
    while True:
        found = {
            name
            for _, text in files
            for name, start, end in functions(text)
            if name.startswith("test_")
            and any(forwards(arguments) for _, arguments in calls(text[start:end], readers))
        }
        if found <= readers:
            return readers
        readers |= found


def string_constants(text: str) -> dict[str, set[str]]:
    constants: dict[str, set[str]] = {}
    for name, value in STR_CONST.findall(text):
        constants.setdefault(name, set()).add(value)
    return constants


def violations(root: pathlib.Path) -> list[str]:
    settings = documented(root)
    files = sources(root)
    readers = find_readers(files)
    # Constant names repeat across crates (`ENV_INTERVAL_SECS` names a
    # different variable in each eviction loop), so an unqualified name
    # resolves in its own file before the rest of the tree.
    everywhere: dict[str, set[str]] = {}
    for _, text in files:
        for name, values in string_constants(text).items():
            everywhere.setdefault(name, set()).update(values)

    failures = []
    for relative, text in files:
        local = string_constants(text)

        def location(offset: int) -> str:
            return f"{relative}:{text.count(chr(10), 0, offset) + 1}"

        for call, arguments in calls(text, readers):
            reader = call.group(1)
            names: set[str] = set()
            for literal, path, constant in NAME_ARG.findall(arguments):
                if literal:
                    names.add(literal)
                elif path in ("", "Self::") and constant in local:
                    names |= local[constant]
                else:
                    names |= everywhere.get(constant, set())
            for name in sorted(names & settings):
                failures.append(
                    f"{location(call.start())}: {name} is documented in {DOCS} but "
                    f"read through test hook `{reader}`, which release builds ignore; "
                    "read it from the environment or drop it from the docs"
                )
            if not names:
                owner = enclosing_function(text, call.start())
                if owner is None or not owner.startswith("test_"):
                    failures.append(
                        f"{location(call.start())}: test hook `{reader}` is called with "
                        f"a forwarded variable name inside `{owner or 'no function'}`; "
                        "only a function named `test_*` may forward one, so its own "
                        "callers stay checked"
                    )

        for use in PRIMITIVE_USE.finditer(text):
            after = text[use.end() :].lstrip()
            before = text[max(0, use.start() - 8) : use.start()]
            line_start = text.rfind("\n", 0, use.start()) + 1
            in_use = re.match(r"\s*(?:pub(?:\([^)]*\))?\s+)?use\b", text[line_start:])
            if after.startswith(("(", "::")) or re.search(r"\bfn\s+$", before) or in_use:
                continue
            failures.append(
                f"{location(use.start())}: test hook `{use.group(1)}` is used as a "
                "value; call it with the variable name so the name stays checked"
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
