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
environment exactly like a real read. This guard checks the source instead,
over Rust files under `crates/`, skipping `target/`, test and bench files, and
items under a `#[cfg(...)]` that mentions `test`.

A *reader* is `test_hook`, `test_hook_var`, an import alias of either in the
same file, or a function named `test_*` in the same crate that passes one of
its own string parameters, as a whole argument, to a reader
(`EnvConfig::test_raw`, for example). Two rules:

1. No reader call may name a documented variable, as a string literal or as a
   `&str` constant. A constant resolves in the reading file first, then in the
   rest of its crate; a name with more than one value in that scope is left
   unresolved rather than guessed.
2. A function that passes one of its own string parameters, as a whole
   argument, to a reader must be named `test_*`. Otherwise its callers would
   hide reads from rule 1.

Not covered, by design, because a textual check can't follow them reliably:
names built at run time (`format!`, `concat!`), constants defined by other
constants or in another crate, and names forwarded through locals, closures,
macros or generic `AsRef<str>` parameters. The guard's job is the common
regression, a documented setting switched to a test-hook reader; review covers
the rest.

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
SPACE_NAME = re.compile(r"SPACE_[A-Z0-9_]+")
CHAR_LITERAL = re.compile(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'\n])'")
RAW_STRING = re.compile(r'b?r(#*)"')
FN_DEF = re.compile(r"\bfn\s+([a-z_][a-z0-9_]*)")
# A call, with an optional turbofish: `name(`, `name::<T>(`.
CALL = re.compile(r"\b([a-z_][a-z0-9_]*)\s*(?:::\s*<[^()]*?>\s*)?\(")
ALIAS = re.compile(r"\b(test_hook_var|test_hook)\s+as\s+([a-z_][a-z0-9_]*)")
# `#[cfg(test)]`, `#[cfg(all(test, ...))]` and the like.
TEST_CFG = re.compile(r"#\[cfg\([^\]]*\btest\b[^\]]*\)\]")
NAME_ARG = re.compile(
    r'"(SPACE_[A-Z0-9_]+)"|(?<![A-Za-z0-9_"])((?:[A-Za-z_][A-Za-z0-9_]*::)*)([A-Z][A-Z0-9_]+)\b'
)
# A parameter passed whole: `name`, `&name`, `&*name`, `name.as_str()`, ...
WHOLE_ARGUMENT = re.compile(
    r"&?\*?\s*([a-z_][a-z0-9_]*)(?:\.(?:as_str|as_ref|to_owned|to_string|clone)\(\))?"
)
STR_PARAMETER = re.compile(
    r"(?:^|,)\s*(?:mut\s+)?([a-z_][a-z0-9_]*)\s*:\s*(?:&\s*(?:'\w+\s+)?(?:mut\s+)?)?(?:str|String)\b"
)
STR_CONST = re.compile(
    r"\b(?:const|static)\s+([A-Z][A-Z0-9_]*)\s*:\s*&\s*(?:'static\s+)?str\s*=\s*r?#*\"(SPACE_[A-Z0-9_]+)\""
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


def code_only(text: str) -> str:
    """Blank comments and string contents, keeping offsets and line numbers.

    A string that is exactly a `SPACE_*` name is kept, since that's how a read
    names its variable; any other string becomes `""` padded with spaces, so
    text inside it can't look like code. Char literals are skipped whole so
    `'"'` doesn't open a string.
    """
    out = list(text)
    index, length = 0, len(text)

    def blank(start: int, end: int) -> None:
        out[start:end] = [c if c == "\n" else " " for c in text[start:end]]

    while index < length:
        char = text[index]
        raw = RAW_STRING.match(text, index) if char in "br" else None
        if char == "'" and (literal := CHAR_LITERAL.match(text, index)):
            # Blanked so a `'}'` can't end a brace match early.
            blank(index + 1, literal.end() - 1)
            index = literal.end()
        elif raw or char == '"':
            if raw:
                closing = '"' + raw.group(1)
                body = raw.end()
            else:
                closing, body = '"', index + 1
            end = body
            while end < length and not text.startswith(closing, end):
                end += 2 if text[end] == "\\" and not raw else 1
            if not SPACE_NAME.fullmatch(text[body:end]):
                blank(body, end)
            index = end + len(closing)
        elif text.startswith("//", index):
            end = text.find("\n", index)
            end = length if end == -1 else end
            blank(index, end)
            index = end
        elif text.startswith("/*", index):
            depth, end = 1, index + 2
            while end < length and depth:
                if text.startswith("/*", end):
                    depth, end = depth + 1, end + 2
                elif text.startswith("*/", end):
                    depth, end = depth - 1, end + 2
                else:
                    end += 1
            blank(index, end)
            index = end
        else:
            index += 1
    return "".join(out)


def matching(text: str, start: int, opener: str, closer: str) -> int:
    """The index just past the `closer` matching the `opener` at `start`."""
    depth = 0
    for index in range(start, len(text)):
        if text[index] == opener:
            depth += 1
        elif text[index] == closer:
            depth -= 1
            if depth == 0:
                return index + 1
    return len(text)


def without_test_items(text: str) -> str:
    """Blank every item under a test `cfg`, through its body or `;`."""
    for cfg in reversed(list(TEST_CFG.finditer(text))):
        brace, semicolon = text.find("{", cfg.end()), text.find(";", cfg.end())
        if brace == -1 and semicolon == -1:
            continue
        if brace == -1 or (semicolon != -1 and semicolon < brace):
            end = semicolon + 1
        else:
            end = matching(text, brace, "{", "}")
        text = text[: cfg.start()] + re.sub(r"[^\n]", " ", text[cfg.start() : end]) + text[end:]
    return text


def crate_of(root: pathlib.Path, relative: pathlib.Path) -> pathlib.Path:
    for parent in (root / relative).parents:
        if (parent / "Cargo.toml").exists() or parent == root:
            return parent
    return root


def sources(root: pathlib.Path) -> list[tuple[pathlib.Path, pathlib.Path, str]]:
    found = []
    for path in sorted(root.glob("crates/**/*.rs")):
        relative = path.relative_to(root)
        if "target" in relative.parts or is_test_source(relative):
            continue
        text = without_test_items(code_only(path.read_text()))
        found.append((relative, crate_of(root, relative), text))
    return found


class Function:
    def __init__(self, name: str, parameters: set[str], start: int, end: int) -> None:
        self.name, self.parameters, self.start, self.end = name, parameters, start, end


def functions(text: str) -> list[Function]:
    found = []
    for match in FN_DEF.finditer(text):
        index = match.end()
        if text[index:].lstrip().startswith("<"):
            index = matching(text, text.index("<", index), "<", ">")
        open_paren = text.find("(", index)
        if open_paren == -1:
            continue
        close_paren = matching(text, open_paren, "(", ")")
        brace, semicolon = text.find("{", close_paren), text.find(";", close_paren)
        if brace == -1 or (semicolon != -1 and semicolon < brace):
            continue  # a declaration without a body
        signature = text[open_paren + 1 : close_paren - 1]
        # Only string parameters can carry a variable name; `env: &dyn
        # Environment` passed along to `test_hook` is not forwarding.
        parameters = set(STR_PARAMETER.findall(signature))
        found.append(Function(match.group(1), parameters, brace, matching(text, brace, "{", "}")))
    return found


def innermost(functions_in_file: list[Function], offset: int) -> Function | None:
    enclosing = [f for f in functions_in_file if f.start <= offset < f.end]
    return min(enclosing, key=lambda f: f.end - f.start) if enclosing else None


def reader_calls(text: str, readers: set[str]):
    # A definition (`fn test_raw(var: &str)`) also matches, but its signature
    # holds no name-shaped argument and no forwarded parameter, so it reports
    # nothing.
    for match in CALL.finditer(text):
        if match.group(1) in readers:
            yield match, text[match.end() : matching(text, match.end() - 1, "(", ")") - 1]


def top_level_arguments(arguments: str) -> list[str]:
    parts, depth, current = [], 0, []
    for char in arguments:
        if char in "([{<":
            depth += 1
        elif char in ")]}>":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return [part.strip() for part in parts]


def forwarded(arguments: str, function: Function | None) -> set[str]:
    """The enclosing function's own string parameters passed as a whole argument."""
    if function is None:
        return set()
    names = set()
    for argument in top_level_arguments(arguments):
        whole = WHOLE_ARGUMENT.fullmatch(argument)
        if whole and whole.group(1) in function.parameters:
            names.add(whole.group(1))
    return names


def file_readers(text: str, crate_readers: set[str]) -> set[str]:
    return set(PRIMITIVES) | crate_readers | {alias for _, alias in ALIAS.findall(text)}


def find_readers(files) -> dict[pathlib.Path, set[str]]:
    """Each crate's `test_*` wrappers, which count as readers only there."""
    readers: dict[pathlib.Path, set[str]] = {crate: set() for _, crate, _ in files}
    while True:
        changed = False
        for _, crate, text in files:
            defined = functions(text)
            for call, arguments in reader_calls(text, file_readers(text, readers[crate])):
                owner = innermost(defined, call.start())
                if (
                    owner
                    and owner.name.startswith("test_")
                    and owner.name not in readers[crate]
                    and forwarded(arguments, owner)
                ):
                    readers[crate].add(owner.name)
                    changed = True
        if not changed:
            return readers


def string_constants(text: str) -> dict[str, set[str]]:
    constants: dict[str, set[str]] = {}
    for name, value in STR_CONST.findall(text):
        constants.setdefault(name, set()).add(value)
    return constants


def violations(root: pathlib.Path) -> list[str]:
    settings = documented(root)
    files = sources(root)
    crate_readers = find_readers(files)
    by_crate: dict[pathlib.Path, dict[str, set[str]]] = {}
    for _, crate, text in files:
        for name, values in string_constants(text).items():
            by_crate.setdefault(crate, {}).setdefault(name, set()).update(values)

    failures = []
    for relative, crate, text in files:
        local = string_constants(text)
        defined = functions(text)
        for call, arguments in reader_calls(text, file_readers(text, crate_readers[crate])):
            line = f"{relative}:{text.count(chr(10), 0, call.start()) + 1}"
            reader = call.group(1)
            names: set[str] = set()
            for literal, path, constant in NAME_ARG.findall(arguments):
                if literal:
                    names.add(literal)
                elif path in ("", "Self::") and constant in local:
                    if len(local[constant]) == 1:
                        names |= local[constant]
                elif len(values := by_crate.get(crate, {}).get(constant, set())) == 1:
                    names |= values
            for name in sorted(names & settings):
                failures.append(
                    f"{line}: {name} is documented in {DOCS} but read through test "
                    f"hook `{reader}`, which release builds ignore; read it from the "
                    "environment or drop it from the docs"
                )
            owner = innermost(defined, call.start())
            if owner and not owner.name.startswith("test_"):
                for parameter in sorted(forwarded(arguments, owner)):
                    failures.append(
                        f"{line}: `{owner.name}` passes its parameter `{parameter}` to "
                        f"test hook `{reader}`; name it `test_*` so its callers are "
                        "checked as test-hook reads"
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
