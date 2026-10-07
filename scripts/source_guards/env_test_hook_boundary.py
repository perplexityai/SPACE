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
over Rust files under `crates/`, skipping `target/` and test and bench files
(`tests.rs`, `*_tests.rs`, `tests/`, `benches/`).

Test-only code inside a production file (`#[cfg(test)]` items) is checked like
the rest. Telling it apart takes a Rust parser; a textual guess that skips too
much hides release code, which is the one failure this guard can't afford. A
test helper that trips the guard belongs in a test file anyway.

A *reader* is `test_hook`, `test_hook_var`, an import alias of either in the
same file, or a function named `test_*` (in the same crate, or anywhere if it's
`pub`, a trait method, or a trait impl's method)
that passes one of its own string parameters, as a whole argument, to a reader
(`EnvConfig::test_raw`, for example). Two rules:

1. No reader call may name a documented variable, as a string literal or as a
   `&str` constant. A constant resolves in the narrowest scope that defines it:
   its `impl` block for `Self::`, then the reading file, then its crate. A name
   with several values there stands for all of them, so the check fails closed.
2. A function that passes one of its own string parameters, as a whole
   argument, to a reader must be named `test_*`. Otherwise its callers would
   hide reads from rule 1.

Not covered, by design, because a textual check can't follow them reliably:
names built at run time (`format!`, `concat!`), constants defined by other
constants or in another crate, names reached through fields or indexing, hooks
passed as function values, and names forwarded through locals, closures,
macros, type aliases, pattern parameters or generic `AsRef<str>` parameters.
A constant also resolves only in the narrowest scope that defines its name, so
a same-named constant there (a function-local one, or a test fixture in the
same file) hides one imported from elsewhere. Aliases defined in another file,
and aliases of `test_*` wrappers, aren't followed. Rule 2 flags any string
parameter forwarded to a reader, including one a multi-argument wrapper uses as
a value rather than a name, and one a closure parameter of the same name
shadows. A test module declared with `#[cfg(test)] mod x;` is
scanned unless its file is named like a test file. The guard's job is the common
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
NAME_ARG = re.compile(
    r'"(SPACE_[A-Z0-9_]+)"|(?<![A-Za-z0-9_"])((?:[A-Za-z_][A-Za-z0-9_]*::)*)([A-Z][A-Z0-9_]*)\b'
)
# A parameter passed whole: `name`, `&name`, `*name`, `name?`, `name.trim()`,
# `&*name.as_str()`: references, derefs, `?` and argument-free method calls.
WHOLE_ARGUMENT = re.compile(
    r"[&*\s]*([a-z_][a-z0-9_]*)(?:\??\.[a-z_][a-z0-9_]*\(\s*\))*\??"
)
PARAMETER = re.compile(r"(?:mut\s+)?([a-z_][a-z0-9_]*)\s*:(.*)", re.S)
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
        text = code_only(path.read_text())
        found.append((relative, crate_of(root, relative), text))
    return found


class Function:
    def __init__(
        self, name: str, parameters: set[str], start: int, end: int, public: bool
    ) -> None:
        self.name, self.parameters, self.start, self.end = name, parameters, start, end
        self.public = public


TRAIT = re.compile(r"\btrait\s+[A-Za-z_]\w*[^;{]*\{")
# `impl Trait for Type`; a `for<'a>` bound in an inherent impl doesn't count.
IMPL_FOR = re.compile(r"\bimpl\b[^;{]*\bfor\s+(?!<)[^;{]*\{")


def generics_end(text: str, start: int) -> int:
    """The index just past the `>` closing the generics opened at `start`.

    The `>` of `->` (`F: Fn() -> u8`) doesn't close anything.
    """
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "<":
            depth += 1
        elif text[index] == ">" and text[index - 1] != "-":
            depth -= 1
            if depth == 0:
                return index + 1
    return len(text)


def body_start(text: str, index: int) -> int | None:
    """The `{` opening a function body after its parameters, or `None` for a
    declaration. A `;` inside brackets (`-> [u8; 4]`) doesn't end it."""
    depth = 0
    for position in range(index, len(text)):
        char = text[position]
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        elif depth == 0 and char == ";":
            return None
        elif depth == 0 and char == "{":
            return position
    return None


def functions(text: str) -> list[Function]:
    found = []
    # Methods of a trait, or of a trait impl, are callable wherever the trait
    # is, so they count as public like a `pub fn`.
    traits = [
        (block.end() - 1, matching(text, block.end() - 1, "{", "}"))
        for pattern in (TRAIT, IMPL_FOR)
        for block in pattern.finditer(text)
    ]
    for match in FN_DEF.finditer(text):
        index = match.end()
        if text[index:].lstrip().startswith("<"):
            index = generics_end(text, text.index("<", index))
        open_paren = text.find("(", index)
        if open_paren == -1:
            continue
        close_paren = matching(text, open_paren, "(", ")")
        brace = body_start(text, close_paren)
        if brace is None:
            continue  # a declaration without a body
        # Only string parameters can carry a variable name; `env: &dyn
        # Environment` passed along to `test_hook` is not forwarding.
        parameters = set()
        for parameter in top_level_arguments(text[open_paren + 1 : close_paren - 1]):
            typed = PARAMETER.fullmatch(re.sub(r"#\[[^\]]*\]\s*", "", parameter))
            if typed and re.search(r"\b(?:str|String)\b", typed.group(2)):
                parameters.add(typed.group(1))
        prefix = text[max(0, match.start() - 40) : match.start()]
        public = bool(
            re.search(r'\bpub\s+(?:(?:async|const|unsafe|extern(?:\s+"[^"]*")?)\s+)*$', prefix)
        ) or any(start < match.start() < end for start, end in traits)
        found.append(
            Function(match.group(1), parameters, brace, matching(text, brace, "{", "}"), public)
        )
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
    for index, char in enumerate(arguments):
        if char in "([{<":
            depth += 1
        elif char in ")]}" or (char == ">" and arguments[index - 1 : index] != "-"):
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


PUBLIC = pathlib.Path("<pub>")


def find_readers(files) -> dict[pathlib.Path, set[str]]:
    """`test_*` wrappers per crate; `pub` ones also count in every crate.

    A private or `pub(crate)` wrapper's name only means a reader where it can
    be called, so another crate's unrelated `test_raw` doesn't become one.
    """
    readers: dict[pathlib.Path, set[str]] = {crate: set() for _, crate, _ in files}
    readers[PUBLIC] = set()
    parsed = [(crate, text, functions(text)) for _, crate, text in files]
    while True:
        changed = False
        for crate, text, defined in parsed:
            for call, arguments in reader_calls(text, file_readers(text, readers[crate] | readers[PUBLIC])):
                owner = innermost(defined, call.start())
                if not (
                    owner
                    and owner.name.startswith("test_")
                    and forwarded(arguments, owner)
                ):
                    continue
                # Checked separately, so a private wrapper found first doesn't
                # stop a same-named `pub` one counting in other crates.
                if owner.name not in readers[crate]:
                    readers[crate].add(owner.name)
                    changed = True
                if owner.public and owner.name not in readers[PUBLIC]:
                    readers[PUBLIC].add(owner.name)
                    changed = True
        if not changed:
            return readers


# An `impl` block, not `-> impl Trait` or `x: impl Trait` in a signature.
IMPL = re.compile(r"(?:^|[;{}])\s*(?:unsafe\s+)?impl\b[^;{]*\{", re.M)


def enclosing_impl_constants(text: str, impls, offset: int) -> dict[str, set[str]]:
    """The constants of the innermost `impl` block around `offset`."""
    around = [(start, end) for start, end in impls if start <= offset < end]
    if not around:
        return {}
    start, end = min(around, key=lambda span: span[1] - span[0])
    return string_constants(text[start:end])


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
        impls = [(m.end() - 1, matching(text, m.end() - 1, "{", "}")) for m in IMPL.finditer(text)]
        visible = crate_readers[crate] | crate_readers[PUBLIC]
        for call, arguments in reader_calls(text, file_readers(text, visible)):
            line = f"{relative}:{text.count(chr(10), 0, call.start()) + 1}"
            reader = call.group(1)
            names: set[str] = set()
            for literal, path, constant in NAME_ARG.findall(arguments):
                if literal:
                    names.add(literal)
                    continue
                # A name stands for every value it has in the narrowest scope
                # that defines it. Wider scopes aren't merged in: across a
                # crate, names like `ENV_INTERVAL_SECS` legitimately mean a
                # hook in one file and a documented setting in another.
                scopes = []
                if path == "Self::":
                    scopes.append(enclosing_impl_constants(text, impls, call.start()))
                if path in ("", "Self::"):
                    scopes.append(local)
                scopes.append(by_crate.get(crate, {}))
                names |= next((scope[constant] for scope in scopes if constant in scope), set())
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
