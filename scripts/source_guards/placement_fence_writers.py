#!/usr/bin/env python3
"""Every production writer of a non-NULL `sandboxes.active_node_id` must consult
the termination fence.

The fence (`pplx_space_common_svc::sql::node_placement_fence`) takes
`FOR KEY SHARE` on the `nodes` row and refuses a committed `drain_phase` of
`drained`/`terminating`, so a placement cannot be committed onto a node the
provider is about to destroy. Its one structural weakness is that it is an
*enumeration* of call sites rather than a schema constraint: a writer of that
column added later is silently unfenced, and the failure mode is a user's sandbox
destroyed with its instance.

That is not hypothetical. The first list of writers this fence was built against
had four; there were six. The two it missed were found by reading a diff's own prose back
against the source, which is not a repeatable process. This is.

**What it enforces.** For each SQL statement in production Rust that assigns a
non-NULL value to `sandboxes.active_node_id`, the enclosing `fn` must also mention
the fence. Function scope, not file scope: a file that fences one statement and
not its neighbour is the gap most worth catching, and a file-level check passes it.

**What it deliberately does not enforce**, stated so the guard is not read as
stronger than it is:

* **Ordering.** It does not check that the fence is called before the write. It
  cannot: `update_and_fetch_post_wake_sandbox` builds its SQL into a `let` above
  the fence call and executes it below, so textual order is not execution order,
  and a position rule fires on that ordinary idiom. Requiring a *call* rather than
  a mention (see [`FENCE_CALL`]) closes the case that actually bit — a doc comment
  naming the type marking an unfenced statement as fenced.
* **That the verdict is honoured.** A caller could fence and ignore the answer.
* **One fence per write.** A function with two `active_node_id` writes and one
  fence call passes on both. No such function exists today — all six sites are in
  six distinct functions — and closing it needs the ordering it cannot have.
* **SQL assembled outside a literal.** A helper returning `"active_node_id = $1"`
  as a fragment, interpolated by a caller's `format!`, is invisible: the scanner
  reads string literals, and in that shape no literal contains both the statement
  and the assignment. This is the one gap worth knowing about, because the repo
  does build SQL from shared fragments elsewhere (`sandbox_residency`). A writer
  in that shape needs the fence by review, not by this guard.

Those three are the job of the two-transaction interleaving tests in
`crates/libs/cluster/controller/server/src/node_drain/store/placement_fence_tests.rs`
and the per-writer tests beside each call site. This answers exactly one question:
did somebody add a writer without thinking about the fence at all.

**Why it reads string literals rather than lines.** `active_node_id = $1` means
opposite things either side of a clause boundary — an assignment in a `SET`, a
comparison in a `WHERE` — and Rust's own `let active_node_id = …` is neither. The
only way to tell without a SQL parser is to know which clause is open, which is
only meaningful inside the string literal holding the statement. An earlier
line-oriented version of this guard got that wrong in both directions: it first
reported 40 predicate reads as writes, and then, "fixed", found *zero* sites and
passed vacuously. Hence [`test_guard_finds_every_known_writer`], which pins the
site count against the real tree so a future refactor cannot quietly mute this.

[`KNOWN_UNFENCED`] is the escape hatch and doubles as the record of the writers
that are knowingly unfenced. Adding to it should be a review conversation, which
is why each entry carries its reason and tracking issue inline.
"""

from __future__ import annotations

import pathlib
import re
import subprocess

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

COLUMN = "active_node_id"

# A fence *call*, not a mention. The parenthesis is the point: an earlier version
# matched the bare type name `PlacementFence`, which appears in doc comments all
# over this feature, so a comment could mark an unfenced statement as fenced. The
# two names are the shared helper and the api-side wrapper that adds the metric
# and the WARN.
# Every entry point that takes the fence. Listed rather than prefix-matched: a
# regex ending in an optional suffix would also accept a future
# `check_placement_fence_later` that does something else, and the guard's
# whole job is to not accept things that merely look right. Adding an entry
# point means adding it here — which the pinned site count forces, since
# renaming a call site without updating this list fails the guard.
FENCE_CALL = re.compile(
    r"\b(fence_node_for_placement"
    r"|check_placement_fence"
    r"|check_placement_fence_for_execution)\s*\("
)

# Clause keywords that close a `SET` list.
# What ends a `SET` list, and what ends an `INSERT` column list, are different
# things. Conflating them cost the guard the most common statement shape in this
# repo: a bare `)` was treated as a `SET` terminator so that
# `SET placement_changed_at = NOW(), active_node_id = $1` lost the clause at
# `NOW()` and never saw the assignment on the next line. The six real writers all
# happen to put `active_node_id` *first*, so the guard passed on luck of column
# ordering.
SET_CLAUSE_END = re.compile(r"\b(WHERE|FROM|RETURNING|VALUES|ON\s+CONFLICT)\b", re.IGNORECASE)
# `VALUES` also closes a `SET` list (above); `)` closes only a column list.
INSERT_COLUMNS_END = re.compile(r"\)|\bVALUES\b", re.IGNORECASE)
SET_CLAUSE_START = re.compile(r"\bSET\b", re.IGNORECASE)

# The value is captured rather than excluded with `(?!NULL)`. A lookahead after
# `\s*` is a trap — the quantifier backtracks to zero width so the lookahead is
# evaluated against the space and succeeds, which classified every
# `active_node_id = NULL` detachment as a placement. A captured token cannot
# backtrack into agreeing with itself.
SET_ASSIGNMENT = re.compile(rf"\b{COLUMN}\s*=\s*(?P<value>\S*)", re.IGNORECASE)

# A detachment is an assignment of literal NULL and nothing else. Matching any
# token that merely *starts* with NULL read `NULLIF($1, $3)` — a placement — as a
# detachment and skipped it silently. `\S*` above plus this exact test also makes
# an assignment whose value sits on the next line (`SET active_node_id =` ⏎ `$1,`)
# match with an empty value, which is not NULL, so it counts as a write.
DETACHMENT = re.compile(r"^NULL\b[,)\s]*$", re.IGNORECASE)

# `INSERT INTO sandboxes (…)` names the column in a list far from its `VALUES`,
# so the column list is its own shape.
INSERT_INTO_SANDBOXES = re.compile(r"INSERT\s+INTO\s+sandboxes\b", re.IGNORECASE)
BARE_COLUMN = re.compile(rf"(^|[\s(,]){COLUMN}\s*($|[\s),])", re.IGNORECASE)

FN_START = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+|unsafe\s+)?(?:async\s+)?fn\s+"
)

# Writers that are knowingly unfenced, each with its reason. Not a convenience
# list: every entry is a live hole in the fence, and the reason is what a
# reviewer weighs.
KNOWN_UNFENCED = {
    # `completion_sql` resolves its node *inside* the UPDATE, from a
    # `sandbox_k8s_executions` join, so a correct fence needs the node resolved
    # and locked in a prior statement — a restructure of that statement's
    # atomicity, not a call added to it. It has no production caller yet (that
    # crate's operations land one per PR), so nothing reaches it today. Owned by
    # the PR that gives it one.
    (
        pathlib.Path("crates/libs/placement/machine/src/postgres.rs"),
        "completion_sql",
    ): "resolves its node inside the UPDATE; needs a prior locking statement",
}

# Writers that take only an executor, so the fence cannot live in them: their
# caller takes it, in the same transaction, before calling them. Each entry
# names that caller. The site counts as fenced only while the caller still calls
# both the fence and the writer, and while it is the writer's only production
# caller — a second caller would inherit the write without the fence.
FENCED_BY_CALLER = {
    (
        pathlib.Path("crates/libs/sandbox/store/src/create.rs"),
        "insert_pooled",
    ): (
        pathlib.Path("crates/libs/api/src/controllers/sandbox/queries.rs"),
        "insert_pooled_sandbox",
    ),
}

EXCLUDED_DIRS = {".git", "target", "__pycache__", ".venv"}

# The count of writer sites the guard is expected to find in the current tree,
# excluding anything in `KNOWN_UNFENCED`. Pinned so a regression that makes the
# scanner blind fails loudly instead of passing. Bump it deliberately, in the
# same commit as the writer you added — and only after confirming the new site is
# fenced.
EXPECTED_FENCED_SITES = 6


def is_test_source(relative: pathlib.Path) -> bool:
    """Test sources construct fenced and unfenced placements on purpose."""
    name = relative.name
    return (
        name == "tests.rs"
        or name.endswith("_tests.rs")
        or name.endswith("_test_support.rs")
        or "tests" in relative.parts
        or any(part.endswith("_tests") for part in relative.parts[:-1])
        or "db-test-fixtures" in relative.parts
    )


def production_rust_files() -> list[pathlib.Path]:
    result = subprocess.run(
        ("git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "*.rs"),
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
    )
    paths = []
    for raw in result.stdout.decode().split("\0"):
        if not raw:
            continue
        relative = pathlib.Path(raw)
        if any(part in EXCLUDED_DIRS for part in relative.parts):
            continue
        if is_test_source(relative):
            continue
        if (REPO_ROOT / relative).is_file():
            paths.append(relative)
    return paths


RAW_STRING_OPEN = re.compile(r'(?:b?r)(#*)"')


def string_literals(text: str) -> list[tuple[int, str]]:
    """`(0-based start line, contents)` for every double-quoted Rust literal.

    A character scanner, because the literals holding these statements are
    multi-line and a per-line quote count cannot track them. Escapes are honoured
    so a `\\"` inside a literal does not end it.

    Raw strings are scanned by their hash delimiters rather than treated as
    ordinary quoted strings. An earlier revision claimed they "fall out naturally
    since the opening quote is still a quote", which holds only for `r"…"` with no
    embedded quote: in `r#"… "col" …"#` the inner quote ended the literal early,
    split the statement, and lost the enclosing `SET`/`INSERT` context — so
    `assigning_lines` reported no site and the pinned count stayed put, letting an
    unfenced writer through. Backslash is also not an escape inside a raw string,
    which the shared path would have mishandled the same way.
    """
    literals: list[tuple[int, str]] = []
    index = 0
    line = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == "\n":
            line += 1
            index += 1
            continue
        raw = RAW_STRING_OPEN.match(text, index)
        if raw and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_")):
            start_line = line
            closer = '"' + raw.group(1)
            end = text.find(closer, raw.end())
            end = length if end == -1 else end
            chunk = text[raw.end() : end]
            line += chunk.count("\n")
            literals.append((start_line, chunk))
            index = length if end == length else end + len(closer)
            continue
        if char != '"':
            index += 1
            continue
        start_line = line
        index += 1
        chunk: list[str] = []
        while index < length:
            char = text[index]
            if char == "\\" and index + 1 < length:
                chunk.append(text[index : index + 2])
                index += 2
                continue
            if char == '"':
                index += 1
                break
            if char == "\n":
                line += 1
            chunk.append(char)
            index += 1
        literals.append((start_line, "".join(chunk)))
    return literals


def assigning_lines(text: str) -> list[int]:
    """0-based line indices that assign a non-NULL `active_node_id`.

    Two shapes, both resolved against which clause is open inside the literal:

    * in a `SET` list, `active_node_id = <non-NULL>` is a write;
    * in an `INSERT INTO sandboxes (…)` column list, a bare `active_node_id` is
      a write.

    Assignments to NULL are detachments, not placements, and are excluded — the
    fence exists to stop a sandbox landing on a node that is leaving, and nothing
    lands by being cleared.
    """
    hits: list[int] = []
    for start_line, literal in string_literals(text):
        if COLUMN.lower() not in literal.lower():
            continue
        in_set = False
        in_insert_columns = False
        paren_depth = 0
        set_depth = 0
        insert_depth = 0
        for offset, line in enumerate(literal.splitlines()):
            # One positional walk over every boundary and every candidate on the
            # line. Earlier revisions handled the `INSERT` column list in a
            # separate branch that `continue`d past the `SET` walk, so a statement
            # putting `INSERT … VALUES (…) ON CONFLICT … DO UPDATE SET
            # active_node_id = $n` on ONE physical line was never evaluated: the
            # branch opened the column list, the same line's `VALUES` closed it,
            # and the assignment was skipped. The count then stayed at 6 and CI
            # went green on a wholly unfenced writer — the vacuous pass the pinned
            # count exists to prevent, reached through a different door.
            # Parenthesis depth at each character, carried across lines so a
            # multi-line subquery stays nested. **Clause boundaries only count at
            # depth 0.** Without this, a scalar subquery inside a `SET`
            # expression ends the clause from the inside: `SET target_node_id =
            # (SELECT id FROM nodes WHERE address = $3), active_node_id = $1` has
            # `FROM` and `WHERE` at depth 1, and treating them as terminators
            # loses the assignment on the next line. That is the same defect as
            # the bare `)` removed earlier — one token meaning two things
            # depending on nesting — and fixing it by depth rather than by
            # blacklisting tokens closes the class instead of one instance.
            depth_at = []
            running = paren_depth
            for char in line:
                if char == "(":
                    running += 1
                depth_at.append(running)
                if char == ")":
                    running = max(0, running - 1)
            paren_depth = running

            def depth_of(position: int) -> int:
                return depth_at[position] if position < len(depth_at) else 0

            events: list[tuple[int, int, re.Match[str] | None]] = []
            events += [(m.start(), 0, None) for m in INSERT_INTO_SANDBOXES.finditer(line)]
            events += [(m.start(), 1, None) for m in SET_CLAUSE_START.finditer(line)]
            events += [(m.start(), 2, None) for m in SET_CLAUSE_END.finditer(line)]
            events += [(m.start(), 3, None) for m in INSERT_COLUMNS_END.finditer(line)]
            events += [(m.start(), 4, m) for m in BARE_COLUMN.finditer(line)]
            events += [(m.start(), 5, m) for m in SET_ASSIGNMENT.finditer(line)]
            for position, kind, match in sorted(
                events, key=lambda event: (event[0], event[1])
            ):
                position_depth = depth_of(position)
                if kind == 0:
                    in_insert_columns = True
                    insert_depth = position_depth
                    in_set = False
                elif kind == 1:
                    # A `SET` opens at whatever depth it sits at. It is not
                    # required to be at depth 0: `WITH updated AS ( UPDATE …
                    # SET … )` — the pooled post-wake writer's shape — puts the
                    # whole statement at depth 1, and gating on depth 0 lost that
                    # writer entirely. The pinned site count is what caught it.
                    in_set = True
                    set_depth = position_depth
                elif kind == 2:
                    # A terminator only ends the clause it is level with or
                    # outside of. `FROM`/`WHERE` inside a scalar subquery —
                    # `SET target_node_id = (SELECT id FROM nodes WHERE …),
                    # active_node_id = $1` — sits deeper than the `SET` and must
                    # not end it.
                    if in_set and position_depth <= set_depth:
                        in_set = False
                elif kind == 3:
                    if in_insert_columns and position_depth <= insert_depth:
                        in_insert_columns = False
                elif kind == 4:
                    if in_insert_columns:
                        hits.append(start_line + offset)
                elif (
                    in_set
                    and position_depth == set_depth
                    and match is not None
                    and not DETACHMENT.match(match.group("value"))
                ):
                    hits.append(start_line + offset)
    return sorted(set(hits))


FN_NAME = re.compile(r"\bfn\s+([A-Za-z_][A-Za-z0-9_]*)")


def fn_name(header: str) -> str | None:
    """The identifier a `fn` header declares, or `None` if the line declares none.

    Exact identity, because the allowlist is an exemption: a substring test let
    `completion_sql_v2` inherit `completion_sql`'s exemption and vanish from both
    the violation list and the pinned count, which is the one failure mode
    `KNOWN_UNFENCED`'s per-function keying exists to prevent.
    """
    match = FN_NAME.search(header)
    return match.group(1) if match else None


CHAR_LITERAL = re.compile(r"'(?:\\(?:u\{[0-9a-fA-F]{1,6}\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'")


def code_only_lines(text: str) -> list[str]:
    """`text.splitlines()` with comments and literal *contents* blanked to spaces.

    Line count and column positions are preserved, so the result indexes
    identically to `splitlines()` and can be used interchangeably with it.

    This exists for one caller: deciding whether a function actually *calls* the
    fence. Searching raw lines let a `//` comment or a string mentioning
    `fence_node_for_placement(` certify a writer as fenced — fail-open, and in
    the direction that silently readmits the entire bug class this guard covers.
    Blanking rather than deleting keeps every other index in this module valid.

    Handles line comments, block comments (nesting, as Rust allows), plain and
    escaped string literals, char literals, and raw strings with any hash count.
    """
    out = []
    i, n = 0, len(text)
    block_depth = 0
    while i < n:
        char = text[i]
        if block_depth:
            if text.startswith("/*", i):
                block_depth += 1
                out.append("  ")
                i += 2
                continue
            if text.startswith("*/", i):
                block_depth -= 1
                out.append("  ")
                i += 2
                continue
            out.append("\n" if char == "\n" else " ")
            i += 1
            continue
        if text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end == -1 else end
            out.append(" " * (end - i))
            i = end
            continue
        if text.startswith("/*", i):
            block_depth = 1
            out.append("  ")
            i += 2
            continue
        # Raw string: r"..." / r#"..."# / br#"..."#
        raw = re.compile(r'(?:b?r)(#*)"').match(text, i)
        if raw and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            hashes = raw.group(1)
            close = '"' + hashes
            end = text.find(close, raw.end())
            end = n if end == -1 else end + len(close)
            out.append("".join("\n" if c == "\n" else " " for c in text[i:end]))
            i = end
            continue
        # A `'` opens a char literal only when one closes it right after
        # (`'x'`, `'\\n'`, `'\\u{..}'`). Otherwise it is a lifetime (`'_`,
        # `'a`), and treating it as a quote blanked real code up to the next
        # lifetime — fence calls included, which read as an unfenced writer.
        if char == "'" and not CHAR_LITERAL.match(text, i):
            out.append(char)
            i += 1
            continue
        if char in '"\'':
            quote = char
            out.append(" ")
            i += 1
            while i < n:
                if text[i] == "\\":
                    # Keep an escaped newline (a `\\` line continuation) a
                    # newline, or every later line shifts by one.
                    out.append(" " + ("\n" if text[i + 1 : i + 2] == "\n" else " "))
                    i += 2
                    continue
                if text[i] == quote:
                    out.append(" ")
                    i += 1
                    break
                out.append("\n" if text[i] == "\n" else " ")
                i += 1
            continue
        out.append(char)
        i += 1
    return "".join(out).splitlines()


def enclosing_fn_span(lines: list[str], index: int) -> tuple[int, int]:
    """`[start, end)` line range of the `fn` containing `index`.

    Scans back to the nearest `fn` header and forward to the next one at the same
    or shallower indentation. Brace counting would be exact but breaks on braces
    inside the SQL literals this guard reads; the next-header bound errs *wide*,
    which means a false pass rather than a false alarm. That is the wrong
    direction for safety and is only acceptable because the case worth catching —
    a wholly new writer nobody fenced — has no fence marker anywhere near it under
    either bound.
    """
    start = 0
    for candidate in range(index, -1, -1):
        if FN_START.match(lines[candidate]):
            start = candidate
            break
    indent = len(lines[start]) - len(lines[start].lstrip())
    end = len(lines)
    for candidate in range(start + 1, len(lines)):
        if FN_START.match(lines[candidate]) and (
            len(lines[candidate]) - len(lines[candidate].lstrip()) <= indent
        ):
            end = candidate
            break
    return start, end


def exempt(relative: pathlib.Path, header: str) -> bool:
    """Is this specific `(file, function)` allowlisted?

    Keyed on the function and not the file, because a file-wide skip hides *future*
    writers added beside the known one — from the violation check and from the
    pinned count both, so neither would notice. The exemption has to be as narrow
    as the thing it exempts.
    """
    declared = fn_name(header)
    return any(
        relative == path and name == declared for (path, name) in KNOWN_UNFENCED
    )


def fenced_by_caller(writer: str, caller_path: pathlib.Path, caller: str) -> bool:
    """Does `caller` fence and call `writer`, and is it `writer`'s only caller?"""
    call = re.compile(rf"\b{re.escape(writer)}\s*\(")
    callers = []
    for relative in production_rust_files():
        text = (REPO_ROOT / relative).read_text(encoding="utf-8", errors="replace")
        if writer not in text:
            continue
        lines = text.splitlines()
        code = code_only_lines(text)
        for index, line in enumerate(code):
            if call.search(line) and fn_name(lines[index]) != writer:
                start, end = enclosing_fn_span(lines, index)
                callers.append((relative, fn_name(lines[start]), code[start:end]))
    if len(callers) != 1:
        return False
    relative, name, body = callers[0]
    return (
        relative == caller_path
        and name == caller
        and any(FENCE_CALL.search(line) for line in body)
    )


def writer_sites() -> list[tuple[pathlib.Path, int, bool]]:
    """`(path, 1-based line, fenced)` for every writer site outside the allowlist."""
    sites = []
    for relative in production_rust_files():
        text = (REPO_ROOT / relative).read_text(encoding="utf-8", errors="replace")
        # Lowercased: every pattern below is case-insensitive, so a case-sensitive
        # early-out here silently exempted `SET ACTIVE_NODE_ID = $1`.
        if COLUMN not in text.lower():
            continue
        hits = assigning_lines(text)
        if not hits:
            continue
        lines = text.splitlines()
        # Comment- and literal-blanked twin of `lines`, same indexing. The
        # fence call is looked for in *this* one: a `//` mentioning
        # `fence_node_for_placement(` used to mark a writer fenced, which is a
        # guard that certifies the thing it cannot see. `lines` stays the
        # source of truth for headers and spans, which need the real text.
        code = code_only_lines(text)
        for index in hits:
            start, end = enclosing_fn_span(lines, index)
            if exempt(relative, lines[start]):
                continue
            caller = FENCED_BY_CALLER.get((relative, fn_name(lines[start])))
            if caller is not None:
                fenced = fenced_by_caller(fn_name(lines[start]), *caller)
            else:
                fenced = any(FENCE_CALL.search(line) for line in code[start:end])
            sites.append((relative, index + 1, fenced))
    return sites


def violations() -> list[str]:
    found = []
    for relative, line, fenced in writer_sites():
        if fenced:
            continue
        text = (REPO_ROOT / relative).read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        start, _ = enclosing_fn_span(lines, line - 1)
        header = lines[start].strip().rstrip("{").strip()
        found.append(
            f"{relative}:{line}: writes {COLUMN} in `{header}` "
            "with no termination-fence check"
        )
    return found


def main() -> None:
    found = violations()
    if found:
        raise SystemExit(
            "every production writer of a non-NULL sandboxes.active_node_id must consult the\n"
            "termination fence (pplx_space_common_svc::sql::node_placement_fence) in the same\n"
            "transaction, or a placement can be committed onto a node the provider is about to\n"
            "destroy. Call `check_placement_fence` and honour its verdict; or, if the writer\n"
            "genuinely cannot be fenced, add it to KNOWN_UNFENCED with a tracking issue and say\n"
            "why in review.\n\n" + "\n".join(found)
        )
    sites = writer_sites()
    if len(sites) != EXPECTED_FENCED_SITES:
        raise SystemExit(
            f"expected {EXPECTED_FENCED_SITES} fenced sandboxes.{COLUMN} writer sites, found "
            f"{len(sites)}. A guard that finds nothing passes for the wrong reason, so this is a\n"
            "failure either way: if you added or removed a writer, update EXPECTED_FENCED_SITES\n"
            "in the same commit; if you did not, the scanner has gone blind and the guard is no\n"
            "longer protecting anything.\n\n"
            + "\n".join(f"{path}:{line}" for path, line, _ in sites)
        )
    known = ", ".join(
        f"{path}::{name} ({reason})"
        for (path, name), reason in sorted(KNOWN_UNFENCED.items())
    )
    print(
        f"placement fence writer guard passed; {len(sites)} writer sites all fenced"
        + (f"; knowingly unfenced: {known}" if known else "")
    )


if __name__ == "__main__":
    main()
