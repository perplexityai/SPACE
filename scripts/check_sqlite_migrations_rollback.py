#!/usr/bin/env python3
"""Reject SQLite up migrations that the release before them can't run on.

SQLite rollbacks are image-only: the previous release starts on the schema a
newer one left. So each statement a change adds to a SQLite up migration must
be on an allowlist of changes that release runs on unchanged, or carry a
marker that says why it is safe anyway. db/README.md, under RULES_HEADING,
has the allowlist and the markers.

Usage:
  check_sqlite_migrations_rollback.py --base <commit> <db/<store>/<version>_<name>.up.sql>...
    The change's migrations are read from the working tree, the base's from
    <commit>; an empty --base compares with no base migrations.
  check_sqlite_migrations_rollback.py --list-markers <target-ref> <current-ref>
    Lists the markers in the SQLite up migrations <current-ref> has and
    <target-ref> doesn't, newest first per store.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

RULES_HEADING = "Rollback-safe SQLite migrations"
MARKER = "space-allow-rollback-unsafe"
# Each marker kind, and why a statement of that kind breaks the previous release.
KINDS = {
    "schema": "the previous release runs on a schema this changes or constrains",
    "data": "after a rollback the previous release writes rows in the old form or keeps writing what"
    " this copied, and on roll-forward the statement doesn't rerun",
}
COMMON, POSTGRES = "common", "postgres"
# The oldest engine the guard runs on: the CI image's Debian python3. The app
# bundles a newer SQLite, so syntax this one doesn't know fails the check.
MIN_SQLITE = (3, 40, 0)
MIN_PYTHON = (3, 11)  # tomllib


@dataclass(frozen=True)
class Migration:
    version: int
    path: str
    sql: str


@dataclass
class Finding:
    kind: str
    object: str
    detail: str
    file: str
    line: int


@dataclass(frozen=True)
class Marker:
    file: str
    line: int
    kind: str
    object: str
    reason: str


# SQLite folds only ASCII letters when it compares identifiers.
ASCII_FOLD = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def fold(name: str) -> str:
    return name.translate(ASCII_FOLD)


TOKEN = re.compile(
    r"""\s+|(?P<comment>--[^\n]*|/\*.*?(?:\*/|\Z))
    |(?P<string>'(?:[^']|'')*')
    |(?P<quoted>"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\])
    |(?P<word>[A-Za-z0-9_$\u0080-\U0010ffff]+)
    |(?P<punct>.)""",
    re.DOTALL | re.VERBOSE,
)


def lex(sql: str) -> list[tuple[str, str]]:
    """SQL as (kind, text) tokens, comments dropped: a `word` or `quoted`
    identifier ASCII-folded and unquoted, a `string` as written, or `punct`."""
    out = []
    for match in TOKEN.finditer(sql):
        kind = match.lastgroup
        if kind == "quoted":
            out.append((kind, fold(match.group(0)[1:-1])))
        elif kind in ("word", "punct"):
            out.append((kind, fold(match.group(0))))
        elif kind == "string":
            out.append((kind, match.group(0)))
    return out


def tokens(sql: str) -> tuple[tuple[str, str], ...]:
    return tuple(lex(sql))


def statements(sql: str) -> list[tuple[int, str]]:
    """The statements with their first lines, split where SQLite says one is
    complete, so strings, comments and trigger bodies are SQLite's to read."""
    out, buffer, line = [], "", 1
    for piece in sql.split(";"):
        buffer += piece + ";"
        if sqlite3.complete_statement(buffer):
            lead = re.match(r"(?:\s+|--[^\n]*|/\*.*?\*/)*", buffer, re.DOTALL).group(0)
            if tokens(buffer) != (("punct", ";"),):
                out.append((line + lead.count("\n"), buffer))
            line += buffer.count("\n")
            buffer = ""
    if tokens(buffer[:-1]):
        out.append((line, buffer[:-1]))
    return out


def statement_tokens(sql: str) -> set[tuple[tuple[str, str], ...]]:
    return {tokens(text) for _, text in statements(sql)}


def top_level(toks) -> list[tuple[str, str]]:
    """The tokens outside parentheses; only punctuation nests."""
    depth, out = 0, []
    for tok in toks:
        if tok[0] == "punct" and tok[1] in "()":
            depth += 1 if tok[1] == "(" else -1
        elif depth == 0:
            out.append(tok)
    return out


def words(toks) -> list[str]:
    """Words and punctuation as they are; identifiers and strings marked, so
    a quoted name is never read as a keyword or a delimiter."""
    return [text if kind in ("word", "punct") else f"\0{text}" for kind, text in toks]


def plain_insert(sql: str) -> bool:
    """An INSERT ... VALUES or DEFAULT VALUES with no REPLACE or upsert. A
    token check: SQLite's transfer optimisation reports no reads for an
    INSERT ... SELECT * from a table of the same shape."""
    outer = words(top_level(lex(sql)))
    if "replace" in outer or "into" not in outer or any(outer[i : i + 2] == ["on", "conflict"] for i in range(len(outer))):
        return False
    rest = outer[outer.index("into") + 2 :]
    rest = rest[2:] if rest[:1] == ["."] else rest
    rest = rest[2:] if rest[:1] == ["as"] else rest
    return rest[:1] == ["values"] or rest[:2] == ["default", "values"]


def replaces_on_conflict(table_sql: str) -> bool:
    """The table declares ON CONFLICT REPLACE, so a plain INSERT can replace a row."""
    toks = words(lex(table_sql))
    return any(toks[i : i + 3] == ["on", "conflict", "replace"] for i in range(len(toks)))


# Classifying


class ApplyError(Exception):
    pass


def schema_objects(conn: sqlite3.Connection) -> dict[tuple[str, str], tuple[str, str]]:
    """Each object of the main schema by (type, folded name): table and SQL.
    A table and a trigger can share a name."""
    return {
        (kind, fold(name)): (fold(table), sql or "")
        for kind, name, table, sql in conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_schema")
    }


def columns(conn: sqlite3.Connection, table: str) -> tuple[list[tuple], list[tuple]]:
    return (
        conn.execute("SELECT * FROM pragma_table_xinfo(?) ORDER BY cid", (table,)).fetchall(),
        conn.execute("SELECT * FROM pragma_foreign_key_list(?)", (table,)).fetchall(),
    )


def column_definition(sql: str, column: str) -> list[str]:
    """The top-level words of one column's definition in a CREATE TABLE.
    Quoted identifiers are names, never delimiters."""
    toks = lex(sql)
    opening = next((i for i, tok in enumerate(toks) if tok == ("punct", "(")), None)
    if opening is None:
        return []
    depth, current, definitions = 0, [], []
    for tok in toks[opening + 1 :]:
        if tok == ("punct", ")") and depth == 0:
            break
        if tok[0] == "punct" and tok[1] in "()":
            depth += 1 if tok[1] == "(" else -1
        if tok == ("punct", ",") and depth == 0:
            definitions.append(current)
            current = []
        else:
            current.append(tok)
    definitions.append(current)
    return next(
        (words(top_level(d[1:])) for d in definitions if d and d[0][0] in ("word", "quoted") and d[0][1] == column), []
    )


def added_column_problem(conn: sqlite3.Connection, table: str, before, after) -> str | None:
    """Why an ALTER TABLE isn't one allowed ADD COLUMN, or None."""
    (old, old_keys), (new, new_keys) = before, after
    if len(new) != len(old) + 1 or new[: len(old)] != old or new_keys != old_keys:
        return "it changes the table's existing columns or keys"
    _, name, _, notnull, default, _, hidden = new[-1]
    definition = column_definition(schema_objects(conn)[("table", fold(table))][1], fold(name))
    if "check" in definition or "references" in definition:
        return f"column {name} has a CHECK or REFERENCES"
    null_default = default is None or conn.execute(f"SELECT ({default}) IS NULL").fetchone()[0]
    if hidden == 2 and notnull:
        return f"generated column {name} is NOT NULL"
    if hidden == 0 and notnull and null_default:
        return f"column {name} is NOT NULL without a non-NULL default"
    return None


# Statements that open or create other database files: VACUUM attaches one.
DENIED = (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH)


class Denied(Exception):
    pass


def guarded(conn: sqlite3.Connection, run, record=None) -> None:
    """Runs `run()` with an authorizer that denies DENIED and passes every
    other action to `record`."""
    denied = []

    def authorize(action, arg1, arg2, database, trigger):
        if action in DENIED:
            denied.append(action)
            return sqlite3.SQLITE_DENY
        if record is not None:
            record(action, arg1, arg2, database, trigger)
        return sqlite3.SQLITE_OK

    conn.set_authorizer(authorize)
    try:
        run()
    except sqlite3.DatabaseError:
        if denied:
            raise Denied("the guard doesn't allow ATTACH, DETACH or VACUUM in migrations: they open or create other database files") from None
        raise
    finally:
        conn.set_authorizer(None)


def alter_target(conn: sqlite3.Connection, statement: str) -> str | None:
    """The table an ALTER TABLE names, as SQLite resolves it: prepared under
    an authorizer that denies the first action, so it doesn't run."""
    seen = []

    def probe(action, arg1, arg2, database, trigger):
        seen.append((action, arg1, arg2))
        return sqlite3.SQLITE_DENY

    conn.set_authorizer(probe)
    try:
        conn.execute(statement)
    except sqlite3.DatabaseError:
        pass
    finally:
        conn.set_authorizer(None)
    if seen and seen[0][0] == sqlite3.SQLITE_ALTER_TABLE and seen[0][1] == "main":
        return seen[0][2]
    return None


def read_roots(conn: sqlite3.Connection, statement: str) -> set[int]:
    """The root pages a statement's program opens to read, from EXPLAIN,
    which prepares it without running it."""
    return {row[3] for row in conn.execute(f"EXPLAIN {statement}") if row[1] == "OpenRead"}


def classify(conn: sqlite3.Connection, statement: str, existing: dict, base_triggers: set[str]) -> list[tuple[str, str, str]]:
    """Runs one statement and returns its (kind, object, detail) findings.
    Every object name comes from SQLite's authorizer."""
    toks = words(lex(statement))
    target = alter_target(conn, statement) if toks[:1] == ["alter"] else None
    tables = {name for kind, name in existing if kind == "table"}
    before = columns(conn, target) if target and fold(target) in tables else None
    actions = []
    guarded(conn, lambda: conn.execute(statement), lambda *action: actions.append(action))
    found: dict[tuple[str, str], str] = {}
    add = lambda kind, obj, detail: found.setdefault((kind, obj), detail)  # noqa: E731
    dropped = {fold(a[1]) for a in actions if a[0] == sqlite3.SQLITE_DROP_TABLE}
    creates_index = any(a[0] == sqlite3.SQLITE_CREATE_INDEX for a in actions)
    for action, arg1, arg2, database, trigger in actions:
        trigger = fold(trigger) if trigger else None
        if trigger in base_triggers:
            continue  # the previous release's own trigger
        if action == sqlite3.SQLITE_ALTER_TABLE:
            database, arg1 = arg1, arg2
        if database not in (None, "main") or (arg1 and fold(arg1).startswith("sqlite_")):
            continue
        name = fold(arg1) if arg1 else ""
        if action in (sqlite3.SQLITE_READ, sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION,
                      sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT, sqlite3.SQLITE_RECURSIVE,
                      sqlite3.SQLITE_CREATE_VIEW) or (action == sqlite3.SQLITE_REINDEX and creates_index):
            continue
        if action == sqlite3.SQLITE_CREATE_TABLE:
            keys: dict[int, list] = {}
            for key in columns(conn, arg1)[1]:
                keys.setdefault(key[0], []).append(key)
            for key in keys.values():
                if fold(key[0][2]) in tables:
                    obj = f"{name}.{fold(key[0][3])}" if len(key) == 1 else name
                    add("schema", obj, f"new table {name} has a foreign key into existing table {fold(key[0][2])}")
            if any(a[0] == sqlite3.SQLITE_READ and fold(a[1]) in tables for a in actions):
                add("data", name, f"new table {name} is created from rows of an existing table")
        elif action == sqlite3.SQLITE_CREATE_INDEX:
            unique = any(fold(row[1]) == name and row[2] for row in conn.execute("SELECT * FROM pragma_index_list(?)", (arg2,)))
            if unique and fold(arg2) in tables:
                add("schema", name, f"unique index {name} created on existing table {fold(arg2)}")
        elif action == sqlite3.SQLITE_CREATE_TRIGGER:
            add("schema", name, f"trigger {name} created")
        elif action in DROPS:
            # A dropped table's own indexes and triggers go with it.
            if (DROPS[action], name) in existing and not (action != sqlite3.SQLITE_DROP_TABLE and fold(arg2 or "") in dropped):
                add("schema", name, f"existing {DROPS[action]} {name} dropped")
        elif action == sqlite3.SQLITE_ALTER_TABLE:
            if before is not None and name == fold(target):
                after = columns(conn, arg1) if ("table", name) in schema_objects(conn) else ([], [])
                problem = added_column_problem(conn, arg1, before, after) if after[0] else "it renames the table"
                if problem:
                    old = {fold(row[1]) for row in before[0]}
                    new = {fold(row[1]) for row in after[0]}
                    changed = sorted(old - new) or sorted(new - old)
                    obj = f"{name}.{changed[0]}" if after[0] and len(changed) == 1 else name
                    add("schema", obj, f"ALTER TABLE {name} isn't an allowed ADD COLUMN: {problem}")
        elif action in (sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE):
            by = f" (by trigger {trigger})" if trigger else ""
            if name in tables and action == sqlite3.SQLITE_INSERT:
                if trigger or not plain_insert(statement) or replaces_on_conflict(existing[("table", name)][1]):
                    add("data", name, f"INSERT{by} into existing table {name} isn't a plain INSERT ... VALUES")
            elif name in tables and name not in dropped:
                word = "UPDATE" if action == sqlite3.SQLITE_UPDATE else "DELETE"
                add("data", name, f"{word}{by} rewrites rows of existing table {name}")
            elif action == sqlite3.SQLITE_INSERT and copies(conn, statement, actions, tables):
                add("data", name, f"INSERT into new table {name} copies rows of an existing table")
        elif action == sqlite3.SQLITE_PRAGMA:
            # ALTER TABLE runs its own; only a PRAGMA statement counts.
            if toks[:1] == ["pragma"]:
                add("schema", name, f"PRAGMA {name} isn't on the allowlist")
        else:
            add("schema", toks[0], f"{toks[0].upper()} statements aren't on the allowlist")
    return [(kind, obj, detail) for (kind, obj), detail in found.items()]


DROPS = {
    sqlite3.SQLITE_DROP_TABLE: "table",
    sqlite3.SQLITE_DROP_INDEX: "index",
    sqlite3.SQLITE_DROP_VIEW: "view",
    sqlite3.SQLITE_DROP_TRIGGER: "trigger",
}


def copies(conn: sqlite3.Connection, statement: str, actions: list, tables: set[str]) -> bool:
    """An INSERT into a new table that reads an existing one: a READ the
    authorizer reports, or, for an INSERT ... SELECT that the transfer
    optimisation reports no reads for, a table its program opens."""
    if any(a[0] == sqlite3.SQLITE_READ and a[1] and fold(a[1]) in tables for a in actions):
        return True
    if plain_insert(statement):
        return False
    roots = {page: fold(name) for name, page in conn.execute("SELECT name, rootpage FROM sqlite_schema WHERE type = 'table'")}
    return any(roots.get(page) in tables for page in read_roots(conn, statement))


def apply(conn: sqlite3.Connection, migration: Migration, label: str) -> None:
    # SQLx runs each migration in a transaction unless it opts out.
    sql = migration.sql if migration.sql.startswith("-- no-transaction") else f"BEGIN;\n{migration.sql}\n;COMMIT;"
    try:
        guarded(conn, lambda: conn.executescript(sql))
    except (sqlite3.Error, Denied) as error:
        raise apply_error(migration, label, error) from error


def apply_error(migration: Migration, label: str, error: Exception) -> ApplyError:
    return ApplyError(
        f"{migration.path}: failed to apply on the {label} schema: {error} (this guard runs"
        f" SQLite {sqlite3.sqlite_version}; the app bundles the one libsqlite3-sys in Cargo.lock carries)"
    )


def open_database(directory: str, name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(Path(directory) / f"{name}.db", isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")  # as SQLx's SQLite connections do
    return conn


# Markers


MARKER_TEXT = re.compile(rf"{MARKER}\b([^\n]*)")
MARKER_BODY = re.compile(r"\s*:\s*([^\s:]+)\s*:\s*([A-Za-z0-9-]+)\s*:(.*)")
MARKER_SYNTAX = f"-- {MARKER}: <object>: <{'|'.join(KINDS)}>: <reason>"


def markers(migration: Migration) -> tuple[list[Marker], list[str]]:
    """The markers in the migration's SQL comments, and an error for each
    that doesn't parse, names an unknown kind, or gives no reason."""
    found, errors = [], []
    for match in TOKEN.finditer(migration.sql):
        for text in MARKER_TEXT.finditer(match.group("comment") or ""):
            line = migration.sql.count("\n", 0, match.start() + text.start()) + 1
            where = f"{migration.path}:{line}: {MARKER} marker"
            body = MARKER_BODY.match(text.group(1).split("*/")[0])
            if body is None:
                errors.append(f"{where} must name an object and a kind: {MARKER_SYNTAX}")
                continue
            obj, kind, reason = fold(re.sub(r'["`\[\]]', "", body.group(1))), body.group(2), body.group(3).strip()
            if kind not in KINDS:
                errors.append(f"{where} for {obj} names unknown kind {kind}; the kinds are {', '.join(KINDS)}")
            elif not any(ch.isalnum() for ch in reason) or reason.startswith("<") or "<why" in reason:
                errors.append(f"{where} for {obj}: {kind} needs a reason, not an empty or placeholder one")
            else:
                found.append(Marker(migration.path, line, kind, obj, reason))
    return found, errors


# One store


def check_store(base: list[Migration], prelude: list[Migration], changed: list[Migration],
                landed: dict[int, Migration], consumers: list[str] | None = None,
                ) -> tuple[list[Finding], list[tuple[Finding, Marker]], list[str]]:
    """Classifies the statements `changed` adds, applied after `base` and then
    `prelude` (a consumer's pending common migrations), and matches markers.
    `consumers` is set for db/common, whose changed migrations each need one
    `common: schema` marker. Returns the unmarked findings, the exempted
    ones, and the errors."""
    findings: list[Finding] = []
    with tempfile.TemporaryDirectory() as directory:
        conn = open_database(directory, "head")
        for migration in base:
            apply(conn, migration, "base")
        existing = schema_objects(conn)
        base_triggers = {name for kind, name in existing if kind == "trigger"}
        for migration in prelude:
            apply(conn, migration, "consumer's base")
        for migration in changed:
            # In a landed migration only the statements its base revision lacks.
            known = statement_tokens(landed[migration.version].sql) if migration.version in landed else set()
            transactional = not migration.sql.startswith("-- no-transaction")
            try:
                conn.execute("BEGIN") if transactional else None
                for line, statement in statements(migration.sql):
                    if tokens(statement) not in known:
                        for kind, obj, detail in classify(conn, statement, existing, base_triggers):
                            findings.append(Finding(kind, obj, detail, migration.path, line))
                conn.execute("COMMIT") if transactional else None
            except (sqlite3.Error, Denied) as error:
                raise apply_error(migration, "head", error) from error
    errors, found = [], []
    for migration in changed:
        file_markers, marker_errors = markers(migration)
        errors += marker_errors
        revised = migration.version not in landed or statement_tokens(migration.sql) != statement_tokens(landed[migration.version].sql)
        if consumers is not None and revised and not any(
            (m.kind, m.object) == ("schema", COMMON) for m in file_markers if migration.version not in landed
        ):
            errors.append(
                f"{migration.path}: a db/common migration needs one `-- {MARKER}: {COMMON}: schema: <reason>`"
                f" whose reason covers every consumer: {', '.join(consumers)}. The guard doesn't classify it"
                " against each consumer's own schema, and a landed migration can't carry it"
            )
        if migration.version in landed:
            kept = {(m.kind, m.object, m.reason) for m in markers(landed[migration.version])[0]}
            errors += [
                f"{m.file}:{m.line}: {MARKER} marker in a landed migration, which never exempts: add the"
                " statement and its marker to a new migration" for m in file_markers if (m.kind, m.object, m.reason) not in kept
            ]
        else:
            found += file_markers
    # A db/common migration's marker covers every finding in its file.
    shared = {m.file: m for m in found if consumers is not None and (m.kind, m.object) == ("schema", COMMON)}
    keys = {(f.kind, f.object, f.file) for f in findings}
    errors += [
        f"{m.file}:{m.line}: stale {MARKER} marker: no {m.kind} finding on {m.object} in this file; remove it"
        for m in found if (m.kind, m.object, m.file) not in keys and shared.get(m.file) is not m
    ]
    by_key = {(m.kind, m.object, m.file): m for m in found}
    marker_of = lambda f: by_key.get((f.kind, f.object, f.file)) or shared.get(f.file)  # noqa: E731
    exempted = [(f, marker_of(f)) for f in findings if marker_of(f)]
    return [f for f in findings if not marker_of(f)], exempted, errors


# Git and the repository


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def catalog(directory: str, ref: str | None) -> list[Migration]:
    """The up migrations directly in db/<directory>, from <ref> or, without
    one, the working tree, in version order."""
    if ref is None:
        paths = [str(p) for p in Path("db", directory).glob("*.up.sql")]
        read = lambda path: Path(path).read_text(encoding="utf-8")  # noqa: E731
    elif ref == "":
        return []
    else:
        paths = git("ls-tree", "-z", "--name-only", ref, "--", f"db/{directory}/").split("\0")
        read = lambda path: git("show", f"{ref}:{path}")  # noqa: E731
    found = []
    for path in paths:
        match = re.fullmatch(rf"db/{re.escape(directory)}/(\d+)_[^/]+\.up\.sql", path)
        if match:
            found.append(Migration(int(match.group(1)), path, read(path)))
    return sorted(found, key=lambda m: m.version)


def common_consumers(ref: str | None = None) -> list[str]:
    """The stores the migrator applies db/common to, from the state-ownership
    rules archcheck enforces, in the working tree or at `ref`."""
    import tomllib

    text = Path("ARCHITECTURE.toml").read_text() if ref is None else git("show", f"{ref}:ARCHITECTURE.toml")
    return sorted(tomllib.loads(text)["rules"]["layers"]["state"]["shared_catalogs"][COMMON])


def list_markers(target: str, current: str) -> int:
    """Prints the markers in the SQLite up migrations `current` has and
    `target` doesn't: 0 for none, 1 for some, 2 when they can't be read."""
    try:
        entries: dict[str, list[tuple[int, str]]] = {}
        errors = []
        for path in git("ls-tree", "-z", "-d", "--name-only", current, "--", "db/").split("\0"):
            store = Path(path).name
            if not path or store == POSTGRES:
                continue
            landed = {m.version for m in catalog(store, target)}
            for migration in catalog(store, current):
                if migration.version in landed:
                    continue
                found, marker_errors = markers(migration)
                errors += marker_errors
                # Each consumer's database has its own common history.
                for owner in common_consumers(current) if store == COMMON and found else [store]:
                    entries.setdefault(owner, []).extend(
                        (migration.version, f"{owner} {migration.version} {m.kind} {m.object}: {m.reason}") for m in found
                    )
        lines = [line for owner in sorted(entries) for _, line in sorted(entries[owner], reverse=True)]
    except (subprocess.CalledProcessError, OSError, UnicodeDecodeError, KeyError, ValueError) as error:
        errors = [f"can't read the migrations of {target} or {current}: {error}"]
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if errors:
        return 2
    if not lines:
        print(f"No {MARKER} markers in SQLite migrations that {current} has and {target} doesn't.")
        return 0
    print(f"{MARKER} markers in SQLite migrations that {current} has and {target} doesn't, newest first"
          f" per store; a `common` line is the shared catalog in that store's database. Before rolling back to"
          f" {target} past one whose reason names a release newer than {target}, stop that store and run"
          " `space-db-migrator <store> ... down` until the marked version is gone from its histories: each run"
          " reverts the newest migration first, so every newer migration above it goes too.")
    print("\n".join(lines))
    return 1


def fresh_install(migrations: list[Migration]) -> None:
    """Applies a consumer's catalogs to an empty database, shared first, as
    a new node does."""
    with tempfile.TemporaryDirectory() as directory:
        conn = open_database(directory, "fresh")
        for migration in migrations:
            apply(conn, migration, "fresh install")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", help="The base commit; empty for none.")
    parser.add_argument("--list-markers", nargs=2, metavar=("TARGET", "CURRENT"))
    parser.add_argument("paths", nargs="*")
    args = parser.parse_args()
    if args.list_markers:
        return list_markers(*args.list_markers)
    if args.base is None or not args.paths:
        parser.error("--base and at least one path are required")
    if sqlite3.sqlite_version_info < MIN_SQLITE or sys.version_info < MIN_PYTHON:
        print(f"ERROR: this guard needs Python {'.'.join(map(str, MIN_PYTHON))} and SQLite"
              f" {'.'.join(map(str, MIN_SQLITE))} or newer; {sys.executable} is {sys.version.split()[0]}"
              f" with SQLite {sqlite3.sqlite_version}.", file=sys.stderr)
        return 2
    by_store: dict[str, set[str]] = {}
    for path in args.paths:
        by_store.setdefault(Path(path).parent.name, set()).add(path)
    common_base, common_head = catalog(COMMON, args.base), catalog(COMMON, None)
    base_versions = {m.version for m in common_base}
    pending = [m for m in common_head if m.path in by_store.get(COMMON, ()) and m.version not in base_versions]
    consumers = common_consumers()
    checks = sorted(by_store.keys() | ({s for s in consumers if Path("db", s).is_dir()} if pending else set()))
    failed = 0
    for store in checks:
        consumes = store in consumers
        store_base = catalog(store, args.base)
        changed = [m for m in catalog(store, None) if m.path in by_store.get(store, ())]
        try:
            findings, exempted, errors = check_store(
                (common_base if consumes else []) + store_base, pending if consumes else [],
                changed, {m.version: m for m in store_base}, consumers if store == COMMON else None)
            if consumes and pending:
                fresh_install(common_head + catalog(store, None))
        except ApplyError as error:
            findings, exempted, errors = [], [], [str(error)]
        for f, m in exempted:
            print(f"Exempted: {f.file}:{f.line}: {store}: {f.object}: {f.kind}: {f.detail}\n  by {m.file}:{m.line}: {m.reason}")
        for error in errors:
            print(f"ERROR: {store}: {error}", file=sys.stderr)
        for f in findings:
            print(f"ERROR: {f.file}:{f.line}: {store}: {f.object}: {f.kind}: {f.detail}\n"
                  f"  Why: {KINDS[f.kind]}.\n"
                  f"  If the previous release doesn't depend on it, add this line to {f.file}:\n"
                  f"    -- {MARKER}: {f.object}: {f.kind}: <why the previous release doesn't depend on it,"
                  f" e.g. release N stopped reading it>\n"
                  f'  See db/README.md, "{RULES_HEADING}".', file=sys.stderr)
        failed += len(errors) + len(findings)
    if failed:
        print(f"{failed} SQLite rollback finding(s) or error(s). SQLite rollbacks are image-only: the previous"
              " release runs on the schema these migrations leave, so each statement needs to be on the"
              " allowlist or carry a marker.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
