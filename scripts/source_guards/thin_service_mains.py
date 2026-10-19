#!/usr/bin/env python3
"""Keep every public service `main.rs` thin.

SPACE is becoming a dependency of a private consumer: a deployment that needs
private wiring supplies its own `main` for a service and links the service's
library crate. That only works, and only stays cheap, while the public `main`
is nothing but the seam — clap flags mapped into the library's run config,
telemetry init, a shutdown token, one call into the library. Everything that
is the service (config resolution from the environment, embedded migrations,
server assembly, drain and handoff sequencing) belongs in the library, where a
private main gets it for free and an upstream change to it is not a paired
change the pin bump refuses.

`crates/apps/data/plane/svc/src/main.rs` is the exemplar. This guard holds
every deployable service binary to that shape. It is a source guard, not a
type check: each rule is a textual property that the shape cannot satisfy by
accident, and each failure message names the rule so the fix is obvious.

Services whose split has not merged yet are allowlisted below. An allowlisted
main is still checked and its failures are reported, but they do not fail the
guard. The entry is removed by the PR that makes the main thin: an allowlisted
main that already passes every rule FAILS the guard, so a merged split cannot
leave its entry behind.

Run standalone (`python scripts/source_guards/thin_service_mains.py`).
"""

from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
APPS = REPO_ROOT / "crates/apps"

# Every deployable service binary: the `svc` crates plus the two one-shot jobs
# and the cluster controller, whose crate directory is not named `svc`. Paths
# are crate directories relative to the repository root.
EXTRA_SUBJECTS = (
    "crates/apps/db/migrator",
    "crates/apps/template/publish",
)

# Not services in the sense above: developer tools, a benchmark, a guest test.
# Test doubles under `crates/apps/mock/` are excluded as a directory below,
# alongside `/cli/` crates: they stand in for external systems in tests and
# never get a private main.
EXCLUDED = (
    "crates/apps/net/gateway/bench",
    "crates/apps/spaced/guest/loopback-test",
)

# Rule (e) caps the WIRING, not the file. The clap surface (the `#[derive(Parser)]`
# struct, any `#[derive(ValueEnum)]` enum, and their doc comments and `#[arg]`
# attributes) is user-facing help text that legitimately grows with every flag a
# service takes; volmgr's is 116 lines on its own. What must stay tiny is
# everything else: imports, the `From<Cli>` conversion, and `main`. The merged
# thin mains sit at 27 to 52 wiring lines; 80 leaves room for a runtime builder
# and an allocator attribute without admitting a server assembly.
MAX_WIRING_LINES = 80
# A clap-derived item: the `#[derive(...)]` line naming Parser or ValueEnum through
# the first `}` at column 0. Rust formatting closes every top-level item that way.
CLAP_DERIVE = re.compile(r"^#\[derive\([^\]]*\b(?:Parser|ValueEnum)\b")

# Exceptions for mains awaiting their library-plus-thin-main split.
# Add an entry only for a NEW service that ships before its split, with the PR
# that removes it; the guard fails once the main becomes compliant so the entry
# cannot outlive its reason.
ALLOWLIST: dict[str, str] = {}

# Rule (a): a `mod` declaration means sibling modules, a tests.rs, or an inline
# module body, so the main holds more than the seam. Both forms count: `mod x;`
# and `mod x {`, with or without a visibility or `#[cfg(test)]` on the line
# before. `mod` inside a comment or string is not a declaration (comments are
# stripped before matching); only a line that begins with the declaration counts.
MOD_DECL = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*[;{]", re.M)
# Rule (b): embedded migrations belong to the library that owns the store.
# Any spelling: the qualified macro, the bare macro after `use sqlx::migrate`, or
# the import itself. A thin main has no reason to name the migrator at all.
MIGRATE_MACRO = re.compile(r"\bmigrate!\s*\(|\buse\s+sqlx::migrate\b")
# Rule (c): environment reads other than clap `env =` attributes. The service
# reads its own environment; the main reads only flags.
# Any spelling: `env::var`, `env::vars`, `env::var_os`, `env::vars_os`, the
# `std::env` path itself, or an import of it (`use std::env::var; var(..)`).
# clap's `env = "..."` attribute is the one sanctioned way to read the
# environment and does not match any of these.
ENV_READ = re.compile(r"\benv::vars?(?:_os)?\s*\(|\bstd::env\b|\buse\s+std::env\b")
# Rule (d): server assembly. Any of these in a main means the process is
# wiring the service rather than calling it.
SERVER_ASSEMBLY = (
    "axum::serve",
    "Server::builder",
    "serve_with_incoming",
    ".add_service(",
    "bind_unix_socket",
    "ServiceMutex::acquire",
)


class GuardError(Exception):
    """A public service main that is not thin, or an allowlist entry that is stale."""


def _display(path: pathlib.Path) -> str:
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path)


def _strip_comments(text: str) -> str:
    """Drop `//` comments so a rule explained in prose does not trip itself."""
    return "\n".join(line.split("//", 1)[0] for line in text.splitlines())


def _wiring_lines(text: str) -> int:
    """Lines of `text` outside clap-derived items (see MAX_WIRING_LINES)."""
    lines = text.splitlines()
    counted = 0
    i = 0
    while i < len(lines):
        if CLAP_DERIVE.match(lines[i]):
            while i < len(lines) and not lines[i].startswith("}"):
                i += 1
            i += 1  # the closing brace
            continue
        counted += 1
        i += 1
    return counted


def subject_crates() -> list[pathlib.Path]:
    """Every crate directory the guard holds to the thin-main shape, sorted."""
    subjects: set[pathlib.Path] = set()
    if APPS.is_dir():
        for main in APPS.glob("**/svc/src/main.rs"):
            subjects.add(main.parent.parent)
    for extra in EXTRA_SUBJECTS:
        crate = REPO_ROOT / extra
        if (crate / "src/main.rs").is_file():
            subjects.add(crate)
    excluded = {REPO_ROOT / e for e in EXCLUDED}
    return sorted(
        c
        for c in subjects
        if c not in excluded
        and "/cli/" not in f"/{_display(c)}/"
        and not _display(c).startswith("crates/apps/mock/")
    )


def violations(crate: pathlib.Path) -> list[str]:
    """Every rule the crate's main breaks, as one message each. Empty = thin."""
    main = crate / "src/main.rs"
    text = main.read_text(encoding="utf-8")
    code = _strip_comments(text)
    found: list[str] = []

    others = sorted(p.name for p in (crate / "src").iterdir() if p.name != "main.rs")
    if others:
        found.append(
            f"(f) src/ holds more than main.rs: {', '.join(others)}; modules and tests move to the service library"
        )
    if MOD_DECL.search(code):
        found.append("(a) declares a module; sibling modules and tests.rs belong in the service library")
    if MIGRATE_MACRO.search(code):
        found.append("(b) embeds sqlx::migrate!; migrations belong to the library that owns the store")
    if ENV_READ.search(code):
        found.append(
            "(c) reads the environment with env::var; a main reads only its flags (clap `env = ...` is fine), "
            "the service reads its own environment"
        )
    hits = [token for token in SERVER_ASSEMBLY if token in code]
    if hits:
        found.append(
            f"(d) assembles the server ({', '.join(hits)}); binding, layering, and serving belong in the "
            "library's run function"
        )
    wiring = _wiring_lines(text)
    if wiring > MAX_WIRING_LINES:
        found.append(
            f"(e) {wiring} lines outside the clap surface, over the {MAX_WIRING_LINES}-line cap for a thin "
            "main's wiring (the Parser/ValueEnum items are not counted)"
        )
    return found


def check() -> tuple[list[str], list[str]]:
    """(failures, burn_down_notes). Failures are fatal; notes are allowlisted mains still fat."""
    failures: list[str] = []
    notes: list[str] = []
    seen: set[str] = set()
    for crate in subject_crates():
        rel = _display(crate)
        seen.add(rel)
        found = violations(crate)
        reason = ALLOWLIST.get(rel)
        if reason is None:
            for message in found:
                failures.append(f"{rel}/src/main.rs: {message}")
        elif found:
            notes.append(f"{rel}/src/main.rs: allowlisted, burn down ({reason}): " + "; ".join(found))
        else:
            failures.append(
                f"{rel}: allowlisted but already compliant; remove its ALLOWLIST entry in "
                f"{_display(pathlib.Path(__file__))} ({reason})"
            )
    for rel in sorted(set(ALLOWLIST) - seen):
        failures.append(f"{rel}: in ALLOWLIST but is not a subject crate; remove the entry")
    return failures, notes


def main() -> None:
    failures, notes = check()
    for note in notes:
        print(f"note: {note}")
    if failures:
        raise GuardError("public service mains are not thin:\n  " + "\n  ".join(failures))
    print(f"thin-service-mains guard: ok ({len(subject_crates())} mains, {len(notes)} allowlisted burning down)")


if __name__ == "__main__":
    try:
        main()
    except GuardError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)
