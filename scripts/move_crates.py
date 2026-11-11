#!/usr/bin/env python3
"""Move workspace crates into their layer directories, mechanically.

archcheck's `layers` rule (crates/tools/archcheck/README.md, Layers) gives every
crate a layer and, below `crates/<layer>/`, a path: its package name without
`pplx-space-`, split on `-`, optionally one leaf directory deeper where another
crate's path would nest inside it. The crates that predate the layer
directories are listed in `[rules.layers.unmoved]`. This tool moves them, from
a reviewed mapping, and rewrites what names their paths or renamed packages, so
the move can be regenerated on a newer main instead of rebased.

Usage:
  move_crates.py --generate [--mapping FILE]
  move_crates.py --apply [--mapping FILE]
  move_crates.py --verify [--base REV] [--mapping FILE] [--skip-arch]

The mapping (scripts/move_crates.toml) has one row per workspace crate in
`[crates]`: its target directory, an optional new package name, or `"stay"` for
a legacy crate (`[rules.layers] legacy`), which stays until it is dissolved
(another crate stays only as `{ stay = "<reason>" }`). `[files]` maps each
tracked file below `crates/apps` or `crates/libs` outside a crate, such as a
family's AGENTS.md, to where it goes, or `"stay"`. `[stale_paths]` maps each
file the move leaves as it is, and that may keep naming old paths, to why.

--generate rewrites the mapping from `cargo metadata --no-deps`, the `unmoved`
list and the name rule, keeping every choice the file already makes (a rename,
a leaf, a reviewed reason to stay, a file's target). It fails on a crate
without a layer, a row whose target breaks the layout rule, a crate whose path
would nest inside another's with no leaf to resolve it, a rename of a package
that archcheck's package-keyed baseline tables name (they have no rename map,
so the entry would read as new), and a file outside any crate family.

--apply refuses a dirty tree or a mapping --generate would change. It moves
every mapped crate in one run with `git mv`, each crate's files keeping their
layout so rename detection pairs them, and rewrites every tracked text file
outside `[stale_paths]`:

* a repository path into a moved crate (`crates/libs/x/src/lib.rs`), or into a
  directory of crates (`crates/libs/fam/**`), which becomes one target per
  layer its crates went to: a line holding only the path is repeated, a quoted
  path in TOML becomes several, and prose or a comment takes a `{a,b}` glob;
* a `..`-relative path, with or without a `crates/` part, that reaches a
  tracked path from the file's directory or its crate's manifest directory
  (Cargo `path =`, `build.rs` roots, `include_str!`, `sqlx::migrate!`,
  `CARGO_MANIFEST_DIR` joins and `format!("{}/..", env!("CARGO_MANIFEST_DIR"))`,
  a script-directory expansion such as `$(dirname "$0")/`, Markdown links),
  recomputed from the file's and the target's new places; and a symlink's
  target, the same way;
* renamed packages and the renamed library's Rust name, bounded by characters
  that can't continue them or by a regex `\\b`, and `Cargo.lock`, which
  `cargo update --workspace` then rewrites in its own order;
* `ARCHITECTURE.toml`: moved crates leave `unmoved`, and a rewritten glob that
  another entry of its list covers is dropped; `architecture-baseline.toml`
  tables whose keys changed are re-sorted as archcheck writes them, each
  comment staying with the entries it heads;
* each agent plugin whose content changed gets a patch version bump.

The run exits 1 after listing what it could not rewrite exactly: a path split
across layers where a single path must go (code, or a `{a,b}` list after it), a
`..` path that names a moved crate but reaches nothing tracked or reaches
different paths from the file and the manifest directory, a climb in a file
that moves after any other shell expansion (`"$CRATE_DIR/../x"`), a climb
after a literal directory that, read from the file's directory, reaches a path
the move changes, a binary or non-UTF-8 file that names a moved path or
renamed package, and an `ARCHITECTURE.toml` glob list whose matched files the
move changes. It leaves, and lists, references to `crates/apps` or
`crates/libs` themselves (`crates/libs/**`), which still hold the crates that
stay. It also leaves an unquoted bare `..`
(Rust's `..Default::default()`, prose), a climb after a format string's other
`{...}` (`format!("{base}/../x")`, which says nothing about what it holds), a
path that names nothing tracked and no moved crate, such as a test's synthetic
paths, and a climb in a file that stays after a shell expansion.

--verify compares the workspace with REV's (default HEAD), for every row: the
same packages modulo their renames, each at its mapped directory and every
other crate where it was; at least one crate moved; git's
detected rename pairs exactly the move's for regular files (identical
symlinks, such as the `CLAUDE.md` links, can't be told apart and git pairs them
arbitrarily, so they are listed instead, and an open edit to one may need
re-pointing); no tracked file outside `[stale_paths]` naming a moved crate's
old directory or a directory the move emptied, holding a `..` path that
reached a tracked path from the file's old place and reaches none from its new
one (read from the same bases --apply uses: the file's directory, the
manifest directory, or either), holding a climb after another shell expansion
in a file that moved, or containing a renamed package's or library's old name
(a plain substring search, skipping current names it begins); every tracked
symlink resolving; and `cargo arch` exiting 0. Both
--apply and --verify print the old -> new paths and package names for
downstream repositories.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import subprocess
import sys
import tempfile
import tomllib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

LAYERS = (
    "l10_foundation",
    "l20_domain",
    "l30_storage",
    "l40_api",
    "l50_app",
    "test_support",
    "tools",
)
PREFIX = "pplx-space-"
OLD_ROOTS = ("crates/apps", "crates/libs")
DEFAULT_MAPPING = "scripts/move_crates.toml"
BASELINE = "architecture-baseline.toml"
STAY = "stay"

# A repository path into the old directories, after an optional run of `../`.
OLD_PATH = r"(?:\./)?(?:\.\./)*crates/(?:apps|libs)(?:/[A-Za-z0-9_.*\-]+)*/?"
# A path that climbs: `..` segments, then any path.
RELATIVE = r"(?:\./)?\.\.(?:/\.\.)*(?:/[A-Za-z0-9_.*\-]+)*/?"
TOKEN = re.compile(rf"{OLD_PATH}|{RELATIVE}")
WORD = re.compile(r"[A-Za-z0-9_\-.]")
GLOB_CHARS = re.compile(r"[*?\[{]")
# A line that holds one path and list punctuation only: repeated per target.
LIST_LINE = re.compile(r"""^(?P<head>\s*(?:-\s+)?(?P<q>["']?))(?P<tok>\S+?)(?P<tail>(?P=q),?\s*(?:#.*)?)$""")
COMMENT_LINE = re.compile(r"^\s*(?:#|//|/\*|\*|--|<!--)")
# Shell expansions of a script's own directory, which a climb may follow.
SCRIPT_DIRS = (
    "${SCRIPT_DIR}/",
    "$SCRIPT_DIR/",
    '$(dirname "$0")/',
    '$(dirname "${BASH_SOURCE[0]}")/',
    '$(dirname "$BASH_SOURCE")/',
    "${BASH_SOURCE%/*}/",
)
OLD_ROOT = re.compile(r"crates/(?:apps|libs)/")
# A shell expansion ending the text before a climb's `/`: `$X`, `${X}`, `$(...)`.
EXPANSION = re.compile(r"\$(?:\w+|\{[^}]*\}|\([^)]*\))$")
# A Rust format string that starts with the manifest directory:
# `format!("{}/../x", env!("CARGO_MANIFEST_DIR"))`, the argument on its line or the next.
MANIFEST_FORMAT_HEAD = re.compile(r'"\{\}$')
MANIFEST_FORMAT_ARG = re.compile(r'^[^"]*"\s*,\s*env!\(\s*"CARGO_MANIFEST_DIR"\s*\)')


def climb_prefix(line: str, start: int, end: int, next_line: str = "") -> str | None:
    """What a climb right after `/` is relative to: a script-directory
    expansion, `"/` after the manifest directory, or `{}/` in a format
    string whose argument is the manifest directory; None for anything
    else."""
    head = line[:start]
    pre = next((p for p in (*SCRIPT_DIRS, '"/') if head.endswith(p)), None)
    if pre is not None:
        return pre
    if MANIFEST_FORMAT_HEAD.search(head[:-1]) and MANIFEST_FORMAT_ARG.match(line[end:] + next_line.lstrip()):
        return "{}/"
    return None


class MoveError(Exception):
    pass


@dataclass(frozen=True)
class Crate:
    name: str
    dir: str  # repository-relative
    lib: str | None  # the library target's Rust name, if any
    bins: tuple[str, ...]


@dataclass
class Row:
    to: str | None  # None: stays
    rename: str | None = None
    reason: str | None = None  # why a crate that is not legacy stays


@dataclass
class Mapping:
    crates: dict[str, Row] = field(default_factory=dict)
    files: dict[str, str | None] = field(default_factory=dict)
    stale_paths: dict[str, str] = field(default_factory=dict)


# Workspace facts


def run(cmd: list[str], root: Path, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=root, check=check, capture_output=True, text=True)


def workspace(root: Path) -> dict[str, Crate]:
    out = run(["cargo", "metadata", "--no-deps", "--format-version", "1", "--offline"], root).stdout
    meta = json.loads(out)
    base = Path(meta["workspace_root"]).resolve()
    crates = {}
    for package in meta["packages"]:
        manifest = Path(package["manifest_path"]).resolve()
        rel = manifest.parent.relative_to(base).as_posix()
        lib = next(
            (t["name"] for t in package["targets"] if set(t["kind"]) & {"lib", "rlib", "proc-macro"}),
            None,
        )
        bins = tuple(t["name"] for t in package["targets"] if "bin" in t["kind"])
        crates[package["name"]] = Crate(package["name"], "" if rel == "." else rel, lib, bins)
    return crates


def architecture(root: Path) -> tuple[dict[str, str], set[str]]:
    """`[rules.layers.unmoved]` as package -> layer, and `legacy`."""
    rules = tomllib.loads((root / "ARCHITECTURE.toml").read_text())["rules"]["layers"]
    unmoved = {pkg: layer for layer, pkgs in rules.get("unmoved", {}).items() for pkg in pkgs}
    return unmoved, set(rules.get("legacy", []))


def baseline_packages(root: Path) -> dict[str, list[str]]:
    """Each package archcheck's package-keyed baseline tables name, with the
    tables: they have no rename map."""
    path = root / BASELINE
    if not path.exists():
        return {}
    data = tomllib.loads(path.read_text())
    named: dict[str, list[str]] = defaultdict(list)
    tables = {f"[layers.{t}]": v for t, v in data.get("layers", {}).items()}
    tables["[crate_names]"] = data.get("crate_names", {})
    for table, entries in tables.items():
        for key, value in entries.items():
            for package in [key, *(v.split("/")[0] for v in value if isinstance(value, list))]:
                if table not in named[package]:
                    named[package].append(table)
    return named


def tracked(root: Path, rev: str | None = None) -> list[str]:
    if rev is None:
        out = run(["git", "ls-files", "-z"], root).stdout
    else:
        out = run(["git", "ls-tree", "-r", "-z", "--name-only", rev], root).stdout
    return [p for p in out.split("\0") if p]


def with_ancestors(files: list[str]) -> set[str]:
    paths = {"."}
    for f in files:
        paths.add(f)
        d = posixpath.dirname(f)
        while d and d not in paths:
            paths.add(d)
            d = posixpath.dirname(d)
    return paths


def under(path: str, d: str) -> bool:
    return path == d or path.startswith(d + "/")


def layer_dir(path: str) -> str | None:
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "crates" and parts[1] in LAYERS:
        return parts[1]
    return None


def segments(package: str) -> list[str]:
    return package.removeprefix(PREFIX).split("-")


def name_dir(layer: str, package: str) -> str:
    return "/".join(["crates", layer, *segments(package)])


def leaf_of(layer: str, package: str, to: str) -> str | None:
    """The leaf `to` takes below the package's path, or None if it takes none;
    raises if `to` is neither."""
    expected = name_dir(layer, package)
    if to == expected:
        return None
    parent, leaf = posixpath.split(to)
    if parent == expected and leaf and "-" not in leaf:
        return leaf
    raise MoveError(
        f"{package}: {to} breaks the layout rule: below crates/{layer} its path is "
        f"{expected}, or one directory below that without `-`"
    )


# The mapping file


def load_mapping(path: Path) -> Mapping:
    if not path.exists():
        return Mapping()
    data = tomllib.loads(path.read_text())
    mapping = Mapping()
    for package, value in data.get("crates", {}).items():
        if value == STAY:
            mapping.crates[package] = Row(None)
        elif STAY in value:
            mapping.crates[package] = Row(None, reason=value[STAY])
        else:
            mapping.crates[package] = Row(value["to"], value.get("rename"))
    for old, new in data.get("files", {}).items():
        mapping.files[old] = None if new == STAY else new
    mapping.stale_paths = dict(data.get("stale_paths", {}))
    return mapping


def q(s: str) -> str:
    return json.dumps(s)


def dump_mapping(mapping: Mapping, notes: dict[str, str]) -> str:
    out = [
        "# Where each workspace crate moves, read by scripts/move_crates.py. Generated",
        "# by `scripts/move_crates.py --generate`, which keeps the choices below, then",
        "# reviewed. A crate's target is crates/<layer>/ and its package name without",
        "# `pplx-space-`, split on `-`, with a leaf where another crate's path would",
        "# nest inside it; `rename` changes the package name only, never a binary's.",
        "# A legacy crate stays until it is dissolved; another stays only for a",
        "# reviewed reason.",
        "",
        "[crates]",
    ]
    by_layer: dict[str, list[tuple[str, Row]]] = defaultdict(list)
    for package, row in mapping.crates.items():
        by_layer[layer_dir(row.to) if row.to else STAY].append((package, row))
    for layer in (*LAYERS, STAY):
        rows = sorted(by_layer.get(layer, []), key=lambda r: (r[1].to or "", r[0]))
        if not rows:
            continue
        out.append(f"# {layer} ({len(rows)})")
        for package, row in rows:
            if row.to is None and row.reason:
                line = f"{q(package)} = {{ {STAY} = {q(row.reason)} }}"
            elif row.to is None:
                line = f"{q(package)} = {q(STAY)}"
            else:
                fields = [f"to = {q(row.to)}"]
                if row.rename:
                    fields.append(f"rename = {q(row.rename)}")
                line = f"{q(package)} = {{ {', '.join(fields)} }}"
            if package in notes:
                line += f"  # {notes[package]}"
            out.append(line)
    out += [
        "",
        "# Tracked files below crates/apps and crates/libs outside a crate, moving",
        "# with the layer most of their family's crates go to.",
        "[files]",
    ]
    for old, new in sorted(mapping.files.items()):
        out.append(f"{q(old)} = {q(new or STAY)}")
    out += [
        "",
        "# Files the move leaves as they are, which may keep naming old paths, with why.",
        "[stale_paths]",
    ]
    for path, reason in sorted(mapping.stale_paths.items()):
        out.append(f"{q(path)} = {q(reason)}")
    return "\n".join(out) + "\n"


# Generation


def generate(root: Path, existing: Mapping) -> tuple[Mapping, dict[str, str], list[str]]:
    """The mapping for today's workspace, the per-row notes, and what changed."""
    crates = workspace(root)
    unmoved, legacy = architecture(root)
    baselined = baseline_packages(root)
    errors: list[str] = []
    changes: list[str] = []
    result = Mapping(stale_paths=dict(existing.stale_paths))
    names = set(crates)
    # A row stays keyed by the name it was reviewed under, so a crate an
    # earlier run renamed is found by its new name.
    renamed = {r.rename: k for k, r in existing.crates.items() if r.rename and k not in names}
    keys = {}
    for name, crate in sorted(crates.items()):
        layer = layer_dir(crate.dir) or unmoved.get(name)
        key = renamed.get(name, name)
        keys[key] = name
        row = existing.crates.get(key)
        if layer is None:
            errors.append(f"{name} at {crate.dir}: no layer (not in a layer directory or `unmoved`)")
            continue
        if name in legacy:
            if row is not None and row.to is not None:
                errors.append(f"{name} is legacy and stays, but its row moves it to {row.to}")
            result.crates[key] = Row(None)
            continue
        if row is not None and row.to is None:
            if row.reason:
                result.crates[key] = row
            else:
                errors.append(f"{name}: only a legacy crate stays without a reason")
            continue
        rename = row.rename if row else None
        if rename and key == name and rename in names:
            errors.append(f"{name}: rename to {rename}, which is another crate's name")
        if rename and key == name and name in baselined:
            errors.append(
                f"{name}: renaming it re-keys its entries in {BASELINE} {baselined[name]}, "
                "which no_new_entries reads as new; fix those edges first, or keep the crate "
                "in place with a reviewed `stay` reason"
            )
        new_name = rename or name
        leaf = None
        if row is not None:
            if layer_dir(row.to) != layer:
                errors.append(f"{name}: {row.to} is outside crates/{layer}, the crate's layer")
                continue
            try:
                leaf = leaf_of(layer, new_name, row.to)
            except MoveError as e:
                errors.append(str(e))
                continue
        to = name_dir(layer, new_name) + (f"/{leaf}" if leaf else "")
        if rename and key == name and name.replace("-", "_") != (crate.lib or name.replace("-", "_")):
            errors.append(f"{name}: library {crate.lib} has its own name; rename it by hand")
        if rename and name in crate.bins:
            errors.append(f"{name}: a binary takes the package name; give it [[bin]] name first")
        if row is None:
            changes.append(f"new row: {name} -> {to}")
        result.crates[key] = Row(to, rename)
    for key in sorted(set(existing.crates) - set(keys)):
        changes.append(f"dropped row for {key}, no longer in the workspace")

    final = {key: (row.to if row.to else crates[keys[key]].dir) for key, row in result.crates.items()}
    notes = {}
    by_dir = sorted(final.items(), key=lambda item: item[1])
    for outer, outer_dir in by_dir:
        for inner, inner_dir in by_dir:
            if inner != outer and inner_dir.startswith(outer_dir + "/"):
                errors.append(
                    f"{outer} at {outer_dir} would contain {inner} at {inner_dir}: "
                    f"give {outer} a reviewed leaf (`server` for a service)"
                )
    for dup, count in Counter(final.values()).items():
        if count > 1:
            errors.append(f"{count} crates map to {dup}")
    for name, row in result.crates.items():
        if row.to is None:
            continue
        layer = layer_dir(row.to)
        new_name = row.rename or name
        bits = []
        if row.rename:
            bits.append("renamed")
        if leaf_of(layer, new_name, row.to):
            plain = name_dir(layer, new_name)
            nested = sorted(n for n, d in final.items() if d.startswith(plain + "/") and n != name)
            bits.append(f"leaf: {', '.join(nested)} nest in {plain.removeprefix('crates/')}"
                        if nested else "leaf")
        if bits:
            notes[name] = "; ".join(bits)

    files = tracked(root)
    crate_dirs = [c.dir for c in crates.values() if c.dir]
    for f in files:
        if not f.startswith(tuple(r + "/" for r in OLD_ROOTS)):
            continue
        if any(under(f, d) for d in crate_dirs):
            continue
        if f in existing.files:
            result.files[f] = existing.files[f]
            continue
        target = family_target(f, crates, result)
        if target is None:
            errors.append(f"{f}: outside every crate family; map it in [files] by hand")
        else:
            result.files[f] = target
            changes.append(f"new file row: {f} -> {target}")
    tracked_now = set(files)
    for f in sorted(set(existing.files) - set(result.files)):
        if existing.files[f] in tracked_now:
            result.files[f] = existing.files[f]  # moved by an earlier run
        else:
            changes.append(f"dropped file row for {f}")
    if errors:
        raise MoveError("the mapping cannot be generated:\n  " + "\n  ".join(errors))
    return result, notes, changes


def family_target(f: str, crates: dict[str, Crate], mapping: Mapping) -> str | None:
    """Where a non-crate file goes: below the deepest directory holding it and
    crates, into the layer most of those crates move to."""
    d = posixpath.dirname(f)
    while d.count("/") >= 2:
        family = [n for n, c in crates.items() if c.dir.startswith(d + "/")]
        moving = [n for n in family if mapping.crates.get(n) and mapping.crates[n].to]
        if moving:
            votes = Counter(layer_dir(mapping.crates[n].to) for n in moving).most_common()
            if len(votes) > 1 and votes[0][1] == votes[1][1]:
                return None
            layer = votes[0][0]
            dirs = [mapping.crates[n].to for n in moving if layer_dir(mapping.crates[n].to) == layer]
            common = posixpath.commonpath(dirs) if len(dirs) > 1 else posixpath.dirname(dirs[0])
            if common.count("/") < 2:
                return None
            return posixpath.join(common, posixpath.relpath(f, d))
        d = posixpath.dirname(d)
    return None


# The move plan


@dataclass
class Plan:
    crates: dict[str, Crate]  # before the move
    moved: dict[str, str]  # old crate dir -> new
    files: dict[str, str]  # old non-crate file -> new
    renames: dict[str, str]  # package -> new name
    lib_renames: dict[str, str]  # Rust library name -> new
    old_files: list[str]
    old_paths: set[str]  # tracked files and their directories, before

    def __post_init__(self) -> None:
        self.crate_dirs = sorted((c.dir for c in self.crates.values() if c.dir), key=len, reverse=True)

    def owner(self, path: str) -> str | None:
        """The crate directory a path is in, before the move."""
        return next((d for d in self.crate_dirs if under(path, d)), None)

    def new_path(self, path: str) -> str:
        """Where a path (a file, a directory or a glob inside a crate) goes."""
        if path in self.files:
            return self.files[path]
        d = self.owner(path)
        if d is not None and d in self.moved:
            return self.moved[d] + path[len(d):]
        return path

    def path_lines(self) -> list[str]:
        return [f"{old} -> {new}" for old, new in sorted(self.moved.items())] + [
            f"{old} -> {new}" for old, new in sorted(self.files.items())
        ]


def plan(root: Path, mapping: Mapping) -> Plan:
    crates = workspace(root)
    moved, renames, lib_renames, files = {}, {}, {}, {}
    for name, row in mapping.crates.items():
        if row.to is None:
            continue
        crate = crates.get(name) or crates[row.rename]
        if crate.dir != row.to:
            moved[crate.dir] = row.to
        if row.rename and crate.name == name:
            renames[name] = row.rename
            if crate.lib:
                lib_renames[crate.lib] = row.rename.replace("-", "_")
    old_files = tracked(root)
    present = set(old_files)
    for old, new in mapping.files.items():
        if new and old in present:
            files[old] = new
    return Plan(crates, moved, files, renames, lib_renames, old_files, with_ancestors(old_files))


# Rewriting


@dataclass
class Rewriter:
    plan: Plan
    new_files: set[str] = field(default_factory=set)
    unrewritten: list[str] = field(default_factory=list)
    # The paths each rewritten file now names in place of old ones.
    replaced: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    # `crates/apps` and `crates/libs` themselves, left as they are.
    umbrella: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Directories the move empties, below an old root: families.
        self.families = {
            p for p in self.plan.old_paths
            if p.startswith(tuple(r + "/" for r in OLD_ROOTS))
            and self.plan.owner(p) is None and p not in self.plan.files
            and any(d.startswith(p + "/") for d in self.plan.crate_dirs)
        }

    def note(self, where: str, detail: str) -> None:
        self.unrewritten.append(f"{where}: {detail}")

    # Repository paths

    def map_repo(self, path: str) -> list[str] | None:
        """New forms of a repository path into the old directories: one, several
        for a directory of crates split across layers, or None to leave it."""
        suffix = ""
        literal = path
        glob = GLOB_CHARS.search(path)
        if glob:
            literal = path[: path.rfind("/", 0, glob.start())]
            suffix = path[len(literal):]
        elif path.endswith("/"):
            literal, suffix = path[:-1], "/"
        owner = self.plan.owner(literal)
        nested = [d for d in self.plan.crate_dirs if d.startswith(literal + "/")]
        if owner is not None and not (glob and nested):
            return [self.plan.new_path(literal) + suffix]
        if literal in self.plan.files:
            return [self.plan.files[literal] + suffix]
        if literal not in self.families and not (glob and nested):
            return None
        if literal.count("/") < 2:
            return None  # crates/apps or crates/libs itself: an umbrella reference
        files = set(self.plan.files.values())
        return [t if t in files and suffix.startswith("/") else t + suffix for t in self.expand(literal)]

    def expand(self, family: str) -> list[str]:
        """The directories a family directory's contents end up in."""
        rel = family.split("/", 2)[2]
        inside = [f for f in self.plan.old_files if f.startswith(family + "/")]
        targets: list[str] = []
        by_layer: dict[str, list[str]] = defaultdict(list)
        stays = False
        for f in inside:
            new = self.plan.new_path(f)
            if new == f:
                stays = True
            else:
                by_layer[layer_dir(new)].append(f)
        if stays:
            targets.append(family)
        for layer, olds in sorted(by_layer.items(), key=lambda kv: LAYERS.index(kv[0])):
            cand = f"crates/{layer}/{rel}"
            mine = {self.plan.new_path(f) for f in olds}
            theirs = {p for p in self.new_files if p.startswith(cand + "/")}
            if all(p.startswith(cand + "/") for p in mine) and theirs == mine:
                targets.append(cand)
            else:
                dirs = {self.plan.owner(f) for f in olds}
                for d in sorted(x for x in dirs if x):
                    targets.append(self.plan.moved.get(d, d))
                targets.extend(sorted(self.plan.files[f] for f in olds if f in self.plan.files))
        return targets

    # Relative paths

    def bases(self, old_file: str, lines: list[str], i: int, start: int, pre: str) -> list[str]:
        here = posixpath.dirname(old_file) or "."
        manifest = self.plan.owner(old_file)
        name = posixpath.basename(old_file)
        if pre in SCRIPT_DIRS:
            return [here]
        if manifest is None:
            return [here]
        if pre == "{}/":
            return [manifest]
        if name in ("Cargo.toml", "build.rs"):
            return [manifest]
        if name.endswith(".rs"):
            before = lines[i][:start]
            context = "".join(lines[max(0, i - 3): i]) + before
            if re.search(r"include(?:_str|_bytes)?!\s*\(\s*\"?$", "".join(lines[max(0, i - 1): i]) + before):
                return [here]
            if re.search(r"#\[path\s*=\s*\"$", before) or "snapshot_path" in before:
                return [here]
            if "migrate!" in before or "CARGO_MANIFEST_DIR" in context or pre == '"/':
                return [manifest]
            return [here, manifest]
        return [here]

    def map_relative(self, tok: str, bases: list[str], where: str) -> str | None:
        """A `..`-relative path recomputed for the file's and target's new
        places, or None to leave it. A path that names an old crate directory
        but reaches no tracked path from any base is noted."""
        literal, suffix = tok, ""
        glob = GLOB_CHARS.search(tok)
        if glob:
            cut = tok.rfind("/", 0, glob.start())
            literal, suffix = tok[:cut], tok[cut:]
        elif tok.endswith("/") and tok != "/":
            literal, suffix = tok[:-1], "/"
        lead = "./" if literal.startswith("./") else ""
        results = set()
        for base in bases:
            target = posixpath.normpath(posixpath.join(base, literal))
            if target.startswith("..") or target not in self.plan.old_paths:
                continue
            mapped = None if target == "." else self.map_repo(target + suffix)
            news = [n[: len(n) - len(suffix)] for n in mapped] if mapped else [target]
            if len(news) != 1:
                self.note(where, f"{tok} names {target}, split across {', '.join(news)}")
                return None
            new_base = self.new_base(base)
            if new_base == base and news[0] == target:
                results.add(tok)  # neither end moves
            else:
                results.add(lead + posixpath.relpath(news[0], new_base) + suffix)
        if len(results) > 1:
            self.note(where, f"{tok} resolves from both its file and its manifest directory")
            return None
        if not results:
            if (m := OLD_ROOT.search(literal)) and self.map_repo(literal[m.start():] + suffix):
                self.note(where, f"{tok} names a moved path but reaches nothing tracked from {', '.join(bases)}")
            return None
        new_tok = results.pop()
        return None if new_tok == tok else new_tok

    def new_base(self, base: str) -> str:
        if base == ".":
            return "."
        return self.plan.new_path(base)

    # Files

    def rewrite(self, old_file: str, new_file: str, text: str) -> str:
        lines = text.splitlines(keepends=True)
        out: list[str] = []
        toml = new_file.endswith(".toml")
        for i, line in enumerate(lines):
            where = f"{new_file}:{i + 1}"
            replacements: list[tuple[int, int, list[str]]] = []
            for m in TOKEN.finditer(line):
                start, end, tok = m.start(), m.end(), m.group(0)
                prev = line[start - 1] if start else ""
                if prev and WORD.match(prev):
                    continue
                if end < len(line) and WORD.match(line[end]):
                    continue  # `..Default::default()`, `/...`
                pre = ""
                if prev == "/":
                    following = lines[i + 1] if i + 1 < len(lines) else ""
                    pre = climb_prefix(line, start, end, following) or ""
                    if not pre and not tok.startswith("crates/"):
                        self.after_unknown_prefix(tok, line[: start - 1], old_file, new_file, where)
                        continue
                while tok.endswith(".") and not (tok.endswith("/..") or tok in ("..", "./..")):
                    tok, end = tok[:-1], end - 1  # a sentence's full stop
                if not re.search(r"[^./]", tok) and not (pre or prev in "\"'"):
                    continue  # an unquoted bare `..` is Rust syntax or prose
                if pre == '"/' and "CARGO_MANIFEST_DIR" not in "".join(lines[max(0, i - 3): i + 1]):
                    continue  # `"/../x"` joins the manifest directory only after it
                if end < len(line) and line[end] == "{":
                    # `dir/{a,b}.rs` inside one crate is a prefix rewrite; a
                    # brace list after a directory of crates is not.
                    path = tok[tok.index("crates/"):].rstrip("/") if "crates/" in tok else None
                    if path is None or self.plan.owner(path) is None:
                        if path and self.map_repo(path):
                            self.note(where, f"{tok}{{...}}: a brace list after a moved directory of crates")
                        continue
                if tok.startswith(("crates/", "./crates/")):
                    news = self.map_repo(tok[tok.index("crates/"):])
                    if news is not None and tok.startswith("./"):
                        news = ["./" + n for n in news]
                    if news is None and umbrella(tok[tok.index("crates/"):]):
                        self.umbrella.append(f"{where}: {tok}")
                else:
                    bases = self.bases(old_file, lines, i, start, pre)
                    if len(bases) > 1 and not re.search(r"[^./]", tok):
                        continue  # a bare climb with no telling where it starts
                    new = self.map_relative(tok, bases, where)
                    news = None if new is None else [new]
                if news is not None and news != [tok]:
                    replacements.append((start, end, news))
                    self.replaced[new_file].update(news)
            out.extend(self.apply_line(line, replacements, toml, new_file, where))
        return "".join(out)

    def relink(self, root: Path, old_file: str, new_file: str) -> bool:
        """Point a symlink that moved, or that points into a moved crate, at
        its target's new place; True if it changed."""
        path = root / new_file
        target = os.readlink(path)
        if posixpath.isabs(target):
            return False
        resolved = posixpath.normpath(posixpath.join(posixpath.dirname(old_file) or ".", target))
        if resolved.startswith(".."):
            return False
        if resolved not in self.plan.old_paths:
            if OLD_ROOT.search(resolved + "/"):
                self.note(new_file, f"a symlink to {target}, which reaches nothing tracked")
            return False
        news = (None if resolved == "." else self.map_repo(resolved)) or [resolved]
        if len(news) != 1:
            self.note(new_file, f"a symlink to {target}, split across {', '.join(news)}")
            return False
        new_target = posixpath.relpath(news[0], posixpath.dirname(new_file) or ".")
        if new_target == target:
            return False
        path.unlink()
        path.symlink_to(new_target)
        return True

    def after_unknown_prefix(self, tok: str, head: str, old_file: str, new_file: str, where: str) -> None:
        """A climb after `/` and something this tool can't place. After a
        shell expansion (`$CRATE_DIR/../x`) in a file that moves it is noted,
        bare or not: the expansion's directory is unknown. After a literal
        directory (`up/../x`) it is noted when, read from the file's directory,
        it reaches a tracked path the move changes. A format string's other
        `{}` or `{name}` is left: nothing says what it holds."""
        if m := EXPANSION.search(head):
            if old_file != new_file:
                self.note(where, f"{tok} follows {m.group(0)}, whose directory this tool can't tell")
            return
        if re.search(r"[^./]", tok) and not head.endswith("}"):
            here = posixpath.dirname(old_file) or "."
            target = posixpath.normpath(posixpath.join(here, tok))
            if target in self.plan.old_paths and (old_file != new_file or self.plan.new_path(target) != target):
                self.note(where, f"{tok} follows a path this tool can't place, and it reaches {target}")

    def apply_line(self, line: str, replacements, toml: bool, new_file: str, where: str) -> list[str]:
        if not replacements:
            return [line]
        multi = [r for r in replacements if len(r[2]) > 1]
        if multi:
            body = line.rstrip("\n")
            nl = line[len(body):]
            m = LIST_LINE.match(body)
            if len(replacements) == 1 and m and m.start("tok") == multi[0][0] and m.end("tok") == multi[0][1]:
                return [f"{m['head']}{t}{m['tail']}{nl}" for t in multi[0][2]]
        result = line
        for start, end, news in sorted(replacements, reverse=True):
            if len(news) == 1:
                new = news[0]
            elif toml and start and line[start - 1] in "\"'" and line[end:end + 1] == line[start - 1]:
                quote = line[start - 1]
                new = f"{quote}, {quote}".join(news)
            elif (new_file.endswith(".md") or COMMENT_LINE.match(line)) and brace(news):
                new = brace(news)
            else:
                self.note(where, f"{line[start:end]} splits into {', '.join(news)}")
                continue
            result = result[:start] + new + result[end:]
        return [result]


def umbrella(path: str) -> bool:
    """Whether a path is `crates/apps` or `crates/libs` itself, or a glob
    straight below one."""
    glob = GLOB_CHARS.search(path)
    literal = path[: path.rfind("/", 0, glob.start())] if glob else path
    return literal.rstrip("/") in OLD_ROOTS


def brace(paths: list[str]) -> str | None:
    """`crates/{a,b}/rest` for paths that differ only in their layer."""
    parts = [p.split("/", 2) for p in paths]
    if all(len(p) == 3 and p[0] == "crates" for p in parts) and len({p[2] for p in parts}) == 1:
        return f"crates/{{{','.join(p[1] for p in parts)}}}/{parts[0][2]}"
    return None


def rename_packages(text: str, plan: Plan) -> str:
    """Each renamed package and library name, bounded by characters that can't
    continue it, or by a regex `\\b` (the `b` of which would otherwise read as
    part of the name, as in an ARCHITECTURE.toml pattern)."""
    for old, new in plan.renames.items():
        text = re.sub(rf"(?:(?<![A-Za-z0-9_\-])|(?<=\\b)){re.escape(old)}(?![A-Za-z0-9_\-])", new, text)
    for old, new in plan.lib_renames.items():
        text = re.sub(rf"(?:(?<![A-Za-z0-9_])|(?<=\\b)){re.escape(old)}(?![A-Za-z0-9_])", new, text)
    return text


# ARCHITECTURE.toml and the baseline


def glob_regex(glob: str) -> re.Pattern:
    """globset's syntax with `literal_separator`, as archcheck reads it."""
    out, i = "", 0
    while i < len(glob):
        c = glob[i]
        if glob.startswith("**/", i):
            out += "(?:.*/)?"
            i += 3
        elif glob.startswith("/**", i) and i + 3 == len(glob):
            out += "(?:/.*)?"
            i += 3
        elif glob.startswith("**", i):
            out += ".*"
            i += 2
        elif c == "*":
            out += "[^/]*"
            i += 1
        elif c == "?":
            out += "[^/]"
            i += 1
        elif c == "{":
            close = glob.index("}", i)
            out += "(?:" + "|".join(re.escape(a) for a in glob[i + 1: close].split(",")) + ")"
            i = close + 1
        else:
            out += re.escape(c)
            i += 1
    return re.compile(out + r"\Z")


GLOB_LIST = re.compile(r"^(?P<key>include|allow)\s*=\s*\[(?P<body>[^\]]*)\]", re.M | re.S)
UNMOVED = re.compile(r"^\[rules\.layers\.unmoved\]\n(?P<body>.*?)(?=^\[|\Z)", re.M | re.S)


def fix_architecture(text: str, plan: Plan, new_globs: set[str], paths_after: set[str]) -> str:
    """Drop moved crates' `unmoved` entries, and each rewritten glob that
    another entry of its list covers."""
    moved = {n for n, c in plan.crates.items() if c.dir in plan.moved}
    moved |= {plan.renames[n] for n in moved if n in plan.renames}
    m = UNMOVED.search(text)
    if m:
        body = "".join(
            ln for ln in m["body"].splitlines(keepends=True)
            if not ((e := re.fullmatch(r'\s*"([^"]+)",?\s*', ln)) and e[1] in moved)
        )
        body = re.sub(r"^\w+ = \[\n?\]\n", "", body, flags=re.M)
        text = text[: m.start("body")] + body + text[m.end("body"):]

    def matches(glob: str) -> set[str]:
        rx = glob_regex(glob)
        return {p for p in paths_after if rx.match(p)}

    def dedupe(lm: re.Match) -> str:
        entries = re.findall(r'"([^"]*)"', lm["body"])
        drop = []
        for e in entries:
            if e not in new_globs:
                continue
            mine = matches(e)
            if mine and any(o != e and o not in drop and mine <= matches(o) for o in entries):
                drop.append(e)
        if not drop:
            return lm.group(0)
        body = lm["body"]
        if "\n" in body:
            for e in drop:
                body = re.sub(rf'(?m)^[ \t]*"{re.escape(e)}",?[ \t]*(?:#.*)?\n', "", body, count=1)
        else:
            body = ", ".join(f'"{e}"' for e in entries if e not in drop)
        return lm.group(0)[: lm.start("body") - lm.start()] + body + lm.group(0)[lm.end("body") - lm.start():]

    return GLOB_LIST.sub(dedupe, text)


def sort_tables(text: str, tables: set[str]) -> str:
    """Re-sort the entries (and array items) of the named tables, as archcheck
    writes them: by key, each a key line and its continuation."""
    parts = re.split(r"(?m)^(\[[^\]\n]+\]\n)", text)
    out = [parts[0]]
    for header, body in zip(parts[1::2], parts[2::2]):
        name = header.strip()[1:-1]
        if name in tables:
            body = sort_body(body)
        out += [header, body]
    return "".join(out)


KEY_LINE = re.compile(r'^"?([^\s"=#]+)"?\s*=')


def sort_body(body: str) -> str:
    """Sort a table's entries by key without moving its comments off the
    entries they describe: comment and blank lines head a group with the
    entries after them, up to the next comment or blank line. Entries sort
    within their group and groups by their first key, after any entries
    before the first comment; lines after the last entry stay at the end."""
    groups: list[tuple[list[str], list[tuple[str, list[str]]]]] = []
    pending: list[str] = []
    open_array = False
    for ln in body.splitlines(keepends=True):
        if open_array:
            groups[-1][1][-1][1].append(ln)
            open_array = ln.strip() != "]"
        elif m := KEY_LINE.match(ln):
            if pending or not groups:
                groups.append((pending, []))
                pending = []
            groups[-1][1].append((m[1], [ln]))
            open_array = ln.rstrip().endswith("[")
        else:
            pending.append(ln)

    def items(lines: list[str]) -> list[str]:
        """A multi-line array's items, sorted, when no comment is among them."""
        inner = lines[1:-1]
        if lines[0].rstrip().endswith("[") and inner and not any(ln.lstrip().startswith("#") for ln in inner):
            return [lines[0], *sorted(inner), lines[-1]]
        return lines

    # Entries before the first comment have no heading, so they stay first:
    # anywhere else they would read as part of the group above them.
    lead = [groups.pop(0)] if groups and not groups[0][0] else []
    out = []
    for head, entries in lead + sorted(groups, key=lambda g: min(k for k, _ in g[1])):
        out += head
        for _, lines in sorted(entries, key=lambda e: e[0]):
            out += items(lines)
    return "".join(out) + "".join(pending)


def glob_changes(text: str, before: str, plan: Plan, files_after: set[str]) -> list[str]:
    """ARCHITECTURE.toml glob lists whose matched tracked files are not their
    old matches, moved."""
    def lists(t: str) -> list[tuple[str, str, list[str]]]:
        found = []
        for m in GLOB_LIST.finditer(t):
            headers = re.findall(r"(?m)^\[([^\]\n]+)\]", t[: m.start()])
            header = headers[-1] if headers else ""
            found.append((header, m["key"], re.findall(r'"([^"]*)"', m["body"])))
        return found

    def union(globs: list[str], paths) -> set[str]:
        rxs = [glob_regex(g) for g in globs]
        return {p for p in paths if any(rx.match(p) for rx in rxs)}

    problems = []
    old_lists, new_lists = lists(before), lists(text)
    if [(h, k) for h, k, _ in old_lists] != [(h, k) for h, k, _ in new_lists]:
        return ["ARCHITECTURE.toml: its glob lists changed shape"]
    for (header, key, old), (_, _, new) in zip(old_lists, new_lists):
        was = {plan.new_path(f) for f in union(old, plan.old_files)}
        now = union(new, files_after)
        if was != now:
            detail = []
            if now - was:
                detail.append(f"now also matches {len(now - was)} files, such as {min(now - was)}")
            if was - now:
                detail.append(f"no longer matches {len(was - now)} files, such as {min(was - now)}")
            problems.append(f"ARCHITECTURE.toml: [{header}] {key} {'; '.join(detail)}")
    return problems


# Commands


def text_of(data: bytes) -> str | None:
    """A file's text, or None for a binary (a NUL in its first 8 KiB) or one
    that is not UTF-8."""
    if b"\0" in data[:8192]:
        return None
    try:
        return data.decode()
    except UnicodeDecodeError:
        return None


PLUGIN_VERSIONS = (
    ("plugin.yaml", re.compile(r"(?m)^(version:\s*['\"]?)(\d+)\.(\d+)\.(\d+)")),
    (".claude-plugin/plugin.json", re.compile(r'("version":\s*")(\d+)\.(\d+)\.(\d+)')),
    (".codex-plugin/plugin.json", re.compile(r'("version":\s*")(\d+)\.(\d+)\.(\d+)')),
)


def bump_plugins(root: Path, changed: list[str]) -> list[str]:
    """Bump the patch version of each agent plugin whose shipped content the
    rewrite changed, in its three manifests (.agents/plugins/AGENTS.md)."""
    plugins = sorted({f.split("/")[2] for f in changed if f.startswith(".agents/plugins/") and f.count("/") >= 3})
    lines = []
    for plugin in plugins:
        base = root / ".agents/plugins" / plugin
        for name, rx in PLUGIN_VERSIONS:
            path = base / name
            if not path.exists():
                continue
            text = path.read_text()
            m = rx.search(text)
            if m is None:
                raise MoveError(f"{path.relative_to(root)} has no version to bump")
            version = f"{m[2]}.{m[3]}.{int(m[4]) + 1}"
            path.write_text(text[: m.start()] + m[1] + version + text[m.end():])
        lines.append(f"plugin {plugin}: version bumped to {version}")
    return lines


def is_text(path: Path) -> str | None:
    if path.is_symlink() or not path.is_file():
        return None
    return text_of(path.read_bytes())


def apply(root: Path, mapping_path: Path) -> int:
    mapping = load_mapping(mapping_path)
    regenerated, _, changes = generate(root, mapping)
    if changes or regenerated.crates != mapping.crates or regenerated.files != mapping.files:
        raise MoveError(
            "the mapping is out of date with the workspace; run --generate and review:\n  "
            + "\n  ".join(changes or ["rows differ from what --generate writes"])
        )
    if run(["git", "status", "--porcelain"], root).stdout.strip():
        raise MoveError("the tree has uncommitted changes; --apply needs a clean one")
    p = plan(root, mapping)
    if not p.moved and not p.files:
        print("nothing to move")
        return 0
    for outer in p.moved:
        left = [d for d in p.crate_dirs if d.startswith(outer + "/") and d not in p.moved]
        if left:
            raise MoveError(f"{outer} holds {', '.join(left)}, which stays: move it too")
    before_arch = (root / "ARCHITECTURE.toml").read_text()

    # Move: the deepest crate first, so a crate's directory holds only its
    # own files when it moves.
    for old in sorted(p.moved, key=lambda d: d.count("/"), reverse=True):
        new = p.moved[old]
        (root / new).parent.mkdir(parents=True, exist_ok=True)
        run(["git", "mv", old, new], root)
    for old, new in sorted(p.files.items()):
        (root / new).parent.mkdir(parents=True, exist_ok=True)
        run(["git", "mv", old, new], root)
    for d in sorted(p.old_paths, key=lambda d: d.count("/"), reverse=True):
        if d.startswith(tuple(r + "/" for r in OLD_ROOTS)) and (root / d).is_dir():
            try:
                (root / d).rmdir()
            except OSError:
                pass

    new_of = {f: p.new_path(f) for f in p.old_files}
    rw = Rewriter(p, set(new_of.values()))
    # The mapping records the crates as they were; the allowlist names files
    # that must keep naming them.
    left_alone = {mapping_path.relative_to(root).as_posix(), *mapping.stale_paths}
    needles = [n.encode() for n in (*p.moved, *p.files, *p.renames, *p.lib_renames)]
    not_utf8, changed = [], []
    for old, new in sorted(new_of.items(), key=lambda kv: kv[1]):
        path = root / new
        if new in left_alone or not (path.is_file() or path.is_symlink()):
            continue
        if path.is_symlink():
            if rw.relink(root, old, new):
                changed.append(new)
            continue
        data = path.read_bytes()
        text = text_of(data)
        if text is None:
            kind = "binary" if b"\0" in data[:8192] else "not UTF-8"
            if any(n in data for n in needles):
                rw.note(new, f"{kind}, so not rewritten, but it names a moved path or renamed package")
            elif kind == "not UTF-8":
                not_utf8.append(new)
            continue
        updated = rename_packages(rw.rewrite(old, new, text), p)
        if new == "ARCHITECTURE.toml":
            updated = fix_architecture(updated, p, rw.replaced[new], with_ancestors(sorted(rw.new_files)))
        if new in ("ARCHITECTURE.toml", "architecture-baseline.toml"):
            updated = sort_tables(updated, rekeyed_tables(text, updated))
        if updated != text:
            path.write_text(updated)
            changed.append(new)
    bumped = bump_plugins(root, changed)

    problems = list(rw.unrewritten)
    problems += glob_changes((root / "ARCHITECTURE.toml").read_text(), before_arch, p, rw.new_files)
    # Cargo rewrites the lock file in its own order; only workspace members,
    # which it resolves from the manifests, change.
    run(["cargo", "update", "--workspace", "--offline"], root)
    run(["git", "add", "-u"], root)
    print_lists(p)
    for line in bumped:
        print(line)
    if rw.umbrella:
        print(f"left as they are: {len(rw.umbrella)} reference(s) to crates/apps or crates/libs itself:")
        for line in rw.umbrella:
            print(f"  {line}")
    if not_utf8:
        print(f"not UTF-8, and naming nothing that moved ({len(not_utf8)}):")
        for f in not_utf8:
            print(f"  {f}")
    if problems:
        print(f"\n{len(problems)} reference(s) not rewritten:", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return 1
    return 0


def rekeyed_tables(before: str, after: str) -> set[str]:
    """The tables whose keys the rewrite changed."""
    def keys(text: str) -> dict[str, list[str]]:
        result: dict[str, list[str]] = defaultdict(list)
        current = ""
        for ln in text.splitlines():
            if m := re.match(r"^\[([^\]]+)\]$", ln):
                current = m[1]
            elif m := re.match(r'^"?([^\s"=#]+)"?\s*=', ln):
                result[current].append(m[1])
        return result

    b, a = keys(before), keys(after)
    return {t for t in a if a[t] != b.get(t)}


def print_lists(p: Plan) -> None:
    lines = p.path_lines()
    print(f"paths ({len(lines)}):")
    for ln in lines:
        print(f"  {ln}")
    print(f"packages ({len(p.renames)}):")
    for old, new in sorted(p.renames.items()):
        print(f"  {old} -> {new}")


def base_crates(root: Path, rev: str) -> dict[str, str]:
    """Package -> directory at `rev`, from `cargo metadata` on a checkout."""
    with tempfile.TemporaryDirectory(prefix="move-crates-base-") as tmp:
        checkout = Path(tmp) / "base"
        run(["git", "worktree", "add", "--detach", "--quiet", str(checkout), rev], root)
        try:
            return {name: crate.dir for name, crate in workspace(checkout).items()}
        finally:
            run(["git", "worktree", "remove", "--force", str(checkout)], root)


CLIMB = re.compile(RELATIVE)
OLD_PATH_MENTION = re.compile(r"(?<![A-Za-z0-9_\-])crates/(?:apps|libs)(?:/[A-Za-z0-9_.\-]+)*")


def climbs(line: str, next_line: str = ""):
    """The `..` paths in a line that start where a path can, as `(path,
    base)`: base "dir" after a script-directory expansion, "manifest" after
    the manifest directory (`"/`, a `{}/` format of it), "unknown" after
    another shell expansion, and "either" for one that starts the path. Bare
    climbs, which every directory has, are left out, and so are climbs inside
    a longer literal path or after a format string's other `{...}`, as
    --apply leaves them."""
    for m in CLIMB.finditer(line):
        start, end = m.start(), m.end()
        tok = m.group(0) if m.group(0).endswith("/..") else m.group(0).rstrip(".")
        prev = line[start - 1] if start else ""
        if prev and WORD.match(prev) or end < len(line) and WORD.match(line[end]):
            continue
        base = "either"
        if prev == "/":
            pre = climb_prefix(line, start, end, next_line)
            if pre in SCRIPT_DIRS:
                base = "dir"
            elif pre is not None:
                base = "manifest"
            elif EXPANSION.search(line[: start - 1]):
                base = "unknown"
            else:
                continue
        if re.search(r"[^./]", tok):
            yield tok, base


def owner_in(path: str, dirs: list[str]) -> str | None:
    return next((d for d in sorted(dirs, key=len, reverse=True) if under(path, d)), None)


def verify(root: Path, mapping_path: Path, rev: str, skip_arch: bool) -> int:
    mapping = load_mapping(mapping_path)
    before = base_crates(root, rev)
    after = {n: c.dir for n, c in workspace(root).items()}
    targets = {n: r.to for n, r in mapping.crates.items() if r.to}
    renames = {n: r.rename for n, r in mapping.crates.items() if r.rename}
    problems = []
    expected = {renames.get(n, n) for n in before}
    if set(after) != expected:
        missing = sorted(expected - set(after))
        extra = sorted(set(after) - expected)
        problems.append(f"packages differ from {rev}'s modulo the renames: missing {missing}, new {extra}")
    moved = {}
    for name, old_dir in before.items():
        new_name = renames.get(name, name)
        want = targets.get(name, old_dir)
        new_dir = after.get(new_name)
        if new_dir is not None and new_dir != want:
            problems.append(f"{new_name} is at {new_dir}, not {want}")
        if new_dir and new_dir != old_dir:
            moved[old_dir] = new_dir
    old_files = tracked(root, rev)
    files_now = tracked(root)
    old_set, now_set = set(old_files), set(files_now)
    file_moves = {
        old: new for old, new in mapping.files.items()
        if new and old in old_set and new in now_set
    }
    if not moved and not file_moves:
        problems.append(f"no crate moved relative to {rev}: pass the revision before the move as --base")

    def new_of(old: str) -> str:
        if old in file_moves:
            return file_moves[old]
        d = owner_in(old, list(before.values()))
        return moved[d] + old[len(d):] if d in moved else old

    back = {new: old for old, new in file_moves.items()}
    back_dirs = {new: old for old, new in moved.items()}

    def old_of(new: str) -> str:
        if new in back:
            return back[new]
        d = owner_in(new, list(after.values()))
        return back_dirs[d] + new[len(d):] if d in back_dirs else new

    # git's rename pairs, which carry open changes across, must be the plan's.
    diff = run(["git", "diff", "-z", "--name-status", "--find-renames", "-l20000", rev], root)
    if "rename" in diff.stderr.lower():
        problems.append(f"git skipped rename detection: {diff.stderr.strip()}")
    fields = diff.stdout.split("\0")
    deleted, added, mispaired, alike = set(), set(), [], []
    i = 0
    while i < len(fields) - 1:
        status = fields[i]
        if status.startswith(("R", "C")):
            old, new = fields[i + 1], fields[i + 2]
            if status.startswith("R") and new_of(old) != new:
                planned = root / new_of(old)
                if planned.is_symlink() and (root / new).is_symlink() and os.readlink(planned) == os.readlink(root / new):
                    alike.append(f"{old} -> {new}")
                else:
                    mispaired.append(f"{old} -> {new}, but the move puts it at {new_of(old)}")
            i += 3
        else:
            (deleted if status == "D" else added if status == "A" else set()).add(fields[i + 1])
            i += 2
    unpaired = sorted(o for o in deleted if new_of(o) != o and new_of(o) in added)
    if alike:
        # A symlink's content is its target: git can't tell identical ones
        # apart, and a change to one can only retarget it.
        print(
            f"identical symlinks, which git pairs arbitrarily, not checked against the move "
            f"({len(alike)}; an open edit to one may need re-pointing):"
        )
        for line in alike:
            print(f"  {line}")
    if mispaired:
        problems.append(f"{len(mispaired)} rename(s) git pairs differently from the move:\n    " + "\n    ".join(mispaired))
    if unpaired:
        problems.append(f"{len(unpaired)} moved file(s) git does not pair as renames:\n    "
                        + "\n    ".join(f"{o} -> {new_of(o)}" for o in unpaired))

    old_paths, now_paths = with_ancestors(old_files), with_ancestors(files_now)
    gone = set(moved) | set(file_moves)
    for path in old_paths:
        if (path.startswith(tuple(r + "/" for r in OLD_ROOTS)) and path not in now_paths
                and any(under(d, path) for d in moved)):
            gone.add(path)
    old_names = {o for o in renames if o not in after}
    old_names |= {o.replace("-", "_") for o in old_names}
    current = set(after) | {n.replace("-", "_") for n in after}
    stale = []
    for f in files_now:
        if f in mapping.stale_paths:
            continue
        text = is_text(root / f)
        if text is None:
            continue
        old_f = old_of(f)
        # Each base kind's directories, before the move and now.
        then_dir, now_dir = posixpath.dirname(old_f) or ".", posixpath.dirname(f) or "."
        then_manifest = owner_in(old_f, list(before.values())) or "."
        now_manifest = owner_in(f, list(after.values())) or "."
        bases = {
            "dir": ([then_dir], [now_dir]),
            "manifest": ([then_manifest], [now_manifest]),
            "either": ([then_dir, then_manifest], [now_dir, now_manifest]),
        }
        lines = text.splitlines()
        for i, line in enumerate(lines, 1):
            for m in OLD_PATH_MENTION.finditer(line):
                if any(under(m.group(0).rstrip("."), d) for d in gone):
                    stale.append(f"{f}:{i}: {m.group(0)}")
            for tok, base in climbs(line, lines[i] if i < len(lines) else ""):
                if base == "unknown":
                    if old_f != f:
                        stale.append(f"{f}:{i}: {tok} follows an expansion whose directory can't be checked")
                    continue
                glob = GLOB_CHARS.search(tok)
                literal = (tok[: glob.start()] if glob else tok).rstrip("/")
                then_bases, now_bases = bases[base]
                then = any(posixpath.normpath(posixpath.join(b, literal)) in old_paths for b in then_bases)
                now = any(posixpath.normpath(posixpath.join(b, literal)) in now_paths for b in now_bases)
                if then and not now:
                    stale.append(f"{f}:{i}: {tok} reached a tracked path from {old_f} and reaches none now")
            for name in old_names:
                at = line.find(name)
                while at != -1:
                    if not any(len(c) > len(name) and line.startswith(c, at) for c in current):
                        stale.append(f"{f}:{i}: {name}")
                        break
                    at = line.find(name, at + 1)
    if stale:
        problems.append(f"{len(stale)} reference(s) to old paths or names:\n    " + "\n    ".join(stale))
    for f in files_now:
        path = root / f
        if path.is_symlink() and not path.exists():
            problems.append(f"{f}: symlink to {os.readlink(path)} does not resolve")
    if not skip_arch:
        arch = run(["cargo", "arch"], root, check=False)
        if arch.returncode != 0:
            problems.append(f"cargo arch exited {arch.returncode}:\n{arch.stdout}{arch.stderr}")
    print(f"paths ({len(moved) + len(file_moves)}):")
    for old, new in sorted({**moved, **file_moves}.items()):
        print(f"  {old} -> {new}")
    print(f"packages ({len(renames)}):")
    for old, new in sorted(renames.items()):
        print(f"  {old} -> {new}")
    if mapping.stale_paths:
        print(f"stale-path allowlist ({len(mapping.stale_paths)}):")
        for path, reason in sorted(mapping.stale_paths.items()):
            print(f"  {path}: {reason}")
    if problems:
        print("\nverify failed:", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("verify: ok")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--generate", action="store_true", help="refresh the mapping from the workspace")
    mode.add_argument("--apply", action="store_true", help="move the mapped crates and rewrite references")
    mode.add_argument("--verify", action="store_true", help="check a move against --base")
    parser.add_argument("--mapping", default=DEFAULT_MAPPING, help=f"mapping file (default: {DEFAULT_MAPPING})")
    parser.add_argument("--base", default="HEAD", help="revision before the move, for --verify (default: HEAD)")
    parser.add_argument("--skip-arch", action="store_true", help="do not run `cargo arch` in --verify")
    parser.add_argument("--repo", default=".", help="repository root (default: .)")
    args = parser.parse_args(argv)
    root = Path(args.repo).resolve()
    mapping_path = root / args.mapping
    try:
        if args.generate:
            mapping, notes, changes = generate(root, load_mapping(mapping_path))
            mapping_path.write_text(dump_mapping(mapping, notes))
            for line in changes:
                print(line)
            counts = Counter(layer_dir(r.to) if r.to else STAY for r in mapping.crates.values())
            print(", ".join(f"{layer} {counts[layer]}" for layer in (*LAYERS, STAY) if counts[layer]))
            return 0
        if args.apply:
            return apply(root, mapping_path)
        return verify(root, mapping_path, args.base, args.skip_arch)
    except MoveError as e:
        print(f"move_crates: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
