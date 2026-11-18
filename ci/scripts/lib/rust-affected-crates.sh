#!/usr/bin/env bash
# Which workspace crates a change can affect, so that the clippy job skips a
# lint of named crates that the change cannot reach: run_clippy_checks asks
# lint_affects_any whether to run the sbx-mgr production checks. Pull CI and
# the merge queue pass the base check-needed.sh decided against as
# SPACE_RUST_LINT_BASE (ci/check-scopes/README.md); Main CI passes none, and
# every check runs.
#
# It lives apart from run-rust-checks.sh so that a change to which crates it
# picks keeps the Rust caches: their keys hash run-rust-checks.sh (see "Name
# the Cargo target directory" and "Name the Rust compiler cache" in
# .github/workflows/common-ci.yml).

# The crates of LISTED (one per line) that the change since BASE can affect,
# one per line: those whose own directory changed, those that include a
# changed file from another crate's directory (include_str!, include_bytes!),
# and every workspace crate that depends on one of them, through any kind of
# dependency. A change to Cargo.lock affects the workspace crates whose
# resolved dependency tree holds a package it changed (see
# lock_affected_crates below). Prints every listed crate when the change
# touches any other path outside all crates that a crate's lint may read (the
# root manifest, the toolchain, Cargo's configuration, proto/, db/,
# templates/, ...), since Cargo, a build script or an include can read it;
# ci/check-scopes/rust.skip and UNREAD_BY_LINT below name the paths none
# reads. Main CI passes no base and lints all.
affected_lint_crates() {
    local metadata="$1" base="$2" listed="$3"
    python3 - "${metadata}" "${base}" "${listed}" <<'PY'
import fnmatch
import json
import os
import re
import subprocess
import sys
import tomllib

metadata, base, listed = sys.argv[1], sys.argv[2], sys.argv[3].split()
with open(metadata) as metadata_file:
    meta = json.load(metadata_file)
root = meta["workspace_root"]
packages = {p["name"]: p for p in meta["packages"]}
directory = {
    name: os.path.relpath(os.path.dirname(p["manifest_path"]), root)
    for name, p in packages.items()
}
# Innermost first, so a crate nested in another's directory owns its files.
by_depth = sorted(directory.items(), key=lambda item: -len(item[1]))


def owner(path):
    for name, crate_dir in by_depth:
        if path == crate_dir or path.startswith(crate_dir + "/"):
            return name
    return None


def unread_by_rust(path):
    """Whether ci/check-scopes/rust.skip says no Rust check reads PATH, with
    check-needed.sh's rules: `*` crosses `/`, the last matching line wins."""
    matched = False
    with open(os.path.join(root, "ci/check-scopes/rust.skip")) as scope:
        for line in scope:
            pattern = line.split("#", 1)[0].strip()
            if pattern.startswith("!"):
                if fnmatch.fnmatchcase(path, pattern[1:]):
                    matched = False
            elif pattern and fnmatch.fnmatchcase(path, pattern):
                matched = True
    return matched


# Outside every crate, paths that a Rust check may read but no crate's lint
# does: archcheck's files, nextest's configuration, the workflows, and CI
# scripts other than the lint's own (run-rust-checks.sh, this file and the
# image it runs in, ci/Dockerfile.ci).
UNREAD_BY_LINT = (
    "architecture-baseline.toml",
    "ARCHITECTURE.toml",
    ".config/nextest.toml",
    ".coderabbit.yaml",
    ".gitleaks.toml",
    "lefthook.yaml",
    "Tiltfile",
)
# The workflow sets the lint's command and environment, so it counts too.
LINT_INPUTS_IN_CI = (
    ".github/workflows/rust-static-ci.yml",
    "ci/Dockerfile.ci",
    "ci/scripts/run-rust-checks.sh",
    "ci/scripts/lib/rust-affected-crates.sh",
)


def unread_by_lint(path):
    if path in LINT_INPUTS_IN_CI:
        return False
    if path in UNREAD_BY_LINT or path.startswith(".github/"):
        return True
    return path.startswith("ci/")


# Root files that configure every crate's build, whatever Cargo.lock says:
# the root manifest holds [workspace.dependencies], [patch] and the profiles.
def configures_every_build(path):
    return path in ("Cargo.toml", "rust-toolchain", "rust-toolchain.toml") or (
        fnmatch.fnmatchcase(path, ".cargo/config*")
    )


def lint_every_crate(reason):
    print(f"{reason}: linting every crate.", file=sys.stderr)
    print("\n".join(listed))
    sys.exit(0)


def git(*args):
    return subprocess.run(
        ["git", "-c", f"safe.directory={root}", *args],
        cwd=root, stdout=subprocess.PIPE, check=False,
    )


def lock_packages(revision):
    """Cargo.lock at REVISION: its [[package]] entries by (name, version,
    source), and its other top-level fields; None if it cannot be parsed."""
    shown = git("show", f"{revision}:Cargo.lock")
    if shown.returncode != 0:
        return None
    try:
        lock = tomllib.loads(shown.stdout.decode())
        entries = {}
        for package in lock.pop("package", []):
            key = (package["name"], package["version"], package.get("source"))
            if key in entries:
                return None
            entries[key] = package
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, KeyError, TypeError, AttributeError):
        return None
    return entries, lock


def lock_affected_crates():
    """The workspace crates whose resolved dependency tree holds a package
    that Cargo.lock changes since BASE: one it adds, or whose entry (version,
    source, checksum, dependencies) differs, workspace members included. A
    package the lock removes needs no search of its own: whatever depended on
    it now lists other dependencies or another version, and so changed too.
    None, with the reason printed, when the change cannot be told apart.

    The tree is the resolve graph of `cargo metadata --all-features` without
    --filter-platform: every feature of every member, every target platform,
    and normal, build and dev edges. Each lint configuration builds one crate
    with none or all of its features for the host, so its build is a subgraph
    of that crate's tree there; unifying the workspace's features can only add
    crates. `cargo tree -i` reads the same resolve, but takes one call per
    package and fails on a package it does not resolve."""
    before, after = lock_packages(base), lock_packages("HEAD")
    if before is None or after is None:
        print("Cannot parse Cargo.lock on both sides of the change.", file=sys.stderr)
        return None
    (base_entries, base_rest), (entries, rest) = before, after
    if base_rest != rest:
        print("Cargo.lock changes outside its package list.", file=sys.stderr)
        return None
    changes = sorted(key for key, entry in entries.items() if base_entries.get(key) != entry)
    removed = sorted(key for key in base_entries if key not in entries)

    resolved = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1", "--all-features"],
        cwd=root, stdout=subprocess.PIPE, check=False,
    )
    if resolved.returncode != 0:
        print("cargo metadata cannot resolve the workspace.", file=sys.stderr)
        return None
    graph = json.loads(resolved.stdout)
    key_of = {
        package["id"]: (package["name"], package["version"], package["source"])
        for package in graph["packages"]
    }
    id_of = {key: package_id for package_id, key in key_of.items()}
    dependents_of = {}
    for node in graph["resolve"]["nodes"]:
        for dependency in node["deps"]:
            dependents_of.setdefault(dependency["pkg"], set()).add(node["id"])
    members = set(graph["workspace_members"])

    def describe(key):
        name, version, source = key
        if key not in entries:
            return f"{name} {version} (removed)"
        if key in base_entries:
            if base_entries[key].get("dependencies") != entries[key].get("dependencies"):
                return f"{name} {version} (its dependencies)"
            return f"{name} {version} (its lock entry)"
        earlier = [k for k in removed if k[0] == name]
        if not earlier:
            return f"{name} {version} (added)"
        if all(k[1] == version for k in earlier):
            return f"{name} {version} (its source)"
        return f"{name} {', '.join(k[1] for k in earlier)} -> {version}"

    print(f"Cargo.lock changes {len(changes)} and removes {len(removed)} packages:", file=sys.stderr)
    for key in removed:
        if not any(k[0] == key[0] for k in changes):
            print(f"  {describe(key)}", file=sys.stderr)
    crates = set()
    for key in changes:
        if key not in id_of:
            print(f"cargo metadata does not resolve {describe(key)}.", file=sys.stderr)
            return None
        holders, pending = {id_of[key]}, [id_of[key]]
        while pending:
            for dependent in dependents_of.get(pending.pop(), set()) - holders:
                holders.add(dependent)
                pending.append(dependent)
        names = sorted(key_of[package_id][0] for package_id in holders & members)
        print(f"  {describe(key)}: in the tree of {len(names)}: {' '.join(names)}", file=sys.stderr)
        crates.update(names)
    print(
        f"Cargo.lock affects {len(crates)} workspace crates: {' '.join(sorted(crates))}",
        file=sys.stderr,
    )
    return crates


listing = git("diff", "--name-only", "--no-renames", "-z", base, "HEAD")
if listing.returncode != 0:
    lint_every_crate(f"Cannot list the change against {base}")
# surrogateescape: a path that is not UTF-8 still compares and matches.
changed = [
    path for path in listing.stdout.decode(errors="surrogateescape").split("\0") if path
]
for path in changed:
    if configures_every_build(path):
        lint_every_crate(f"{path} configures every crate's build")
outside = [
    p
    for p in changed
    if p != "Cargo.lock"
    and owner(p) is None
    and not unread_by_rust(p)
    and not unread_by_lint(p)
]
if outside:
    lint_every_crate(f"{outside[0]} is outside every crate")

affected = {owner(p) for p in changed} - {None}
if "Cargo.lock" in changed:
    from_lock = lock_affected_crates()
    if from_lock is None:
        lint_every_crate("Cannot tell which crates the Cargo.lock change affects")
    affected |= from_lock
# A crate whose source reads a changed file from another crate's directory:
# include!, include_str! or include_bytes! of a literal path, relative to the
# source file, or of concat!(env!("CARGO_MANIFEST_DIR"), "..."), relative to
# the crate, and #[path = "..."], relative to the source file. An include of
# concat!(env!("OUT_DIR"), ...) reads the crate's own build-script output,
# whose inputs outside every crate already check them all. Any other include
# of a computed path could read anything, so it checks every crate. An
# argument cannot start with a backslash, so `include_str!(\"...\")` is
# escaped text in a string literal, such as a test fixture's, not an include.
changed_set = set(changed)
literal = re.compile(r'\binclude(?:_str|_bytes)?!\(\s*"([^"]+)"|#\[path\s*=\s*"([^"]+)"\]')
manifest_relative = re.compile(
    r'\binclude(?:_str|_bytes)?!\(\s*concat!\(\s*env!\(\s*"CARGO_MANIFEST_DIR"\s*\)\s*,\s*"([^"]+)"\s*\)\s*\)'
)
computed = re.compile(r'\binclude(?:_str|_bytes)?!\(\s*(?=[^\s"\\])(?!concat!\(\s*env!\(\s*"OUT_DIR")')
for name, crate_dir in directory.items():
    for dirpath, dirnames, filenames in os.walk(os.path.join(root, crate_dir)):
        dirnames[:] = [d for d in dirnames if d != "target"]
        for filename in filenames:
            if not filename.endswith(".rs"):
                continue
            source = os.path.join(dirpath, filename)
            with open(source, errors="ignore") as text:
                code = text.read()
            targets = [
                os.path.join(dirpath, a or b) for a, b in literal.findall(code)
            ] + [
                os.path.join(root, crate_dir) + "/" + relative
                for relative in manifest_relative.findall(code)
            ]
            for target in targets:
                target = os.path.relpath(os.path.normpath(target), root)
                if target in changed_set and owner(target) != name:
                    affected.add(name)
            if len(computed.findall(code)) > len(manifest_relative.findall(code)):
                relative_source = os.path.relpath(source, root)
                lint_every_crate(f"{relative_source} includes a path this cannot resolve")
dependents = {}
for name, package in packages.items():
    for dependency in package["dependencies"]:
        if dependency["name"] in packages:
            dependents.setdefault(dependency["name"], set()).add(name)
pending = list(affected)
while pending:
    for dependent in dependents.get(pending.pop(), set()) - affected:
        affected.add(dependent)
        pending.append(dependent)
for name in listed:
    if name in affected:
        print(name)
print(
    f"The change affects {len(affected)} workspace crates; "
    f"{sum(name in affected for name in listed)} of {len(listed)} listed crates need linting alone.",
    file=sys.stderr,
)
PY
}

# Whether the change since SPACE_RUST_LINT_BASE can affect any of the crates
# named after TARGET_DIR, per affected_lint_crates; always without a base, as
# in Main CI. run_clippy_checks uses it for its per-crate configurations.
lint_affects_any() {
    local target_dir="$1"
    shift
    [ -n "${SPACE_RUST_LINT_BASE:-}" ] || return 0
    # Called as a condition, where set -e does not apply: every failure is
    # checked here and answers "affected", so an error runs the checks.
    local affected
    if ! mkdir -p "${target_dir}" ||
        ! cargo metadata --locked --format-version 1 --no-deps >"${target_dir}/lint-metadata.json" ||
        ! affected="$(affected_lint_crates "${target_dir}/lint-metadata.json" \
            "${SPACE_RUST_LINT_BASE}" "$(printf '%s\n' "$@")")"; then
        echo "Cannot tell which crates the change affects: checking them all." >&2
        return 0
    fi
    [ -n "${affected}" ]
}
