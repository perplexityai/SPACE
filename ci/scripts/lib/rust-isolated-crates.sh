#!/usr/bin/env bash
# Lints workspace libraries one at a time, so a crate that uses a dependency
# feature it gets only through workspace feature unification fails; the
# workspace clippy cannot see that. GitHub CI runs this as its own job,
# `run-rust-checks.sh isolated-crates`, beside the clippy and test jobs.
#
# It lives apart from run-rust-checks.sh so that a change to which crates it
# picks keeps the Rust caches: their keys hash run-rust-checks.sh (see "Name
# the Cargo target directory" and "Name the Rust compiler cache" in
# .github/workflows/common-ci.yml). It builds in its own directory,
# target/buildkite-isolated-crates, which is never cached. To run it locally,
# from the repository root:
#
#   bash -euo pipefail -c 'source ci/scripts/lib/rust-isolated-crates.sh; run_isolated_crate_checks'

# The workspace libraries run_isolated_crate_checks lints, one per line: every
# library, other than common-core, that reaches common-core at most directly.
# No other workspace member in its normal dependency graph may reach
# common-core. A crate split out of it is linted once it depends on nothing
# else that reaches it, and a new library joins
# without a CI edit, wherever it lives. A crate that reaches common-core
# through another crate builds that crate's graph too, so linting all of them
# would take several times as long; the workspace clippy still lints them.
#
# The declared dependencies, optional ones included, decide first. They can
# only overstate a graph, so a crate they select stays selected. A crate they
# leave out is decided again from the normal graph Cargo resolves for it with
# all of its features, on every target: an optional dependency that nothing in
# its build enables is not part of it. The libraries left out are named on
# stderr, so a crate that drops out shows in the log.
isolated_crates() {
    local metadata="$1"
    python3 - "${metadata}" <<'PY'
import concurrent.futures
import json
import subprocess
import sys

COMMON_CORE = "pplx-space-common-core"
with open(sys.argv[1]) as metadata_file:
    packages = {p["name"]: p for p in json.load(metadata_file)["packages"]}


def declared_edges(name):
    return {
        dependency["name"]
        for dependency in packages[name]["dependencies"]
        if dependency["kind"] is None and dependency["name"] in packages
    }


def resolved_edges(name):
    """The normal dependency edges of the graph Cargo resolves for crate
    `name` with all of its features, read from `cargo tree --prefix depth`."""
    result = subprocess.run(
        ["cargo", "tree", "--locked", "-q", "-p", name, "--all-features",
         "-e", "normal", "--target", "all", "--prefix", "depth", "-f", "{p}"],
        stdout=subprocess.PIPE,
        text=True,
    )
    if result.returncode != 0:
        sys.exit(f"isolated_crates: cargo tree failed for {name}")
    edges, path = {}, []
    for line in result.stdout.splitlines():
        digits = len(line) - len(line.lstrip("0123456789"))
        depth, package = int(line[:digits]), line[digits:].split()[0]
        del path[depth:]
        if path:
            edges.setdefault(path[-1], set()).add(package)
        path.append(package)
    if not path or path[0] != name:
        sys.exit(f"isolated_crates: cargo tree printed no graph for {name}")
    return lambda member: edges.get(member, set())


def reaches(start, edges):
    seen, pending = set(), [start]
    while pending:
        for dependency in edges(pending.pop()) - seen:
            seen.add(dependency)
            pending.append(dependency)
    return seen


def classify(name, edges):
    others = reaches(name, edges) - {COMMON_CORE}
    if any(COMMON_CORE in reaches(other, edges) for other in others):
        return "indirect"
    return "selected"


libraries = sorted(
    name
    for name, package in packages.items()
    if any(
        kind in ("lib", "rlib", "proc-macro")
        for target in package["targets"]
        for kind in target["kind"]
    )
)
undecided = [
    name
    for name in libraries
    if name != COMMON_CORE
    and classify(name, declared_edges) != "selected"
]
# Cargo's package-cache lock serializes most of each `cargo tree`; running a
# few at once still overlaps the rest, which saves about a third.
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    resolved = dict(zip(undecided, pool.map(resolved_edges, undecided)))
groups = {"selected": [], "indirect": []}
for name in libraries:
    if name == COMMON_CORE:
        continue
    if name in resolved:
        group = classify(name, resolved[name])
    else:
        group = "selected"
    groups[group].append(name)
for name in sorted(groups["selected"]):
    print(name)
print(
    f"Not linted alone, reaching common-core through another crate: "
    f"{len(groups['indirect'])}: {' '.join(groups['indirect'])}",
    file=sys.stderr,
)
PY
}

# The crates of LISTED (one per line) that the change since BASE can affect,
# one per line: those whose own directory changed, those that include a
# changed file from another crate's directory (include_str!, include_bytes!),
# and every workspace crate that depends on one of them, through any kind of
# dependency. A change to Cargo.lock affects the workspace crates whose
# resolved dependency tree holds a package it changed (see
# lock_affected_crates below). Prints every listed crate when the change
# touches any other path outside all crates that such a lint may read (the
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
# alone does: archcheck's files, nextest's configuration, the workflows, and
# CI scripts other than this lint's own (run-rust-checks.sh, this file and
# the image it runs in, ci/Dockerfile.ci).
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
    "ci/scripts/lib/rust-isolated-crates.sh",
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

# Lint each crate isolated_crates names on its own, with its minimum supported
# features and with all of them. Use one Cargo invocation per crate and
# configuration so Cargo cannot unify features across packages. `--all-targets`
# builds the library with its dev-dependencies' features too, so the library is also
# linted alone with `--lib`. That run is a no-op taking a fraction of a second
# when the dev-dependencies change nothing, which is less than asking
# `cargo tree` whether they do. Every crate runs, and every failing crate and
# configuration is named at the end. A new crate needs no step in
# run-rust-checks.sh: this finds it.
run_isolated_crate_checks() {
    local target_dir="target/buildkite-isolated-crates"
    mkdir -p "${target_dir}"
    echo "--- :rust: Finding the workspace crates to lint alone"
    cargo metadata --locked --format-version 1 --no-deps >"${target_dir}/metadata.json"
    local listed crates=()
    listed="$(isolated_crates "${target_dir}/metadata.json")"
    mapfile -t crates <<<"${listed}"
    if [ -z "${listed}" ]; then
        echo "::error::No workspace crate qualifies to be linted alone; the selection is broken." >&2
        return 1
    fi
    # Pull CI and the merge queue pass the base check-needed.sh decided
    # against (ci/check-scopes/README.md); Main CI passes none.
    local affected
    if [ -n "${SPACE_RUST_LINT_BASE:-}" ]; then
        # A failed selection lints every crate, whether or not set -e applies.
        if ! affected="$(affected_lint_crates "${target_dir}/metadata.json" \
            "${SPACE_RUST_LINT_BASE}" "${listed}")"; then
            echo "Cannot tell which crates the change affects: linting them all." >&2
            affected="${listed}"
        fi
        listed="${affected}"
        if [ -z "${listed}" ]; then
            echo "The change affects no crate linted alone."
            return 0
        fi
        mapfile -t crates <<<"${listed}"
    fi
    echo "Linting ${#crates[@]} crates alone: ${crates[*]}"

    local crate config flags=() failures=() run_count=0
    local minimal_features
    for crate in "${crates[@]}"; do
        minimal_features="--no-default-features"
        # Cluster controller requires its Kubernetes client even in Direct mode
        # for leader election when the controller runs inside a pod.
        if [[ "${crate}" == pplx-space-cluster-controller-server ]]; then
            minimal_features+=" --features kubernetes"
        fi
        for config in "${minimal_features} --all-targets" "${minimal_features} --lib" \
            "--all-features --all-targets" "--all-features --lib"; do
            echo "--- :rust: Linting ${crate} alone (${config})"
            run_count=$((run_count + 1))
            read -ra flags <<<"${config}"
            if ! CARGO_TARGET_DIR="${target_dir}" cargo clippy --locked -p "${crate}" \
                "${flags[@]}" -- -D warnings; then
                echo "::error::${crate} fails clippy built alone (${config})." >&2
                failures+=("${crate} (${config})")
            fi
        done
    done
    echo "Linted ${#crates[@]} crates alone in ${run_count} configurations."
    if [ "${#failures[@]}" -ne 0 ]; then
        echo "Built alone, these fail clippy; declare the dependency features they use:" >&2
        printf '  %s\n' "${failures[@]}" >&2
        return 1
    fi
}
