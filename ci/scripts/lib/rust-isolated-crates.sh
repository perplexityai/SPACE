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
# library, other than common-core and common-svc, that reaches common-core at
# most directly. No other workspace member in its normal dependency graph may
# reach common-core or common-svc. A crate split out of the two is linted once
# it depends on nothing else that reaches them, and a new library joins
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

COMMON_CORE, COMMON_SVC = "pplx-space-common-core", "pplx-space-common-svc"
# Reach common-core, besides directly, only through pplx-space-metrics, which
# is linted alone itself, and build quickly alone, so they are linted as well.
# Each entry must be a workspace library the rule leaves out for reaching
# common-core through another crate.
ALSO = {
    "pplx-space-ingress-admission",
    "pplx-space-tokio-runtime",
    "pplx-space-uds-peer-auth",
}

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
    if COMMON_SVC in others:
        return "rest"
    if any(reaches(other, edges) & {COMMON_CORE, COMMON_SVC} for other in others):
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
    if name not in (COMMON_CORE, COMMON_SVC)
    and classify(name, declared_edges) != "selected"
]
# Cargo's package-cache lock serializes most of each `cargo tree`; running a
# few at once still overlaps the rest, which saves about a third.
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    resolved = dict(zip(undecided, pool.map(resolved_edges, undecided)))
groups = {"selected": [], "indirect": [], "rest": []}
for name in libraries:
    if name in (COMMON_CORE, COMMON_SVC):
        group = "rest"
    elif name in resolved:
        group = classify(name, resolved[name])
    else:
        group = "selected"
    groups[group].append(name)
for name in sorted(ALSO):
    if name not in libraries:
        sys.exit(f"isolated_crates: {name} is listed in ALSO but is not a workspace library")
    if name not in groups["indirect"]:
        sys.exit(
            f"isolated_crates: {name} is listed in ALSO but the rule does not leave it "
            "out for reaching common-core through another crate; remove it"
        )
    groups["indirect"].remove(name)
    groups["selected"].append(name)
for name in sorted(groups["selected"]):
    print(name)
print(
    f"Not linted alone, reaching common-core through another crate: "
    f"{len(groups['indirect'])}: {' '.join(groups['indirect'])}",
    file=sys.stderr,
)
print(
    f"Not linted alone, being common-core or common-svc or reaching common-svc: "
    f"{len(groups['rest'])}: {' '.join(groups['rest'])}",
    file=sys.stderr,
)
PY
}

# The crates of LISTED (one per line) that the change since BASE can affect,
# one per line: those whose own directory changed, those that include a
# changed file from another crate's directory (include_str!, include_bytes!),
# and every workspace crate that depends on one of them, through any kind of
# dependency. Prints every listed crate when the change touches a path outside
# all crates that such a lint may read (Cargo.lock, the root manifest, the
# toolchain, proto/, db/, templates/, ...), since Cargo, a build script or an
# include can read it; ci/check-scopes/rust.skip and UNREAD_BY_LINT below name
# the paths none reads. Main CI passes no base and lints all.
affected_lint_crates() {
    local metadata="$1" base="$2" listed="$3"
    python3 - "${metadata}" "${base}" "${listed}" <<'PY'
import fnmatch
import json
import os
import re
import subprocess
import sys

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
LINT_INPUTS_IN_CI = (
    "ci/Dockerfile.ci",
    "ci/scripts/run-rust-checks.sh",
    "ci/scripts/lib/rust-isolated-crates.sh",
)


def unread_by_lint(path):
    if path in UNREAD_BY_LINT or path.startswith(".github/"):
        return True
    return path.startswith("ci/") and path not in LINT_INPUTS_IN_CI


listing = subprocess.run(
    ["git", "-c", f"safe.directory={root}", "diff", "--name-only", "--no-renames",
     "-z", base, "HEAD"],
    cwd=root, stdout=subprocess.PIPE, check=False,
)
if listing.returncode != 0:
    print(f"Cannot list the change against {base}: linting every crate.", file=sys.stderr)
    print("\n".join(listed))
    sys.exit(0)
changed = [path for path in listing.stdout.decode().split("\0") if path]
outside = [
    p for p in changed if owner(p) is None and not unread_by_rust(p) and not unread_by_lint(p)
]
if outside:
    print(f"{outside[0]} is outside every crate: linting every crate.", file=sys.stderr)
    print("\n".join(listed))
    sys.exit(0)

affected = {owner(p) for p in changed} - {None}
# A crate that includes a changed file from another crate's directory.
changed_set = set(changed)
include = re.compile(r'include_(?:str|bytes)!\(\s*"([^"]+)"')
for name, crate_dir in directory.items():
    for dirpath, dirnames, filenames in os.walk(os.path.join(root, crate_dir)):
        dirnames[:] = [d for d in dirnames if d != "target"]
        for filename in filenames:
            if not filename.endswith(".rs"):
                continue
            source = os.path.join(dirpath, filename)
            with open(source, errors="ignore") as text:
                for match in include.finditer(text.read()):
                    target = os.path.relpath(
                        os.path.normpath(os.path.join(dirpath, match.group(1))), root
                    )
                    if target in changed_set and owner(target) != name:
                        affected.add(name)
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
    mkdir -p "${target_dir}"
    cargo metadata --locked --format-version 1 --no-deps >"${target_dir}/lint-metadata.json"
    local affected
    affected="$(affected_lint_crates "${target_dir}/lint-metadata.json" \
        "${SPACE_RUST_LINT_BASE}" "$(printf '%s\n' "$@")")"
    [ -n "${affected}" ]
}

# Lint each crate isolated_crates names on its own, with no features and with
# all of them. One Cargo invocation per crate and configuration: a single one
# would unify features across its packages again. `--all-targets` builds the
# library with its dev-dependencies' features too, so the library is also
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
    if [ -n "${SPACE_RUST_LINT_BASE:-}" ]; then
        listed="$(affected_lint_crates "${target_dir}/metadata.json" \
            "${SPACE_RUST_LINT_BASE}" "${listed}")"
        if [ -z "${listed}" ]; then
            echo "The change affects no crate linted alone."
            return 0
        fi
        mapfile -t crates <<<"${listed}"
    fi
    echo "Linting ${#crates[@]} crates alone: ${crates[*]}"

    local crate config flags=() failures=() run_count=0
    local configs=(
        "--no-default-features --all-targets"
        "--no-default-features --lib"
        "--all-features --all-targets"
        "--all-features --lib"
    )
    for crate in "${crates[@]}"; do
        for config in "${configs[@]}"; do
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
