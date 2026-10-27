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
# it depends on nothing else that reaches them, and a new crate under
# crates/libs/ joins without a CI edit. A crate that reaches common-core
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
