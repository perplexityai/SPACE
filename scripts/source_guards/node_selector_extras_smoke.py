#!/usr/bin/env python3
"""Keep `local.node_selector_extras_render_and_enforce` honest about what it mirrors.

Two of the scenario's correctness conditions live in files it cannot read at
runtime, and both are *totality* conditions — the kind a static guard is the
right tool for:

* **Every renderer must get the variable.** `SandboxPodConfigValues::from_env`
  has more than one caller, and setting `SPACE_SANDBOX_NODE_SELECTOR_LABELS` on
  only one leaves the other composing the selector from the old config — a
  half-configured experiment that still passes its positive phase. The renderer
  set is derived here from the Rust call sites, so an additional renderer fails this
  guard instead of quietly falling outside the scenario's reach.

  It is derived from the *shipping binaries*, not from the call-site paths. Most
  call sites are in library crates, and a library maps to however many app crates
  depend on it: `SandboxPodConfigValues::from_env` inside
  `KubernetesState::try_from_env` is reached by the api, internal api, and cluster
  controller, so attributing that one file to one Deployment would miss the
  others. Resolution walks the workspace dependency graph transitively, so a
  binary that reaches a call site through an intermediate crate is a renderer
  too.

  The one thing it cannot see is a call reached under a different spelling — a
  `use … as` alias, or the constructor renamed. That is a real limit, not a
  covered case: whitespace between the type and `::from_env` is normalized away,
  but an alias would make a call site invisible here.
* **The rejection string it asserts on is owned by the API.** Rewording the
  `empty_cluster` message would turn the negative phase into a devbox run that
  fails at the end. Catching it in CI costs seconds.

The scenario's constants are read by importing `space_smoke.node_selector_extras`
(`RENDERER_DEPLOYMENTS`, `SERVER_CAPACITY_MESSAGE`), so the guard validates the
values the scenario actually runs with rather than a parse of its source. The
shell suite this replaced (`smoke-kind-node-selector-extras.sh`, retired
2026-09-23) was parsed for the same two facts.

Run standalone (`python scripts/source_guards/node_selector_extras_smoke.py`) or
under pytest (`pytest scripts/tests/test_node_selector_extras_smoke.py`), which
also mutates each input to prove both checks bite.
"""

from __future__ import annotations

import importlib
import pathlib
import re
import sys
from collections.abc import Sequence

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCENARIO_MODULE = "space_smoke.node_selector_extras"
ADMISSION = REPO_ROOT / "crates/libs/api/src/cluster_admission/mod.rs"
CRATES = REPO_ROOT / "crates"

# The Rust construction whose call sites are the renderers that must be
# configured together.
CONFIG_FROM_ENV = "SandboxPodConfigValues::from_env"

# The Deployment each app crate ships as. Keyed by the crate directory, since
# that is what a dependency edge resolves to.
APP_DEPLOYMENTS = {
    "crates/apps/api/svc": "space-api",
    "crates/apps/internal/api/svc": "space-internal-api",
    "crates/apps/cluster/controller/svc": "space-cluster-controller",
}


class GuardError(Exception):
    """A mirrored invariant the scenario no longer satisfies."""


def _display(path: pathlib.Path) -> str:
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path)


def _read(path: pathlib.Path) -> str:
    if not path.is_file():
        raise GuardError(f"{_display(path)} is missing")
    return path.read_text(encoding="utf-8")


def _join_continuations(text: str) -> str:
    return re.sub(r"\\\n[ \t]*", "", text)


def scenario_constants() -> tuple[tuple[str, ...], str]:
    """`(RENDERER_DEPLOYMENTS, SERVER_CAPACITY_MESSAGE)` as the scenario module defines them."""

    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    module = importlib.import_module(SCENARIO_MODULE)
    deployments = getattr(module, "RENDERER_DEPLOYMENTS", None)
    message = getattr(module, "SERVER_CAPACITY_MESSAGE", None)
    if not isinstance(deployments, tuple) or not all(isinstance(d, str) and d for d in deployments):
        raise GuardError(f"{SCENARIO_MODULE}.RENDERER_DEPLOYMENTS is not a tuple of Deployment names")
    if not isinstance(message, str):
        raise GuardError(f"{SCENARIO_MODULE}.SERVER_CAPACITY_MESSAGE is not a string")
    return deployments, message


_PACKAGE_TABLE = re.compile(r"^\[\s*package\s*\](?:\s*#.*)?$")


def _package_name(cargo_toml: pathlib.Path) -> str | None:
    in_package = False
    for line in cargo_toml.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_package = bool(_PACKAGE_TABLE.match(stripped))
            continue
        if not in_package:
            continue
        match = re.match(r'name\s*=\s*"([^"]+)"', stripped)
        if match:
            return match.group(1)
    return None


def _declares_binary(cargo_toml: pathlib.Path) -> bool:
    if (cargo_toml.parent / "src" / "main.rs").is_file():
        return True
    if any((cargo_toml.parent / "src" / "bin").glob("*.rs")):
        return True
    return any(
        line.strip().startswith("[[bin]]")
        for line in cargo_toml.read_text(encoding="utf-8").splitlines()
    )


_RUNTIME_DEP_TABLE = re.compile(r"^\[\s*(?:target\..*\.)?dependencies\s*\](?:\s*#.*)?$")
_RUNTIME_DEP_ENTRY = re.compile(
    r"^\[\s*(?:target\..*\.)?dependencies\s*\.\s*([A-Za-z0-9_-]+)\s*\](?:\s*#.*)?$"
)


def _runtime_dependencies(cargo_toml: pathlib.Path) -> set[str]:
    dependencies: set[str] = set()
    in_table = False
    section_entry: str | None = None
    for line in cargo_toml.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_table = bool(_RUNTIME_DEP_TABLE.match(stripped))
            entry = _RUNTIME_DEP_ENTRY.match(stripped)
            section_entry = entry.group(1) if entry else None
            if section_entry is not None:
                dependencies.add(section_entry)
            continue
        renamed = re.match(r'package\s*=\s*"([^"]+)"', stripped)
        if renamed and section_entry is not None:
            dependencies.discard(section_entry)
            dependencies.add(renamed.group(1))
            continue
        if not in_table:
            continue
        match = re.match(r"([A-Za-z0-9_-]+)\s*[=.]", stripped)
        if match:
            name = match.group(1)
            inline_rename = re.search(r'package\s*=\s*"([^"]+)"', stripped)
            dependencies.add(inline_rename.group(1) if inline_rename else name)
    return dependencies


def _owning_crate(source: pathlib.Path) -> pathlib.Path | None:
    for parent in source.parents:
        if (parent / "Cargo.toml").is_file():
            return parent
        if parent == CRATES:
            return None
    return None


def _calls_constructor(text: str) -> bool:
    text = re.sub(r"'(?:[^'\\\n]|\\.)'", "' '", text)
    text = re.sub(r'"(?:[^"\\\n]|\\.)*"', '""', text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    text = "\n".join(re.sub(r"//.*$", "", line) for line in text.splitlines())
    return bool(re.search(rf"{re.escape(CONFIG_FROM_ENV)}\s*\(", re.sub(r"\s*::\s*", "::", text)))


def _call_site_crates() -> dict[str, str]:
    crates: dict[str, str] = {}
    for source in sorted(CRATES.rglob("*.rs")):
        relative = source.relative_to(CRATES.parent).as_posix()
        if source.name == "tests.rs" or "/tests/" in relative:
            continue
        if not _calls_constructor(source.read_text(encoding="utf-8")):
            continue
        crate = _owning_crate(source)
        if crate is None:
            raise GuardError(f"{relative} sits in no crate this guard can resolve")
        package = _package_name(crate / "Cargo.toml")
        if package is None:
            raise GuardError(f"{crate.name}/Cargo.toml declares no package name")
        crates.setdefault(package, relative)
    if not crates:
        raise GuardError(
            f"no non-test call site of {CONFIG_FROM_ENV} was found under crates/; "
            f"either it was renamed or the scenario no longer has a renderer to cover"
        )
    return crates


def _workspace_graph() -> tuple[dict[str, pathlib.Path], dict[str, set[str]]]:
    directories: dict[str, pathlib.Path] = {}
    dependencies: dict[str, set[str]] = {}
    for cargo_toml in sorted(CRATES.rglob("Cargo.toml")):
        package = _package_name(cargo_toml)
        if package is None:
            if _declares_binary(cargo_toml):
                raise GuardError(
                    f"{cargo_toml.relative_to(CRATES.parent).as_posix()} declares a "
                    f"binary but no readable [package] name, so it cannot be checked "
                    f"for reaching {CONFIG_FROM_ENV}"
                )
            continue
        directories[package] = cargo_toml.parent
        dependencies[package] = _runtime_dependencies(cargo_toml)
    return directories, dependencies


def _renderer_deployments() -> dict[str, str]:
    call_sites = _call_site_crates()
    directories, dependencies = _workspace_graph()

    def reaches(package: str) -> str | None:
        seen: set[str] = set()
        stack = [package]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            if current in call_sites:
                return call_sites[current]
            stack.extend(dependencies.get(current, ()))
        return None

    deployments: dict[str, str] = {}
    unmapped: list[str] = []
    for package, directory in sorted(directories.items()):
        relative_dir = directory.relative_to(CRATES.parent).as_posix()
        if not _declares_binary(directory / "Cargo.toml"):
            continue
        via = reaches(package)
        if via is None:
            continue
        if relative_dir in APP_DEPLOYMENTS:
            deployments.setdefault(APP_DEPLOYMENTS[relative_dir], via)
        else:
            unmapped.append(relative_dir)
    if unmapped:
        raise GuardError(
            f"{', '.join(sorted(unmapped))} reach {CONFIG_FROM_ENV} but map to no "
            f"Deployment. Those binaries compose the sandbox node selector too, so "
            f"the scenario has to set SPACE_SANDBOX_NODE_SELECTOR_LABELS on their "
            f"workloads; add them to APP_DEPLOYMENTS and cover them in "
            f"{SCENARIO_MODULE}.RENDERER_DEPLOYMENTS."
        )
    if not deployments:
        raise GuardError(
            f"no app crate was found to reach {CONFIG_FROM_ENV}; either it was renamed "
            f"or the scenario no longer has a renderer to cover"
        )
    return deployments


def check_every_renderer_is_configured(declared_deployments: Sequence[str]) -> None:
    """The scenario's renderer list is exactly the set the Rust call sites imply."""
    declared = set(declared_deployments)
    renderers = _renderer_deployments()
    missing = sorted(set(renderers) - declared)
    if missing:
        sources = ", ".join(f"{name} ({renderers[name]})" for name in missing)
        raise GuardError(
            f"{SCENARIO_MODULE}.RENDERER_DEPLOYMENTS omits {sources}. That call site "
            f"composes the sandbox node selector, and configuring only some renderers "
            f"leaves the rest composing from the old config."
        )
    extra = sorted(declared - set(renderers))
    if extra:
        raise GuardError(
            f"{SCENARIO_MODULE}.RENDERER_DEPLOYMENTS names {', '.join(extra)}, which no "
            f"non-test {CONFIG_FROM_ENV} call site maps to. Either the mapping in "
            f"APP_DEPLOYMENTS is stale or the scenario is configuring a workload that "
            f"does not compose the selector."
        )


# Short enough to keep the constant editable, long enough that the prefix
# identifies the `empty_cluster` gate rather than any 429 the API can emit.
MINIMUM_REJECTION_PREFIX = 24


def _brace_blocks(text: str, opener: str) -> list[str]:
    bodies = []
    for match in re.finditer(opener, text):
        depth = 1
        start = match.end()
        index = start
        while index < len(text) and depth:
            if text[index] == "{":
                depth += 1
            elif text[index] == "}":
                depth -= 1
            index += 1
        if depth == 0:
            bodies.append(text[start : index - 1])
    return bodies


def check_rejection_message_matches_the_api(expected: str) -> None:
    """The asserted 429 text is still a prefix of the API's `EmptyCluster` message."""
    if len(expected) < MINIMUM_REJECTION_PREFIX:
        raise GuardError(
            f"{SCENARIO_MODULE}.SERVER_CAPACITY_MESSAGE is "
            f"{len(expected)} characters ('{expected}'), under the "
            f"{MINIMUM_REJECTION_PREFIX} needed to identify the empty-cluster gate. "
            f"A short or empty prefix matches any rejection the API can send, in the "
            f"scenario's own test as well as in this check."
        )
    admission = _join_continuations(_read(ADMISSION))
    # A variant construction, or a struct construction whose `kind` names the
    # empty-cluster rejection. The struct opener also matches the definition
    # and destructurings; neither names the kind, so the filter drops them.
    blocks = _brace_blocks(admission, r"(?:ClusterAdmissionError|Self)::EmptyCluster\s*\{") + [
        body
        for body in _brace_blocks(admission, r"\bClusterAdmissionError\s*\{")
        if re.search(r"\bkind\s*:\s*ActivationRejectionKind::EmptyCluster\b", body)
    ]
    if not blocks:
        raise GuardError(
            f"no ClusterAdmissionError::EmptyCluster construction in "
            f"{_display(ADMISSION)}; the gate the scenario's negative phase asserts on "
            f"was renamed or removed"
        )
    constructions = [body for body in blocks if re.search(r"\bmessage\s*:", body)]
    if not constructions:
        raise GuardError(
            f"every ClusterAdmissionError::EmptyCluster block in "
            f"{_display(ADMISSION)} destructures rather than constructs, so the message "
            f"the scenario asserts on is no longer set where this guard can read it"
        )
    for body in constructions:
        message = re.search(r'\bmessage\s*:\s*"((?:[^"\\]|\\.)*)"', body)
        if message is None:
            raise GuardError(
                f"an EmptyCluster construction in {_display(ADMISSION)} sets `message` "
                f"to something other than a string literal, so the scenario cannot be "
                f"checked against the text the wire actually sends"
            )
        if not message.group(1).startswith(expected):
            raise GuardError(
                f"the scenario asserts the create is rejected with '{expected}…', but "
                f"an EmptyCluster message in {_display(ADMISSION)} is "
                f"'{message.group(1)}'. Update {SCENARIO_MODULE}, or the negative phase "
                f"will fail only on a devbox run."
            )


def main() -> None:
    deployments, message = scenario_constants()
    check_every_renderer_is_configured(deployments)
    check_rejection_message_matches_the_api(message)


if __name__ == "__main__":
    try:
        main()
    except GuardError as error:
        raise SystemExit(f"node-selector-extras smoke guard: {error}") from error
    print("node-selector-extras smoke guard: ok")
