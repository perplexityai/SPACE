"""Cover the node-selector-extras scenario's source guard, including that it bites.

The guard mirrors two facts that live outside the scenario: which crates compose
the sandbox node selector, and what the API's `empty_cluster` message says. A
guard that only ever passes is indistinguishable from one that cannot fail, so
every case below feeds the matching check a mutated input and asserts it rejects.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/source_guards/node_selector_extras_smoke.py"
SPEC = importlib.util.spec_from_file_location("node_selector_extras_smoke", SCRIPT)
assert SPEC is not None
guard = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = guard
assert SPEC.loader is not None
SPEC.loader.exec_module(guard)

DEPLOYMENTS, MESSAGE = guard.scenario_constants()


def _crate(
    tmp_path: pathlib.Path,
    crate_dir: str,
    package: str,
    *,
    call_site: str | None = None,
    depends_on: tuple[str, ...] = (),
    binary: bool = False,
) -> None:
    root = tmp_path / crate_dir
    (root / "src").mkdir(parents=True, exist_ok=True)
    manifest = f'[package]\nname = "{package}"\n\n[dependencies]\n'
    manifest += "".join(f'{dependency} = {{ path = "../x" }}\n' for dependency in depends_on)
    (root / "Cargo.toml").write_text(manifest, encoding="utf-8")
    if binary:
        (root / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
    if call_site is not None:
        (root / "src" / call_site).write_text(
            f"let c = {guard.CONFIG_FROM_ENV}()?;\n", encoding="utf-8"
        )


def _stage(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(guard, "CRATES", tmp_path / "crates")


def _stage_real_shape(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The tree's actual shape: the call site in a library both apps link."""
    _crate(tmp_path, "crates/libs/api", "pplx-space-api", call_site="substrate.rs")
    _crate(
        tmp_path, "crates/apps/api/svc", "pplx-space-api-svc",
        depends_on=("pplx-space-api",), binary=True,
    )
    _crate(
        tmp_path,
        "crates/apps/cluster/controller/svc",
        "pplx-space-cluster-controller",
        call_site="warm_pool.rs",
        depends_on=("pplx-space-api",),
        binary=True,
    )
    _stage(tmp_path, monkeypatch)


def test_current_repository_satisfies_every_mirrored_invariant() -> None:
    guard.main()


def test_guard_rejects_a_scenario_that_configures_only_one_renderer() -> None:
    with pytest.raises(guard.GuardError, match="space-cluster-controller"):
        guard.check_every_renderer_is_configured(("space-api",))


def test_guard_rejects_a_renderer_list_naming_a_workload_that_composes_nothing() -> None:
    with pytest.raises(guard.GuardError, match="space-data-plane"):
        guard.check_every_renderer_is_configured((*DEPLOYMENTS, "space-data-plane"))


def test_guard_rejects_a_rejection_message_the_api_does_not_send() -> None:
    with pytest.raises(guard.GuardError, match="no eligible worker nodes"):
        guard.check_rejection_message_matches_the_api("Server capacity limit: no eligible worker nodes")


@pytest.mark.parametrize("blanked", ["", "Server capacity"])
def test_guard_rejects_a_rejection_prefix_too_short_to_identify_the_gate(blanked: str) -> None:
    with pytest.raises(guard.GuardError, match="characters"):
        guard.check_rejection_message_matches_the_api(blanked)


def test_the_message_check_is_anchored_on_the_empty_cluster_construction(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = tmp_path / "cluster_admission.rs"
    admission.write_text(
        'fn log() { tracing::warn!("Server capacity limit: no eligible sandbox '
        'activation nodes are available"); }\n'
        "fn reject() -> E { ClusterAdmissionError::EmptyCluster {\n"
        '    message: "Server rate limit: slow down".to_string(),\n'
        "} }\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "ADMISSION", admission)
    with pytest.raises(guard.GuardError, match="Server rate limit"):
        guard.check_rejection_message_matches_the_api(MESSAGE)


def test_the_message_check_reads_every_empty_cluster_construction(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = tmp_path / "cluster_admission.rs"
    admission.write_text(
        "fn cold() -> E { ClusterAdmissionError::EmptyCluster {\n"
        '    message: "Server capacity limit: no eligible sandbox activation nodes are '
        'available.".to_string(),\n'
        "} }\n"
        "fn warm() -> E { ClusterAdmissionError::EmptyCluster {\n"
        '    message: "Server rate limit: slow down".to_string(),\n'
        "} }\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "ADMISSION", admission)
    with pytest.raises(guard.GuardError, match="Server rate limit"):
        guard.check_rejection_message_matches_the_api(MESSAGE)


def test_the_message_check_reads_the_message_field_not_any_literal(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = tmp_path / "cluster_admission.rs"
    admission.write_text(
        "fn reject() -> E { ClusterAdmissionError::EmptyCluster {\n"
        '    reason: "Server capacity limit: no eligible sandbox activation nodes",\n'
        '    message: "Server rate limit: slow down".to_string(),\n'
        "} }\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "ADMISSION", admission)
    with pytest.raises(guard.GuardError, match="Server rate limit"):
        guard.check_rejection_message_matches_the_api(MESSAGE)


def test_the_message_check_skips_a_destructuring_pattern(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = tmp_path / "cluster_admission.rs"
    admission.write_text(
        "fn f(e: E) { match e { Self::EmptyCluster { retry_after, .. } => {} } }\n"
        "fn reject() -> E { ClusterAdmissionError::EmptyCluster {\n"
        '    message: "Server capacity limit: no eligible sandbox activation nodes are '
        'available.".to_string(),\n'
        "} }\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(guard, "ADMISSION", admission)
    guard.check_rejection_message_matches_the_api(MESSAGE)


def _struct_shape(empty_cluster_message: str) -> str:
    return (
        "pub struct ClusterAdmissionError { kind: ActivationRejectionKind, message: String }\n"
        "fn f(e: E) { let ClusterAdmissionError { kind, message } = e; }\n"
        "fn shed() -> E { ClusterAdmissionError {\n"
        "    kind: ActivationRejectionKind::RateLimited,\n"
        '    message: "Server rate limit: slow down".to_string(),\n'
        "} }\n"
        "fn reject() -> E { ClusterAdmissionError {\n"
        "    kind: ActivationRejectionKind::EmptyCluster,\n"
        f'    message: "{empty_cluster_message}".to_string(),\n'
        "} }\n"
    )


def test_the_message_check_reads_a_struct_construction_by_its_kind(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = tmp_path / "cluster_admission.rs"
    monkeypatch.setattr(guard, "ADMISSION", admission)

    admission.write_text(_struct_shape("Server capacity limit: gone"), encoding="utf-8")
    with pytest.raises(guard.GuardError, match="Server capacity limit: gone"):
        guard.check_rejection_message_matches_the_api(MESSAGE)

    # The rate-limit construction's own text isn't read.
    admission.write_text(
        _struct_shape(
            "Server capacity limit: no eligible sandbox activation nodes are available."
        ),
        encoding="utf-8",
    )
    guard.check_rejection_message_matches_the_api(MESSAGE)


def test_a_renderer_reached_through_a_library_is_found(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stage_real_shape(tmp_path, monkeypatch)
    assert sorted(guard._renderer_deployments()) == ["space-api", "space-cluster-controller"]


def test_an_unmapped_binary_reaching_the_constructor_fails_closed(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stage_real_shape(tmp_path, monkeypatch)
    _crate(tmp_path, "crates/apps/other/svc", "pplx-other", depends_on=("pplx-space-api",), binary=True)
    with pytest.raises(guard.GuardError, match="crates/apps/other/svc"):
        guard._renderer_deployments()


def test_a_test_module_call_site_does_not_widen_the_set(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stage_real_shape(tmp_path, monkeypatch)
    _crate(tmp_path, "crates/apps/other/svc", "pplx-other", call_site="tests.rs", binary=True)
    assert sorted(guard._renderer_deployments()) == ["space-api", "space-cluster-controller"]
