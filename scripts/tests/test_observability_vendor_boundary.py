"""The CI boundary must reject SDK imports through Cargo, not just obvious names."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "observability_vendor_boundary",
    ROOT / "scripts/source_guards/observability_vendor_boundary.py",
)
assert SPEC is not None and SPEC.loader is not None
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


def test_repository_keeps_vendor_dependencies_in_observability() -> None:
    assert guard.violations(ROOT) == []


@pytest.mark.parametrize(
    "section,dependency",
    [
        ("dependencies", 'datadog-opentelemetry = "0.5"'),
        ("dev-dependencies", 'sdk = { package = "datadog-opentelemetry", version = "0.5" }'),
        ("build-dependencies", 'sdk = { workspace = true }'),
        ('target.\'cfg(unix)\'.dependencies', 'libdd-dogstatsd-client = "5"'),
        ("dependencies", 'cadence = "1"'),
        ("dependencies", 'exporter = { package = "opentelemetry-datadog", version = "0.17" }'),
    ],
)
def test_vendor_dependency_cannot_escape_through_aliases_or_dependency_tables(
    tmp_path: Path, section: str, dependency: str
) -> None:
    (tmp_path / "Cargo.toml").write_text(
        '[workspace.dependencies]\nsdk = { package = "datadog-opentelemetry", version = "0.5" }\n'
    )
    manifest = tmp_path / "crates/libs/product/Cargo.toml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(f'[package]\nname = "product"\n[{section}]\n{dependency}\n')
    failures = guard.violations(tmp_path)
    assert len(failures) == 1
    assert "belongs in crates/l10_foundation/observability/Cargo.toml" in failures[0]


@pytest.mark.parametrize("optional,valid", [(True, True), (False, False)])
def test_vendor_dependency_is_optional_even_inside_its_owner(
    tmp_path: Path, optional: bool, valid: bool
) -> None:
    (tmp_path / "Cargo.toml").write_text("[workspace]\n")
    manifest = tmp_path / guard.OWNER
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        '[dependencies]\ndatadog-opentelemetry = { version = "0.5", '
        f'optional = {str(optional).lower()} }}\n'
    )
    assert (guard.violations(tmp_path) == []) == valid


def test_product_can_use_neutral_facade_and_otel_api(tmp_path: Path) -> None:
    (tmp_path / "Cargo.toml").write_text("[workspace]\n")
    manifest = tmp_path / "crates/libs/product/Cargo.toml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        '[dependencies]\npplx-space-metrics = { path = "../metrics" }\nopentelemetry = "0.32"\n'
    )
    assert guard.violations(tmp_path) == []
