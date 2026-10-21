"""The crates each Rust artifact lane's digest covers, read from the workspace manifests."""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "ci/scripts/run-space-bundle-builds.py"


def load_bundle_builds_module():
    spec = importlib.util.spec_from_file_location("run_space_bundle_builds_inputs", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


bundle_builds = load_bundle_builds_module()


def cargo() -> str | None:
    found = shutil.which("cargo")
    if found is None and os.environ.get("SPACE_CI_REQUIRE_TEST_DEPS") == "1":
        pytest.fail("cargo is required for the metadata oracle in CI")
    return found


def cargo_packages(root: Path) -> dict:
    """The workspace graph as `cargo metadata` reports it: the oracle."""
    # Run from the repository so rustup selects its pinned toolchain.
    metadata = json.loads(
        subprocess.run(
            [
                cargo(), "metadata", "--format-version", "1", "--no-deps", "--offline",
                "--manifest-path", str(root / "Cargo.toml"),
            ],
            cwd=REPO_ROOT, check=True, capture_output=True, text=True,
        ).stdout
    )
    workspace_root = Path(metadata["workspace_root"])
    members = [
        package for package in metadata["packages"]
        if package["id"] in set(metadata["workspace_members"])
    ]
    directory = {
        package["name"]: Path(package["manifest_path"]).parent.relative_to(workspace_root)
        for package in members
    }
    return {
        directory[package["name"]]: bundle_builds.RustPackage(
            binaries=frozenset(
                target["name"] for target in package["targets"] if "bin" in target["kind"]
            ),
            dependencies=frozenset(
                directory[dependency["name"]]
                for dependency in package["dependencies"]
                if dependency["kind"] != "dev" and dependency["name"] in directory
            ),
        )
        for package in members
    }


def write(root: Path, relative: str, content: str = "") -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def fixture_workspace(root: Path) -> None:
    write(
        root,
        "Cargo.toml",
        '[workspace]\nresolver = "2"\nmembers = ["apps/svc", "apps/other"]\n'
        "[workspace.dependencies]\n"
        'fixture-core = { path = "libs/core" }\n'
        'shared = { path = "libs/shared-impl", package = "fixture-shared-impl" }\n',
    )
    write(
        root,
        "apps/svc/Cargo.toml",
        '[package]\nname = "fixture-svc"\nversion = "0.1.0"\nedition = "2021"\n'
        '[[bin]]\nname = "fixture-tool"\npath = "src/tool.rs"\n'
        "[dependencies]\n"
        "fixture-core = { workspace = true }\n"
        "shared = { workspace = true }\n"
        'alias = { path = "../../libs/aliased", package = "fixture-aliased" }\n'
        'optional = { path = "../../libs/optional", optional = true, package = "fixture-optional" }\n'
        'anyhow = "1"\n'
        "[build-dependencies]\n"
        'build-helper = { path = "../../libs/build-helper", package = "fixture-build-helper" }\n'
        "[target.'cfg(windows)'.dependencies]\n"
        'windows-only = { path = "../../libs/windows-only", package = "fixture-windows-only" }\n'
        "[dev-dependencies]\n"
        'test-only = { path = "../../libs/test-only", package = "fixture-test-only" }\n',
    )
    write(root, "apps/svc/src/main.rs", "fn main() {}\n")
    write(root, "apps/svc/src/tool.rs", "fn main() {}\n")
    write(
        root,
        "apps/other/Cargo.toml",
        '[package]\nname = "fixture-other"\nversion = "0.1.0"\nedition = "2021"\n'
        '[dependencies]\nunrelated = { path = "../../libs/unrelated", package = "fixture-unrelated" }\n',
    )
    write(root, "apps/other/src/bin/other-tool.rs", "fn main() {}\n")
    write(
        root,
        "libs/core/Cargo.toml",
        '[package]\nname = "fixture-core"\nversion = "0.1.0"\nedition = "2021"\n'
        '[dependencies]\ndeep = { path = "../deep", package = "fixture-deep" }\n',
    )
    for directory in (
        "shared-impl", "aliased", "optional", "build-helper", "windows-only",
        "test-only", "unrelated", "deep",
    ):
        write(
            root,
            f"libs/{directory}/Cargo.toml",
            f'[package]\nname = "fixture-{directory}"\nversion = "0.1.0"\nedition = "2021"\n',
        )
    for directory in (root / "libs").iterdir():
        write(directory, "src/lib.rs")


def test_a_lane_covers_what_its_binaries_reach_without_dev_dependencies(tmp_path, monkeypatch):
    fixture_workspace(tmp_path)
    monkeypatch.setattr(
        bundle_builds,
        "RUST_ARTIFACT_BINARIES",
        {"tool": ("fixture-tool",), "other": ("other-tool",), "missing": ("fixture-svc", "gone")},
    )

    assert bundle_builds.rust_artifact_packages("tool", tmp_path) == tuple(
        Path(path)
        for path in (
            "apps/svc",
            "libs/aliased",
            "libs/build-helper",
            "libs/core",
            "libs/deep",
            "libs/optional",
            "libs/shared-impl",
            "libs/windows-only",
        )
    )
    assert bundle_builds.rust_artifact_packages("other", tmp_path) == (
        Path("apps/other"),
        Path("libs/unrelated"),
    )
    with pytest.raises(SystemExit, match="Rust binaries: gone$"):
        bundle_builds.rust_artifact_packages("missing", tmp_path)
    if cargo() is not None:
        assert bundle_builds.rust_workspace_packages(tmp_path) == cargo_packages(tmp_path)


@pytest.mark.skipif(cargo() is None, reason="cargo not installed")
def test_the_manifest_reader_matches_cargo_on_this_workspace():
    assert bundle_builds.rust_workspace_packages(REPO_ROOT) == cargo_packages(REPO_ROOT)
