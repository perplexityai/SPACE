#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# ///
"""Build and publish SPACE bundle images via `uv run --script`.

Registry operations require SPACE_BUNDLE_TARGET_ACCOUNTS, a JSON object mapping
SPACE_BUNDLE_TARGETS names to AWS account IDs. S3 compiler caching is opt-in via
SCCACHE_BUCKET; local builds need neither setting.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
RELEASE_IMAGE_MANIFEST = REPO_ROOT / "ci/space-helm-release-images.json"
MIRROR_IMAGES_SCRIPT = SCRIPT_DIR / "mirror-images.sh"
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", AWS_REGION)
SCCACHE_REGION = os.environ.get("SCCACHE_REGION", AWS_REGION)
SCCACHE_S3_KEY_PREFIX = os.environ.get(
    "SCCACHE_S3_KEY_PREFIX", "sccache/space/v1/release"
)
SCCACHE_S3_RW_MODE = os.environ.get("SCCACHE_S3_RW_MODE", "READ_WRITE")
SCCACHE_S3_SERVER_SIDE_ENCRYPTION = os.environ.get(
    "SCCACHE_S3_SERVER_SIDE_ENCRYPTION", "true"
)

SPACE_UI_API_URL = "http://space-api:8080"
SPACE_UI_DATA_PLANE_URL = "http://space-data-plane:8090"
ECR_TRANSIENT_ERROR_EXIT_STATUS = 75
# The BuildKit daemon runs privileged and does the registry push, so it's
# pinned rather than pulled by a mutable tag. v0.32.2 is what `buildx-stable-1`
# resolved to when this was pinned.
BUILDKIT_IMAGE = (
    "moby/buildkit:v0.32.2"
    "@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8"
)
BUILDX_RATE_LIMIT_MAX_ATTEMPTS = 5
BUILDX_RATE_LIMIT_INITIAL_DELAY_SECONDS = 2
# Downstream builder subclasses still read these during their compatibility transition.
MAIN_HISTORY_DEPTH = 100
MAIN_HISTORY_READ_ATTEMPTS = 3
# BuildKit reports registry failures as `unexpected status from <METHOD>
# request to <url>: <status>`. Restrict 5xx retries to that request context.
REGISTRY_TRANSIENT_ERROR_PATTERN = re.compile(
    r"(?:\btoomanyrequests:\s*Rate exceeded\b"
    r"|\b429 Too Many Requests\b"
    r"|\bunexpected status from [A-Z]+ request to \S+:\s*"
    r"(?:500 Internal Server Error|502 Bad Gateway|503 Service Unavailable"
    r"|504 Gateway Timeout)\b)",
    re.IGNORECASE,
)

RELEASE_IMAGE_REPOSITORIES: dict[str, str] = json.loads(
    RELEASE_IMAGE_MANIFEST.read_text()
)

RUST_ARTIFACT_INPUT_MANIFEST = Path("ci/rust-artifact-inputs.json")
RUST_ARTIFACT_BINARIES = {
    "worker": (
        "spaced",
        "spaced-svc",
        "space-buildmgr",
        "space-buildmgr-svc",
        "space-tmplmgr",
        "space-nodemgr-svc",
        "space-sbxmgr",
        "space-nodectl",
        "space-sbxmgr-svc",
        "space-spaced-host-svc",
        "space-netgtw-svc",
        "space-tmplmgr-svc",
        "space-volmgr",
        "space-credmgr-svc",
        "space-volmgr-svc",
        "space-node-api-svc",
        "space-internal-api-svc",
        "space-nodectl-svc",
        "space-db-migrator",
        "space-runtime-supervisor-svc",
        "space-rtmgr-svc",
        "space-rtmgr",
    ),
    "builder": ("space-buildmgr", "space-tmplmgr", "space-db-migrator"),
    "api": ("space-api-svc", "space-db-migrator", "space-template-publish"),
    "data-plane": ("space-data-plane-svc",),
    "cluster-controller": ("space-cluster-controller",),
}

RUST_DEPENDENCY_INPUTS = (
    Path(".cargo/config.toml"),
    Path("Cargo.toml"),
    Path("Cargo.lock"),
    Path("rust-toolchain.toml"),
    Path("docker/Dockerfile.rust-build-common"),
    Path("docker/run-cargo-with-sccache.sh"),
)


# Asked for explicitly so ECR returns the stored manifest unconverted when a
# re-run checks for this commit's image. These are the two single-image
# formats buildx writes with `--provenance false`; BatchGetImage accepts no
# index or manifest-list types.
ECR_MANIFEST_MEDIA_TYPES = (
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)


@dataclass(frozen=True)
class EcrImage:
    manifest: str
    media_type: str | None
    digest: str


@dataclass(frozen=True)
class Target:
    name: str
    account_id: str

    @property
    def registry_host(self) -> str:
        return f"{self.account_id}.dkr.ecr.{AWS_REGION}.amazonaws.com"


def run(
    args: Sequence[str],
    *,
    check: bool = True,
    capture: bool = False,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(args),
        check=False,
        text=True,
        capture_output=capture,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        input=input_text,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            args,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result


def run_buildx_command(
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    command = list(args)
    delay_seconds = BUILDX_RATE_LIMIT_INITIAL_DELAY_SECONDS

    for attempt in range(1, BUILDX_RATE_LIMIT_MAX_ATTEMPTS + 1):
        transient = False
        with subprocess.Popen(
            command,
            text=True,
            stdout=None,
            stderr=subprocess.PIPE,
            cwd=str(cwd) if cwd is not None else None,
            env=env,
        ) as process:
            assert process.stderr is not None
            for line in process.stderr:
                sys.stderr.write(line)
                sys.stderr.flush()
                transient = (
                    transient or REGISTRY_TRANSIENT_ERROR_PATTERN.search(line) is not None
                )
            returncode = process.wait()

        if returncode == 0:
            return subprocess.CompletedProcess(command, returncode)
        if not transient:
            raise subprocess.CalledProcessError(returncode, command)
        if attempt == BUILDX_RATE_LIMIT_MAX_ATTEMPTS:
            print(
                f"Buildx transient registry error persisted for {attempt} attempts; "
                f"exiting {ECR_TRANSIENT_ERROR_EXIT_STATUS} so Buildkite can retry.",
                file=sys.stderr,
            )
            raise SystemExit(ECR_TRANSIENT_ERROR_EXIT_STATUS)

        print(
            f"Buildx transient registry error on attempt {attempt}/"
            f"{BUILDX_RATE_LIMIT_MAX_ATTEMPTS}; retrying in {delay_seconds}s.",
            file=sys.stderr,
        )
        time.sleep(delay_seconds)
        delay_seconds *= 2

    raise AssertionError("unreachable")


def run_buildx(args: Sequence[str], *, push: bool, context: Path = REPO_ROOT) -> None:
    command = [*args, "--push" if push else "--load", str(context)]
    run_buildx_command(command)


def run_buildx_local_output(
    args: Sequence[str], destination: Path, *, context: Path = REPO_ROOT
) -> None:
    run_buildx_command(
        [
            *args,
            "--output",
            f"type=local,dest={destination}",
            str(context),
        ]
    )


def repository_inputs_digest(
    root: Path, relative_inputs: Sequence[Path], *, namespace: str
) -> str:
    """Hash repository inputs by path, executable bit, and contents."""
    digest = hashlib.sha256()
    digest.update(namespace.encode())
    digest.update(b"\0")

    files: set[Path] = set()
    for relative_input in relative_inputs:
        path = root / relative_input
        if not path.exists() and not path.is_symlink():
            raise SystemExit(f"Rust build input does not exist: {relative_input}")
        if path.is_dir():
            files.update(item for item in path.rglob("*") if not item.is_dir())
        else:
            files.add(path)

    for path in sorted(files, key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(b"executable\0" if path.stat().st_mode & 0o111 else b"regular\0")
        if path.is_symlink():
            digest.update(b"symlink\0")
            digest.update(os.readlink(path).encode())
        else:
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
        digest.update(b"\0")
    return digest.hexdigest()


def rust_artifact_input_manifest(source_root: Path = REPO_ROOT) -> dict[str, Any]:
    try:
        manifest = json.loads(
            (source_root / RUST_ARTIFACT_INPUT_MANIFEST).read_text()
        )
        if not isinstance(manifest, dict) or set(manifest) != {
            "version",
            "common",
            "lanes",
        }:
            raise TypeError("manifest must contain version, common, and lanes")
        if manifest["version"] != 1:
            raise ValueError("unsupported manifest version")
        common = manifest["common"]
        lanes = manifest["lanes"]
        if not isinstance(common, list) or not isinstance(lanes, dict):
            raise TypeError("common must be a list and lanes must be an object")
        for lane, inputs in lanes.items():
            if not isinstance(inputs, dict) or set(inputs) != {"extra", "packages"}:
                raise TypeError(f"{lane} must contain extra and packages lists")
            for group in ("extra", "packages"):
                if not isinstance(inputs[group], list):
                    raise TypeError(f"{lane}.{group} must be a list")
        declared = [*common]
        for inputs in lanes.values():
            declared.extend(inputs["extra"])
            declared.extend(inputs["packages"])
        for value in declared:
            path = Path(value) if isinstance(value, str) else None
            if (
                path is None
                or path.is_absolute()
                or path == Path(".")
                or ".." in path.parts
            ):
                raise ValueError(f"invalid repository-relative input: {value!r}")
    except (KeyError, OSError, TypeError, ValueError) as exc:
        raise SystemExit(
            f"Could not load Rust artifact input manifest: {exc}"
        ) from exc
    return manifest


def rust_artifact_inputs(
    feature: str, source_root: Path = REPO_ROOT
) -> tuple[Path, ...]:
    try:
        manifest = rust_artifact_input_manifest(source_root)
        lane = manifest["lanes"][feature]
    except KeyError as exc:
        raise SystemExit(f"Unknown Rust artifact lane: {feature}") from exc
    return tuple(
        Path(path)
        for path in (*manifest["common"], *lane["extra"], *lane["packages"])
    )


def rust_artifact_input_manifest_from_metadata(
    metadata: dict[str, Any], repo_root: Path
) -> dict[str, list[str]]:
    """Select each bundle's non-dev workspace dependency closure."""
    workspace_members = set(metadata["workspace_members"])
    packages = {
        package["id"]: package
        for package in metadata["packages"]
        if package["id"] in workspace_members
    }
    package_by_name = {
        package["name"]: package_id for package_id, package in packages.items()
    }
    package_by_binary = {
        target["name"]: package_id
        for package_id, package in packages.items()
        for target in package["targets"]
        if "bin" in target["kind"]
    }

    manifest: dict[str, list[str]] = {}
    for feature, binaries in RUST_ARTIFACT_BINARIES.items():
        try:
            pending = [package_by_binary[binary] for binary in binaries]
        except KeyError as exc:
            raise SystemExit(
                f"Could not find workspace package for Rust binary {exc.args[0]!r}"
            ) from exc

        selected: set[str] = set()
        while pending:
            package_id = pending.pop()
            if package_id in selected:
                continue
            selected.add(package_id)
            for dependency in packages[package_id]["dependencies"]:
                dependency_id = package_by_name.get(dependency["name"])
                if dependency["kind"] != "dev" and dependency_id is not None:
                    pending.append(dependency_id)

        manifest[feature] = sorted(
            str(
                Path(packages[package_id]["manifest_path"]).parent.relative_to(
                    repo_root
                )
            )
            for package_id in selected
        )
    return manifest


def generate_rust_artifact_input_manifest(
    repo_root: Path = REPO_ROOT,
) -> dict[str, list[str]]:
    result = run(
        ["cargo", "metadata", "--format-version", "1", "--locked", "--no-deps"],
        capture=True,
        cwd=repo_root,
    )
    return rust_artifact_input_manifest_from_metadata(
        json.loads(result.stdout), repo_root
    )


def regenerate_rust_artifact_input_manifest(repo_root: Path = REPO_ROOT) -> Path:
    manifest_path = repo_root / RUST_ARTIFACT_INPUT_MANIFEST
    manifest = rust_artifact_input_manifest(repo_root)
    package_inputs = generate_rust_artifact_input_manifest(repo_root)
    if set(manifest["lanes"]) != set(package_inputs):
        raise SystemExit("Rust artifact input manifest lanes do not match the build lanes")
    for feature, inputs in package_inputs.items():
        manifest["lanes"][feature]["packages"] = inputs
    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return manifest_path


class BundleBuilder:
    def __init__(
        self,
        features: list[str],
        *,
        source_root: Path = REPO_ROOT,
        source_revision: str | None = None,
        artifact_source_kind: str | None = None,
        artifact_source_revision: str | None = None,
    ) -> None:
        self.features = features
        self.source_root = source_root.resolve()
        self.source_revision = source_revision
        self.artifact_source_kind = artifact_source_kind
        self.artifact_source_revision = artifact_source_revision
        self.aws_cli_ready = False
        self.aws_env_ready = False
        self._targets: list[Target] | None = None
        self.ecr_logged_in_hosts: set[str] = set()
        self.buildx_builder_name = ""
        self.disable_external_cache = False
        self._branch_name: str | None = None
        self._uv_bin: str | None = None
        self._rust_artifact_digests: dict[str, str] = {}
        self._rust_dependency_digests: dict[str, str] = {}
        self._rust_artifact_contexts: dict[str, Path] = {}
        self._temporary_directories: list[tempfile.TemporaryDirectory[str]] = []

    def normalized_publish_policy(self) -> str:
        value = os.environ.get("SPACE_BUNDLE_PUBLISH", "auto")
        if value in {"", "auto"}:
            return "auto"
        if value in {"1", "true", "TRUE", "yes", "YES", "on", "ON"}:
            return "enabled"
        if value in {"0", "false", "FALSE", "no", "NO", "off", "OFF"}:
            return "disabled"
        raise SystemExit(
            f"Invalid SPACE_BUNDLE_PUBLISH value: {value!r}. Expected auto, 0, or 1."
        )

    def should_publish_bundle_images(self) -> bool:
        return self.normalized_publish_policy() != "disabled"

    def publish_mode_message(self) -> str:
        policy = self.normalized_publish_policy()
        if policy == "enabled":
            return f"Publishing enabled via SPACE_BUNDLE_PUBLISH={os.environ.get('SPACE_BUNDLE_PUBLISH')}."
        if policy == "disabled":
            return f"Publishing disabled via SPACE_BUNDLE_PUBLISH={os.environ.get('SPACE_BUNDLE_PUBLISH')}."
        return "Publishing enabled by default."

    def normalized_release_lto_policy(self) -> str:
        value = os.environ.get("SPACE_BUNDLE_RELEASE_LTO", "auto")
        if value in {"", "auto"}:
            return "auto"
        if value in {"1", "true", "TRUE", "yes", "YES", "on", "ON", "enabled", "thin"}:
            return "thin"
        if value == "fat":
            return "fat"
        if value in {"0", "false", "FALSE", "no", "NO", "off", "OFF", "disabled"}:
            return "disabled"
        raise SystemExit(
            f"Invalid SPACE_BUNDLE_RELEASE_LTO value: {value!r}. "
            "Expected auto, thin, fat, or disabled."
        )

    def release_lto_mode(self) -> str:
        policy = self.normalized_release_lto_policy()
        if policy != "auto":
            return policy
        if self.is_main_branch_context() or self.pull_request_number():
            return "thin"
        return "disabled"

    def cargo_release_lto_value(self) -> str:
        return {"thin": "thin", "fat": "true", "disabled": "false"}[
            self.release_lto_mode()
        ]

    def release_lto_message(self) -> str:
        policy = self.normalized_release_lto_policy()
        if policy == "thin":
            return (
                "Rust release ThinLTO enabled via "
                f"SPACE_BUNDLE_RELEASE_LTO={os.environ.get('SPACE_BUNDLE_RELEASE_LTO')}."
            )
        if policy == "fat":
            return (
                "Rust release fat LTO enabled via "
                f"SPACE_BUNDLE_RELEASE_LTO={os.environ.get('SPACE_BUNDLE_RELEASE_LTO')}."
            )
        if policy == "disabled":
            return (
                "Rust release LTO disabled via "
                f"SPACE_BUNDLE_RELEASE_LTO={os.environ.get('SPACE_BUNDLE_RELEASE_LTO')}."
            )
        if self.release_lto_mode() == "thin":
            return "Rust release ThinLTO enabled automatically for PR and main bundle builds."
        return "Rust release LTO disabled automatically outside PR and main bundle builds."

    def normalized_cache_policy(self) -> str:
        value = os.environ.get("SPACE_BUNDLE_CACHE", "auto")
        if value in {"", "auto"}:
            return "auto"
        if value in {"read-only", "readonly"}:
            return "read-only"
        if value in {"1", "true", "TRUE", "yes", "YES", "on", "ON", "enabled"}:
            return "enabled"
        if value in {"0", "false", "FALSE", "no", "NO", "off", "OFF", "disabled"}:
            return "disabled"
        raise SystemExit(
            f"Invalid SPACE_BUNDLE_CACHE value: {value!r}. "
            "Expected auto, enabled, read-only, or disabled."
        )

    def cache_backend(self) -> str:
        value = os.environ.get("SPACE_BUNDLE_CACHE_BACKEND", "registry")
        if value in {"", "auto", "registry"}:
            return "registry"
        if value == "gha":
            return "gha"
        raise SystemExit(
            f"Invalid SPACE_BUNDLE_CACHE_BACKEND value: {value!r}. "
            "Expected registry or gha."
        )

    def branch_name(self) -> str | None:
        if self._branch_name is not None:
            return self._branch_name
        branch = os.environ.get("BUILDKITE_BRANCH")
        github_ref = os.environ.get("GITHUB_REF", "")
        if not branch and github_ref.startswith("refs/heads/"):
            branch = github_ref.removeprefix("refs/heads/")
        if branch:
            self._branch_name = branch
            return branch
        result = run(["git", "branch", "--show-current"], capture=True, cwd=REPO_ROOT, check=False)
        branch = result.stdout.strip() or None
        self._branch_name = branch
        return branch

    def is_main_branch_context(self) -> bool:
        return self.branch_name() == "main"

    def resolve_targets(self) -> list[Target]:
        if self._targets is not None:
            return self._targets

        accounts = json.loads(os.environ.get("SPACE_BUNDLE_TARGET_ACCOUNTS", "{}"))
        if not isinstance(accounts, dict) or not all(
            isinstance(name, str)
            and name
            and isinstance(account, str)
            and re.fullmatch(r"[0-9]{12}", account)
            for name, account in accounts.items()
        ):
            raise SystemExit(
                "SPACE_BUNDLE_TARGET_ACCOUNTS must map target names to 12-digit AWS account IDs"
            )
        spec = os.environ.get("SPACE_BUNDLE_TARGETS", "auto")
        if spec in {"", "auto"}:
            # Default: PR builds publish to testing only. Post-merge `main`
            # builds also fan out to prod so the prod cluster can pull the
            # latest images by commit sha. Other branches stay on testing.
            names = ["testing", "prod"] if (
                self.is_main_branch_context() and not self.pull_request_number()
            ) else ["testing"]
        else:
            names = [name.strip() for name in spec.split(",") if name.strip()]

        targets: list[Target] = []
        seen: set[str] = set()
        for name in names:
            if name in seen:
                continue
            seen.add(name)
            if name not in accounts:
                raise SystemExit(
                    f"Target {name!r} needs an account in SPACE_BUNDLE_TARGET_ACCOUNTS. "
                    f"Configured: {sorted(accounts)}."
                )
            targets.append(Target(name=name, account_id=accounts[name]))

        if not targets:
            raise SystemExit("SPACE_BUNDLE_TARGETS resolved to an empty list.")

        self._targets = targets
        return targets

    def primary_target(self) -> Target:
        # The Buildkite agent runs in this account; the rust-build artifact
        # image stays here so the bundle build can pull it without a
        # cross-account hop.
        return self.resolve_targets()[0]

    def should_use_cache(self) -> bool:
        if self.disable_external_cache:
            return False
        policy = self.normalized_cache_policy()
        if policy in {"enabled", "read-only"}:
            return True
        if policy == "disabled":
            return False
        return bool(os.environ.get("BUILDKITE") or self.should_publish_bundle_images())

    def should_use_registry_cache(self) -> bool:
        return self.cache_backend() == "registry" and self.should_use_cache()

    def should_use_gha_cache(self) -> bool:
        return self.cache_backend() == "gha" and self.should_use_cache()

    def should_export_registry_cache(self) -> bool:
        if not self.should_use_registry_cache():
            return False
        policy = self.normalized_cache_policy()
        if policy == "read-only":
            return False
        if policy == "enabled":
            return True
        if policy == "disabled":
            return False
        # Publishing BuildKit caches is synchronous and can add more than a
        # minute after compilation has finished. PR builds still consume the
        # main cache and use sccache for compiler outputs; main refreshes the
        # BuildKit cache without extending the PR critical path.
        return self.is_main_branch_context() and not self.pull_request_number()

    def should_export_cache(self) -> bool:
        if not self.should_use_cache():
            return False
        if self.cache_backend() == "registry":
            return self.should_export_registry_cache()
        return self.normalized_cache_policy() == "enabled"

    def pull_request_number(self) -> str | None:
        value = os.environ.get("BUILDKITE_PULL_REQUEST")
        if not value or value == "false":
            return None
        return value

    def cache_mode_message(self) -> str:
        policy = self.normalized_cache_policy()
        backend = self.cache_backend()
        if policy == "read-only":
            return (
                f"{backend} cache enabled for imports only via "
                f"SPACE_BUNDLE_CACHE={os.environ.get('SPACE_BUNDLE_CACHE')}."
            )
        if policy == "enabled":
            return (
                f"{backend} cache enabled via "
                f"SPACE_BUNDLE_CACHE={os.environ.get('SPACE_BUNDLE_CACHE')}."
            )
        if policy == "disabled":
            return (
                f"{backend} cache disabled via "
                f"SPACE_BUNDLE_CACHE={os.environ.get('SPACE_BUNDLE_CACHE')}."
            )
        if not self.should_use_cache():
            return f"{backend} cache disabled automatically."
        if backend == "gha":
            return "gha cache auto mode: importing cache and skipping exports."
        pr = self.pull_request_number()
        if pr:
            return (
                f"Registry cache auto mode: reusing content-addressed Rust dependency cache; "
                f"importing main + existing PR-{pr} image cache and skipping synchronous "
                "cache exports."
            )
        if self.is_main_branch_context():
            return (
                "Registry cache auto mode: reusing content-addressed Rust dependency cache; "
                "importing and exporting main image cache."
            )
        return "Registry cache auto mode: importing cache and skipping exports outside main."

    def should_use_explicit_buildx_builder(self) -> bool:
        return self.should_publish_bundle_images() or self.should_use_cache()

    def should_push_remote_rust_artifact(self) -> bool:
        # The GitHub Actions cache carries Rust artifacts between runs, so a
        # gha build hands them to the bundle build as a local context and keeps
        # the registry to deployable images.
        return bool(self.buildx_builder_name) and self.cache_backend() == "registry"

    @staticmethod
    def repository_name_for_feature(feature: str) -> str:
        return RELEASE_IMAGE_REPOSITORIES.get(feature, f"space/{feature}")

    def dockerfile_for_feature(self, feature: str) -> Path:
        dockerfiles = {
            "worker": self.source_root / "docker/Dockerfile.worker",
            "builder": self.source_root / "docker/Dockerfile.builder-bundle",
            "api": self.source_root / "docker/Dockerfile.api",
            "data-plane": self.source_root / "docker/Dockerfile.data-plane",
            "cluster-controller": self.source_root
            / "docker/Dockerfile.cluster-controller",
            "ui": self.source_root / "docker/Dockerfile.ui",
            "rollout-mng": self.source_root / "docker/Dockerfile.space-rollout-mng",
        }
        try:
            return dockerfiles[feature]
        except KeyError as exc:
            raise SystemExit(f"Unknown SPACE feature: {feature}") from exc

    @staticmethod
    def rust_build_target_for_feature(feature: str) -> str:
        targets = {
            "worker": "build-artifacts-worker",
            "builder": "build-artifacts-builder",
            "api": "build-artifacts-api",
            "data-plane": "build-artifacts-data-plane",
            "cluster-controller": "build-artifacts-cluster-controller",
        }
        try:
            return targets[feature]
        except KeyError as exc:
            raise SystemExit(f"Feature {feature} does not have a Rust artifact target.") from exc

    @staticmethod
    def is_rust_feature(feature: str) -> bool:
        return feature in {"worker", "builder", "api", "data-plane", "cluster-controller"}

    @staticmethod
    def docker_build_args_for_feature(feature: str) -> list[str]:
        if feature == "ui":
            return [
                f"NEXT_PUBLIC_API_URL={SPACE_UI_API_URL}",
                f"NEXT_PUBLIC_DATA_PLANE_URL={SPACE_UI_DATA_PLANE_URL}",
            ]
        return []

    @staticmethod
    def cache_tag_for_kind(kind: str) -> str:
        mapping = {"rust": "buildcache-rust", "image": "buildcache-image"}
        try:
            return mapping[kind]
        except KeyError as exc:
            raise SystemExit(f"Unknown cache kind: {kind}") from exc

    @staticmethod
    def sanitize_cache_tag_component(value: str) -> str:
        sanitized = re.sub(r"[^0-9A-Za-z_.-]+", "-", value).strip(".-")
        return sanitized[:100] or "unknown"

    @staticmethod
    def aws_command(*args: str) -> list[str]:
        return ["aws", "--no-cli-pager", *args]

    @staticmethod
    def ci_commit() -> str | None:
        return os.environ.get("BUILDKITE_COMMIT") or os.environ.get("GITHUB_SHA")

    @classmethod
    def space_bundle_image_tag(cls) -> str:
        return cls.ci_commit() or "local"

    def local_image_ref_for_feature(self, feature: str) -> str:
        return f"space-{feature}:{self.space_bundle_image_tag()}"

    def local_rust_build_image(self, feature: str) -> str:
        return f"space-rust-build-{feature}:{self.space_bundle_image_tag()}"

    def remote_image_ref(self, target: Target, feature: str) -> str:
        return (
            f"{target.registry_host}/{self.repository_name_for_feature(feature)}:"
            f"{self.space_bundle_image_tag()}"
        )

    def artifact_source_tag(self) -> str:
        if not self.artifact_source_kind or not self.artifact_source_revision:
            raise SystemExit("artifact source identity is not configured")
        return (
            f"{self.space_bundle_image_tag()}-source-{self.artifact_source_kind}-"
            f"{self.artifact_source_revision}"
        )

    def remote_source_identity_ref(self, target: Target, feature: str) -> str:
        return (
            f"{target.registry_host}/{self.repository_name_for_feature(feature)}:"
            f"{self.artifact_source_tag()}"
        )

    def remote_image_refs(self, target: Target, feature: str) -> list[str]:
        refs = [self.remote_image_ref(target, feature)]
        if self.artifact_source_kind:
            refs.append(self.remote_source_identity_ref(target, feature))
        return refs

    def ecr_image_args(self, target: Target, feature: str) -> tuple[str, ...]:
        return (
            "--region", AWS_REGION,
            "--registry-id", target.account_id,
            "--repository-name", self.repository_name_for_feature(feature),
        )

    def published_images(
        self, target: Target, feature: str, tags: Sequence[str]
    ) -> tuple[dict[str, EcrImage], dict[str, str]]:
        """The stored manifests of whichever of `tags` exist, and the failure
        code of each tag that exists but couldn't be read.

        A missing tag (`ImageNotFound`) is simply absent from both.
        """
        response = json.loads(
            run(
                self.aws_command(
                    "ecr", "batch-get-image", *self.ecr_image_args(target, feature),
                    "--image-ids", *(f"imageTag={tag}" for tag in tags),
                    "--accepted-media-types", *ECR_MANIFEST_MEDIA_TYPES,
                    "--query",
                    "{images: images[].[imageId.imageTag,imageManifest,"
                    "imageManifestMediaType,imageId.imageDigest], "
                    "failures: failures[].[imageId.imageTag,failureCode]}",
                    "--output", "json",
                ),
                capture=True,
            ).stdout
        )
        images = {
            tag: EcrImage(manifest=manifest, media_type=media_type, digest=digest)
            for tag, manifest, media_type, digest in response.get("images") or ()
        }
        failures = {
            tag: code
            for tag, code in response.get("failures") or ()
            if code != "ImageNotFound"
        }
        return images, failures

    def own_image(self, target: Target, feature: str) -> EcrImage | None:
        """This build's commit-tag image, or None when it isn't published.

        Any failure other than a missing tag stops the job, because a re-run
        taking "not published" would rebuild over an immutable tag.
        """
        tag = self.space_bundle_image_tag()
        images, failures = self.published_images(target, feature, [tag])
        if tag in failures:
            raise SystemExit(
                f"Reading {feature}:{tag} from {target.name} failed: {failures[tag]}"
            )
        return images.get(tag)

    def commit_images_published(self, feature: str) -> bool:
        """Whether every target already holds this commit's image.

        Commit tags are immutable, so a re-run that rebuilt and pushed again
        would be rejected; it skips the build instead.
        """
        if not (
            self.should_publish_bundle_images()
            and self.ci_commit()
            and not self.artifact_source_kind
        ):
            return False
        self.prepare_aws_environment()
        return all(
            self.own_image(target, feature) is not None
            for target in self.resolve_targets()
        )

    def rust_artifact_digest(self, feature: str) -> str:
        if feature not in self._rust_artifact_digests:
            namespace = (
                f"space-rust-artifact-v2\0feature={feature}"
                f"\0lto={self.cargo_release_lto_value()}"
            )
            if self.source_revision:
                namespace += f"\0source={self.source_revision}"
            self._rust_artifact_digests[feature] = repository_inputs_digest(
                self.source_root,
                rust_artifact_inputs(feature, self.source_root),
                namespace=namespace,
            )
        return self._rust_artifact_digests[feature]

    def rust_dependency_digest(self, feature: str) -> str:
        if feature not in self._rust_dependency_digests:
            manifests = tuple(
                path.relative_to(self.source_root)
                for path in sorted((self.source_root / "crates").rglob("Cargo.toml"))
            )
            namespace = (
                f"space-rust-dependencies-v1\0feature={feature}"
                f"\0lto={self.cargo_release_lto_value()}"
            )
            if self.source_revision:
                namespace += f"\0source={self.source_revision}"
            self._rust_dependency_digests[feature] = repository_inputs_digest(
                self.source_root,
                (*RUST_DEPENDENCY_INPUTS, *manifests),
                namespace=namespace,
            )
        return self._rust_dependency_digests[feature]

    def rust_artifact_tag(self, feature: str) -> str:
        return f"rust-content-{self.rust_artifact_digest(feature)}"

    def remote_rust_artifact_ref(self, target: Target, feature: str) -> str:
        return (
            f"{target.registry_host}/{self.repository_name_for_feature(feature)}:"
            f"{self.rust_artifact_tag(feature)}"
        )

    def rust_dependency_cache_tag(self, feature: str) -> str:
        return f"buildcache-rust-deps-{self.rust_dependency_digest(feature)}"

    def rust_dependency_cache_ref(self, target: Target, feature: str) -> str:
        return (
            f"{target.registry_host}/{self.repository_name_for_feature(feature)}:"
            f"{self.rust_dependency_cache_tag(feature)}"
        )

    @classmethod
    def build_git_sha(cls) -> str:
        commit = (cls.ci_commit() or "").strip()
        if commit:
            return commit
        result = run(
            ["git", "rev-parse", "HEAD"],
            capture=True,
            cwd=REPO_ROOT,
            check=False,
        )
        return result.stdout.strip() or "unknown"

    def main_cache_ref(self, target: Target, feature: str, kind: str) -> str:
        return (
            f"{target.registry_host}/{self.repository_name_for_feature(feature)}:"
            f"{self.cache_tag_for_kind(kind)}"
        )

    def pr_cache_tag_for_kind(self, kind: str) -> str | None:
        pr = self.pull_request_number()
        if not pr:
            return None
        return f"{self.cache_tag_for_kind(kind)}-pr-{pr}"

    def pr_cache_ref(self, target: Target, feature: str, kind: str) -> str | None:
        tag = self.pr_cache_tag_for_kind(kind)
        if not tag:
            return None
        return f"{target.registry_host}/{self.repository_name_for_feature(feature)}:{tag}"

    @staticmethod
    def buildx_cache_to_option(cache_ref: str) -> str:
        return (
            f"type=registry,ref={cache_ref},mode=max,oci-mediatypes=true,"
            "image-manifest=true,ignore-error=true"
        )

    @staticmethod
    def gha_cache_scope(feature: str, kind: str) -> str:
        return f"space-bundle-{feature}-{kind}"

    def append_gha_cache(self, command: list[str], feature: str, kind: str) -> None:
        if not self.should_use_gha_cache():
            return
        scope = self.gha_cache_scope(feature, kind)
        command.extend(["--cache-from", f"type=gha,scope={scope}"])
        if self.should_export_cache():
            command.extend(
                [
                    "--cache-to",
                    f"type=gha,scope={scope},mode=max,ignore-error=true",
                ]
            )

    def ensure_uv(self) -> str:
        if self._uv_bin is not None:
            return self._uv_bin
        uv_cache_dir = Path(
            os.environ.get("UV_CACHE_DIR", f"{os.environ.get('TMPDIR', '/tmp')}/space-uv-cache")
        )
        uv_cache_dir.mkdir(parents=True, exist_ok=True)
        os.environ["UV_CACHE_DIR"] = str(uv_cache_dir)
        ensure_uv_script = self.source_root / "ci/scripts/ensure-uv.sh"
        uv_bin = run(
            ["bash", str(ensure_uv_script)], capture=True, cwd=self.source_root
        ).stdout.strip()
        if not uv_bin:
            raise SystemExit("Failed to resolve uv while preparing Buildkite tooling.")
        self._uv_bin = uv_bin
        return uv_bin

    def run_install_tools(self, *tools: str) -> None:
        if self.should_publish_bundle_images() and os.environ.get("GITHUB_ACTIONS"):
            # A publishing CI build holds registry credentials and can mint the
            # job's OIDC token, so it never downloads and runs an installer:
            # the tools must already be on the runner.
            raise SystemExit(
                f"Publishing builds don't install tools; {', '.join(tools)} "
                "must already be on PATH."
            )
        uv_bin = self.ensure_uv()
        install_tools_script = self.source_root / "ci/scripts/install-tools.py"
        run(
            [uv_bin, "run", "--script", str(install_tools_script), *tools],
            cwd=self.source_root,
        )

    def wait_for_docker(self) -> None:
        if not (
            shutil.which("docker")
            and run(["docker", "buildx", "version"], check=False).returncode == 0
        ):
            self.run_install_tools("docker-cli", "docker-buildx-plugin")
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if run(["docker", "info"], check=False).returncode == 0:
                break
            time.sleep(1)
        else:
            raise SystemExit("Docker daemon is not available.")

        if run(["docker", "buildx", "version"], check=False).returncode != 0:
            raise SystemExit("docker buildx is not available after installing the Docker CLI tools.")

    def setup_buildx_builder(self) -> None:
        if not self.should_use_explicit_buildx_builder() or self.buildx_builder_name:
            return

        self.buildx_builder_name = f"space-bundle-{random.randint(1000, 9999)}{random.randint(1000, 9999)}"
        print(f"--- :docker: Creating buildx builder {self.buildx_builder_name}")

        create = run(
            [
                "docker",
                "buildx",
                "create",
                "--name",
                self.buildx_builder_name,
                "--driver",
                "docker-container",
                "--driver-opt",
                f"image={BUILDKIT_IMAGE}",
                "--use",
            ],
            check=False,
        )
        if create.returncode != 0:
            print(
                "--- :warning: Failed to create a docker-container buildx builder; falling back to the default builder."
            )
            print(
                "--- :warning: External cache is disabled for this run because the default builder may not support it."
            )
            self.buildx_builder_name = ""
            self.disable_external_cache = True
            self.require_pinned_builder_when_publishing()
            return

        inspect = run(
            ["docker", "buildx", "inspect", self.buildx_builder_name, "--bootstrap"],
            check=False,
        )
        if inspect.returncode == 0:
            return

        print(
            f"--- :warning: Failed to bootstrap buildx builder {self.buildx_builder_name}; falling back to the default builder."
        )
        print(
            "--- :warning: External cache is disabled for this run because the default builder may not support it."
        )
        run(["docker", "buildx", "rm", "--force", self.buildx_builder_name], check=False)
        self.buildx_builder_name = ""
        self.disable_external_cache = True
        self.require_pinned_builder_when_publishing()

    def require_pinned_builder_when_publishing(self) -> None:
        # The default builder is the runner's own, unpinned BuildKit, and the
        # builder does the push, so a publishing build doesn't fall back to it.
        if self.should_publish_bundle_images():
            raise SystemExit(
                f"Couldn't start the pinned BuildKit builder ({BUILDKIT_IMAGE}); "
                "publishing builds don't fall back to the default builder."
            )

    def cleanup_buildx_builder(self) -> None:
        if self.buildx_builder_name:
            run(["docker", "buildx", "rm", "--force", self.buildx_builder_name], check=False)
            self.buildx_builder_name = ""
        for temporary_directory in self._temporary_directories:
            temporary_directory.cleanup()
        self._temporary_directories.clear()
        self._rust_artifact_contexts.clear()

    def ensure_aws_cli(self) -> None:
        if self.aws_cli_ready:
            return
        if not shutil.which("aws"):
            self.run_install_tools("aws-cli")
        self.aws_cli_ready = True

    def prepare_aws_environment(self) -> None:
        if self.aws_env_ready:
            return
        self.ensure_aws_cli()
        os.environ.pop("AWS_PROFILE", None)
        os.environ.pop("AWS_DEFAULT_PROFILE", None)
        os.environ["AWS_PAGER"] = ""
        self.aws_env_ready = True

    def image_tag_exists(self, target: Target, feature: str, tag: str) -> bool:
        repository_name = self.repository_name_for_feature(feature)
        result = run(
            self.aws_command(
                "ecr",
                "describe-images",
                "--region",
                AWS_REGION,
                "--registry-id",
                target.account_id,
                "--repository-name",
                repository_name,
                "--image-ids",
                f"imageTag={tag}",
            ),
            check=False,
        )
        return result.returncode == 0

    def cache_tag_exists(
        self, target: Target, feature: str, kind: str, *, tag: str | None = None
    ) -> bool:
        return self.image_tag_exists(
            target,
            feature,
            tag or self.cache_tag_for_kind(kind),
        )

    def ensure_ecr_login(self) -> None:
        # ECR auth tokens issued by `get-login-password` are scoped to the IAM
        # principal that called STS; the same token authorizes pushes to any
        # ECR repo (in any account) where that principal is admitted by the
        # repository policy. So we obtain one token and `docker login` to each
        # target's registry host with it.
        targets = self.resolve_targets()
        unseen = [t for t in targets if t.registry_host not in self.ecr_logged_in_hosts]
        if not unseen:
            return
        self.prepare_aws_environment()
        password = run(
            self.aws_command("ecr", "get-login-password", "--region", AWS_REGION),
            capture=True,
        ).stdout
        for target in unseen:
            print(f"--- :docker: Logging into {target.registry_host} ({target.name})")
            run(
                ["docker", "login", "--username", "AWS", "--password-stdin", target.registry_host],
                input_text=password,
            )
            self.ecr_logged_in_hosts.add(target.registry_host)

    @staticmethod
    def mirrored_base_image_contexts(command: Sequence[str]) -> list[str]:
        """Build contexts that take the command's Dockerfile's bases from GitHub CI's GHCR mirror.

        Empty unless SPACE_CI_IMAGE_MIRROR is set, so other builders keep the
        Dockerfile's own FROM images. See ci/mirrored-images.txt.
        """
        if not os.environ.get("SPACE_CI_IMAGE_MIRROR") or "--file" not in command:
            return []
        dockerfile = command[list(command).index("--file") + 1]
        result = run(
            ["bash", str(MIRROR_IMAGES_SCRIPT), "build-contexts", dockerfile],
            capture=True,
        )
        sys.stderr.write(result.stderr)
        return result.stdout.split()

    def docker_buildx_base_cmd(self) -> list[str]:
        command = ["docker", "buildx", "build"]
        if self.buildx_builder_name:
            command.extend(["--builder", self.buildx_builder_name])
        return command

    @staticmethod
    def rust_sccache_build_options() -> list[str]:
        bucket = os.environ.get("SCCACHE_BUCKET", "").strip()
        if not bucket:
            return ["--build-arg", "SPACE_SCCACHE_ENABLED=false"]
        options = [
            "--build-arg",
            f"SPACE_SCCACHE_BUCKET={bucket}",
            "--build-arg",
            f"SPACE_SCCACHE_REGION={SCCACHE_REGION}",
            "--build-arg",
            f"SPACE_SCCACHE_S3_KEY_PREFIX={SCCACHE_S3_KEY_PREFIX}",
            "--build-arg",
            f"SPACE_SCCACHE_S3_RW_MODE={SCCACHE_S3_RW_MODE}",
            "--build-arg",
            (
                "SPACE_SCCACHE_S3_SERVER_SIDE_ENCRYPTION="
                f"{SCCACHE_S3_SERVER_SIDE_ENCRYPTION}"
            ),
        ]

        role_arn = os.environ.get("AWS_ROLE_ARN")
        token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")
        if not role_arn or not token_file or not Path(token_file).is_file():
            print(
                "--- :warning: IRSA web identity is unavailable; "
                "release builds will run without sccache."
            )
            options.extend(["--build-arg", "SPACE_SCCACHE_ENABLED=false"])
            return options

        print(
            f"--- :zap: Enabling release sccache via s3://{bucket}/"
            f"{SCCACHE_S3_KEY_PREFIX}"
        )
        options.extend(
            [
                "--secret",
                f"id=aws_web_identity_token,src={token_file}",
                "--build-arg",
                "SPACE_SCCACHE_ENABLED=true",
                "--build-arg",
                f"SPACE_SCCACHE_ROLE_ARN={role_arn}",
            ]
        )
        return options

    def maybe_append_cache_import(self, command: list[str], feature: str, kind: str) -> None:
        if self.should_use_gha_cache():
            self.append_gha_cache(command, feature, kind)
            return
        if not self.should_use_registry_cache():
            return
        label = "Rust" if kind == "rust" else "image"

        # Cache imports come from the primary target only. Other targets
        # (e.g. prod on `main`) only ever see main-line images, so their
        # caches are derivative of the primary's and add nothing on import.
        primary = self.primary_target()
        imported: list[str] = []

        pr_tag = self.pr_cache_tag_for_kind(kind)
        if pr_tag and self.cache_tag_exists(primary, feature, kind, tag=pr_tag):
            command.extend(
                ["--cache-from", f"type=registry,ref={self.pr_cache_ref(primary, feature, kind)}"]
            )
            imported.append(f"{primary.name} PR-{self.pull_request_number()}")

        if self.cache_tag_exists(primary, feature, kind):
            command.extend(
                ["--cache-from", f"type=registry,ref={self.main_cache_ref(primary, feature, kind)}"]
            )
            imported.append(f"{primary.name} main")

        if not imported:
            print(f"--- :docker: No existing {label} cache found for {feature}; skipping cache import.")
        else:
            print(f"--- :docker: Importing {label} cache for {feature} from: {', '.join(imported)}")

    def maybe_append_cache_export(
        self, command: list[str], feature: str, kind: str, *, targets: list[Target]
    ) -> None:
        if self.should_use_gha_cache():
            return
        if not self.should_export_registry_cache():
            return

        pr = self.pull_request_number()
        label = "Rust" if kind == "rust" else "image"
        exported: list[str] = []
        for target in targets:
            if pr:
                cache_ref = self.pr_cache_ref(target, feature, kind)
                where = f"{target.name} PR-{pr}"
            else:
                cache_ref = self.main_cache_ref(target, feature, kind)
                where = f"{target.name} main"
            if not cache_ref:
                continue
            command.extend(
                [
                    "--cache-to",
                    self.buildx_cache_to_option(cache_ref),
                ]
            )
            exported.append(where)
        if exported:
            print(f"--- :docker: Exporting {label} cache for {feature} to: {', '.join(exported)}")

    def append_rust_dependency_cache(self, command: list[str], feature: str) -> None:
        if self.should_use_gha_cache():
            self.append_gha_cache(command, feature, "rust")
            return
        if not self.should_use_registry_cache():
            return

        primary = self.primary_target()
        cache_tag = self.rust_dependency_cache_tag(feature)
        cache_ref = self.rust_dependency_cache_ref(primary, feature)
        if self.image_tag_exists(primary, feature, cache_tag):
            command.extend(["--cache-from", f"type=registry,ref={cache_ref}"])
            print(
                f"--- :docker: Importing content-addressed Rust dependency cache for {feature}."
            )
            return

        # Seed the first content-addressed cache from the previous mutable main
        # cache so rollout does not throw away already-built dependencies.
        if self.cache_tag_exists(primary, feature, "rust"):
            command.extend(
                [
                    "--cache-from",
                    f"type=registry,ref={self.main_cache_ref(primary, feature, 'rust')}",
                ]
            )
            print(f"--- :docker: Seeding Rust dependency cache for {feature} from main.")

        if self.should_export_registry_cache():
            command.extend(["--cache-to", self.buildx_cache_to_option(cache_ref)])
            print(
                f"--- :docker: Exporting new Rust dependency cache for {feature} "
                f"({self.rust_dependency_digest(feature)[:12]})."
            )

    def create_rust_artifact_context(self, feature: str) -> Path:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix=f"space-rust-artifact-{feature}-"
        )
        self._temporary_directories.append(temporary_directory)
        context = Path(temporary_directory.name) / "rootfs"
        self._rust_artifact_contexts[feature] = context
        return context

    def rust_artifact_build_options(self, feature: str) -> list[str]:
        """Select the artifact recipe; composition may extend it without owning publication."""
        return [
            "--file", str(self.source_root / "docker/Dockerfile.rust-build-common"),
            "--target", self.rust_build_target_for_feature(feature),
        ]

    def build_rust_artifacts(self, feature: str) -> None:
        # The Rust build artifact image is consumed only by the bundle build
        # in this same job, so it always lives in the primary (BK agent's)
        # account. Bundle builds for non-primary targets cross-account-pull
        # nothing extra: they reference the primary's rust artifact image.
        push_remote = self.should_push_remote_rust_artifact()
        local_context: Path | None = None
        if push_remote:
            primary = self.primary_target()
            output_ref = self.remote_rust_artifact_ref(primary, feature)
            if self.image_tag_exists(primary, feature, self.rust_artifact_tag(feature)):
                print(
                    f"--- :zap: Reusing content-identical Rust artifacts for {feature} "
                    f"({self.rust_artifact_digest(feature)[:12]})."
                )
                return
        elif self.buildx_builder_name:
            # A docker-container builder can't see images loaded into the
            # Docker daemon, so the bundle build receives the artifacts as a
            # named build context instead.
            local_context = self.create_rust_artifact_context(feature)
            output_ref = str(local_context)
        else:
            output_ref = self.local_rust_build_image(feature)

        print(f"--- :docker: Building Rust artifacts for {feature} ({output_ref})")
        command = self.docker_buildx_base_cmd()
        command.extend(
            [
                "--platform",
                "linux/amd64",
                "--progress",
                "plain",
                "--provenance",
                "false",
                "--build-arg",
                f"SPACE_RUST_ARTIFACT_SET={feature}",
                "--build-arg",
                f"SPACE_CARGO_RELEASE_LTO={self.cargo_release_lto_value()}",
            ]
        )
        command.extend(self.rust_artifact_build_options(feature))
        command.extend(self.mirrored_base_image_contexts(command))
        if local_context is None:
            command.extend(["--tag", output_ref])
        command.extend(self.rust_sccache_build_options())

        self.append_rust_dependency_cache(command, feature)

        if local_context is not None:
            run_buildx_local_output(command, local_context, context=self.source_root)
        else:
            run_buildx(command, push=push_remote, context=self.source_root)

    def build_space_bundle_image(self, feature: str) -> None:
        dockerfile = self.dockerfile_for_feature(feature)
        publish = self.should_publish_bundle_images()
        targets = self.resolve_targets() if publish or self.should_use_registry_cache() else []

        if publish:
            output_refs = [
                ref for target in targets for ref in self.remote_image_refs(target, feature)
            ]
            primary_label = output_refs[0]
        else:
            output_refs = [self.local_image_ref_for_feature(feature)]
            primary_label = output_refs[0]

        print(f"--- :docker: Building {primary_label}")
        if publish and len(targets) > 1:
            print(
                "--- :docker: Bundle image will also be pushed to: "
                + ", ".join(f"{t.name} ({t.registry_host})" for t in targets[1:])
            )
        command = self.docker_buildx_base_cmd()
        command.extend(
            [
                "--file",
                str(dockerfile),
                "--platform",
                "linux/amd64",
                "--progress",
                "plain",
                "--provenance",
                "false",
            ]
        )
        if self.artifact_source_kind and self.artifact_source_revision:
            command.extend(
                [
                    "--label",
                    f"org.opencontainers.image.revision={self.build_git_sha()}",
                    "--label",
                    f"org.openspace.source.kind={self.artifact_source_kind}",
                    "--label",
                    (
                        "org.openspace.source.revision="
                        f"{self.artifact_source_revision}"
                    ),
                ]
            )
        for ref in output_refs:
            command.extend(["--tag", ref])

        for build_arg in self.docker_build_args_for_feature(feature):
            command.extend(["--build-arg", build_arg])
        command.extend(self.mirrored_base_image_contexts(command))

        if self.is_rust_feature(feature):
            if self.should_push_remote_rust_artifact():
                rust_build_image = self.remote_rust_artifact_ref(self.primary_target(), feature)
            elif feature in self._rust_artifact_contexts:
                rust_build_image = f"space-rust-artifact-{feature}"
                command.extend(
                    [
                        "--build-context",
                        f"{rust_build_image}={self._rust_artifact_contexts[feature]}",
                    ]
                )
            else:
                rust_build_image = self.local_rust_build_image(feature)
            command.extend(["--build-arg", f"SPACE_RUST_BUILD_IMAGE={rust_build_image}"])
            command.extend(["--build-arg", f"SPACE_GIT_SHA={self.build_git_sha()}"])

        self.maybe_append_cache_import(command, feature, "image")
        self.maybe_append_cache_export(command, feature, "image", targets=targets[:1])

        run_buildx(command, push=publish, context=self.source_root)

    def publish_space_bundle_image_message(self, feature: str) -> None:
        if not self.should_publish_bundle_images():
            print(
                f"--- :package: Publish disabled; retained {self.local_image_ref_for_feature(feature)} locally."
            )
            return
        for target in self.resolve_targets():
            print(
                f"--- :package: Published {', '.join(self.remote_image_refs(target, feature))}"
            )

    def build_space_bundle_feature(self, feature: str) -> None:
        if self.commit_images_published(feature):
            print(
                f"--- :package: {feature} is already published for "
                f"{self.space_bundle_image_tag()}; skipping the build."
            )
        else:
            if self.is_rust_feature(feature):
                self.build_rust_artifacts(feature)
            self.build_space_bundle_image(feature)
            self.publish_space_bundle_image_message(feature)

    def targets_message(self) -> str:
        if not self.should_publish_bundle_images() and not self.should_use_registry_cache():
            return "No registry targets needed for this local build."
        targets = self.resolve_targets()
        names = ", ".join(f"{t.name} ({t.account_id})" for t in targets)
        return f"Publish targets: {names}."

    def execute(self) -> None:
        print(f"--- :docker: {self.publish_mode_message()}")
        print(f"--- :docker: {self.release_lto_message()}")
        print(f"--- :docker: {self.cache_mode_message()}")
        print(f"--- :docker: {self.targets_message()}")
        self.wait_for_docker()
        self.setup_buildx_builder()
        try:
            if self.should_publish_bundle_images() or self.should_use_registry_cache():
                self.ensure_ecr_login()
            for feature in self.features:
                self.build_space_bundle_feature(feature)
        finally:
            self.cleanup_buildx_builder()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and publish SPACE bundle images."
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=REPO_ROOT,
        help="repository tree used for Dockerfiles, build inputs, and build context",
    )
    parser.add_argument(
        "--source-revision",
        help="optional immutable source identity included in content-addressed cache keys",
    )
    parser.add_argument(
        "--artifact-source-kind",
        choices=("space", "pinned"),
        help="source repository that produced the release artifact",
    )
    parser.add_argument(
        "--artifact-source-revision",
        help="exact source revision that produced the release artifact",
    )
    parser.add_argument(
        "--regenerate-rust-artifact-inputs",
        action="store_true",
        help="regenerate the committed per-bundle Rust dependency manifest",
    )
    parser.add_argument(
        "features",
        nargs="*",
        metavar="FEATURE",
    )
    args = parser.parse_args(argv)
    supported_features = {
        "worker",
        "builder",
        "api",
        "data-plane",
        "cluster-controller",
        "ui",
        "rollout-mng",
    }
    unknown_features = sorted(set(args.features) - supported_features)
    if unknown_features:
        parser.error(f"unknown bundle feature(s): {', '.join(unknown_features)}")
    if args.regenerate_rust_artifact_inputs:
        if args.features:
            parser.error(
                "--regenerate-rust-artifact-inputs cannot be combined with bundle features"
            )
    elif not args.features:
        args.features = [
            "worker",
            "builder",
            "api",
            "data-plane",
            "cluster-controller",
            "ui",
            "rollout-mng",
        ]
    if bool(args.artifact_source_kind) != bool(args.artifact_source_revision):
        parser.error(
            "--artifact-source-kind and --artifact-source-revision must be provided together"
        )
    if args.artifact_source_revision and not re.fullmatch(
        r"[0-9a-f]{40}", args.artifact_source_revision
    ):
        parser.error("--artifact-source-revision must be a full lowercase git SHA")
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.regenerate_rust_artifact_inputs:
        source_root = args.source_root.resolve()
        manifest_path = regenerate_rust_artifact_input_manifest(source_root)
        print(f"Regenerated {manifest_path.relative_to(source_root)}")
        return 0
    BundleBuilder(
        args.features,
        source_root=args.source_root,
        source_revision=args.source_revision,
        artifact_source_kind=args.artifact_source_kind,
        artifact_source_revision=args.artifact_source_revision,
    ).execute()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
