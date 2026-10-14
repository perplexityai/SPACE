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
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
RELEASE_IMAGE_MANIFEST = REPO_ROOT / "ci/space-helm-release-images.json"
MIRROR_IMAGES_SCRIPT = SCRIPT_DIR / "mirror-images.sh"
MIRRORED_IMAGES_LIST = REPO_ROOT / "ci/mirrored-images.txt"
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
# resolved to when this was pinned. GitHub CI takes it from the GHCR mirror
# (ci/mirrored-images.txt lists it under this name) before the source.
BUILDKIT_IMAGE = (
    "moby/buildkit:v0.32.2"
    "@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8"
)
BUILDX_RATE_LIMIT_MAX_ATTEMPTS = 5
BUILDX_RATE_LIMIT_INITIAL_DELAY_SECONDS = 2
# The registry login is a few seconds of work, so wait out a short outage:
# five attempts of at most 30 seconds each, 5 + 10 + 20 + 40 seconds apart,
# so at most 225 seconds per command.
ECR_LOGIN_MAX_ATTEMPTS = 5
ECR_LOGIN_INITIAL_DELAY_SECONDS = 5
REGISTRY_COMMAND_TIMEOUT_SECONDS = 30
# Bounds each lookup of a cache tag, so an unreachable registry costs a cold
# build rather than a hung job.
REGISTRY_LOOKUP_TIMEOUT_SECONDS = 60
# Bound each step of starting a builder, so a stalled image pull moves on to
# the next BuildKit candidate: at most 300 seconds per candidate, 600 for the
# mirror and the source together.
BUILDX_CREATE_TIMEOUT_SECONDS = 60
BUILDX_BOOTSTRAP_TIMEOUT_SECONDS = 180
BUILDX_RM_TIMEOUT_SECONDS = 60
# Bounds mirror-images.sh's lookup of a Dockerfile's mirrored bases, which
# inspects each one in the mirror; past it, the build uses the FROM images.
MIRROR_LOOKUP_TIMEOUT_SECONDS = 120
# Downstream builder subclasses still read these during their compatibility transition.
MAIN_HISTORY_DEPTH = 100
MAIN_HISTORY_READ_ATTEMPTS = 3
REGISTRY_RATE_LIMIT_PATTERN = re.compile(
    r"(?:\btoomanyrequests:\s*Rate exceeded\b|\b429 Too Many Requests\b)",
    re.IGNORECASE,
)
# A push the network or registry dropped. A refusal (bad credentials, an
# immutable tag) names none of these, so it fails at once.
TRANSIENT_PUSH_FAILURE_PATTERN = re.compile(
    r"\bfailed to push\b.*(?:TLS handshake timeout|i/o timeout|connection reset"
    r"|connection refused|unexpected EOF|broken pipe|no such host"
    r"|server misbehaving|context deadline exceeded"
    r"|\b50[0234] (?:Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout)\b)",
    re.IGNORECASE,
)
# A build that could not read a layer blob a manifest named: missing,
# unreadable or corrupt. BuildKit reports it the same way while building and,
# for layers it took from a cache, while pushing ("failed to push ...:
# httpReadSeeker: ..."). The blob may belong to an imported cache or to a
# mirrored base image.
LAYER_READ_FAILURE_PATTERN = re.compile(
    r"^ERROR:.*(?:httpReadSeeker|could not fetch content descriptor"
    r"|unexpected commit digest|failed size validation)"
)
# The progress lines of BuildKit's cache export, which logs its failures even
# when `ignore-error=true` keeps them from failing the build.
CACHE_EXPORT_VERTEX_PATTERN = re.compile(r"^#(\d+) exporting cache\b")
VERTEX_ERROR_PATTERN = re.compile(r"^#(\d+) ERROR: (.*)")
# Answers a retry cannot change: rejected, expired or missing credentials.
REGISTRY_AUTH_FAILURE_PATTERN = re.compile(
    r"(?:\b40[013] (?:Bad Request|Unauthorized|Forbidden)\b|unauthorized|denied"
    r"|ExpiredToken|InvalidClientTokenId|UnrecognizedClient|SignatureDoesNotMatch"
    r"|Unable to locate credentials)",
    re.IGNORECASE,
)
# Holds every feature's layer cache under feature-prefixed tags: a registry
# host, optionally with a port, and an untagged repository path.
CACHE_REPOSITORY_PATTERN = re.compile(
    r"[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9][a-z0-9._-]*)+"
)
TRUSTED_CACHE_WRITER_REPOSITORY = "perplexityai/SPACE"

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
    # Copied into the dependency stage before `cargo chef cook`.
    Path("infra/space/identity/node-identities.sh"),
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
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(args),
        check=False,
        text=True,
        capture_output=capture,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        input=input_text,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            args,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result


def run_registry_command(
    args: Sequence[str], *, description: str, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a short registry call, retrying all but authentication failures.

    Exits ECR_TRANSIENT_ERROR_EXIT_STATUS once ECR_LOGIN_MAX_ATTEMPTS attempts
    have failed. Standard output is captured and never printed, since it can
    hold a registry password.
    """
    delay_seconds = ECR_LOGIN_INITIAL_DELAY_SECONDS
    for attempt in range(1, ECR_LOGIN_MAX_ATTEMPTS + 1):
        try:
            result = run(
                args,
                check=False,
                capture=True,
                input_text=input_text,
                timeout=REGISTRY_COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            result = subprocess.CompletedProcess(
                args, 1, "", f"timed out after {REGISTRY_COMMAND_TIMEOUT_SECONDS}s\n"
            )
        if result.returncode == 0:
            return result
        sys.stderr.write(result.stderr)
        if REGISTRY_AUTH_FAILURE_PATTERN.search(result.stderr):
            raise subprocess.CalledProcessError(result.returncode, args)
        if attempt == ECR_LOGIN_MAX_ATTEMPTS:
            print(
                f"{description} failed {attempt} times; exiting "
                f"{ECR_TRANSIENT_ERROR_EXIT_STATUS} to mark the failure transient.",
                file=sys.stderr,
            )
            raise SystemExit(ECR_TRANSIENT_ERROR_EXIT_STATUS)
        print(
            f"{description} failed (attempt {attempt}/{ECR_LOGIN_MAX_ATTEMPTS}); "
            f"retrying in {delay_seconds}s.",
            file=sys.stderr,
        )
        time.sleep(delay_seconds)
        delay_seconds *= 2

    raise AssertionError("unreachable")


def registry_ref_exists(ref: str) -> bool:
    """Whether a registry serves `ref`. An unreachable registry counts as a miss."""
    try:
        result = run(
            ["docker", "buildx", "imagetools", "inspect", "--raw", ref],
            check=False,
            capture=True,
            timeout=REGISTRY_LOOKUP_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        print(f"--- :warning: Looking up {ref} timed out; treating it as absent.")
        return False
    if result.returncode != 0 and "not found" not in result.stderr:
        error = result.stderr.strip().splitlines()[-1:] or ["no error output"]
        print(f"--- :warning: Couldn't look up {ref}; treating it as absent: {error[0]}")
    return result.returncode == 0


def mirrored_image_ref(name: str) -> str | None:
    """GitHub CI's GHCR mirror of an image ci/mirrored-images.txt lists, by digest.

    None unless SPACE_CI_IMAGE_MIRROR is set, as for mirror-images.sh, and for
    an unlisted image.
    """
    if not os.environ.get("SPACE_CI_IMAGE_MIRROR"):
        return None
    listing = Path(os.environ.get("SPACE_MIRRORED_IMAGES") or MIRRORED_IMAGES_LIST)
    for line in listing.read_text().splitlines():
        fields = line.split()
        if len(fields) >= 3 and fields[0] == name:
            digest, destination = fields[1], fields[2]
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise SystemExit(f"Malformed line in {listing}: {line}")
            return f"{destination.rsplit(':', 1)[0]}@{digest}"
    return None


def without_external_sources(command: Sequence[str]) -> list[str]:
    """The command without its cache imports and registry build contexts.

    The build then takes every layer from the Dockerfile's own sources. Cache
    exports stay, so a rebuild on main replaces a damaged cache; local build
    contexts, such as the Rust artifacts, stay too.
    """
    result: list[str] = []
    args = iter(command)
    for arg in args:
        if arg == "--cache-from":
            next(args, None)
        elif arg == "--build-context":
            value = next(args, "")
            if "=docker-image://" not in value:
                result.extend([arg, value])
        elif arg.startswith("--cache-from=") or (
            arg.startswith("--build-context=") and "=docker-image://" in arg
        ):
            continue
        else:
            result.append(arg)
    return result


def external_source_names(command: Sequence[str]) -> list[str]:
    """How BuildKit's errors name the registries the command's cache imports
    and registry build contexts read: each repository by reference and by its
    registry API URL. A failure naming one, such as resolving a mirrored
    base's metadata, is that copy's failure, not the build's.
    """
    names: list[str] = []
    args = list(command)
    for index, arg in enumerate(args):
        if arg in {"--cache-from", "--build-context"} and index + 1 < len(args):
            value = args[index + 1]
        elif arg.startswith(("--cache-from=", "--build-context=")):
            value = arg.split("=", 1)[1]
        else:
            continue
        if "=docker-image://" in value:
            ref = value.split("=docker-image://", 1)[1]
        else:
            refs = [part[4:] for part in value.split(",") if part.startswith("ref=")]
            if not refs:
                continue
            ref = refs[0]
        repository = ref.split("@", 1)[0]
        host, _, path = repository.partition("/")
        if ":" in path.rsplit("/", 1)[-1]:
            path = path.rsplit(":", 1)[0]
        names.extend([f"{host}/{path}", f"{host}/v2/{path}/"])
    return names


def run_buildx_command(
    args: Sequence[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    published: Callable[[], bool] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a buildx build, retrying registry rate limits and dropped pushes.

    A build that fails reading a layer while it imports a cache or takes a
    base from a registry build context (GitHub CI's mirror), or fails with an
    error that names one of those registries, runs once more without either, so a damaged or unreachable copy costs a cold build from
    the sources rather than the job. `published`, when given, says whether a
    push the build reported as failed has reached the registry anyway; a
    retry would then collide with the immutable tag it wrote. A cache export
    that failed is reported as a warning, since it does not fail the build.
    """
    command = [str(arg) for arg in args]
    delay_seconds = BUILDX_RATE_LIMIT_INITIAL_DELAY_SECONDS
    attempt = 1

    while True:
        print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
        retryable = False
        source_failed = False
        sources = external_source_names(command)
        export_vertices: set[str] = set()
        export_error = ""
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
                retryable = (
                    retryable
                    or REGISTRY_RATE_LIMIT_PATTERN.search(line) is not None
                    or TRANSIENT_PUSH_FAILURE_PATTERN.search(line) is not None
                )
                source_failed = (
                    source_failed
                    or LAYER_READ_FAILURE_PATTERN.search(line) is not None
                    or (line.startswith("ERROR:") and any(name in line for name in sources))
                )
                if match := CACHE_EXPORT_VERTEX_PATTERN.match(line):
                    export_vertices.add(match.group(1))
                elif (match := VERTEX_ERROR_PATTERN.match(line)) and match.group(1) in export_vertices:
                    export_error = match.group(2).strip()
            returncode = process.wait()

        if returncode == 0:
            if export_error:
                print(
                    f"::warning::Exporting the layer cache failed, so later runs reuse "
                    f"an older cache or build cold: {export_error}"
                )
            return subprocess.CompletedProcess(command, returncode)
        fallback = without_external_sources(command)
        if source_failed and fallback != command:
            if published is not None and published():
                print(
                    "Buildx reported a failed push, but the registry holds the image; "
                    "not pushing again.",
                    file=sys.stderr,
                )
                return subprocess.CompletedProcess(command, 0)
            print(
                "::warning::The build could not use the layer cache or the image "
                "mirror; building once more from the Dockerfile's sources without "
                "them."
            )
            command = fallback
            continue
        if not retryable:
            raise subprocess.CalledProcessError(returncode, command)
        if published is not None and published():
            print(
                "Buildx reported a failed push, but the registry holds the image; "
                "not pushing again.",
                file=sys.stderr,
            )
            return subprocess.CompletedProcess(command, 0)
        if attempt == BUILDX_RATE_LIMIT_MAX_ATTEMPTS:
            print(
                f"Buildx registry errors persisted for {attempt} attempts; exiting "
                f"{ECR_TRANSIENT_ERROR_EXIT_STATUS} to mark the failure transient.",
                file=sys.stderr,
            )
            raise SystemExit(ECR_TRANSIENT_ERROR_EXIT_STATUS)

        print(
            f"Buildx registry rate limit or transient push failure on attempt "
            f"{attempt}/{BUILDX_RATE_LIMIT_MAX_ATTEMPTS}; retrying in {delay_seconds}s.",
            file=sys.stderr,
        )
        time.sleep(delay_seconds)
        delay_seconds *= 2
        attempt += 1


def run_buildx(
    args: Sequence[str],
    *,
    push: bool,
    context: Path = REPO_ROOT,
    published: Callable[[], bool] | None = None,
) -> None:
    command = [*args, "--push" if push else "--load", str(context)]
    run_buildx_command(command, published=published)


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
        self._abandoned_builders: list[str] = []
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

    @staticmethod
    def cache_repository() -> str | None:
        """The repository SPACE_BUNDLE_CACHE_REPOSITORY names for the registry
        cache, or None to keep it in each feature's primary-target repository.
        """
        value = os.environ.get("SPACE_BUNDLE_CACHE_REPOSITORY", "")
        if not value:
            return None
        if not CACHE_REPOSITORY_PATTERN.fullmatch(value):
            raise SystemExit(
                f"Invalid SPACE_BUNDLE_CACHE_REPOSITORY value: {value!r}. "
                "Expected a lowercase registry repository without a tag or digest."
            )
        return value

    def uses_target_cache(self) -> bool:
        """Whether the registry cache lives in the publish targets' repositories."""
        return self.should_use_registry_cache() and not self.cache_repository()

    def cache_target(self) -> Target | None:
        return None if self.cache_repository() else self.primary_target()

    def cache_location(self, target: Target | None) -> str:
        return self.cache_repository() or (target.name if target else "unknown")

    @staticmethod
    def require_trusted_cache_writer() -> None:
        # Imports are optional and may come from anywhere that can read the
        # repository. Only main pushes of this repository write it; the
        # workflow, not this check, withholds the token's write permission.
        trusted = (
            os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_EVENT_NAME") == "push"
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_REPOSITORY") == TRUSTED_CACHE_WRITER_REPOSITORY
        )
        if not trusted:
            raise SystemExit(
                "Only main push CI in "
                f"{TRUSTED_CACHE_WRITER_REPOSITORY} writes SPACE_BUNDLE_CACHE_REPOSITORY; "
                "set SPACE_BUNDLE_CACHE=read-only elsewhere."
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
            export = True
        elif policy == "disabled":
            return False
        else:
            # Publishing BuildKit caches is synchronous and can add more than a
            # minute after compilation has finished. PR builds still consume the
            # main cache and use sccache for compiler outputs; main refreshes the
            # BuildKit cache without extending the PR critical path.
            export = self.is_main_branch_context() and not self.pull_request_number()
        if export and self.cache_repository():
            self.require_trusted_cache_writer()
        return export

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
        if backend == "registry" and self.cache_repository():
            backend = f"registry ({self.cache_repository()})"
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
        # A gha build, or one whose cache has its own repository, hands the
        # artifacts to the bundle build as a local context and keeps the
        # publish targets to deployable images.
        return (
            bool(self.buildx_builder_name)
            and self.cache_backend() == "registry"
            and not self.cache_repository()
        )

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

    def push_reached_registry(self, feature: str) -> bool:
        """Whether a push buildx reported as failed was stored anyway. A
        lookup that fails too counts as no, so the push is retried."""
        try:
            return self.commit_images_published(feature)
        except (subprocess.CalledProcessError, SystemExit) as exc:
            print(f"--- :warning: Couldn't check the published {feature} image: {exc}")
            return False

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

    def rust_dependency_cache_ref(self, target: Target | None, feature: str) -> str:
        return self.cache_ref(target, feature, self.rust_dependency_cache_tag(feature))

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

    def cache_ref(self, target: Target | None, feature: str, tag: str) -> str:
        """Where the registry cache keeps `tag`: in SPACE_BUNDLE_CACHE_REPOSITORY
        under the feature's prefix, or else in the target's feature repository.
        """
        repository = self.cache_repository()
        if repository:
            return f"{repository}:{feature}-{tag}"
        if target is None:
            raise SystemExit("The registry cache needs a target or SPACE_BUNDLE_CACHE_REPOSITORY.")
        return f"{target.registry_host}/{self.repository_name_for_feature(feature)}:{tag}"

    def main_cache_ref(self, target: Target | None, feature: str, kind: str) -> str:
        return self.cache_ref(target, feature, self.cache_tag_for_kind(kind))

    def pr_cache_tag_for_kind(self, kind: str) -> str | None:
        pr = self.pull_request_number()
        if not pr:
            return None
        return f"{self.cache_tag_for_kind(kind)}-pr-{pr}"

    def pr_cache_ref(self, target: Target | None, feature: str, kind: str) -> str | None:
        tag = self.pr_cache_tag_for_kind(kind)
        if not tag:
            return None
        return self.cache_ref(target, feature, tag)

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

    @staticmethod
    def buildkit_images() -> list[str]:
        """The pinned BuildKit image's references, in the order to try them:
        GitHub CI's mirror of it when it is listed, then its source."""
        mirrored = mirrored_image_ref(BUILDKIT_IMAGE)
        return [mirrored, BUILDKIT_IMAGE] if mirrored else [BUILDKIT_IMAGE]

    @staticmethod
    def remove_buildx_builder(name: str) -> bool:
        try:
            return run(
                ["docker", "buildx", "rm", "--force", name],
                check=False,
                timeout=BUILDX_RM_TIMEOUT_SECONDS,
            ).returncode == 0
        except subprocess.TimeoutExpired:
            print(f"--- :warning: Removing buildx builder {name} timed out.")
            return False

    def start_buildx_builder(self, name: str, image: str) -> bool:
        create = [
            "docker",
            "buildx",
            "create",
            "--name",
            name,
            "--driver",
            "docker-container",
            "--driver-opt",
            f"image={image}",
            "--use",
        ]
        # The docker-container driver pulls its image at bootstrap, not create.
        bootstrap = ["docker", "buildx", "inspect", name, "--bootstrap"]
        for step, command, timeout in (
            ("create", create, BUILDX_CREATE_TIMEOUT_SECONDS),
            ("bootstrap", bootstrap, BUILDX_BOOTSTRAP_TIMEOUT_SECONDS),
        ):
            try:
                failed = run(command, check=False, timeout=timeout).returncode != 0
            except subprocess.TimeoutExpired:
                failed = True
                print(f"--- :warning: {step.capitalize()} timed out after {timeout}s.")
            if failed:
                print(f"--- :warning: Failed to {step} buildx builder {name} from {image}.")
                # A create that timed out may still have registered the builder;
                # one that can't be removed now is removed again at cleanup.
                if not self.remove_buildx_builder(name):
                    self._abandoned_builders.append(name)
                return False
        return True

    def setup_buildx_builder(self) -> None:
        if not self.should_use_explicit_buildx_builder() or self.buildx_builder_name:
            return

        for image in self.buildkit_images():
            # A fresh name per image, so a builder a failed attempt couldn't
            # remove doesn't block the next one.
            name = f"space-bundle-{random.randint(1000, 9999)}{random.randint(1000, 9999)}"
            print(f"--- :docker: Creating buildx builder {name} from {image}")
            if self.start_buildx_builder(name, image):
                self.buildx_builder_name = name
                return

        print(
            "--- :warning: Couldn't start a docker-container buildx builder; falling back to the default builder."
        )
        print(
            "--- :warning: External cache is disabled for this run because the default builder may not support it."
        )
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
            self.remove_buildx_builder(self.buildx_builder_name)
            self.buildx_builder_name = ""
        for name in self._abandoned_builders:
            self.remove_buildx_builder(name)
        self._abandoned_builders.clear()
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
        self, target: Target | None, feature: str, kind: str, *, tag: str | None = None
    ) -> bool:
        tag = tag or self.cache_tag_for_kind(kind)
        if self.cache_repository():
            return registry_ref_exists(self.cache_ref(target, feature, tag))
        if target is None:
            raise SystemExit("The registry cache needs a target or SPACE_BUNDLE_CACHE_REPOSITORY.")
        return self.image_tag_exists(target, feature, tag)

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
        password = run_registry_command(
            self.aws_command("ecr", "get-login-password", "--region", AWS_REGION),
            description="Fetching the ECR login password",
        ).stdout
        for target in unseen:
            print(f"--- :docker: Logging into {target.registry_host} ({target.name})")
            run_registry_command(
                ["docker", "login", "--username", "AWS", "--password-stdin", target.registry_host],
                description=f"Logging into {target.registry_host}",
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
        try:
            result = run(
                ["bash", str(MIRROR_IMAGES_SCRIPT), "build-contexts", dockerfile],
                capture=True,
                timeout=MIRROR_LOOKUP_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            print(
                f"::warning::Looking up the mirrored bases of {dockerfile} timed out "
                f"after {MIRROR_LOOKUP_TIMEOUT_SECONDS}s; building from its FROM images."
            )
            return []
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
        primary = self.cache_target()
        location = self.cache_location(primary)
        imported: list[str] = []

        pr_tag = self.pr_cache_tag_for_kind(kind)
        if pr_tag and self.cache_tag_exists(primary, feature, kind, tag=pr_tag):
            command.extend(
                ["--cache-from", f"type=registry,ref={self.pr_cache_ref(primary, feature, kind)}"]
            )
            imported.append(f"{location} PR-{self.pull_request_number()}")

        if self.cache_tag_exists(primary, feature, kind):
            command.extend(
                ["--cache-from", f"type=registry,ref={self.main_cache_ref(primary, feature, kind)}"]
            )
            imported.append(f"{location} main")

        if not imported:
            print(
                f"--- :docker: No existing {label} cache found for {feature} at "
                f"{self.main_cache_ref(primary, feature, kind)}; skipping cache import."
            )
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
        # A cache repository holds one cache for every target.
        locations: list[Target | None] = [None] if self.cache_repository() else [*targets]
        for target in locations:
            if pr:
                cache_ref = self.pr_cache_ref(target, feature, kind)
                where = f"{self.cache_location(target)} PR-{pr}"
            else:
                cache_ref = self.main_cache_ref(target, feature, kind)
                where = f"{self.cache_location(target)} main"
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
        if self.cache_repository():
            # One rolling tag per feature, refreshed by every main push, as for
            # the image cache: a dependency change still imports the toolchain
            # layers, any input change is re-exported by the next main push,
            # and every superseded version is untagged, so pruning untagged
            # versions bounds the repository.
            self.maybe_append_cache_import(command, feature, "rust")
            self.maybe_append_cache_export(command, feature, "rust", targets=[])
            return

        primary = self.cache_target()
        cache_tag = self.rust_dependency_cache_tag(feature)
        cache_ref = self.rust_dependency_cache_ref(primary, feature)
        if self.cache_tag_exists(primary, feature, "rust", tag=cache_tag):
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

    def check_cache_written(self, command: Sequence[str], feature: str, kind: str) -> None:
        """Warn when a build that exported to the cache repository left no cache.

        Exports ignore errors so a cache problem never fails a build; this
        keeps a lasting one (no package, no write permission) from going
        unnoticed while every run stays cold.
        """
        if not self.cache_repository() or "--cache-to" not in command:
            return
        ref = self.main_cache_ref(None, feature, kind)
        if not registry_ref_exists(ref):
            print(
                f"::warning::The {kind} layer cache for {feature} was not written to "
                f"{ref}; later runs will build cold until it is."
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
        self.check_cache_written(command, feature, "rust")

    def build_space_bundle_image(self, feature: str) -> None:
        dockerfile = self.dockerfile_for_feature(feature)
        publish = self.should_publish_bundle_images()
        targets = self.resolve_targets() if publish or self.uses_target_cache() else []

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

        run_buildx(
            command,
            push=publish,
            context=self.source_root,
            published=(lambda: self.push_reached_registry(feature)) if publish else None,
        )
        self.check_cache_written(command, feature, "image")

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
        if not self.should_publish_bundle_images() and not self.uses_target_cache():
            return "No registry targets needed for this build."
        targets = self.resolve_targets()
        names = ", ".join(f"{t.name} ({t.account_id})" for t in targets)
        return f"Publish targets: {names}."

    def execute(self) -> None:
        print(f"--- :docker: {self.publish_mode_message()}")
        print(f"--- :docker: {self.release_lto_message()}")
        print(f"--- :docker: {self.cache_mode_message()}")
        print(f"--- :docker: {self.targets_message()}")
        self.wait_for_docker()
        try:
            # Inside the try, so builders a failed setup left are removed even
            # when a publishing build then exits.
            self.setup_buildx_builder()
            if self.should_publish_bundle_images() or self.uses_target_cache():
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
