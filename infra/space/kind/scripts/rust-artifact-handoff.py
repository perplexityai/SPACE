#!/usr/bin/env python3
"""Hand off one verified producer image within a GitHub Actions run.

A PR's producer either builds the image or, with `reuse`, takes the one main
CI published for the same Rust input key (`build-rust-artifacts.sh
--input-key`) from a registry package that only main writes. Main always
builds; a job of its own then `publish`es the image, so the shards never wait
for the push. Either way the shards load and verify the exact image the
producer recorded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
IMAGE = "space-rust-build-kind:local"
PRODUCER = "Build shared Kind Rust artifacts"
BUILD = ROOT / "infra/space/kind/scripts/build-rust-artifacts.sh"
# Set by build-rust-artifacts.sh on a Docker-mode image with an exact input key.
INPUT_KEY_LABEL = "space.pplx.net/kind-rust-input-key"
REVISION_LABEL = "org.opencontainers.image.revision"
# A healthy pull of the image takes well under a minute. One still running
# after this has stalled, and all of its wait is added to the build that
# then runs anyway. The log reports each pull's duration to calibrate this.
PULL_TIMEOUT_SECONDS = 90


def context() -> dict:
    repository = os.environ["GITHUB_REPOSITORY"]
    revision = os.environ["GITHUB_SHA"]
    run = os.environ["GITHUB_RUN_ID"]
    attempt = os.environ["GITHUB_RUN_ATTEMPT"]
    if (
        not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)
        or not re.fullmatch(r"[a-f0-9]{40}", revision)
        or not re.fullmatch(r"[1-9][0-9]*", run)
        or not re.fullmatch(r"[1-9][0-9]*", attempt)
    ):
        raise ValueError("invalid GitHub run identity")
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    if head != revision:
        raise ValueError("checkout does not match this run's source SHA")
    return {
        "repository": repository,
        "source_sha": revision,
        "run_id": run,
        "run_attempt": attempt,
    }


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def labels(reference: str) -> dict:
    value = json.loads(
        subprocess.check_output(
            ["docker", "image", "inspect", "-f", "{{json .Config.Labels}}", reference],
            text=True,
            timeout=60,
        )
    )
    return value if isinstance(value, dict) else {}


def input_key() -> str:
    """This checkout's Rust input key; fails when its inputs are uncommitted."""

    key = subprocess.check_output(
        ["bash", str(BUILD), "--input-key"], cwd=ROOT, text=True, timeout=120
    ).strip()
    if not re.fullmatch(r"[a-f0-9]{64}", key):
        raise ValueError("invalid Rust input key")
    return key


def image_source(identity: dict) -> tuple[str, str]:
    """(commit the local image was built from, its input key or "").

    An image without a key label was built by this producer. One with a key
    label may have been built by main from another commit.
    """

    image_labels = labels(IMAGE)
    key = image_labels.get(INPUT_KEY_LABEL, "")
    if not key:
        return identity["source_sha"], ""
    built_from = image_labels.get(REVISION_LABEL, "")
    if not re.fullmatch(r"[a-f0-9]{64}", key) or not re.fullmatch(
        r"[a-f0-9]{40}", built_from
    ):
        raise ValueError("producer image carries an invalid input key or revision")
    return built_from, key


def reuse(repository: str) -> None:
    """Take main's image for this checkout's Rust input key, if it has one.

    Never fails: any miss leaves the build step to run, as without reuse.
    """

    try:
        key = input_key()
        reference = f"{repository}:{key}"
        started = time.monotonic()
        try:
            # Never echo a remote error body or authentication diagnostics.
            pulled = subprocess.run(
                ["docker", "pull", "--quiet", reference],
                capture_output=True,
                text=True,
                timeout=PULL_TIMEOUT_SECONDS,
                check=False,
            ).returncode
        except subprocess.TimeoutExpired:
            pulled = None
        elapsed = time.monotonic() - started
        if pulled != 0:
            outcome = "timed out" if pulled is None else "failed"
            print(
                f"::notice::The pull of the Rust artifact image for input key "
                f"{key[:12]} {outcome} after {elapsed:.0f} s; building it",
                flush=True,
            )
            return
        print(f"Pulled {reference} in {elapsed:.0f} s", flush=True)
        image_labels = labels(reference)
        built_from = image_labels.get(REVISION_LABEL, "")
        if image_labels.get(INPUT_KEY_LABEL) != key or not re.fullmatch(
            r"[a-f0-9]{40}", built_from
        ):
            print(
                "::notice::The published Rust artifact image does not carry its "
                "input key; building it",
                flush=True,
            )
            return
        subprocess.run(["docker", "tag", reference, IMAGE], check=True, timeout=60)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(
            f"::notice::Rust artifact reuse stopped ({type(error).__name__}); "
            "building the image",
            flush=True,
        )
        return
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write("reused=true\n")
    print(
        f"::notice::Reusing the Rust artifact image main built from {built_from} "
        f"for input key {key[:12]}; skipping the build",
        flush=True,
    )


def publish(repository: str) -> None:
    """Publish the loaded producer image under its input key (main only).

    Main builds on every push, so this replaces the key's earlier image.
    Never fails: a later PR with the same key then builds it.
    """

    try:
        identity = context()
        built_from, key = image_source(identity)
        if not key:
            print(
                "::notice::The Rust artifact image has no input key; not publishing it",
                flush=True,
            )
            return
        if built_from != identity["source_sha"] or key != input_key():
            raise ValueError("artifact image was not built from this checkout")
        reference = f"{repository}:{key}"
        subprocess.run(["docker", "tag", IMAGE, reference], check=True, timeout=60)
        subprocess.run(
            ["docker", "push", "--quiet", reference],
            capture_output=True,
            check=True,
            timeout=600,
        )
    except (KeyError, OSError, ValueError, subprocess.SubprocessError) as error:
        print(
            f"::warning::Could not publish the Rust artifact image ({type(error).__name__})",
            flush=True,
        )
        return
    print(f"Published the Rust artifact image for input key {key}", flush=True)


def record(directory: Path) -> None:
    identity = context()
    archive = directory / "kind-rust.tar.gz"
    image = subprocess.check_output(
        ["docker", "image", "inspect", "-f", "{{.Id}}", IMAGE], text=True
    ).strip()
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", image):
        raise ValueError("producer returned an invalid Docker image identity")
    built_from, key = image_source(identity)
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                **identity,
                "image_id": image,
                "archive_sha256": digest(archive),
                "built_from": built_from,
                "input_key": key,
            },
            indent=2,
        )
        + "\n"
    )


# Each `gh api` call is bounded; the wait as a whole is bounded by its deadline.
API_CALL_SECONDS = 30
# Backoff between attempts of one lookup after a transient failure.
API_RETRY_SECONDS = (5, 10, 20, 30)
# `gh` exits 4 when it has no authentication to send; none appears later.
GH_AUTH_EXIT = 4
# The client errors worth retrying: a request timeout, a rate limit, and a 404,
# which a run's newly created attempt may answer before its jobs are listed.
RETRIED_CLIENT_STATUSES = ("404", "408", "429")


def api_pages(path: str, what: str, deadline: float) -> list:
    """The slurped pages of a `gh api` GET, retried through transient failures.

    What cannot heal fails at once: `gh` without authentication, a refused
    token (HTTP 401, or a 403 that is not a rate limit) and any other client
    error but a request timeout, a rate limit or a 404. GitHub reports rate
    limits as 403 or 429. Everything else (a 5xx, a rate limit, a 404, a
    dropped connection or a call that outlives its timeout) is retried with
    backoff until the caller's deadline: a shard that gives up while the
    producer is still building fails the run for an API outage it could have
    waited out. Each call's timeout is capped at the time left, so the wait
    ends at its deadline. Only the HTTP status is ever printed, never a remote
    error body or authentication diagnostics.
    """

    attempt = 0
    while True:
        call_seconds = max(1, min(API_CALL_SECONDS, int(deadline - time.monotonic())))
        try:
            result = subprocess.run(
                ["gh", "api", "--paginate", "--slurp", path],
                capture_output=True,
                text=True,
                timeout=call_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            failure = f"no answer within {call_seconds} s"
        else:
            if not result.returncode:
                return json.loads(result.stdout)
            if result.returncode == GH_AUTH_EXIT:
                raise RuntimeError(
                    f"cannot read {what}: gh has no authentication; Actions read access is required"
                )
            match = re.search(r"HTTP ([0-9]{3})", result.stderr)
            status = match.group(1) if match else None
            rate_limited = status in ("403", "429") and "rate limit" in result.stderr.lower()
            if status in ("401", "403") and not rate_limited:
                raise RuntimeError(
                    f"cannot read {what} (HTTP {status}); Actions read access is required"
                )
            if (
                status is not None
                and status.startswith("4")
                and status not in RETRIED_CLIENT_STATUSES
                and not rate_limited
            ):
                raise RuntimeError(f"cannot read {what} (HTTP {status})")
            if status is None:
                failure = "a transport failure"
            else:
                failure = f"HTTP {status}" + (" (rate limit)" if rate_limited else "")
        pause = API_RETRY_SECONDS[min(attempt, len(API_RETRY_SECONDS) - 1)]
        if time.monotonic() + pause >= deadline:
            raise RuntimeError(f"cannot read {what} before the wait's deadline: {failure}")
        attempt += 1
        print(f"Retrying the {what} lookup in {pause} s after {failure}", flush=True)
        time.sleep(pause)


def artifact_attempt(prefix: str, revision: str, current: int, deadline: float) -> int:
    """The newest attempt in this run that uploaded the producer image."""

    for attempt in range(current, 0, -1):
        name = f"kind-rust-{revision}-{attempt}"
        pages = api_pages(
            f"{prefix}/artifacts?name={name}&per_page=100",
            "Rust producer artifacts",
            deadline,
        )
        if any(
            artifact["name"] == name and not artifact["expired"]
            for page in pages
            for artifact in page["artifacts"]
        ):
            return attempt
    raise RuntimeError(
        "Rust producer succeeded but no attempt in this run uploaded its image"
    )


def wait(timeout: int) -> None:
    identity = context()
    prefix = f"repos/{identity['repository']}/actions/runs/{identity['run_id']}"
    deadline = time.monotonic() + timeout
    print(
        "Waiting for this attempt's Rust producer after cluster preparation", flush=True
    )
    while time.monotonic() < deadline:
        # A failed-jobs-only rerun does not rerun a successful producer. Select
        # the nearest producer in this run, never a different run or commit;
        # load still checks the source SHA and exact selected attempt.
        jobs = []
        current_attempt = int(identity["run_attempt"])
        for attempt in range(current_attempt, 0, -1):
            jobs = [
                job
                for page in api_pages(
                    f"{prefix}/attempts/{attempt}/jobs?per_page=100",
                    "Rust producer status",
                    deadline,
                )
                for job in page["jobs"]
                if job["name"] == PRODUCER or job["name"].endswith(" / " + PRODUCER)
            ]
            if jobs:
                break
        if len(jobs) > 1:
            raise ValueError("multiple Rust producers in this run attempt")
        if jobs and jobs[0]["status"] == "completed":
            if jobs[0]["conclusion"] != "success":
                raise RuntimeError(
                    "Rust producer did not succeed; refusing local compilation fallback"
                )
            # A failed-jobs-only rerun carries the successful producer into
            # the new attempt and reports it with the new `run_attempt`, but
            # its upload stays named for the attempt that actually ran. The
            # upload is the handoff, so select the attempt from it.
            producer_attempt = artifact_attempt(
                prefix, identity["source_sha"], current_attempt, deadline
            )
            with Path(os.environ["GITHUB_ENV"]).open("a") as output:
                output.write(f"SPACE_KIND_RUST_PRODUCER_ATTEMPT={producer_attempt}\n")
            print(
                f"Rust producer passed for run attempt {producer_attempt}", flush=True
            )
            return
        time.sleep(min(10, max(0, deadline - time.monotonic())))
    raise TimeoutError("Rust producer did not finish before the handoff deadline")


def load(directory: Path) -> None:
    identity = context()
    producer = os.environ.get(
        "SPACE_KIND_RUST_PRODUCER_ATTEMPT", identity["run_attempt"]
    )
    if not producer.isdecimal() or not 1 <= int(producer) <= int(
        identity["run_attempt"]
    ):
        raise ValueError("invalid producer attempt")
    identity["run_attempt"] = producer
    archive = directory / "kind-rust.tar.gz"
    manifest_path = directory / "manifest.json"
    if any(
        path.is_symlink() or not path.is_file() for path in (archive, manifest_path)
    ):
        raise ValueError("handoff requires regular archive and manifest files")
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, dict):
        raise TypeError("artifact manifest must be an object")
    if (
        any(manifest.get(key) != value for key, value in identity.items())
        or not isinstance(manifest.get("image_id"), str)
        or not re.fullmatch(r"sha256:[a-f0-9]{64}", manifest["image_id"])
        or manifest.get("archive_sha256") != digest(archive)
    ):
        raise ValueError(
            "artifact does not match this run attempt, source, or archive digest"
        )
    built_from, key = manifest.get("built_from"), manifest.get("input_key")
    if (
        not isinstance(built_from, str)
        or not re.fullmatch(r"[a-f0-9]{40}", built_from)
        or not isinstance(key, str)
        or not re.fullmatch(r"(?:[a-f0-9]{64})?", key)
    ):
        raise ValueError("artifact manifest names an invalid image source")
    subprocess.run(
        ["docker", "image", "load", "--input", str(archive)],
        check=True,
        timeout=300,
    )
    actual = subprocess.check_output(
        ["docker", "image", "inspect", "-f", "{{.Id}}", IMAGE], text=True
    ).strip()
    if actual != manifest["image_id"]:
        raise ValueError(
            "loaded Rust image does not match the producer's image identity"
        )
    environment = {
        **os.environ,
        "SPACE_KIND_RUST_ARTIFACTS": "prebuilt",
        "SPACE_KIND_PREBUILT_RUST_IMAGE_ID": actual,
        # The commit the image was built from; when it is not this checkout's,
        # prebuilt mode requires the input key to match instead.
        "SPACE_KIND_PREBUILT_RUST_SHA": built_from,
        "SPACE_KIND_PREBUILT_RUST_KEY": key,
    }
    subprocess.run(
        ["bash", str(BUILD)],
        cwd=ROOT,
        env=environment,
        check=True,
    )
    with Path(os.environ["GITHUB_ENV"]).open("a") as output:
        output.writelines(
            f"{key}={environment[key]}\n"
            for key in (
                "SPACE_KIND_PREBUILT_RUST_IMAGE_ID",
                "SPACE_KIND_PREBUILT_RUST_SHA",
                "SPACE_KIND_PREBUILT_RUST_KEY",
            )
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("record", "load"):
        commands.add_parser(name).add_argument("directory", type=Path)
    for name in ("reuse", "publish"):
        commands.add_parser(name).add_argument("repository")
    commands.add_parser("wait").add_argument("--timeout", type=int, default=65 * 60)
    args = parser.parse_args()
    if args.command == "wait":
        if args.timeout <= 0:
            parser.error("timeout must be positive")
        wait(args.timeout)
    elif args.command == "record":
        record(args.directory)
    elif args.command == "reuse":
        reuse(args.repository)
    elif args.command == "publish":
        publish(args.repository)
    else:
        load(args.directory)


if __name__ == "__main__":
    try:
        main()
    except (
        KeyError,
        TypeError,
        ValueError,
        RuntimeError,
        OSError,
        subprocess.SubprocessError,
    ) as error:
        raise SystemExit(f"Rust artifact handoff stopped: {error}") from None
