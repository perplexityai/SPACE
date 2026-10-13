#!/usr/bin/env python3
"""Hand off one verified producer image within a GitHub Actions run."""

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


def record(directory: Path) -> None:
    identity = context()
    archive = directory / "kind-rust.tar.gz"
    image = subprocess.check_output(
        ["docker", "image", "inspect", "-f", "{{.Id}}", IMAGE], text=True
    ).strip()
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", image):
        raise ValueError("producer returned an invalid Docker image identity")
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                **identity,
                "image_id": image,
                "archive_sha256": digest(archive),
            },
            indent=2,
        )
        + "\n"
    )


def wait(timeout: int) -> None:
    identity = context()
    prefix = f"repos/{identity['repository']}/actions/runs/{identity['run_id']}"
    deadline = time.monotonic() + timeout
    read_failures = 0
    print(
        "Waiting for this attempt's Rust producer after cluster preparation", flush=True
    )
    while time.monotonic() < deadline:
        # A failed-jobs-only rerun does not rerun a successful producer. Select
        # the nearest producer in this run, never a different run or commit;
        # load still checks the source SHA and exact selected attempt.
        jobs = []
        retry_lookup = False
        current_attempt = int(identity["run_attempt"])
        for attempt in range(current_attempt, 0, -1):
            result = subprocess.run(
                [
                    "gh",
                    "api",
                    "--paginate",
                    "--slurp",
                    f"{prefix}/attempts/{attempt}/jobs?per_page=100",
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if result.returncode:
                # Never echo a remote error body or authentication diagnostics.
                read_failures += 1
                if read_failures >= 3 or any(
                    status in result.stderr for status in ("HTTP 401", "HTTP 403")
                ):
                    raise RuntimeError(
                        "cannot read Rust producer status; Actions read access is required"
                    )
                print(
                    "Retrying producer status lookup after a transport failure",
                    flush=True,
                )
                time.sleep(min(5, max(0, deadline - time.monotonic())))
                retry_lookup = True
                break
            jobs = [
                job
                for page in json.loads(result.stdout)
                for job in page["jobs"]
                if job["name"] == PRODUCER or job["name"].endswith(" / " + PRODUCER)
            ]
            if jobs:
                break
        if retry_lookup:
            continue
        read_failures = 0
        if len(jobs) > 1:
            raise ValueError("multiple Rust producers in this run attempt")
        if jobs and jobs[0]["status"] == "completed":
            if jobs[0]["conclusion"] != "success":
                raise RuntimeError(
                    "Rust producer did not succeed; refusing local compilation fallback"
                )
            # Some rerun responses retain successful jobs from an older
            # attempt. Use the job's actual producer attempt, not the query's.
            producer_attempt = jobs[0]["run_attempt"]
            if (
                not isinstance(producer_attempt, int)
                or not 1 <= producer_attempt <= attempt
            ):
                raise ValueError("invalid producer job attempt")
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
        "SPACE_KIND_PREBUILT_RUST_SHA": identity["source_sha"],
    }
    subprocess.run(
        ["bash", "infra/space/kind/scripts/build-rust-artifacts.sh"],
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
            )
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("record", "load"):
        commands.add_parser(name).add_argument("directory", type=Path)
    commands.add_parser("wait").add_argument("--timeout", type=int, default=65 * 60)
    args = parser.parse_args()
    if args.command == "wait":
        if args.timeout <= 0:
            parser.error("timeout must be positive")
        wait(args.timeout)
    elif args.command == "record":
        record(args.directory)
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
