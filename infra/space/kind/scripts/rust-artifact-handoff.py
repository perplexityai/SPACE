#!/usr/bin/env python3
"""Hand off one verified producer image within a GitHub Actions run.

A PR's producer either builds the image or, with `reuse`, takes the one main
CI published for the same Rust input key (`build-rust-artifacts.sh
--input-key`) from a registry package that only main writes. Main always
builds; a job of its own then `publish`es the image, so the shards never wait
for the push, and records the digest it pushed. Either way the shards load and
verify the exact image the producer recorded.

Where the package cannot be read directly, `reuse` can pull main's image by
digest through a pull-through mirror of ghcr.io that serves content by digest
(SPACE_KIND_REGISTRY_MIRROR), finding the digest for its key in main's digest
records.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import time
import zipfile
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
# How often `started` reads the producer's status while it waits for a runner.
STARTED_POLL_SECONDS = 3
# A host and path prefix under which a pull-through mirror of ghcr.io serves
# ghcr.io/<owner>/<repo> as <mirror>/<owner>/<repo>, by digest only.
MIRROR_ENV = "SPACE_KIND_REGISTRY_MIRROR"
# A JSON list of {"key", "digest"} records to read instead of main's (tests,
# offline use).
DIGESTS_FILE_ENV = "SPACE_KIND_RUST_DIGESTS_FILE"
# Main CI's digest record of each image it publishes: one file in an Actions
# artifact of this name, uploaded by main push runs of this workflow.
DIGEST_ARTIFACT = "kind-rust-artifact-digest"
DIGEST_SOURCE = "perplexityai/SPACE"
DIGEST_WORKFLOW = "main-ci.yml"
# The registry keeps the twenty newest images; older records name pruned ones.
DIGEST_CANDIDATES = 20
DIGEST_LOOKUP_SECONDS = 60


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


def labels(reference: str, quiet: bool = False) -> dict:
    value = json.loads(
        subprocess.check_output(
            ["docker", "image", "inspect", "-f", "{{json .Config.Labels}}", reference],
            text=True,
            timeout=60,
            stderr=subprocess.DEVNULL if quiet else None,
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


def pull(reference: str) -> tuple[int | None, float]:
    """`docker pull`'s exit status (None when it stalled) and its duration."""

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
    return pulled, time.monotonic() - started


def main_build_commit(reference: str, key: str, quiet: bool = False) -> str | None:
    """The commit a pulled image was built from, if it carries this input key."""

    image_labels = labels(reference, quiet)
    built_from = image_labels.get(REVISION_LABEL, "")
    if image_labels.get(INPUT_KEY_LABEL) != key or not re.fullmatch(
        r"[a-f0-9]{40}", built_from
    ):
        return None
    return built_from


class Miss(Exception):
    """Why the mirror cannot serve this key's image; reuse goes on without it."""


def gh_output(path: str, deadline: float) -> bytes:
    """One `gh api` GET within the deadline; never prints its error output."""

    seconds = int(deadline - time.monotonic())
    if seconds < 1:
        raise Miss("the GitHub API lookup ran out of time")
    try:
        result = subprocess.run(
            ["gh", "api", path], capture_output=True, timeout=seconds, check=False
        )
    except subprocess.TimeoutExpired:
        raise Miss("the GitHub API did not answer in time") from None
    if result.returncode:
        match = re.search(rb"HTTP ([0-9]{3})", result.stderr)
        raise Miss(
            "the GitHub API refused the lookup"
            + (f" (HTTP {match.group(1).decode()})" if match else "")
        )
    return result.stdout


def published_records(key: str) -> list:
    """Main's digest records, those likeliest to hold this key first.

    Each record is a downloaded Actions artifact, so only the newest
    DIGEST_CANDIDATES records of main push runs are read, records from this
    checkout's own history first: its key is most often its main base's. Only
    main push runs count, so a pull request's upload under the same name never
    does; a record exists only where publish pushed a verified image.
    """

    deadline = time.monotonic() + DIGEST_LOOKUP_SECONDS
    prefix = f"repos/{DIGEST_SOURCE}/actions"
    artifacts = json.loads(
        gh_output(f"{prefix}/artifacts?name={DIGEST_ARTIFACT}&per_page=100", deadline)
    )["artifacts"]
    runs = json.loads(
        gh_output(
            f"{prefix}/workflows/{DIGEST_WORKFLOW}/runs"
            "?branch=main&event=push&per_page=100",
            deadline,
        )
    )["workflow_runs"]
    main_runs = {run["id"] for run in runs}
    candidates = [
        artifact
        for artifact in artifacts
        if artifact["name"] == DIGEST_ARTIFACT
        and not artifact["expired"]
        and artifact["workflow_run"]["id"] in main_runs
    ]
    if not candidates:
        raise Miss(f"no main push run has a {DIGEST_ARTIFACT} artifact yet")
    candidates.sort(key=lambda artifact: artifact["created_at"], reverse=True)
    candidates = candidates[:DIGEST_CANDIDATES]
    # One bounded walk of local history: it reads only commits HEAD reaches,
    # so a partial or shallow clone never fetches. Ordering only saves
    # downloads, so a walk that fails keeps the newest-first order.
    try:
        walk = subprocess.run(
            ["git", "rev-list", "--max-count=2000", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=max(1, int(deadline - time.monotonic())),
            check=False,
        )
        history = set(walk.stdout.split()) if not walk.returncode else set()
    except (OSError, subprocess.SubprocessError):
        history = set()
    candidates.sort(key=lambda artifact: artifact["workflow_run"].get("head_sha") not in history)
    records = []
    for artifact in candidates:
        archive = zipfile.ZipFile(
            io.BytesIO(gh_output(f"{prefix}/artifacts/{artifact['id']}/zip", deadline))
        )
        for name in archive.namelist():
            record = json.loads(archive.read(name))
            records.append(record)
            if isinstance(record, dict) and record.get("key") == key:
                return records
    return records


def mirror_digest(repository: str, key: str) -> tuple[str, str | None]:
    """The digest main published for this key, and the commit its record names
    if it names one, from a records file or main's runs."""

    path = os.environ.get(DIGESTS_FILE_ENV)
    if path:
        records = json.loads(Path(path).read_text())
        where = DIGESTS_FILE_ENV
    else:
        records = published_records(key)
        where = f"main's {DIGEST_ARTIFACT} artifacts"
    if isinstance(records, dict):
        records = [records]
    if not isinstance(records, list):
        raise Miss(f"{where} is not a list of digest records")
    for record in records:
        if (
            isinstance(record, dict)
            and record.get("key") == key
            and record.get("repository", repository) == repository
        ):
            found = record.get("digest")
            commit = record.get("commit")
            if (
                not isinstance(found, str)
                or not re.fullmatch(r"sha256:[a-f0-9]{64}", found)
                or not (commit is None or isinstance(commit, str))
            ):
                raise Miss(f"{where} names an invalid digest for input key {key[:12]}")
            return found, commit
    raise Miss(f"{where} has no digest for input key {key[:12]}")


def reuse_through_mirror(repository: str, key: str) -> tuple[str, str] | None:
    """Pull main's image for this key by digest through the mirror and tag it.

    Returns (commit it was built from, its digest reference under the
    registry's own name). Logs neither the mirror's address nor a remote
    error, so the log is safe to share.
    """

    mirror = os.environ.get(MIRROR_ENV, "").strip().rstrip("/")
    if not mirror:
        return None
    try:
        if not re.fullmatch(r"[A-Za-z0-9.-]+(?::[0-9]+)?(?:/[A-Za-z0-9._-]+)*", mirror):
            raise Miss(f"{MIRROR_ENV} is not a registry host and path")
        if not repository.startswith("ghcr.io/"):
            raise Miss("the mirror serves ghcr.io repositories only")
        found, commit = mirror_digest(repository, key)
        upstream = f"{repository}@{found}"
        reference = f"{mirror}/{repository.removeprefix('ghcr.io/')}@{found}"
        pulled, elapsed = pull(reference)
        if pulled != 0:
            raise Miss(
                f"the pull of {upstream} through it "
                f"{'timed out' if pulled is None else 'failed'} after {elapsed:.0f} s"
            )
        print(f"Pulled {upstream} through the registry mirror in {elapsed:.0f} s", flush=True)
        # Quiet: Docker's errors would name the mirror.
        built_from = main_build_commit(reference, key, quiet=True)
        if built_from is None or commit not in (None, built_from):
            raise Miss("the image it served does not carry its input key and commit")
        subprocess.run(
            ["docker", "tag", reference, IMAGE], capture_output=True, check=True, timeout=60
        )
    except (
        Miss,
        KeyError,
        TypeError,
        ValueError,
        OSError,
        zipfile.BadZipFile,
        subprocess.SubprocessError,
    ) as error:
        reason = error if isinstance(error, Miss) else f"the lookup stopped ({type(error).__name__})"
        print(f"Not reusing the Rust artifact image through a registry mirror: {reason}", flush=True)
        return None
    return built_from, upstream


def reuse(repository: str) -> None:
    """Take main's image for this checkout's Rust input key, if it has one.

    With SPACE_KIND_REGISTRY_MIRROR set, first by digest through that mirror,
    then by tag from the repository itself. Never fails: any miss leaves the
    build step to run, as without reuse.
    """

    try:
        key = input_key()
        mirrored = reuse_through_mirror(repository, key)
        if mirrored:
            built_from, reference = mirrored
            via = "mirror"
        else:
            via = "registry"
            reference = f"{repository}:{key}"
            pulled, elapsed = pull(reference)
            if pulled != 0:
                outcome = "timed out" if pulled is None else "failed"
                print(
                    f"::notice::The pull of the Rust artifact image for input key "
                    f"{key[:12]} {outcome} after {elapsed:.0f} s; building it",
                    flush=True,
                )
                return
            print(f"Pulled {reference} in {elapsed:.0f} s", flush=True)
            built_from = main_build_commit(reference, key)
            if built_from is None:
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
        output.write(f"reused=true\nreused_from={via}\nreused_image={reference}\n")
    print(
        f"::notice::Reusing the Rust artifact image main built from {built_from} "
        f"for input key {key[:12]}; skipping the build",
        flush=True,
    )


def pushed_digest(output: str, tag: str) -> str:
    """The digest `docker push` reports for the tag it pushed.

    Only the push's own report: the local image's RepoDigests can name an
    index of which the push sent one platform's manifest.
    """

    reported = re.findall(
        rf"^{re.escape(tag)}: digest: (sha256:[a-f0-9]{{64}}) ", output, re.MULTILINE
    )
    if not reported:
        raise ValueError("the push reported no digest")
    return reported[-1]


def publish(repository: str, record_path: Path | None) -> None:
    """Publish the loaded producer image under its input key (main only).

    Main builds on every push, so this replaces the key's earlier image. With
    `--record`, also writes {key, digest, repository, commit} for the digest it
    pushed, for readers that can only pull by digest. Never fails: a later PR
    with the same key then builds it, and a missing record costs a box the same.
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
        pushed = subprocess.run(
            ["docker", "push", reference],
            capture_output=True,
            text=True,
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
    if record_path is None:
        return
    try:
        found = pushed_digest(pushed.stdout, key)
        record_path.parent.mkdir(parents=True, exist_ok=True)
        partial = record_path.with_name(record_path.name + ".tmp")
        partial.write_text(
            json.dumps(
                {
                    "key": key,
                    "digest": found,
                    "repository": repository,
                    "commit": built_from,
                }
            )
            + "\n"
        )
        partial.replace(record_path)
    except (OSError, ValueError) as error:
        print(
            f"::warning::Could not record the published image's digest ({type(error).__name__})",
            flush=True,
        )
        return
    print(f"Recorded its digest {found}", flush=True)


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


def started(timeout: int) -> None:
    """Return once this attempt's Rust producer has a runner.

    The shards wait on the producer's image anyway, about two minutes after
    their own preparation, so Kind CI requests their runners only after the
    producer has one: asked for together, the shards took the large
    runners already up and the producer, the one job the run waits on, waited
    about a minute for a new one. This never fails the run. When the producer
    does not start within `timeout`, or its status cannot be read, it says so
    and returns, and the shards are requested anyway: with no large runner
    up, the producer boots a new one regardless, and the shards should not
    wait for that too.
    """

    deadline = time.monotonic() + timeout
    try:
        identity = context()
        prefix = f"repos/{identity['repository']}/actions/runs/{identity['run_id']}"
        while time.monotonic() < deadline:
            jobs = [
                job
                for page in api_pages(
                    f"{prefix}/attempts/{identity['run_attempt']}/jobs?per_page=100",
                    "Rust producer status",
                    deadline,
                )
                for job in page["jobs"]
                if job["name"] == PRODUCER or job["name"].endswith(" / " + PRODUCER)
            ]
            if any(job["status"] in ("in_progress", "completed") for job in jobs):
                print("The Rust producer has a runner; requesting the shards' runners", flush=True)
                return
            time.sleep(min(STARTED_POLL_SECONDS, max(0, deadline - time.monotonic())))
    except (RuntimeError, ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(f"::warning::Could not read the Rust producer's status ({error}); requesting the shards' runners now")
        return
    print(f"::warning::The Rust producer did not start within {timeout} s; requesting the shards' runners now")


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
    commands.add_parser("reuse").add_argument("repository")
    publish_parser = commands.add_parser("publish")
    publish_parser.add_argument("repository")
    publish_parser.add_argument("--record", type=Path, help="write the pushed digest's record here")
    commands.add_parser("wait").add_argument("--timeout", type=int, default=65 * 60)
    commands.add_parser("started").add_argument("--timeout", type=int, default=75)
    args = parser.parse_args()
    if args.command in ("wait", "started") and args.timeout <= 0:
        parser.error("timeout must be positive")
    if args.command == "wait":
        wait(args.timeout)
    elif args.command == "started":
        started(args.timeout)
    elif args.command == "record":
        record(args.directory)
    elif args.command == "reuse":
        reuse(args.repository)
    elif args.command == "publish":
        publish(args.repository, args.record)
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
