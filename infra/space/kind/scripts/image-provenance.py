#!/usr/bin/env python3
"""Stamp each Kind image with what it was built from, and check what runs.

`build` wraps one image build, as the Tiltfile's custom_builds run it. The
build identity it gives the image is the checkout's commit
(`telemetry_config.checkout_commit`, Kind's one version source) when the
image is what that commit builds, and
`<commit>-dirty-<content>` when it is not, where <content> is the first 12
hex digits of the image's config digest as built with the plain commit.

The identity reaches the image the way the commit always did: the
`SPACE_GIT_SHA` build argument, which the Dockerfiles write to
`/etc/space/build-git-sha` (read by `pplx_space_observability::git_sha`, so
the startup banner, the `git_sha` metric tags and the net-gateway's reported
build) and to the `org.opencontainers.image.revision` label. Both sit in each
Dockerfile's final stage, after its package and dependency layers, so a
changed identity never reruns a compile or a package install. Only an image
whose Dockerfile takes no such argument (`--revision-label`: the UI and
seed-publish) gets the label from this script; every other image's label
comes from the argument, so the check below also proves the argument reached
the build.

An image is what the commit builds when the worktree was clean and unchanged
for the whole build, or when its content equals that of such a build at the
same commit, which every one records under `<state>/provenance/clean/`. So a
dirty worktree changes the identity only of the images its changes actually
reach: one edited service's image reports `-dirty-`, the others keep the
commit. The content is the config digest, which also holds each layer's
creation time, so this holds only while BuildKit's cache still has the clean
build's layers; a layer rebuilt after the cache dropped it makes the image
`-dirty-` until a clean build records it again. A clean build runs exactly
the command it is given, once, with the commit as its identity, so CI and
release-like runs are unchanged. A dirty one whose content differs builds a
second time with its dirty identity; every layer up to the identity is cached
by then.

"Clean and unchanged" is judged from two snapshots of the build's inputs
(`snapshot`): one taken before the chain's first step, which the caller
passes as `--since` because the chain compiles Rust and assembles the seed
before this script runs (without it, the one this script takes when it
starts), and one after the docker build. Each holds HEAD,
`git status`, and the stat data of every tracked and untracked file, so an
edit reverted during the build still counts. The Rust artifact image
(`space-rust-build-kind:local`) and the seed directory are shared by every
checkout on the host, so their writers (build-rust-artifacts.sh,
build-seed.sh) mark each run (lib/shared-build-inputs.sh), and the snapshots
also hold the other checkouts' marks: a writer from another checkout that ran
during the build, or was running when it started, may have replaced what the
image copied, and the build is not certified clean.

Each build for a cluster writes its record, the image's ID, config digest and
identity, to `<state>/provenance/<cluster>/<image>.json`; the last build of
each image wins. A build without `--cluster` is recorded for no cluster,
though a clean one still adds its content to the clean set. Nothing prunes `clean/`, the records or the bring-up's `runs/`;
each entry is a few kilobytes and can be deleted at any time.

`check` compares every SPACE container in the cluster's `space` namespace with
those records: the image ID the kubelet reports it running (the config
digest), and the revision label of that image in its node's containerd store
(`crictl inspecti`, as the superseded-image prune reads labels). Pods being
deleted, and Failed pods (evicted or rejected, which their ReplicaSet keeps
but does not count), run nothing and are listed instead. It writes the
comparison to the given JSON file and exits 1, naming each stale workload and
the remedy, when any container runs something other than the latest build of
its image.

Usage:
  image-provenance.py snapshot
  image-provenance.py build [--cluster <name>] --image <name> [--since <snapshot>]
                            [--revision-label] -- <docker build command>...
  image-provenance.py check --cluster <name> --output <file>
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))
from telemetry_config import checkout_commit  # noqa: E402

REPO_ROOT = SCRIPTS.parents[3]
IMAGES_LIB = SCRIPTS.parent / "lib/space-images.sh"
STATE_DIR = Path(
    os.environ.get("SPACE_TILT_STATE_DIR") or Path.home() / ".space-devbox/kind"
)
PROVENANCE_DIR = STATE_DIR / "provenance"
# One mark per checkout that ran a writer of the shared build inputs; see
# lib/shared-build-inputs.sh, which names each by the same hash.
WRITERS_DIR = PROVENANCE_DIR / "writers"
REVISION_LABEL = "org.opencontainers.image.revision"
NAMESPACE = "space"


def log(message: str) -> None:
    print(f"[image-provenance] {message}", file=sys.stderr, flush=True)


def run(args: list[str], timeout: int = 60) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True, timeout=timeout
    ).stdout


def write_json(path: Path, value) -> None:
    """Replace `path` by rename, so a reader never sees half a record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(handle.name, path)


def hex_digest(value: str) -> str:
    """The hex of `sha256:<hex>`, `<repo>@sha256:<hex>` or a bare hex string."""
    return value.rsplit("@", 1)[-1].removeprefix("sha256:")


# ── build ─────────────────────────────────────────────────────────────────────


def git(*args: str) -> str:
    # --no-optional-locks: `status` would otherwise refresh the index and take
    # index.lock, failing a concurrent `git add` or `git commit` of the user's.
    return run(["git", "--no-optional-locks", "-C", str(REPO_ROOT), *args])


def worktree_status() -> str:
    """`git status --porcelain`: empty when the worktree matches HEAD.

    Ignored files (build output such as `target/`) do not count; untracked
    ones do, because a build context or the Rust source sync copies them.
    """
    return git("status", "--porcelain")


def writer_marks() -> tuple[str, bool]:
    """(digest of the other checkouts' writer marks, whether one is running).

    A writer touches its mark when it starts, after taking a shared lock on
    it that it holds until it exits, so a mark that is locked belongs to a
    writer still running.
    """
    ours = hashlib.sha256(str(REPO_ROOT).encode()).hexdigest()[:16]
    digest, running = hashlib.sha256(), False
    for mark in sorted(WRITERS_DIR.glob("*")):
        if mark.name == ours:
            continue
        try:
            with mark.open() as handle:
                digest.update(f"{mark.name} {os.fstat(handle.fileno()).st_mtime_ns}\n".encode())
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    running = True
        except OSError:
            continue
    return digest.hexdigest()[:16], running


def snapshot() -> str:
    """The identity of what a build reads, as one token.

    HEAD; the worktree's status and the stat data of every tracked and
    untracked file, which change when a file is edited even if it is edited
    back; the other checkouts' writer marks; and whether the worktree is clean
    and no other checkout's writer is running. Two equal snapshots that end in
    `clean:idle` bracket a build whose inputs were this commit's throughout.
    """
    head = git("rev-parse", "HEAD").strip()
    status = worktree_status()
    files = hashlib.sha256(status.encode())
    for path in git("ls-files", "-z", "--cached", "--others", "--exclude-standard").split("\0"):
        if not path:
            continue
        try:
            st = os.lstat(REPO_ROOT / path)
            files.update(f"{path} {st.st_mode} {st.st_size} {st.st_mtime_ns} {st.st_ctime_ns}\n".encode())
        except FileNotFoundError:
            files.update(f"{path} -\n".encode())
    writers, running = writer_marks()
    return ":".join([
        head, files.hexdigest()[:16], writers,
        "dirty" if status.strip() else "clean",
        "busy" if running else "idle",
    ])


def tag_of(command: list[str]) -> str:
    for index, arg in enumerate(command):
        if arg in ("-t", "--tag") and index + 1 < len(command):
            return command[index + 1]
        if arg.startswith("--tag="):
            return arg.removeprefix("--tag=")
    raise SystemExit("image-provenance: the build command names no -t/--tag")


def image_digests(ref: str) -> tuple[str, str]:
    """(image ID, config digest) of a local image.

    The image ID is what Docker and Tilt name the image by: on Docker's
    containerd store its manifest digest, on the graph-driver store its config
    digest. The config digest is what the kubelet reports a container running,
    and it covers the image's layers and settings, so it is the content
    measure. An index, which BuildKit's default attestations produce, carries
    neither for one platform; the callers build with --provenance=false.
    """
    image = json.loads(run(["docker", "image", "inspect", ref]))[0]
    descriptor = image.get("Descriptor") or {}
    if descriptor.get("mediaType", "").endswith("index.v1+json"):
        raise SystemExit(
            f"image-provenance: {ref} is an image index; build it with --provenance=false"
        )
    config = (descriptor.get("annotations") or {}).get("config.digest") or image["Id"]
    return image["Id"], config


def docker_build(command: list[str], identity: str, revision_label: bool) -> None:
    env = dict(os.environ, SPACE_GIT_SHA=identity)
    label = ["--label", f"{REVISION_LABEL}={identity}"] if revision_label else []
    status = subprocess.run(command + label, env=env).returncode
    if status:
        raise SystemExit(status)


def read_clean_builds(path: Path) -> set[str]:
    try:
        return set(json.loads(path.read_text()))
    except (OSError, ValueError):
        return set()


def add_clean_build(path: Path, content: str) -> None:
    """Add `content` to the clean set, under a lock: Tilt runs up to eight
    builds at once, and an unlocked read-modify-write could drop an entry."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with (path.parent / ".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        known = read_clean_builds(path)
        if content not in known:
            write_json(path, sorted(known | {content}))


def why_not_clean(before: str, after: str) -> str:
    b, a = before.split(":"), after.split(":")
    if b[3] == "dirty":
        return "built from a worktree that differs from HEAD"
    if b[0] != a[0]:
        return "HEAD moved during the build"
    if b[1] != a[1]:
        return "the worktree changed during the build"
    return "another checkout wrote the shared Rust artifact image or seed during the build"


def build(args: argparse.Namespace) -> int:
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("image-provenance: no build command given after --")
    ref = tag_of(command)
    commit = checkout_commit() or "unknown"
    if commit == "unknown":
        # Outside Git there is no commit to compare with; the image reports
        # "unknown", as before.
        docker_build(command, commit, args.revision_label)
        image_id, config = image_digests(ref)
        content, identity, clean = config, commit, False
    else:
        if args.since == "":
            raise SystemExit("image-provenance: --since is empty; the chain's snapshot step did not run")
        before = args.since or snapshot()
        docker_build(command, commit, args.revision_label)
        image_id, content = image_digests(ref)
        config = content
        after = snapshot()
        clean = before == after and before.endswith(":clean:idle")
        clean_builds = PROVENANCE_DIR / "clean" / commit / f"{args.image}.json"
        if clean:
            identity = commit
            add_clean_build(clean_builds, content)
        elif content in read_clean_builds(clean_builds):
            identity = commit
            log(f"{args.image}: the build's inputs differ from {commit}, "
                f"but this image is what {commit} builds")
        else:
            identity = f"{commit}-dirty-{hex_digest(content)[:12]}"
            log(f"{args.image}: {why_not_clean(before, after)}; stamping {identity}")
            docker_build(command, identity, args.revision_label)
            image_id, config = image_digests(ref)
    record = {
        "image": args.image,
        "ref": ref,
        "image_id": image_id,
        "config_digest": config,
        "content": content,
        "identity": identity,
        "commit": commit,
        "certified_clean": clean,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if args.cluster:
        write_json(PROVENANCE_DIR / args.cluster / f"{args.image}.json", record)
    log(f"{args.image}: {identity} ({hex_digest(image_id)[:12]})")
    return 0


# ── check ─────────────────────────────────────────────────────────────────────


def local_image_names() -> list[str]:
    return run(["bash", str(IMAGES_LIB)]).split()


def repository_name(ref: str) -> str:
    """`space-worker` for `docker.io/library/space-worker:tilt-…` and the like."""
    name = ref.split("@", 1)[0]
    last = name.rsplit("/", 1)[-1]
    return last.split(":", 1)[0]


def workload_of(pod: dict) -> str:
    """The pod's controller as `<kind>/<name>`, a ReplicaSet's as its Deployment."""
    for owner in pod["metadata"].get("ownerReferences") or []:
        if not owner.get("controller"):
            continue
        kind, name = owner["kind"], owner["name"]
        template_hash = pod["metadata"].get("labels", {}).get("pod-template-hash")
        if kind == "ReplicaSet" and template_hash and name.endswith(f"-{template_hash}"):
            return f"deployment/{name.removesuffix(f'-{template_hash}')}"
        return f"{kind.lower()}/{name}"
    return f"pod/{pod['metadata']['name']}"


class Labels:
    """Revision labels of images in the nodes' containerd stores, by image ID."""

    def __init__(self) -> None:
        self.cache: dict[tuple[str, str], str | None] = {}

    def revision(self, node: str, image_id: str) -> str | None:
        key = (node, image_id)
        if key not in self.cache:
            try:
                info = json.loads(
                    run(["docker", "exec", node, "crictl", "inspecti", "-o", "json", image_id])
                )
                labels = info["info"]["imageSpec"]["config"].get("Labels") or {}
                # "" for an image without the label, which no build here makes.
                self.cache[key] = labels.get(REVISION_LABEL, "")
            except (subprocess.SubprocessError, OSError, KeyError, ValueError):
                self.cache[key] = None
        return self.cache[key]


def revision_text(revision: str | None) -> str:
    if revision is None:
        return "unknown revision"
    return revision or "no revision label"


def check(args: argparse.Namespace) -> int:
    names = set(local_image_names())
    records = {}
    for name in names:
        try:
            record = json.loads((PROVENANCE_DIR / args.cluster / f"{name}.json").read_text())
        except (OSError, ValueError):
            continue
        if isinstance(record, dict) and {"config_digest", "identity"} <= record.keys():
            records[name] = record
    pods = json.loads(
        run(["kubectl", "--context", f"kind-{args.cluster}", "-n", NAMESPACE,
             "get", "pods", "-o", "json"])
    )["items"]
    labels = Labels()
    containers, terminating, failed = [], [], []
    for pod in pods:
        meta, spec, status = pod["metadata"], pod["spec"], pod.get("status", {})
        if meta.get("deletionTimestamp"):
            terminating.append(meta["name"])
            continue
        # A Failed pod (evicted, or rejected at admission) runs nothing, and
        # its ReplicaSet neither counts nor deletes it, so no rollout removes it.
        if status.get("phase") == "Failed":
            failed.append({"pod": meta["name"], "workload": workload_of(pod),
                           "reason": status.get("reason")})
            continue
        statuses = {
            s["name"]: s
            for s in (status.get("initContainerStatuses") or [])
            + (status.get("containerStatuses") or [])
        }
        for container in (spec.get("initContainers") or []) + spec["containers"]:
            image = repository_name(container["image"])
            if image not in names:
                continue
            container_status = statuses.get(container["name"], {})
            running = container_status.get("imageID", "")
            waiting = (container_status.get("state") or {}).get("waiting") or {}
            record = records.get(image)
            revision = labels.revision(spec.get("nodeName", ""), running) if running else None
            if not record:
                verdict = "no build record"
            elif not running:
                verdict = "not started"
            elif hex_digest(running) != hex_digest(record["config_digest"]):
                verdict = "stale image"
            elif revision is not None and revision != record["identity"]:
                verdict = "revision mismatch"
            else:
                verdict = "latest"
            containers.append({
                "pod": meta["name"],
                "container": container["name"],
                "workload": workload_of(pod),
                "node": spec.get("nodeName"),
                "image": image,
                "image_ref": container["image"],
                "image_id": running,
                "revision": revision,
                # The chart-set service.version, which is not the build's.
                "service_version": next(
                    (e.get("value") for e in container.get("env") or []
                     if e["name"] == "SPACE_VERSION"),
                    None,
                ),
                "expected_image_id": record["config_digest"] if record else None,
                "expected_identity": record["identity"] if record else None,
                "verdict": verdict,
                # Why a container that hasn't started is waiting, from its status.
                "waiting_reason": waiting.get("reason") if not running else None,
                "waiting_message": waiting.get("message") if not running else None,
            })
    stale = [c for c in containers if c["verdict"] != "latest"]
    commit = checkout_commit() or "unknown"
    report = {
        "cluster": args.cluster,
        "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "checkout": {"commit": commit,
                     "dirty": bool(worktree_status().strip()) if commit != "unknown" else None},
        "builds": [records[name] for name in sorted(records)],
        "containers": containers,
        "terminating_pods": terminating,
        "failed_pods": failed,
        "verdict": "stale" if stale else "latest",
    }
    output = Path(args.output)
    write_json(output, report)
    counts: dict[tuple[str, str], int] = {}
    for c in containers:
        key = (c["image"], revision_text(c["revision"]))
        counts[key] = counts.get(key, 0) + 1
    for (image, revision), count in sorted(counts.items()):
        log(f"{image}: {count} container(s) at {revision}")
    for pod in failed:
        log(f"skipped Failed pod {pod['pod']} ({pod['workload']}, {pod['reason'] or 'no reason'})")
    if not containers:
        log(f"ERROR: no SPACE container found in namespace {NAMESPACE} of kind-{args.cluster}")
        return 1
    not_started = [c for c in stale if c["verdict"] == "not started"]
    outdated = [c for c in stale if c["verdict"] != "not started"]
    if not_started:
        # A pod that hasn't started (Pending, image pull, crash before start) is
        # not a stale build: rebuilding won't help, so name why it is waiting.
        log("ERROR: these containers have not started:")
        for row in not_started:
            reason = row["waiting_reason"] or "no waiting reason reported"
            detail = f": {row['waiting_message']}" if row["waiting_message"] else ""
            log(f"  {row['pod']} ({row['workload']}, container {row['container']}): "
                f"{reason}{detail}")
        log(f"  Remedy: see why with `kubectl --context kind-{args.cluster} -n {NAMESPACE} "
            "describe pod <name>` for each pod above, fix the cause, then rerun this check.")
    if outdated:
        log("ERROR: these workloads do not run the latest build of their image:")
        for workload in sorted({c["workload"] for c in outdated}):
            row = next(c for c in outdated if c["workload"] == workload)
            running = hex_digest(row["image_id"])[:12] or "nothing"
            latest = hex_digest(row["expected_image_id"] or "")[:12] or "none"
            log(f"  {workload} ({row['image']}): {row['verdict']}; runs {running} at "
                f"{revision_text(row['revision'])}, latest build {latest} at "
                f"{row['expected_identity'] or 'none'}")
        log("  Remedy: let Tilt finish rolling (`tilt get uiresources`), then "
            "`tilt trigger <name>` for each workload above (its Tilt resource has its "
            "name), which rebuilds the image and redeploys it; if Tilt is stopped, "
            "rerun this bring-up without SKIP_BOOTSTRAP=1, which restarts it.")
    if stale:
        log(f"  Record: {output}")
        return 1
    log(f"every SPACE container runs the latest build of its image; record: {output}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("snapshot")
    build_parser = sub.add_parser("build")
    build_parser.add_argument("--cluster")
    build_parser.add_argument("--image", required=True)
    build_parser.add_argument("--since", help="the snapshot taken before the chain's first step")
    build_parser.add_argument("--revision-label", action="store_true",
                              help="set the revision label; for a Dockerfile that sets none")
    build_parser.add_argument("command", nargs=argparse.REMAINDER)
    check_parser = sub.add_parser("check")
    check_parser.add_argument("--cluster", required=True)
    check_parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        if args.action == "snapshot":
            # Outside Git, a token `build` ignores, as it ignores the commit.
            print(snapshot() if checkout_commit() else "unknown")
            return 0
        return build(args) if args.action == "build" else check(args)
    except subprocess.CalledProcessError as error:
        log(f"ERROR: {' '.join(error.cmd[:6])} failed: {(error.stderr or '').strip()}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
