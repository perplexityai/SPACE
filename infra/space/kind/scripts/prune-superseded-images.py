#!/usr/bin/env python3
"""Remove superseded builds of SPACE's locally built images from the Kind nodes,
or with --host from the host's Docker store.

Every bring-up loads fresh builds of the images in lib/space-images.sh into
the containerd store of each node that runs them, which lives under
$SPACE_KIND_DATA_ROOT and survives cluster deletion. Nothing removed the old
builds: a moved `:local` tag leaves the previous image behind untagged, and
each changed Tilt build adds another `:tilt-<hash>` tag. `crictl rmi --prune`
is no remedy, because it also removes every image no container is using at
that moment, including the seed-publish image between publishes, which Kind
cannot pull back.

An image is removed only when all of these hold:

* it belongs to one of those repositories: every tag and digest it carries
  names one, or, untagged, its `org.opencontainers.image.title` label does;
* no current reference names it: the `<name>:local` refs (the retired scripted
  bring-up loaded its builds under them),
  every image a workload in the cluster references (pods, their controllers,
  Jobs and CronJobs, the rendered charts as applied), and every image the Tilt
  serving this cluster last built;
* no container on that node, running or exited, uses it.

With --host the same rules select from the host's Docker store, where the
builds are made, except that each image also keeps its newest build nothing
current names: the build before the current one. There every Tilt build adds a
`:tilt-build-<time>-<nonce>` tag and the `:tilt-<hash>` Tilt deploys
(custom_build), and on Docker's graph-driver store a rebuilt `:local` leaves
the previous build untagged. Tilt's docker_prune_settings pruner removes none
of them: it passes over every image with more than one tag
(internal/engine/dockerprune/docker_pruner.go in tilt-dev/tilt).

Anything else, third-party images included, is left alone. Pruning is skipped
while the Tilt serving this cluster has a build pending or in progress, because
a just-loaded image is not deployed yet, and checked again before each removal,
stopping the pass as soon as Tilt has begun one. The sizes it logs are what
`crictl images` (or, on the host, `docker image inspect`) reports, the image
content; the unpacked layers free more, and layers a kept build shares stay.

Usage:
  prune-superseded-images.py                  # one pass
  prune-superseded-images.py --dry-run        # report only
  prune-superseded-images.py --watch 30 \\
      --tilt-resource prune-superseded-images  # the Tiltfile's resource
  prune-superseded-images.py --host           # the host's Docker store

Set SPACE_KIND_PRUNE_SUPERSEDED_IMAGES=0 to keep the nodes' builds, and
SPACE_TILT_PRUNE_HOST_IMAGES=0 to keep the host's.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

KIND_DIR = Path(__file__).resolve().parents[1]
IMAGES_LIB = KIND_DIR / "lib/space-images.sh"
OPT_OUT = "SPACE_KIND_PRUNE_SUPERSEDED_IMAGES"
HOST_OPT_OUT = "SPACE_TILT_PRUNE_HOST_IMAGES"
TITLE_LABEL = "org.opencontainers.image.title"
LOCAL_TAG = "local"
# The workload kinds whose pod templates name images. A ReplicaSet scaled to
# zero is a Deployment's rollback history, not a current reference.
WORKLOAD_KINDS = "pods,deployments,replicasets,statefulsets,daemonsets,jobs,cronjobs"
TILT_BUSY = ("pending", "in_progress")
# While a pass left a superseded image behind (a container still held it, or
# its removal failed), the watcher runs another pass after this many ticks even
# if no reference changed: nothing it fingerprints moves when that container
# goes away.
RETRY_TICKS = 10


class PruneError(RuntimeError):
    """State the selection depends on could not be read; remove nothing."""


def log(message: str) -> None:
    print(f"[prune-images] {message}", flush=True)


def normalize(ref: str) -> str | None:
    """Expand a reference the way containerd names it, or None for an image ID.

    `space-api:local` is `docker.io/library/space-api:local`; a bare name means
    `:latest`. Kubernetes, crictl and Tilt each print a different spelling.
    """
    ref = ref.strip()
    if not ref or ref.startswith("sha256:"):
        return None
    name, at, digest = ref.partition("@")
    tag = ""
    if ":" in name.rsplit("/", 1)[-1]:
        name, tag = name.rsplit(":", 1)
    first, slash, rest = name.partition("/")
    if slash and ("." in first or ":" in first or first == "localhost"):
        domain, path = first, rest
    else:
        domain, path = "docker.io", name
    if domain == "docker.io" and "/" not in path:
        path = f"library/{path}"
    if at:
        return f"{domain}/{path}@{digest}"
    return f"{domain}/{path}:{tag or 'latest'}"


def repository(ref: str) -> str:
    """The repository of a normalized reference."""
    name = ref.split("@", 1)[0]
    if ":" in name.rsplit("/", 1)[-1]:
        name = name.rsplit(":", 1)[0]
    return name


@dataclass(frozen=True)
class Image:
    id: str
    refs: tuple[str, ...]
    size: int
    title: str | None = None
    # When the image was last built or tagged, in seconds since the epoch; only
    # the host store reports it.
    tagged_at: float = 0.0

    def describe(self) -> str:
        if self.refs:
            return ", ".join(self.refs)
        return f"{self.title} (untagged {self.id.removeprefix('sha256:')[:12]})"


def space_repositories(names: list[str]) -> frozenset[str]:
    return frozenset(repository(normalize(name)) for name in names)


def parse_images(crictl_images: dict) -> list[Image]:
    """`crictl images -o json` as Images, references normalized."""
    images = []
    for item in crictl_images.get("images") or []:
        refs = [
            normalize(ref)
            for ref in (item.get("repoTags") or []) + (item.get("repoDigests") or [])
        ]
        images.append(
            Image(
                id=item["id"],
                refs=tuple(sorted({ref for ref in refs if ref})),
                size=int(item.get("size") or 0),
            )
        )
    return images


def containers_in_use(crictl_ps: dict) -> tuple[set[str], set[str]]:
    """Image IDs and references of every container `crictl ps -a` reports."""
    ids: set[str] = set()
    refs: set[str] = set()
    for container in crictl_ps.get("containers") or []:
        image = container.get("image") or {}
        for value in (
            container.get("imageRef"),
            container.get("imageId"),
            image.get("image"),
            image.get("userSpecifiedImage"),
        ):
            if not value:
                continue
            if value.startswith("sha256:"):
                ids.add(value)
            elif normalized := normalize(value):
                refs.add(normalized)
    return ids, refs


def is_space_image(image: Image, repos: frozenset[str]) -> bool:
    if image.refs:
        return all(repository(ref) in repos for ref in image.refs)
    # Untagged: the tag moved to a newer build. Only the build's own title
    # label still says which image it was.
    title = normalize(image.title or "")
    return title is not None and repository(title) in repos


def superseded(
    images: list[Image],
    repos: frozenset[str],
    current_refs: set[str],
    in_use_ids: set[str],
) -> list[Image]:
    """The images on one node that are safe to remove. Pure; see the module doc."""
    return [
        image
        for image in images
        if image.id not in in_use_ids
        and not any(ref in current_refs for ref in image.refs)
        and is_space_image(image, repos)
    ]


def image_repository(image: Image) -> str:
    """The SPACE repository of an image `is_space_image` accepted."""
    if image.refs:
        return min(repository(ref) for ref in image.refs)
    return repository(normalize(image.title or "") or "")


def superseded_on_host(
    images: list[Image],
    repos: frozenset[str],
    current_refs: set[str],
    in_use_ids: set[str],
) -> list[Image]:
    """The host images that are safe to remove. Pure; see the module doc.

    As on a node, plus: the newest build of each image that nothing current
    names stays, so the build before the current one is always kept.
    """
    candidates = superseded(images, repos, current_refs, set())
    previous: dict[str, Image] = {}
    for image in candidates:
        repo = image_repository(image)
        if repo not in previous or image.tagged_at > previous[repo].tagged_at:
            previous[repo] = image
    keep = {image.id for image in previous.values()} | in_use_ids
    return [image for image in candidates if image.id not in keep]


def parse_time(value: str | None) -> float:
    """A Docker timestamp in seconds since the epoch; 0 when absent."""
    try:
        return datetime.fromisoformat(value or "").timestamp()
    except ValueError:
        return 0.0


def parse_docker_images(inspects: list) -> list[Image]:
    """`docker image inspect` output as Images, references normalized."""
    images = []
    for item in inspects:
        refs = [
            normalize(ref)
            for ref in (item.get("RepoTags") or []) + (item.get("RepoDigests") or [])
        ]
        labels = (item.get("Config") or {}).get("Labels") or {}
        images.append(
            Image(
                id=item["Id"],
                refs=tuple(sorted({ref for ref in refs if ref})),
                size=int(item.get("Size") or 0),
                title=labels.get(TITLE_LABEL),
                tagged_at=max(
                    parse_time((item.get("Metadata") or {}).get("LastTagTime")),
                    parse_time(item.get("Created")),
                ),
            )
        )
    return images


def docker_containers_in_use(inspects: list) -> tuple[set[str], set[str]]:
    """Image IDs and references of every container `docker ps -a` lists."""
    ids: set[str] = set()
    refs: set[str] = set()
    for container in inspects:
        if image_id := container.get("Image"):
            ids.add(image_id)
        if normalized := normalize((container.get("Config") or {}).get("Image") or ""):
            refs.add(normalized)
    return ids, refs


def _pod_specs(item: dict):
    spec = item.get("spec") or {}
    kind = item.get("kind")
    if kind == "Pod":
        yield spec
    elif kind == "CronJob":
        job = (spec.get("jobTemplate") or {}).get("spec") or {}
        yield (job.get("template") or {}).get("spec") or {}
    elif kind == "ReplicaSet" and not (
        spec.get("replicas") or (item.get("status") or {}).get("replicas")
    ):
        return
    else:
        yield (spec.get("template") or {}).get("spec") or {}


def workload_refs(kubectl_list: dict) -> set[str]:
    """Every image reference the cluster's workloads name.

    Container images, plus env values that parse as one of these images, for
    workloads that hand an image to something they launch.
    """
    refs: set[str] = set()
    for item in kubectl_list.get("items") or []:
        for pod_spec in _pod_specs(item):
            for field in ("initContainers", "containers", "ephemeralContainers"):
                for container in pod_spec.get(field) or []:
                    if normalized := normalize(container.get("image") or ""):
                        refs.add(normalized)
                    for env in container.get("env") or []:
                        value = env.get("value") or ""
                        if (
                            value
                            and " " not in value
                            and (normalized := normalize(value))
                        ):
                            refs.add(normalized)
    return refs


# ── Environment access ─────────────────────────────────────────────────────


def run(args: list[str], timeout: int = 60) -> str:
    try:
        result = subprocess.run(
            args, check=True, capture_output=True, text=True, timeout=timeout
        )
    except (OSError, subprocess.SubprocessError) as error:
        stderr = getattr(error, "stderr", None)
        detail = stderr.strip() if isinstance(stderr, str) and stderr.strip() else error
        raise PruneError(f"{' '.join(args[:4])}: {detail}") from error
    return result.stdout


def run_json(args: list[str], timeout: int = 60) -> dict:
    output = run(args, timeout)
    try:
        return json.loads(output or "{}")
    except json.JSONDecodeError as error:
        raise PruneError(f"{' '.join(args[:4])}: unparseable output") from error


def local_image_names() -> list[str]:
    names = run(["bash", str(IMAGES_LIB)]).split()
    if not names:
        raise PruneError(f"{IMAGES_LIB} listed no images")
    return names


@dataclass(frozen=True)
class TiltView:
    busy: tuple[str, ...]
    refs: frozenset[str]


def tilt_view(cluster: str, port: str, own_resource: str | None) -> TiltView | None:
    """The Tilt serving this cluster on `port`, or None when there is none."""
    if not shutil.which("tilt"):
        return None

    def get(*args: str) -> dict | None:
        try:
            return run_json(["tilt", "get", *args, "--port", port, "-o", "json"], 10)
        except PruneError:
            return None

    session = get("cluster", "default")
    if session is None:
        return None
    context = (
        ((session.get("status") or {}).get("connection") or {}).get("kubernetes") or {}
    ).get("context")
    if context != f"kind-{cluster}":
        return None
    resources, maps = get("uiresources"), get("imagemaps")
    if resources is None or maps is None:
        raise PruneError(
            f"Tilt on port {port} serves kind-{cluster} but did not answer"
        )
    busy = tuple(
        sorted(
            item["metadata"]["name"]
            for item in resources.get("items") or []
            if (item.get("status") or {}).get("updateStatus") in TILT_BUSY
            and item["metadata"]["name"] != own_resource
        )
    )
    refs = set()
    for item in maps.get("items") or []:
        status = item.get("status") or {}
        for value in (status.get("image"), status.get("imageFromCluster")):
            if value and (normalized := normalize(value)):
                refs.add(normalized)
    return TiltView(busy, frozenset(refs))


@dataclass
class Pruner:
    cluster: str
    tilt_port: str
    tilt_resource: str | None
    require_tilt: bool
    dry_run: bool
    host: bool = False

    @property
    def context(self) -> str:
        return f"kind-{self.cluster}"

    def tilt(self) -> TiltView | None:
        view = tilt_view(self.cluster, self.tilt_port, self.tilt_resource)
        if view is None and self.require_tilt:
            raise PruneError(f"no Tilt on port {self.tilt_port} serves {self.context}")
        return view

    def current_refs(self, names: list[str], view: TiltView | None) -> set[str]:
        refs = {normalize(f"{name}:{LOCAL_TAG}") for name in names}
        refs |= workload_refs(
            run_json(
                [
                    "kubectl",
                    "--context",
                    self.context,
                    "get",
                    WORKLOAD_KINDS,
                    "--all-namespaces",
                    "-o",
                    "json",
                ],
                120,
            )
        )
        if view is not None:
            refs |= view.refs
        return refs

    def nodes(self) -> list[str]:
        nodes = run(["kind", "get", "nodes", "--name", self.cluster]).split()
        if not nodes:
            raise PruneError(f"kind cluster {self.cluster} has no nodes")
        return sorted(nodes)

    def title(self, node: str, image_id: str) -> str | None:
        try:
            info = run_json(
                ["docker", "exec", node, "crictl", "inspecti", "-o", "json", image_id]
            )
        except PruneError:
            return None
        config = (((info.get("info") or {}).get("imageSpec") or {}).get("config")) or {}
        return (config.get("Labels") or {}).get(TITLE_LABEL)

    def assert_tilt_unchanged(self, view: TiltView | None, current: set[str]) -> None:
        """Raise when the Tilt this pass started from has begun an update.

        Its new build is loaded before it is deployed, so nothing may be
        removed from then on. Checked before every removal.
        """
        if view is None:
            return
        again = self.tilt()
        if again is None or again.busy or not again.refs <= current:
            raise PruneError("Tilt started an update; stopping")

    def prune_node(
        self, node: str, repos: frozenset[str], current: set[str], view: TiltView | None
    ) -> tuple[int, int, bool]:
        """Remove one node's superseded images.

        Returns the count removed, their summed image size, and whether a
        superseded image stayed behind because a container held it or its
        removal failed.
        """
        images = parse_images(
            run_json(["docker", "exec", node, "crictl", "images", "-o", "json"])
        )
        in_use_ids, in_use_refs = containers_in_use(
            run_json(["docker", "exec", node, "crictl", "ps", "-a", "-o", "json"])
        )
        images = [
            Image(image.id, image.refs, image.size, self.title(node, image.id))
            if not image.refs
            else image
            for image in images
        ]
        doomed = superseded(images, repos, current | in_use_refs, in_use_ids)
        held = len(superseded(images, repos, current, set())) > len(doomed)
        if not doomed:
            return 0, 0, held
        if self.dry_run:
            log(f"{node}: would remove {len(doomed)} superseded image(s):")
            for image in doomed:
                log(f"  {image.describe()}")
            return len(doomed), sum(image.size for image in doomed), held
        removed: list[Image] = []
        failed = False
        try:
            for image in doomed:
                self.assert_tilt_unchanged(view, current)
                try:
                    run(["docker", "exec", node, "crictl", "rmi", image.id])
                    removed.append(image)
                except PruneError as error:
                    failed = True
                    log(
                        f"WARNING: {node}: could not remove {image.describe()}: {error}"
                    )
        finally:
            # Also when Tilt stopped the pass part-way: say what is gone.
            if removed:
                log(
                    f"{node}: removed {len(removed)} superseded image(s), "
                    f"{human(sum(image.size for image in removed))} of image content:"
                )
                for image in removed:
                    log(f"  {image.describe()}")
        return len(removed), sum(image.size for image in removed), held or failed

    def docker_inspect(self, kind: str, ids: list[str]) -> list:
        if not ids:
            return []
        output = run(["docker", kind, "inspect", *ids], 120)
        try:
            return json.loads(output or "[]")
        except json.JSONDecodeError as error:
            raise PruneError(f"docker {kind} inspect: unparseable output") from error

    def prune_host(
        self, repos: frozenset[str], current: set[str], view: TiltView | None
    ) -> tuple[int, int, bool]:
        """Remove the host Docker store's superseded images; as prune_node.

        A tagged image goes by untagging each of its tags, never by ID, so
        Docker deletes it only if it gained no tag since it was listed.
        """
        ids = sorted(set(run(["docker", "image", "ls", "-q", "--no-trunc"]).split()))
        images = parse_docker_images(self.docker_inspect("image", ids))
        containers = run(["docker", "ps", "-aq", "--no-trunc"]).split()
        in_use_ids, in_use_refs = docker_containers_in_use(
            self.docker_inspect("container", containers)
        )
        in_use_ids |= {
            image.id
            for image in images
            if any(ref in in_use_refs for ref in image.refs)
        }
        doomed = superseded_on_host(images, repos, current, in_use_ids)
        held = len(superseded_on_host(images, repos, current, set())) > len(doomed)
        if not doomed:
            return 0, 0, held
        if self.dry_run:
            log(f"host: would remove {len(doomed)} superseded image(s):")
            for image in doomed:
                log(f"  {image.describe()}")
            return len(doomed), sum(image.size for image in doomed), held
        removed: list[Image] = []
        failed = False
        try:
            for image in doomed:
                self.assert_tilt_unchanged(view, current)
                tags = [ref for ref in image.refs if "@" not in ref]
                try:
                    run(["docker", "image", "rm", *(tags or [image.id])])
                    removed.append(image)
                except PruneError as error:
                    failed = True
                    log(f"WARNING: host: could not remove {image.describe()}: {error}")
        finally:
            if removed:
                log(
                    f"host: removed {len(removed)} superseded image(s), "
                    f"{human(sum(image.size for image in removed))} of image content:"
                )
                for image in removed:
                    log(f"  {image.describe()}")
        return len(removed), sum(image.size for image in removed), held or failed

    def prune(self) -> bool | None:
        """One pass over every node, or over the host store with --host.

        None when it stood down for a busy Tilt; otherwise whether a
        superseded image stayed behind, for the watcher to retry.
        """
        view = self.tilt()
        if view is not None and view.busy:
            log(
                f"Tilt is updating {', '.join(view.busy)}; not pruning until it settles"
            )
            return None
        names = local_image_names()
        repos = space_repositories(names)
        current = self.current_refs(names, view)
        if self.host:
            return self.prune_host(repos, current, view)[2]
        total_images = total_bytes = 0
        left_behind = False
        for node in self.nodes():
            count, size, held = self.prune_node(node, repos, current, view)
            total_images += count
            total_bytes += size
            left_behind |= held
        if total_images and not self.dry_run:
            log(
                f"removed {total_images} superseded SPACE image(s) in total, "
                f"{human(total_bytes)} of image content under $SPACE_KIND_DATA_ROOT"
            )
        return left_behind

    def fingerprint(self) -> str | None:
        """What a new pass would see as current, or None while Tilt is busy."""
        view = self.tilt()
        if view is not None and view.busy:
            return None
        names = local_image_names()
        repos = space_repositories(names)
        # Only SPACE references: sandbox pods come and go on the pause image
        # all day, and their churn is no reason for a pass.
        refs = sorted(
            ref for ref in self.current_refs(names, view) if repository(ref) in repos
        )
        return hashlib.sha256("\n".join(refs).encode()).hexdigest()

    def tick(self, state: WatchState) -> None:
        """One watcher round: prune when the references changed, or when the
        last pass left something behind and RETRY_TICKS rounds have passed."""
        seen = self.fingerprint()
        state.since += 1
        due = state.left_behind and state.since >= RETRY_TICKS
        if seen is None or (seen == state.last and not due):
            return
        left_behind = self.prune()
        if left_behind is not None:
            state.last, state.left_behind, state.since = seen, left_behind, 0

    def watch(self, interval: int) -> None:
        """Prune whenever the current references change and Tilt is idle.

        A finished rollout changes them too (its old pods go), so the build it
        replaced is removed once nothing uses it.
        """
        state = WatchState()
        warned = None
        while True:
            try:
                self.tick(state)
                warned = None
            except PruneError as error:
                # Once per distinct cause: Tilt takes a while to connect.
                if str(error) != warned:
                    log(f"WARNING: not pruning yet: {error}")
                warned = str(error)
            time.sleep(interval)


@dataclass
class WatchState:
    last: str | None = None
    left_behind: bool = False
    since: int = 0


def human(count: int) -> str:
    value = float(count)
    for unit in ("B", "KiB", "MiB"):
        if value < 1024:
            return f"{count} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GiB"


def positive_seconds(value: str) -> int:
    seconds = int(value)
    if seconds < 1:
        raise argparse.ArgumentTypeError("must be at least 1 second")
    return seconds


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n", 1)[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cluster", default=os.environ.get("SPACE_KIND_CLUSTER", "space-local")
    )
    parser.add_argument("--tilt-port", default=os.environ.get("TILT_PORT", "10350"))
    parser.add_argument(
        "--tilt-resource",
        help="this process's own Tilt resource, ignored when checking whether Tilt is busy",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--host",
        action="store_true",
        help="prune the host's Docker store, keeping each image's previous build",
    )
    parser.add_argument(
        "--watch",
        type=positive_seconds,
        metavar="SECONDS",
        help="keep running, pruning after each settled Tilt update (implies a Tilt is required)",
    )
    args = parser.parse_args(argv)
    opt_out = HOST_OPT_OUT if args.host else OPT_OUT
    if os.environ.get(opt_out, "1") == "0":
        log(f"superseded-image pruning is off ({opt_out}=0)")
        if args.watch is not None:
            # A Tilt serve_cmd that exits shows as an error; idle instead.
            while True:
                time.sleep(3600)
        return 0
    pruner = Pruner(
        cluster=args.cluster,
        tilt_port=args.tilt_port,
        tilt_resource=args.tilt_resource,
        require_tilt=args.watch is not None,
        dry_run=args.dry_run,
        host=args.host,
    )
    if args.watch is not None:
        pruner.watch(args.watch)
    try:
        pruner.prune()
    except PruneError as error:
        log(f"WARNING: stopped without removing anything further: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
