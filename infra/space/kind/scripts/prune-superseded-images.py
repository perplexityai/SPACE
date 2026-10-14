#!/usr/bin/env python3
"""Remove superseded builds of SPACE's locally built images from the Kind nodes.

Every bring-up loads fresh builds of the images in lib/space-images.sh into
every node's containerd store, which lives under $SPACE_KIND_DATA_ROOT and
survives cluster deletion. Nothing removed the old builds: a moved `:local` tag
leaves the previous image behind untagged, and each Tilt build adds another
`:tilt-<hash>` tag. `crictl rmi --prune` is no remedy, because it also removes
every image no container is using at that moment, including the seed-publish
image between publishes, which Kind cannot pull back.

An image is removed only when all of these hold:

* it belongs to one of those repositories: every tag and digest it carries
  names one, or, untagged, its `org.opencontainers.image.title` label does;
* no current reference names it: the `<name>:local` refs build-images.sh loads,
  every image a workload in the cluster references (pods, their controllers,
  Jobs and CronJobs, the rendered charts as applied), and every image the Tilt
  serving this cluster last built;
* no container on that node, running or exited, uses it.

Anything else, third-party images included, is left alone. Pruning is skipped
while the Tilt serving this cluster has a build pending or in progress, because
a just-loaded image is not deployed yet.

Usage:
  prune-superseded-images.py                  # one pass (bootstrap.sh)
  prune-superseded-images.py --dry-run        # report only
  prune-superseded-images.py --watch 30 \\
      --tilt-resource prune-superseded-images  # the Tiltfile's resource

Set SPACE_KIND_PRUNE_SUPERSEDED_IMAGES=0 to disable it.
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
from pathlib import Path

KIND_DIR = Path(__file__).resolve().parents[1]
IMAGES_LIB = KIND_DIR / "lib/space-images.sh"
OPT_OUT = "SPACE_KIND_PRUNE_SUPERSEDED_IMAGES"
TITLE_LABEL = "org.opencontainers.image.title"
LOCAL_TAG = "local"
# Where every node keeps the store, the bind mount from $SPACE_KIND_DATA_ROOT.
NODE_STORE = "/var/lib/containerd"
# The workload kinds whose pod templates name images. A ReplicaSet scaled to
# zero is a Deployment's rollback history, not a current reference.
WORKLOAD_KINDS = "pods,deployments,replicasets,statefulsets,daemonsets,jobs,cronjobs"
TILT_BUSY = ("pending", "in_progress")


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

    def store_bytes(self, node: str) -> int | None:
        try:
            return int(
                run(["docker", "exec", node, "du", "-sxb", NODE_STORE]).split()[0]
            )
        except (PruneError, ValueError, IndexError):
            return None

    def title(self, node: str, image_id: str) -> str | None:
        try:
            info = run_json(
                ["docker", "exec", node, "crictl", "inspecti", "-o", "json", image_id]
            )
        except PruneError:
            return None
        config = (((info.get("info") or {}).get("imageSpec") or {}).get("config")) or {}
        return (config.get("Labels") or {}).get(TITLE_LABEL)

    def prune_node(
        self, node: str, repos: frozenset[str], current: set[str], view: TiltView | None
    ) -> tuple[int, int]:
        images = parse_images(
            run_json(["docker", "exec", node, "crictl", "images", "-o", "json"])
        )
        in_use_ids, in_use_refs = containers_in_use(
            run_json(["docker", "exec", node, "crictl", "ps", "-a", "-o", "json"])
        )
        images = [
            Image(image.id, image.refs, image.size, self.title(node, image.id))
            if not image.refs and image.id not in in_use_ids
            else image
            for image in images
        ]
        doomed = superseded(images, repos, current | in_use_refs, in_use_ids)
        if not doomed:
            return 0, 0
        if self.dry_run:
            log(f"{node}: would remove {len(doomed)} superseded image(s):")
            for image in doomed:
                log(f"  {image.describe()}")
            return len(doomed), 0
        # A build may have started while this node was read. Its image is not
        # deployed yet, so stop rather than remove anything Tilt now has.
        if view is not None:
            again = self.tilt()
            if again is None or again.busy or not again.refs <= current:
                raise PruneError(f"Tilt started an update; stopping before {node}")
        before = self.store_bytes(node)
        removed = []
        for image in doomed:
            try:
                run(["docker", "exec", node, "crictl", "rmi", image.id])
                removed.append(image)
            except PruneError as error:
                log(f"WARNING: {node}: could not remove {image.describe()}: {error}")
        after = self.store_bytes(node)
        freed = before - after if before is not None and after is not None else 0
        log(
            f"{node}: removed {len(removed)} superseded image(s), "
            f"freed {human(max(freed, 0))} of the node store:"
        )
        for image in removed:
            log(f"  {image.describe()}")
        return len(removed), max(freed, 0)

    def prune(self) -> bool:
        """One pass over every node. False when it stood down."""
        view = self.tilt()
        if view is not None and view.busy:
            log(
                f"Tilt is updating {', '.join(view.busy)}; not pruning until it settles"
            )
            return False
        names = local_image_names()
        repos = space_repositories(names)
        current = self.current_refs(names, view)
        total_images = total_bytes = 0
        for node in self.nodes():
            count, freed = self.prune_node(node, repos, current, view)
            total_images += count
            total_bytes += freed
        if total_images and not self.dry_run:
            log(
                f"removed {total_images} superseded SPACE image(s) in total, "
                f"freeing {human(total_bytes)} under $SPACE_KIND_DATA_ROOT"
            )
        return True

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

    def watch(self, interval: int) -> None:
        """Prune whenever the current references change and Tilt is idle.

        A finished rollout changes them too (its old pods go), so the build it
        replaced is removed once nothing uses it.
        """
        last = warned = None
        while True:
            try:
                seen = self.fingerprint()
                if seen is not None and seen != last and self.prune():
                    last = seen
                warned = None
            except PruneError as error:
                # Once per distinct cause: Tilt takes a while to connect.
                if str(error) != warned:
                    log(f"WARNING: not pruning yet: {error}")
                warned = str(error)
            time.sleep(interval)


def human(count: int) -> str:
    value = float(count)
    for unit in ("B", "KiB", "MiB"):
        if value < 1024:
            return f"{count} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GiB"


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
        "--watch",
        type=int,
        metavar="SECONDS",
        help="keep running, pruning after each settled Tilt update (implies a Tilt is required)",
    )
    args = parser.parse_args(argv)
    if os.environ.get(OPT_OUT, "1") == "0":
        log(f"superseded-image pruning is off ({OPT_OUT}=0)")
        if args.watch:
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
    )
    if args.watch:
        pruner.watch(args.watch)
    try:
        pruner.prune()
    except PruneError as error:
        log(f"WARNING: stopped without removing anything further: {error}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
