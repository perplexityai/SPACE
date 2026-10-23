#!/usr/bin/env python3
"""Print the Rust build (test-hooks or release) a Kind cluster's SPACE pods run.

The build is read from what runs, not from what was rendered: for every
running container of a SPACE Rust image (RUST_IMAGES) in namespace `space`,
the `space.pplx.net/kind-rust-build` label of the image that container runs,
read on its node (`crictl inspecti`). runtime-image-cache.sh copies that label
from the artifact image the runtime image took its binaries from; an image
without it predates the release lane, and was built with test hooks. Each such
pod's own `space.pplx.net/kind-rust-build` label, the build its chart was
rendered for (`rustBuild`, which decides whether the charts set test hooks),
must agree with its image; a pod without it was rendered before the lane, with
the hooks.

Exits 0 printing the one build when every container agrees, 1 when they do
not (the disagreeing containers on stderr), 2 when the cluster cannot be read,
and 3 when no SPACE Rust container runs yet.

Usage:
  deployed-rust-build.py [--cluster space-local] [--wait SECONDS] [--pods]

--wait first waits that long for every Deployment and DaemonSet that runs a
SPACE Rust image to finish rolling out; --pods prints one line per container
to stderr, the per-pod evidence.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass

NAMESPACE = "space"
LABEL = "space.pplx.net/kind-rust-build"
BUILDS = ("test-hooks", "release")
# The runtime images built from the Rust artifact image (the Tiltfile's rust
# runtime builds). space-seed-publish carries only the
# seed's guest binaries, and its Job's pod does not stay running.
RUST_IMAGES = frozenset(
    {"space-api", "space-data-plane", "space-cluster-controller", "space-worker", "space-kind-key-broker"}
)


class ReadError(Exception):
    pass


def run(args: list[str], timeout: int = 60) -> str:
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError) as error:
        raise ReadError(f"{' '.join(args[:4])}: {error}") from None
    if result.returncode != 0:
        raise ReadError(f"{' '.join(args[:4])}: {result.stderr.strip() or result.returncode}")
    return result.stdout


def image_repository(ref: str) -> str:
    """`space-api` for `space-api:local`, `docker.io/library/space-api:tilt-…`, …"""
    name = ref.split("@", 1)[0].rsplit("/", 1)[-1]
    return name.split(":", 1)[0]


def runs_rust_image(pod_spec: dict) -> bool:
    containers = (pod_spec.get("containers") or []) + (pod_spec.get("initContainers") or [])
    return any(image_repository(c.get("image", "")) in RUST_IMAGES for c in containers)


@dataclass(frozen=True)
class Container:
    pod: str
    name: str
    node: str
    image_id: str
    rendered: str
    image_build: str = ""


class Cluster:
    def __init__(self, name: str) -> None:
        self.name = name
        self.context = f"kind-{name}"
        self._image_builds: dict[tuple[str, str], str] = {}

    def kubectl_json(self, *args: str) -> dict:
        out = run(["kubectl", "--context", self.context, "-n", NAMESPACE, *args, "-o", "json"])
        try:
            return json.loads(out)
        except ValueError as error:
            raise ReadError(f"kubectl {' '.join(args)}: {error}") from None

    def wait_for_rollouts(self, timeout: int) -> None:
        workloads = self.kubectl_json("get", "deployments,daemonsets")
        for item in workloads.get("items") or []:
            if not runs_rust_image(((item.get("spec") or {}).get("template") or {}).get("spec") or {}):
                continue
            ref = f"{item['kind'].lower()}/{item['metadata']['name']}"
            print(f"[deployed-rust-build] waiting for {ref} to roll out", file=sys.stderr)
            run(
                ["kubectl", "--context", self.context, "-n", NAMESPACE, "rollout", "status",
                 ref, f"--timeout={timeout}s"],
                timeout=timeout + 30,
            )

    def containers(self) -> list[Container]:
        found = []
        for pod in self.kubectl_json("get", "pods").get("items") or []:
            meta, spec, status = pod["metadata"], pod.get("spec") or {}, pod.get("status") or {}
            # A pod being deleted is on its way out of every Service already.
            if meta.get("deletionTimestamp") or status.get("phase") != "Running":
                continue
            images = {c["name"]: c.get("image", "") for c in spec.get("containers") or []}
            rendered = (meta.get("labels") or {}).get(LABEL) or "test-hooks"
            for container in status.get("containerStatuses") or []:
                if image_repository(images.get(container["name"], "")) not in RUST_IMAGES:
                    continue
                if "running" not in (container.get("state") or {}):
                    continue
                found.append(
                    Container(meta["name"], container["name"], spec.get("nodeName", ""),
                              container.get("imageID", ""), rendered)
                )
        return found

    def image_build(self, node: str, image_id: str) -> str:
        key = (node, image_id)
        if key not in self._image_builds:
            if not node.startswith(f"{self.name}-") or not image_id:
                raise ReadError(f"no Kind node or image for container on {node!r} ({image_id!r})")
            info = json.loads(run(["docker", "exec", node, "crictl", "inspecti", "-o", "json", image_id]))
            config = (((info.get("info") or {}).get("imageSpec") or {}).get("config")) or {}
            self._image_builds[key] = (config.get("Labels") or {}).get(LABEL) or "test-hooks"
        return self._image_builds[key]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cluster", default="space-local")
    parser.add_argument("--wait", type=int, default=0, metavar="SECONDS")
    parser.add_argument("--pods", action="store_true")
    args = parser.parse_args(argv)

    cluster = Cluster(args.cluster)
    try:
        if args.wait > 0:
            cluster.wait_for_rollouts(args.wait)
        containers = [
            Container(c.pod, c.name, c.node, c.image_id, c.rendered, cluster.image_build(c.node, c.image_id))
            for c in cluster.containers()
        ]
    except (ReadError, ValueError) as error:
        print(f"[deployed-rust-build] ERROR: cannot read the Rust build: {error}", file=sys.stderr)
        return 2
    if args.pods:
        for c in sorted(containers, key=lambda c: (c.pod, c.name)):
            print(f"{c.pod} {c.name} node={c.node} image={c.image_id.removeprefix('sha256:')[:12]} "
                  f"image_build={c.image_build} rendered={c.rendered}", file=sys.stderr)
    if not containers:
        print(f"[deployed-rust-build] no SPACE Rust container runs in kind-{args.cluster}", file=sys.stderr)
        return 3
    builds = {c.image_build for c in containers}
    disagreeing = [c for c in containers if c.image_build != c.rendered or c.image_build not in BUILDS]
    if len(builds) == 1 and not disagreeing:
        print(builds.pop())
        return 0
    print(f"[deployed-rust-build] the SPACE Rust containers run no single build: {sorted(builds)}", file=sys.stderr)
    by_build: dict[str, list[str]] = {}
    for c in containers:
        by_build.setdefault(f"image {c.image_build}, rendered {c.rendered}", []).append(f"{c.pod}/{c.name}")
    for key, names in sorted(by_build.items()):
        print(f"  {key}: {', '.join(sorted(names))}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
