#!/usr/bin/env python3
"""Load one SPACE image build into the Kind nodes that can run it.

Every custom_build in the Tiltfile ends with this script instead of Tilt's
own "Loading image to KIND" step (`disable_push`), and Tilt deploys the
reference it writes (`outputs_image_ref_to`). Tilt's step ran
`kind load docker-image` into every node of the cluster, and kind skips a node
only when the image ID Docker reports equals the one the node's CRI reports.
On Docker's containerd image store the first is the manifest digest and the
second the config digest, so they never match, and every Tilt start copied
every image to all six nodes again, changed or not. This script:

* tags the build `<name>:tilt-<the first 16 hex digits of its config
  digest>`, so an unchanged image keeps its reference (no workload rolls)
  and a changed one always gets a new reference. --build-ref is read once:
  the tag is applied to the image ID that read returned, so a build that
  retags --build-ref meanwhile cannot put its content under this reference;
* loads that reference onto each node a pod running the image can schedule
  onto, except a node whose CRI already holds it at that config digest. A
  digest that cannot be read counts as absent, so the node is loaded;
* writes the reference to --ref-file, for Tilt to deploy.

Where the image's pods can schedule comes from the manifests Tilt deploys: the
Tiltfile passes, in SPACE_KIND_IMAGE_PLACEMENT, the nodeSelector, required
node affinity and nodeName of every pod template that runs the image (its
`deploy_yaml` helper records them from the rendered charts). A node is a target
when it satisfies any one of them. The set only ever errs toward more nodes:
tolerations and taints are ignored; an affinity operator or field this script
does not evaluate matches every node; a label key that no node carries, which
the bring-up has not applied yet (label-kind-infra), matches every node; a
node whose labels cannot be read is a target; and an image with no recorded
pod template goes to every node. A node that gains a matching label later gets
the image at the image's next build.

Usage:
  kind-load-image.py --cluster <name> --image <name> --build-ref <ref> --ref-file <path>
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

PLACEMENT_ENV = "SPACE_KIND_IMAGE_PLACEMENT"


def log(message: str) -> None:
    print(f"[kind-load] {message}", file=sys.stderr, flush=True)


def run(args: list[str], timeout: int = 60) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True, timeout=timeout
    ).stdout


def hex_digest(value: str) -> str:
    return value.rsplit("@", 1)[-1].removeprefix("sha256:")


def resolve(ref: str) -> tuple[str, str]:
    """(image ID, config digest) of a local image, from one read of `ref`.

    The config digest is what the nodes' CRI names the image by. On Docker's
    containerd image store the image ID is the manifest digest and the config
    digest is its descriptor's annotation; on the graph-driver store the
    image ID is the config digest.
    """
    image = json.loads(run(["docker", "image", "inspect", ref]))[0]
    descriptor = image.get("Descriptor") or {}
    if descriptor.get("mediaType", "").endswith("index.v1+json"):
        raise SystemExit(f"kind-load: {ref} is an image index; build it with --provenance=false")
    return image["Id"], (descriptor.get("annotations") or {}).get("config.digest") or image["Id"]


def content_ref(image: str, config: str) -> str:
    return f"{image}:tilt-{hex_digest(config)[:16]}"


# ── Placement ─────────────────────────────────────────────────────────────


def requirement_matches(requirement: dict, labels: dict[str, str], known_keys: set[str]) -> bool:
    """A NodeSelectorRequirement against one node's labels, erring toward True."""
    key, operator = requirement.get("key", ""), requirement.get("operator", "")
    values = requirement.get("values") or []
    if key not in known_keys:
        return True
    if operator == "In":
        return key in labels and labels[key] in values
    if operator == "NotIn":
        return key not in labels or labels[key] not in values
    if operator == "Exists":
        return key in labels
    if operator == "DoesNotExist":
        return key not in labels
    return True


def field_matches(requirement: dict, node: str) -> bool:
    """A matchFields requirement; Kubernetes supports only metadata.name."""
    if requirement.get("key") != "metadata.name":
        return True
    values = requirement.get("values") or []
    if requirement.get("operator") == "In":
        return node in values
    if requirement.get("operator") == "NotIn":
        return node not in values
    return True


def term_matches(term: dict, node: str, labels: dict[str, str], known_keys: set[str]) -> bool:
    return all(
        requirement_matches(r, labels, known_keys) for r in term.get("matchExpressions") or []
    ) and all(field_matches(r, node) for r in term.get("matchFields") or [])


def can_schedule(constraint: dict, node: str, labels: dict[str, str] | None, known_keys: set[str]) -> bool:
    """Whether a pod with these scheduling constraints may land on `node`."""
    if constraint.get("nodeName"):
        return constraint["nodeName"] == node
    if labels is None:
        return True
    for key, value in (constraint.get("nodeSelector") or {}).items():
        if key in known_keys and labels.get(key) != value:
            return False
    terms = constraint.get("nodeSelectorTerms") or []
    return not terms or any(term_matches(t, node, labels, known_keys) for t in terms)


def target_nodes(
    constraints: list[dict] | None, nodes: list[str], labels: dict[str, dict[str, str]] | None
) -> list[str]:
    """The nodes any of the image's pod templates can schedule onto."""
    if not constraints or labels is None:
        return list(nodes)
    known_keys = {key for node_labels in labels.values() for key in node_labels}
    return [
        node for node in nodes
        if any(can_schedule(c, node, labels.get(node), known_keys) for c in constraints)
    ]


# ── Cluster access ────────────────────────────────────────────────────────


def kind_nodes(cluster: str) -> list[str]:
    nodes = run(["kind", "get", "nodes", "--name", cluster]).split()
    if not nodes:
        raise SystemExit(f"kind-load: kind cluster {cluster} has no nodes")
    return sorted(nodes)


def node_labels(cluster: str) -> dict[str, dict[str, str]] | None:
    """Each registered node's labels, or None when they cannot be read."""
    try:
        listed = json.loads(run(["kubectl", "--context", f"kind-{cluster}", "get", "nodes", "-o", "json"]))
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        log(f"cannot read the nodes' labels ({error}); loading onto every node")
        return None
    return {
        item["metadata"]["name"]: item["metadata"].get("labels") or {}
        for item in listed.get("items") or []
    }


def holds(node: str, ref: str, config: str) -> bool:
    """Whether the node's CRI has `ref` at `config`; False when it cannot say."""
    try:
        info = json.loads(run(["docker", "exec", node, "crictl", "inspecti", "-o", "json", ref], 30))
        return hex_digest(info["status"]["id"]) == hex_digest(config)
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, TypeError):
        return False


def placement() -> list[dict] | None:
    raw = os.environ.get(PLACEMENT_ENV, "")
    try:
        constraints = json.loads(raw) if raw else None
    except ValueError:
        log(f"{PLACEMENT_ENV} is not JSON; loading onto every node")
        return None
    return constraints if isinstance(constraints, list) else None


def load(args: argparse.Namespace) -> int:
    started = time.monotonic()
    image_id, config = resolve(args.build_ref)
    ref = content_ref(args.image, config)
    run(["docker", "tag", image_id, ref])
    nodes = kind_nodes(args.cluster)
    constraints = placement()
    if not constraints:
        log(f"{args.image}: no deployed pod template runs it; loading onto every node")
    targets = target_nodes(constraints, nodes, node_labels(args.cluster))
    with ThreadPoolExecutor(max_workers=len(targets) or 1) as pool:
        present = dict(zip(targets, pool.map(lambda node: holds(node, ref, config), targets)))
    missing = [node for node in targets if not present[node]]
    skipped = [node for node in nodes if node not in targets]
    if skipped:
        log(f"{ref}: no pod running it can schedule onto {', '.join(skipped)}")
    if len(missing) < len(targets):
        log(f"{ref}: already on {', '.join(n for n in targets if present[n])}")
    if missing:
        log(f"{ref}: loading onto {', '.join(missing)}")
        load_started = time.monotonic()
        status = subprocess.run(
            ["kind", "load", "docker-image", "--name", args.cluster, "--nodes", ",".join(missing), ref]
        ).returncode
        if status:
            log(f"{ref}: kind load failed with status {status}")
            return status
        log(f"{ref}: loaded onto {len(missing)} node(s) in {time.monotonic() - load_started:.1f} s")
    path = Path(args.ref_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ref + "\n")
    # The build reference names this invocation: kind-bringup-timeline.py
    # counts each load once by it.
    log(f"{ref}: on {len(targets)} of {len(nodes)} nodes, {len(missing)} loaded, "
        f"in {time.monotonic() - started:.1f} s (build {args.build_ref})")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--image", required=True, help="the repository, e.g. space-api")
    parser.add_argument("--build-ref", required=True, help="the reference the build tagged")
    parser.add_argument("--ref-file", required=True, help="where Tilt reads the reference to deploy")
    args = parser.parse_args(argv)
    try:
        return load(args)
    except subprocess.CalledProcessError as error:
        log(f"ERROR: {' '.join(error.cmd[:6])} failed: {(error.stderr or '').strip()}")
        return 2
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        log(f"ERROR: {error}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
