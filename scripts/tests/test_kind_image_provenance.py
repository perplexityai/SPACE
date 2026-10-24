"""What the Kind image-provenance check refuses, against canned cluster output.

Never touches a cluster: `kubectl` and `crictl` answers are canned in the
shapes those tools print, and build records live in a temporary state dir.
"""

import copy
import importlib.util
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "infra/space/kind/scripts/image-provenance.py"
spec = importlib.util.spec_from_file_location("kind_image_provenance", SCRIPT)
provenance = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = provenance
spec.loader.exec_module(provenance)

CLUSTER = "space-local"
COMMIT = "c" * 40
LATEST = f"sha256:{1:064x}"
OLDER = f"sha256:{2:064x}"
API_REF = "docker.io/library/space-api:tilt-0000000000000001"


def api_pod(name: str, template: str, *, image_id: str | None, waiting: str | None = None) -> dict:
    """A space-api pod: started on `image_id`, or waiting for `waiting`."""
    status: dict = {"name": "api", "imageID": image_id or ""}
    if image_id:
        status["state"] = {"running": {}}
    elif waiting:
        status["state"] = {"waiting": {"reason": waiting}}
    else:
        status["state"] = {"waiting": {}}
    return {
        "metadata": {
            "name": f"space-api-{template}-{name}",
            "labels": {"pod-template-hash": template},
            "ownerReferences": [
                {"controller": True, "kind": "ReplicaSet", "name": f"space-api-{template}"}
            ],
        },
        "spec": {"nodeName": "space-local-worker3",
                 "containers": [{"name": "api", "image": API_REF}]},
        "status": {"phase": "Running" if image_id else "Pending",
                   "containerStatuses": [status]},
    }


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    """Point the check at a canned cluster; returns the pod list to fill in."""
    monkeypatch.setattr(provenance, "PROVENANCE_DIR", tmp_path / "provenance")
    provenance.write_json(
        tmp_path / "provenance" / CLUSTER / "space-api.json",
        {"config_digest": LATEST, "identity": COMMIT},
    )
    monkeypatch.setattr(provenance, "local_image_names", lambda: ["space-api"])
    monkeypatch.setattr(provenance, "checkout_commit", lambda: COMMIT)
    monkeypatch.setattr(provenance, "worktree_status", lambda: "")
    pods: list[dict] = []

    def run(args, timeout=60):
        if args[0] == "kubectl":
            return json.dumps({"items": copy.deepcopy(pods)})
        if args[:4] == ["docker", "exec", "space-local-worker3", "crictl"]:
            return json.dumps({"info": {"imageSpec": {"config": {
                "Labels": {provenance.REVISION_LABEL: COMMIT}}}}})
        raise AssertionError(f"unexpected command {args}")

    monkeypatch.setattr(provenance, "run", run)
    return pods


def check(tmp_path) -> tuple[int, dict]:
    output = tmp_path / "provenance.json"
    code = provenance.check(provenance.argparse.Namespace(cluster=CLUSTER, output=str(output)))
    return code, json.loads(output.read_text())


def test_scaled_up_replica_of_a_proven_template_is_not_refused(cluster, tmp_path):
    # The autoscaler added a third replica the second before the check ran.
    cluster += [
        api_pod("a", "5d8f7c9b6a", image_id=LATEST),
        api_pod("b", "5d8f7c9b6a", image_id=LATEST),
        api_pod("c", "5d8f7c9b6a", image_id=None, waiting="ContainerCreating"),
    ]
    code, report = check(tmp_path)
    assert code == 0
    assert report["verdict"] == "latest"
    assert [c["verdict"] for c in report["containers"]] == ["latest", "latest", "starting"]


@pytest.mark.parametrize("waiting", ["ErrImagePull", "ImagePullBackOff", "CreateContainerConfigError"])
def test_replica_that_cannot_start_is_still_refused(cluster, tmp_path, waiting):
    cluster += [
        api_pod("a", "5d8f7c9b6a", image_id=LATEST),
        api_pod("c", "5d8f7c9b6a", image_id=None, waiting=waiting),
    ]
    code, report = check(tmp_path)
    assert code == 1
    assert report["containers"][1]["verdict"] == "not started"


def test_new_template_with_no_started_pod_is_still_refused(cluster, tmp_path):
    # A rollout's first pod has no started sibling on its template to vouch for it.
    cluster += [
        api_pod("a", "5d8f7c9b6a", image_id=LATEST),
        api_pod("n", "7b4c6d8e9f", image_id=None, waiting="ContainerCreating"),
    ]
    code, report = check(tmp_path)
    assert code == 1
    assert report["containers"][1]["verdict"] == "not started"


def test_creating_replica_beside_a_stale_sibling_is_still_refused(cluster, tmp_path):
    cluster += [
        api_pod("a", "5d8f7c9b6a", image_id=OLDER),
        api_pod("c", "5d8f7c9b6a", image_id=None, waiting="ContainerCreating"),
    ]
    code, report = check(tmp_path)
    assert code == 1
    assert [c["verdict"] for c in report["containers"]] == ["stale image", "not started"]
