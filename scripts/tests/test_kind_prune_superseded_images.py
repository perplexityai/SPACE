"""Which images the Kind superseded-image prune removes, against fake node output.

Never touches a cluster: `crictl`, `kubectl`, `kind` and `tilt` answers are
canned, in the shapes those tools print.
"""

import copy
import importlib.util
import json
import pathlib
import re
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
KIND_DIR = ROOT / "infra/space/kind"
SCRIPT = KIND_DIR / "scripts/prune-superseded-images.py"
spec = importlib.util.spec_from_file_location("kind_prune_superseded_images", SCRIPT)
prune = importlib.util.module_from_spec(spec)
# Registered first: its dataclasses resolve annotations through sys.modules.
sys.modules[spec.name] = prune
spec.loader.exec_module(prune)

NAMES = subprocess.run(
    ["bash", str(KIND_DIR / "lib/space-images.sh")],
    check=True,
    capture_output=True,
    text=True,
).stdout.split()
REPOS = prune.space_repositories(NAMES)


def image_id(n: int) -> str:
    return f"sha256:{n:064x}"


# One node's `crictl images -o json`, after a few rebuilds. The titles stand in
# for the label `crictl inspecti` reports on each untagged image.
CRICTL_IMAGES = {
    "images": [
        # The api build `:local` names now, and the one it named before.
        {
            "id": image_id(1),
            "repoTags": ["docker.io/library/space-api:local"],
            "repoDigests": [],
            "size": "140000000",
        },
        {"id": image_id(2), "repoTags": [], "repoDigests": [], "size": "140000000"},
        # Tilt builds of the worker: the deployed tag and two older ones.
        {
            "id": image_id(3),
            "repoTags": ["docker.io/library/space-worker:tilt-0000000000000003"],
            "repoDigests": [],
            "size": "360000000",
        },
        {
            "id": image_id(4),
            "repoTags": ["docker.io/library/space-worker:tilt-0000000000000004"],
            "repoDigests": [],
            "size": "360000000",
        },
        {
            "id": image_id(5),
            "repoTags": ["docker.io/library/space-worker:tilt-0000000000000005"],
            "repoDigests": [],
            "size": "360000000",
        },
        # An old seed publisher that a container still uses.
        {"id": image_id(6), "repoTags": [], "repoDigests": [], "size": "215000000"},
        # The seed publisher `:local` names: its Job finished, so nothing runs
        # it. `crictl rmi --prune` removes exactly this one.
        {
            "id": image_id(7),
            "repoTags": ["docker.io/library/space-seed-publish:local"],
            "repoDigests": [],
            "size": "215000000",
        },
        # Third-party: tagged, digest-only, untagged with another title, and
        # untagged with no title at all.
        {
            "id": image_id(8),
            "repoTags": ["docker.io/library/postgres:16-alpine"],
            "repoDigests": ["docker.io/library/postgres@sha256:" + "a" * 64],
            "size": "100000000",
        },
        {
            "id": image_id(9),
            "repoTags": [],
            "repoDigests": ["docker.io/grafana/grafana@sha256:" + "b" * 64],
            "size": "470000000",
        },
        {"id": image_id(10), "repoTags": [], "repoDigests": [], "size": "50000000"},
        {"id": image_id(11), "repoTags": [], "repoDigests": [], "size": "50000000"},
        # A SPACE-named repository under another registry is not SPACE's.
        {
            "id": image_id(12),
            "repoTags": ["ghcr.io/example/space-api:local"],
            "repoDigests": [],
            "size": "140000000",
        },
        # One image carrying a SPACE tag and a third-party one.
        {
            "id": image_id(13),
            "repoTags": [
                "docker.io/library/space-ui:tilt-0000000000000013",
                "registry.example.com/ui:1",
            ],
            "repoDigests": [],
            "size": "97000000",
        },
        # A UI build only the Tilt session's image map names: built and
        # loaded, not deployed yet.
        {
            "id": image_id(14),
            "repoTags": ["docker.io/library/space-ui:tilt-0000000000000014"],
            "repoDigests": [],
            "size": "97000000",
        },
        # An older UI build by tag and one untagged.
        {
            "id": image_id(15),
            "repoTags": ["docker.io/library/space-ui:tilt-0000000000000015"],
            "repoDigests": [],
            "size": "97000000",
        },
        {"id": image_id(16), "repoTags": [], "repoDigests": [], "size": "97000000"},
        # A retired SPACE repository the harness no longer builds.
        {
            "id": image_id(17),
            "repoTags": ["docker.io/library/space-node-stack:tilt-0000000000000017"],
            "repoDigests": [],
            "size": "400000000",
        },
    ]
}
TITLES = {
    image_id(2): "space-api",
    image_id(6): "space-seed-publish",
    image_id(10): "Grafana",
    image_id(16): "space-ui",
}

CRICTL_PS = {
    "containers": [
        {
            "imageRef": image_id(6),
            "image": {"image": image_id(6), "userSpecifiedImage": ""},
            "state": "CONTAINER_EXITED",
        },
        {
            "imageRef": image_id(8),
            "image": {"image": image_id(8), "userSpecifiedImage": "postgres:16-alpine"},
            "state": "CONTAINER_RUNNING",
        },
    ]
}


def pod_template(*images, env=()):
    return {
        "spec": {
            "containers": [
                {"image": image, "env": [{"name": k, "value": v} for k, v in env]}
                for image in images
            ]
        }
    }


KUBECTL_WORKLOADS = {
    "items": [
        {
            "kind": "DaemonSet",
            "spec": {"template": pod_template("space-worker:tilt-0000000000000003")},
        },
        # Rollback history: a scaled-to-zero ReplicaSet is not current.
        {
            "kind": "ReplicaSet",
            "spec": {
                "replicas": 0,
                "template": pod_template("space-worker:tilt-0000000000000004"),
            },
            "status": {"replicas": 0},
        },
        {
            "kind": "Deployment",
            "spec": {
                "template": pod_template(
                    "space-api:local",
                    env=[("SPACE_SANDBOX_POD_IMAGE", "registry.k8s.io/pause:3.10")],
                )
            },
        },
        {
            "kind": "Pod",
            "spec": pod_template("docker.io/library/space-ui:tilt-0000000000000013")[
                "spec"
            ],
        },
    ]
}


def node_images():
    return [
        prune.Image(image.id, image.refs, image.size, TITLES.get(image.id))
        for image in prune.parse_images(CRICTL_IMAGES)
    ]


def current_refs(*extra):
    refs = {prune.normalize(f"{name}:local") for name in NAMES}
    refs |= prune.workload_refs(KUBECTL_WORKLOADS)
    return refs | {prune.normalize(ref) for ref in extra}


def selected(refs):
    in_use_ids, in_use_refs = prune.containers_in_use(CRICTL_PS)
    doomed = prune.superseded(node_images(), REPOS, refs | in_use_refs, in_use_ids)
    return {image.id for image in doomed}


def test_superseded_selection():
    removed = selected(current_refs("space-ui:tilt-0000000000000014"))
    # Older SPACE builds go: the untagged api, the undeployed worker tags (one
    # only rollback history names), and both older UI builds.
    assert removed == {image_id(n) for n in (2, 4, 5, 15, 16)}


@pytest.mark.parametrize(
    "kept, why",
    [
        (1, "the :local tag build-images.sh loads"),
        (3, "the tag a DaemonSet deploys"),
        (6, "untagged, but a container uses it"),
        (7, "the seed publisher no container runs right now"),
        (8, "third-party"),
        (9, "third-party, digest only"),
        (10, "untagged with a third-party title"),
        (11, "untagged with no title"),
        (12, "another registry's repository"),
        (13, "a SPACE tag shared with a third-party one"),
        (14, "the build Tilt has not deployed yet"),
        (17, "a repository the harness no longer builds"),
    ],
)
def test_kept(kept, why):
    assert image_id(kept) not in selected(
        current_refs("space-ui:tilt-0000000000000014")
    ), why


def test_without_the_tilt_session_its_undeployed_build_is_superseded():
    # Why the Tilt image map is read: the cluster alone cannot see a build
    # that is loaded but not rolled out.
    assert image_id(14) in selected(current_refs())


def test_selection_is_idempotent():
    refs = current_refs("space-ui:tilt-0000000000000014")
    first = selected(refs)
    remaining = [image for image in node_images() if image.id not in first]
    in_use_ids, in_use_refs = prune.containers_in_use(CRICTL_PS)
    assert prune.superseded(remaining, REPOS, refs | in_use_refs, in_use_ids) == []


@pytest.mark.parametrize(
    "ref, normalized",
    [
        ("space-api:local", "docker.io/library/space-api:local"),
        ("space-api", "docker.io/library/space-api:latest"),
        ("docker.io/library/space-api:tilt-1", "docker.io/library/space-api:tilt-1"),
        ("grafana/grafana:13", "docker.io/grafana/grafana:13"),
        ("localhost:5000/space-api:x", "localhost:5000/space-api:x"),
        ("registry.k8s.io/pause:3.10", "registry.k8s.io/pause:3.10"),
        (
            "space-api@sha256:" + "c" * 64,
            "docker.io/library/space-api@sha256:" + "c" * 64,
        ),
        ("sha256:" + "d" * 64, None),
        ("", None),
    ],
)
def test_normalize(ref, normalized):
    assert prune.normalize(ref) == normalized


def test_workload_refs_reach_every_pod_template():
    refs = prune.workload_refs(
        {
            "items": [
                {
                    "kind": "CronJob",
                    "spec": {
                        "jobTemplate": {
                            "spec": {"template": pod_template("space-api:cron")}
                        }
                    },
                },
                {
                    "kind": "Job",
                    "spec": {
                        "template": {
                            "spec": {
                                "initContainers": [{"image": "space-worker:init"}],
                                "containers": [],
                            }
                        }
                    },
                },
                {
                    "kind": "ReplicaSet",
                    "spec": {"replicas": 0, "template": pod_template("space-api:old")},
                    "status": {"replicas": 1},
                },
                {
                    "kind": "Deployment",
                    "spec": {
                        "template": pod_template(
                            "busybox", env=[("BUILDER_IMAGE", "space-worker:env")]
                        )
                    },
                },
                {
                    "kind": "Pod",
                    "spec": {
                        "containers": [],
                        "ephemeralContainers": [{"image": "space-worker:debug"}],
                    },
                },
            ]
        }
    )
    assert {
        "docker.io/library/space-api:cron",
        "docker.io/library/space-worker:init",
        "docker.io/library/space-api:old",
        "docker.io/library/space-worker:env",
        "docker.io/library/space-worker:debug",
    } <= refs


# ── One pass, with every command answered by a fake ─────────────────────────

NODES = ["space-local-control-plane", "space-local-worker"]


class FakeCommands:
    def __init__(self, tilt=None, kubectl_fails=False, after_rmi=None):
        self.tilt = tilt
        self.kubectl_fails = kubectl_fails
        self.after_rmi = after_rmi
        self.ps = copy.deepcopy(CRICTL_PS)
        self.removed = {node: [] for node in NODES}
        self.image_lists = 0

    def __call__(self, args, timeout=60):
        match args:
            case ["bash", _]:
                return "\n".join(NAMES)
            case ["kind", "get", "nodes", "--name", "space-local"]:
                return "\n".join(NODES)
            case ["kubectl", "--context", "kind-space-local", "get", *_]:
                if self.kubectl_fails:
                    raise prune.PruneError("kubectl: connection refused")
                return json.dumps(KUBECTL_WORKLOADS)
            case ["tilt", "get", what, *_]:
                if self.tilt is None:
                    raise prune.PruneError("tilt: connection refused")
                return json.dumps(self.tilt[what])
            case ["docker", "exec", node, "crictl", "images", "-o", "json"]:
                self.image_lists += 1
                gone = set(self.removed[node])
                images = [i for i in CRICTL_IMAGES["images"] if i["id"] not in gone]
                return json.dumps({"images": images})
            case ["docker", "exec", _, "crictl", "ps", "-a", "-o", "json"]:
                return json.dumps(self.ps)
            case ["docker", "exec", _, "crictl", "inspecti", "-o", "json", image]:
                labels = {prune.TITLE_LABEL: TITLES[image]} if image in TITLES else None
                return json.dumps(
                    {"info": {"imageSpec": {"config": {"Labels": labels}}}}
                )
            case ["docker", "exec", node, "crictl", "rmi", image]:
                self.removed[node].append(image)
                if self.after_rmi:
                    self.after_rmi(self)
                return ""
        raise AssertionError(f"unexpected command: {args}")


def tilt_state(busy=(), images=("space-ui:tilt-0000000000000014",)):
    return {
        "cluster": {
            "status": {"connection": {"kubernetes": {"context": "kind-space-local"}}}
        },
        "uiresources": {
            "items": [
                {
                    "metadata": {"name": name},
                    "status": {"updateStatus": "in_progress" if name in busy else "ok"},
                }
                for name in ("space-api", "space-ui", "prune-superseded-images")
            ]
        },
        "imagemaps": {"items": [{"status": {"image": image}} for image in images]},
    }


def pass_with(monkeypatch, fake, *argv):
    monkeypatch.setattr(prune, "run", fake)
    monkeypatch.setattr(prune.shutil, "which", lambda _: "/usr/bin/tilt")
    return prune.main(["--cluster", "space-local", *argv])


def test_pass_removes_on_every_node_and_reports_space(monkeypatch, capsys):
    fake = FakeCommands(tilt=tilt_state())
    assert pass_with(monkeypatch, fake) == 0
    for node in NODES:
        assert set(fake.removed[node]) == {image_id(n) for n in (2, 4, 5, 15, 16)}
    output = capsys.readouterr().out
    assert (
        "space-local-worker: removed 5 superseded image(s), "
        "1005.2 MiB of image content:" in output
    )
    assert "space-worker:tilt-0000000000000004" in output
    assert "space-api (untagged " in output
    assert "removed 10 superseded SPACE image(s) in total" in output

    # A second pass finds nothing and says nothing.
    assert pass_with(monkeypatch, fake) == 0
    assert capsys.readouterr().out == ""


def test_dry_run_removes_nothing(monkeypatch, capsys):
    fake = FakeCommands()
    assert pass_with(monkeypatch, fake, "--dry-run") == 0
    assert fake.removed == {node: [] for node in NODES}
    assert "would remove 6 superseded image(s)" in capsys.readouterr().out


def test_busy_tilt_stands_down(monkeypatch, capsys):
    fake = FakeCommands(tilt=tilt_state(busy=("space-api",)))
    assert pass_with(monkeypatch, fake) == 0
    assert fake.removed == {node: [] for node in NODES}
    assert "Tilt is updating space-api" in capsys.readouterr().out


def test_own_resource_does_not_count_as_busy(monkeypatch):
    fake = FakeCommands(tilt=tilt_state(busy=("prune-superseded-images",)))
    assert (
        pass_with(monkeypatch, fake, "--tilt-resource", "prune-superseded-images") == 0
    )
    assert fake.removed[NODES[0]]


def test_unreadable_cluster_removes_nothing(monkeypatch, capsys):
    fake = FakeCommands(kubectl_fails=True)
    assert pass_with(monkeypatch, fake) == 1
    assert fake.removed == {node: [] for node in NODES}
    assert "connection refused" in capsys.readouterr().out


def test_opt_out(monkeypatch, capsys):
    monkeypatch.setenv(prune.OPT_OUT, "0")
    fake = FakeCommands()
    assert pass_with(monkeypatch, fake) == 0
    assert fake.removed == {node: [] for node in NODES}
    assert f"{prune.OPT_OUT}=0" in capsys.readouterr().out


# ── The image list matches what both bring-up paths build ───────────────────


def test_tiltfile_builds_exactly_the_listed_images():
    tiltfile = (ROOT / "Tiltfile").read_text()
    built = set(re.findall(r'ref = "([a-z0-9-]+):local"', tiltfile))
    built |= set(re.findall(r'rust_runtime_image\(\s*"([a-z0-9-]+)"', tiltfile))
    assert built == set(NAMES)


def test_each_listed_image_carries_its_title_label():
    # The only way an untagged build is recognized as one of these images.
    build_images = (KIND_DIR / "scripts/build-images.sh").read_text()
    titles = set()
    for dockerfile in set(re.findall(r"docker/Dockerfile\.[a-z-]+", build_images)):
        titles |= set(
            re.findall(
                r'LABEL org\.opencontainers\.image\.title="([^"]+)"',
                (ROOT / dockerfile).read_text(),
            )
        )
    assert set(NAMES) <= titles


def test_watch_passes_only_when_space_references_change(monkeypatch):
    fake = FakeCommands(tilt=tilt_state())
    monkeypatch.setattr(prune, "run", fake)
    monkeypatch.setattr(prune.shutil, "which", lambda _: "/usr/bin/tilt")
    pruner = prune.Pruner("space-local", "10350", None, True, False)
    first = pruner.fingerprint()
    # A new sandbox pod on the pause image is not a reason for a pass.
    sandbox = {
        "kind": "Pod",
        "spec": pod_template("registry.k8s.io/pause:3.10")["spec"],
    }
    monkeypatch.setitem(
        KUBECTL_WORKLOADS, "items", KUBECTL_WORKLOADS["items"] + [sandbox]
    )
    assert pruner.fingerprint() == first
    # A rollout to a new worker build is.
    rollout = {
        "kind": "Pod",
        "spec": pod_template("space-worker:tilt-0000000000000099")["spec"],
    }
    monkeypatch.setitem(
        KUBECTL_WORKLOADS, "items", KUBECTL_WORKLOADS["items"] + [rollout]
    )
    assert pruner.fingerprint() != first
    # And nothing is decided while Tilt is updating.
    fake.tilt = tilt_state(busy=("space-ui",))
    assert pruner.fingerprint() is None


def test_tilt_turning_busy_between_removals_stops_the_pass(monkeypatch, capsys):
    # The first removal starts a Tilt update; nothing after it may go, on this
    # node or the next.
    def start_update(fake):
        fake.tilt = tilt_state(busy=("space-api",))

    fake = FakeCommands(tilt=tilt_state(), after_rmi=start_update)
    assert pass_with(monkeypatch, fake) == 1
    assert sum(len(images) for images in fake.removed.values()) == 1
    output = capsys.readouterr().out
    # What did go is still reported.
    assert f"{NODES[0]}: removed 1 superseded image(s)" in output
    assert "Tilt started an update" in output


def test_tilt_building_a_new_image_between_removals_stops_the_pass(monkeypatch):
    # Not busy any more, but its image map names a build this pass never saw.
    def new_build(fake):
        fake.tilt = tilt_state(
            images=("space-ui:tilt-0000000000000014", "space-api:tilt-new")
        )

    fake = FakeCommands(tilt=tilt_state(), after_rmi=new_build)
    assert pass_with(monkeypatch, fake) == 1
    assert sum(len(images) for images in fake.removed.values()) == 1


def test_watch_retries_an_image_a_container_held(monkeypatch):
    fake = FakeCommands(tilt=tilt_state())
    monkeypatch.setattr(prune, "run", fake)
    monkeypatch.setattr(prune.shutil, "which", lambda _: "/usr/bin/tilt")
    pruner = prune.Pruner("space-local", "10350", None, True, False)
    state = prune.WatchState()
    held = image_id(6)  # untagged seed publisher an exited container still uses

    pruner.tick(state)
    assert all(held not in images for images in fake.removed.values())
    assert state.left_behind

    # The container goes; no reference changes, so the fingerprint cannot see it.
    fake.ps = {"containers": []}
    for _ in range(prune.RETRY_TICKS - 1):
        pruner.tick(state)
        assert all(held not in images for images in fake.removed.values())
    pruner.tick(state)
    assert all(held in images for images in fake.removed.values())
    assert not state.left_behind

    # Nothing left behind: idle ticks read no node.
    lists = fake.image_lists
    for _ in range(3 * prune.RETRY_TICKS):
        pruner.tick(state)
    assert fake.image_lists == lists


@pytest.mark.parametrize("seconds", ["0", "-5"])
def test_watch_interval_must_be_positive(seconds, capsys, monkeypatch):
    def never(*_):
        raise AssertionError("the watcher started")

    monkeypatch.setattr(prune.Pruner, "watch", never)
    with pytest.raises(SystemExit) as exit:
        prune.main(["--watch", seconds])
    assert exit.value.code == 2
    assert "must be at least 1 second" in capsys.readouterr().err
