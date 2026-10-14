"""The space-cluster umbrella refuses values that would install Kind defaults.

`infra/space/cluster/README.md` lists the inputs a caller's values file must
supply and the role shape it must keep. Helm merges each subchart's defaults
into the umbrella's values, so an input the caller leaves out reaches
`templates/validate.yaml` as the Kind default rather than as absent. These
checks start from `values.example.yaml`, which renders both releases, then
remove one input, or set it to its Kind value, and require the render to fail
naming that input.
"""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile

import pytest
import yaml
from kind_chart_render import HELM_BIN, REPO_ROOT

CLUSTER_DIR = REPO_ROOT / "infra/space/cluster"
CHART = CLUSTER_DIR / "chart"
# The JSON round trip gives every alias in the example its own copy, so
# removing a key from one subchart leaves the others' copies intact.
EXAMPLE = json.loads(json.dumps(yaml.safe_load((CLUSTER_DIR / "values.example.yaml").read_text())))
RELEASES = {
    "space": [],
    "space-builder": ["-f", str(CHART / "values-builder.yaml")],
}
CONTROL_PLANE_IMAGES = [
    "space-api-local",
    "space-internal-api-local",
    "space-credmgr-authority-local",
    "space-data-plane-local",
    "space-cluster-controller-local",
]
NODE_IMAGES = [
    "space-node-manager-local",
    "space-runtime-manager-local",
    "space-volume-manager-local",
    "space-credential-manager-local",
    "space-template-manager-local",
    "space-sandbox-manager-local",
    "space-node-ctl-local",
    "space-node-api-local",
]


def _render(release: str, values: dict) -> subprocess.CompletedProcess[str]:
    if HELM_BIN is None:
        raise AssertionError("helm is required to render the cluster chart")
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as handle:
        yaml.safe_dump(values, handle)
        handle.flush()
        return subprocess.run(
            [
                HELM_BIN,
                "template",
                release,
                str(CHART),
                "--namespace",
                "space",
                *RELEASES[release],
                "-f",
                handle.name,
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )


def _without(path: tuple[str, ...]) -> dict:
    values = copy.deepcopy(EXAMPLE)
    parent = values
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    return values


def _with(path: tuple[str, ...], value: object) -> dict:
    values = copy.deepcopy(EXAMPLE)
    parent = values
    for key in path[:-1]:
        parent = parent.setdefault(key, {})
    parent[path[-1]] = value
    return values


def _assert_refused(release: str, values: dict, named: str) -> None:
    result = _render(release, values)
    assert result.returncode != 0, f"{release} rendered without {named}"
    assert named in result.stderr, result.stderr


@pytest.mark.parametrize("release", RELEASES)
def test_example_values_render(release: str) -> None:
    result = _render(release, copy.deepcopy(EXAMPLE))
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("chart", CONTROL_PLANE_IMAGES + NODE_IMAGES)
def test_missing_image_repository_is_refused(chart: str) -> None:
    # Every subchart's own image.repository is a Kind name, so the key reads
    # as present after Helm's merge: presence alone would not catch this.
    _assert_refused("space", _without((chart, "image", "repository")), chart)


@pytest.mark.parametrize(
    "repository", ["space-api", "space-data-plane", "space-cluster-controller", "space-worker"]
)
def test_kind_image_repository_is_refused(repository: str) -> None:
    values = _with(("space-node-api-local", "image"), {"repository": repository, "tag": "1.2.3"})
    _assert_refused("space", values, "space-node-api-local.image.repository")


@pytest.mark.parametrize(
    "path",
    [
        ("space-api-local", "clusterId"),
        ("space-api-local", "postgres", "rolesSecretName"),
        ("space-internal-api-local", "postgres", "rolesSecretName"),
        ("space-cluster-controller-local", "postgres", "rolesSecretName"),
        ("space-credmgr-authority-local", "postgres", "rolesSecretName"),
        ("space-credmgr-authority-local", "keyBroker", "url"),
        ("space-credmgr-authority-local", "keyBroker", "caSecretName"),
        ("space-credmgr-authority-local", "keyBroker", "tokenSecretName"),
        ("space-credmgr-authority-local", "keyBroker", "tenantNamespace"),
        ("space-credmgr-authority-local", "keyBroker", "grantScheme"),
        ("space-cluster-controller-local", "snapshotEndpoint"),
        ("space-sandbox-manager-local", "snapshots", "endpoint"),
        ("space-node-manager-local", "node", "objectStore", "endpoint"),
    ],
    ids=".".join,
)
def test_missing_required_input_is_refused(path: tuple[str, ...]) -> None:
    _assert_refused("space", _without(path), ".".join(path))


def test_missing_image_tag_is_refused() -> None:
    _assert_refused("space", _without(("space-api-local", "image", "tag")), "space-api-local.image")


@pytest.mark.parametrize(
    ("release", "switch", "value"),
    [
        ("space", "controlPlane", False),
        ("space", "nodeServices", False),
        ("space", "buildManager", True),
        ("space-builder", "controlPlane", True),
        ("space-builder", "nodeServices", False),
        ("space-builder", "buildManager", False),
    ],
)
def test_component_switch_change_is_refused(release: str, switch: str, value: bool) -> None:
    # The shared values file feeds both releases, so a flipped switch would
    # otherwise prune components from a running install on the next upgrade.
    _assert_refused(release, _with((switch, "enabled"), value), f"{switch}.enabled")


def test_mode_off_needs_no_telemetry_identity() -> None:
    values = copy.deepcopy(EXAMPLE)
    del values["global"]["observability"]["environment"]
    del values["global"]["observability"]["cluster"]
    assert _render("space", values).returncode == 0
    values["global"]["observability"]["mode"] = "datadog"
    _assert_refused("space", values, "global.observability.environment")
