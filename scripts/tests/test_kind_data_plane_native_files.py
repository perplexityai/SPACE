"""Kind serves Native files from the data plane, as docs/architecture.md says.

The data plane has the API's activation listener decide each request and
sends the sandbox call to the runtime the API grants: the listener must admit
its ServiceAccount, and sandbox pods must accept its calls. Kind's values
admit it to both; the charts' defaults and the caller-provided-cluster chart,
which leave Native files to the API, admit it to neither. Its drain lets a
files request it already took finish: a files request can take its upload
time before it wakes its sandbox, wait up to 135 s for the wake (the API's
blocking wait budget) and then transfer its body, and the drain deadline
(`--shutdown-grace-secs`) cancels whatever is still running. With Native files
on, the chart refuses a deadline shorter than all three together.
"""

import subprocess
import tempfile
from urllib.parse import urlsplit

import pytest
import yaml

from kind_chart_render import API_CHART, KIND_DIR, REPO_ROOT, env_by_name, object_named, render

DATA_PLANE_CHART = KIND_DIR / "charts/space-data-plane-local"
NETWORK_POLICY_CHART = KIND_DIR / "charts/space-sandbox-network-policy"
# The values files the Tiltfile passes after each chart's values.yaml.
API_KIND_VALUES = API_CHART / "values-kind.yaml"
NETWORK_POLICY_KIND_VALUES = NETWORK_POLICY_CHART / "values-kind.yaml"


def _admitted(manifests: list[dict]) -> list[str]:
    """The ServiceAccounts the API's activation listener admits."""
    env = env_by_name(object_named(manifests, "Deployment", "space-api"))
    return env["SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS"]["value"].split(",")


# render() installs every chart in this namespace, as the Tiltfile does, so
# the data plane's pods run here.
NAMESPACE = "space"


def _sandbox_ingress(manifests: list[dict]) -> list[tuple[str | None, str, list[dict]]]:
    """Each component sandbox pods accept: the namespace its peer selects
    (None when the peer selects none), its name and its ports."""
    policy = object_named(manifests, "NetworkPolicy", "space-sandbox-allow")
    return [
        (
            peer.get("namespaceSelector", {}).get("matchLabels", {}).get("kubernetes.io/metadata.name"),
            peer["podSelector"]["matchLabels"]["app.kubernetes.io/name"],
            rule["ports"],
        )
        for rule in policy["spec"]["ingress"]
        for peer in rule["from"]
    ]


def _shutdown_grace(*values: str) -> str:
    deployment = object_named(
        render(DATA_PLANE_CHART, "space", *values), "Deployment", "space-data-plane"
    )
    args = deployment["spec"]["template"]["spec"]["containers"][0]["args"]
    return args[args.index("--shutdown-grace-secs") + 1]


def test_native_files_render_with_a_drain_that_covers_an_upload_a_wake_and_its_transfer():
    assert _shutdown_grace() == "175"
    assert (
        _shutdown_grace(
            "--set",
            "nativeFiles.enabled=true",
            "--set",
            "nativeFiles.uploadSeconds=40",
            "--set",
            "nativeFiles.transferSeconds=30",
            "--set",
            "shutdownGraceSeconds=205",
            "--set",
            "terminationGracePeriodSeconds=230",
        )
        == "205"
    )


@pytest.mark.parametrize(
    "values",
    [
        ("--set", "shutdownGraceSeconds=174"),
        ("--set", "nativeFiles.uploadSeconds=26"),
        ("--set", "nativeFiles.transferSeconds=16"),
    ],
)
def test_native_files_refuse_a_drain_shorter_than_an_upload_a_wake_and_its_transfer(values):
    with pytest.raises(subprocess.CalledProcessError) as refused:
        render(DATA_PLANE_CHART, "space", "--set", "nativeFiles.enabled=true", *values)
    assert "shutdownGraceSeconds must cover nativeFiles.uploadSeconds" in refused.value.stderr


def test_without_native_files_the_drain_may_be_shorter():
    assert _shutdown_grace("--set", "nativeFiles.enabled=false", "--set", "shutdownGraceSeconds=60") == "60"


def test_kind_serves_native_files_decided_by_the_apis_listener():
    deployment = object_named(render(DATA_PLANE_CHART, "space"), "Deployment", "space-data-plane")
    env = env_by_name(deployment)
    assert env["SPACE_DATA_PLANE_NATIVE_FILES"]["value"] == "serve"
    # The listener's Service and port, over the internal RPC TLS its leaf is
    # issued under, with the projected token it reviews.
    endpoint = urlsplit(env["SPACE_INGRESS_ACTIVATION_ENDPOINT"]["value"])
    assert (endpoint.scheme, endpoint.hostname, endpoint.port) == ("https", "space-api", 9801)
    assert "SPACE_INTERNAL_RPC_INSECURE_DEV" not in env
    assert env["SPACE_INTERNAL_RPC_TLS_CA"]["value"] == "/etc/space-rpc-ca/ca.crt"
    token_path = env["SPACE_CLUSTER_API_TOKEN_PATH"]["value"]
    volumes = {volume["name"]: volume for volume in deployment["spec"]["template"]["spec"]["volumes"]}
    mounts = {
        mount["mountPath"]: mount["name"]
        for mount in deployment["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    }
    directory, filename = token_path.rsplit("/", 1)
    token = volumes[mounts[directory]]["projected"]["sources"][0]["serviceAccountToken"]
    assert (token["path"], token["audience"]) == (filename, "space-cluster-api")
    # The listener admits the data plane's ServiceAccount.
    admitted = _admitted(render(API_CHART, "space", "-f", str(API_KIND_VALUES)))
    assert admitted == ["space-internal-api", "space-data-plane"]
    assert deployment["spec"]["template"]["spec"]["serviceAccountName"] in admitted


def _mebibytes(quantity: str) -> int:
    assert quantity.endswith("Mi"), quantity
    return int(quantity[:-2])


def test_kind_bounds_native_writes_to_the_data_planes_memory():
    deployment = object_named(render(DATA_PLANE_CHART, "space"), "Deployment", "space-data-plane")
    env = env_by_name(deployment)
    assert env["SPACE_DATA_PLANE_NATIVE_UPLOAD_ADMISSION"]["value"] == "true"
    # A write's peak memory is about twice the bytes it reserves, so twice
    # each budget must leave room in the pod's limit for everything else.
    limit = _mebibytes(deployment["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["memory"])
    for name in (
        "SPACE_DATA_PLANE_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES",
        "SPACE_DATA_PLANE_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_ORG",
        "SPACE_DATA_PLANE_MAX_IN_FLIGHT_NATIVE_UPLOAD_BYTES_PER_SANDBOX",
    ):
        assert 2 * int(env[name]["value"]) <= limit * 2**20 // 2, name


def test_off_leaves_the_data_plane_without_native_files():
    deployment = object_named(
        render(DATA_PLANE_CHART, "space", "--set", "nativeFiles.enabled=false"),
        "Deployment",
        "space-data-plane",
    )
    env = env_by_name(deployment)
    assert "SPACE_DATA_PLANE_NATIVE_FILES" not in env
    assert "SPACE_INGRESS_ACTIVATION_ENDPOINT" not in env
    assert "SPACE_DATA_PLANE_NATIVE_UPLOAD_ADMISSION" not in env


def test_kinds_sandbox_pods_accept_the_data_planes_calls_to_their_runtime():
    sources = _sandbox_ingress(
        render(NETWORK_POLICY_CHART, "space", "-f", str(NETWORK_POLICY_KIND_VALUES))
    )
    assert (NAMESPACE, "space-data-plane", [{"protocol": "TCP", "port": 9800}]) in sources


def test_by_default_the_charts_admit_the_data_plane_to_neither():
    assert _admitted(render(API_CHART, "space")) == ["space-internal-api"]
    sources = _sandbox_ingress(render(NETWORK_POLICY_CHART, "space"))
    assert {(namespace, name) for namespace, name, _ in sources} == {
        (NAMESPACE, "space-api"),
        (NAMESPACE, "space-cluster-controller"),
    }


def test_the_cluster_chart_leaves_native_files_to_the_api():
    cluster = REPO_ROOT / "infra/space/cluster"
    values = yaml.safe_load((cluster / "values.example.yaml").read_text())
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as handle:
        yaml.safe_dump(values, handle)
        handle.flush()
        manifests = render(cluster / "chart", "space", "-f", handle.name)
    env = env_by_name(object_named(manifests, "Deployment", "space-data-plane"))
    assert "SPACE_DATA_PLANE_NATIVE_FILES" not in env
    # So neither the activation listener nor sandbox pods admit it.
    assert _admitted(manifests) == ["space-internal-api"]
    assert "space-data-plane" not in [name for _, name, _ in _sandbox_ingress(manifests)]
