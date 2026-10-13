"""Kind's ingress activation delegation follows docs/internal-rpc-tls.md.

internal-api delegates cold wakes to the api's IngressActivationApi listener
("Ingress activation delegation"). These checks render both charts and assert
that the listener serves the CA-issued identity naming the Service internal-api
dials, that internal-api dials it over https and presents a projected
space-cluster-api token, and that the listener admits only internal-api by
TokenReview without static fallback.
"""

from urllib.parse import urlsplit

from kind_chart_render import API_CHART, KIND_DIR, env_by_name, object_named, render

INTERNAL_API_CHART = KIND_DIR / "charts/space-internal-api-local"
RPC_TLS_SCRIPT = KIND_DIR / "scripts/internal-rpc-tls.sh"


def _volumes(workload: dict) -> dict[str, dict]:
    return {volume["name"]: volume for volume in workload["spec"]["template"]["spec"]["volumes"]}


def _mounts(workload: dict) -> dict[str, str]:
    container = workload["spec"]["template"]["spec"]["containers"][0]
    return {mount["mountPath"]: mount["name"] for mount in container["volumeMounts"]}


def _mounted_secret(workload: dict, path: str) -> str:
    """The Secret mounted at the directory holding `path`."""
    directory = path.rsplit("/", 1)[0]
    return _volumes(workload)[_mounts(workload)[directory]]["secret"]["secretName"]


def test_internal_api_dials_the_listener_over_tls_with_a_cluster_api_token():
    deployment = object_named(render(INTERNAL_API_CHART, "space"), "Deployment", "space-internal-api")
    env = env_by_name(deployment)
    endpoint = urlsplit(env["SPACE_INGRESS_ACTIVATION_ENDPOINT"]["value"])
    assert (endpoint.scheme, endpoint.hostname, endpoint.port) == ("https", "space-api", 9801)
    assert "SPACE_INTERNAL_RPC_INSECURE_DEV" not in env
    assert _mounted_secret(deployment, env["SPACE_INTERNAL_RPC_TLS_CA"]["value"]) == "space-internal-rpc-ca"

    token_path = env["SPACE_CLUSTER_API_TOKEN_PATH"]["value"]
    directory, filename = token_path.rsplit("/", 1)
    projected = _volumes(deployment)[_mounts(deployment)[directory]]["projected"]["sources"]
    assert {"path": filename, "audience": "space-cluster-api"}.items() <= projected[0][
        "serviceAccountToken"
    ].items()


def test_the_listener_serves_an_identity_naming_the_dialled_service():
    manifests = render(API_CHART, "space")
    deployment = object_named(manifests, "Deployment", "space-api")
    env = env_by_name(deployment)
    assert env["SPACE_INGRESS_ACTIVATION_LISTEN"]["value"] == "0.0.0.0:9801"
    assert "SPACE_INTERNAL_RPC_INSECURE_DEV" not in env
    secret = _mounted_secret(deployment, env["SPACE_INTERNAL_RPC_TLS_CERT"]["value"])
    assert secret == _mounted_secret(deployment, env["SPACE_INTERNAL_RPC_TLS_KEY"]["value"])
    # The api runs as space-api (docker/Dockerfile.api): its group, and only
    # its group, reads the root-owned key.
    pod = deployment["spec"]["template"]["spec"]
    assert pod["securityContext"]["fsGroup"] == 952
    mount = _mounts(deployment)[env["SPACE_INTERNAL_RPC_TLS_KEY"]["value"].rsplit("/", 1)[0]]
    assert _volumes(deployment)[mount]["secret"]["defaultMode"] == 0o440

    service = object_named(manifests, "Service", "space-api")
    assert {"name": "activation", "port": 9801, "targetPort": "activation"}.items() <= next(
        port for port in service["spec"]["ports"] if port["name"] == "activation"
    ).items()
    # internal-rpc-tls.sh issues that Secret under the cluster CA with the
    # Service's name as its only DNS SAN.
    leaves = [line for line in RPC_TLS_SCRIPT.read_text().splitlines() if line.startswith(f'  "{secret}|')]
    assert len(leaves) == 1, f"internal-rpc-tls.sh issues no leaf for {secret}"
    assert leaves[0].strip('" ').split("|")[2] == "space-api"


def test_the_listener_admits_only_internal_api_by_token_review():
    manifests = render(API_CHART, "space")
    env = env_by_name(object_named(manifests, "Deployment", "space-api"))
    assert env["SPACE_K8S_TOKEN_AUTH_ENABLED"]["value"] == "true"
    assert env["SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED"]["value"] == "false"
    assert env["SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS"]["value"] == "space-internal-api"

    binding = object_named(manifests, "ClusterRoleBinding", "space-api-token-review")
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": "space-api", "namespace": "space"}]
    role = object_named(manifests, "ClusterRole", binding["roleRef"]["name"])
    assert role["rules"] == [
        {"apiGroups": ["authentication.k8s.io"], "resources": ["tokenreviews"], "verbs": ["create"]}
    ]
