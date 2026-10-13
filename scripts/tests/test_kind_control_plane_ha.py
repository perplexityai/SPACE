"""Kind's control plane renders the replicated shape it mirrors.

A live run shows replicas serving; it does not show that the replicas leave the
migrations to a single Job (the migrator asks deployments to serialize runs per
database), that the entry Services carry only the public HTTP port on the node
ports the cluster template maps to the host, that the autoscalers own the
replica counts the Deployments therefore omit, that the controller stays leader
elected at two replicas, or that the admin surface runs apart from the
Service internal-api and clients address.
"""

import yaml

from kind_chart_render import API_CHART, CLUSTER_CONTROLLER_CHART, KIND_DIR, env_by_name, object_named, render

DATA_PLANE_CHART = KIND_DIR / "charts/space-data-plane-local"
CLUSTER_TEMPLATE = KIND_DIR / "kind-cluster.yaml.tmpl"


def _mapped_node_ports() -> dict[int, int]:
    nodes = yaml.safe_load(CLUSTER_TEMPLATE.read_text())["nodes"]
    return {
        mapping["containerPort"]: mapping["hostPort"]
        for node in nodes
        for mapping in node.get("extraPortMappings", [])
        if mapping.get("listenAddress") == "127.0.0.1"
    }


def test_migrations_run_once_in_a_job_not_in_each_api_replica():
    documents = render(API_CHART, "space")
    job = object_named(documents, "Job", "space-db-migrate")
    steps = [c["name"] for c in job["spec"]["template"]["spec"]["initContainers"]]
    steps += [c["name"] for c in job["spec"]["template"]["spec"]["containers"]]
    assert steps == ["bootstrap-roles", "migrate", "bootstrap-roles-grants"]
    for name in ("space-api", "space-admin-api"):
        pod = object_named(documents, "Deployment", name)["spec"]["template"]["spec"]
        assert "initContainers" not in pod, name


def test_entries_carry_only_http_on_the_node_ports_the_cluster_maps():
    mapped = _mapped_node_ports()
    for chart, name, host_port in (
        (API_CHART, "space-api-entry", 8080),
        (DATA_PLANE_CHART, "space-data-plane-entry", 8090),
    ):
        service = object_named(render(chart, "space"), "Service", name)
        assert service["spec"]["type"] == "NodePort"
        [port] = service["spec"]["ports"]
        assert port["targetPort"] == "http"
        assert mapped[port["nodePort"]] == host_port


def test_autoscalers_own_the_replica_counts():
    for chart, name in ((API_CHART, "space-api"), (DATA_PLANE_CHART, "space-data-plane")):
        documents = render(chart, "space")
        deployment = object_named(documents, "Deployment", name)
        hpa = object_named(documents, "HorizontalPodAutoscaler", name)
        assert "replicas" not in deployment["spec"]
        assert deployment["spec"]["strategy"]["rollingUpdate"]["maxUnavailable"] == 0
        assert hpa["spec"]["scaleTargetRef"]["name"] == name
        assert hpa["spec"]["minReplicas"] >= 2
        assert {m["resource"]["name"] for m in hpa["spec"]["metrics"]} == {"cpu", "memory"}


def test_the_controller_runs_two_leader_elected_replicas():
    deployment = object_named(render(CLUSTER_CONTROLLER_CHART, "space"), "Deployment", "space-cluster-controller")
    assert deployment["spec"]["replicas"] == 2
    env = env_by_name(deployment)
    assert env["SPACE_CLUSTER_CONTROLLER_LEADER_ELECTION_ENABLED"]["value"] == "true"


def test_the_admin_surface_runs_apart_from_the_shared_service():
    documents = render(API_CHART, "space")
    admin = object_named(documents, "Deployment", "space-admin-api")
    container = admin["spec"]["template"]["spec"]["containers"][0]
    assert container["args"][:2] == ["--surface", "admin"]
    assert "SPACE_INGRESS_ACTIVATION_LISTEN" not in env_by_name(admin)
    labels = admin["spec"]["template"]["metadata"]["labels"]
    for service in ("space-api", "space-api-entry"):
        selector = object_named(documents, "Service", service)["spec"]["selector"]
        assert not all(labels.get(key) == value for key, value in selector.items()), service
