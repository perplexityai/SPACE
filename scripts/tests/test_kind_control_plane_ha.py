"""Cross-resource API isolation and autoscaler ownership boundaries."""

from kind_chart_render import API_CHART, KIND_DIR, env_by_name, object_named, render

DATA_PLANE_CHART = KIND_DIR / "charts/space-data-plane-local"


def test_autoscalers_own_the_replica_counts():
    for chart in (API_CHART, DATA_PLANE_CHART):
        documents = render(chart, "space")
        autoscalers = [doc for doc in documents if doc.get("kind") == "HorizontalPodAutoscaler"]
        assert autoscalers, f"{chart.name} renders no autoscaler"
        for autoscaler in autoscalers:
            target = autoscaler["spec"]["scaleTargetRef"]
            assert target["kind"] == "Deployment"
            deployment = object_named(documents, target["kind"], target["name"])
            assert "replicas" not in deployment["spec"], target["name"]


def test_the_admin_surface_runs_apart_from_the_shared_service():
    documents = render(API_CHART, "space")
    admin = object_named(documents, "Deployment", "space-admin-api")
    container = admin["spec"]["template"]["spec"]["containers"][0]
    args = container["args"]
    assert args[args.index("--surface") + 1] == "admin"
    assert "SPACE_INGRESS_ACTIVATION_LISTEN" not in env_by_name(admin)
    labels = admin["spec"]["template"]["metadata"]["labels"]
    for service in ("space-api", "space-api-entry"):
        selector = object_named(documents, "Service", service)["spec"]["selector"]
        assert not all(labels.get(key) == value for key, value in selector.items()), service
