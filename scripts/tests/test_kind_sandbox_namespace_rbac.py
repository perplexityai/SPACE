"""The API and cluster-controller can watch pods in both namespaces they read.

Sandbox pods live in the sandbox namespace and the node-api and node-service
pods the substrate discovers nodes from live in the release namespace. One Role
used to cover both because they were one namespace; with them apart, a
grant in only one leaves the other's reflector refused with 403 and, for the
node-api pods, no node to drain or suspend on. This checks the rendered RBAC
grants each service account list and watch on pods in each namespace.
"""

from kind_chart_render import API_CHART, CLUSTER_CONTROLLER_CHART, KIND_DIR, render

INTERNAL_API_CHART = KIND_DIR / "charts/space-internal-api-local"


def _pod_readers(documents: list[dict]) -> set[tuple[str, str]]:
    """(namespace, service account) pairs bound to a Role that can list and watch pods."""
    roles = {
        (doc["metadata"]["namespace"], doc["metadata"]["name"])
        for doc in documents
        if doc["kind"] == "Role"
        and any(
            "pods" in rule.get("resources", []) and {"list", "watch"} <= set(rule.get("verbs", []))
            for rule in doc.get("rules", [])
        )
    }
    return {
        (doc["metadata"]["namespace"], subject["name"])
        for doc in documents
        if doc["kind"] == "RoleBinding"
        and (doc["metadata"]["namespace"], doc["roleRef"]["name"]) in roles
        for subject in doc["subjects"]
    }


def test_api_and_controller_watch_pods_in_the_sandbox_and_release_namespaces():
    documents = [
        *render(API_CHART, "space"),
        *render(CLUSTER_CONTROLLER_CHART, "space"),
        *render(INTERNAL_API_CHART, "space"),
    ]
    readers = _pod_readers(documents)
    for account in ("space-api", "space-cluster-controller"):
        for namespace in ("sandboxes", "space"):
            assert (namespace, account) in readers, (namespace, account)
    assert ("sandboxes", "space-internal-api") in readers
