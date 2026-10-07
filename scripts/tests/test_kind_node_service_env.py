"""Single-service settings reach only the Kind pods whose code reads them.

Each Kind node service runs in its own pod, so a setting belongs to a service
exactly when that service's chart renders it. These checks render every node
service, per role, and the control-plane credential-manager authority, and
assert that the credential database, its password, the AWS credentials and the
Secrets Manager reader role reach only the authority, and the static
object-store credentials only volume-manager and build-manager (the services
that transfer objects with them).
"""

import subprocess

import pytest
from kind_chart_render import KIND_DIR, render

NODE_RELEASES = [
    line.split()
    for line in subprocess.run(
        ["bash", str(KIND_DIR / "lib/node-services.sh"), "releases"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    if line.strip()
]
AUTHORITY_CHART = KIND_DIR / "charts/space-credmgr-authority-local"

CREDENTIAL_AUTHORITY_ONLY = {
    "DATABASE_URL",
    "SPACE_DB_CREDS_PASSWORD",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN",
}
OBJECT_STORE_STATIC = {
    "SPACE_OBJECT_STORE_STATIC_ACCESS_KEY_ID",
    "SPACE_OBJECT_STORE_STATIC_SECRET_ACCESS_KEY",
}
OBJECT_STORE_CONSUMERS = {"volume-manager", "build-manager"}


def _rendered_env(manifests: list[dict]) -> set[str]:
    names = set()
    for document in manifests:
        spec = document.get("spec", {}).get("template", {}).get("spec", {})
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            names.update(entry["name"] for entry in container.get("env", []))
    return names


def _node_service_env(release: str, role: str, service: str) -> set[str]:
    chart = KIND_DIR / f"charts/space-{service}-local"
    extra = ["--set", f"node.role={role}"]
    if role == "builder" and (chart / "values-builder.yaml").exists():
        extra += ["-f", str(chart / "values-builder.yaml")]
    return _rendered_env(render(chart, release, *extra))


@pytest.mark.parametrize(("release", "role", "service"), NODE_RELEASES)
def test_node_services_receive_no_credential_authority_settings(release, role, service):
    leaked = _node_service_env(release, role, service) & CREDENTIAL_AUTHORITY_ONLY
    assert not leaked, f"{release} {service} renders {sorted(leaked)}"


@pytest.mark.parametrize(("release", "role", "service"), NODE_RELEASES)
def test_static_object_store_credentials_reach_only_their_consumers(release, role, service):
    present = _node_service_env(release, role, service) & OBJECT_STORE_STATIC
    if service in OBJECT_STORE_CONSUMERS:
        assert present == OBJECT_STORE_STATIC, f"{release} {service} renders {sorted(present)}"
    else:
        assert not present, f"{release} {service} renders {sorted(present)}"


def test_the_authority_reads_the_secrets_manager_role_from_the_optional_aws_secret():
    manifests = render(AUTHORITY_CHART, "space")
    deployment = next(
        document for document in manifests
        if document.get("kind") == "Deployment" and document["metadata"]["name"] == "space-credmgr-authority"
    )
    env = {entry["name"]: entry for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert CREDENTIAL_AUTHORITY_ONLY <= set(env)
    role = env["SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN"]["valueFrom"]["secretKeyRef"]
    assert role == {
        "name": "space-aws-creds",
        "key": "SPACE_CREDMGR_SECRETS_MANAGER_ROLE_ARN",
        "optional": True,
    }
    assert not _rendered_env(manifests) & OBJECT_STORE_STATIC
