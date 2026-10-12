"""Single-service settings reach only the Kind pods whose code reads them.

Each Kind node service runs in its own pod, so a setting belongs to a service
exactly when that service's chart renders it. These checks render every node
service, per role, and the control-plane credential-manager authority, and
assert that the credential database, its password, the AWS credentials and the
Secrets Manager reader role reach only the authority, and the static
object-store credentials only volume-manager and build-manager (the services
that transfer objects with them). Volume-manager's streaming upload budget is
unset by default, so the service default applies, and renders exactly as set.
Sandbox-manager verifies route tokens from a bundle file on a Secret volume and
receives no signing key.
"""

import subprocess
import tempfile
from pathlib import Path, PurePosixPath

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
STREAM_BUDGET = "SPACE_VOLUME_STREAM_PUSH_BUFFER_BUDGET_BYTES"
VOLUME_MANAGER_CHART = KIND_DIR / "charts/space-volume-manager-local"


def _rendered_env(manifests: list[dict]) -> set[str]:
    names = set()
    for document in manifests:
        spec = document.get("spec", {}).get("template", {}).get("spec", {})
        for container in spec.get("containers", []) + spec.get("initContainers", []):
            names.update(entry["name"] for entry in container.get("env", []))
    return names


def _node_service_render(release: str, role: str, service: str) -> list[dict]:
    chart = KIND_DIR / f"charts/space-{service}-local"
    extra = ["--set", f"node.role={role}"]
    if role == "builder" and (chart / "values-builder.yaml").exists():
        extra += ["-f", str(chart / "values-builder.yaml")]
    return render(chart, release, *extra)


def _node_service_env(release: str, role: str, service: str) -> set[str]:
    return _rendered_env(_node_service_render(release, role, service))


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


@pytest.mark.parametrize(("release", "role", "service"), NODE_RELEASES)
def test_no_node_service_sets_a_stream_upload_budget_by_default(release, role, service):
    assert STREAM_BUDGET not in _node_service_env(release, role, service)


def _volume_manager_budget(*extra: str) -> str | None:
    for document in render(VOLUME_MANAGER_CHART, "space", *extra):
        for container in document.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
            for entry in container.get("env", []):
                if entry["name"] == STREAM_BUDGET:
                    return entry["value"]
    return None


@pytest.mark.parametrize(
    ("value", "expected"),
    [("0", "0"), ("134217728", "134217728")],
)
def test_volume_manager_renders_a_set_stream_upload_budget_exactly(value, expected):
    # A numeric 0 must still select file uploads, and a number from a values
    # file (parsed as a float) must not render in exponent form.
    assert _volume_manager_budget("--set-string", f"streamPushBufferBudgetBytes={value}") == expected
    assert _volume_manager_budget("--set", f"streamPushBufferBudgetBytes={value}") == expected
    with tempfile.TemporaryDirectory() as directory:
        values = Path(directory) / "values.yaml"
        values.write_text(f"streamPushBufferBudgetBytes: {value}\n")
        assert _volume_manager_budget("-f", str(values)) == expected


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


ROUTE_BUNDLE_PATH = "SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE_PATH"
ROUTE_SIGNING_OR_INLINE = {
    "SPACE_API_ROUTE_ED25519_VERIFICATION_BUNDLE",
    "SPACE_API_ROUTE_ED25519_SIGNING_KEY",
    "SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH",
}
SANDBOX_MANAGER_RELEASES = [entry for entry in NODE_RELEASES if entry[2] == "sandbox-manager"]


@pytest.mark.parametrize(("release", "role", "service"), SANDBOX_MANAGER_RELEASES)
def test_sandbox_manager_reads_the_route_bundle_from_a_whole_secret_directory(release, role, service):
    manifests = _node_service_render(release, role, service)
    leaked = _rendered_env(manifests) & ROUTE_SIGNING_OR_INLINE
    assert not leaked, f"{release} {service} renders {sorted(leaked)}"
    daemonset = next(document for document in manifests if document.get("kind") == "DaemonSet")
    pod = daemonset["spec"]["template"]["spec"]
    container = next(entry for entry in pod["containers"] if entry["name"] == "sandbox-manager")
    env = {entry["name"]: entry for entry in container["env"]}
    bundle = PurePosixPath(env[ROUTE_BUNDLE_PATH]["value"])
    assert bundle.is_absolute(), bundle

    # The deepest mount holding the bundle serves it. A directory mount sees the
    # kubelet's updates to the Secret; a subPath mount (or one of the file
    # itself) keeps the bundle the pod started with, so a rotated key would be
    # refused until the pod restarts.
    mounts = [
        mount for mount in container.get("volumeMounts", [])
        if bundle == PurePosixPath(mount["mountPath"]) or PurePosixPath(mount["mountPath"]) in bundle.parents
    ]
    assert mounts, f"no volume mount serves {bundle}"
    mount = max(mounts, key=lambda entry: len(PurePosixPath(entry["mountPath"]).parts))
    assert "subPath" not in mount and "subPathExpr" not in mount, mount
    relative = bundle.relative_to(mount["mountPath"])
    assert relative.parts, f"{bundle} is mounted as a file, not served from a directory"
    volume = next(entry for entry in pod["volumes"] if entry["name"] == mount["name"])
    assert "secret" in volume, volume
    items = volume["secret"].get("items")
    if items is not None:
        assert str(relative) in {item["path"] for item in items}, volume
