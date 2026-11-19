"""Chart-owned Native upload drain validation and memory budget safety.

A request can upload its body, wait up to 135 seconds for wake, then transfer
it. A chart must reject a drain budget that cannot cover all three phases.
Runtime native-file scenarios own routing and authorization evidence.
"""

import subprocess

import pytest

from kind_chart_render import KIND_DIR, env_by_name, object_named, render

DATA_PLANE_CHART = KIND_DIR / "charts/space-data-plane-local"
def _shutdown_grace(*values: str) -> str:
    deployment = object_named(
        render(DATA_PLANE_CHART, "space", *values), "Deployment", "space-data-plane"
    )
    args = deployment["spec"]["template"]["spec"]["containers"][0]["args"]
    return args[args.index("--shutdown-grace-secs") + 1]


def test_native_files_render_with_a_drain_that_covers_an_upload_a_wake_and_its_transfer():
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


def _mebibytes(quantity: str) -> int:
    assert quantity.endswith("Mi"), quantity
    return int(quantity[:-2])


def test_kind_bounds_native_writes_to_the_data_planes_memory():
    deployment = object_named(render(DATA_PLANE_CHART, "space"), "Deployment", "space-data-plane")
    env = env_by_name(deployment)
    # Numeric budgets protect the pod only when admission enforces them.
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


