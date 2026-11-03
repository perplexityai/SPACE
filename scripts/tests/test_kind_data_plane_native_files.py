"""The data plane's drain lets a Native files request it already took finish.

A files request can take its upload time before it wakes its sandbox, wait up
to 135 s for the wake (the API's blocking wait budget) and then transfer its
body, and the drain deadline (`--shutdown-grace-secs`) cancels whatever is
still running. With Native files on, the chart refuses a deadline shorter than
all three together.
"""

import subprocess

import pytest

from kind_chart_render import KIND_DIR, object_named, render

DATA_PLANE_CHART = KIND_DIR / "charts/space-data-plane-local"


def _shutdown_grace(*values: str) -> str:
    deployment = object_named(
        render(DATA_PLANE_CHART, "space", *values), "Deployment", "space-data-plane"
    )
    args = deployment["spec"]["template"]["spec"]["containers"][0]["args"]
    return args[args.index("--shutdown-grace-secs") + 1]


def test_native_files_render_with_a_drain_that_covers_an_upload_a_wake_and_its_transfer():
    assert _shutdown_grace("--set", "nativeFiles.enabled=true") == "175"
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
    assert _shutdown_grace("--set", "shutdownGraceSeconds=60") == "60"
