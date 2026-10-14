"""Unit tests for scripts/space-cluster-smoke.py, the in-cluster smoke runner.

Run from the repo root:

    python -m pytest scripts/tests/test_space_cluster_smoke.py -q

The runner lives at `scripts/space-cluster-smoke.py` (kebab case for the
container entry point), so it is imported via importlib rather than a normal
module path.
"""

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import queue
import socket
import sys
import types
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2,
    metrics_service_pb2_grpc,
)
from opentelemetry.proto.metrics.v1 import metrics_pb2

SCRIPT_PATH = pathlib.Path(__file__).resolve().parent.parent / "space-cluster-smoke.py"
_spec = importlib.util.spec_from_file_location("space_cluster_smoke", SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
smoke = importlib.util.module_from_spec(_spec)
sys.modules["space_cluster_smoke"] = smoke
_spec.loader.exec_module(smoke)


BASE_ENVIRONMENT = {
    "E2B_API_KEY": "e2b_key",
    "E2B_DOMAIN": "e2b.example.test",
    "SPACE_SMOKE_CLUSTER_ID": "prod-3",
}

HEADER_SECRET = "authorization=smoke-metrics-test-secret"


@pytest.fixture
def metrics_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    # SPACE_SERVICE_NAME, SPACE_ENV and SPACE_VERSION now outrank the OTel and
    # DD_* settings, so an inherited one would decide what an exporter test
    # sees. Every test using this fixture has to start from none of them.
    for key in os.environ:
        if key.startswith(("OTEL_", "DD_")) or key in (
            "SPACE_METRICS_SINK",
            "SPACE_SERVICE_NAME",
            "SPACE_ENV",
            "SPACE_VERSION",
        ):
            monkeypatch.delenv(key)


class MetricsReceiver(metrics_service_pb2_grpc.MetricsServiceServicer):
    def __init__(self) -> None:
        self.requests: queue.Queue[metrics_service_pb2.ExportMetricsServiceRequest] = (
            queue.Queue()
        )
        self.unavailable = False

    def Export(self, request, context):
        if self.unavailable:
            context.abort(grpc.StatusCode.UNAVAILABLE, "test receiver unavailable")
        self.requests.put(request)
        return metrics_service_pb2.ExportMetricsServiceResponse()


@pytest.fixture
def metrics_receiver():
    receiver = MetricsReceiver()
    with ThreadPoolExecutor(max_workers=2) as pool:
        server = grpc.server(pool)
        metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(receiver, server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield receiver, f"http://127.0.0.1:{port}"
        finally:
            server.stop(grace=None).wait()


def config(**overrides: str):
    environment = dict(BASE_ENVIRONMENT)
    environment.update(overrides)
    return smoke.load_config(environment)


def test_by_default_the_suites_resolve_the_ecw_hostname_themselves() -> None:
    # Script-backed SDK suites ignore a Host override and derive every
    # data-plane host from the domain, so the default is no endpoint override.
    resolved = config()

    assert resolved.api_url is None
    assert resolved.api_host is None
    assert resolved.data_plane_url is None


def test_an_endpoint_override_without_a_host_header_is_refused() -> None:
    with pytest.raises(smoke.ConfigurationError, match="ECW_API_HOST"):
        config(E2B_API_URL="http://space-api:8080")


def test_a_run_needs_a_key_and_a_domain() -> None:
    with pytest.raises(smoke.ConfigurationError, match="E2B_API_KEY"):
        smoke.load_config({"E2B_DOMAIN": "e2b.example"})
    with pytest.raises(smoke.ConfigurationError, match="E2B_DOMAIN"):
        smoke.load_config({"E2B_API_KEY": "key"})


def test_unknown_suite_names_are_rejected_rather_than_silently_dropped() -> None:
    with pytest.raises(smoke.ConfigurationError, match="netpol"):
        config(SPACE_SMOKE_SUITES="typed,netpol")


def test_every_script_backed_suite_entrypoint_exists() -> None:
    for suite in smoke.SUITES:
        if suite.args[0] == "-m":
            continue
        assert (smoke.REPO_ROOT / suite.args[0]).is_file(), suite.id


def test_selected_suites_keep_declaration_order() -> None:
    resolved = config(
        SPACE_SMOKE_SUITES="egress,runtime,typed",
        SPACE_SMOKE_TEMPLATES="custom-runtime",
        SPACE_SMOKE_RUNTIME_TEMPLATES="custom-runtime",
    )

    assert [suite.id for suite in resolved.suites] == [
        "typed",
        "runtime",
        "egress",
    ]


def test_opt_in_suites_stay_out_of_a_default_run() -> None:
    assert "network-policy" not in {suite.id for suite in config().suites}
    assert "runtime" in {suite.id for suite in config().suites}
    assert "network-policy" in {
        suite.id for suite in config(SPACE_SMOKE_SUITES="network-policy").suites
    }


def test_network_policy_suite_uses_the_typed_category_and_failure_details() -> None:
    suite = smoke.SUITES_BY_ID["network-policy"]

    assert suite.args == (
        "-m",
        "space_smoke",
        "run",
        "--category",
        "network_policy",
    )
    assert suite.reports_failure_details


def test_only_template_reading_suites_expand_across_the_matrix() -> None:
    plan = smoke.build_plan(config(SPACE_SMOKE_TEMPLATES="seed,code-runtime"))

    assert [job.name for job in plan] == [
        "typed/seed",
        "typed/code-runtime",
        "egress",
    ]


@pytest.mark.parametrize("template", ["custom-runtime", "custom-runtime:release"])
def test_runtime_suite_only_expands_for_configured_template(template: str) -> None:
    plan = smoke.build_plan(
        config(
            SPACE_SMOKE_SUITES="runtime",
            SPACE_SMOKE_TEMPLATES=f"seed,{template},code-runtime",
            SPACE_SMOKE_RUNTIME_TEMPLATES="custom-runtime",
        )
    )

    assert [job.name for job in plan] == [f"runtime/{template}"]


@pytest.mark.parametrize(
    ("runtime_templates", "unknown"),
    [
        ("custom-runtim", "custom-runtim"),
        ("custom-runtime,missing-runtime", "missing-runtime"),
        ("missing-b,missing-a", "missing-a, missing-b"),
    ],
)
def test_runtime_templates_outside_the_matrix_are_rejected(
    runtime_templates: str, unknown: str
) -> None:
    with pytest.raises(smoke.ConfigurationError, match=f"unknown: {unknown}"):
        config(
            SPACE_SMOKE_TEMPLATES="seed,custom-runtime:release",
            SPACE_SMOKE_RUNTIME_TEMPLATES=runtime_templates,
        )


@pytest.mark.parametrize("runtime_templates", [None, "", "   "])
def test_requested_runtime_suite_requires_explicit_templates(runtime_templates):
    environment = {
        **BASE_ENVIRONMENT,
        "SPACE_SMOKE_SUITES": "typed,runtime",
        "SPACE_SMOKE_TEMPLATES": "seed,custom-runtime",
    }
    if runtime_templates is not None:
        environment["SPACE_SMOKE_RUNTIME_TEMPLATES"] = runtime_templates

    with pytest.raises(smoke.ConfigurationError, match="SPACE_SMOKE_RUNTIME_TEMPLATES"):
        smoke.load_config(environment)


def test_main_fails_when_filtering_produces_no_jobs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name, value in BASE_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SPACE_SMOKE_SUITES", "runtime")
    monkeypatch.setenv("SPACE_SMOKE_TEMPLATES", "seed")

    assert smoke.main([]) == smoke.EXIT_HARD_FAIL
    assert "SPACE_SMOKE_RUNTIME_TEMPLATES" in capsys.readouterr().err


def test_default_plan_runs_runtime_certification_when_configured_template_is_present() -> None:
    plan = smoke.build_plan(config(SPACE_SMOKE_TEMPLATES="seed,custom-runtime", SPACE_SMOKE_RUNTIME_TEMPLATES="custom-runtime"))

    assert [job.name for job in plan] == [
        "typed/seed",
        "typed/custom-runtime",
        "runtime/custom-runtime",
        "egress",
    ]


def test_suite_environment_selects_the_deployed_backend_and_the_template() -> None:
    resolved = config()
    job = smoke.Job(smoke.SUITES_BY_ID["typed"], "code-runtime")

    environment = smoke.suite_environment(resolved, job, None)

    assert environment["SPACE_SMOKE_BACKEND"] == "deployed"
    assert environment["E2B_TEMPLATE"] == "code-runtime"
    assert environment["E2B_DOMAIN"] == "e2b.example.test"
    assert "E2B_API_URL" not in environment
    assert "ECW_API_HOST" not in environment


def test_private_suite_loads_only_its_explicit_scenario_pack() -> None:
    resolved = config()
    private_job = smoke.Job(smoke.SUITES_BY_ID["runtime"], "custom-runtime")
    public_job = smoke.Job(smoke.SUITES_BY_ID["typed"], "seed")

    private_environment = smoke.suite_environment(resolved, private_job, None)
    public_environment = smoke.suite_environment(resolved, public_job, None)

    assert private_environment["SPACE_SMOKE_SCENARIO_PACKAGES"] == (
        "space_smoke.private_scenarios.scenarios"
    )
    assert "SPACE_SMOKE_SCENARIO_PACKAGES" not in public_environment


def test_an_inherited_endpoint_override_does_not_leak_into_the_suites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A stray E2B_API_URL in the Job's environment would send every suite at an
    # endpoint the run never chose, and the two that honour it would 404.
    monkeypatch.setenv("E2B_API_URL", "http://leftover:8080")
    monkeypatch.setenv("ECW_DATA_PLANE_BASE_URL", "http://leftover:8090")
    resolved = config()
    job = smoke.Job(smoke.SUITES_BY_ID["typed"], "seed")

    environment = smoke.suite_environment(resolved, job, None)

    assert "E2B_API_URL" not in environment
    assert "ECW_DATA_PLANE_BASE_URL" not in environment


def test_a_suite_without_its_own_detail_file_does_not_inherit_the_previous_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPACE_SMOKE_FAILURE_DETAILS_PATH", "/tmp/previous-suite.txt")
    resolved = config()
    job = smoke.Job(smoke.SUITES_BY_ID["egress"], None)

    environment = smoke.suite_environment(resolved, job, None)

    assert "SPACE_SMOKE_FAILURE_DETAILS_PATH" not in environment


def test_soft_exit_codes_are_warnings_only_for_the_suites_that_define_them() -> None:
    runtime_suite = smoke.SUITES_BY_ID["runtime"]
    typed = smoke.SUITES_BY_ID["typed"]

    assert smoke.classify(runtime_suite, 42, strict_egress=False) == smoke.WARN
    assert smoke.classify(runtime_suite, 1, strict_egress=False) == smoke.FAIL
    assert smoke.classify(typed, 42, strict_egress=False) == smoke.FAIL
    assert smoke.classify(typed, 0, strict_egress=False) == smoke.PASS


def test_strict_mode_upgrades_the_egress_soft_failure_to_a_regression() -> None:
    egress = smoke.SUITES_BY_ID["egress"]

    assert smoke.classify(egress, 42, strict_egress=False) == smoke.WARN
    assert smoke.classify(egress, 42, strict_egress=True) == smoke.FAIL
    # Strictness is the egress smoke's own knob; it must not reclassify a runtime
    # scenario's quarantined warning.
    assert (
        smoke.classify(smoke.SUITES_BY_ID["runtime"], 42, strict_egress=True)
        == smoke.WARN
    )


def _result(outcome: str) -> smoke.JobResult:
    return smoke.JobResult(
        job=smoke.Job(smoke.SUITES_BY_ID["typed"], "seed"),
        exit_code=0 if outcome == smoke.PASS else 1,
        outcome=outcome,
        duration_ms=1000,
    )


def test_a_hard_failure_outranks_a_warning_in_the_run_status() -> None:
    assert smoke.run_exit_code([_result(smoke.PASS)]) == smoke.EXIT_PASS
    assert (
        smoke.run_exit_code([_result(smoke.PASS), _result(smoke.WARN)])
        == smoke.EXIT_SOFT_FAIL
    )
    assert (
        smoke.run_exit_code([_result(smoke.WARN), _result(smoke.FAIL)])
        == smoke.EXIT_HARD_FAIL
    )


def test_every_suite_runs_even_after_one_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    invoked: list[list[str]] = []

    def runner(command, **kwargs):
        invoked.append(list(command))
        return types.SimpleNamespace(returncode=1)

    monkeypatch.setattr(smoke.subprocess, "run", runner)
    monkeypatch.setattr(smoke, "report", lambda *args, **kwargs: None)
    for name, value in BASE_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SPACE_SMOKE_TEMPLATES", "seed")
    monkeypatch.setenv("SPACE_SMOKE_PYTHON", "/space/smoke/bin/python")

    assert smoke.main([]) == smoke.EXIT_HARD_FAIL
    assert invoked == [
        [
            "/space/smoke/bin/python",
            "-m",
            "space_smoke",
            "run",
            "--category",
            "ecw",
        ],
        ["/space/smoke/bin/python", "scripts/tests/test-sandbox-egress-isolation.py"],
    ]


def test_metrics_carry_the_cluster_suite_and_template() -> None:
    resolved = config(SPACE_SMOKE_TEMPLATES="seed")
    results = [
        smoke.JobResult(
            job=smoke.Job(smoke.SUITES_BY_ID["runtime"], "custom-runtime"),
            exit_code=42,
            outcome=smoke.WARN,
            duration_ms=2500,
        )
    ]

    lines = smoke.statsd_lines(
        smoke.metric_points(resolved, results, exit_code=smoke.EXIT_SOFT_FAIL)
    )

    assert (
        "space.smoke.suite.result:1|c|#cluster_id:prod-3,suite:runtime,outcome:warn,template:custom-runtime"
        in lines
    )
    assert (
        "space.smoke.suite.duration_ms:2500|g|#cluster_id:prod-3,suite:runtime,outcome:warn,template:custom-runtime"
        in lines
    )
    assert "space.smoke.run.result:1|c|#cluster_id:prod-3,outcome:warn" in lines
    assert "space.smoke.run.duration_ms:2500|g|#cluster_id:prod-3,outcome:warn" in lines


def test_the_slack_body_names_the_cluster_and_only_the_non_passing_suites() -> None:
    resolved = config()
    results = [
        smoke.JobResult(
            job=smoke.Job(smoke.SUITES_BY_ID["typed"], "seed"),
            exit_code=0,
            outcome=smoke.PASS,
            duration_ms=1000,
        ),
        smoke.JobResult(
            job=smoke.Job(smoke.SUITES_BY_ID["runtime"], "custom-runtime"),
            exit_code=1,
            outcome=smoke.FAIL,
            duration_ms=1000,
            details="runtime.example_launch — API boundary not reached",
        ),
    ]

    message = smoke.slack_message(resolved, results, exit_code=smoke.EXIT_HARD_FAIL)

    assert "prod-3" in message
    assert "Typed runtime certification suite (custom-runtime)" in message
    assert "runtime.example_launch" in message
    assert "Typed ECW invariant suite" not in message


def test_the_summary_document_records_every_suite_verdict() -> None:
    resolved = config()
    results = [
        smoke.JobResult(
            job=smoke.Job(smoke.SUITES_BY_ID["egress"], None),
            exit_code=42,
            outcome=smoke.WARN,
            duration_ms=1500,
        )
    ]

    document = smoke.summary_document(resolved, results, exit_code=smoke.EXIT_SOFT_FAIL)

    assert document["cluster_id"] == "prod-3"
    assert document["outcome"] == smoke.WARN
    assert document["suites"] == [
        {
            "suite": "egress",
            "label": "Security smoke / egress isolation",
            "template": None,
            "outcome": smoke.WARN,
            "exit_code": 42,
            "duration_ms": 1500,
            "details": "",
        }
    ]


def test_a_hung_suite_is_killed_and_the_rest_still_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Without a per-suite ceiling one wedged suite consumes the Job's whole
    # deadline, and the later suites, the summary, and the metrics go with it.
    invoked: list[list[str]] = []

    def runner(command, **kwargs):
        invoked.append(list(command))
        assert kwargs["timeout"] > 0
        if "test-sandbox-egress-isolation.py" in command[-1]:
            raise smoke.subprocess.TimeoutExpired(command, kwargs["timeout"])
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(smoke.subprocess, "run", runner)
    monkeypatch.setattr(smoke, "report", lambda *args, **kwargs: None)
    for name, value in BASE_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("SPACE_SMOKE_TEMPLATES", "seed")
    monkeypatch.setenv("SPACE_SMOKE_PYTHON", "/space/smoke/bin/python")

    assert smoke.main([]) == smoke.EXIT_HARD_FAIL
    assert len(invoked) == 2


def test_a_killed_suite_reports_a_regression_not_a_warning() -> None:
    def runner(command, **kwargs):
        raise smoke.subprocess.TimeoutExpired(command, kwargs["timeout"])

    # The runtime suite treats a classified scenario warning as soft; a suite that
    # produced no verdict at all has tolerated nothing.
    result = smoke.run_job(
        config(),
        smoke.Job(smoke.SUITES_BY_ID["runtime"], "custom-runtime"),
        runner=runner,
    )

    assert result.exit_code == smoke.EXIT_TIMEOUT
    assert result.outcome == smoke.FAIL
    assert "no verdict within" in result.details


def test_every_suite_declares_a_ceiling_it_can_be_held_to() -> None:
    for suite in smoke.SUITES:
        assert suite.timeout_seconds > 0, suite.id


def test_the_run_budget_stops_the_plan_and_still_reports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The per-suite ceilings sum past the Job's deadline on purpose, so the run
    # budget is what guarantees the summary and the metrics still happen.
    clock = iter([0.0, 0.0, 1000.0, 1000.0, 1000.0, 1000.0])
    monkeypatch.setattr(smoke.time, "monotonic", lambda: next(clock))

    resolved = config(
        SPACE_SMOKE_RUN_BUDGET_SECONDS="900", SPACE_SMOKE_TEMPLATES="seed"
    )
    plan = smoke.build_plan(resolved)
    started: list[str] = []

    def fake_run_job(config_, job, *, timeout_seconds=None):
        started.append(job.name)
        return smoke.JobResult(job=job, exit_code=0, outcome=smoke.PASS, duration_ms=1)

    monkeypatch.setattr(smoke, "run_job", fake_run_job)
    results = list(smoke.run_plan(resolved, plan))

    assert started == ["typed/seed"]
    assert [r.outcome for r in results] == [
        smoke.PASS,
        smoke.SKIPPED,
    ]
    # A skipped suite is not a pass: the invariant went unchecked.
    assert smoke.run_exit_code(results) == smoke.EXIT_HARD_FAIL


def test_a_suite_never_gets_more_time_than_the_budget_has_left(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(smoke.time, "monotonic", lambda: 0.0)
    resolved = config(
        SPACE_SMOKE_RUN_BUDGET_SECONDS="120", SPACE_SMOKE_TEMPLATES="seed"
    )
    granted: list[int | None] = []

    def fake_run_job(config_, job, *, timeout_seconds=None):
        granted.append(timeout_seconds)
        return smoke.JobResult(job=job, exit_code=0, outcome=smoke.PASS, duration_ms=1)

    monkeypatch.setattr(smoke, "run_job", fake_run_job)
    list(smoke.run_plan(resolved, smoke.build_plan(resolved)))

    assert granted and all(value == 120 for value in granted)


def test_without_a_budget_each_suite_keeps_its_own_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolved = config(SPACE_SMOKE_TEMPLATES="seed")
    granted: list[int | None] = []

    def fake_run_job(config_, job, *, timeout_seconds=None):
        granted.append(timeout_seconds)
        return smoke.JobResult(job=job, exit_code=0, outcome=smoke.PASS, duration_ms=1)

    monkeypatch.setattr(smoke, "run_job", fake_run_job)
    list(smoke.run_plan(resolved, smoke.build_plan(resolved)))

    assert granted == [None, None]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, "datadog"),
        ("auto", "datadog"),
        (" DaTaDoG\t", "datadog"),
        (" OTLP ", "otlp"),
        ("off", "off"),
        ("", None),
        ("typo", None),
        ("journald", None),
    ],
)
def test_metrics_sink_selection(raw: str | None, expected: str | None) -> None:
    environment = {} if raw is None else {"SPACE_METRICS_SINK": raw}
    if expected is None:
        with pytest.raises(ValueError, match="SPACE_METRICS_SINK"):
            smoke.configured_metrics_sink(environment)
    else:
        assert smoke.configured_metrics_sink(environment) == expected


@pytest.mark.parametrize(
    ("environment", "endpoint", "error"),
    [
        ({}, None, "missing OTLP endpoint"),
        ({"ENDPOINT": " http://collector:4317\t"}, "http://collector:4317", None),
        (
            {
                "ENDPOINT": "http://generic:4317",
                "METRICS_ENDPOINT": "http://metrics:4317",
                "PROTOCOL": "http/protobuf",
                "METRICS_PROTOCOL": " GRPC ",
            },
            "http://metrics:4317",
            None,
        ),
        (
            {
                "ENDPOINT": "http://generic:4317",
                "METRICS_ENDPOINT": " \t",
                "HEADERS": " ",
                "METRICS_HEADERS": "\t",
            },
            "http://generic:4317",
            None,
        ),
        ({"ENDPOINT": "https://collector:4317"}, None, "plaintext http://"),
        ({"ENDPOINT": "collector:4317"}, None, "plaintext http://"),
        ({"ENDPOINT": "http://[bad-ipv6"}, None, "must be a URL"),
        (
            {"ENDPOINT": "http://user:smoke-metrics-test-secret@collector:4317"},
            None,
            "must not embed credentials",
        ),
        ({"ENDPOINT": "http://:4317"}, None, "must include a host"),
        ({"ENDPOINT": "http://collector:4317/v1/metrics"}, None, "must not include a path"),
        ({"ENDPOINT": "http://col lector:4317"}, None, "whitespace or Helm value separators"),
        ({"ENDPOINT": "http://collector,other:4317"}, None, "whitespace or Helm value separators"),
        ({"ENDPOINT": "http://collector\\:4317"}, None, "whitespace or Helm value separators"),
        ({"ENDPOINT": "http://{collector}:4317"}, None, "whitespace or Helm value separators"),
        # urlsplit reports an empty or unreadable port the same way it reports
        # none, so each of these once looked like a bare host.
        ({"ENDPOINT": "http://collector:65536"}, None, "port between 1 and 65535"),
        ({"ENDPOINT": "http://collector:abc"}, None, "port between 1 and 65535"),
        ({"ENDPOINT": "http://collector:"}, None, "port between 1 and 65535"),
        # Only the parser sees a malformed full port; the text check reads the
        # last colon and would take 1234 for a valid one.
        (
            {"ENDPOINT": "http://collector:4317:1234"},
            None,
            "port between 1 and 65535",
        ),
        # An empty query or fragment is still a query or fragment.
        ({"ENDPOINT": "http://collector:4317?"}, None, "query or fragment"),
        ({"ENDPOINT": "http://collector:4317#"}, None, "query or fragment"),
        ({"ENDPOINT": "http://[::1]:4317"}, "http://[::1]:4317", None),
        ({"ENDPOINT": "http://collector:4317/"}, "http://collector:4317/", None),
        ({"ENDPOINT": "http://collector"}, "http://collector", None),
        (
            {"ENDPOINT": "http://collector:4317", "PROTOCOL": "http/protobuf"},
            None,
            "require grpc",
        ),
        ({"HEADERS": HEADER_SECRET}, None, "OTEL_EXPORTER_OTLP_HEADERS"),
        (
            {"METRICS_HEADERS": HEADER_SECRET},
            None,
            "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
        ),
    ],
)
def test_otlp_metrics_transport(environment, endpoint, error) -> None:
    environment = {
        "OTEL_EXPORTER_OTLP_" + key: value for key, value in environment.items()
    }
    if error is None:
        assert smoke.otlp_metrics_endpoint(environment) == endpoint
    else:
        with pytest.raises(ValueError, match=error) as caught:
            smoke.otlp_metrics_endpoint(environment)
        assert "smoke-metrics-test-secret" not in str(caught.value)


def test_metrics_off_ignores_invalid_transports(
    metrics_environment, monkeypatch, capsys
) -> None:
    monkeypatch.setenv("SPACE_METRICS_SINK", "off")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", HEADER_SECRET)
    monkeypatch.setenv("DD_DOGSTATSD_PORT", "invalid")
    monkeypatch.setattr(smoke, "OTLP_AVAILABLE", False)
    monkeypatch.setattr(
        socket, "socket", lambda *args, **kwargs: pytest.fail("off opened a socket")
    )

    smoke.emit_metrics(config(), [], exit_code=smoke.EXIT_PASS)

    assert capsys.readouterr().err == ""


def test_default_metrics_keep_dogstatsd_names_tags_and_types(
    metrics_environment, monkeypatch
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        monkeypatch.setenv("DD_AGENT_HOST", "127.0.0.1")
        monkeypatch.setenv("DD_DOGSTATSD_PORT", str(receiver.getsockname()[1]))
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", HEADER_SECRET)
        results = [
            smoke.JobResult(
                smoke.Job(smoke.SUITES_BY_ID["typed"], "seed:latest"),
                0,
                smoke.PASS,
                2500,
            )
        ]

        smoke.emit_metrics(config(), results, exit_code=smoke.EXIT_PASS)

        assert {receiver.recv(4096).decode() for _ in range(4)} == {
            "space.smoke.suite.result:1|c|#cluster_id:prod-3,suite:typed,outcome:pass,template:seed:latest",
            "space.smoke.suite.duration_ms:2500|g|#cluster_id:prod-3,suite:typed,outcome:pass,template:seed:latest",
            "space.smoke.run.result:1|c|#cluster_id:prod-3,outcome:pass",
            "space.smoke.run.duration_ms:2500|g|#cluster_id:prod-3,outcome:pass",
        }


def test_otlp_metrics_export_before_exit(
    metrics_environment, metrics_receiver, monkeypatch, capsys
) -> None:
    receiver, endpoint = metrics_receiver
    monkeypatch.setenv("SPACE_METRICS_SINK", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE", "CUMULATIVE"
    )
    monkeypatch.setenv(
        "OTEL_RESOURCE_ATTRIBUTES",
        "service.name=resource-name,service.version=build-1,deployment.environment.name=test,team=space%20team",
    )
    monkeypatch.setenv("OTEL_SERVICE_NAME", "smoke-test")
    results = [
        smoke.JobResult(
            smoke.Job(smoke.SUITES_BY_ID["typed"], "seed:latest"), 0, smoke.PASS, 2500
        ),
        smoke.JobResult(
            smoke.Job(smoke.SUITES_BY_ID["egress"], None), 42, smoke.WARN, 1000
        ),
    ]

    smoke.emit_metrics(config(), results, exit_code=smoke.EXIT_SOFT_FAIL)

    request = receiver.requests.get(timeout=1)
    assert len(request.resource_metrics) == 1
    resource = request.resource_metrics[0]
    attrs = {item.key: item.value.string_value for item in resource.resource.attributes}
    assert {
        key: attrs[key]
        for key in (
            "service.name",
            "service.version",
            "deployment.environment.name",
            "team",
        )
    } == {
        "service.name": "smoke-test",
        "service.version": "build-1",
        "deployment.environment.name": "test",
        "team": "space team",
    }
    assert len(resource.scope_metrics) == 1
    metrics = {metric.name: metric for metric in resource.scope_metrics[0].metrics}
    assert set(metrics) == {
        "space.smoke.suite.result",
        "space.smoke.suite.duration_ms",
        "space.smoke.run.result",
        "space.smoke.run.duration_ms",
    }
    for name in ("space.smoke.suite.result", "space.smoke.run.result"):
        metric = metrics[name]
        assert metric.unit == "1"
        assert metric.sum.is_monotonic
        assert (
            metric.sum.aggregation_temporality
            == metrics_pb2.AGGREGATION_TEMPORALITY_DELTA
        )
        assert all(point.as_int == 1 for point in metric.sum.data_points)
    for name in ("space.smoke.suite.duration_ms", "space.smoke.run.duration_ms"):
        metric = metrics[name]
        assert metric.WhichOneof("data") == "gauge"
        assert metric.unit == "ms"
    assert metrics["space.smoke.run.duration_ms"].gauge.data_points[0].as_int == 3500
    assert [
        point.as_int
        for point in metrics["space.smoke.suite.duration_ms"].gauge.data_points
    ] == [2500, 1000]
    suite_tags = [
        {item.key: item.value.string_value for item in point.attributes}
        for point in metrics["space.smoke.suite.result"].sum.data_points
    ]
    assert suite_tags == [
        {
            "cluster_id": "prod-3",
            "suite": "typed",
            "outcome": "pass",
            "template": "seed:latest",
        },
        {"cluster_id": "prod-3", "suite": "egress", "outcome": "warn"},
    ]
    run_tags = {
        item.key: item.value.string_value
        for item in metrics["space.smoke.run.result"].sum.data_points[0].attributes
    }
    assert run_tags == {"cluster_id": "prod-3", "outcome": "warn"}
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "failure", ["headers", "receiver", "dependency", "datadog-port", "journald"]
)
def test_metric_failures_warn_once_without_changing_report(
    failure, metrics_environment, metrics_receiver, monkeypatch, capsys, caplog
) -> None:
    receiver, endpoint = metrics_receiver
    monkeypatch.setenv("SPACE_METRICS_SINK", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
    if failure == "headers":
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", HEADER_SECRET)
    elif failure == "receiver":
        receiver.unavailable = True
    elif failure == "dependency":
        monkeypatch.setattr(smoke, "OTLP_AVAILABLE", False)
    elif failure == "journald":
        monkeypatch.setenv("SPACE_METRICS_SINK", "journald")
    else:
        monkeypatch.setenv("SPACE_METRICS_SINK", "datadog")
        monkeypatch.setenv("DD_AGENT_HOST", "127.0.0.1")
        monkeypatch.setenv("DD_DOGSTATSD_PORT", "invalid")

    smoke.report(config(), [], exit_code=smoke.EXIT_PASS)

    output = capsys.readouterr()
    assert output.err.count("warning: could not emit smoke metrics:") == 1
    assert not caplog.records
    assert "smoke-metrics-test-secret" not in output.err
    assert json.loads(output.out.split("\n", 1)[1])["exit_code"] == 0
    assert receiver.requests.empty()


IDENTITY_KEYS = (
    "SPACE_SERVICE_NAME",
    "SPACE_ENV",
    "SPACE_VERSION",
    "OTEL_SERVICE_NAME",
    "OTEL_RESOURCE_ATTRIBUTES",
    "DD_SERVICE",
    "DD_ENV",
    "DD_VERSION",
)


@pytest.mark.parametrize(
    "settings, expected",
    [
        (
            {"SPACE_SERVICE_NAME": "space-smoke", "SPACE_ENV": "space-env", "SPACE_VERSION": "space-1"},
            {"service.name": "space-smoke", "deployment.environment.name": "space-env", "service.version": "space-1"},
        ),
        (
            {"OTEL_SERVICE_NAME": "otel-smoke"},
            {"service.name": "otel-smoke"},
        ),
        (
            {"OTEL_RESOURCE_ATTRIBUTES": "service.name=resource-smoke,deployment.environment.name=resource-env,service.version=resource-1"},
            {"service.name": "resource-smoke", "deployment.environment.name": "resource-env", "service.version": "resource-1"},
        ),
        (
            {"DD_SERVICE": "dd-smoke", "DD_ENV": "dd-env", "DD_VERSION": "dd-1"},
            {"service.name": "dd-smoke", "deployment.environment.name": "dd-env", "service.version": "dd-1"},
        ),
        (
            {
                "SPACE_SERVICE_NAME": "space-smoke", "SPACE_ENV": "space-env", "SPACE_VERSION": "space-1",
                "OTEL_SERVICE_NAME": "otel-smoke",
                "OTEL_RESOURCE_ATTRIBUTES": "service.name=resource-smoke,deployment.environment.name=resource-env,service.version=resource-1",
                "DD_SERVICE": "dd-smoke", "DD_ENV": "dd-env", "DD_VERSION": "dd-1",
            },
            {"service.name": "space-smoke", "deployment.environment.name": "space-env", "service.version": "space-1"},
        ),
        # A blank SPACE or OTel value is skipped; the Datadog value is not.
        (
            {"SPACE_SERVICE_NAME": " ", "OTEL_SERVICE_NAME": "\t", "DD_SERVICE": "dd-smoke"},
            {"service.name": "dd-smoke"},
        ),
        ({}, {"service.name": "space-cluster-smoke"}),
    ],
)
def test_otlp_identity_resolves_space_then_otel_then_datadog(
    settings, expected
) -> None:
    """The precedence rules, read straight off the resolver.

    One row below carries the same settings through a live receiver, which is
    what proves the resolver is the resource the exporter actually sends.
    """
    attributes = smoke.resolve_identity_attributes(settings)
    for key, value in expected.items():
        assert attributes.get(key) == value, key
    # A resource never publishes a blank identity attribute.
    for key in ("deployment.environment.name", "service.version"):
        if key not in expected:
            assert key not in attributes


def test_resolved_identity_is_the_exported_resource(
    metrics_environment, metrics_receiver, monkeypatch
) -> None:
    settings = {
        "SPACE_SERVICE_NAME": "space-smoke",
        "SPACE_ENV": "space-env",
        "SPACE_VERSION": "space-1",
        "OTEL_SERVICE_NAME": "otel-smoke",
        "DD_SERVICE": "dd-smoke",
    }
    receiver, endpoint = metrics_receiver
    monkeypatch.setenv("SPACE_METRICS_SINK", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
    for key in IDENTITY_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in settings.items():
        monkeypatch.setenv(key, value)

    smoke.emit_metrics(config(), [], exit_code=smoke.EXIT_PASS)

    request = receiver.requests.get(timeout=2)
    attributes = {
        item.key: item.value.string_value
        for item in request.resource_metrics[0].resource.attributes
    }
    assert attributes["service.name"] == "space-smoke"
    assert attributes["deployment.environment.name"] == "space-env"
    assert attributes["service.version"] == "space-1"


# "not-a-port" and "8125 8126" exercise one branch; keep one, plus the bounds.
@pytest.mark.parametrize("port", ["not-a-port", "0", "70000"])
def test_invalid_dogstatsd_port_warns_before_the_suites_run(port) -> None:
    environment = {"SPACE_METRICS_SINK": "datadog", "DD_DOGSTATSD_PORT": port}
    warning = smoke.telemetry_configuration_warning(environment)
    # The warning names the setting. Its exact wording is free to change, and
    # a warning never touches the verdict.
    assert warning is not None and "DD_DOGSTATSD_PORT" in warning


@pytest.mark.parametrize(
    "environment",
    [
        {"SPACE_METRICS_SINK": "datadog", "DD_DOGSTATSD_PORT": "8126"},
        {"SPACE_METRICS_SINK": "datadog"},
        # Only the Datadog sink uses the port, so other sinks ignore it. The
        # otlp sink still needs its own endpoint to be usable.
        {
            "SPACE_METRICS_SINK": "otlp",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://receiver:4317",
            "DD_DOGSTATSD_PORT": "not-a-port",
        },
        {"SPACE_METRICS_SINK": "off", "DD_DOGSTATSD_PORT": "not-a-port"},
    ],
)
def test_usable_or_unused_dogstatsd_port_is_silent(environment) -> None:
    assert smoke.telemetry_configuration_warning(environment) is None


def test_invalid_dogstatsd_port_does_not_change_the_verdict(
    metrics_environment, monkeypatch, capsys
) -> None:
    monkeypatch.setenv("SPACE_METRICS_SINK", "datadog")
    monkeypatch.setenv("DD_AGENT_HOST", "127.0.0.1")
    monkeypatch.setenv("DD_DOGSTATSD_PORT", "not-a-port")

    # Returning at all is the point: a telemetry problem is reported, never
    # raised, so the verdict the caller already computed is what it exits with.
    smoke.emit_metrics(config(), [], exit_code=smoke.EXIT_PASS)

    assert "DD_DOGSTATSD_PORT" in capsys.readouterr().err


@pytest.mark.parametrize("sink", ["bogus", "journald", "Datadog+OTLP"])
def test_selector_typo_warns_early_without_changing_the_verdict(
    sink, metrics_environment, monkeypatch, capsys
) -> None:
    monkeypatch.setenv("SPACE_METRICS_SINK", sink)
    # The early check must report it rather than raising out of main().
    warning = smoke.telemetry_configuration_warning(os.environ)
    assert warning is not None and "SPACE_METRICS_SINK" in warning

    # And emitting metrics with the same typo still cannot change a verdict.
    smoke.emit_metrics(config(), [], exit_code=smoke.EXIT_PASS)
    assert "SPACE_METRICS_SINK" in capsys.readouterr().err


@pytest.mark.parametrize(
    "environment",
    [
        {"SPACE_METRICS_SINK": "otlp"},
        {"SPACE_METRICS_SINK": "otlp", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL": "http/protobuf"},
        {
            "SPACE_METRICS_SINK": "otlp",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://receiver:4317",
        },
        {
            "SPACE_METRICS_SINK": "otlp",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://receiver:4317",
            "OTEL_EXPORTER_OTLP_HEADERS": "authorization=fixture",
        },
    ],
)
def test_unusable_otlp_configuration_warns_early(environment) -> None:
    warning = smoke.telemetry_configuration_warning(environment)
    assert warning is not None
    assert "fixture" not in warning


def test_usable_otlp_configuration_is_silent() -> None:
    assert (
        smoke.telemetry_configuration_warning(
            {
                "SPACE_METRICS_SINK": "otlp",
                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://receiver:4317",
            }
        )
        is None
    )


def test_shared_producer_attributes_are_dropped_and_recomputed(
    metrics_environment, metrics_receiver, monkeypatch
) -> None:
    receiver, endpoint = metrics_receiver
    monkeypatch.setenv("SPACE_METRICS_SINK", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", endpoint)
    monkeypatch.setenv("HOSTNAME", "smoke-host")
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.delenv("SPACE_K8S_NODE_NAME", raising=False)
    monkeypatch.delenv("KUBE_NODE_NAME", raising=False)
    monkeypatch.delenv("SPACE_K8S_POD_NAME", raising=False)
    monkeypatch.setenv(
        "OTEL_RESOURCE_ATTRIBUTES",
        "service.instance.id=shared,host.name=shared,k8s.node.name=shared,"
        "k8s.pod.name=shared,team=space%20platform",
    )

    smoke.emit_metrics(config(), [], exit_code=smoke.EXIT_PASS)

    request = receiver.requests.get(timeout=2)
    attributes = {
        item.key: item.value.string_value
        for item in request.resource_metrics[0].resource.attributes
    }
    for key in smoke.PRODUCER_ATTRIBUTE_KEYS:
        assert attributes.get(key) != "shared", key
    # Recomputed from this process, matching the Rust and Go resolvers.
    assert attributes["service.instance.id"] == f"smoke-host:{os.getpid()}"
    assert attributes["host.name"] == "smoke-host"
    assert "k8s.node.name" not in attributes
    assert "k8s.pod.name" not in attributes
    # Non-identity attributes survive, percent-decoded like the SDK.
    assert attributes["team"] == "space platform"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("space%20team", "space team"),
        ("service%2Fapi", "service/api"),
        ("100%25", "100%"),
        # A malformed escape is kept, as the Rust and Go resolvers keep it.
        ("%zz", "%zz"),
        ("%2", "%2"),
        # Bytes that are not valid UTF-8 are kept verbatim rather than turned
        # into replacement characters, which would rewrite the value silently
        # and disagree with every other producer.
        ("%ff", "%ff"),
        ("caf%c3%a9", "café"),
        ("no-escapes", "no-escapes"),
    ],
)
def test_resource_attribute_values_are_percent_decoded(raw, expected) -> None:
    attributes = smoke.parse_resource_attributes(f"team={raw}")
    assert attributes["team"] == expected
