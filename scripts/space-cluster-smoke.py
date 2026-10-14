#!/usr/bin/env python3
"""Run the SPACE smoke suites against the cluster this process is running in.

This is the entrypoint of the ephemeral in-cluster smoke Job. It owns suite
selection, the template matrix, one aggregated verdict, and reporting; the
selected suites own their assertions and can also be invoked directly.

The caller supplies an ECW domain that resolves from the Job and injects an API
key through the environment. Endpoint overrides are available for suites that
support them; the runner does not provision DNS, ingress, or credentials.

Exit status, matching the suites it runs so a wrapper can keep one rule:

    0   every suite passed
    42  only soft failures (a quarantined scenario warning, or an egress probe
        that still reaches an in-VPC address while that gap is open)
    1   anything else

Configuration is environment-only, because the caller is a Job spec:

    E2B_API_KEY                 required; the cluster's ECW smoke key
    E2B_DOMAIN                  required; e.g. e2b.example.test
    EGRESS_POD_IPS              required for egress; comma-separated pod IPv4 addresses
    E2B_API_URL                 endpoint override; unset when using domain-based
                                routing. Requires ECW_API_HOST; only the typed
                                suite reads the override
    ECW_API_HOST                Host header paired with E2B_API_URL
    ECW_DATA_PLANE_BASE_URL     data-plane endpoint override
    SPACE_SMOKE_TEMPLATES       comma-separated template matrix; default seed
    SPACE_SMOKE_RUNTIME_TEMPLATES   comma-separated names eligible for runtime certification; default empty
    SPACE_SMOKE_SUITES          comma-separated suite ids; default every
                                non-opt-in suite below
    SPACE_SMOKE_CLUSTER_ID      cluster identity for metrics and reports
    SECURITY_SMOKE_STRICT       1 to hard-fail the egress smoke's soft failures
    SPACE_SMOKE_SUMMARY_PATH    write the JSON run summary here as well
    SPACE_SMOKE_SLACK_WEBHOOK_URL   post the summary to Slack when set
    SPACE_SMOKE_SLACK_NOTIFY    on-failure (default) or always
    SPACE_METRICS_SINK         auto/datadog (default), otlp, or off;
                                journald is unsupported and disables metrics
    DD_AGENT_HOST/DD_DOGSTATSD_PORT  DogStatsD address; no host means no metrics
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT  OTLP endpoint, falling back to
                                OTEL_EXPORTER_OTLP_ENDPOINT; no default
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL  grpc only (generic OTLP fallback)
    OTEL_RESOURCE_ATTRIBUTES/OTEL_SERVICE_NAME  OTLP resource identity
    SPACE_SERVICE_NAME/SPACE_ENV/SPACE_VERSION  OTLP identity, outranking the
                                OTel and DD_* settings above

OTLP requires plaintext http:// gRPC and rejects nonblank signal-specific or
shared OTLP header variables. The locked project dependencies provide the SDK
in the smoke image; direct Python runs without them retain DogStatsD/off support.
Export is synchronous before exit, bounded to three seconds, and failures only
warn. Duration metrics remain gauges in both sinks pending a compatibility audit
of existing Datadog consumers; counters use delta temporality.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

# Image dependency discovery imports this module before installing the SDK.
try:
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics import Counter, Histogram, MeterProvider
    from opentelemetry.sdk.metrics.export import (
        AggregationTemporality,
        InMemoryMetricReader,
        MetricExportResult,
    )
    from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation
    from opentelemetry.sdk.resources import Resource
except ImportError:
    OTLP_AVAILABLE = False
else:
    OTLP_AVAILABLE = True

REPO_ROOT = Path(__file__).resolve().parent.parent

PASS = "pass"
WARN = "warn"
FAIL = "fail"
# Never started: the run's budget ran out before this suite's turn. Distinct
# from a failure — nothing was learned about the invariant either way.
SKIPPED = "skipped"

EXIT_PASS = 0
EXIT_SOFT_FAIL = 42
EXIT_HARD_FAIL = 1
# What a killed suite reports, matching the shell convention `timeout` uses.
# Never a soft failure: a suite that produced no verdict has not tolerated
# anything, it has told us nothing.
EXIT_TIMEOUT = 124

# A suite's failure detail is one line per failing scenario, and a cluster whose
# ECW endpoint is unreachable fails all of them at once. The full text is in the
# suite's own streamed output either way; the summary keeps a readable head of
# it so the artifact and the Slack body stay legible.
DETAILS_CHARACTER_BUDGET = 4000

# Below this, a suite cannot reach even a sandbox create, so the run reports the
# rest as skipped instead of starting work it cannot finish.
MINIMUM_SUITE_SECONDS = 60


@dataclass(frozen=True)
class Suite:
    """One smoke suite with its invocation, selection, and timeout policy."""

    id: str
    label: str
    # Arguments after the interpreter. The image installs every suite's pinned
    # dependencies into one environment at build time, so the suites run
    # directly rather than through `uv run`, which would want an index at run
    # time.
    args: tuple[str, ...]
    # A suite that reads E2B_TEMPLATE runs once per configured template; the
    # rest run once for the cluster.
    per_template: bool
    # A categorized suite may apply to only part of the cluster's configured
    # template matrix. None means every configured template.
    applicable_templates: frozenset[str] | None = None
    # Exit codes this suite uses for "ran, found only tolerated failures". Every
    # other non-zero code is a regression.
    soft_exit_codes: frozenset[int] = frozenset()
    # Suites that a default run skips. Opt in through SPACE_SMOKE_SUITES.
    default_enabled: bool = True
    # Suites that write their own failure detail for the alert body.
    reports_failure_details: bool = False
    # Optional scenario packs layered onto the portable public inventory.
    scenario_packages: tuple[str, ...] = ()
    # Ceiling for one invocation. It bounds a hang, not a healthy run: without
    # it one wedged suite consumes the Job's whole deadline, losing later suites,
    # the summary, and metrics. The run as a whole is still
    # capped by the Job's activeDeadlineSeconds, which is why these can sum past
    # it.
    timeout_seconds: int = 1800



SUITES: tuple[Suite, ...] = (
    Suite(
        id="typed",
        label="Typed ECW invariant suite",
        args=("-m", "space_smoke", "run", "--category", "ecw"),
        per_template=True,
        reports_failure_details=True,
    ),
    Suite(
        id="runtime",
        label="Typed runtime certification suite",
        args=("-m", "space_smoke", "run", "--category", "runtime"),
        per_template=True,
        applicable_templates=frozenset(),
        soft_exit_codes=frozenset({42}),
        reports_failure_details=True,
        scenario_packages=("space_smoke.private_scenarios.scenarios",),
    ),
    Suite(
        id="egress",
        label="Security smoke / egress isolation",
        args=("scripts/tests/test-sandbox-egress-isolation.py",),
        per_template=False,
        soft_exit_codes=frozenset({42}),
        timeout_seconds=900,
    ),
    Suite(
        id="network-policy",
        label="Typed ECW network-policy invariant suite",
        args=("-m", "space_smoke", "run", "--category", "network_policy"),
        per_template=False,
        default_enabled=False,
        reports_failure_details=True,
        timeout_seconds=2400,
    ),
)

SUITES_BY_ID = {suite.id: suite for suite in SUITES}


class ConfigurationError(RuntimeError):
    """The Job spec does not describe a runnable smoke."""


@dataclass(frozen=True)
class Job:
    """One suite invocation: a suite, and the template it runs against."""

    suite: Suite
    template: str | None

    @property
    def name(self) -> str:
        if self.template is None:
            return self.suite.id
        return f"{self.suite.id}/{self.template}"

    @property
    def label(self) -> str:
        if self.template is None:
            return self.suite.label
        return f"{self.suite.label} ({self.template})"


@dataclass(frozen=True)
class JobResult:
    job: Job
    exit_code: int
    outcome: str
    duration_ms: int
    details: str = ""


@dataclass(frozen=True)
class Config:
    cluster_id: str
    python: str
    domain: str
    api_key: str
    # Transport overrides. Unset selects domain-based routing, including for
    # SDKs that ignore a Host override and derive each data-plane host as
    # `{port}-{sandbox_id}.<domain>`. The caller must provide DNS and ingress.
    api_url: str | None
    api_host: str | None
    data_plane_url: str | None
    templates: tuple[str, ...]
    suites: tuple[Suite, ...]
    strict_egress: bool
    summary_path: Path | None
    slack_webhook_url: str | None
    slack_notify_always: bool
    # Wall-clock the whole plan may take. The chart derives it from the Job's
    # activeDeadlineSeconds less a reporting margin, because the per-suite
    # ceilings sum well past that deadline: without a run-level budget, enough
    # hung suites would have Kubernetes kill the Job before it could write the
    # summary and the metrics — exactly what the per-suite ceilings exist to
    # prevent.
    run_budget_seconds: int | None


def _split_list(raw: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def load_config(environment: Mapping[str, str]) -> Config:
    """Build the run configuration from the Job's environment."""

    domain = environment.get("E2B_DOMAIN", "").strip()
    if not domain:
        raise ConfigurationError("E2B_DOMAIN is required")
    api_key = environment.get("E2B_API_KEY", "").strip()
    if not api_key:
        raise ConfigurationError(
            "E2B_API_KEY is required; inject the smoke API key through the environment"
        )

    templates = _split_list(environment.get("SPACE_SMOKE_TEMPLATES", "seed"))
    if not templates:
        raise ConfigurationError("SPACE_SMOKE_TEMPLATES resolved to no templates")

    requested = _split_list(environment.get("SPACE_SMOKE_SUITES", ""))
    if requested:
        unknown = [name for name in requested if name not in SUITES_BY_ID]
        if unknown:
            raise ConfigurationError(
                f"unknown smoke suites: {', '.join(sorted(unknown))} "
                f"(known: {', '.join(sorted(SUITES_BY_ID))})"
            )
        # Keep declaration order so focused runs report suites consistently.
        suites = tuple(suite for suite in SUITES if suite.id in set(requested))
    else:
        suites = tuple(suite for suite in SUITES if suite.default_enabled)

    runtime_templates = frozenset(
        _split_list(environment.get("SPACE_SMOKE_RUNTIME_TEMPLATES", ""))
    )
    if "runtime" in requested and not runtime_templates:
        raise ConfigurationError(
            "SPACE_SMOKE_RUNTIME_TEMPLATES is required when requesting runtime; "
            "configure the certification templates explicitly"
        )
    template_names = {template.split(":", 1)[0] for template in templates}
    unknown_templates = sorted(runtime_templates - template_names)
    if unknown_templates:
        raise ConfigurationError(
            "SPACE_SMOKE_RUNTIME_TEMPLATES must be a subset of SPACE_SMOKE_TEMPLATES; "
            f"unknown: {', '.join(unknown_templates)}"
        )
    suites = tuple(
        replace(suite, applicable_templates=runtime_templates)
        if suite.id == "runtime"
        else suite
        for suite in suites
    )

    summary_path = environment.get("SPACE_SMOKE_SUMMARY_PATH", "").strip()

    run_budget_raw = environment.get("SPACE_SMOKE_RUN_BUDGET_SECONDS", "").strip()
    run_budget_seconds: int | None = None
    if run_budget_raw:
        try:
            run_budget_seconds = int(run_budget_raw)
        except ValueError as error:
            raise ConfigurationError(
                "SPACE_SMOKE_RUN_BUDGET_SECONDS must be an integer, got "
                f"{run_budget_raw!r}"
            ) from error
        if run_budget_seconds <= 0:
            raise ConfigurationError(
                "SPACE_SMOKE_RUN_BUDGET_SECONDS must be positive"
            )

    # An endpoint override without a host header reaches the API as the
    # endpoint's own host, which matches no configured ECW pair and answers 404
    # — indistinguishable from a bad API key at the caller. Refuse the pair
    # rather than default one, and refuse it here so it fails before the first
    # suite rather than 27 scenarios in.
    api_url = environment.get("E2B_API_URL", "").strip() or None
    api_host = environment.get("ECW_API_HOST", "").strip() or None
    if api_url is not None and api_host is None:
        raise ConfigurationError(
            "E2B_API_URL requires ECW_API_HOST; without it the API answers 404 "
            "and the run reads as an authentication failure"
        )

    return Config(
        cluster_id=environment.get("SPACE_SMOKE_CLUSTER_ID", "").strip() or "unknown",
        # The interpreter every suite runs under. Defaults to this process's own,
        # which in the image is the environment holding every suite's pinned
        # dependencies.
        python=environment.get("SPACE_SMOKE_PYTHON", "").strip() or sys.executable,
        domain=domain,
        api_key=api_key,
        api_url=api_url,
        api_host=api_host,
        data_plane_url=environment.get("ECW_DATA_PLANE_BASE_URL", "").strip() or None,
        templates=templates,
        suites=suites,
        strict_egress=environment.get("SECURITY_SMOKE_STRICT", "").strip() == "1",
        summary_path=Path(summary_path) if summary_path else None,
        slack_webhook_url=environment.get("SPACE_SMOKE_SLACK_WEBHOOK_URL", "").strip()
        or None,
        slack_notify_always=environment.get("SPACE_SMOKE_SLACK_NOTIFY", "").strip()
        == "always",
        run_budget_seconds=run_budget_seconds,
    )


def build_plan(config: Config) -> tuple[Job, ...]:
    """Expand the selected suites across the template matrix."""

    jobs: list[Job] = []
    for suite in config.suites:
        if suite.per_template:
            jobs.extend(
                Job(suite, template)
                for template in config.templates
                if suite.applicable_templates is None
                or template.split(":", 1)[0] in suite.applicable_templates
            )
        else:
            jobs.append(Job(suite, None))
    plan = tuple(jobs)
    if not plan:
        suite_ids = ", ".join(suite.id for suite in config.suites) or "<none>"
        templates = ", ".join(config.templates)
        raise ConfigurationError(
            "suite/template filtering produced no smoke jobs "
            f"(suites: {suite_ids}; templates: {templates})"
        )
    return plan


def suite_environment(config: Config, job: Job, details_path: Path | None) -> dict[str, str]:
    """The environment one suite process runs with."""

    environment = dict(os.environ)
    environment.update(
        {
            "E2B_API_KEY": config.api_key,
            "E2B_DOMAIN": config.domain,
            # The typed suite defaults to its local Kind backend.
            "SPACE_SMOKE_BACKEND": "deployed",
        }
    )
    # Set only when configured. Unset means every suite resolves the ECW
    # hostname itself, which is what lets the three that drive the stock SDK
    # run here unmodified.
    for name, value in (
        ("E2B_API_URL", config.api_url),
        ("ECW_API_HOST", config.api_host),
        ("ECW_DATA_PLANE_BASE_URL", config.data_plane_url),
    ):
        if value is None:
            environment.pop(name, None)
        else:
            environment[name] = value
    if job.suite.id == "runtime":
        environment["SPACE_SMOKE_RUNTIME_TEMPLATES"] = ",".join(
            sorted(job.suite.applicable_templates or ())
        )
    if job.template is not None:
        environment["E2B_TEMPLATE"] = job.template
    if config.strict_egress:
        environment["SECURITY_SMOKE_STRICT"] = "1"
    if details_path is not None:
        environment["SPACE_SMOKE_FAILURE_DETAILS_PATH"] = str(details_path)
    else:
        # Inherited from a previous suite otherwise: every suite in this process
        # shares one environment, and a stale path makes one suite's detail file
        # look like the next one's.
        environment.pop("SPACE_SMOKE_FAILURE_DETAILS_PATH", None)
    if job.suite.scenario_packages:
        environment["SPACE_SMOKE_SCENARIO_PACKAGES"] = ",".join(
            job.suite.scenario_packages
        )
    else:
        environment.pop("SPACE_SMOKE_SCENARIO_PACKAGES", None)
    return environment


def classify(suite: Suite, exit_code: int, *, strict_egress: bool) -> str:
    """Map one suite's exit code onto pass / warn / fail."""

    if exit_code == 0:
        return PASS
    if exit_code in suite.soft_exit_codes and not (
        strict_egress and suite.id == "egress"
    ):
        return WARN
    return FAIL


def run_exit_code(results: Sequence[JobResult]) -> int:
    """One status for the whole run, in the suites' own vocabulary."""

    if any(result.outcome in (FAIL, SKIPPED) for result in results):
        return EXIT_HARD_FAIL
    if any(result.outcome == WARN for result in results):
        return EXIT_SOFT_FAIL
    return EXIT_PASS


def truncate_details(details: str) -> str:
    """Keep a readable head of one suite's failure detail."""

    stripped = details.strip()
    if len(stripped) <= DETAILS_CHARACTER_BUDGET:
        return stripped
    head = stripped[:DETAILS_CHARACTER_BUDGET].rsplit("\n", 1)[0]
    remaining = len(stripped) - len(head)
    return f"{head}\n… {remaining} more characters in the suite's own output"


def skipped_result(job: Job) -> JobResult:
    """A suite the run had no budget left to start."""

    return JobResult(
        job=job,
        exit_code=EXIT_HARD_FAIL,
        outcome=SKIPPED,
        duration_ms=0,
        details="not started: the run's budget was spent before its turn",
    )


def run_job(
    config: Config, job: Job, *, runner=None, timeout_seconds: int | None = None
) -> JobResult:
    """Run one suite process, streaming its output to this process's stdout."""

    # Resolved per call rather than bound as a default, so a caller (and the
    # unit tests) can substitute the runner.
    runner = runner or subprocess.run
    details_path: Path | None = None
    temporary_directory: tempfile.TemporaryDirectory[str] | None = None
    if job.suite.reports_failure_details:
        temporary_directory = tempfile.TemporaryDirectory(prefix="space-smoke-")
        details_path = Path(temporary_directory.name) / "failure-details.txt"

    print(f"+++ {job.label}", flush=True)
    started = time.monotonic()
    try:
        details = ""
        try:
            completed = runner(
                [config.python, *job.suite.args],
                cwd=str(REPO_ROOT),
                env=suite_environment(config, job, details_path),
                check=False,
                timeout=timeout_seconds or job.suite.timeout_seconds,
            )
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            # A hang is this suite's failure, not the run's: the remaining
            # suites still run and still report.
            exit_code = EXIT_TIMEOUT
            details = (
                f"no verdict within {timeout_seconds or job.suite.timeout_seconds}s; "
                "the suite was killed and the remaining suites continued"
            )
        if exit_code != 0 and not details and details_path is not None and details_path.exists():
            details = truncate_details(
                details_path.read_text(encoding="utf-8", errors="replace")
            )
    finally:
        if temporary_directory is not None:
            temporary_directory.cleanup()

    duration_ms = int((time.monotonic() - started) * 1000)
    outcome = classify(job.suite, exit_code, strict_egress=config.strict_egress)
    print(
        f"--- {job.label}: {outcome} (exit {exit_code}, {duration_ms // 1000}s)",
        flush=True,
    )
    return JobResult(
        job=job,
        exit_code=exit_code,
        outcome=outcome,
        duration_ms=duration_ms,
        details=details,
    )


def run_plan(config: Config, plan: Sequence[Job]):
    """Run the plan, giving each suite the smaller of its ceiling and the budget."""

    started = time.monotonic()
    for index, job in enumerate(plan):
        timeout_seconds: int | None = None
        if config.run_budget_seconds is not None:
            remaining = int(config.run_budget_seconds - (time.monotonic() - started))
            # Nothing useful happens in the last few seconds of the budget, and a
            # suite started there is a sandbox created and abandoned.
            if remaining < MINIMUM_SUITE_SECONDS:
                for pending in plan[index:]:
                    print(
                        f"--- {pending.label}: skipped (run budget spent)", flush=True
                    )
                    yield skipped_result(pending)
                return
            timeout_seconds = min(job.suite.timeout_seconds, remaining)
        yield run_job(config, job, timeout_seconds=timeout_seconds)


def summary_document(
    config: Config, results: Sequence[JobResult], *, exit_code: int
) -> dict[str, object]:
    """The machine-readable run record, also the body of every report."""

    return {
        "cluster_id": config.cluster_id,
        "domain": config.domain,
        "templates": list(config.templates),
        "exit_code": exit_code,
        "outcome": {
            EXIT_PASS: PASS,
            EXIT_SOFT_FAIL: WARN,
            EXIT_HARD_FAIL: FAIL,
        }[exit_code],
        "suites": [
            {
                "suite": result.job.suite.id,
                "label": result.job.suite.label,
                "template": result.job.template,
                "outcome": result.outcome,
                "exit_code": result.exit_code,
                "duration_ms": result.duration_ms,
                "details": result.details,
            }
            for result in results
        ],
    }


@dataclass(frozen=True)
class SmokeMetric:
    name: str
    value: int
    kind: Literal["counter", "gauge"]
    attributes: Mapping[str, str]


def metric_points(
    config: Config, results: Sequence[JobResult], *, exit_code: int
) -> list[SmokeMetric]:
    metrics: list[SmokeMetric] = []
    for result in results:
        attrs = {
            "cluster_id": config.cluster_id,
            "suite": result.job.suite.id,
            "outcome": result.outcome,
        }
        if result.job.template is not None:
            attrs["template"] = result.job.template
        metrics.extend(
            (
                SmokeMetric("space.smoke.suite.result", 1, "counter", attrs),
                SmokeMetric(
                    "space.smoke.suite.duration_ms", result.duration_ms, "gauge", attrs
                ),
            )
        )
    run_attrs = {
        "cluster_id": config.cluster_id,
        "outcome": {EXIT_PASS: PASS, EXIT_SOFT_FAIL: WARN, EXIT_HARD_FAIL: FAIL}[
            exit_code
        ],
    }
    metrics.extend(
        (
            SmokeMetric("space.smoke.run.result", 1, "counter", run_attrs),
            SmokeMetric(
                "space.smoke.run.duration_ms",
                sum(result.duration_ms for result in results),
                "gauge",
                run_attrs,
            ),
        )
    )
    return metrics


def configured_metrics_sink(environment: Mapping[str, str]) -> str:
    raw = environment.get("SPACE_METRICS_SINK", "auto")
    sink = raw.strip().lower()
    if sink in {"auto", "datadog"}:
        return "datadog"
    if sink in {"otlp", "off"}:
        return sink
    if sink == "journald":
        raise ValueError(
            "SPACE_METRICS_SINK=journald is not supported by the smoke runner; use datadog, otlp, or off"
        )
    raise ValueError(
        f"invalid SPACE_METRICS_SINK {raw!r}; expected auto, datadog, otlp, or off"
    )


def otlp_metrics_endpoint(environment: Mapping[str, str]) -> str:
    for key in ("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "OTEL_EXPORTER_OTLP_HEADERS"):
        if environment.get(key, "").strip():
            raise ValueError(
                f"{key} is not allowed with the plaintext OTLP metric profile"
            )
    protocol = (
        environment.get("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "").strip()
        or environment.get("OTEL_EXPORTER_OTLP_PROTOCOL", "").strip()
        or "grpc"
    )
    if protocol.lower() != "grpc":
        raise ValueError(f"OTLP metrics require grpc, got {protocol!r}")
    endpoint = (
        environment.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "").strip()
        or environment.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    )
    if not endpoint:
        raise ValueError(
            "missing OTLP endpoint; set OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT"
        )
    rejection = otlp_endpoint_rejection(endpoint)
    if rejection:
        raise ValueError(
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or "
            f"OTEL_EXPORTER_OTLP_ENDPOINT {rejection}"
        )
    return endpoint


def _port_text(netloc: str) -> str | None:
    """The port text a netloc carries, if it carries the delimiter at all.

    An IPv6 literal keeps its own colons inside brackets, so the delimiter is
    the colon after the closing bracket.
    """
    host, bracket, rest = netloc.rpartition("]")
    if bracket:
        return rest[1:] if rest.startswith(":") else None
    _, separator, port = netloc.rpartition(":")
    return port if separator else None


def otlp_endpoint_rejection(endpoint: str) -> str | None:
    """Why an endpoint cannot be used, or None when it can.

    The rule the Rust services and the Go shim apply, so one Collector address
    works for every producer. Each message describes the requirement rather
    than repeating the value, which can embed credentials.
    """
    # Keep the Kind profile and direct smoke entry points consistent. URL
    # parsing can discard tabs/newlines, and Helm treats these separators as
    # structure when a profile forwards an endpoint through --set.
    if any(character.isspace() or character in ",\\{}" for character in endpoint):
        return "must not contain whitespace or Helm value separators"
    # A valid endpoint is scheme, host, optional port and at most a bare "/",
    # so either marker is a rejection however empty it is. urlsplit reports an
    # empty query as "" and drops an empty fragment, so read the raw text.
    if "?" in endpoint or "#" in endpoint:
        return "must not include a query or fragment"
    try:
        parsed = urllib.parse.urlsplit(endpoint)
    except ValueError:
        return "must be a URL"
    if parsed.scheme != "http":
        return "must use the plaintext http:// scheme"
    if parsed.username is not None or parsed.password is not None:
        return "must not embed credentials"
    if not parsed.hostname:
        return "must include a host"
    # An omitted port is left to the scheme default, but a port that is present
    # has to be usable. Two checks, because neither sees everything:
    # `urlsplit().port` answers None for an absent port and for an empty one
    # alike, so the text decides that; and the text only inspects the last
    # colon, so `host:4317:1234` needs the parser to reject the whole port.
    port = _port_text(parsed.netloc)
    if port is not None and not (
        port.isascii() and port.isdigit() and 1 <= int(port) <= 65535
    ):
        return "must use a port between 1 and 65535"
    try:
        parsed.port
    except ValueError:
        return "must use a port between 1 and 65535"
    if parsed.path not in ("", "/"):
        return "must not include a path"
    return None


SMOKE_SERVICE_NAME = "space-cluster-smoke"

# Identity resolves SPACE > OTel > DD > default, matching
# crates/libs/observability/src/identity.rs and the Go shim. A blank SPACE or
# OTel value is skipped so the first nonblank one wins; the Datadog fallback is
# used raw. DogStatsD output is unchanged because its formatter is untouched,
# not because this resolver preserves it.

# Identify the process, node or pod. They are always computed locally and never
# inherited, so one shared OTEL_RESOURCE_ATTRIBUTES cannot make two producers
# look like one.
PRODUCER_ATTRIBUTE_KEYS = (
    "service.instance.id",
    "host.name",
    "k8s.node.name",
    "k8s.pod.name",
)


def producer_attributes(environment: Mapping[str, str]) -> dict[str, str]:
    """This run's own producer identity, matching the Rust and Go resolvers."""
    host = (
        environment.get("HOSTNAME", "").strip()
        or environment.get("SPACE_NODE_ID", "").strip()
        or "unknown-host"
    )
    attributes = {"service.instance.id": f"{host}:{os.getpid()}"}
    node = (
        environment.get("SPACE_K8S_NODE_NAME", "").strip()
        or environment.get("KUBE_NODE_NAME", "").strip()
    )
    if node:
        attributes["host.name"] = node
        attributes["k8s.node.name"] = node
    elif "KUBERNETES_SERVICE_HOST" not in environment and host != "unknown-host":
        # In Kubernetes, HOSTNAME is the pod name and must not name a host.
        attributes["host.name"] = host
    if pod := environment.get("SPACE_K8S_POD_NAME", "").strip():
        attributes["k8s.pod.name"] = pod
    return attributes


def parse_resource_attributes(raw: str) -> dict[str, str]:
    """Parse the SDK's comma-separated key=value grammar, ignoring bad entries."""
    attributes: dict[str, str] = {}
    for entry in raw.split(","):
        key, separator, value = entry.partition("=")
        if separator:
            attributes[key.strip()] = _percent_decode(value.strip())
    return attributes


def _percent_decode(value: str) -> str:
    """Decode the SDK's percent escapes the way the Rust and Go resolvers do.

    `unquote` substitutes replacement characters for bytes that are not valid
    UTF-8, which silently rewrites the value. Keep such a value verbatim
    instead, so every producer reports the same thing.
    """
    if "%" not in value:
        return value
    try:
        return urllib.parse.unquote_to_bytes(value).decode("utf-8")
    except UnicodeDecodeError:
        return value


def _first_nonblank(*values: str) -> str:
    for value in values:
        if value.strip():
            return value.strip()
    return ""


def resolve_identity_attributes(
    environment: Mapping[str, str],
) -> dict[str, str]:
    """Resource attributes for this run, with service, environment and version
    resolved. Every other attribute the SDK detects is left untouched."""
    attributes = parse_resource_attributes(
        environment.get("OTEL_RESOURCE_ATTRIBUTES", "")
    )
    for key in PRODUCER_ATTRIBUTE_KEYS:
        attributes.pop(key, None)
    service = (
        _first_nonblank(
            environment.get("SPACE_SERVICE_NAME", ""),
            environment.get("OTEL_SERVICE_NAME", ""),
            attributes.pop("service.name", ""),
        )
        or environment.get("DD_SERVICE", "")
        or SMOKE_SERVICE_NAME
    )
    resolved = dict(attributes)
    resolved["service.name"] = service
    for key, preferred, datadog in (
        (
            "deployment.environment.name",
            _first_nonblank(
                environment.get("SPACE_ENV", ""),
                attributes.pop("deployment.environment.name", ""),
            ),
            environment.get("DD_ENV", ""),
        ),
        (
            "service.version",
            _first_nonblank(
                environment.get("SPACE_VERSION", ""),
                attributes.pop("service.version", ""),
            ),
            environment.get("DD_VERSION", ""),
        ),
    ):
        # A resource must not publish a blank attribute, so the raw Datadog
        # fallback is trimmed here even though DogStatsD keeps it verbatim.
        value = (preferred or datadog).strip()
        resolved.pop(key, None)
        if value:
            resolved[key] = value
    resolved.update(producer_attributes(environment))
    return resolved


def dogstatsd_port(environment: Mapping[str, str]) -> int:
    raw = environment.get("DD_DOGSTATSD_PORT", "").strip() or "8125"
    try:
        port = int(raw)
    except ValueError:
        raise ConfigurationError(
            f"invalid DD_DOGSTATSD_PORT {raw!r}; expected a port number"
        ) from None
    if not 1 <= port <= 65535:
        raise ConfigurationError(
            f"invalid DD_DOGSTATSD_PORT {raw!r}; expected a port number"
        )
    return port


def telemetry_configuration_warning(environment: Mapping[str, str]) -> str | None:
    """Report unusable telemetry configuration before the suites run.

    These settings are otherwise parsed only when metrics are emitted, long
    after the verdict is decided, so a selector typo used to raise there and a
    bad port used to raise inside the sender. Reporting them early never
    changes the verdict: a run with broken telemetry still reports its suites.
    """
    try:
        sink = configured_metrics_sink(environment)
        if sink == "datadog":
            dogstatsd_port(environment)
        elif sink == "otlp":
            # Validates the headers, protocol and endpoint together.
            otlp_metrics_endpoint(environment)
    except (ValueError, ConfigurationError) as error:
        return str(error)
    return None


def statsd_lines(metrics: Sequence[SmokeMetric]) -> list[str]:
    wire_types = {"counter": "c", "gauge": "g"}
    return [
        f"{metric.name}:{metric.value}|{wire_types[metric.kind]}|#"
        + ",".join(f"{key}:{value}" for key, value in metric.attributes.items())
        for metric in metrics
    ]


def emit_datadog_metrics(metrics: Sequence[SmokeMetric]) -> None:
    host = os.environ.get("DD_AGENT_HOST", "").strip()
    if not host:
        return
    port = dogstatsd_port(os.environ)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        for line in statsd_lines(metrics):
            sock.sendto(line.encode("utf-8"), (host, port))


class ExportDiagnostics(logging.Filter):
    """Collapse the SDK's per-retry logs into the run's single failure warning."""

    def __init__(self) -> None:
        super().__init__()
        self.last_error = "OTLP metrics export failed"

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.WARNING:
            self.last_error = record.getMessage()
        return False


def emit_otlp_metrics(metrics: Sequence[SmokeMetric], endpoint: str) -> None:
    if not OTLP_AVAILABLE:
        raise RuntimeError(
            "OTLP metrics require the project dependencies; run uv sync --frozen"
        )
    reader = InMemoryMetricReader(
        preferred_temporality={
            Counter: AggregationTemporality.DELTA,
            Histogram: AggregationTemporality.DELTA,
        },
        preferred_aggregation={
            Histogram: ExponentialBucketHistogramAggregation(max_size=160, max_scale=8)
        },
    )
    # Resource.create merges the OTel environment detector, which would re-add
    # the identity and producer keys the resolver just decided. Construct the
    # resource directly so the resolver is the only source.
    resource = Resource(resolve_identity_attributes(os.environ))
    provider = MeterProvider(
        metric_readers=[reader], resource=resource, shutdown_on_exit=False
    )
    exporter = None
    diagnostics = ExportDiagnostics()
    export_logger = logging.getLogger("opentelemetry.exporter.otlp.proto.grpc.exporter")
    export_logger.addFilter(diagnostics)
    try:
        exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True, timeout=3)
        meter = provider.get_meter(SMOKE_SERVICE_NAME)
        for metric in metrics:
            if metric.kind == "counter":
                meter.create_counter(metric.name, unit="1").add(
                    metric.value, metric.attributes
                )
            else:
                meter.create_gauge(metric.name, unit="ms").set(
                    metric.value, metric.attributes
                )
        # This one-shot producer has no periodic worker: collect and export all
        # results synchronously, then close the channel before the process exits.
        data = reader.get_metrics_data()
        if data is not None and exporter.export(data) != MetricExportResult.SUCCESS:
            raise RuntimeError(diagnostics.last_error)
    finally:
        try:
            if exporter is not None:
                exporter.shutdown(timeout_millis=3000)
        finally:
            try:
                provider.shutdown(timeout_millis=3000)
            finally:
                export_logger.removeFilter(diagnostics)


def emit_metrics(
    config: Config, results: Sequence[JobResult], *, exit_code: int
) -> None:
    try:
        sink = configured_metrics_sink(os.environ)
        if sink == "off":
            return
        metrics = metric_points(config, results, exit_code=exit_code)
        if sink == "otlp":
            emit_otlp_metrics(metrics, otlp_metrics_endpoint(os.environ))
        else:
            emit_datadog_metrics(metrics)
    except Exception as error:
        # Telemetry configuration and receiver failures must not change a verdict.
        print(f"warning: could not emit smoke metrics: {error}", file=sys.stderr)


def slack_message(config: Config, results: Sequence[JobResult], *, exit_code: int) -> str:
    """Slack mrkdwn for one run, in the shape the smoke alert channel already uses."""

    banner = {EXIT_PASS: ":white_check_mark:", EXIT_SOFT_FAIL: ":warning:", EXIT_HARD_FAIL: ":x:"}[
        exit_code
    ]
    verdict = {EXIT_PASS: "passed", EXIT_SOFT_FAIL: "warned", EXIT_HARD_FAIL: "failed"}[
        exit_code
    ]
    lines = [
        f"{banner} *In-cluster SPACE smoke* — `{config.cluster_id}` {verdict}",
        f"> domain: `{config.domain}` · templates: `{', '.join(config.templates)}`",
    ]
    for result in results:
        if result.outcome == PASS:
            continue
        glyph = ":warning:" if result.outcome == WARN else ":x:"
        lines.append(f"> {glyph} {result.job.label} (exit {result.exit_code})")
        if result.details:
            for detail in result.details.splitlines()[:5]:
                lines.append(f"> • {detail.strip()}")
    return "\n".join(lines)


def post_slack(config: Config, message: str) -> None:
    """Post one run summary to Slack. Never fails the run."""

    if config.slack_webhook_url is None:
        return
    payload = json.dumps({"text": message}).encode("utf-8")
    request = urllib.request.Request(
        config.slack_webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            response.read()
    except (urllib.error.URLError, OSError) as error:  # pragma: no cover - network
        print(f"warning: could not post the smoke summary to Slack: {error}", file=sys.stderr)


def report(config: Config, results: Sequence[JobResult], *, exit_code: int) -> None:
    document = summary_document(config, results, exit_code=exit_code)
    rendered = json.dumps(document, indent=2, sort_keys=True)
    print("+++ Smoke run summary", flush=True)
    print(rendered, flush=True)
    if config.summary_path is not None:
        config.summary_path.write_text(rendered + "\n", encoding="utf-8")

    emit_metrics(config, results, exit_code=exit_code)

    if config.slack_webhook_url is not None and (
        exit_code != EXIT_PASS or config.slack_notify_always
    ):
        post_slack(config, slack_message(config, results, exit_code=exit_code))


def main(argv: Sequence[str] | None = None) -> int:
    if argv:
        print(
            f"{Path(__file__).name} takes no arguments; it is configured through "
            "the environment (see the module docstring)",
            file=sys.stderr,
        )
        return EXIT_HARD_FAIL

    try:
        config = load_config(os.environ)
        plan = build_plan(config)
    except ConfigurationError as error:
        print(f"error: {error}", file=sys.stderr)
        return EXIT_HARD_FAIL

    if warning := telemetry_configuration_warning(os.environ):
        print(f"warning: {warning}", file=sys.stderr, flush=True)

    print(
        f"--- Running {len(plan)} smoke suites against {config.cluster_id} "
        f"({config.domain}) via {config.api_url}",
        flush=True,
    )

    # Every suite runs even after one fails so each applicable invariant reports
    # its own verdict.
    # What does stop the plan is the run budget — reporting is worth more than
    # the last suite of a run that has already spent its Job's whole deadline.
    results = [result for result in run_plan(config, plan)]
    exit_code = run_exit_code(results)
    report(config, results, exit_code=exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
