#!/usr/bin/env python3
"""Opt-in Kind integration evidence using SPACE's existing logging_process fixture.

Creates only owned fixture Jobs and restarts only this chart's Alloy DaemonSet.
Copies the exact emitted records into the Kind node journal (not Loki's API).
Leaves completed Jobs until their logs and source/collector/stored records are saved.
"""

import argparse
import json
import re
import subprocess
import time
import urllib.parse
from collections import Counter
from pathlib import Path

KUBE = ["kubectl", "--context", "kind-space-local", "-n", "space-logs"]
HARNESS = Path(__file__).resolve().parents[4] / "infra/space/kind/logs"
NODE = "space-local-worker"
CHILD = [
    "/usr/local/bin/logging_process",
    "--exact",
    "logging_child",
    "--nocapture",
    "--quiet",
]


def command(*args, input=None, timeout=60):
    return subprocess.run(
        args, input=input, capture_output=True, text=True, check=True, timeout=timeout
    ).stdout


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def records(text):
    return [line for line in text.splitlines() if line.startswith("{")]


def query(expression, start):
    params = urllib.parse.urlencode(
        {
            "query": expression,
            "start": int(start * 1e9),
            "end": time.time_ns(),
            "limit": 5000,
            "direction": "forward",
        }
    )
    url = (
        "/api/v1/namespaces/space-logs/services/http:loki:3100/proxy/loki/api/v1/query_range?"
        + params
    )
    result = json.loads(command(*KUBE, "get", "--raw", url))
    if result["status"] != "success":
        raise RuntimeError(result)
    return result


def lines(result):
    return [
        value[1] for stream in result["data"]["result"] for value in stream["values"]
    ]


def until(predicate, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise TimeoutError("Bounded collection/readiness wait expired")
        time.sleep(1)


def collect(expression, start, expected, timeout=60):
    """Return partial evidence on a delivery timeout so loss is reported explicitly."""
    deadline = time.monotonic() + timeout
    while True:
        result = query(expression, start)
        if set(expected) <= set(lines(result)) or time.monotonic() >= deadline:
            return result
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run", required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,15}", args.run):
        parser.error("run must be a short lowercase Kubernetes name")
    command("bash", str(HARNESS / "local.sh"), "status")
    values = json.loads(
        command(
            "helm",
            "get",
            "values",
            "space-logs",
            "--kube-context",
            "kind-space-local",
            "-n",
            "space-logs",
            "-o",
            "json",
        )
    )
    if not values.get("validationEcho"):
        parser.error("Enable validationEcho for the three-stage comparison")
    args.output.mkdir(parents=True, exist_ok=False)
    start = time.time()
    checks = []
    jobs = []
    provenance = {
        "start": start,
        "run": args.run,
        "image": args.image,
        "context": "kind-space-local",
    }
    save(args.output / "run.json", provenance)

    def check(name, passed, observed):
        row = {
            "check": name,
            "status": "passed" if passed else "failed",
            "observed": observed,
        }
        checks.append(row)
        print(json.dumps(row), flush=True)
        save(args.output / "checks.json", checks)

    def launch(case, settings, context=False, stderr=False, repeat=False):
        name = f"logs-fixture-{args.run}-{case}"
        env = {
            "SPACE_LOGGING_TEST_CHILD": "context" if context else "plain",
            "SPACE_METRICS_SINK": "off",
            "SPACE_CLUSTER_ID": "fixture-cluster",
            "RUST_LOG": "info",
            **settings,
        }
        invocation = " ".join(CHILD) + (" >&2" if stderr else "")
        if repeat:
            invocation = (
                "i=0; while [ $i -lt 45 ]; do "
                + invocation
                + "; i=$((i+1)); sleep 1; done"
            )
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": name,
                "labels": {"space-local-owner": "logs", "logs-run": args.run},
            },
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": 180,
                "template": {
                    "metadata": {
                        "labels": {
                            "app": "logs-fixture",
                            "space-local-owner": "logs",
                            "logs-run": args.run,
                        }
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "nodeSelector": {"kubernetes.io/hostname": NODE},
                        "automountServiceAccountToken": False,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65534,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "fixture",
                                "image": args.image,
                                "imagePullPolicy": "Never",
                                "command": ["/bin/sh", "-ec", invocation],
                                "env": [
                                    {"name": k, "value": v} for k, v in env.items()
                                ],
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "resources": {
                                    "requests": {"cpu": "10m", "memory": "16Mi"},
                                    "limits": {
                                        "cpu": "250m",
                                        "memory": "128Mi",
                                        "ephemeral-storage": "32Mi",
                                    },
                                },
                            }
                        ],
                    },
                },
            },
        }
        command(*KUBE, "create", "-f", "-", input=json.dumps(job))
        jobs.append(name)
        save(args.output / "jobs.json", jobs)
        save(args.output / (case + "-job.json"), job)

        def running_pod():
            items = json.loads(
                command(*KUBE, "get", "pods", "-l", f"job-name={name}", "-o", "json")
            )["items"]
            return next(
                (
                    p["metadata"]["name"]
                    for p in items
                    if p["status"]["phase"] in ("Running", "Succeeded")
                ),
                None,
            )

        return name, until(running_pod)

    legacy = {
        "DD_SERVICE": "legacy",
        "DD_ENV": "testing",
        "DD_VERSION": "legacy-revision",
    }
    neutral = {
        "SPACE_LOG_SERVICE": "neutral",
        "SPACE_LOG_ENV": "testing",
        "SPACE_LOG_VERSION": "neutral-revision",
    }
    cases = [
        ("default", {}, "fixture-service", None, None, False, False),
        ("legacy", legacy, "legacy", "testing", "legacy-revision", False, False),
        ("neutral", neutral, "neutral", "testing", "neutral-revision", False, False),
        (
            "conflict",
            {**legacy, **neutral, "SPACE_LOG_ENV": "prod"},
            "neutral",
            "prod",
            "neutral-revision",
            False,
            False,
        ),
        (
            "blank",
            {**legacy, **{k: " \t" for k in neutral}},
            "legacy",
            "testing",
            "legacy-revision",
            False,
            False,
        ),
        (
            "trimmed",
            {
                **legacy,
                "SPACE_LOG_SERVICE": " neutral ",
                "SPACE_LOG_ENV": " TESTING ",
                "SPACE_LOG_VERSION": " neutral-revision ",
            },
            "neutral",
            "TESTING",
            "neutral-revision",
            False,
            False,
        ),
        (
            "experimental",
            {**legacy, **neutral, "SPACE_LOG_ENV": "experimental"},
            "neutral",
            "experimental",
            "neutral-revision",
            False,
            False,
        ),
        (
            "production",
            {**legacy, **neutral, "SPACE_LOG_ENV": "production"},
            "neutral",
            "production",
            "neutral-revision",
            False,
            False,
        ),
        (
            "legacy-prod",
            {**legacy, "DD_ENV": "prod"},
            "legacy",
            "prod",
            "legacy-revision",
            False,
            False,
        ),
        (
            "allblank",
            {k: " \t" for k in [*legacy, *neutral]},
            "fixture-service",
            None,
            None,
            False,
            False,
        ),
        (
            "net-gateway",
            {**neutral, "SPACE_LOG_SERVICE": "space-net-gateway"},
            "space-net-gateway",
            "testing",
            "neutral-revision",
            True,
            False,
        ),
        (
            "spaced-host",
            {
                **neutral,
                "SPACE_LOG_SERVICE": "space-spaced-host",
                "SPACE_LOG_ENV": "experimental",
            },
            "space-spaced-host",
            "experimental",
            "neutral-revision",
            True,
            False,
        ),
        ("stderr", neutral, "neutral", "testing", "neutral-revision", False, True),
    ]
    expected = {}
    for case, settings, service, environment, version, context, stderr in cases:
        job, pod = launch(case, settings, context, stderr)
        command(
            *KUBE,
            "wait",
            "--for=condition=complete",
            "job/" + job,
            "--timeout=60s",
            timeout=70,
        )
        output = command(*KUBE, "logs", pod, "--tail=-1")
        (args.output / (case + "-application.log")).write_text(output)
        emitted = records(output)
        expected[case] = emitted
        parsed = [json.loads(line) for line in emitted]
        ordinary = next((r for r in parsed if r.get("fixture") == "ordinary"), {})
        check(
            case + ": application identity",
            ordinary.get("service") == service
            and ordinary.get("env") == environment
            and ordinary.get("version") == version,
            {k: ordinary.get(k) for k in ("service", "env", "version")},
        )
        check(
            case + ": typed fields and parent span",
            ordinary.get("count") == 7
            and type(ordinary.get("count")) is int
            and ordinary.get("signed") == -7
            and ordinary.get("ratio") == 1.25
            and ordinary.get("ready") is True
            and ordinary.get("span", {}).get("attempt") == 2
            and ordinary.get("sandbox_id")
            == ("synthetic-sandbox" if context else None),
            ordinary,
        )
        details = [
            r for r in parsed if r.get("message") == "failed gRPC request parameters"
        ]
        permitted = environment is not None and environment.lower() == "testing"
        check(
            case + ": request detail boundary and filtering",
            len(details) == int(permitted)
            and all(
                r.get("grpc.request.params") == '{"safe":true}'
                and r.get("grpc.request.params_bytes") == 13
                and r.get("grpc.request.params_truncated") is False
                for r in details
            )
            and "suppressed fixture" not in output,
            {"details": len(details), "permitted": permitted},
        )
        # Exactly the application's JSON bytes traverse journald's MESSAGE field.
        command(
            "docker",
            "exec",
            "-i",
            NODE,
            "systemd-cat",
            "--identifier=space-oss-log-fixture",
            "--priority=info",
            input="\n".join(emitted) + "\n",
        )
        for source in ("container", "journal"):
            expression = '{cluster="fixture-cluster",log_source=' + json.dumps(source) + "}"
            result = collect(expression, start, emitted)
            save(args.output / f"{case}-{source}-loki.json", result)
            matched = Counter(line for line in lines(result) if line in emitted)
            positive = any(
                json.loads(line).get("fixture") == "ordinary" for line in matched
            )
            check(
                case + ": " + source + " exact bytes and positive control",
                set(matched) == set(emitted)
                and all(n == 1 for n in matched.values())
                and positive,
                {
                    "expected": len(emitted),
                    "stored": sum(matched.values()),
                    "ordinary": positive,
                    "missing": len(set(emitted) - set(matched)),
                    "duplicates": sum(n - 1 for n in matched.values()),
                },
            )

    # Save per-node collector output before a rollout removes the old pods.
    def collector_snapshot(suffix):
        pods = json.loads(
            command(*KUBE, "get", "pods", "-l", "app=alloy", "-o", "json")
        )["items"]
        save(args.output / (suffix + "-alloy-pods.json"), pods)
        for pod in pods:
            name = pod["metadata"]["name"]
            (args.output / f"{suffix}-{name}.log").write_text(
                command(*KUBE, "logs", name, "--tail=-1")
            )
        return pods

    collector_snapshot("before-restart")
    for case, emitted in expected.items():
        echoed = []
        for path in args.output.glob("before-restart-alloy-*.log"):
            echoed.extend(
                json.loads(line).get("entry") for line in records(path.read_text())
            )
        counts = Counter(line for line in echoed if line in emitted)
        check(
            case + ": collector output preserves both transport copies",
            all(counts[line] == 2 for line in emitted),
            dict(Counter(counts.values())),
        )

    restart_start = time.time()
    job, pod = launch(
        "restart",
        {
            "SPACE_LOG_SERVICE": "space-oss-restart",
            "SPACE_LOG_ENV": "experimental",
            "SPACE_LOG_VERSION": args.run,
        },
        repeat=True,
    )
    # Keep journal traffic flowing independently while Alloy rolls on the node.
    follow = subprocess.Popen([*KUBE, "logs", "-f", pod], stdout=subprocess.PIPE)
    journal = subprocess.Popen(
        [
            "docker",
            "exec",
            "-i",
            NODE,
            "systemd-cat",
            "--identifier=space-oss-log-fixture",
            "--priority=info",
        ],
        stdin=follow.stdout,
    )
    follow.stdout.close()
    try:
        until(
            lambda: lines(
                query('{log_source="journal",service="space-oss-restart"}', restart_start)
            )
        )
        event = {"requested": time.time()}
        command("bash", str(HARNESS / "local.sh"), "restart", timeout=190)
        event["ready"] = time.time()
        save(args.output / "restart.json", event)
        command(
            *KUBE,
            "wait",
            "--for=condition=complete",
            "job/" + job,
            "--timeout=90s",
            timeout=100,
        )
        follow.wait(timeout=15)
        journal.wait(timeout=15)
    finally:
        for process in (follow, journal):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
    if follow.returncode or journal.returncode:
        raise RuntimeError("Journal bridge failed")
    output = command(*KUBE, "logs", pod, "--tail=-1")
    (args.output / "restart-application.log").write_text(output)
    emitted = records(output)
    for source in ("container", "journal"):
        # Explicit event-service overrides intentionally use another stream label.
        expression = '{cluster="fixture-cluster",log_source=' + json.dumps(source) + "}"
        result = collect(expression, restart_start, emitted)
        save(args.output / f"restart-{source}-loki.json", result)
        matched = Counter(line for line in lines(result) if line in emitted)
        check(
            "restart: " + source + " observed delivery",
            set(matched) == set(emitted),
            {
                "emitted": len(emitted),
                "stored": sum(matched.values()),
                "missing": len(set(emitted) - set(matched)),
                "duplicates": sum(n - 1 for n in matched.values()),
            },
        )
    collector_snapshot("after-restart")
    save(args.output / "jobs.json", jobs)
    save(
        args.output / "run.json",
        {**provenance, "end": time.time(), "cases": len(cases)},
    )
    save(
        args.output / "real-space-loki.json",
        query('{namespace="space"} | json | __error__=""', start),
    )
    raise SystemExit(1 if any(row["status"] == "failed" for row in checks) else 0)


if __name__ == "__main__":
    main()
