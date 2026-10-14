#!/usr/bin/env python3
"""Certify the task-owned space-local Kind cluster; Python standard library only."""

import argparse
import fcntl
import hashlib
import json
import os
import platform
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

from assertions import (
    counter_monotonic,
    distinct_producers,
    gauge_lifecycle,
    identities,
    native_histograms,
    series,
    trace_evidence,
    verdict,
)

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
KUBE = ["kubectl", "--context", "kind-space-local", "--request-timeout=30s"]
CHARTS = ROOT / "infra/space/observability/charts"


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def clean_environment():
    # Bootstrap can consume optional cloud credentials and a watched config.
    # Neither belongs in this credential-free, explicitly selected experiment.
    return {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("SPACE_", "DD_", "OTEL_", "AWS_", "ADMIN_", "CERT_"))
        and k
        not in (
            "API_URL",
            "DATA_PLANE_URL",
            "TEMPLATE",
            "SKIP_BOOTSTRAP",
            "KIND_CLUSTER",
            "KUBECONFIG",
            "HELM_NAMESPACE",
            "GO_BIN",
            "KEEP_SANDBOX",
            "KEEP_SANDBOX_ON_SUCCESS",
        )
    }


class Certification:
    def __init__(self, args):
        self.args = args
        self.contract = json.loads((HERE / "expectations.json").read_text())
        self.output = args.artifacts.resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        if (self.output / "report.json").exists():
            raise RuntimeError(
                "use a new artifacts directory; refusing to overwrite a previous report"
            )
        self.state = args.state.resolve()
        self.state.mkdir(parents=True, exist_ok=True)
        self.env = clean_environment()
        self.env["KUBECONFIG"] = str(self.state / "kubeconfig")
        self.started = time.time()
        self.report = {
            "schema_version": 1,
            "started": self.started,
            "checks": [],
            "timings": [],
            "artifacts": str(self.output),
            "mode": args.command,
            "completed": False,
        }
        self.processes = []
        self.report["expectations"] = self.contract
        save(self.output / "expectations.json", self.contract)
        self.fixed = set(args.require_fix)
        self.source = self.state / "source"
        self.version = ""
        self.sequence = 0

    def command(self, *args, cwd=ROOT, timeout=120, env=None, capture=True):
        self.sequence += 1
        name = f"command-{self.sequence:03d}.log"
        started = time.time()
        with (
            (self.output / name).open("w") as log,
            (self.output / (name + ".stderr")).open("w") as errors,
        ):
            process = subprocess.Popen(
                [str(a) for a in args],
                cwd=cwd,
                env=env or self.env,
                stdout=log,
                stderr=errors,
                start_new_session=True,
            )
            try:
                rc = process.wait(timeout=timeout)
            except BaseException:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            finally:
                self.report["timings"].append(
                    {
                        "command": [str(a) for a in args],
                        "seconds": time.time() - started,
                        "exit_code": process.returncode,
                        "log": name,
                        "stderr": name + ".stderr",
                    }
                )
        if rc:
            raise RuntimeError(f"{args[0]} exited {rc}; see {name}")
        return (self.output / name).read_text().strip() if capture else ""

    def check(self, name, passed, detail=None, fix=None, skip=None):
        row = {
            "check": name,
            "status": "skip"
            if skip
            else verdict(passed, bool(fix and fix not in self.fixed)),
            "detail": skip or detail,
            "last_command_log": f"command-{self.sequence:03d}.log",
        }
        if fix:
            row["fix"] = f"https://github.com/perplexityai/SPACE/pull/{fix}"
            row["fix_in_revision"] = fix in self.fixed
        self.report["checks"].append(row)
        print(f"{row['status'].upper()}: {name}", flush=True)
        self.write_report()

    def write_report(self):
        self.report["ended"] = time.time()
        self.report["seconds"] = self.report["ended"] - self.started
        counts = {
            s: sum(c["status"] == s for c in self.report["checks"])
            for s in ("pass", "fail", "xfail", "skip")
        }
        self.report["counts"] = counts
        self.report["certified"] = (
            self.args.command == "run"
            and self.report["completed"]
            and bool(counts["pass"])
            and not any(counts[s] for s in ("fail", "xfail", "skip"))
        )
        save(self.output / "report.json", self.report)
        text = [
            "# SPACE OTLP certification",
            "",
            json.dumps(counts),
            "",
            "Full certification: "
            + ("PASS" if self.report["certified"] else "NOT COMPLETE"),
            "",
            f"Runtime: `{self.report.get('runtime_revision', 'not built')}`",
            f"Charts: `{self.report.get('chart_revision', 'not rendered')}`",
            f"Elapsed: {self.report['seconds']:.1f}s",
            "",
            "| Check | Result | Evidence / limitation |",
            "|---|---|---|",
        ]
        for row in self.report["checks"]:
            detail = json.dumps(row["detail"]).replace("|", "\\|").replace("\n", " ")
            if row.get("fix"):
                detail += f" [fix]({row['fix']})"
            text.append(f"| {row['check']} | {row['status']} | {detail} |")
        text += [
            "",
            "Exact image IDs, chart trees, query responses, commands and timings: `report.json` and sibling artifacts.",
            "",
        ]
        (self.output / "summary.md").write_text("\n".join(text))

    def revisions(self):
        revision = self.args.runtime_revision or self.contract["runtime_revision"]
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ValueError(
                "--runtime-revision must be a full commit SHA already fetched locally"
            )
        self.report["chart_revision"] = self.command("git", "rev-parse", "HEAD")
        if self.command(
            "git", "diff", "HEAD", "--", "infra/space/observability", "infra/space/kind"
        ):
            raise RuntimeError("chart/harness inputs must be committed")
        if not self.source.exists():
            self.command("git", "worktree", "add", "--detach", self.source, revision)
        actual = self.command("git", "rev-parse", "HEAD", cwd=self.source)
        if actual != revision or self.command(
            "git", "status", "--porcelain", cwd=self.source
        ):
            raise RuntimeError(
                "state/source must be clean and at the requested revision; use a new state directory"
            )
        self.report["runtime_revision"] = revision
        self.version = revision
        for number, fix in self.contract["fixes"].items():
            tree = ROOT if fix["tree"] == "charts" else self.source
            ancestor = (
                subprocess.run(
                    ["git", "merge-base", "--is-ancestor", fix["revision"], "HEAD"],
                    cwd=tree,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                ).returncode
                == 0
            )
            merged = self.command(
                "git",
                "log",
                "--format=%s",
                "--fixed-strings",
                f"--grep=(#{number})",
                cwd=tree,
            )
            if ancestor or merged:
                self.fixed.add(number)
        self.report["fixes_present"] = sorted(self.fixed)
        self.env.update(
            SPACE_KIND_TELEMETRY_MODE="otel",
            SPACE_KIND_DATADOG_MODE="off",
            SPACE_KIND_VERSION=self.version,
            SPACE_KIND_ENV="local-certification",
            SPACE_KIND_CLUSTER="space-local",
            SPACE_KIND_CLUSTER_ID="kind-local",
            SPACE_KIND_DATA_ROOT=str(self.state / "kind-data"),
            SPACE_KIND_CONFIG=str(self.state / "kind.yaml"),
            SPACE_KIND_RUST_ARTIFACTS="docker",
            SPACE_SEED_OUTPUT_DIR=str(self.state / "seed"),
            SPACE_CONSOLE_PASSWORD=secrets.token_urlsafe(24),
        )

    def render(self):
        charts = ["metrics", "logs", "traces"]
        if self.args.grafana:
            charts.append("grafana")
        records = {}
        for chart in charts:
            path = CHARTS / chart
            # Render under the names the real install uses, so this manifest
            # corresponds to the deployed one: release_init derives the release
            # and the namespace from the same short signal name.
            release = f"space-{chart}"
            self.command("helm", "dependency", "build", path)
            self.command("helm", "lint", path, "-f", path / "profiles/kind.yaml")
            manifest = self.command(
                "helm",
                "template",
                release,
                path,
                "--namespace",
                release,
                "-f",
                path / "profiles/kind.yaml",
            )
            (self.output / f"{chart}.yaml").write_text(manifest + "\n")
            records[chart] = {
                "tree": self.command(
                    "git", "rev-parse", f"HEAD:infra/space/observability/charts/{chart}"
                ),
                "render_sha256": hashlib.sha256(manifest.encode()).hexdigest(),
            }
        records["common"] = {
            "tree": self.command(
                "git", "rev-parse", "HEAD:infra/space/observability/charts/common"
            )
        }
        self.report["charts"] = records
        self.check("charts.lint-render", True, records)

    def preflight(self):
        for tool in (
            "docker",
            "kind",
            "kubectl",
            "helm",
            "go",
            "jq",
            "curl",
            "openssl",
            "timeout",
            "node",
        ):
            if not shutil.which(tool):
                raise RuntimeError(f"missing prerequisite: {tool}")
        self.report["tools"] = {
            name: self.command(*cmd)
            for name, cmd in {
                "docker": ["docker", "version", "--format", "{{.Server.Version}}"],
                "kind": ["kind", "version"],
                "helm": ["helm", "version", "--short"],
                "kubectl": ["kubectl", "version", "--client", "-o", "json"],
                "go": ["go", "version"],
            }.items()
        }
        if platform.system() != "Linux" or platform.machine() != "x86_64":
            raise RuntimeError("full runtime requires Linux x86_64 with KVM")
        if not Path("/dev/kvm").exists() or not stat.S_ISCHR(
            Path("/dev/kvm").stat().st_mode
        ):
            raise RuntimeError(
                "/dev/kvm is absent or not a character device; Firecracker cannot run"
            )
        self.check("preflight", True, "Linux/KVM and required commands available")

    def owned_cluster(self):
        marker = self.state / "cluster.json"
        if not marker.exists():
            raise RuntimeError(
                "no ownership record; refusing to adopt or delete space-local"
            )
        expected = json.loads(marker.read_text())
        current = self.command(
            "docker", "inspect", "space-local-control-plane", "--format", "{{.Id}}"
        )
        if current != expected["container_id"]:
            raise RuntimeError(
                "space-local container identity changed; refusing teardown"
            )
        label = self.command(
            "docker",
            "inspect",
            "space-local-control-plane",
            "--format",
            '{{index .Config.Labels "io.x-k8s.kind.cluster"}}',
        )
        if label != "space-local":
            raise RuntimeError("container is not the owned Kind cluster")

    def teardown(self):
        clusters = self.command("kind", "get", "clusters").splitlines()
        if "space-local" not in clusters:
            return
        self.owned_cluster()
        self.command("kind", "delete", "cluster", "--name", "space-local", timeout=180)
        remaining = self.command(
            "docker",
            "ps",
            "-a",
            "--filter",
            "label=io.x-k8s.kind.cluster=space-local",
            "-q",
        )
        if remaining:
            raise RuntimeError("Kind containers remain after teardown")
        (self.state / "cluster.json").unlink()
        # Retain only this run's sparse data/build caches; never prune Docker.
        self.check(
            "teardown", True, "deleted owned cluster; state/build caches retained"
        )

    def cluster(self):
        if "space-local" in self.command("kind", "get", "clusters").splitlines():
            raise RuntimeError(
                "space-local already exists: use teardown for this harness's owned cluster, or a separate host"
            )
        scripts = self.source / "infra/space/kind/scripts"
        self.command("bash", scripts / "install-fc-vendor.sh", timeout=900)
        config = self.command("bash", scripts / "render-cluster-config.sh")
        # Resolve the versioned node image to a digest before starting Kind.
        self.command("docker", "pull", self.contract["kind_image"], timeout=600)
        digest = json.loads(
            self.command(
                "docker",
                "image",
                "inspect",
                self.contract["kind_image"],
                "--format",
                "{{json .RepoDigests}}",
            )
        )[0]
        self.report["kind_image"] = digest
        try:
            self.command(
                "kind",
                "create",
                "cluster",
                "--name",
                "space-local",
                "--config",
                config,
                "--image",
                digest,
                timeout=600,
            )
        finally:
            Path(config).unlink(missing_ok=True)
            # Kind normally cleans partial creates itself. Record any surviving
            # task-owned control plane so failures before Helm still tear down.
            result = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "space-local-control-plane",
                    "--format",
                    "{{.Id}}",
                ],
                env=self.env,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                save(
                    self.state / "cluster.json", {"container_id": result.stdout.strip()}
                )
                self.cluster_created = True
        for chart in self.report["charts"]:
            if chart != "common":
                self.command(
                    "bash",
                    ROOT / f"infra/space/kind/{chart}/local.sh",
                    "up",
                    timeout=600,
                )
        self.command(
            "bash",
            scripts / "build-images.sh",
            timeout=self.args.build_timeout,
            capture=False,
        )
        self.record_build_images()
        self.command(
            "bash",
            scripts / "bootstrap.sh",
            "--skip-images",
            "--no-port-forward",
            timeout=1800,
            capture=False,
        )
        shim = self.source / ".cache/containerd-shim-space-v2"
        self.report["shim_sha256"] = hashlib.sha256(shim.read_bytes()).hexdigest()
        self.check(
            "deploy.otel",
            True,
            "SPACE + local backends ready; no Datadog backend credentials or Agent",
        )

    def record_build_images(self):
        names = [
            "space-api",
            "space-data-plane",
            "space-cluster-controller",
            "space-node-stack",
            "space-seed-publish",
            "space-ui",
            "space-worker",
            "space-rust-build-kind",
        ]
        self.report["built_images"] = {
            name: json.loads(
                self.command(
                    "docker",
                    "image",
                    "inspect",
                    name + ":local",
                    "--format",
                    '{"id":{{json .Id}},"repo_digests":{{json .RepoDigests}}}',
                )
            )
            for name in names
        }
        self.check(
            "images.known-builds",
            all(
                re.fullmatch(r"sha256:[0-9a-f]{64}", image["id"])
                for image in self.report["built_images"].values()
            ),
            "fresh builds from pinned source; local tag content IDs recorded before deployment",
        )

    def image_inventory(self, phase):
        pods = json.loads(self.command(*KUBE, "get", "pods", "-A", "-o", "json"))
        inventory = [
            {
                "namespace": p["metadata"]["namespace"],
                "pod": p["metadata"]["name"],
                "uid": p["metadata"]["uid"],
                "node": p["spec"].get("nodeName"),
                "containers": [
                    {
                        "name": c["name"],
                        "image": c["image"],
                        "imageID": c.get("imageID"),
                        "restarts": c.get("restartCount", 0),
                    }
                    for c in (
                        p.get("status", {}).get("containerStatuses", [])
                        + p.get("status", {}).get("initContainerStatuses", [])
                    )
                ],
            }
            for p in pods["items"]
        ]
        save(self.output / f"images-{phase}.json", inventory)
        self.report[f"images_{phase}"] = inventory
        return inventory

    def forward_api(self, service="space-api", remote_port=8080):
        with socket.socket() as port:
            port.bind(("127.0.0.1", 0))
            local_port = port.getsockname()[1]
        log = (self.output / f"{service}-forward-{len(self.processes)}.log").open("w")
        process = subprocess.Popen(
            [
                *KUBE,
                "-n",
                "space",
                "port-forward",
                "--address=127.0.0.1",
                f"svc/{service}",
                f"{local_port}:{remote_port}",
            ],
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        log.close()
        self.processes.append(process)
        address = f"http://127.0.0.1:{local_port}"
        for _ in range(60):
            if process.poll() is not None:
                raise RuntimeError("API port-forward exited")
            try:
                with urllib.request.urlopen(
                    address + "/healthz", timeout=2
                ) as response:
                    if response.status == 200:
                        return address
            except OSError:
                time.sleep(1)
        raise RuntimeError("API port-forward failed readiness")

    def query(self, backend, path, params=None):
        ns, service, port = {
            "prometheus": ("metrics", "prometheus", 9090),
            "loki": ("logs", "loki", 3100),
            "tempo": ("traces", "tempo", 3200),
        }[backend]
        raw = f"/api/v1/namespaces/space-{ns}/services/http:{service}:{port}/proxy{path}"
        if params:
            raw += "?" + urllib.parse.urlencode(params)
        response = json.loads(self.command(*KUBE, "get", "--raw", raw))
        save(
            self.output / f"query-{self.sequence:03d}.json",
            {"backend": backend, "path": path, "params": params, "response": response},
        )
        return response

    def metric(self, name, service, labels=None):
        selectors = {
            "service_name": service,
            "service_version": self.version,
            **(labels or {}),
        }
        match = ",".join(f"{k}={json.dumps(v)}" for k, v in selectors.items())
        if name == "*":
            match += ",__name__=~" + json.dumps(
                self.contract["identity_metric_pattern"]
            )
            name = ""
        # End timestamp and range are shared for every assertion in this phase.
        seconds = int(self.query_end - self.workload_start) + 1
        return series(
            self.query(
                "prometheus",
                "/api/v1/query",
                {"query": f"{name}{{{match}}}[{seconds}s]", "time": self.query_end},
            )
        )

    def logs(self, expression):
        return series(
            self.query(
                "loki",
                "/loki/api/v1/query_range",
                {
                    "query": expression,
                    "start": int(self.logs_start * 1e9),
                    "end": int(self.query_end * 1e9),
                    "limit": 100,
                    "direction": "forward",
                },
            )
        )

    def workload(self, mode):
        api = self.forward_api()
        events = self.output / f"events-{mode}.jsonl"
        env = {
            **self.env,
            "CERT_SOURCE": str(self.source),
            "CERT_API_URL": api,
            "CERT_EVENTS": str(events),
            "CERT_MODE": mode,
            "CERT_DATA_PLANE_URL": self.forward_api("space-data-plane", 8090),
        }
        self.command("bash", HERE / "workload.sh", timeout=1800, env=env, capture=False)
        self.check(f"workload.{mode}", True, {"mode": mode, "events": events.name})
        return [json.loads(line) for line in events.read_text().splitlines()]

    def builder_probe(self):
        pods = json.loads(
            self.command(*KUBE, "-n", "space", "get", "pods", "-o", "json")
        )["items"]
        # Runtime manager is independent: normal sandbox APIs do not call it.
        # Exercise its existing read-only CLI, as smoke-kind-e2e.sh does, so
        # requiring its trace evidence does not depend on accidental traffic.
        for pod in pods:
            name = pod["metadata"]["name"]
            if name.startswith(("space-node-stack-", "space-builder-node-stack-")):
                listing = json.loads(
                    self.command(
                        *KUBE,
                        "-n",
                        "space",
                        "exec",
                        name,
                        "-c",
                        "node-stack",
                        "--",
                        "space-rtmgr",
                        "list",
                        "--json",
                    )
                )
                self.check(
                    f"workload.runtime-manager.{name}",
                    listing.get("complete") is True,
                    {
                        "complete": listing.get("complete"),
                        "artifacts": len(listing.get("artifacts", [])),
                    },
                )
        marker = "cert-" + secrets.token_hex(12)
        for prefix, suffix in (
            ("space-node-stack-", "worker"),
            ("space-builder-node-stack-", "builder"),
        ):
            pod = next(
                p["metadata"]["name"]
                for p in pods
                if p["metadata"]["name"].startswith(prefix)
            )
            self.command(
                *KUBE,
                "-n",
                "space",
                "exec",
                pod,
                "-c",
                "node-stack",
                "--",
                "sh",
                "-c",
                'printf "%s\\n" "$1" > /proc/1/fd/1',
                "certification",
                f"{marker}-{suffix}",
            )
        return marker

    def assess(self, events, marker):
        by_service = {}
        wanted = [p["service"] for p in self.contract["producers"] if "skip" not in p]
        for producer in self.contract["producers"]:
            service = producer["service"]
            if "skip" in producer:
                for signal_name in ("metrics", "logs", "traces"):
                    self.check(f"{signal_name}.{service}", False, skip=producer["skip"])
                continue
            for name in producer["metrics"]:
                found = self.metric(name, service)
                self.check(
                    f"metrics.{service}.{name}", bool(found), {"series": len(found)}
                )
            # Inspect all product series, including gRPC peer-tagged metrics,
            # so a correct anchor cannot hide misattributed logical roles.
            rows = self.metric("*", service)
            by_service[service] = rows
            identity, emitter, cluster = identities(
                rows, service, self.version, "space-local"
            )
            self.check(
                f"identity.{service}",
                identity,
                "service.name, instance and exact version required",
            )
            instance_ids = {r["metric"].get("service_instance_id") for r in rows} - {
                None,
                "",
            }
            self.check(
                f"instances.{service}",
                len(instance_ids) >= producer["min_instances"],
                {
                    "required": producer["min_instances"],
                    "observed": sorted(instance_ids),
                },
            )
            # Missing telemetry remains a hard failure in the presence check.
            self.check(
                f"emitter.{service}",
                emitter,
                "point service must be the emitter",
                fix="362"
                if rows
                and all(r["metric"].get("service") for r in rows)
                and service != "containerd-shim-space-v2"
                else None,
            )
            self.check(
                f"cluster.{service}",
                cluster,
                "k8s.cluster.name=space-local, distinct from platform kind-local",
                fix="377"
                if rows
                and all(
                    r["metric"].get("k8s_cluster_name") in ("space-local", "kind-local")
                    for r in rows
                )
                and service != "containerd-shim-space-v2"
                else None,
            )
            expression = producer.get("logql", "{service=" + json.dumps(service) + "}")
            entries = self.logs(expression)
            self.check(
                f"logs.{service}",
                any(r.get("values") for r in entries),
                {"logql": expression, "streams": len(entries)},
            )
        self.check(
            "identity.distinct-producers",
            distinct_producers(by_service),
            "no instance identity shared by different services",
        )
        for counter in self.contract["counters"]:
            rows = self.metric(counter["name"], counter["service"])
            self.check(
                f"counter.{counter['service']}",
                counter_monotonic(rows),
                "every series needs >=3 actual points over >=20s, no resets, and some growth",
            )
        for histogram in self.contract["histograms"]:
            rows = self.metric(histogram["name"], histogram["service"])
            self.check(
                f"histogram.{histogram['service']}",
                native_histograms(rows),
                "native API histogram buckets and positive counts",
            )
        gauge = self.contract["lifecycle_gauge"]
        rows = self.metric(gauge["name"], gauge["service"], gauge["labels"])
        active = next(e["time"] for e in events if e["event"] == "active")
        deleting = next(e["time"] for e in events if e["event"] == "deleting")
        deleted = next(e["time"] for e in reversed(events) if e["event"] == "deleted")
        self.check(
            "gauge.sandbox-lifecycle",
            gauge_lifecycle(rows, active, deleting, deleted),
            "same identity positive while active, explicit fresh zero after all deletes",
        )
        control = self.logs('{namespace="space"} |= ' + json.dumps(marker + "-worker"))
        self.check(
            "logs.builder-positive-control",
            any(r.get("values") for r in control),
            "ordinary worker marker ingested",
        )
        builder = self.logs('{namespace="space"} |= ' + json.dumps(marker + "-builder"))
        self.check(
            "logs.builder-excluded",
            any(r.get("values") for r in control)
            and not any(r.get("values") for r in builder),
            {"worker_streams": len(control), "builder_streams": len(builder)},
            fix="361" if any(r.get("values") for r in control) else None,
        )
        self.traces(wanted)
        requested = self.metric(
            self.contract["requested_version_metric"], "space-template-manager"
        )
        if requested:
            versions = {r["metric"].get("version_requested") for r in requested}
            self.check(
                "identity.requested-template-version",
                len(versions - {None, "<unset>"}) >= 2 and None not in versions,
                sorted(str(v) for v in versions),
                fix="376",
            )
        else:
            self.check(
                "identity.requested-template-version",
                False,
                skip="Healthy public journey does not generate two failed template hashes; requested-version collision needs the existing semantic_parity transport fixture (fix 376).",
            )

    def traces(self, wanted):
        seen, edges = set(), []
        for service in wanted:
            expression = (
                "{resource.service.name="
                + json.dumps(service)
                + " && resource.service.version="
                + json.dumps(self.version)
                + "}"
            )
            result = self.query(
                "tempo",
                "/api/search",
                {
                    "q": expression,
                    "start": int(self.workload_start),
                    "end": int(self.query_end),
                    "limit": 20,
                },
            )
            for match in result.get("traces", []):
                trace_id = match["traceID"]
                if not re.fullmatch(r"[a-fA-F0-9]{32}", trace_id):
                    raise ValueError("Tempo returned invalid trace ID")
                trace = self.query("tempo", "/api/traces/" + trace_id)
                trace_seen, trace_edges = trace_evidence(
                    trace, trace_id, wanted, self.version
                )
                seen.update(trace_seen)
                edges.extend(trace_edges)
            self.check(
                f"traces.{service}",
                service in seen,
                {"search_matches": len(result.get("traces", []))},
            )
        self.check("traces.cross-service-parenting", bool(edges), edges[:10])

    def receiver_counts(self):
        result = {}
        for backend in ("metrics", "traces"):
            path = f"/api/v1/namespaces/space-{backend}/services/http:collector:8888/proxy/metrics"
            raw = self.command(*KUBE, "get", "--raw", path)
            for line in raw.splitlines():
                if re.match(
                    r"otelcol_receiver_accepted_(metric_points|spans)(?:_total)?[{ ]",
                    line,
                ):
                    key, value = line.rsplit(" ", 1)
                    result[f"{backend}:{key}"] = float(value)
        if not any(k.startswith("metrics:") for k in result) or not any(
            k.startswith("traces:") for k in result
        ):
            raise RuntimeError("missing receiver counters; cannot prove no traffic")
        return result

    def datadog_default(self):
        # Omitting the mode exercises its default through the real renderer.
        self.env.pop("SPACE_KIND_TELEMETRY_MODE")
        config = json.loads(
            self.command(
                "python3",
                self.source / "infra/space/kind/scripts/telemetry_config.py",
                "json",
            )
        )
        self.check(
            "datadog.default-selector", config["mode"] == "datadog", config["mode"]
        )
        self.command(
            "bash",
            self.source / "infra/space/kind/scripts/bootstrap.sh",
            "--skip-images",
            "--no-port-forward",
            timeout=1800,
            capture=False,
        )
        time.sleep(30)
        before_images = self.image_inventory("datadog-before")
        before = self.receiver_counts()
        self.workload("datadog")
        time.sleep(30)
        after = self.receiver_counts()
        after_images = self.image_inventory("datadog-after")

        def collectors(inventory):
            return sorted(
                (
                    p["namespace"],
                    p["pod"],
                    p["uid"],
                    c["name"],
                    c["imageID"],
                    c["restarts"],
                )
                for p in inventory
                if p["namespace"] in ("space-metrics", "space-traces")
                for c in p["containers"]
            )

        stable = bool(collectors(before_images)) and collectors(
            before_images
        ) == collectors(after_images)
        self.check(
            "datadog.no-otlp-traffic",
            before == after and stable,
            {
                "before": before,
                "after": after,
                "pod_images_and_restarts_stable": stable,
            },
        )

    def run(self):
        self.cluster_created = False
        try:
            if self.args.command == "teardown":
                self.teardown()
                return
            self.revisions()
            self.render()
            if self.args.command == "render":
                return
            self.preflight()
            self.logs_start = time.time()
            self.cluster()
            self.image_inventory("otel")
            self.workload_start = time.time()
            marker = self.builder_probe()
            events = self.workload("otel")
            # SDK export=10s; Tempo search excludes the latest 30s. Include the
            # final delete and Alloy cursor discovery before fixing query bounds.
            # Lifecycle gauges sample every 60s, independently of OTLP's 10s
            # reader. Wait through both periods plus backend batching.
            for _ in range(3):
                time.sleep(30)
            self.query_end = time.time()
            self.assess(events, marker)
            self.datadog_default()
            current_images = {
                name: self.command(
                    "docker", "image", "inspect", name + ":local", "--format", "{{.Id}}"
                )
                for name in self.report["built_images"]
            }
            self.check(
                "images.no-build-drift",
                all(
                    current_images[name] == data["id"]
                    for name, data in self.report["built_images"].items()
                ),
                current_images,
            )
            self.report["completed"] = True
        except (Exception, KeyboardInterrupt) as error:
            self.check("harness.execution", False, f"{type(error).__name__}: {error}")
        finally:
            for process in self.processes:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            if self.cluster_created and not self.args.keep_cluster:
                try:
                    self.teardown()
                except Exception as error:
                    self.check("teardown", False, str(error))
            if self.args.command == "run" and not any(
                c["check"] == "workload.otel" for c in self.report["checks"]
            ):
                self.check(
                    "runtime.not-exercised",
                    False,
                    "No successful Firecracker workload; no end-to-end certification claim",
                )
            self.write_report()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "render", "teardown"))
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument(
        "--state", type=Path, default=Path.home() / ".cache/space-otlp-certification"
    )
    parser.add_argument(
        "--runtime-revision", help="Full SHA; default is pinned in expectations.json"
    )
    parser.add_argument("--build-timeout", type=int, default=5400)
    parser.add_argument("--grafana", action="store_true")
    parser.add_argument("--keep-cluster", action="store_true")
    parser.add_argument(
        "--require-fix",
        action="append",
        default=[],
        choices=("361", "362", "376", "377"),
        help="Disable this fix's xfail, including for rebased/cherry-picked fixes",
    )
    args = parser.parse_args()
    os.umask(0o077)
    # A host-wide advisory lock prevents two certification runs racing to own
    # the fixed cluster name. Existing ordinary developer clusters are refused.
    lock = Path.home() / ".cache/space-otlp-certification.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("w") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run = Certification(args)

        def interrupted(_signum, _frame):
            raise KeyboardInterrupt

        signal.signal(signal.SIGTERM, interrupted)
        run.run()
    return 1 if run.report["counts"]["fail"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
