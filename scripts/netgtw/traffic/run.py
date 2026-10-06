#!/usr/bin/env python3
"""Run declarative traffic cases against the production space-netgtw-svc binary."""

import argparse
import fnmatch
import hashlib
import ipaddress
import json
import os
import platform
import re
import secrets
import selectors
import shutil
import ssl
import subprocess
import sys
import time
import tomllib
import xml.etree.ElementTree as ET
from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path

HERE = Path(__file__).resolve().parent
SANDBOX = "01990000-0000-7000-8000-000000000001"
PORTS = {
    "tcp": 9000,
    "udp": 9000,
    "http": 80,
    "tls": 443,
    "connect": 8445,
    "h2c": 8080,
    "http2": 8443,
    "grpc": 50051,
    "grpcs": 50052,
    "ws": 8081,
    "wss": 8444,
    "quic": 443,
    "tls-echo": 9443,
    "dns-udp": 53,
    "dns-tcp": 53,
    "ingress": 9000,
}
TOKENS = {
    "valid": 0,
    "missing": 7,
    "wrong-port": 7,
    "wrong-sandbox": 7,
    "stale-route": 7,
    "invalid-signature": 7,
}
ADDRESSES = [
    "198.18.1.2",
    "198.18.2.2",
    "198.18.53.53",
    "1.1.1.1",
    "8.8.8.8",
    "169.254.169.254",
]
STATES = {"ready": 3, "no-identity": 1, "policy-error": 4}
# NetworkState READY covers policy hydration only. With TLS
# interception on, the gateway CA and credential store are reported separately
# and guest egress fails closed per connection until they are usable.
INTERCEPTION_READINESS_READY = 2
TLS_TEST_ALPNS = {"h2", "http/1.1", "space-unsupported"}
# Per startup stage, sized for a runner still loaded by the harness's builds.
# Waits return as soon as their files exist, so a healthy run never spends it.
STARTUP_SECONDS = 30


def require(condition, message):
    if not condition:
        raise ValueError(message)


def mark_flaky(spec, row):
    if spec.get("flaky", False) and not row["passed"]:
        row["status"] = "flaky"


def fields(value, allowed, context):
    require(isinstance(value, dict), f"{context}: expected a table")
    require(
        not value.keys() - allowed,
        f"{context}: unknown fields {value.keys() - allowed}",
    )


def domain(value):
    require(
        isinstance(value, str)
        and len(value) <= 253
        and all(
            re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
            for label in value.split(".")
        ),
        f"invalid lowercase domain: {value!r}",
    )


def load_cases(directory, pattern):
    cases = []
    names = set()
    for path in sorted(directory.glob("*.toml")):
        document = tomllib.loads(path.read_text())
        fields(document, {"name", "description", "steps"}, str(path))
        name = document.get("name")
        require(
            isinstance(name, str) and name and name not in names,
            f"{path}: missing or duplicate name",
        )
        names.add(name)
        steps = document.get("steps")
        require(
            isinstance(steps, list) and steps, f"{path}: at least one step is required"
        )
        sessions = set()
        for step in steps:
            fields(step, {"policy", "state", "probes"}, name)
            state = step.setdefault("state", "ready")
            require(state in STATES, f"{name}: unknown state {state}")
            policy = step.setdefault("policy", {"default": "deny"})
            fields(
                policy,
                {"default", "allow", "deny", "tls_interception", "credential"},
                name,
            )
            require(
                policy.get("default") in ("allow", "deny"),
                f"{name}: policy.default must be allow or deny",
            )
            tls_interception = policy.setdefault("tls_interception", "off")
            require(
                tls_interception in ("off", "on"),
                f"{name}: policy.tls_interception must be off or on",
            )
            credential = policy.get("credential")
            if credential is not None:
                fields(
                    credential,
                    {
                        "exact_host",
                        "ports",
                        "path_prefix",
                        "methods",
                        "header_name",
                    },
                    name,
                )
                require(
                    tls_interception == "on",
                    f"{name}: credentials require TLS interception",
                )
                domain(credential.get("exact_host"))
                require(
                    isinstance(credential.get("ports"), list)
                    and credential["ports"]
                    and all(
                        type(port) is int and 0 < port <= 65535
                        for port in credential["ports"]
                    )
                    and len(credential["ports"]) == len(set(credential["ports"])),
                    f"{name}: credential ports must be unique valid ports",
                )
                require(
                    isinstance(credential.get("path_prefix"), str)
                    and credential["path_prefix"].startswith("/"),
                    f"{name}: credential path_prefix must be absolute",
                )
                methods = credential.get("methods")
                require(
                    isinstance(methods, list)
                    and methods
                    and all(method in ("GET", "POST") for method in methods)
                    and len(methods) == len(set(methods)),
                    f"{name}: credential methods must be GET or POST",
                )
                require(
                    isinstance(credential.get("header_name"), str)
                    and re.fullmatch(
                        r"[!#$%&'*+.^_`|~0-9a-z-]+", credential["header_name"]
                    ),
                    f"{name}: credential header_name must be lowercase HTTP syntax",
                )
            for key in ("allow", "deny"):
                rules = policy.setdefault(key, [])
                require(isinstance(rules, list), f"{name}: {key} must be an array")
                for rule in rules:
                    require(isinstance(rule, str), f"{name}: rules must be strings")
                    try:
                        ipaddress.ip_network(rule, strict=True)
                    except ValueError:
                        domain(rule.removeprefix("*."))
            probes = step.get("probes")
            require(
                isinstance(probes, list) and probes, f"{name}: every step needs probes"
            )
            for probe in probes:
                fields(
                    probe,
                    {
                        "kind",
                        "target",
                        "port",
                        "host",
                        "authority",
                        "alpn",
                        "downstream_alpn",
                        "upstream_alpn",
                        "preserve_headers",
                        "expect_credential",
                        "switch_authority",
                        "switch_expect",
                        "tunnel_host",
                        "tunnel_authority",
                        "tunnel_method",
                        "proxy_anyauth",
                        "expect",
                        "http_status",
                        "token",
                        "code",
                        "session",
                        "connection",
                        "addresses",
                        "sizes",
                        "flaky",
                    },
                    name,
                )
                kind = probe.get("kind")
                require(kind in PORTS, f"{name}: unknown kind {kind}")
                if "proxy_anyauth" in probe:
                    require(
                        kind == "connect" and type(probe["proxy_anyauth"]) is bool,
                        f"{name}: proxy_anyauth requires CONNECT and a boolean",
                    )
                if "alpn" in probe:
                    require(
                        kind in ("tls", "connect")
                        and isinstance(probe["alpn"], list)
                        and probe["alpn"]
                        and all(
                            protocol in TLS_TEST_ALPNS
                            for protocol in probe["alpn"]
                        )
                        and len(probe["alpn"]) == len(set(probe["alpn"])),
                        f"{name}: alpn requires unique supported fixture protocols",
                    )
                    if kind == "connect":
                        require(
                            not any(
                                field in probe
                                for field in (
                                    "proxy_anyauth", "tunnel_authority", "tunnel_method"
                                )
                            ),
                            f"{name}: CONNECT alpn requires an unmodified tunnel probe",
                        )
                for field in ("downstream_alpn", "upstream_alpn"):
                    if field in probe:
                        require(
                            (
                                kind == "tls"
                                or (kind == "connect" and "alpn" in probe)
                            )
                            and probe[field] in ("h2", "http/1.1"),
                            f"{name}: {field} requires a supported TLS protocol",
                        )
                if "preserve_headers" in probe:
                    headers = probe["preserve_headers"]
                    require(
                        kind in ("tls", "http2")
                        and isinstance(headers, dict)
                        and headers
                        and all(isinstance(value, str) for value in headers.values()),
                        f"{name}: preserve_headers requires HTTP header values",
                    )
                if "expect_credential" in probe:
                    require(
                        probe.get("expect") == "allow"
                        and kind in ("tls", "http2")
                        and isinstance(probe["expect_credential"], str)
                        and re.fullmatch(
                            r"[!#$%&'*+.^_`|~0-9a-z-]+",
                            probe["expect_credential"],
                        ),
                        f"{name}: expect_credential requires allowed inspected HTTP",
                    )
                for field in ("tunnel_host", "tunnel_authority", "tunnel_method"):
                    if field in probe:
                        require(kind == "connect", f"{name}: {field} requires CONNECT")
                        if field == "tunnel_method":
                            require(
                                probe[field] == "CONNECT",
                                f"{name}: unsupported tunnel method",
                            )
                        else:
                            domain(probe[field])
                if "authority" in probe:
                    require(
                        kind in ("http", "tls", "http2"),
                        f"{name}: authority requires HTTP",
                    )
                    domain(probe["authority"])
                if "switch_authority" in probe or "switch_expect" in probe:
                    require(
                        kind in ("tls", "http2")
                        and probe.get("switch_expect") in ("allow", "deny"),
                        f"{name}: invalid switch probe",
                    )
                    domain(probe.get("switch_authority"))
                target = probe.setdefault(
                    "target", "198.18.1.2" if kind != "ingress" else "169.254.0.21"
                )
                require(
                    target in ADDRESSES
                    if kind != "ingress"
                    else target == "169.254.0.21",
                    f"{name}: target has no local fixture: {target}",
                )
                port = probe.setdefault("port", PORTS[kind])
                require(
                    type(port) is int
                    and (port == PORTS[kind] or kind == "udp" and port == 443),
                    f"{name}: unsupported fixture port {port} for {kind}",
                )
                expected = probe.get("expect")
                if "sizes" in probe:
                    sizes = probe["sizes"]
                    require(
                        kind == "udp"
                        and isinstance(sizes, list)
                        and sizes
                        and all(
                            type(size) is int and 32 <= size <= 65507 for size in sizes
                        ),
                        f"{name}: sizes requires UDP datagram byte counts from 32 to 65507",
                    )
                require(
                    expected
                    in (
                        {"answer", "refused", "deny"}
                        if kind.startswith("dns-")
                        else {"allow", "deny"}
                    ),
                    f"{name}: invalid expectation {expected}",
                )
                if "flaky" in probe:
                    require(
                        type(probe["flaky"]) is bool,
                        f"{name}: flaky must be a boolean",
                    )
                if "http_status" in probe:
                    require(
                        kind in ("ws", "wss")
                        and expected == "deny"
                        and type(probe["http_status"]) is int
                        and 400 <= probe["http_status"] <= 599,
                        f"{name}: http_status requires a WebSocket deny probe and an integer from 400 to 599",
                    )
                if kind in (
                    "http",
                    "tls",
                    "tls-echo",
                    "dns-udp",
                    "dns-tcp",
                    "h2c",
                    "http2",
                    "grpc",
                    "grpcs",
                    "ws",
                    "wss",
                    "quic",
                    "connect",
                ):
                    domain(probe.get("host"))
                else:
                    require("host" not in probe, f"{name}: host is not used by {kind}")
                if "addresses" in probe:
                    addresses = probe["addresses"]
                    require(
                        kind.startswith("dns-")
                        and expected == "answer"
                        and isinstance(addresses, list)
                        and addresses,
                        f"{name}: addresses requires a DNS answer and a nonempty array",
                    )
                    for address in addresses:
                        require(
                            isinstance(address, str)
                            and ipaddress.ip_address(address).version == 4,
                            f"{name}: DNS answers must be IPv4 addresses",
                        )
                if kind == "ingress":
                    token = probe.setdefault("token", "valid")
                    require(token in TOKENS, f"{name}: invalid ingress token")
                    code = probe.setdefault(
                        "code", TOKENS[token] if state == "ready" else 9
                    )
                    require(
                        type(code) is int
                        and 0 <= code <= 16
                        and (code == 0) == (expected == "allow"),
                        f"{name}: ingress code contradicts expect",
                    )
                else:
                    require(
                        "token" not in probe and "code" not in probe,
                        f"{name}: token/code only apply to ingress",
                    )
                if "session" in probe:
                    require(
                        kind in ("tcp", "tls-echo", "tls", "http2", "wss")
                        and isinstance(probe["session"], str),
                        f"{name}: unsupported session protocol",
                    )
                    connection = probe.setdefault("connection", "new")
                    require(
                        connection in {"new", "existing"},
                        f"{name}: connection must be new or existing",
                    )
                    require(
                        (probe["session"] in sessions) == (connection == "existing"),
                        f"{name}: session {probe['session']} has incorrect connection order",
                    )
                    sessions.add(probe["session"])
                else:
                    require(
                        "connection" not in probe,
                        f"{name}: connection requires session",
                    )
        if fnmatch.fnmatchcase(name, pattern) or fnmatch.fnmatchcase(
            path.stem, pattern
        ):
            cases.append((path, document))
    require(cases, f"no cases matched {pattern!r} in {directory}")
    return cases


def execute(*args, **kwargs):
    try:
        return subprocess.run(
            [str(arg) for arg in args],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
            **kwargs,
        ).stdout
    except subprocess.CalledProcessError as error:
        raise ValueError(
            f"{error}: {error.stderr.strip()} {error.stdout.strip()}"
        ) from error


class Processes:
    def __init__(self, output):
        self.output = output
        self.stack = ExitStack()
        self.children = []

    def start(self, name, command, interactive=False, *, env=None):
        log = self.stack.enter_context((self.output / f"{name}.log").open("w"))
        child = subprocess.Popen(
            [str(arg) for arg in command],
            stdin=subprocess.PIPE if interactive else subprocess.DEVNULL,
            stdout=subprocess.PIPE if interactive else log,
            stderr=log,
            text=True,
            bufsize=1,
            env=env,
        )
        self.children.append((name, child))
        return child

    def healthy(self):
        for name, child in self.children:
            require(
                child.poll() is None,
                f"{name} exited with {child.returncode}; see {self.output / (name + '.log')}",
            )

    def close(self):
        for _, child in reversed(self.children):
            child.terminate()
        for _, child in reversed(self.children):
            try:
                child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        self.stack.close()


def send(child, request):
    child.stdin.write(json.dumps(request) + "\n")
    child.stdin.flush()
    with selectors.DefaultSelector() as selector:
        selector.register(child.stdout, selectors.EVENT_READ)
        require(
            selector.select(timeout=10),
            "probe process did not answer within 10 seconds",
        )
    line = child.stdout.readline()
    require(line, f"probe process exited unexpectedly ({child.poll()})")
    return json.loads(line)


def udp443_packets():
    chain = json.loads(execute("ip", "netns", "exec", "ngb-gw", "nft", "-j", "list", "chain", "ip", "sandbox", "pre_tproxy_guardrails"))
    return next(
        expr["counter"]["packets"]
        for entry in chain["nftables"]
        if entry.get("rule", {}).get("comment") == "platform-egress-pre-tproxy-guardrail-quic-udp443"
        for expr in entry["rule"]["expr"] if "counter" in expr
    )


def assert_result(spec, result, events):
    """Independent client result plus upstream observation; never evaluate policy here."""
    expected, kind = spec["expect"], spec["kind"]
    flow_bytes = defaultdict(str)
    observed_bytes = defaultdict(str)
    for event in events:
        if event["kind"] == "data":
            connection = tuple(
                event.get(key) for key in ("zone", "target", "port", "connection")
            )
            observed_bytes[connection] += event["data"]
    peer_events = [
        event
        for event in events
        if event.get("target") == spec.get("target")
        and event.get("port") == spec.get("port")
        and event.get("zone") == ("guest" if kind == "ingress" else "upstream")
    ]
    for event in peer_events:
        if event["kind"] == "data":
            flow_bytes[event.get("connection", "")] += event["data"]
    receipt = any(spec["nonce"] in data for data in flow_bytes.values())
    if kind == "ingress":
        require(
            result.get("code") == spec["code"],
            f"ingress result: {result}; wanted gRPC code {spec['code']}",
        )
        if expected == "allow":
            require(
                result.get("data") == spec["nonce"] and receipt,
                f"ingress payload missing: {result}",
            )
    elif kind.startswith("dns-"):
        if expected == "answer":
            require(
                result.get("received") is True
                and result.get("rcode") == 0
                and sorted(result.get("addresses", []))
                == sorted(spec.get("addresses", ["198.18.1.2"])),
                f"wanted DNS A answer: {result}",
            )
        elif expected == "refused":
            require(
                result.get("rcode") == 5 and result.get("addresses") == [],
                f"wanted DNS REFUSED: {result}",
            )
        else:
            require(
                result.get("received") is False,
                f"wanted DNS transport denial: {result}",
            )
        if expected != "answer":
            require(
                not events,
                "denied DNS question reached upstream",
            )
        return
    elif expected == "allow":
        require(
            result.get("received") is True and receipt,
            f"payload was not delivered in both directions: {result}",
        )
        if "downstream_alpn" in spec:
            negotiated = result.get("downstream_alpn", result.get("protocol"))
            require(
                negotiated == spec["downstream_alpn"],
                f"downstream selected {negotiated}; wanted {spec['downstream_alpn']}",
            )
        if "upstream_alpn" in spec:
            negotiated = [
                event.get("alpn")
                for event in peer_events
                if event["kind"] == "tls-handshake"
            ]
            require(
                negotiated == [spec["upstream_alpn"]],
                f"upstream selected {negotiated}; wanted {[spec['upstream_alpn']]}",
            )
        if kind in ("tls", "http2", "connect"):
            authority = spec.get("authority", spec["host"])
            if kind == "connect":
                authority = spec.get(
                    "tunnel_authority", spec.get("tunnel_host", "inner.test")
                )
            elif spec["port"] != 443:
                authority += f":{spec['port']}"
            require(
                any(
                    event["kind"] == "request"
                    and event.get("host") == authority
                    and all(
                        name.lower() == spec.get("expect_credential")
                        or event.get("headers", {}).get(name.lower()) == value
                        for name, value in spec.get("preserve_headers", {}).items()
                    )
                    for event in peer_events
                ),
                "origin authority or application headers changed",
            )
            if credential_header := spec.get("expect_credential"):
                requests = [event for event in peer_events if event["kind"] == "request"]
                require(
                    requests
                    and all(
                        credential_header in event.get("credential_matches", [])
                        and event.get("headers", {}).get(credential_header)
                        == "<redacted>"
                        for event in requests
                    ),
                    "origin did not receive the injected credential on every request",
                )
        if spec.get("switch_expect") == "deny":
            require(
                not any(
                    spec["nonce"] + "-forbidden" in data
                    for data in observed_bytes.values()
                ),
                "forbidden stream payload reached origin",
            )
            require(
                not any(
                    event["kind"] == "request"
                    and event.get("host", "").split(":")[0] == spec["switch_authority"]
                    for event in events
                ),
                "forbidden stream headers reached origin",
            )
        elif spec.get("switch_expect") == "allow":
            switched_authority = spec["switch_authority"]
            if spec["port"] != 443:
                switched_authority += f":{spec['port']}"
            require(
                any(
                    event["kind"] == "request"
                    and event.get("host") == switched_authority
                    and event.get("connection")
                    and spec["nonce"] + "-forbidden"
                    in flow_bytes.get(event["connection"], "")
                    for event in peer_events
                ),
                "allowed authority switch lacks matching request and payload on the same upstream connection",
            )
        if "authority" in spec:
            require(
                any(
                    event["kind"] == "request"
                    and event.get("host", "").split(":")[0] == spec["authority"]
                    for event in peer_events
                ),
                "origin did not receive the requested HTTP authority",
            )
    else:
        require(
            result.get("received") is False,
            f"denied traffic received a response: {result}",
        )
        if kind in ("ws", "wss"):
            require(
                result.get("http_status") == spec["http_status"]
                if "http_status" in spec
                else result.get("http_status", 403) == 403,
                f"unexpected HTTP status for denied WebSocket traffic: {result}",
            )
    if expected == "deny":
        inner = kind == "connect" and (
            "tunnel_authority" in spec or "tunnel_method" in spec
        )
        setup = []
        if inner:
            require(
                result.get("http_status") == 403
                and result.get("phase") == "inner_http",
                "inner denial did not come from the inspected HTTP gate",
            )
            authority = spec.get("tunnel_host", "inner.test") + ":443"
            setup = [
                event
                for event in peer_events
                if event["kind"] == "connect-request"
                and event.get("host") == authority
                and event.get("connection")
            ]
            require(
                len(setup) == 1, "inner denial never established exactly one CONNECT"
            )
            setup += [
                event
                for event in peer_events
                if event["kind"] == "connect"
                and event.get("connection") == setup[0]["connection"]
            ]
        leakage = [event for event in events if event not in setup]
        require(not leakage, f"denied traffic reached peer: {leakage}")


def evaluate_probe(spec, result, events):
    try:
        assert_result(spec, result, events)
    except ValueError as error:
        return {
            "passed": False,
            "status": "fail",
            "error": str(error),
        }
    return {"passed": True, "status": "pass"}


def suite_failed(results, fatal):
    return bool(
        fatal
        or not results
        or any(not row["passed"] and row["status"] != "flaky" for row in results)
    )


def write_reports(output, results, elapsed, fatal):
    (output / "results.json").write_text(
        json.dumps(
            {"elapsed_seconds": elapsed, "fatal": fatal, "results": results}, indent=2
        )
        + "\n"
    )
    failures = sum(
        not row["passed"] and row["status"] != "flaky" for row in results
    ) + bool(fatal)
    skipped = sum(row["status"] == "flaky" for row in results)
    suite = ET.Element(
        "testsuite",
        name="gateway-traffic",
        tests=str(len(results) + bool(fatal)),
        failures=str(failures),
        skipped=str(skipped),
        time=f"{elapsed:.3f}",
    )
    for row in results:
        case = ET.SubElement(
            suite, "testcase", name=row["name"], time=f"{row['seconds']:.3f}"
        )
        if row["status"] == "flaky":
            ET.SubElement(case, "skipped", message=row["error"]).text = json.dumps(
                row, indent=2
            )
        elif not row["passed"]:
            ET.SubElement(case, "failure", message=row["error"]).text = json.dumps(
                row, indent=2
            )
    if fatal:
        ET.SubElement(
            ET.SubElement(suite, "testcase", name="harness"), "failure", message=fatal
        )
    ET.ElementTree(suite).write(
        output / "junit.xml", encoding="unicode", xml_declaration=True
    )


def manifest(args, cases):
    def sha256(path):
        with path.open("rb") as source:
            return hashlib.file_digest(source, "sha256").hexdigest()

    result = {
        "commit": execute("git", "rev-parse", "HEAD").strip(),
        "dirty": bool(execute("git", "status", "--porcelain").strip()),
        "kernel": platform.release(),
        "gateway_sha256": sha256(args.gateway),
        "control_sha256": sha256(args.control),
        "peer_requirements_sha256": sha256(HERE / "requirements.txt"),
        "cases": [
            {"file": path.name, "sha256": sha256(path), "contents": path.read_text()}
            for path, _ in cases
        ],
    }
    if args.origin_certificate:
        result["origin_certificate_sha256"] = sha256(args.origin_certificate)
    return result


def run(args, cases):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    runtime = output / "runtime"
    runtime.mkdir(exist_ok=True)
    credential_headers = sorted(
        {
            step["policy"]["credential"]["header_name"]
            for _, case in cases
            for step in case["steps"]
            if "credential" in step["policy"]
        }
    )
    credential_file = None
    if credential_headers:
        credential_file = runtime / "network-credential"
        credential_file.write_text("Bearer traffic-" + secrets.token_hex(32))
        credential_file.chmod(0o600)
    children = Processes(output)
    started = time.monotonic()
    results, fatal = [], None
    events_path, policy_path = output / "events.jsonl", runtime / "policy.json"
    events_path.touch()
    # Keep sockaddr_un below its length limit even in long CI checkout paths.
    gateway_socket = Path("/space/run/gateway-traffic.sock")
    certificate, private_key = runtime / "certificate.pem", runtime / "tls-key.pem"
    ca_certificate, ca_key = runtime / "ca.pem", runtime / "ca-key.pem"
    ingress_key = runtime / "ingress.der"
    interception_requested = any(
        step["policy"]["tls_interception"] == "on"
        for _, case in cases
        for step in case["steps"]
    )

    def control(*command):
        return json.loads(execute(args.control, *command))

    def install(policy, state="ready"):
        document = {
            "default_action": policy["default"],
            "allow": policy.get("allow", []),
            "deny": policy.get("deny", []),
            "tls_interception": policy.get("tls_interception", "off"),
            "unavailable": state == "policy-error",
            "revision": time.monotonic_ns(),
        }
        if credential := policy.get("credential"):
            require(credential_file is not None, "credential fixture was not initialized")
            document["credential"] = {
                **credential,
                "value_file": str(credential_file),
            }
        temporary = policy_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(document))
        temporary.replace(policy_path)
        result = control("refresh", gateway_socket, policy_path)

        def dependencies_ready(result):
            if result["state"] != STATES["ready"]:
                return False
            # Probes in a TLS-interception step must not race the gateway CA
            # and credential store; the gateway holds/denies until they land.
            return (
                document["tls_interception"] != "on"
                or result.get("interception_readiness") == INTERCEPTION_READINESS_READY
            )

        if state == "no-identity":
            result = control("no-identity", gateway_socket)
        elif state == "ready" and not dependencies_ready(result):
            deadline = time.monotonic() + 10
            while not dependencies_ready(result):
                children.healthy()
                require(
                    time.monotonic() < deadline,
                    f"gateway policy dependencies did not become ready: {result}",
                )
                time.sleep(0.02)
                result = control("state", gateway_socket)
        require(result["state"] == STATES[state], f"expected state {state}: {result}")

    try:
        (output / "manifest.json").write_text(
            json.dumps(manifest(args, cases), indent=2) + "\n"
        )
        hosts = {"peer.test", "inner.test"} | {
            probe["host"]
            for _, case in cases
            for step in case["steps"]
            for probe in step["probes"]
            if "host" in probe
        }
        hosts |= {
            probe["tunnel_host"]
            for _, case in cases
            for step in case["steps"]
            for probe in step["probes"]
            if "tunnel_host" in probe
        }
        if args.origin_certificate:
            shutil.copyfile(args.origin_certificate, certificate)
            shutil.copyfile(args.origin_key, private_key)
            private_key.chmod(0o600)
            # Peer identities never change the production gateway's trust store.
            ca_certificate = Path(ssl.get_default_verify_paths().cafile)
        else:
            execute(
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=SPACE isolated traffic CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-keyout",
                ca_key,
                "-out",
                ca_certificate,
            )
            execute(
                "openssl",
                "req",
                "-new",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:P-256",
                "-nodes",
                "-subj",
                "/CN=SPACE isolated traffic peer",
                "-keyout",
                private_key,
                "-out",
                runtime / "peer.csr",
            )
            extensions = runtime / "extensions.cnf"
            extensions.write_text(
                "basicConstraints=critical,CA:FALSE\nextendedKeyUsage=serverAuth\nsubjectAltName="
                + ",".join(f"DNS:{host}" for host in sorted(hosts))
                + "\n"
            )
            execute(
                "openssl",
                "x509",
                "-req",
                "-in",
                runtime / "peer.csr",
                "-CA",
                ca_certificate,
                "-CAkey",
                ca_key,
                "-CAcreateserial",
                "-days",
                "1",
                "-extfile",
                extensions,
                "-out",
                certificate,
            )
        execute(
            "openssl",
            "genpkey",
            "-algorithm",
            "ED25519",
            "-outform",
            "DER",
            "-out",
            ingress_key,
        )
        policy_path.write_text(
            json.dumps(
                {
                    "default_action": "deny",
                    "allow": [],
                    "deny": [],
                    "tls_interception": "off",
                    "unavailable": False,
                    "revision": 1,
                }
            )
        )
        control_command = [args.control, "serve", policy_path]
        guest_ca = runtime / "guest-ca.pem"
        if interception_requested:
            control_command += ["--tls-root", guest_ca]
        children.start("host-control", control_command)
        for namespace, zone in (("ngb-upstream", "upstream"), ("ngb-client", "guest")):
            command = [
                "ip",
                "netns",
                "exec",
                namespace,
                sys.executable,
                HERE / "peer.py",
                "serve",
                "--zone",
                zone,
                "--events",
                events_path,
                "--health-events",
                output / "health-events.jsonl",
                "--certificate",
                certificate,
                "--key",
                private_key,
                "--ready",
                runtime / f"{zone}.ready",
                "--relay-socket",
                args.relay_socket,
            ]
            if zone == "upstream" and credential_file is not None:
                command += ["--credential-file", credential_file]
                for header in credential_headers:
                    command += ["--credential-header", header]
            for address in ADDRESSES:
                command += ["--address", address]
            children.start(zone, command)
        ready = [
            Path("/space/run/sandbox-host-control/host-control.sock"),
            runtime / "upstream.ready",
            runtime / "guest.ready",
        ]
        if interception_requested:
            ready.append(guest_ca)

        # Each stage gets its own budget, so slow fixtures cannot spend the
        # gateway's.
        def wait_for(paths, stage):
            deadline = time.monotonic() + STARTUP_SECONDS
            while not all(path.exists() for path in paths):
                children.healthy()
                require(
                    time.monotonic() < deadline,
                    f"{stage} startup timed out after {STARTUP_SECONDS}s",
                )
                time.sleep(0.02)

        wait_for(ready, "fixture")
        gateway = children.start(
            "gateway",
            [
                "ip",
                "netns",
                "exec",
                "ngb-gw",
                args.gateway,
                "--socket",
                gateway_socket,
                "--sandbox-id",
                SANDBOX,
                *args.gateway_arg,
            ],
            # This fixture launches the gateway directly, without the runtime
            # supervisor that supplies launch authority in production.
            env={**os.environ, "SPACE_GATEWAY_LAUNCH_CAPABILITY": secrets.token_hex(32)},
        )
        wait_for([gateway_socket], "gateway")
        namespaces = {
            name: execute(
                "ip", "netns", "exec", name, "readlink", "/proc/self/ns/net"
            ).strip()
            for name in ("ngb-client", "ngb-gw", "ngb-upstream")
        }
        gateway_namespace = os.readlink(f"/proc/{gateway.pid}/ns/net")
        require(
            len(set(namespaces.values())) == 3, "traffic namespaces must be distinct"
        )
        require(gateway_namespace == namespaces["ngb-gw"], "gateway is outside ngb-gw")
        (output / "topology.json").write_text(
            json.dumps(
                {
                    "namespaces": namespaces,
                    "gateway_pid": gateway.pid,
                    "gateway_namespace": gateway_namespace,
                    "gateway_rules": json.loads(
                        execute(
                            "ip",
                            "netns",
                            "exec",
                            "ngb-gw",
                            "nft",
                            "--json",
                            "list",
                            "ruleset",
                        )
                    ),
                },
                indent=2,
            )
            + "\n"
        )
        control("configure", gateway_socket, ingress_key)
        install({"default": "deny"})
        client = children.start(
            "client-probes",
            [
                "ip",
                "netns",
                "exec",
                "ngb-client",
                sys.executable,
                HERE / "peer.py",
                "probe-server",
            ],
            True,
        )
        direct = children.start(
            "direct-probes",
            [
                "ip",
                "netns",
                "exec",
                "ngb-upstream",
                sys.executable,
                HERE / "peer.py",
                "probe-server",
            ],
            True,
        )
        for path, case in cases:
            send(client, {"reset": True})
            install({"default": "deny"})
            for step_index, step in enumerate(case["steps"], 1):
                install(step["policy"], step["state"])
                for probe_index, probe in enumerate(step["probes"], 1):
                    name = f"{path.stem}/{step_index}.{probe_index} {probe['kind']} {probe['expect']}"
                    tick = time.monotonic()
                    spec = dict(
                        probe,
                        nonce="traffic-" + secrets.token_hex(12),
                        certificate=str(ca_certificate),
                    )
                    if spec["kind"] == "connect":
                        # Only the outer proxy TLS is intercepted by SPACE.
                        spec["tunnel_certificate"] = str(ca_certificate)
                    if spec["kind"].startswith("dns-") and spec["expect"] == "answer":
                        # Leave room for the production resolver's lookup budget.
                        spec["timeout"] = 9
                    row = {
                        "name": name,
                        "passed": False,
                        "status": "fail",
                        "case": str(path),
                        "step": step_index,
                        "probe": probe_index,
                        "spec": spec,
                    }
                    try:
                        children.healthy()
                        # Reach the very same socket independently of the gateway before
                        # accepting any verdict, including every negative assertion.
                        baseline = dict(
                            spec,
                            kind="tcp" if spec["kind"] == "ingress" else spec["kind"],
                            nonce="health-" + secrets.token_hex(12),
                        )
                        baseline.pop("session", None)
                        baseline.pop("connection", None)
                        baseline.pop("switch_authority", None)
                        baseline.pop("switch_expect", None)
                        baseline.pop("tunnel_authority", None)
                        baseline.pop("tunnel_method", None)
                        if spec["kind"].startswith("dns-") and spec["target"] in (
                            "1.1.1.1",
                            "8.8.8.8",
                        ):
                            baseline.update(dns_over_tls=True, timeout=9)
                        health = send(
                            client if spec["kind"] == "ingress" else direct, baseline
                        )
                        require(
                            health.get("received") is True,
                            f"fixture connectivity failed: {health}",
                        )
                        if step["policy"]["tls_interception"] == "on":
                            spec["certificate"] = str(guest_ca)
                        offset = events_path.stat().st_size
                        count_udp443 = spec["kind"] == "udp" and spec["port"] == 443
                        before = udp443_packets() if count_udp443 else 0
                        if spec["kind"] == "ingress":
                            result = control(
                                "ingress",
                                gateway_socket,
                                ingress_key,
                                spec["port"],
                                spec["token"],
                                spec["nonce"],
                            )
                        else:
                            result = send(client, spec)
                        # Drain peer event callbacks before checking the append-only log.
                        time.sleep(0.02)
                        with events_path.open() as events_file:
                            events_file.seek(offset)
                            events = [json.loads(line) for line in events_file]
                        row.update(result=result, events=events)
                        if count_udp443:
                            row["udp443_counter_delta"] = udp443_packets() - before
                            require(row["udp443_counter_delta"] > 0, "UDP/443 guardrail counter did not increment")
                        children.healthy()
                        row.update(evaluate_probe(spec, result, events))
                    except (ValueError, subprocess.SubprocessError, OSError) as error:
                        row["error"] = str(error)
                    mark_flaky(spec, row)
                    row["seconds"] = time.monotonic() - tick
                    results.append(row)
                    print(
                        f"{row['status'].upper()} {name} ({row['seconds']:.3f}s)"
                        + (f": {row['error']}" if not row["passed"] else ""),
                        flush=True,
                    )
        children.healthy()
    except (
        ValueError,
        subprocess.SubprocessError,
        OSError,
        KeyboardInterrupt,
    ) as error:
        fatal = f"{type(error).__name__}: {error}"
        print(f"HARNESS FAILED: {fatal}", file=sys.stderr)
    finally:
        children.close()
        elapsed = time.monotonic() - started
        write_reports(output, results, elapsed, fatal)
    failed = suite_failed(results, fatal)
    outcome = "FAILED" if failed else "PASSED"
    passed = sum(row["status"] == "pass" for row in results)
    flaky = sum(row["status"] == "flaky" for row in results)
    failures = sum(row["status"] == "fail" for row in results)
    print(
        f"{outcome}: {passed} passed, {flaky} flaky, {failures} failed "
        f"of {len(results)} probes "
        f"in {elapsed:.2f}s; artifacts: {output}"
    )
    return int(failed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, default=HERE / "cases")
    parser.add_argument("--filter", default="*", help="case name or filename glob")
    parser.add_argument(
        "--list",
        action="store_true",
        help="validate and list cases without building or root",
    )
    parser.add_argument("--gateway", type=Path)
    parser.add_argument(
        "--gateway-arg",
        action="append",
        default=[],
        help="extra gateway argument; pass as --gateway-arg=--flag",
    )
    parser.add_argument("--control", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--relay-socket", type=Path)
    parser.add_argument(
        "--origin-certificate", type=Path, help="certificate chain served by test peers"
    )
    parser.add_argument(
        "--origin-key", type=Path, help="private key served by test peers"
    )
    args = parser.parse_args()
    try:
        require(
            bool(args.origin_certificate) == bool(args.origin_key),
            "origin peer certificate and key must be supplied together",
        )
        cases = load_cases(args.cases, args.filter)
    except (ValueError, OSError) as error:
        parser.error(str(error))
    if args.list:
        for path, case in cases:
            print(
                f"{path.name}: {case['name']} ({sum(len(step['probes']) for step in case['steps'])} probes)"
            )
        return 0
    require(
        args.gateway and args.control and args.output,
        "use run.sh to build and enter isolated namespaces",
    )
    require(
        os.environ.get("SPACE_TRAFFIC_ISOLATED") == "1",
        "use run.sh for namespace isolation",
    )
    return run(args, cases)


if __name__ == "__main__":
    sys.exit(main())
