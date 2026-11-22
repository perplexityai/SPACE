#!/usr/bin/env python3
"""A standard-library-only SPACE API walkthrough for the local Linux setup.

Run from the repository root after exporting the installation guide's variables:
    python3 examples/first_sandbox.py
    python3 examples/first_sandbox.py inspect /tmp/space-first-sandbox-...
    python3 examples/first_sandbox.py delete /tmp/space-first-sandbox-...
"""

import argparse
import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
import uuid


REQUEST_TIMEOUT = 30
STATE_TIMEOUT = 300
STREAM_TIMEOUT = 45
COMMAND = {"command": ["sh", "-c", 'printf "Hello from SPACE\\n"'], "timeout": "30s"}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


# Never send management or attachment tokens through an inherited proxy or redirect.
HTTP = build_opener(ProxyHandler({}), NoRedirect())


@contextmanager
def deadline(seconds):
    """Bound the entire exchange, not just each socket read (Linux/main thread)."""
    if seconds <= 0:
        raise TimeoutError("Request deadline expired")

    def expired(*_):
        raise TimeoutError(f"Request exceeded its {seconds:g}-second deadline")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def configuration():
    config = {
        "api_url": os.environ.get("SPACE_API_URL", "http://127.0.0.1:8080").rstrip("/"),
        "data_plane_url": os.environ.get("SPACE_DATA_PLANE_URL", "http://127.0.0.1:8090").rstrip("/"),
        "token": os.environ.get("SPACE_API_TOKEN", ""),
        "template": os.environ.get("SPACE_TEMPLATE", "seed:latest"),
    }
    if not config["token"].strip():
        raise ValueError("Export SPACE_API_TOKEN using the installation guide's login step")
    for key in ("api_url", "data_plane_url"):
        parsed = urlsplit(config[key])
        if (
            parsed.scheme not in {"http", "https"} or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.path or parsed.query or parsed.fragment
        ):
            raise ValueError(f"{key} must be an HTTP(S) origin without credentials or a path")
        parsed.port  # Reject malformed port numbers.
        if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError(f"{key} requires HTTPS; plain HTTP is only allowed on loopback")
    return config


def api(config, method, path, body=None, key=None, timeout=REQUEST_TIMEOUT):
    if not config.get("token"):
        raise ValueError("Management token is missing")
    if not path.startswith("/") or path.startswith("//"):
        raise ValueError("Expected an API path, not an external URL")
    headers = {"Authorization": f"Bearer {config['token']}", "Accept": "application/json"}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    if key:
        headers["Idempotency-Key"] = key
    request = Request(config["api_url"] + path, data=data, headers=headers, method=method)
    with deadline(timeout):
        try:
            with HTTP.open(request, timeout=timeout) as response:
                result = json.load(response)
        except HTTPError as error:
            # Preserve useful API errors, including 404; none mean mutation success.
            with error:
                detail = error.read(4096).decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path}: HTTP {error.code}: {detail}") from error
    if not isinstance(result, dict):
        raise ValueError("Expected a JSON object")
    return result


def save(directory, state):
    temporary = directory / "state.json.tmp"
    temporary.write_text(json.dumps(state, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(directory / "state.json")


def require_id(state, field, prefix):
    value = state.get(field)
    if not isinstance(value, str) or not re.fullmatch(prefix + r"[A-Za-z0-9_-]+", value):
        raise ValueError(f"Missing or invalid {field}; inspect the saved request first")
    return value


def remember_ids(directory, state, response, fields):
    # Save every valid returned ID, even if another required response field is missing.
    for source, target, prefix in fields:
        value = response.get(source)
        if isinstance(value, str) and re.fullmatch(prefix + r"[A-Za-z0-9_-]+", value):
            state[target] = value
            print(f"{target}: {value}", flush=True)
    save(directory, state)
    for _, target, prefix in fields:
        require_id(state, target, prefix)


def mutation(config, directory, state, name, method, path, body=None, retry=False):
    request = state["requests"].get(name)
    wanted = {"method": method, "path": path, "body": json.loads(json.dumps(body))}
    if request is not None:
        if not retry or any(request[field] != value for field, value in wanted.items()):
            raise ValueError(f"{name} already requested; inspect its outcome before retrying")
    else:
        request = {**wanted, "key": f"first-sandbox-{name}-{uuid.uuid4().hex}"}
        state["requests"][name] = request
    save(directory, state)  # Persist the original key/body BEFORE the network call.
    return api(config, method, path, body=request["body"], key=request["key"])


def wait_for_state(config, path, target):
    end = time.monotonic() + STATE_TIMEOUT
    while True:
        remaining = end - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for {path}; inspect before retrying")
        result = api(config, "GET", path, timeout=min(REQUEST_TIMEOUT, remaining))
        state = result.get("state")
        if state == target:
            return
        if state in {"failed", "canceled", "expired", "deleted", "deleting", "stopped", "paused", "suspended"}:
            raise RuntimeError(f"{path} is {state}, expected {target}")
        if not isinstance(state, str) or not state:
            raise ValueError(f"{path} returned no state")
        time.sleep(min(1, max(0, end - time.monotonic())))


def create_sandbox(config, directory, state):
    if "create" in state["requests"]:
        raise ValueError("Creation was already requested; do not create a duplicate")
    state.pop("sandbox_id", None)
    state.pop("operation_id", None)
    body = {"template": config["template"], "runtime": "fc"}
    result = mutation(config, directory, state, "create", "POST", "/v1/sandboxes", body)
    remember_ids(directory, state, result, [
        ("sandbox_id", "sandbox_id", "sbx_"), ("operation_id", "operation_id", "op_"),
    ])
    wait_for_state(config, f"/v1/operations/{state['operation_id']}", "succeeded")
    wait_for_state(config, f"/v1/sandboxes/{state['sandbox_id']}", "running")
    ready = api(config, "GET", f"/v1/sandboxes/{state['sandbox_id']}/processes?limit=1", timeout=180)
    if not isinstance(ready.get("data"), list):
        raise ValueError("Process readiness response did not contain a list")


def event_frames(response, transcript):
    """Parse SSE data fields; ignore event/id fields, comments and heartbeats."""
    data = []
    for line in response:
        transcript.write(line)
        transcript.flush()
        line = line.rstrip(b"\r\n")
        if not line:
            if data:
                frame = json.loads(b"\n".join(data))
                if not isinstance(frame, dict):
                    raise ValueError("Expected a JSON event object")
                yield frame
                data = []
        else:
            field, _, value = line.partition(b":")
            if field == b"data":
                data.append(value[1:] if value.startswith(b" ") else value)
    if data:
        raise ValueError("Stream ended in an incomplete SSE frame")


def consume_events(response, transcript, stdout, stderr):
    completed = False
    for frame in event_frames(response, transcript):
        kind = frame.get("type")
        if kind in {"error", "resync_required"}:
            raise RuntimeError(f"Command stream reported {kind}; keep the transcript")
        if kind in {"stdout", "stderr"}:
            output = stdout if kind == "stdout" else stderr
            output.write(base64.b64decode(frame["data_base64"], validate=True))
            output.flush()
        if kind == "process_exited":
            code = frame.get("exit_code")
            if type(code) is not int or code != 0:
                raise RuntimeError(f"Command did not exit successfully: {code!r}")
            if frame.get("output_truncated") is True:
                raise RuntimeError("Command output was truncated; keep the transcript")
            completed = True
    if not completed:
        raise RuntimeError("Stream ended without a clean process_exited event")


def run_command(config, directory, state):
    sandbox_id = require_id(state, "sandbox_id", "sbx_")
    if "command" in state["requests"]:
        raise ValueError("Command was already requested; a new command is not a stream retry")
    state.pop("session_id", None)
    state.pop("process_id", None)
    command = mutation(config, directory, state, "command", "POST",
                       f"/v1/sandboxes/{sandbox_id}/commands", COMMAND)
    remember_ids(directory, state, command, [
        ("session_id", "session_id", "proc_"), ("process_id", "process_id", "proc_"),
    ])
    attach = command["attach"]
    events_path = f"/v1/sessions/{state['session_id']}/events"
    events_url = config["data_plane_url"] + events_path
    if attach["events_url"] not in {events_path, events_url}:
        raise ValueError("Unexpected events URL; do not bypass the destination check")
    token = attach["token"]
    if not isinstance(token, str) or not token:
        raise ValueError("Command attachment token is missing")
    with tempfile.NamedTemporaryFile(prefix="events-", suffix=".sse", dir=directory, delete=False) as transcript:
        state["transcripts"].append(Path(transcript.name).name)
        save(directory, state)
        print(f"Event transcript: {transcript.name}", flush=True)
        request = Request(events_url, headers={
            "Authorization": f"Bearer {token}", "Accept": "text/event-stream",
        })
        with deadline(STREAM_TIMEOUT):
            with HTTP.open(request, timeout=STREAM_TIMEOUT) as response:
                consume_events(response, transcript, sys.stdout.buffer, sys.stderr.buffer)
    print(f"Command completed successfully: {state['process_id']}", flush=True)


def inspect_run(config, state):
    paths = ["/v1/sandboxes?limit=10"]
    for field in ("operation_id", "delete_operation_id"):
        if state.get(field):
            paths.append(f"/v1/operations/{require_id(state, field, 'op_')}")
    if state.get("sandbox_id"):
        sandbox_id = require_id(state, "sandbox_id", "sbx_")
        paths += [f"/v1/sandboxes/{sandbox_id}", f"/v1/sandboxes/{sandbox_id}/processes?limit=10"]
    # One missing resource must not hide diagnostics for the remaining resources.
    for path in paths:
        print(path, flush=True)
        try:
            print(json.dumps(api(config, "GET", path), indent=2), flush=True)
        except Exception as error:
            print(str(error), file=sys.stderr)


def cleanup_transcripts(directory, state):
    for name in state["transcripts"]:
        if Path(name).name != name or not name.startswith("events-") or not name.endswith(".sse"):
            raise ValueError("Invalid transcript filename in state")
        (directory / name).unlink(missing_ok=True)
    state["transcripts"] = []
    save(directory, state)


def delete_sandbox(config, directory, state, retry=False):
    sandbox_id = require_id(state, "sandbox_id", "sbx_")
    if not state.get("deleted"):
        if not state.get("delete_operation_id"):
            result = mutation(config, directory, state, "delete", "DELETE",
                              f"/v1/sandboxes/{sandbox_id}", retry=retry)
            remember_ids(directory, state, result, [("operation_id", "delete_operation_id", "op_")])
        operation_id = require_id(state, "delete_operation_id", "op_")
        wait_for_state(config, f"/v1/operations/{operation_id}", "succeeded")
        state["deleted"] = True
        save(directory, state)
    print(f"Deleted sandbox: {sandbox_id}", flush=True)
    cleanup_transcripts(directory, state)


def confirm_delete(config, directory, state):
    sandbox_id = require_id(state, "sandbox_id", "sbx_")
    if "delete" in state["requests"]:
        print("A delete was already requested. Inspect its outcome before confirming a retry.")
    if input(f"Delete {sandbox_id} and remove its event transcripts? [y/N] ").strip().lower() == "y":
        # A returned operation is polled; an explicit retry reuses the saved key/body.
        delete_sandbox(config, directory, state, retry=True)
    else:
        print("Sandbox and transcripts kept. Use inspect/delete with this run directory.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", nargs="?", choices=("run", "inspect", "delete"), default="run")
    parser.add_argument("run_directory", nargs="?")
    args = parser.parse_args(argv)
    if (args.action == "run") == bool(args.run_directory):
        parser.error("Use run without a directory; inspect/delete require the saved run directory")
    directory = None
    try:
        config = configuration()
        if args.action == "run":
            directory = Path(tempfile.mkdtemp(prefix="space-first-sandbox-"))
            state = {"api_url": config["api_url"], "data_plane_url": config["data_plane_url"],
                     "requests": {}, "transcripts": []}
            save(directory, state)
            print(f"Run directory: {directory}", flush=True)
            create_sandbox(config, directory, state)
            run_command(config, directory, state)
            inspect_run(config, state)
            confirm_delete(config, directory, state)
        else:
            directory = Path(args.run_directory).resolve()
            state = json.loads((directory / "state.json").read_text())
            if any(state[key] != config[key] for key in ("api_url", "data_plane_url")):
                raise ValueError("Export the same API/data-plane origins used for this run")
            if args.action == "inspect":
                inspect_run(config, state)
            else:
                confirm_delete(config, directory, state)
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f"Stopped: {error or 'interrupted'}", file=sys.stderr)
        if directory:
            print(f"State/transcripts: {directory}. No additional deletion was attempted.", file=sys.stderr)
        return 130 if isinstance(error, KeyboardInterrupt) else 1


if __name__ == "__main__":
    sys.exit(main())
