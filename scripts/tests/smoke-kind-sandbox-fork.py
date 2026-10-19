#!/usr/bin/env python3
"""Public ECW smoke for durable sandbox fork on the local KinD/KVM bench."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


API_URL = os.environ.get("E2B_API_URL", "http://127.0.0.1:8080").rstrip("/")
API_HOST = os.environ.get("ECW_API_HOST", "api.local.e2b.test")
API_KEY = os.environ.get("E2B_API_KEY")
KUBE_CONTEXT = os.environ.get("KUBE_CONTEXT", "kind-space-local")
KUBE_NAMESPACE = os.environ.get("KUBE_NAMESPACE", "space")
TEMPLATE = os.environ.get("E2B_TEMPLATE", "seed")
DATA_PLANE_URL = os.environ.get("E2B_DATA_PLANE_URL", "http://127.0.0.1:8090").rstrip("/")
DATA_DOMAIN = os.environ.get("ECW_DATA_DOMAIN", "local.e2b.test")


def psql(sql: str) -> None:
    subprocess.run(
        [
            "kubectl",
            "--context",
            KUBE_CONTEXT,
            "--namespace",
            KUBE_NAMESPACE,
            "exec",
            "-i",
            "space-postgres-0",
            "--",
            # Loopback TLS, as kind_stack_psql connects: the server refuses
            # unencrypted TCP.
            "env",
            "PGHOST=127.0.0.1",
            "PGSSLMODE=require",
            "psql",
            "-U",
            "spaceuser",
            "-d",
            "spacedb",
            "-v",
            "ON_ERROR_STOP=1",
        ],
        input=sql,
        text=True,
        check=True,
    )


def api_key_lookup_digest(api_key: str) -> str:
    # This reproduces the API's deterministic lookup digest; it is not a
    # password verifier and intentionally must match the production SHA-256.
    completed = subprocess.run(
        ["openssl", "dgst", "-sha256", "-r"],
        input=api_key.encode(),
        capture_output=True,
        check=True,
    )
    return completed.stdout.decode().split(maxsplit=1)[0]


def seed_api_key(api_key: str) -> None:
    key_hash = api_key_lookup_digest(api_key)
    assert len(key_hash) == 64 and key_hash.isalnum(), key_hash
    psql(
        "INSERT INTO api_keys (org_id, name, key_hash, created_by) "
        "VALUES ('00000000-0000-0000-0000-000000000001', "
        f"'sandbox-fork-smoke', '{key_hash}', "
        "'00000000-0000-0000-0000-000000000010') "
        "ON CONFLICT (key_hash) DO NOTHING;"
    )


def request(method: str, path: str, body: dict | None = None, key: str | None = None):
    headers = {"Host": API_HOST, "x-api-key": API_KEY or ""}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["content-type"] = "application/json"
    if key is not None:
        headers["idempotency-key"] = key
    req = urllib.request.Request(API_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=660) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        raw = error.read().decode(errors="replace")
        raise RuntimeError(f"{method} {path} returned {error.code}: {raw}") from error


def data_request(
    sandbox_id: str,
    token: str,
    method: str,
    path: str,
    body: dict | None = None,
) -> str:
    headers = {
        "Host": f"49983-{sandbox_id}.{DATA_DOMAIN}",
        "content-type": "application/json",
        "e2b-sandbox-id": sandbox_id,
        "e2b-sandbox-port": "49983",
        "x-access-token": token,
    }
    data = None if body is None else json.dumps(body).encode()
    deadline = time.monotonic() + 120
    while True:
        req = urllib.request.Request(
            DATA_PLANE_URL + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as response:
                return response.read().decode()
        except urllib.error.HTTPError as error:
            raw = error.read().decode(errors="replace")
            if error.code == 502 and time.monotonic() < deadline:
                time.sleep(1)
                continue
            raise RuntimeError(
                f"{method} data-plane {path} returned {error.code}: {raw}"
            ) from error
        except (urllib.error.URLError, ConnectionError, http.client.RemoteDisconnected):
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def write_marker(sandbox: dict, path: str, marker: str) -> None:
    data_request(
        sandbox["sandboxID"],
        sandbox["envdAccessToken"],
        "POST",
        "/files",
        {"path": path, "content": marker},
    )


def assert_marker(sandbox: dict, path: str, marker: str) -> None:
    query = urllib.parse.urlencode({"path": path})
    restored = data_request(
        sandbox["sandboxID"],
        sandbox["envdAccessToken"],
        "GET",
        f"/files?{query}",
    )
    assert restored == marker, (sandbox["sandboxID"], restored, marker)


def main() -> int:
    global API_KEY
    managed_api_key = None
    created: list[str] = []
    try:
        if not API_KEY:
            API_KEY = uuid.uuid4().hex
            managed_api_key = API_KEY
            seed_api_key(API_KEY)
        status, source = request(
            "POST",
            "/sandboxes",
            {"templateID": TEMPLATE, "timeout": 900, "metadata": {"smoke": "fork"}},
        )
        assert status == 201, status
        source_id = source["sandboxID"]
        source_fork_id = source_id
        created.append(source_id)
        marker_path = f"/home/user/fork-smoke-{uuid.uuid4()}.txt"
        marker = f"durable fork marker {uuid.uuid4()}"
        write_marker(source, marker_path, marker)

        idempotency_keys = [f"kind-fork-{uuid.uuid4()}" for _ in range(2)]

        def fork(key: str):
            return request(
                "POST",
                f"/sandboxes/{source_fork_id}/fork",
                {"count": 1, "timeout": 600},
                key,
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(fork, idempotency_keys))

        assert all(status == 201 for status, _ in outcomes), outcomes
        results = [result for _, response in outcomes for result in response]
        assert len(results) == 2, results
        assert all("error" not in result for result in results), results
        assert all("sandboxID" in result for result in results), results
        child_ids = [result["sandboxID"] for result in results]
        assert len(set(child_ids)) == 2, child_ids
        created.extend(child_ids)
        for result in results:
            assert_marker(result, marker_path, marker)

        status, replay = request(
            "POST",
            f"/sandboxes/{source_fork_id}/fork",
            {"count": 1, "timeout": 600},
            idempotency_keys[0],
        )
        assert status == 201, status
        assert replay == outcomes[0][1], (outcomes[0][1], replay)

        concurrent_replay_key = f"kind-fork-concurrent-replay-{uuid.uuid4()}"
        with ThreadPoolExecutor(max_workers=2) as executor:
            concurrent_replays = list(
                executor.map(fork, [concurrent_replay_key, concurrent_replay_key])
            )
        assert all(status == 201 for status, _ in concurrent_replays), concurrent_replays
        assert concurrent_replays[0][1] == concurrent_replays[1][1], concurrent_replays
        replay_child = concurrent_replays[0][1][0]
        replay_child_id = replay_child["sandboxID"]
        created.append(replay_child_id)
        child_ids.append(replay_child_id)
        assert_marker(replay_child, marker_path, marker)

        status, _ = request("POST", f"/sandboxes/{source_id}/pause", {})
        assert 200 <= status < 300, status
        paused_key = f"kind-fork-paused-{uuid.uuid4()}"
        paused_status, paused_response = fork(paused_key)
        assert paused_status == 201, paused_response
        assert len(paused_response) == 1, paused_response
        paused_child = paused_response[0]
        paused_child_id = paused_child["sandboxID"]
        created.append(paused_child_id)
        child_ids.append(paused_child_id)
        assert_marker(paused_child, marker_path, marker)

        for child_id in child_ids:
            status, child = request("GET", f"/sandboxes/{child_id}")
            assert status == 200, status
            assert child["sandboxID"] == child_id, child

        print(json.dumps({"source": source_id, "children": child_ids}, indent=2))
        return 0
    finally:
        for sandbox_id in reversed(created):
            try:
                request("DELETE", f"/sandboxes/{sandbox_id}")
            except Exception as error:  # cleanup should not hide the smoke result
                print(f"cleanup warning for {sandbox_id}: {error}", file=sys.stderr)
                time.sleep(0.2)
        if managed_api_key is not None:
            try:
                key_hash = api_key_lookup_digest(managed_api_key)
                assert len(key_hash) == 64 and key_hash.isalnum(), key_hash
                psql(
                    "DELETE FROM api_keys WHERE key_hash = "
                    f"'{key_hash}';"
                )
            except Exception as error:
                print(f"API-key cleanup warning: {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
