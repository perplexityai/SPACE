"""The Native files scenarios fail on the answers their checks are there to catch."""

from __future__ import annotations

import base64
import http.client
import json
import socket
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from space_smoke.model import Verdict
from space_smoke.native_hardening import NativeResponse
from space_smoke.runtime import execute_scenario
from space_smoke.scenarios import native_contract_file_upload_admission_429 as admission
from space_smoke.scenarios import native_contract_file_write_size_boundary_on_the_data_plane as boundary
from space_smoke.scenarios.native_contract_file_operations_match_on_the_data_plane import _exercise


def _answer(status: int, body: dict[str, object], text: str = "") -> NativeResponse:
    return NativeResponse(status=status, body=body, headers={}, elapsed_seconds=0.0, text=text)


class _ModifiedCalls:
    """Every files call answers 200; files:modified answers `files`."""

    origin = "http://127.0.0.1:8090"

    def __init__(self, context, files: list[object]) -> None:
        self.test = SimpleNamespace(context=context, new_key=lambda label: label)
        self._files = files

    def call(self, action, body, *, key=None, method="POST"):
        if action == "modified":
            return _answer(200, {
                "files": self._files,
                "observed_since": None,
                "retained_since": None,
                "cursor": None,
                "monitoring_enabled": True,
                "overflowed": False,
            })
        return _answer(200, {}, text="hello")


def _exercise_modified(files: list[object]):
    return execute_scenario(
        title="files:modified",
        body=lambda context: _exercise(_ModifiedCalls(context, files), "/tmp/run"),
    )


@pytest.mark.parametrize(
    "files",
    [
        [],
        [{"path": "d.txt", "root": "/tmp/run", "last_modified_at": "2026-11-04T00:00:00Z", "kinds": ["created"]}],
        # An older guest omits `root`; a kind this build does not know is still a string.
        [{"path": "d.txt", "last_modified_at": None, "kinds": ["modified", "attrib"]}],
    ],
)
def test_modified_entries_of_the_contracts_shape_are_compared_as_a_list(files):
    result = _exercise_modified(files)
    assert result.verdict == Verdict.PASS, result


@pytest.mark.parametrize(
    "entry",
    [
        42,
        {"last_modified_at": None, "kinds": []},
        {"path": 7, "last_modified_at": None, "kinds": []},
        {"path": "d.txt", "kinds": []},
        {"path": "d.txt", "last_modified_at": 5, "kinds": []},
        {"path": "d.txt", "last_modified_at": None},
        {"path": "d.txt", "last_modified_at": None, "kinds": "created"},
        {"path": "d.txt", "last_modified_at": None, "kinds": [1]},
        {"path": "d.txt", "root": None, "last_modified_at": None, "kinds": []},
    ],
)
def test_a_modified_entry_out_of_shape_fails(entry):
    result = _exercise_modified([entry])
    assert result.verdict == Verdict.FAIL, result
    assert "out of its contract's shape" in result.summary


class _BoundaryFixture:
    """A data plane that takes writes up to `boundary.CAP`, keeping what
    `corrupt` makes of each."""

    def __init__(self, context, corrupt) -> None:
        self.context = context
        self.config = SimpleNamespace(data_plane_url="http://127.0.0.1:8090")
        self._corrupt = corrupt
        self._files: dict[str, bytes] = {}

    def create_ready(self) -> str:
        return "sbx_boundary"

    def new_key(self, label: str) -> str:
        return label

    def request(self, method, path, *, body=None, idempotency_key=None, api_url=None, timeout=None):
        action = path.rsplit(":", 1)[1]
        if action == "write":
            content = base64.b64decode(body["data_base64"])
            if len(content) > boundary.CAP:
                return _answer(400, {"error": {"code": "invalid_request", "message": "file write payload is too large"}})
            self._files[body["path"]] = self._corrupt(content)
            return _answer(200, {"bytes_written": len(content)})
        if action == "stat":
            if body["path"] not in self._files:
                return _answer(404, {})
            return _answer(200, {"entry": {"size_bytes": len(self._files[body["path"]])}})
        if action == "read":
            content = self._files[body["path"]]
            return _answer(200, {}, text=content[body["offset"]:body["offset"] + body["length"]].decode())
        raise AssertionError(f"unexpected files:{action}")


def _flip(at: int):
    def corrupt(content: bytes) -> bytes:
        return content[:at] + b"!" + content[at + 1:]

    return corrupt


@pytest.fixture
def small_cap(monkeypatch):
    # The checks scale with the cap; 4 KiB keeps the test fast.
    monkeypatch.setattr(boundary, "CAP", 4096)
    return 4096


def _run_boundary(corrupt):
    return execute_scenario(
        title=boundary.file_write_size_boundary_on_the_data_plane.title,
        body=lambda context: boundary.file_write_size_boundary_on_the_data_plane.verify(
            _BoundaryFixture(context, corrupt)
        ),
    )


def test_a_write_read_back_whole_passes(small_cap):
    result = _run_boundary(lambda content: content)
    assert result.verdict == Verdict.PASS, result


@pytest.mark.parametrize(
    ("where", "at"),
    [("start", lambda cap: 100), ("middle", lambda cap: cap // 2 + 100), ("end", lambda cap: cap - 100)],
)
def test_a_byte_changed_anywhere_it_reads_fails(small_cap, where, at):
    result = _run_boundary(_flip(at(small_cap)))
    assert result.verdict == Verdict.FAIL, result
    assert result.summary == f"the {where} of the file at the cap is not what was written"


class _Connection:
    """An HTTPConnection that is never answered, failing where it is told to."""

    opened: list[_Connection] = []

    def __init__(self, host, port, timeout=None, *, fail_send=False, fail_response=False) -> None:
        self.sock, self._peer = socket.socketpair()
        self.closed = False
        self._fail_send = fail_send
        self._fail_response = fail_response
        _Connection.opened.append(self)

    def putrequest(self, method, path):
        pass

    def putheader(self, name, value):
        pass

    def endheaders(self):
        pass

    def send(self, data):
        if self._fail_send:
            raise OSError("connection reset while sending")

    def getresponse(self):
        if self._fail_response:
            raise OSError("connection reset before the answer")
        raise AssertionError("unexpected getresponse")

    def close(self):
        self.closed = True
        self.sock.close()
        self._peer.close()


@pytest.fixture
def connections(monkeypatch):
    _Connection.opened = []
    monkeypatch.setattr(admission, "REFUSAL_SECONDS", 0.01)
    yield _Connection.opened
    for connection in _Connection.opened:
        connection.close()


def test_a_held_upload_whose_setup_fails_is_closed(monkeypatch, connections):
    monkeypatch.setattr(
        http.client, "HTTPConnection", lambda *args, **kwargs: _Connection(*args, fail_send=True, **kwargs)
    )
    with pytest.raises(OSError):
        admission._held_upload("http://127.0.0.1:8090", "token", "/v1/x", "key", b"{}", 1)
    assert [connection.closed for connection in connections] == [True]


def _admission_fixture(context) -> SimpleNamespace:
    recovery = SimpleNamespace(
        owned_faults=nullcontext,
        data_plane_replicas=lambda: 2,
        one_upload_per_sandbox=nullcontext,
    )
    return SimpleNamespace(
        context=context,
        recovery=recovery,
        config=SimpleNamespace(data_plane_url="http://127.0.0.1:8090", token="token"),
        create_ready=lambda: "sbx_admission",
        new_key=lambda label: label,
    )


@pytest.mark.parametrize(
    "fail",
    [
        # The second of three held writes fails while it is being sent.
        lambda index: {"fail_send": index == 1},
        # Every write is held, and the first to be finished loses its answer.
        lambda index: {"fail_response": True},
    ],
    ids=["while holding", "while finishing"],
)
def test_every_held_upload_is_closed_when_the_scenario_fails(monkeypatch, connections, fail):
    def connection(*args, **kwargs):
        return _Connection(*args, **kwargs, **fail(len(_Connection.opened)))

    monkeypatch.setattr(http.client, "HTTPConnection", connection)
    result = execute_scenario(
        title=admission.file_upload_admission_429.title,
        body=lambda context: admission.file_upload_admission_429.verify(_admission_fixture(context)),
    )
    assert result.verdict != Verdict.PASS, result
    assert connections and all(connection.closed for connection in connections)
    assert "connection reset" in json.dumps(result.summary)
