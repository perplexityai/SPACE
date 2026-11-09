"""The raw `Process.Start` reader the exec-stream fault scenarios assert through."""

from __future__ import annotations

import base64
import io
import json
import unittest

from space_smoke.adapters import _connect_envelope, _ConnectProcessStartStream


def _message(event: dict[str, object], *, flags: int = 0) -> bytes:
    return _connect_envelope(flags, json.dumps(event).encode())


class _Response(io.BytesIO):
    status = 200


class ConnectProcessStartStreamTests(unittest.TestCase):
    def _events(self, body: bytes) -> list[tuple[str, object]]:
        stream = _ConnectProcessStartStream(connection=None, response=_Response(body))
        return [
            (event.kind, event.pid or event.data or event.exit_code or event.error)
            for event in stream.events()
        ]

    def test_reads_start_output_and_exit_in_order(self) -> None:
        body = (
            _message({"event": {"start": {"pid": 7}}})
            + _message({"event": {"data": {"stdout": base64.b64encode(b"a\n").decode()}}})
            + _message({"event": {"keepalive": {}}})
            + _message({"event": {"end": {"exitCode": 3, "exited": True, "status": "exited"}}})
            + _message({}, flags=0x02)
        )
        self.assertEqual(
            self._events(body),
            [("start", 7), ("stdout", b"a\n"), ("keepalive", None), ("end", 3)],
        )

    def test_end_stream_error_and_truncation(self) -> None:
        error = _message({"error": {"code": "unavailable", "message": "gone"}}, flags=0x02)
        self.assertEqual(self._events(error), [("error", "unavailable: gone")])
        self.assertEqual(self._events(_message({"event": {"start": {"pid": 1}}})[:3]), [])


if __name__ == "__main__":
    unittest.main()
