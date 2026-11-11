"""Native `files:*` calls sent to either public origin.

The data plane answers Native `files:*` as well as the API does, deciding each
request through the API: a deployment's load balancer sends the files paths
to it, and Kind exposes both origins directly. These helpers send the same
call to either origin and read the answers in a form two origins can be
compared in: the request id and the times a guest stamps are left out, and a
directory each origin writes in is named by a placeholder.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping
from urllib.parse import quote

from space_smoke.native import _safe_origin
from space_smoke.native_hardening import NativeHardeningFixture, NativeResponse
from space_smoke.runtime import SmokeConfigurationError

# Answer fields whose values differ between two calls that did the same thing.
# Whether each is set is still compared.
_VOLATILE = frozenset({
    "request_id", "modified_at", "observed_since", "retained_since", "last_modified_at", "cursor",
})
DIRECTORY = "<dir>"


def require_distinct_origins(test: NativeHardeningFixture) -> None:
    """Refuse, as a configuration error, an API and data-plane origin that are
    the same. Both calls would then reach whichever server that origin
    serves, and comparing its answers with themselves would pass without
    comparing two servers. Nothing in an answer says which server gave it,
    since the data plane answers as the API does, so the origins are what a
    scenario can check."""
    if _safe_origin(test.config.api_url) == _safe_origin(test.config.data_plane_url):
        raise SmokeConfigurationError(
            "SPACE_DATA_PLANE_URL names the API's origin; comparing the two "
            "servers needs the data plane's own origin (Kind: http://127.0.0.1:8090)"
        )


def data(content: bytes) -> str:
    return base64.b64encode(content).decode()


def error_code(response: NativeResponse) -> object:
    error = response.body.get("error")
    return error.get("code") if isinstance(error, dict) else response.body.get("code")


class FileCalls:
    """Files calls on one sandbox through one origin."""

    def __init__(self, test: NativeHardeningFixture, sandbox_id: str, origin: str) -> None:
        self.test = test
        self.origin = origin
        self._files = f"/v1/sandboxes/{quote(sandbox_id, safe='')}/files"

    def call(
        self,
        action: str,
        body: Mapping[str, object] | None,
        *,
        key: str | None = None,
        method: str = "POST",
        timeout: float | None = None,
    ) -> NativeResponse:
        return self.test.request(
            method,
            f"{self._files}:{action}",
            body=body,
            idempotency_key=key,
            api_url=self.origin,
            timeout=timeout,
        )


def comparable(value: object, directory: str) -> object:
    """`value` with volatile fields masked and `directory` as a placeholder."""
    if isinstance(value, dict):
        return {
            key: ("<set>" if item is not None else None)
            if key in _VOLATILE
            else comparable(item, directory)
            for key, item in sorted(value.items())
        }
    if isinstance(value, list):
        items = [comparable(item, directory) for item in value]
        return sorted(items, key=repr) if all(isinstance(item, dict) for item in items) else items
    if isinstance(value, str):
        return value.replace(directory, DIRECTORY)
    return value
