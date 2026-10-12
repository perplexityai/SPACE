"""Typed fixtures passed to smoke scenario functions."""

from __future__ import annotations

import hashlib
import shlex
import time
from datetime import UTC, datetime, timedelta
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import asdict, dataclass
from enum import StrEnum
from http.client import IncompleteRead
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, ClassVar, cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from space_smoke.capabilities import (
    ConcurrentRestoreControl,
    NodeDrainControl,
    NodeGroupControl,
    NodeFenceControl,
    NodeSelectorExtrasControl,
    PendingBackupControl,
    OrphanedRestorePodGcControl,
    QuarantineRecoveryControl,
    TemplateBuildControl,
    TemplateBuilder,
    RateLimiterControl,
    RateLimitProbe,
    EncryptedResuspendControl,
    EncryptedSnapshotObservation,
    EcwSandboxDeleteRetryable,
    ClusterRetentionControl,
    ColdEvictionConfig,
    ColdEvictionControl,
    CrossNodeRestoreControl,
    ForkableSandbox,
    ReapedArtifact,
    SnapshotReaperConfig,
    SnapshotReaperControl,
    BackupQueued,
    BtrfsSendCapture,
    CompletedTransfer,
    ReleaseStorageRow,
    RestoreFailure,
    SnapshotRowFacts,
    StartupRecoveryControl,
    StartupRecoveryReport,
    SuspendCodecControl,
    SandboxNetworkIsolationControl,
    TelemetryBackendControl,
    LocalEviction,
    RestoreActivation,
    RestoreStateRead,
    SandboxEventRead,
    SandboxPodPlacement,
    ColdCreateObserver,
    CommandResult,
    CommandSandbox,
    DurableLifecycleSandbox,
    EcwCommandResult,
    EcwCreateAttempt,
    EcwDataPlaneNotFound,
    EcwFilesystemEvent,
    EcwPermissionDenied,
    EcwProcess,
    EcwProtocolCompatibility,
    EcwSandbox,
    EcwSandboxInfo,
    EcwSandboxState,
    EcwSandboxUnavailable,
    EcwScenarioSandboxes,
    EcwWatchHandle,
    NetworkConfig,
    ProcessHandle,
    ProcessInfo,
    ProcessSandbox,
    ResidentLifecycleSandbox,
    StoppableLifecycleSandbox,
    RetentionSweeperConfig,
    SandboxInfo,
    SandboxPages,
    ScenarioSandboxes,
    SnapshotRecord,
    TemplateCacheControl,
    TemplateCacheGcConfig,
    WarmPoolCandidate,
    WarmPoolObserver,
)
from space_smoke.adapters import (
    BTRFS_SEND_V1,
    TEMPLATE_PULL_PLAN_MESSAGE,
    TEMPLATE_PULL_START_MESSAGE,
    template_received_uuid,
)
from space_smoke.concurrent_restore import ConcurrentRestoreAssertions
from space_smoke.node_drain import NodeDrainAssertions
from space_smoke.node_groups import NodeGroupAssertions
from space_smoke.node_fence import NodeFenceAssertions
from space_smoke.node_selector_extras import NodeSelectorExtrasAssertions
from space_smoke.orphaned_restore_pod_gc import OrphanedRestorePodGcAssertions
from space_smoke.pending_backup import PendingBackupAssertions
from space_smoke.quarantine_recovery import QuarantineRecoveryAssertions
from space_smoke.template_build import TemplateBuildAssertions
from space_smoke.rate_limiters import RateLimiterAssertions
from space_smoke.sandbox_network_isolation import SandboxNetworkIsolationAssertions
from space_smoke.telemetry_delivery import TelemetryDeliveryAssertions
from space_smoke.config import DEFAULT_SANDBOX_TIMEOUT_SECONDS
from space_smoke.identity import EcwIdentityAssertions
from space_smoke.invariants.lifecycle import prove_idle_deadline_reset
from space_smoke.network_policy import NetworkPolicyAssertions
from space_smoke.runtime import (
    Compensation,
    InvariantViolation,
    ResourceScope,
    ScenarioContext,
    ScenarioTimedOut,
    SmokeConfigurationError,
)

if TYPE_CHECKING:
    from space_smoke.private_template_build import PrivateTemplateBuildAssertions

# One cleanup retry policy for every sandbox delete the harness performs —
# the run-scoped sandbox, the kill-by-metadata backstop, and fork children.
SANDBOX_CLEANUP_ATTEMPTS = 10
SANDBOX_CLEANUP_RETRY_SECONDS = 1.0


def retry_sandbox_delete(delete: Callable[[], None]) -> int:
    """Run one sandbox delete under the shared cleanup retry policy.

    Retries only `EcwSandboxDeleteRetryable` (busy, transient status,
    transport failure); anything else propagates at once. Returns the number
    of attempts used.
    """

    for attempt in range(1, SANDBOX_CLEANUP_ATTEMPTS + 1):
        try:
            delete()
            return attempt
        except EcwSandboxDeleteRetryable:
            if attempt == SANDBOX_CLEANUP_ATTEMPTS:
                raise
            time.sleep(SANDBOX_CLEANUP_RETRY_SECONDS)
    raise AssertionError("unreachable sandbox deletion retry state")


_GUEST_NETWORK_SNAPSHOT_COMMAND = """\
set +e
echo "snapshot_utc=$(date -u +%FT%TZ 2>/dev/null)"
echo "user=$(id 2>/dev/null)"
echo "hostname=$(hostname 2>/dev/null)"
echo "uname=$(uname -a 2>/dev/null)"
echo "--- /etc/resolv.conf"; sed -n '1,40p' /etc/resolv.conf 2>/dev/null
echo "--- /etc/nsswitch.conf hosts"; grep '^hosts:' /etc/nsswitch.conf 2>/dev/null
echo "--- ip route"; ip route show 2>/dev/null || route -n 2>/dev/null
echo "--- ip addr"; ip -brief addr 2>/dev/null || true
echo "--- sockets"; ss -H -tunap 2>/dev/null | head -80 || netstat -tunap 2>/dev/null | head -80 || true
echo "--- /proc/net udp"; sed -n '1,24p' /proc/net/udp /proc/net/udp6 2>/dev/null
"""
_GUEST_NETWORK_SNAPSHOT_TIMEOUT_SECONDS = 8
_GUEST_NETWORK_SNAPSHOT_OUTPUT_LIMIT = 4_000
_SANDBOX_PAGE_CAP = 200
_HTTP_SERVER_PROCESS_TIMEOUT_SECONDS = 0
_HTTP_SERVER_START_TIMEOUT_SECONDS = 10
_HTTP_SERVER_POLL_ATTEMPTS = 8
_HTTP_SERVER_POLL_INTERVAL_SECONDS = 2.0
_HTTP_USER_AGENT = "space-smoke/0.1"
_HTTPS_PROBE_CURL_TIMEOUT_SECONDS = 5
_HTTPS_PROBE_COMMAND_TIMEOUT_SECONDS = 10
_CURL_NETWORK_FAILURE_EXIT_CODES = frozenset({5, 6, 7, 28, 35, 52, 55, 56})
_HTTPS_PROBE_OPEN_RETRY_STATUSES = frozenset({408, 502, 503, 504})
_HTTPS_PROBE_OPEN_RETRY_SIGNATURES = (
    "sandbox is not running",
    "request timeout",
    "gateway time-out",
    "gateway timeout",
    "timed out",
    "timeout",
    "connection closed",
    "connection refused",
    "connection reset",
    "server disconnected",
    "transport error",
    "transport is closing",
    "unavailable",
)
_PAUSE_TRANSPORT_RETRY_EXCEPTION_TYPES = frozenset(
    {"ConnectError", "ReadError", "RemoteProtocolError", "WriteError"}
)
_PAUSE_TRANSPORT_RETRY_SIGNATURES = (
    "connection closed",
    "connection reset",
    "eof occurred in violation of protocol",
    "server disconnected",
)
_PAUSE_COMMITTED_ERROR_SIGNATURE = "502: sandbox is not running"
_GATEWAY_TIMEOUT_SIGNATURES = (
    "408",
    "504",
    "request timeout",
    "gateway time-out",
    "gateway timeout",
    "timed out",
    "timeout",
)


class HttpListeningOn(StrEnum):
    """Guest interface on which a smoke HTTP server listens."""

    ALL_INTERFACES = "0.0.0.0"
    LOOPBACK_ONLY = "127.0.0.1"


ALL_INTERFACES = HttpListeningOn.ALL_INTERFACES
LOOPBACK_ONLY = HttpListeningOn.LOOPBACK_ONLY


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: str
    attempts: int
    error: str | None = None


@dataclass(frozen=True)
class HttpStreamResponse:
    status: int
    lines: tuple[str, ...]
    elapsed_seconds: float
    error: str | None = None


@dataclass(frozen=True)
class HttpsProbeSample:
    """One in-guest HTTPS observation, including curl's real exit code."""

    url: str
    command_exit_code: int
    curl_exit_code: int | None
    http_status: int | None
    stdout: str
    stderr: str
    process_open_attempts: int

    @property
    def reachable(self) -> bool:
        return (
            self.command_exit_code == 0
            and self.curl_exit_code == 0
            and self.http_status == 200
        )

    @property
    def network_reachable(self) -> bool:
        """Whether curl reached any HTTP server, regardless of response status."""

        return self.command_exit_code == 0 and (
            self.curl_exit_code == 0
            or (self.http_status is not None and self.http_status != 0)
        )

    @property
    def blocked_by_network(self) -> bool:
        return (
            self.command_exit_code == 0
            and self.curl_exit_code in _CURL_NETWORK_FAILURE_EXIT_CODES
        )

    def evidence(self) -> dict[str, object]:
        return {
            "command_exit_code": self.command_exit_code,
            "curl_exit_code": self.curl_exit_code,
            "http_status": self.http_status,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "process_open_attempts": self.process_open_attempts,
        }


@dataclass(frozen=True)
class HttpsAccessObservation:
    """The ordered HTTPS samples used to decide one egress invariant."""

    url: str
    samples: tuple[HttpsProbeSample, ...]

    def evidence(self) -> dict[str, object]:
        return {
            "url": self.url,
            "samples": [sample.evidence() for sample in self.samples],
        }


def _is_retryable_https_probe_open_failure(error: Exception) -> bool:
    """Classify only failures that happened before curl produced evidence."""

    if isinstance(error, EcwSandboxUnavailable):
        return True
    if type(error).__name__ == "CommandExitException":
        return False
    if _exception_status(error) in _HTTPS_PROBE_OPEN_RETRY_STATUSES:
        return True
    detail = f"{type(error).__name__}: {error}".casefold()
    return any(signature in detail for signature in _HTTPS_PROBE_OPEN_RETRY_SIGNATURES)


def _is_retryable_pause_transport_failure(
    error: Exception, *, allow_committed_not_running: bool
) -> bool:
    detail = str(error).casefold()
    if type(error).__name__ in _PAUSE_TRANSPORT_RETRY_EXCEPTION_TYPES:
        return any(signature in detail for signature in _PAUSE_TRANSPORT_RETRY_SIGNATURES)
    # The ECW pause can commit the durable suspend and then time out waiting
    # for a terminated Kind pod to disappear. Resolve this response from the
    # public sandbox state exactly like a lost transport reply; never replay it.
    return (
        allow_committed_not_running
        and type(error).__name__ == "SandboxException"
        and _PAUSE_COMMITTED_ERROR_SIGNATURE in detail
    )


def _exception_chain(error: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _is_gateway_timeout(error: BaseException) -> bool:
    return any(
        signature in f"{type(item).__name__}: {item}".casefold()
        for item in _exception_chain(error)
        for signature in _GATEWAY_TIMEOUT_SIGNATURES
    )


def _exception_status(error: Exception) -> int | None:
    for attribute in ("status_code", "status", "code"):
        value = getattr(error, attribute, None)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    response = getattr(error, "response", None)
    value = getattr(response, "status_code", None)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


@dataclass(frozen=True)
class EcwHttpService:
    """A guest HTTP service reached through ECW's public port route."""

    _context: ScenarioContext
    _sandbox: EcwSandbox
    port: int
    _stop_process: Callable[[], None] | None = None

    def stop(self) -> None:
        """Stop this service's owned process and wait boundedly for port closure."""
        if self._stop_process is None:
            raise ValueError("HTTP service has no owned process")
        with self._context.phase("stop owned guest HTTP listener"):
            self._stop_process()
            result = self._sandbox.run(
                _wait_for_listener_command(self.port, present=False),
                timeout=_HTTP_SERVER_START_TIMEOUT_SECONDS,
            )
            self._context.verify(
                result.exit_code == 0,
                "owned guest HTTP listener did not close",
                port=self.port,
                exit_code=result.exit_code,
            )

    def request_from_public_port(
        self,
        path: str = "/",
        *,
        expected_status: int = 200,
        attempts: int = _HTTP_SERVER_POLL_ATTEMPTS,
        interval_seconds: float = _HTTP_SERVER_POLL_INTERVAL_SECONDS,
    ) -> HttpResponse:
        """Request the service through ECW, polling until its route is ready."""

        with self._context.phase("request service through ECW public port"):
            return self._request_when_ready(
                path,
                expected_status=expected_status,
                attempts=attempts,
                interval_seconds=interval_seconds,
            )

    def _request_when_ready(
        self,
        path: str,
        *,
        expected_status: int,
        attempts: int,
        interval_seconds: float,
    ) -> HttpResponse:
        if attempts <= 0:
            raise ValueError("HTTP poll attempts must be positive")
        last = HttpResponse(status=0, body="", attempts=0, error="not attempted")
        for attempt in range(1, attempts + 1):
            last = self._get(path, attempt=attempt)
            self._context.record("http_poll_attempt", attempt)
            self._context.record("http_status", last.status)
            if last.status == expected_status:
                return last
            if attempt < attempts:
                self._context.wait(interval_seconds)
        return last

    def read_from_public_port(
        self,
        path: str,
        *,
        timeout: float,
    ) -> HttpStreamResponse:
        """Read a complete public-port streaming response line by line."""

        with self._context.phase("read stream through ECW public port"):
            route = self._sandbox.public_port(self.port)
            request = Request(
                _cache_busted_url(route.base_url, path),
                headers=_http_headers(route.headers),
                method="GET",
            )
            started_at = time.monotonic()
            status = 0
            lines: list[str] = []
            try:
                with urlopen(request, timeout=timeout) as response:
                    status = response.status
                    lines.extend(
                        line.decode(errors="replace").rstrip("\r\n")
                        for line in response
                    )
                    return HttpStreamResponse(
                        status=status,
                        lines=tuple(lines),
                        elapsed_seconds=time.monotonic() - started_at,
                    )
            except IncompleteRead as error:
                return HttpStreamResponse(
                    status=status,
                    lines=tuple(lines),
                    elapsed_seconds=time.monotonic() - started_at,
                    error=f"IncompleteRead: {error}",
                )
            except HTTPError as error:
                return HttpStreamResponse(
                    status=error.code,
                    lines=(),
                    elapsed_seconds=time.monotonic() - started_at,
                    error=error.read(4_000).decode(errors="replace"),
                )
            except (OSError, TimeoutError, URLError) as error:
                return HttpStreamResponse(
                    status=0,
                    lines=(),
                    elapsed_seconds=time.monotonic() - started_at,
                    error=f"{type(error).__name__}: {error}",
                )

    def _get(self, path: str, *, attempt: int) -> HttpResponse:
        route = self._sandbox.public_port(self.port)
        request = Request(
            _cache_busted_url(route.base_url, path),
            headers=_http_headers(route.headers),
            method="GET",
        )
        try:
            with urlopen(request, timeout=4) as response:
                return HttpResponse(
                    status=response.status,
                    body=response.read().decode(errors="replace"),
                    attempts=attempt,
                )
        except HTTPError as error:
            return HttpResponse(
                status=error.code,
                body=error.read(4_000).decode(errors="replace"),
                attempts=attempt,
                error=str(error),
            )
        except (OSError, TimeoutError, URLError) as error:
            return HttpResponse(
                status=0,
                body="",
                attempts=attempt,
                error=f"{type(error).__name__}: {error}",
            )


@dataclass(frozen=True)
class EcwDelayedHttpStream(EcwHttpService):
    """A delayed guest stream whose ECW public route can be established first."""

    def establish_public_route(self) -> None:
        """Require the stream's health route before testing its lifetime."""

        with self._context.phase("establish ECW public port route"):
            response = self._request_when_ready(
                "/health",
                expected_status=200,
                attempts=_HTTP_SERVER_POLL_ATTEMPTS,
                interval_seconds=_HTTP_SERVER_POLL_INTERVAL_SECONDS,
            )
            self._context.verify(
                (response.status, response.body) == (200, "ok\n"),
                "ECW public port route never became ready",
                expected=(200, "ok\n"),
                actual=(response.status, response.body),
                request_attempts=response.attempts,
                request_error=response.error,
            )


def _cache_busted_url(base_url: str, path: str) -> str:
    normalized_path = f"/{path.lstrip('/')}"
    separator = "&" if "?" in normalized_path else "?"
    return f"{base_url.rstrip('/')}{normalized_path}{separator}cb={time.time_ns()}"


def _http_headers(route_headers: Mapping[str, str]) -> dict[str, str]:
    return {
        **route_headers,
        "User-Agent": _HTTP_USER_AGENT,
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
    }


def _wait_for_listener_command(port: int, *, present: bool = True) -> str:
    if not 1 <= port <= 65535:
        raise ValueError("sandbox port must be between 1 and 65535")
    hex_port = f"{port:04X}"
    condition = "&&" if present else "||"
    return (
        "for i in $(seq 1 20); do "
        f"grep -E ':{hex_port} .* 0A ' /proc/net/tcp /proc/net/tcp6 "
        f">/dev/null 2>&1 {condition} exit 0; "
        "sleep 0.25; "
        "done; exit 1"
    )


def _socket_activated_server_command(
    *,
    listening_on: HttpListeningOn,
    port: int,
    script_path: str,
) -> str:
    listen_address = shlex.quote(f"{listening_on.value}:{port}")
    return (
        "exec systemd-socket-activate "
        f"--listen={listen_address} --accept --inetd "
        f"/bin/sh {shlex.quote(script_path)}"
    )


def _fixed_response_server_script(body_path: str) -> str:
    quoted_body_path = shlex.quote(body_path)
    return f"""\
set -eu
while IFS= read -r header; do
  [ "$header" = "$(printf '\\r')" ] && break
done
content_length=$(wc -c < {quoted_body_path})
printf 'HTTP/1.1 200 OK\\r\\nContent-Length: %s\\r\\nConnection: close\\r\\n\\r\\n' "$content_length"
cat {quoted_body_path}
"""


def _streaming_server_script(
    *,
    lines: Sequence[str],
    every_seconds: float,
) -> str:
    chunks = "\n".join(
        (
            f"printf '{len(line.encode()) + 1:x}\\r\\n'\n"
            f"printf '%s\\n' {shlex.quote(line)}\n"
            "printf '\\r\\n'\n"
            f"sleep {every_seconds!r}"
        )
        for line in lines
    )
    return f"""\
set -eu
IFS= read -r request_line
while IFS= read -r header; do
  [ "$header" = "$(printf '\\r')" ] && break
done
case "$request_line" in
  "GET /health "*|"GET /health?"*)
    printf 'HTTP/1.1 200 OK\\r\\nContent-Type: text/plain\\r\\nContent-Length: 3\\r\\nCache-Control: no-store\\r\\nConnection: close\\r\\n\\r\\nok\\n'
    ;;
  *)
    printf 'HTTP/1.1 200 OK\\r\\nContent-Type: text/plain\\r\\nTransfer-Encoding: chunked\\r\\nCache-Control: no-store\\r\\nConnection: close\\r\\n\\r\\n'
{chunks}
    printf '0\\r\\n\\r\\n'
    ;;
esac
"""


@dataclass(frozen=True)
class EcwDirectoryWatcher:
    """A scenario-owned SDK watcher with bounded event polling."""

    _context: ScenarioContext
    _handle: EcwWatchHandle
    _root: PurePosixPath

    def expect_change(
        self,
        path: str,
        *,
        timeout_seconds: float = 5,
        every_seconds: float = 0.2,
    ) -> None:
        """Require an event for one exact path relative to the watched root."""

        if timeout_seconds <= 0:
            raise ValueError("watcher timeout must be positive")
        if every_seconds <= 0:
            raise ValueError("watcher interval must be positive")
        try:
            expected_name = PurePosixPath(path).relative_to(self._root).as_posix()
        except ValueError as error:
            raise ValueError(
                f"expected watcher path {path!r} must be inside {self._root}"
            ) from error
        if expected_name == ".":
            raise ValueError("expected watcher path must name an entry")

        deadline = time.monotonic() + timeout_seconds
        observed: list[EcwFilesystemEvent] = []
        while True:
            try:
                observed.extend(self._handle.get_new_events())
            except (ScenarioTimedOut, SmokeConfigurationError):
                raise
            except Exception as error:
                self._context.verify(
                    False,
                    "filesystem watcher events could not be read",
                    expected_watcher_path=expected_name,
                    watcher_events=_watcher_event_evidence(observed),
                    watcher_poll_error_type=type(error).__name__,
                    watcher_poll_error=str(error),
                )
                raise AssertionError("unreachable watcher poll failure") from error

            if any(
                event.name.removeprefix("./") == expected_name for event in observed
            ):
                self._context.record(
                    "watcher_events", _watcher_event_evidence(observed)
                )
                return
            if time.monotonic() >= deadline:
                self._context.verify(
                    False,
                    "filesystem watcher did not observe the file",
                    expected_watcher_path=expected_name,
                    watcher_events=_watcher_event_evidence(observed),
                )
            self._context.wait(
                min(every_seconds, max(0.001, deadline - time.monotonic()))
            )


def _watcher_event_evidence(
    events: Sequence[EcwFilesystemEvent],
) -> list[dict[str, str]]:
    return [{"name": event.name, "type": event.type.value} for event in events]


@dataclass(frozen=True)
class SandboxCollection:
    """All sandboxes owned by one scenario fixture."""

    _context: ScenarioContext
    _sandboxes: ScenarioSandboxes

    @property
    def items(self) -> tuple[CommandSandbox, ...]:
        return self._sandboxes.items

    @property
    def ids(self) -> tuple[str, ...]:
        return tuple(sandbox.sandbox_id for sandbox in self.items)

    def list_pages(self, *, page_size: int) -> SandboxPages[SandboxInfo]:
        """Traverse sandbox pages through the selected API adapter."""

        self._context.record("requested_page_size", page_size)
        with self._context.phase("list sandbox pages"):
            return self._sandboxes.list_pages(
                limit=page_size,
                max_pages=_SANDBOX_PAGE_CAP,
            )

    def verify_exact_pagination(
        self,
        result: SandboxPages[SandboxInfo],
        *,
        require_multiple_pages: bool = False,
    ) -> None:
        """Require complete, duplicate-free traversal of the owned collection."""

        expected_ids = set(self.ids)
        observed_ids = [item.sandbox_id for page in result.pages for item in page]
        observed_set = set(observed_ids)
        observed_owned_ids = [
            sandbox_id for sandbox_id in observed_ids if sandbox_id in expected_ids
        ]
        page_sizes = [len(page) for page in result.pages]

        self._context.verify(
            not result.has_more or expected_ids <= observed_set,
            "sandbox pagination exceeded its page cap",
            page_cap=_SANDBOX_PAGE_CAP,
            observed_page_count=len(result.pages),
            observed_page_sizes=page_sizes,
        )
        if require_multiple_pages:
            self._context.verify(
                len(result.pages) > 1,
                "sandbox list did not exercise a second page",
                expected_sandbox_count=len(expected_ids),
                observed_page_count=len(result.pages),
                observed_page_sizes=page_sizes,
            )
        self._context.verify(
            len(observed_owned_ids) == len(set(observed_owned_ids)),
            "sandbox pagination returned a duplicate run-owned sandbox",
            observed_sandbox_ids=observed_ids,
        )
        self._context.verify(
            expected_ids <= observed_set,
            "sandbox pagination did not return every run-owned sandbox",
            expected_sandbox_ids=sorted(expected_ids),
            observed_sandbox_ids=observed_ids,
            missing_sandbox_ids=sorted(expected_ids - observed_set),
        )


class EcwSandboxCollection(SandboxCollection):
    """ECW collection operations beyond the shared pagination seam."""

    _sandboxes: EcwScenarioSandboxes

    def verify_exact_pagination(
        self,
        result: SandboxPages[SandboxInfo],
        *,
        require_multiple_pages: bool = False,
    ) -> None:
        """Require ECW's metadata-filtered listing to match the owned set."""

        self._context.verify(
            not result.has_more,
            "sandbox pagination exceeded its page cap",
            page_cap=_SANDBOX_PAGE_CAP,
            observed_page_count=len(result.pages),
            observed_page_sizes=[len(page) for page in result.pages],
        )
        super().verify_exact_pagination(
            result,
            require_multiple_pages=require_multiple_pages,
        )
        expected_ids = set(self.ids)
        observed_ids = [item.sandbox_id for page in result.pages for item in page]
        observed_set = set(observed_ids)

        self._context.verify(
            len(observed_ids) == len(observed_set),
            "ECW sandbox pagination returned a duplicate sandbox",
            observed_sandbox_ids=observed_ids,
        )
        self._context.verify(
            observed_set == expected_ids,
            "ECW filtered sandbox pagination did not return the exact run-owned set",
            expected_sandbox_ids=sorted(expected_ids),
            observed_sandbox_ids=observed_ids,
            unexpected_sandbox_ids=sorted(observed_set - expected_ids),
        )

    def create(
        self,
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> EcwSandbox:
        """Create another run-owned sandbox with critical cleanup pre-armed."""

        return self._sandboxes.create(metadata)

    def attempt_create(
        self,
        *,
        allow_internet_access: bool | None = None,
        network: NetworkConfig | None = None,
    ) -> EcwCreateAttempt:
        """Observe a raw create using this scenario's template and timeout."""

        with self._context.phase("attempt sandbox creation"):
            return self._sandboxes.attempt_create(
                allow_internet_access=allow_internet_access,
                network=network,
            )


@dataclass(frozen=True)
class CommandSandboxFixture:
    """Shared scenario behavior over a command-capable sandbox."""

    _context: ScenarioContext
    sandbox: CommandSandbox
    _sandboxes: ScenarioSandboxes | None = None
    _durable_lifecycle: DurableLifecycleSandbox | None = None
    _resident_lifecycle: ResidentLifecycleSandbox | None = None
    _stoppable_lifecycle: StoppableLifecycleSandbox | None = None
    _local_cleanup_control: ColdEvictionControl | None = None
    _processes: ProcessSandbox | None = None

    fixture_name: ClassVar[str] = "command_sandbox"
    timeout_minutes: ClassVar[int] = 30
    lifecycle_transition_timeout: ClassVar[int] = 60
    lifecycle_poll_interval: ClassVar[float] = 1
    process_transition_timeout: ClassVar[float] = 60
    sandbox_delete_failure_message: ClassVar[str] = "sandbox deletion did not complete"

    @property
    def sandboxes(self) -> SandboxCollection:
        """The complete run-owned sandbox collection for this scenario."""

        if self._sandboxes is None:
            raise SmokeConfigurationError(
                "this fixture does not provide sandbox collection operations"
            )
        return SandboxCollection(self._context, self._sandboxes)

    def phase(self, name: str) -> AbstractContextManager[None]:
        return self._context.phase(name)

    def record(self, name: str, value: object) -> None:
        """Attach a named fact to the scenario's evidence without asserting it."""
        self._context.record(name, value)

    def verify(
        self,
        condition: bool,
        message: str,
        *,
        failure_evidence: Callable[[], Mapping[str, object]] | None = None,
        **evidence: object,
    ) -> None:
        if not condition and failure_evidence is not None:
            evidence.update(failure_evidence())
        self._context.verify(condition, message, **evidence)

    def warn(self, message: str, **evidence: object) -> None:
        """End the scenario QUARANTINED: a known, narrowly classified failure.

        The evidence is recorded and the run stays visible in the log without
        turning the suite red. Callers must classify before calling — an
        unclassified failure is a `verify` failure, never a warning.
        """

        self._context.warn(message, **evidence)

    def delete_sandbox(self, sandbox: CommandSandbox | None = None) -> int:
        """Delete a run-owned sandbox through the selected API adapter."""

        target = sandbox or self.sandbox
        try:
            return self._scenario_sandboxes().delete(target)
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                self.sandbox_delete_failure_message,
                deleted_sandbox_id=target.sandbox_id,
                delete_error_type=type(error).__name__,
                delete_error=str(error),
            )
            raise AssertionError("unreachable sandbox deletion failure") from error

    def wait_until_deleted(self, sandbox: CommandSandbox | None = None) -> None:
        """Wait for the selected public API to stop returning the sandbox."""

        target = sandbox or self.sandbox
        deadline = time.monotonic() + self.lifecycle_transition_timeout
        while True:
            observed = target.try_get_info()
            if observed is None or time.monotonic() >= deadline:
                break
            self._context.wait(self.lifecycle_poll_interval)
        self.verify(
            observed is None,
            "sandbox remained visible through the public API after deletion",
            deleted_sandbox_id=target.sandbox_id,
            observed_state=self._deleted_observed_state(observed),
        )

    def _scenario_sandboxes(self) -> ScenarioSandboxes:
        if self._sandboxes is None:
            raise SmokeConfigurationError(
                "this fixture does not provide sandbox collection operations"
            )
        return self._sandboxes

    def _deleted_observed_state(self, observed: SandboxInfo | None) -> object:
        return "deleted" if observed is None else "present"

    def _sandbox_run(
        self,
        command: str,
        *,
        timeout: int,
        user: str | None,
        envs: Mapping[str, str] | None,
    ) -> CommandResult:
        if envs is not None:
            raise SmokeConfigurationError(
                "the selected command surface does not support command environment"
            )
        if user is not None:
            raise SmokeConfigurationError(
                "the selected command surface does not support guest users"
            )
        return self.sandbox.run(command, timeout=timeout)

    def run_command(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
        envs: Mapping[str, str] | None = None,
        failure_message: str = "sandbox command did not complete",
        failure_evidence: Callable[[], Mapping[str, object]] | None = None,
    ) -> CommandResult:
        """Run a command and classify client failures as failed invariants."""

        started_at = time.monotonic()
        try:
            return self._sandbox_run(
                command,
                timeout=timeout,
                user=user,
                envs=envs,
            )
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                failure_message,
                failure_evidence=failure_evidence,
                command_elapsed_seconds=time.monotonic() - started_at,
                command_error_type=type(error).__name__,
                command_error=str(error),
            )
            raise AssertionError("unreachable command failure") from error

    def run_command_when_ready(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
        envs: Mapping[str, str] | None = None,
        failure_message: str = "sandbox command data plane did not become ready",
    ) -> CommandResult:
        """Run after the selected adapter's bounded command-readiness gate."""

        return self.run_command(
            command,
            timeout=timeout,
            user=user,
            envs=envs,
            failure_message=failure_message,
        )

    def wait_until_command_matches(
        self,
        command: str,
        *,
        stdout: str,
        exit_code: int = 0,
        timeout: int = 10,
        failure_message: str = "sandbox command result did not become ready",
    ) -> CommandResult:
        """Poll a command until its exact result proves the requested readiness."""

        started_at = time.monotonic()
        deadline = started_at + timeout
        attempts = 0
        while True:
            remaining = deadline - time.monotonic()
            attempts += 1
            result = self.run_command(
                command,
                timeout=max(1, int(remaining) + 1),
                failure_message=failure_message,
            )
            if (result.exit_code, result.stdout) == (exit_code, stdout):
                return result
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.verify(
                    False,
                    failure_message,
                    readiness_attempts=attempts,
                    readiness_elapsed_seconds=time.monotonic() - started_at,
                    command_exit_code=result.exit_code,
                    command_stdout=result.stdout,
                    command_stderr=result.stderr,
                )
            self._context.wait(min(self.lifecycle_poll_interval, remaining))

    def suspend_durably(self) -> None:
        """Persist sandbox state and release its resident capacity."""

        lifecycle = self._durable_lifecycle
        if lifecycle is None:
            raise SmokeConfigurationError(
                "this fixture does not provide durable lifecycle operations"
            )
        try:
            lifecycle.suspend()
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                "sandbox durable suspend did not complete",
                sandbox_id=self.sandbox.sandbox_id,
                suspend_error_type=type(error).__name__,
                suspend_error=str(error),
            )

    def wake_from_durable_suspend(self) -> None:
        """Restore a durably suspended sandbox through its public wake path."""

        lifecycle = self._durable_lifecycle
        if lifecycle is None:
            raise SmokeConfigurationError(
                "this fixture does not provide durable lifecycle operations"
            )
        try:
            lifecycle.wake()
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                "sandbox wake did not complete",
                sandbox_id=self.sandbox.sandbox_id,
                wake_error_type=type(error).__name__,
                wake_error=str(error),
            )

    def pause_resident(self) -> None:
        """Pause the sandbox without releasing resident capacity."""

        lifecycle = self._resident_lifecycle
        if lifecycle is None:
            raise SmokeConfigurationError(
                "this fixture does not provide resident lifecycle operations"
            )
        try:
            lifecycle.pause()
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                "sandbox resident pause did not complete",
                sandbox_id=self.sandbox.sandbox_id,
                pause_error_type=type(error).__name__,
                pause_error=str(error),
            )

    def wake_from_resident_pause(self) -> None:
        """Resume a resident-paused sandbox through its public wake path."""

        lifecycle = self._resident_lifecycle
        if lifecycle is None:
            raise SmokeConfigurationError(
                "this fixture does not provide resident lifecycle operations"
            )
        try:
            lifecycle.wake()
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                "sandbox wake from resident pause did not complete",
                sandbox_id=self.sandbox.sandbox_id,
                wake_error_type=type(error).__name__,
                wake_error=str(error),
            )

    def stop_on_node(self) -> None:
        """Stop the sandbox, leaving its rootfs on the node, and see it report stopped."""

        lifecycle = self._stoppable_lifecycle
        if lifecycle is None:
            raise SmokeConfigurationError(
                "this fixture does not provide stop/start lifecycle operations"
            )
        try:
            lifecycle.stop()
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                "sandbox stop did not complete",
                sandbox_id=self.sandbox.sandbox_id,
                stop_error_type=type(error).__name__,
                stop_error=str(error),
            )

    def start_from_node(self) -> str | None:
        """Start a stopped sandbox from the rootfs its node kept.

        Returns `None` once the sandbox is running again, or the failure text
        when the start operation ended in a terminal non-success state or ran
        out its wait. It does not assert, unlike its lifecycle siblings: the
        caller classifies the failure — a standing, documented bench condition
        is reported through `warn`, anything else through `verify`.
        """

        lifecycle = self._stoppable_lifecycle
        if lifecycle is None:
            raise SmokeConfigurationError(
                "this fixture does not provide stop/start lifecycle operations"
            )
        try:
            lifecycle.start()
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:  # noqa: BLE001 - classified by the caller
            self.record("restart_error_type", type(error).__name__)
            return f"{type(error).__name__}: {error}"
        return None

    def local_cleanup_worker(self) -> str | None:
        """Capture the fixture's worker while its execution is still running."""
        control = self._local_cleanup_control
        if control is None:
            return None
        placement = control.sandbox_pod_placement(self.sandbox.sandbox_id.removeprefix("sbx_"))
        self.verify(placement is not None, "cannot locate the fixture sandbox worker")
        assert placement is not None
        return placement.node_name

    def delete_stopped_and_wait_for_local_cleanup(self, worker: str | None) -> None:
        """Exercise deletion after the runtime route is gone, then observe reconciliation."""
        control = self._local_cleanup_control
        if control is None or worker is None:
            self.delete_sandbox()
            self.wait_until_deleted()
            self.record("local_cleanup_observer", "unavailable outside Kind")
            return
        sandbox_id = self.sandbox.sandbox_id.removeprefix("sbx_")
        deadline = time.monotonic() + 90
        while True:
            pods = control.sandbox_pods(sandbox_id)
            if all(pod.phase in ("Succeeded", "Failed") for pod in pods):
                break
            self.verify(
                time.monotonic() < deadline,
                "stopped execution did not release its task",
                pods=[pod.pod_name for pod in pods],
            )
            self._context.wait(2)
        self.verify(
            control.sandbox_cache_dir_present(worker, sandbox_id),
            "STOP lost the retained rootfs before DELETE",
        )
        since_time = _log_since_time()
        self.delete_sandbox()
        self.wait_until_deleted()
        # Kind configures a 10-second quiescence window; allow normal node
        # reconciliation, its 60-second retry slack and another poll to complete.
        deadline = time.monotonic() + 180
        while True:
            evictions = control.local_evictions(
                sandbox_id, node_names=[worker], since_time=since_time
            )
            if any(event.reason == "cluster_row_absent" for event in evictions):
                break
            self.verify(
                time.monotonic() < deadline,
                "deleted stopped sandbox was not reclaimed by node reconciliation",
                worker=worker,
                sandbox_id=sandbox_id,
            )
            self._context.wait(10)
        self.verify(
            not control.sandbox_cache_dir_present(worker, sandbox_id),
            "node cleanup completed but retained rootfs still exists",
            worker=worker,
        )
        self.record("stopped_delete_local_cleanup", "node eviction completed and rootfs removed")

    def start_background_process(
        self, command: str, *, timeout: int = 60
    ) -> ProcessHandle:
        return self._process_sandbox().start_background(command, timeout=timeout)

    def list_processes(self) -> tuple[ProcessInfo, ...]:
        return self._process_sandbox().list_processes()

    def get_process(self, process_id: str) -> ProcessInfo | None:
        return self._process_sandbox().get_process(process_id)

    def signal_process(self, process_id: str, signal: int) -> bool:
        return self._process_sandbox().signal_process(process_id, signal)

    def kill_process(self, process_id: str) -> bool:
        return self._process_sandbox().kill_process(process_id)

    def wait_until_process_stopped(
        self, process_id: str
    ) -> tuple[tuple[ProcessInfo, ...], ProcessInfo | None]:
        deadline = time.monotonic() + self.process_transition_timeout
        while True:
            processes = self.list_processes()
            process = self.get_process(process_id)
            if (
                not any(item.process_id == process_id for item in processes)
                and process is None
            ):
                return processes, process
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return processes, process
            self._context.wait(min(self.lifecycle_poll_interval, remaining))

    def _process_sandbox(self) -> ProcessSandbox:
        if self._processes is None:
            raise SmokeConfigurationError(
                "this fixture does not provide background process control"
            )
        return self._processes

    def expect_command(
        self,
        result: CommandResult,
        *,
        exit_code: int = 0,
        stdout: str,
    ) -> None:
        """Require one command's exit code and exact standard output."""

        actual = (result.exit_code, result.stdout)
        expected = (exit_code, stdout)
        self.verify(
            actual == expected,
            "sandbox command result did not match",
            expected=expected,
            actual=actual,
            command_exit_code=result.exit_code,
            command_stdout=result.stdout,
            command_stderr=result.stderr,
        )


@dataclass(frozen=True)
class Ecw(CommandSandboxFixture):
    """A safely tracked sandbox accessed through the public ECW API."""

    sandbox: EcwSandbox
    _sandboxes: EcwScenarioSandboxes | None = None
    _sandbox_timeout: int = DEFAULT_SANDBOX_TIMEOUT_SECONDS

    fixture_name: ClassVar[str] = "ecw"
    timeout_minutes: ClassVar[int] = 30
    default_sandbox_timeout: ClassVar[int] = DEFAULT_SANDBOX_TIMEOUT_SECONDS
    lifecycle_transition_timeout: ClassVar[int] = 60
    lifecycle_poll_interval: ClassVar[float] = 1
    process_connect_retry_delays: ClassVar[tuple[float, ...]] = (0.25, 0.75)
    allow_committed_pause_not_running: ClassVar[bool] = False
    sandbox_delete_failure_message: ClassVar[str] = (
        "ECW sandbox deletion did not complete"
    )

    def _process_sandbox(self) -> ProcessSandbox:
        return cast(ProcessSandbox, self.sandbox)

    def _sandbox_run(
        self,
        command: str,
        *,
        timeout: int,
        user: str | None,
        envs: Mapping[str, str] | None,
    ) -> EcwCommandResult:
        options: dict[str, object] = {"timeout": timeout}
        if user is not None:
            options["user"] = user
        if envs is not None:
            options["envs"] = envs
        return self.sandbox.run(command, **options)

    def suspend_durably(self) -> None:
        """ECW pause is the compatibility surface for durable suspend."""

        self.pause()

    def wake_from_durable_suspend(self) -> None:
        """ECW reconnect restores the sandbox by its stored identifier."""

        self.reconnect_by_id()

    @contextmanager
    def quarantine_on_timeout(self, summary: str) -> Iterator[None]:
        """Turn only a recognized gateway/command timeout into a visible warning."""

        try:
            yield
        except InvariantViolation as error:
            if not _is_gateway_timeout(error):
                raise
            chain = tuple(_exception_chain(error))
            cause = chain[-1]
            self._context.warn(
                summary,
                quarantine="gateway_timeout",
                quarantined_error_type=type(cause).__name__,
                quarantined_error=str(cause)[:1_000],
            )

    @property
    def identity(self) -> EcwIdentityAssertions:
        """Assertions for ECW process and filesystem identity contracts."""

        return EcwIdentityAssertions(self)

    @property
    def network_policy(self) -> NetworkPolicyAssertions:
        """Assertions for ECW network-policy contracts."""

        return NetworkPolicyAssertions(self)

    @property
    def compatibility(self) -> EcwProtocolCompatibility:
        """Public protocol probes unavailable through the high-level SDK."""

        return self.sandbox.compatibility

    def wait(self, seconds: float, *, reason: str | None = None) -> None:
        if reason is None:
            self._context.wait(seconds)
            return
        with self.phase(f"wait {reason}"):
            self._context.wait(seconds)

    @contextmanager
    def watch_directory(
        self,
        path: str,
        *,
        recursive: bool = False,
        user: str | None = None,
    ) -> Iterator[EcwDirectoryWatcher]:
        """Create an SDK watcher and require teardown after its invariant."""

        try:
            handle = self.sandbox.files.watch_dir(
                path,
                user=user,
                recursive=recursive,
            )
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:
            self.verify(
                False,
                "filesystem watcher could not be created",
                watcher_path=path,
                watcher_recursive=recursive,
                watcher_create_error_type=type(error).__name__,
                watcher_create_error=str(error),
            )
            raise AssertionError("unreachable watcher creation failure") from error

        cleanup = self._context.scope.defer(
            f"stop filesystem watcher for {path}",
            handle.stop,
        )
        watcher = EcwDirectoryWatcher(
            self._context,
            handle,
            PurePosixPath(path),
        )
        body_completed = False
        try:
            yield watcher
            body_completed = True
        finally:
            # On a body failure, leave cleanup armed for the scenario scope.
            if body_completed:
                try:
                    cleanup.release()
                except Exception as error:  # noqa: BLE001 - stop is the invariant
                    self.verify(
                        False,
                        "filesystem watcher did not stop",
                        watcher_path=path,
                        watcher_stop_error_type=type(error).__name__,
                        watcher_stop_error=str(error),
                    )
                finally:
                    cleanup.disarm()

    def serve_http(
        self,
        *,
        listening_on: HttpListeningOn,
        port: int,
        response: str,
    ) -> EcwHttpService:
        """Start a tracked guest service that returns one exact HTTP response."""

        server_dir = f"/tmp/space-smoke-http-{port}"
        body_path = f"{server_dir}/response"
        script_path = f"{server_dir}/serve.sh"
        with self.phase("prepare guest HTTP server"):
            self.sandbox.files.make_dir(server_dir)
            self.sandbox.files.write(body_path, response)
            self.sandbox.files.write(
                script_path,
                _fixed_response_server_script(body_path),
            )
        command = _socket_activated_server_command(
            listening_on=listening_on,
            port=port,
            script_path=script_path,
        )
        return self._start_http_service(command=command, port=port)

    def serve_delayed_http_stream(
        self,
        *,
        listening_on: HttpListeningOn,
        port: int,
        lines: Sequence[str],
        every_seconds: float,
    ) -> EcwDelayedHttpStream:
        """Start a tracked guest stream that emits exact lines over time."""

        if not lines:
            raise ValueError("stream must contain at least one line")
        if every_seconds <= 0:
            raise ValueError("stream interval must be positive")
        script_path = f"/tmp/space-smoke-stream-{port}.sh"
        script = _streaming_server_script(
            lines=lines,
            every_seconds=every_seconds,
        )
        with self.phase("prepare guest streaming HTTP server"):
            self.sandbox.files.write(script_path, script)
        command = _socket_activated_server_command(
            listening_on=listening_on,
            port=port,
            script_path=script_path,
        )
        service = self._start_http_service(command=command, port=port)
        return EcwDelayedHttpStream(
            service._context,
            service._sandbox,
            service.port,
            service._stop_process,
        )

    def _start_http_service(
        self,
        *,
        command: str,
        port: int,
    ) -> EcwHttpService:
        with self.phase("start guest HTTP service"):
            process = self.sandbox.start_background(
                command,
                # The SDK timeout is the process lifetime. Keep the listener
                # alive until the scenario scope kills its exact SDK handle.
                timeout=_HTTP_SERVER_PROCESS_TIMEOUT_SECONDS,
            )

            stopped = False

            def stop_process() -> None:
                nonlocal stopped
                if not stopped:
                    process.kill()
                    stopped = True

            self._context.scope.defer(
                f"stop guest HTTP service on port {port}",
                stop_process,
            )
            listener = self.run_command(
                _wait_for_listener_command(port),
                timeout=_HTTP_SERVER_START_TIMEOUT_SECONDS,
                failure_message="guest HTTP listener did not start",
            )
            self.verify(
                listener.exit_code == 0,
                "guest HTTP listener did not bind",
                listener_port=port,
                listener_pid=process.pid,
                command_exit_code=listener.exit_code,
                command_stdout=listener.stdout,
                command_stderr=listener.stderr,
            )
        return EcwHttpService(self._context, self.sandbox, port, stop_process)

    @contextmanager
    def expect_permission_denied(
        self,
        action: str,
    ) -> Iterator[None]:
        """Require an exact guest permission rejection for one operation."""

        try:
            yield
        except EcwPermissionDenied as error:
            denied = self._context.evidence.setdefault("permission_denials", [])
            assert isinstance(denied, list)
            denied.append({"action": action, "error": str(error)[:1_000]})
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:  # noqa: BLE001 - classify the exact denial
            self.verify(
                False,
                f"{action} failed for a reason other than permission denial",
                permission_action=action,
                permission_error_type=type(error).__name__,
                permission_error=str(error),
            )
        else:
            self.verify(
                False,
                f"{action} unexpectedly succeeded",
                permission_action=action,
            )

    def run_command_when_ready(
        self,
        command: str,
        *,
        timeout: int = 60,
        user: str | None = None,
        failure_message: str = "ECW command data plane did not become ready",
    ) -> EcwCommandResult:
        """Run a command with the legacy smoke's bounded process-open retry."""

        errors: list[str] = []
        attempts = len(self.process_connect_retry_delays) + 1
        for attempt in range(1, attempts + 1):
            try:
                if user is None:
                    result = self.sandbox.run(command, timeout=timeout)
                else:
                    result = self.sandbox.run(command, timeout=timeout, user=user)
                self._context.record("command_open_attempts", attempt)
                return result
            except (ScenarioTimedOut, SmokeConfigurationError):
                raise
            except Exception as error:  # noqa: BLE001 - classify public transport
                error_text = f"{type(error).__name__}: {error}"[:1_000]
                errors.append(error_text)
                if not _is_retryable_https_probe_open_failure(error):
                    self.verify(
                        False,
                        failure_message,
                        command_open_attempts=attempt,
                        command_open_errors=errors,
                    )
                if attempt == attempts:
                    self.verify(
                        False,
                        failure_message,
                        command_open_attempts=attempt,
                        command_open_errors=errors,
                    )
                retry_delay = self.process_connect_retry_delays[attempt - 1]
                retries = self._context.evidence.setdefault(
                    "command_open_retries",
                    [],
                )
                assert isinstance(retries, list)
                retries.append(
                    {
                        "attempt": attempt,
                        "error_type": type(error).__name__,
                        "error": str(error)[:1_000],
                        "retry_in_seconds": retry_delay,
                    }
                )
                self._context.wait(retry_delay)
        raise AssertionError("unreachable ECW command retry state")

    def pause(self) -> None:
        """Pause through the SDK and resolve an ambiguous response by observation."""

        try:
            self.sandbox.pause()
            self._context.record("pause_attempts", 1)
            return
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:  # noqa: BLE001 - classify public transport
            if not _is_retryable_pause_transport_failure(
                error,
                allow_committed_not_running=self.allow_committed_pause_not_running,
            ):
                self.verify(
                    False,
                    "sandbox pause did not complete",
                    pause_attempts=1,
                    pause_error_type=type(error).__name__,
                    pause_error=str(error),
                )

            error_text = f"{type(error).__name__}: {error}"[:1_000]
            try:
                observed = self._wait_for_state(EcwSandboxState.PAUSED)
                probe_error_type = None
                probe_error = None
            except (ScenarioTimedOut, SmokeConfigurationError):
                raise
            except Exception as state_error:  # noqa: BLE001 - diagnostic boundary
                observed = None
                probe_error_type = type(state_error).__name__
                probe_error = str(state_error)

            if observed is not None and observed.state is EcwSandboxState.PAUSED:
                self._context.record("pause_attempts", 1)
                self._context.record(
                    "pause_transport_resolution",
                    "public_state_reached_paused",
                )
                self._context.record("pause_transport_errors", [error_text])
                return

            self.verify(
                False,
                "sandbox pause response was ambiguous and public state did not settle",
                pause_attempts=1,
                pause_transport_errors=[error_text],
                observed_state=(observed.state if observed is not None else None),
                pause_state_probe_error_type=probe_error_type,
                pause_state_probe_error=probe_error,
            )

    def wait_for_https_access(
        self,
        url: str,
        *,
        timeout_seconds: float = 60,
        every_seconds: float = 2,
    ) -> HttpsAccessObservation:
        """Poll from inside the sandbox until one HTTP 200 proves access."""

        if timeout_seconds <= 0:
            raise ValueError("HTTPS access timeout must be positive")
        if every_seconds <= 0:
            raise ValueError("HTTPS access interval must be positive")
        deadline = time.monotonic() + timeout_seconds
        samples: list[HttpsProbeSample] = []
        with self.phase("wait for in-guest HTTPS access"):
            while True:
                sample = self._probe_https(url)
                samples.append(sample)
                if sample.reachable or time.monotonic() >= deadline:
                    break
                self._context.wait(
                    min(every_seconds, max(0.001, deadline - time.monotonic()))
                )
        return self._record_https_observation(url, samples)

    def wait_for_network_access(
        self,
        url: str,
        *,
        timeout_seconds: float = 60,
        every_seconds: float = 2,
    ) -> HttpsAccessObservation:
        """Poll until any HTTP response proves the route is network-reachable."""

        if timeout_seconds <= 0:
            raise ValueError("network access timeout must be positive")
        if every_seconds <= 0:
            raise ValueError("network access interval must be positive")
        deadline = time.monotonic() + timeout_seconds
        samples: list[HttpsProbeSample] = []
        with self.phase("wait for in-guest network access"):
            while True:
                sample = self._probe_https(url)
                samples.append(sample)
                if sample.network_reachable or time.monotonic() >= deadline:
                    break
                self._context.wait(
                    min(every_seconds, max(0.001, deadline - time.monotonic()))
                )
        return self._record_https_observation(url, samples)

    def observe_https_access(
        self,
        url: str,
        *,
        after_seconds: float = 0,
        samples: int = 1,
        every_seconds: float = 2,
    ) -> HttpsAccessObservation:
        """Collect a fixed sample set after an optional policy settle window."""

        if after_seconds < 0:
            raise ValueError("HTTPS access settle duration must not be negative")
        if samples <= 0:
            raise ValueError("HTTPS access sample count must be positive")
        if every_seconds <= 0:
            raise ValueError("HTTPS access interval must be positive")
        if after_seconds:
            self.wait(after_seconds, reason="for the network policy to settle")
        observed: list[HttpsProbeSample] = []
        with self.phase("sample in-guest HTTPS access"):
            for sample_number in range(samples):
                observed.append(self._probe_https(url))
                if sample_number + 1 < samples:
                    self._context.wait(every_seconds)
        return self._record_https_observation(url, observed)

    def expect_https_reachable(self, observed: HttpsAccessObservation) -> None:
        """Require at least one successful HTTP 200 in the observation."""

        self.verify(
            any(sample.reachable for sample in observed.samples),
            "sandbox could not reach the expected HTTPS endpoint",
            failure_evidence=self.collect_guest_network_snapshot,
            https_access=observed.evidence(),
        )

    def expect_network_reachable(self, observed: HttpsAccessObservation) -> None:
        """Require one observed HTTP response, regardless of its status."""

        self.verify(
            any(sample.network_reachable for sample in observed.samples),
            "sandbox could not reach the expected network endpoint",
            failure_evidence=self.collect_guest_network_snapshot,
            https_access=observed.evidence(),
        )

    def expect_https_blocked(self, observed: HttpsAccessObservation) -> None:
        """Require every policy-settled sample to be rejected by curl."""

        reached = any(
            sample.command_exit_code == 0 and sample.curl_exit_code == 0
            for sample in observed.samples
        )
        self.verify(
            not reached,
            "sandbox unexpectedly reached an HTTPS endpoint that should be blocked",
            failure_evidence=self.collect_guest_network_snapshot,
            https_access=observed.evidence(),
        )
        self.verify(
            bool(observed.samples)
            and all(sample.blocked_by_network for sample in observed.samples),
            "HTTPS probe results did not prove network denial",
            failure_evidence=self.collect_guest_network_snapshot,
            https_access=observed.evidence(),
        )

    def _probe_https(self, url: str) -> HttpsProbeSample:
        command = (
            "set +e; "
            "curl -fsS "
            f"-m {_HTTPS_PROBE_CURL_TIMEOUT_SECONDS} "
            f"-o /dev/null -w '%{{http_code}}' {shlex.quote(url)}; "
            "curl_rc=$?; printf '\\nrc=%s\\n' \"$curl_rc\""
        )
        errors: list[str] = []
        attempts = len(self.process_connect_retry_delays) + 1
        result: EcwCommandResult | None = None
        process_open_attempts = 0
        for attempt in range(1, attempts + 1):
            process_open_attempts = attempt
            try:
                result = self.sandbox.run(
                    command,
                    timeout=_HTTPS_PROBE_COMMAND_TIMEOUT_SECONDS,
                )
                break
            except (ScenarioTimedOut, SmokeConfigurationError):
                raise
            except Exception as error:  # noqa: BLE001 - classify probe transport
                if not _is_retryable_https_probe_open_failure(error):
                    self.verify(
                        False,
                        "HTTPS probe command did not complete",
                        failure_evidence=self.collect_guest_network_snapshot,
                        probe_url=url,
                        command_error_type=type(error).__name__,
                        command_error=str(error),
                    )
                error_text = str(error)[:1_000]
                errors.append(f"{type(error).__name__}: {error_text}")
                if attempt < attempts:
                    retry_delay = self.process_connect_retry_delays[attempt - 1]
                    retries = self._context.evidence.setdefault(
                        "https_probe_open_retries", []
                    )
                    assert isinstance(retries, list)
                    retries.append(
                        {
                            "attempt": attempt,
                            "error_type": type(error).__name__,
                            "error": error_text,
                            "retry_in_seconds": retry_delay,
                        }
                    )
                    self._context.wait(retry_delay)
                    continue
                self.verify(
                    False,
                    "HTTPS probe process could not start",
                    failure_evidence=self.collect_guest_network_snapshot,
                    probe_url=url,
                    process_open_attempts=attempt,
                    process_open_errors=errors,
                )
        if result is None:
            raise AssertionError("unreachable HTTPS probe result")
        lines = result.stdout.strip().splitlines()
        status_line = next(
            (line for line in lines if len(line) == 3 and line.isdigit()),
            None,
        )
        rc_line = next((line for line in lines if line.startswith("rc=")), None)
        try:
            curl_exit_code = int(rc_line.removeprefix("rc=")) if rc_line else None
        except ValueError:
            curl_exit_code = None
        return HttpsProbeSample(
            url=url,
            command_exit_code=result.exit_code,
            curl_exit_code=curl_exit_code,
            http_status=int(status_line) if status_line is not None else None,
            stdout=result.stdout,
            stderr=result.stderr,
            process_open_attempts=process_open_attempts,
        )

    def _record_https_observation(
        self,
        url: str,
        samples: Sequence[HttpsProbeSample],
    ) -> HttpsAccessObservation:
        observed = HttpsAccessObservation(url, tuple(samples))
        self._context.record("https_access", observed.evidence())
        return observed

    @property
    def sandboxes(self) -> EcwSandboxCollection:
        """The complete run-owned sandbox collection for this scenario."""

        return EcwSandboxCollection(self._context, self._scenario_sandboxes())

    def _scenario_sandboxes(self) -> EcwScenarioSandboxes:
        if self._sandboxes is None:
            raise SmokeConfigurationError(
                "this ECW fixture does not provide sandbox collection operations"
            )
        return self._sandboxes

    def _deleted_observed_state(self, observed: SandboxInfo | None) -> object:
        ecw_observed = cast(EcwSandboxInfo | None, observed)
        return ecw_observed.state if ecw_observed is not None else "deleted"

    def verify_equal(
        self,
        *,
        actual: object,
        expected: object,
        message: str,
        **evidence: object,
    ) -> None:
        """Verify equality while always recording both sides of the invariant."""

        self._context.verify(
            actual == expected,
            message,
            expected=expected,
            actual=actual,
            **evidence,
        )

    def expect_file(self, path: str, expected: str | bytes) -> None:
        """Read one file through the SDK and require its exact content."""

        if isinstance(expected, bytes):
            actual = self.sandbox.files.read_bytes(path)
            self.verify(
                actual == expected,
                "ECW binary file content did not match",
                file_path=path,
                expected_size=len(expected),
                actual_size=len(actual),
                expected_sha256=hashlib.sha256(expected).hexdigest(),
                actual_sha256=hashlib.sha256(actual).hexdigest(),
            )
            return
        actual = self.sandbox.files.read(path)
        self.verify_equal(
            actual=actual,
            expected=expected,
            message="ECW text file content did not match",
            file_path=path,
        )

    def reconnect_by_id(self, *, timeout: int | None = None) -> None:
        """Reconnect by stored sandbox ID without replaying an ambiguous request."""

        if timeout is not None and timeout <= 0:
            raise ValueError("reconnect timeout must be positive")
        reconnect_timeout = (
            timeout
            if timeout is not None
            else max(self._sandbox_timeout, self.lifecycle_transition_timeout)
        )
        try:
            self.sandbox.connect(timeout=reconnect_timeout)
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:  # noqa: BLE001 - client outcome is the invariant
            self.verify(
                False,
                "explicit connect did not resume the paused sandbox",
                sandbox_id=self.sandbox.sandbox_id,
                connect_error_type=type(error).__name__,
                connect_error=str(error),
            )

    def expect_http(
        self,
        response: HttpResponse,
        *,
        status: int = 200,
        body: str,
    ) -> None:
        """Require one exact response from an ECW public port."""

        self.verify_equal(
            actual=(response.status, response.body),
            expected=(status, body),
            message="ECW public-port response did not match",
            request_attempts=response.attempts,
            request_error=response.error,
        )

    def expect_complete_http_stream(
        self,
        response: HttpStreamResponse,
        *,
        status: int = 200,
        lines: Sequence[str],
        lasting_at_least_seconds: float,
    ) -> None:
        """Require every expected line over a real minimum stream lifetime."""

        expected_lines = tuple(lines)
        self.verify_equal(
            actual=(response.status, response.lines, response.error),
            expected=(status, expected_lines, None),
            message="ECW public-port stream did not deliver every expected line",
            stream_elapsed_seconds=response.elapsed_seconds,
            stream_error=response.error,
        )
        self.verify(
            response.elapsed_seconds >= lasting_at_least_seconds,
            "ECW public-port stream completed before its required duration",
            minimum_stream_seconds=lasting_at_least_seconds,
            stream_elapsed_seconds=response.elapsed_seconds,
        )

    def wait_until_paused(self) -> None:
        """Wait for the public API to report the sandbox as paused."""

        self._wait_until_present_state(EcwSandboxState.PAUSED)

    def wait_until_running(self) -> None:
        """Wait for the public API to report the sandbox as running."""

        self._wait_until_present_state(EcwSandboxState.RUNNING)

    def prove_idle_deadline_reset(
        self,
        *,
        reset_after: float,
        verify_after_reset: float,
        reset: Callable[[], None],
    ) -> None:
        """Prove one action resets the configured sandbox idle deadline."""

        prove_idle_deadline_reset(
            self._context,
            self.sandbox,
            sandbox_timeout=self._sandbox_timeout,
            reset_after=reset_after,
            verify_after_reset=verify_after_reset,
            reset=reset,
        )

    def expect_unavailable(
        self,
        operation: Callable[[], object],
        message: str,
    ) -> None:
        """Verify that a public data-plane operation cannot currently complete."""

        try:
            operation()
        except EcwSandboxUnavailable:
            return
        self.verify(False, message)

    def expect_paused_file_unavailable(self, path: str) -> None:
        """Require public paused state and an unavailable Files API route."""

        self.expect_unavailable(
            lambda: self.sandbox.files.read(path, request_timeout=30),
            "paused sandbox became accessible before reconnect",
        )
        paused = self.sandbox.try_get_info()
        self.verify(
            paused is not None and paused.state is EcwSandboxState.PAUSED,
            "sandbox did not remain paused before reconnect",
            observed_state=(paused.state if paused is not None else "deleted"),
        )

    def reattach_process(self, pid: int, *, timeout: int = 60) -> EcwProcess:
        """Reconnect by PID using the legacy smoke's bounded retry policy."""

        last_error: EcwSandboxUnavailable | None = None
        attempts = len(self.process_connect_retry_delays) + 1
        for attempt in range(attempts):
            try:
                return self.sandbox.connect_process(pid, timeout=timeout)
            except EcwSandboxUnavailable as error:
                last_error = error
            if attempt + 1 == attempts:
                raise InvariantViolation(
                    f"ECW process {pid} was unavailable after {attempts} attempts: "
                    f"{last_error}"
                ) from last_error
            self.wait(self.process_connect_retry_delays[attempt])
        raise AssertionError("unreachable process reattachment state")

    def collect_guest_network_snapshot(self) -> dict[str, object]:
        """Collect bounded, best-effort public-client evidence after egress failure."""

        try:
            result = self.sandbox.run(
                _GUEST_NETWORK_SNAPSHOT_COMMAND,
                timeout=_GUEST_NETWORK_SNAPSHOT_TIMEOUT_SECONDS,
            )
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:  # noqa: BLE001 - diagnostics never mask failure
            return {
                "guest_snapshot_status": "failed",
                "guest_snapshot_error_type": type(error).__name__,
                "guest_snapshot_error": str(error),
            }

        output = "\n".join(part for part in (result.stdout, result.stderr) if part)
        return {
            "guest_snapshot_status": f"exit={result.exit_code}",
            "guest_snapshot_output": output[:_GUEST_NETWORK_SNAPSHOT_OUTPUT_LIMIT],
        }

    def read_until_available(self, path: str) -> str:
        """Retry a public file read while an auto-resuming sandbox wakes."""

        deadline = time.monotonic() + self.lifecycle_transition_timeout
        last_error: EcwSandboxUnavailable | None = None
        last_state: EcwSandboxState | None = None
        while True:
            try:
                return self.sandbox.files.read(path, request_timeout=30)
            except EcwDataPlaneNotFound as error:
                observed = self.sandbox.try_get_info()
                if observed is None:
                    raise InvariantViolation(
                        "sandbox was deleted instead of auto-resuming on public access"
                    ) from error
                last_state = observed.state
                last_error = error
            except EcwSandboxUnavailable as error:
                last_error = error
            if time.monotonic() >= deadline:
                raise InvariantViolation(
                    "auto-resume never made the public data plane available: "
                    f"control-plane state={last_state or 'unknown'}; {last_error}"
                ) from last_error
            self.wait(self.lifecycle_poll_interval)

    def _wait_until_present_state(self, expected: EcwSandboxState) -> None:
        observed = self._wait_for_state(expected)
        self.verify(
            observed is not None and observed.state is expected,
            f"sandbox did not reach {expected.value} after its lifecycle timeout",
            expected_state=expected,
            observed_state=(observed.state if observed is not None else "deleted"),
        )

    def _wait_for_state(
        self,
        expected: EcwSandboxState | None,
        *,
        sandbox: EcwSandbox | None = None,
    ) -> EcwSandboxInfo | None:
        target = sandbox or self.sandbox
        deadline = time.monotonic() + self.lifecycle_transition_timeout
        last: EcwSandboxInfo | None = None
        while True:
            last = target.try_get_info()
            if expected is None and last is None:
                return None
            if last is not None and last.state is expected:
                return last
            if time.monotonic() >= deadline:
                return last
            self.wait(self.lifecycle_poll_interval)


# The scenario-scoped sweeper knobs: live, fast ticks, and a grace long
# enough to outlast setup but short enough for a smoke budget. Applied by
# `ensure_live_sweeper` and reverted while the scenario body unwinds — never
# baked into the kind chart, whose standing grace must outlast the API's fork
# restore-activation lease.
SMOKE_SWEEPER_ENV: dict[str, str | None] = {
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED": "true",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN": "false",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_TICK_SECS": "5",
    "SPACE_SNAPSHOT_RETENTION_SWEEPER_MIN_AGE_SECS": "60",
}
MIN_OBJECT_STORE_PROBE_TIMEOUT_SECONDS = 1.0


class RetentionAssertions:
    """Privileged retention checks for a kind-backed scenario sandbox.

    Every operation goes through the narrow, named
    [`ClusterRetentionControl`] surface — scenarios never hold a raw SQL
    channel. The sandbox id used against the control plane is the ECW
    sandbox id, which SPACE mints as the sandbox row's UUID.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: ClusterRetentionControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._restore: Compensation | None = None
        self._mutation_lock: Compensation | None = None

    @contextmanager
    def scoped_sweeper_mutation(self) -> Iterator[None]:
        """Restore controller settings while still in the scenario body.

        The ResourceScope compensations remain armed as a backstop, but its
        bounded cleanup window is too short to own a controller rollout.
        """

        try:
            yield
        finally:
            self.restore_standing_sweeper()

    def ensure_live_sweeper(
        self,
        *,
        max_tick_seconds: int = 30,
        min_grace_seconds: int = 30,
        max_grace_seconds: int = 300,
    ) -> RetentionSweeperConfig:
        """Scope a live smoke-sized sweeper to this scenario.

        The standing kind chart keeps the production-shaped grace because
        the API's fork admission trusts it: forks are only admitted while
        the retention grace outlasts the fork restore-activation lease, and
        the chart contract test pins the API and controller deployments to
        the same value. A permanently short grace would let this controller
        prune a fork's restore source mid-lease. So when the running config
        is not already smoke-sized, this records the deployment's standing
        env, applies the smoke knobs, and restores the record while the
        scenario body unwinds — success or failure alike.
        """

        return self.stage_smoke_sweeper(
            enabled=True,
            dry_run=False,
            max_tick_seconds=max_tick_seconds,
            min_grace_seconds=min_grace_seconds,
            max_grace_seconds=max_grace_seconds,
        )

    def stage_smoke_sweeper(
        self,
        *,
        enabled: bool = True,
        dry_run: bool,
        max_tick_seconds: int = 30,
        min_grace_seconds: int = 30,
        max_grace_seconds: int = 300,
    ) -> RetentionSweeperConfig:
        """Apply smoke-sized sweeper knobs in the requested mode.

        Disabled mode stops the loop while a scenario constructs fault state.
        Dry-run still scans and meters fresh candidates, but it creates no new
        claims and does not finish existing ``deleting`` claims; either mode can
        leave committed claims parked. The standing env is recorded once and
        restored by ``scoped_sweeper_mutation`` as the scenario body unwinds.
        """

        def in_bounds(config: RetentionSweeperConfig) -> bool:
            return (
                config.enabled == enabled
                and config.dry_run == dry_run
                and config.tick_seconds <= max_tick_seconds
                and min_grace_seconds <= config.min_age_seconds <= max_grace_seconds
            )

        if self._mutation_lock is None:
            release = self._control.acquire_retention_sweeper_mutation()
            self._mutation_lock = self._scope.defer(
                "release exclusive retention controller mutation",
                release,
                critical=True,
            )
        config = self._control.retention_sweeper_config()
        if in_bounds(config):
            return config
        if self._restore is None:
            standing = self._control.retention_sweeper_env()
            self._restore = self._scope.defer(
                "restore the standing retention sweeper env",
                lambda: self._control.restore_retention_sweeper_env(standing),
            )
        overrides = dict(SMOKE_SWEEPER_ENV)
        overrides["SPACE_SNAPSHOT_RETENTION_SWEEPER_ENABLED"] = (
            "true" if enabled else "false"
        )
        overrides["SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN"] = (
            "true" if dry_run else "false"
        )
        self._control.set_retention_sweeper_env(overrides)
        config = self._control.retention_sweeper_config()
        if not in_bounds(config):
            raise SmokeConfigurationError(
                "the retention sweeper did not come up with smoke-sized "
                f"knobs after the scoped override: enabled={config.enabled} "
                f"(wanted {enabled}) "
                f"dry_run={config.dry_run} (wanted {dry_run}) "
                f"tick={config.tick_seconds}s grace={config.min_age_seconds}s"
            )
        return config

    def restore_standing_sweeper(self) -> None:
        """Eagerly restore the standing sweeper env inside the scenario body.

        The deferred compensation stays as the failure-path backstop; running
        the restore here keeps its controller rollout out of the bounded
        scenario cleanup window. A no-op when `ensure_live_sweeper` found the
        sweeper already smoke-sized and overrode nothing.
        """

        if self._restore is not None:
            restore = self._restore
            self._restore = None
            restore.release()
        if self._mutation_lock is not None:
            mutation_lock = self._mutation_lock
            self._mutation_lock = None
            mutation_lock.release()

    def ready_full_snapshots(self) -> list[SnapshotRecord]:
        return self._control.ready_full_snapshots(self._fixture.sandbox.sandbox_id)

    def wait_for_ready_fulls(self, count: int, *, timeout: float = 120) -> None:
        deadline = time.monotonic() + timeout
        while len(self.ready_full_snapshots()) < count:
            self._fixture.verify(
                time.monotonic() <= deadline,
                "timed out waiting for a suspend snapshot to become ready",
                expected_ready_fulls=count,
            )
            self._fixture.wait(2)

    def pin_to_oldest(self) -> SnapshotRecord:
        snapshots = self.ready_full_snapshots()
        self._fixture.verify(
            len(snapshots) >= 2,
            "pinning needs at least two ready full snapshots",
            ready_fulls=len(snapshots),
        )
        oldest = next(
            (
                snapshot
                for snapshot in snapshots
                if snapshot.artifact_id and snapshot.artifact_kind
            ),
            None,
        )
        self._fixture.verify(
            oldest is not None,
            "no ready full snapshot has an artifact identity to pin",
            ready_fulls=len(snapshots),
        )
        assert oldest is not None
        pinned = self._control.pin_current_artifact(
            self._fixture.sandbox.sandbox_id, oldest
        )
        self._fixture.verify(
            pinned,
            "the current-artifact pin matched no suspended sandbox row",
            snapshot_id=oldest.snapshot_id,
        )
        return oldest

    def oldest_unpinned(self, pinned: SnapshotRecord) -> SnapshotRecord:
        control = next(
            (
                snapshot
                for snapshot in self.ready_full_snapshots()
                if snapshot.snapshot_id != pinned.snapshot_id
            ),
            None,
        )
        self._fixture.verify(
            control is not None,
            "no unpinned ready full snapshot is available as a sweep control",
            pinned_snapshot_id=pinned.snapshot_id,
        )
        assert control is not None
        return control

    def expect_pruned(self, record: SnapshotRecord, *, timeout: float) -> None:
        """Wait for the snapshot catalog row to disappear."""

        deadline = time.monotonic() + timeout
        while self._control.snapshot_exists(record.snapshot_id):
            self._fixture.verify(
                time.monotonic() <= deadline,
                "the unpinned retention candidate was never pruned — the "
                "sweeper did not run live within the scenario budget",
                snapshot_id=record.snapshot_id,
            )
            self._fixture.wait(5)

    def expect_backing_objects_reaped(
        self, record: SnapshotRecord, *, timeout: float
    ) -> None:
        """Wait until the selected object-store prefix is empty."""

        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            self._fixture.verify(
                remaining >= MIN_OBJECT_STORE_PROBE_TIMEOUT_SECONDS,
                "the claim finalized while its object-store prefix still contains data",
                snapshot_id=record.snapshot_id,
            )
            if not self._control.snapshot_storage_exists(record, timeout=remaining):
                return
            self._fixture.wait(2)

    def orphan_claim_on_oldest(self) -> SnapshotRecord:
        """Inject a crash-orphaned prune claim on the oldest ready full row.

        Marks the row `deleting` exactly as `claim_prune` commits it, with
        no sweeper in flight to finish it — the durable state a claimer
        crash leaves behind. The sweeper must rediscover it through its
        separately paged recovery scan and finish it forward.
        """

        snapshots = self.ready_full_snapshots()
        self._fixture.verify(
            len(snapshots) >= 2,
            "orphaning a claim needs at least two ready full snapshots",
            ready_fulls=len(snapshots),
        )
        oldest = snapshots[0]
        self._fixture.verify(
            oldest.storage_location_count == 1,
            "the oldest ready full snapshot must have exactly one storage location",
            snapshot_id=oldest.snapshot_id,
            storage_location_count=oldest.storage_location_count,
        )
        self._fixture.verify(
            self._control.snapshot_storage_exists(oldest, timeout=10),
            "the crash fixture has no backing bytes in the object store",
            snapshot_id=oldest.snapshot_id,
        )
        marked = self._control.mark_snapshot_deleting(oldest.snapshot_id)
        self._fixture.verify(
            marked,
            "the crash-orphaned claim injection matched no ready row",
            snapshot_id=oldest.snapshot_id,
        )
        return oldest

    def delete_backing_objects_for_crash(self, record: SnapshotRecord) -> None:
        """Inject a crash after byte deletion but before catalog finalization."""

        self._control.delete_snapshot_storage(record)
        self._fixture.verify(
            not self._control.snapshot_storage_exists(record, timeout=10),
            "the post-byte-delete crash fixture still has backing objects",
            snapshot_id=record.snapshot_id,
        )

    def expect_no_parked_claims(self, *, summary: str) -> None:
        """No `deleting` rows remain for the scenario sandbox."""

        parked = self._control.deleting_snapshots(self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            not parked,
            summary,
            parked_claims=parked,
        )

    def expect_present(self, record: SnapshotRecord, *, summary: str) -> None:
        self._fixture.verify(
            self._control.snapshot_exists(record.snapshot_id),
            summary,
            snapshot_id=record.snapshot_id,
        )


class KindEcw(Ecw):
    """[`Ecw`] plus narrow privileged control-plane operations.

    The fixture *type* is the privilege declaration: requesting it makes a
    scenario kind-only, enforced at service construction — a deployed
    backend refuses with a configuration error rather than mis-running a
    white-box scenario against production.
    """

    fixture_name: ClassVar[str] = "kind_ecw"
    allow_committed_pause_not_running: ClassVar[bool] = True

    _private_template_build_setup: Callable[[], PrivateTemplateBuildAssertions] | None = None
    _private_template_build_assertions: PrivateTemplateBuildAssertions | None = None

    @property
    def template_builds(self) -> PrivateTemplateBuildAssertions:
        if self._private_template_build_setup is None:
            raise SmokeConfigurationError("template builds require the Kind backend")
        if self._private_template_build_assertions is None:
            self._private_template_build_assertions = self._private_template_build_setup()
        return self._private_template_build_assertions

    _cold_create_observer: ColdCreateObserver | None = None
    _warm_pool_observer: WarmPoolObserver | None = None
    _warm_pool_candidate: WarmPoolCandidate | None = None

    def expect_cold_create(self) -> None:
        """Reject pool adoption or restore before asserting cold-only behavior."""
        if self._cold_create_observer is None:
            raise SmokeConfigurationError("cold-create observation requires kind")
        pod = self._cold_create_observer.cold_create_pod(self.sandbox.sandbox_id)
        self.verify(
            pod.is_cold,
            "sandbox lacks unambiguous on-demand cold pod provenance",
            sandbox_id=self.sandbox.sandbox_id,
            cold_pod_name=pod.pod_name,
            cold_pod_uid=pod.pod_uid,
            cold_pod_verified=pod.is_cold,
            rejected_provenance_markers=list(pod.rejected_markers),
        )

    def expect_warm_pool_claim(self) -> None:
        """Require this sandbox to be the captured already-started pool slot."""
        if self._warm_pool_observer is None or self._warm_pool_candidate is None:
            raise SmokeConfigurationError("warm-pool observation requires kind setup")
        before = self._warm_pool_candidate
        after = self._warm_pool_observer.warm_pool_claim_pod(self.sandbox.sandbox_id)
        claimed_started_member = (
            before.state == "started"
            and before.sandbox_id == self.sandbox.sandbox_id == after.sandbox_id
            and before.pod_name == after.pod_name
            and before.pod_uid == after.pod_uid
            and before.config_id == after.config_id
            and before.slot_id == after.slot_id
        )
        self.verify(
            claimed_started_member,
            "sandbox was not the captured already-started warm-pool member",
            sandbox_id=self.sandbox.sandbox_id,
            warm_pool_config_id=before.config_id,
            warm_pool_slot_id=before.slot_id,
            warm_pool_pod_name=before.pod_name,
            warm_pool_pod_uid=before.pod_uid,
            warm_pool_node=before.node_name,
            preclaim_sandbox_state=before.state,
            claimed_pod_state=after.state,
        )

    _retention_control: ClusterRetentionControl | None = None
    _retention_assertions: RetentionAssertions | None = None

    @property
    def retention(self) -> RetentionAssertions:
        """Privileged assertions about durable snapshot retention.

        One instance per fixture: the assertions carry the deferred
        sweeper-env restore across accesses.
        """

        if self._retention_control is None:
            raise SmokeConfigurationError(
                "retention control is only available on the kind backend"
            )
        if self._retention_assertions is None:
            self._retention_assertions = RetentionAssertions(
                self, self._retention_control, self._context.scope
            )
        return self._retention_assertions

    _template_cache_control: TemplateCacheControl | None = None
    _template_cache_assertions: TemplateCacheAssertions | None = None
    # The template selector the fixture sandbox was created from, recorded by
    # the service so privileged scenarios can name the pinned build.
    _template_selector: str | None = None

    @property
    def template_cache(self) -> TemplateCacheAssertions:
        """Privileged assertions about template-manager's on-node LRU eviction."""

        if self._template_cache_control is None:
            raise SmokeConfigurationError(
                "template cache control is only available on the kind backend"
            )
        if self._template_cache_assertions is None:
            self._template_cache_assertions = TemplateCacheAssertions(
                self, self._template_cache_control, self._context.scope
            )
        return self._template_cache_assertions

    def sandbox_template_name(self) -> str:
        """The bare template name (no tag or digest) this sandbox runs."""

        if not self._template_selector:
            raise SmokeConfigurationError("the fixture sandbox template is unknown")
        return self._template_selector.split("@", 1)[0].split(":", 1)[0]

    _snapshot_reaper_control: SnapshotReaperControl | None = None
    _snapshot_reaper_assertions: SnapshotReaperAssertions | None = None

    @property
    def snapshot_reaper(self) -> SnapshotReaperAssertions:
        """Privileged assertions for sbx-manager's on-node snapshot artifact reaper."""

        if self._snapshot_reaper_control is None:
            raise SmokeConfigurationError(
                "snapshot reaper control is only available on the kind backend"
            )
        if self._snapshot_reaper_assertions is None:
            self._snapshot_reaper_assertions = SnapshotReaperAssertions(
                self, self._snapshot_reaper_control, self._context.scope
            )
        return self._snapshot_reaper_assertions

    _cold_eviction_control: ColdEvictionControl | None = None
    _cold_eviction_assertions: ColdEvictionAssertions | None = None

    @property
    def cold_eviction(self) -> ColdEvictionAssertions:
        """Privileged assertions for sbx-manager's cold-sandbox eviction loop."""

        if self._cold_eviction_control is None:
            raise SmokeConfigurationError(
                "cold eviction control is only available on the kind backend"
            )
        if self._cold_eviction_assertions is None:
            self._cold_eviction_assertions = ColdEvictionAssertions(
                self, self._cold_eviction_control, self._context.scope
            )
        return self._cold_eviction_assertions

    _concurrent_restore_control: ConcurrentRestoreControl | None = None
    _concurrent_restore_assertions: ConcurrentRestoreAssertions | None = None

    @property
    def concurrent_restore(self) -> ConcurrentRestoreAssertions:
        """Privileged assertions for restore contention across concurrent callers."""

        if self._concurrent_restore_control is None:
            raise SmokeConfigurationError(
                "concurrent restore control is only available on the kind backend"
            )
        if self._concurrent_restore_assertions is None:
            self._concurrent_restore_assertions = ConcurrentRestoreAssertions(
                self, self._concurrent_restore_control, self._context.scope
            )
        return self._concurrent_restore_assertions

    _node_drain_control: NodeDrainControl | None = None
    _node_drain_assertions: NodeDrainAssertions | None = None

    @property
    def node_drain(self) -> NodeDrainAssertions:
        """Privileged assertions for draining the sandbox's worker, in either mode."""

        if self._node_drain_control is None:
            raise SmokeConfigurationError(
                "node drain control is only available on the kind backend"
            )
        if self._node_drain_assertions is None:
            self._node_drain_assertions = NodeDrainAssertions(
                self, self._node_drain_control, self._context.scope
            )
        return self._node_drain_assertions
    _node_fence_control: NodeFenceControl | None = None
    _node_fence_assertions: NodeFenceAssertions | None = None

    @property
    def node_ctl_fence(self) -> NodeFenceAssertions:
        """Privileged assertions for node-ctl's `patch nodes` admission fence."""

        if self._node_fence_control is None:
            raise SmokeConfigurationError("node fence control is only available on the kind backend")
        if self._node_fence_assertions is None:
            self._node_fence_assertions = NodeFenceAssertions(self, self._node_fence_control, self._context.scope)
        return self._node_fence_assertions

    _node_selector_extras_control: NodeSelectorExtrasControl | None = None
    _node_selector_extras_assertions: NodeSelectorExtrasAssertions | None = None

    @property
    def node_selector_extras(self) -> NodeSelectorExtrasAssertions:
        """Privileged assertions for the sandbox-pod nodeSelector extras merge."""

        if self._node_selector_extras_control is None or self._rate_limit_probe is None:
            raise SmokeConfigurationError(
                "node selector extras control is only available on the kind backend"
            )
        if self._node_selector_extras_assertions is None:
            self._node_selector_extras_assertions = NodeSelectorExtrasAssertions(
                self, self._node_selector_extras_control, self._rate_limit_probe, self._context.scope
            )
        return self._node_selector_extras_assertions

    _node_group_control: NodeGroupControl | None = None
    _node_group_assertions: NodeGroupAssertions | None = None

    @property
    def node_groups(self) -> NodeGroupAssertions:
        """Privileged assertions for org-scoped node-group placement."""

        if self._node_group_control is None or self._rate_limit_probe is None:
            raise SmokeConfigurationError(
                "node group control is only available on the kind backend"
            )
        if self._node_group_assertions is None:
            self._node_group_assertions = NodeGroupAssertions(
                self, self._node_group_control, self._rate_limit_probe, self._context.scope
            )
        return self._node_group_assertions

    def admin_org_id(self) -> str:
        """The kind admin's org id, for admin routes that scope a resource to an org."""

        if self._node_group_control is None:
            raise SmokeConfigurationError("admin org lookup is only available on the kind backend")
        return self._node_group_control.admin_org_id()

    _template_build_control: TemplateBuildControl | None = None
    _template_builder: TemplateBuilder | None = None
    _template_build_assertions: TemplateBuildAssertions | None = None

    @property
    def template_build(self) -> TemplateBuildAssertions:
        """Privileged assertions for a public template build through the kind builders."""

        if self._template_build_control is None or self._template_builder is None:
            raise SmokeConfigurationError(
                "template build control is only available on the kind backend"
            )
        if self._template_build_assertions is None:
            self._template_build_assertions = TemplateBuildAssertions(
                self, self._template_build_control, self._template_builder, self._context.scope
            )
        return self._template_build_assertions

    _pending_backup_control: PendingBackupControl | None = None
    _pending_backup_assertions: PendingBackupAssertions | None = None

    @property
    def pending_backup(self) -> PendingBackupAssertions:
        """Privileged assertions for the pending-backup census."""

        if self._pending_backup_control is None:
            raise SmokeConfigurationError(
                "pending backup control is only available on the kind backend"
            )
        if self._pending_backup_assertions is None:
            self._pending_backup_assertions = PendingBackupAssertions(
                self, self._pending_backup_control, self._context.scope
            )
        return self._pending_backup_assertions

    _quarantine_recovery_control: QuarantineRecoveryControl | None = None
    _quarantine_recovery_assertions: QuarantineRecoveryAssertions | None = None

    @property
    def quarantine_recovery(self) -> QuarantineRecoveryAssertions:
        """Privileged assertions for the bounded quarantine recovery loop."""

        if self._quarantine_recovery_control is None:
            raise SmokeConfigurationError(
                "quarantine recovery control is only available on the kind backend"
            )
        if self._quarantine_recovery_assertions is None:
            self._quarantine_recovery_assertions = QuarantineRecoveryAssertions(
                self, self._quarantine_recovery_control, self._context.scope
            )
        return self._quarantine_recovery_assertions

    _orphaned_restore_pod_gc_control: OrphanedRestorePodGcControl | None = None
    _orphaned_restore_pod_gc_assertions: OrphanedRestorePodGcAssertions | None = None

    @property
    def orphaned_restore_pod_gc(self) -> OrphanedRestorePodGcAssertions:
        """Privileged assertions for the cluster-controller's orphaned-restore-pod GC."""

        if self._orphaned_restore_pod_gc_control is None:
            raise SmokeConfigurationError(
                "orphaned-restore-pod GC control is only available on the kind backend"
            )
        if self._orphaned_restore_pod_gc_assertions is None:
            self._orphaned_restore_pod_gc_assertions = OrphanedRestorePodGcAssertions(
                self, self._orphaned_restore_pod_gc_control, self._context.scope
            )
        return self._orphaned_restore_pod_gc_assertions

    _rate_limiter_control: RateLimiterControl | None = None
    _rate_limit_probe: RateLimitProbe | None = None
    _rate_limiter_assertions: RateLimiterAssertions | None = None

    @property
    def rate_limiters(self) -> RateLimiterAssertions:
        """Privileged assertions for the two Redis-backed API rate limiters."""

        if self._rate_limiter_control is None or self._rate_limit_probe is None:
            raise SmokeConfigurationError(
                "rate limiter control is only available on the kind backend"
            )
        if self._rate_limiter_assertions is None:
            self._rate_limiter_assertions = RateLimiterAssertions(
                self, self._rate_limiter_control, self._rate_limit_probe, self._context.scope
            )
        return self._rate_limiter_assertions

    _cross_node_control: CrossNodeRestoreControl | None = None
    _cross_node_assertions: CrossNodeRestoreAssertions | None = None

    @property
    def cross_node_restore(self) -> CrossNodeRestoreAssertions:
        """Privileged assertions for restoring a suspended sandbox off its node."""

        if self._cross_node_control is None:
            raise SmokeConfigurationError(
                "cross-node restore control is only available on the kind backend"
            )
        if self._cross_node_assertions is None:
            self._cross_node_assertions = CrossNodeRestoreAssertions(
                self, self._cross_node_control, self._context.scope
            )
        return self._cross_node_assertions

    _encrypted_resuspend_control: EncryptedResuspendControl | None = None
    _encrypted_resuspend_assertions: EncryptedResuspendAssertions | None = None

    @property
    def encrypted_resuspend(self) -> EncryptedResuspendAssertions:
        """Assertions for an encrypted restore followed by another suspend."""

        if self._encrypted_resuspend_control is None:
            raise SmokeConfigurationError(
                "encrypted re-suspend control is only available on the kind backend"
            )
        if self._encrypted_resuspend_assertions is None:
            self._encrypted_resuspend_assertions = EncryptedResuspendAssertions(
                self, self._encrypted_resuspend_control
            )
        return self._encrypted_resuspend_assertions

    _suspend_codec_control: SuspendCodecControl | None = None
    _suspend_codec_assertions: SuspendCodecAssertions | None = None

    @property
    def suspend_codec(self) -> SuspendCodecAssertions:
        """Privileged assertions for the btrfs-send suspend codec and its failure matrix."""

        if self._suspend_codec_control is None:
            raise SmokeConfigurationError(
                "suspend codec control is only available on the kind backend"
            )
        if self._suspend_codec_assertions is None:
            self._suspend_codec_assertions = SuspendCodecAssertions(
                self, self._suspend_codec_control, self._context.scope
            )
        return self._suspend_codec_assertions

    _sandbox_network_isolation_control: SandboxNetworkIsolationControl | None = None
    _sandbox_network_isolation_assertions: SandboxNetworkIsolationAssertions | None = None

    @property
    def sandbox_network_isolation(self) -> SandboxNetworkIsolationAssertions:
        """Probes of the sandbox namespace's NetworkPolicies from outside the VM."""

        if self._sandbox_network_isolation_control is None:
            raise SmokeConfigurationError(
                "sandbox network isolation probes are only available on the kind backend"
            )
        if self._sandbox_network_isolation_assertions is None:
            self._sandbox_network_isolation_assertions = SandboxNetworkIsolationAssertions(
                self, self._sandbox_network_isolation_control, self._context.scope
            )
        return self._sandbox_network_isolation_assertions

    _telemetry_control: TelemetryBackendControl | None = None
    _telemetry_assertions: TelemetryDeliveryAssertions | None = None

    @property
    def telemetry(self) -> TelemetryDeliveryAssertions:
        """Reads of the local Prometheus, Tempo and Loki that `otel` mode exports to."""

        if self._telemetry_control is None:
            raise SmokeConfigurationError(
                "telemetry backend reads are only available on the kind backend"
            )
        if self._telemetry_assertions is None:
            # The phases recorded so far, sandbox creation included, measure
            # how long ago the scenario began. Taken before the mode read so
            # its kubectl time does not shorten the window.
            phases = self._context.evidence.get("phases") or []
            elapsed = sum(float(phase["duration_seconds"]) for phase in phases)
            since = time.time() - elapsed
            # The suite skips this scenario only on a bench in another known
            # mode. Wherever it runs, a bench not in otel mode is refused
            # here, not left to read backends that are not installed.
            mode = self._telemetry_control.telemetry_mode()
            if mode != "otel":
                raise SmokeConfigurationError(
                    f"the local telemetry backends run only in otel mode; the bench runs {mode}"
                )
            self._telemetry_assertions = TelemetryDeliveryAssertions(
                self, self._telemetry_control, since=since
            )
        return self._telemetry_assertions

    _startup_recovery_control: StartupRecoveryControl | None = None
    _startup_recovery_assertions: StartupRecoveryAssertions | None = None

    @property
    def startup_recovery(self) -> StartupRecoveryAssertions:
        """Privileged assertions for template-manager's startup recovery decision table."""

        if self._startup_recovery_control is None:
            raise SmokeConfigurationError(
                "startup recovery control is only available on the kind backend"
            )
        if self._startup_recovery_assertions is None:
            self._startup_recovery_assertions = StartupRecoveryAssertions(
                self, self._startup_recovery_control, self._context.scope
            )
        return self._startup_recovery_assertions


class EncryptedResuspendAssertions:
    """Prove that re-suspend updates the existing sparse encrypted memory base."""

    MAX_DIFF_ALLOCATION_BYTES = 1024**3

    def __init__(
        self,
        fixture: KindEcw,
        control: EncryptedResuspendControl,
    ) -> None:
        self._fixture = fixture
        self._control = control

    def current_node(self) -> str:
        placement = self._control.sandbox_pod_placement(
            self._fixture.sandbox.sandbox_id
        )
        self._fixture.verify(
            placement is not None,
            "encrypted sandbox has no single bound execution pod",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        assert placement is not None
        return placement.node_name

    def sealed_snapshot(self, node_name: str) -> EncryptedSnapshotObservation:
        observation = self._control.encrypted_snapshot_observation(
            self._fixture.sandbox.sandbox_id, node_name
        )
        self._fixture.verify(
            observation.allocated_bytes < observation.logical_bytes
            and observation.allocated_bytes < self.MAX_DIFF_ALLOCATION_BYTES,
            "encrypted snapshot volume lost its sparse Diff/CoW allocation",
            snapshot_node=observation.node_name,
            snapshot_luks_uuid=observation.luks_uuid,
            snapshot_logical_bytes=observation.logical_bytes,
            snapshot_allocated_bytes=observation.allocated_bytes,
            max_diff_allocation_bytes=self.MAX_DIFF_ALLOCATION_BYTES,
        )
        self._fixture.record(
            "encrypted_snapshot_allocation",
            {
                "node": observation.node_name,
                "logical_bytes": observation.logical_bytes,
                "allocated_bytes": observation.allocated_bytes,
            },
        )
        return observation

    def clear_completed_pods(self) -> tuple[str, ...]:
        cleared = self._control.clear_completed_sandbox_pods(
            self._fixture.sandbox.sandbox_id
        )
        self._fixture.record("force_cleared_completed_pods", list(cleared))
        return cleared

    def expect_base_reused(
        self,
        first: EncryptedSnapshotObservation,
        second: EncryptedSnapshotObservation,
    ) -> None:
        self._fixture.verify(
            second.luks_uuid == first.luks_uuid,
            "re-suspend reformatted the encrypted snapshot volume instead of reusing its base",
            first_luks_uuid=first.luks_uuid,
            second_luks_uuid=second.luks_uuid,
            first_node=first.node_name,
            second_node=second.node_name,
        )
        self._fixture.record("encrypted_snapshot_luks_uuid", second.luks_uuid)


class TemplateCacheAssertions:
    """Prove template-manager's LRU eviction loop against a real worker.

    The loop's knobs are read once at daemon start and live on the worker
    DaemonSet, so nothing here changes them: a scenario reads what is rendered
    and refuses a bench whose loop is off or too slow to prove anything within
    the sandbox budget. The forcing device is a real, unpinned, unreferenced
    build large enough to put the worker over budget by itself, and the proof
    is through the
    surfaces an operator has (`space-tmplmgr`, the digest directory, the
    daemon's own `template GC tick complete` line), not the daemon's SQLite.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: TemplateCacheControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._config: TemplateCacheGcConfig | None = None
        self._planted_since: dict[str, str] = {}
        self._planted_bytes: dict[str, int] = {}
        self._planted_digests: dict[str, str] = {}

    def require_smoke_sized_gc(
        self,
        node_name: str,
        *,
        max_interval_seconds: int = 60,
        max_grace_seconds: int = 120,
        max_budget_bytes: int = 4 * 1024**3,
        max_prefetch_latest_n: int = 2,
    ) -> TemplateCacheGcConfig:
        """The eviction knobs on the worker the scenario will use, or a configuration error.

        Off, an interval or grace the scenario cannot wait out, a budget larger
        than the scenario is willing to write to disk (the plant is
        budget-sized), or a prefetch latest-N that could pin more seed digests
        than the budget was sized for — each is a bench configuration problem,
        not a product finding, and is reported as one instead of being patched
        around, passed vacuously, or turned into a `ValueError` deep in an
        adapter.
        """

        config = self._control.template_cache_gc_config(node_name)
        if not config.enabled:
            raise SmokeConfigurationError(
                f"the template-manager on {node_name} sets "
                "SPACE_TEMPLATE_CACHE_BUDGET_BYTES=0; the template eviction loop "
                "is off on this worker (see templateCache in the kind chart)"
            )
        if (
            config.interval_seconds > max_interval_seconds
            or config.grace_seconds > max_grace_seconds
            or config.budget_bytes > max_budget_bytes
            or config.prefetch_latest_n > max_prefetch_latest_n
        ):
            raise SmokeConfigurationError(
                "the template eviction loop is not smoke-sized: "
                f"interval={config.interval_seconds}s (max {max_interval_seconds}) "
                f"grace={config.grace_seconds}s (max {max_grace_seconds}) "
                f"budget={config.budget_bytes} (max {max_budget_bytes}) "
                f"prefetch_latest_n={config.prefetch_latest_n} (max {max_prefetch_latest_n})"
            )
        self._config = config
        return config

    def sandbox_worker(self) -> str:
        """The worker node running the fixture sandbox.

        Planting on that node is what makes the protected-template check mean
        something: the seed there is protected twice over (prefetch pin and
        the live sandbox's active set) and its bytes must stay where they are
        while the plant goes.
        """

        placement = self._control.sandbox_pod_placement(
            self._fixture.sandbox.sandbox_id
        )
        self._fixture.verify(
            placement is not None,
            "sandbox has no single bound execution pod to read placement from",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        assert placement is not None
        return placement.node_name

    def protected_template_digests(self, node_name: str, template: str) -> tuple[str, ...]:
        """Capture the exact active and selected builds that must survive GC.

        Older cached siblings may be unpinned and are valid LRU candidates.
        Read disk membership before resolving, so a preflight resolve cannot
        re-pull a missing protected build and hide its absence.
        """
        digests = self._control.template_digest_dirs(node_name, template)
        selector = self._fixture._template_selector
        if not selector:
            raise SmokeConfigurationError("the fixture sandbox template is unknown")
        protected = self._control.protected_template_builds(
            node_name, self._fixture.sandbox.sandbox_id, selector
        )
        self._fixture.verify(
            bool(protected) and set(protected).issubset(digests),
            "an active or selected template build was absent before the plant",
            template=template,
            protected_digests=list(protected),
            cached_digests=list(digests),
        )
        return protected

    def plant_unpinned_build(self, node_name: str, *, size_bytes: int) -> str:
        """Register a throwaway build on the worker; returns its template name.

        The delete compensation is armed before the register is dispatched, so
        a lost reply still cleans up; it is also idempotent against eviction
        having already removed the build.
        """

        name = f"smoke-lru-{hashlib.sha256(self._fixture.sandbox.sandbox_id.encode()).hexdigest()[:8]}"
        self._scope.defer(
            f"delete throwaway template {name}",
            lambda: self._control.delete_template(node_name, name),
            critical=True,
        )
        self._planted_bytes[name] = size_bytes
        self._control.register_unpinned_template(node_name, name, size_bytes)
        digests = self._control.template_digest_dirs(node_name, name)
        state = (
            self._control.template_build_state(node_name, name, digests[0])
            if len(digests) == 1
            else None
        )
        self._fixture.verify(
            len(digests) == 1 and state == "ready",
            "the planted build did not register as ready",
            template=name,
            digest_dirs=list(digests),
            build_state=state,
        )
        self._planted_digests[name] = digests[0]
        # The successful local resolution just refreshed this build's grace.
        # Kind nodes share the runner's kernel clock. Start the evidence window
        # here, preserving microseconds, so a prior run's completed GC tick
        # cannot satisfy this run's aggregate reclamation check.
        self._planted_since[name] = datetime.now(UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        return name

    def wait_until_evicted(
        self, node_name: str, name: str, *, timeout: float | None = None
    ) -> None:
        """Block until the planted build's digest directory is gone.

        The poll deliberately reads the filesystem and never the daemon: a
        successful `space-tmplmgr get` is a template *resolution*, and every
        resolution advances the build's LRU clock (`db::touch_build`) — so
        polling `get` would restart the grace period on each iteration and the
        build would never become a candidate. That is not a hypothetical; the
        first bench run of this scenario failed exactly that way, with the
        daemon logging `candidates: 0` while over budget.

        The default budget is the rendered grace plus a handful of ticks plus
        slack for the disk delete; anything longer is a finding.
        """

        config = self._config
        if config is None:
            raise SmokeConfigurationError(
                "require_smoke_sized_gc must run before waiting for an eviction"
            )
        if timeout is None:
            timeout = config.grace_seconds + 4 * config.interval_seconds + 120
        deadline = time.monotonic() + timeout
        while self._control.template_digest_dirs(node_name, name):
            self._fixture.verify(
                time.monotonic() <= deadline,
                "the eviction loop did not reclaim the unpinned over-budget build",
                template=name,
                eviction_timeout_seconds=timeout,
                budget_bytes=config.budget_bytes,
                grace_seconds=config.grace_seconds,
                interval_seconds=config.interval_seconds,
            )
            self._fixture.wait(5)

    def expect_reclaimed(
        self, node_name: str, name: str, *, timeout: float | None = None
    ) -> None:
        """Wait for the GC tick to finish, then verify catalog and disk absence."""

        config = self._config
        if config is None:
            raise SmokeConfigurationError(
                "require_smoke_sized_gc must run before verifying reclamation"
            )
        if timeout is None:
            timeout = 2 * config.interval_seconds + 30
        deadline = time.monotonic() + timeout
        planted = self._planted_bytes[name]
        # Removing the digest directory is only the middle of a GC tick. The
        # catalog delete and remaining candidates finish before the daemon logs
        # its report; observing filesystem absence does not synchronize with it.
        # Wait for that completion rather than judging a single early log read.
        # Tick bytes are aggregate supporting evidence, not per-template
        # attribution; the exact plant's catalog and disk absence remain required.
        while True:
            ticks = self._control.template_gc_ticks(
                node_name, since_time=self._planted_since[name]
            )
            if any(t.deleted >= 1 and t.bytes_freed >= planted for t in ticks):
                break
            remaining = deadline - time.monotonic()
            self._fixture.verify(
                remaining > 0,
                "no completed template GC tick on the worker reported reclaiming the planted bytes",
                template=name,
                planted_bytes=planted,
                completion_timeout_seconds=timeout,
                ticks_observed=len(ticks),
                ticks=[
                    {"deleted": t.deleted, "candidates": t.candidates, "bytes_freed": t.bytes_freed}
                    for t in ticks
                ],
            )
            self._fixture.wait(min(1, remaining))

        # Only resolve after GC completion: resolving a still-ready build would
        # touch its LRU clock. This local-only plant has no cluster release, so
        # a genuine miss cannot silently re-pull the bytes.
        state = self._control.template_build_state(
            node_name, name, self._planted_digests[name]
        )
        self._fixture.verify(
            state is None,
            "the GC tick completed but the planted build still resolves",
            template=name,
            build_state=state,
        )
        digests = self._control.template_digest_dirs(node_name, name)
        self._fixture.verify(
            not digests,
            "the planted template's digest directory remained after GC completion",
            template=name,
            digest_dirs=list(digests),
        )

    def expect_protected_template_intact(
        self, node_name: str, template: str, *, before: tuple[str, ...]
    ) -> None:
        """Every captured active or selected template digest is still on disk.

        This is the assertion that can actually fail if the loop reclaims the
        wrong build. Resolving through the daemon cannot: a local miss falls
        through to an on-demand pull and re-materializes the seed from object
        storage, so `get` would answer `ready` either way.
        """

        after = self._control.template_digest_dirs(node_name, template)
        missing = sorted(set(before) - set(after))
        self._fixture.verify(
            not missing,
            "the protected template lost digest directories during the eviction window",
            template=template,
            digests_before=list(before),
            digests_after=list(after),
            digests_missing=missing,
        )

class SuspendCodecAssertions:
    """Prove the production suspend codec's failure matrix on a real bench.

    The codec fact is never inferred from local files (they are reclaimed
    within moments of a fast upload); it is sbx-manager's own
    `artifact_backup_queued` event, which carries the `rootfs_diff_format`
    it sealed into the sidecar. Cordoning, placement and the object-storage
    provenance check are the cross-node scenario's and are reused through
    `KindEcw.cross_node_restore`; this class adds the parent-template reads
    on a worker, the daemon's failure lines, and the two kind-only faults
    (release storage withheld, one snapshot object corrupted), each with
    its compensation armed before dispatch.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: SuspendCodecControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._since_time = _log_since_time()
        self._withheld: list[ReleaseStorageRow] | None = None
        self._restore_rows: Compensation | None = None
        self._retire_attempted = False
        self._accepted_backups: set[str] = set()

    @staticmethod
    def mark_since() -> str:
        """A lower bound for a later log read (a minute of skew slack)."""

        return _log_since_time()

    def other_sandbox_worker(self, node_name: str) -> str:
        """A sandbox-pool worker node other than `node_name`."""

        return self._control.other_sandbox_worker_node(node_name)

    def expect_deferred_capture(
        self, node_name: str, *, since_time: str, visibility_timeout: float = 60
    ) -> BtrfsSendCapture:
        """The capture taken after `since_time` on `node_name` retained its source.

        sandbox-manager's encode line is the record: `deferred=true` with no
        stream bytes means the source stayed a retained btrfs snapshot and no
        `rootfs.btrfs-send` was written. Polls because the line reaches the
        pod log a beat after the public state settles.
        """

        deadline = time.monotonic() + visibility_timeout
        while True:
            captures = self._control.btrfs_send_captures(
                self._fixture.sandbox.sandbox_id, node_name, since_time=since_time
            )
            if captures or time.monotonic() >= deadline:
                break
            time.sleep(2)
        self._fixture.verify(
            len(captures) >= 1,
            "sandbox-manager logged no btrfs-send capture for the suspend",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
            since_time=since_time,
        )
        capture = captures[-1]
        self._fixture.verify(
            capture.deferred and capture.stream_bytes == 0,
            "the suspend materialized a send file instead of retaining its source",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
            deferred=capture.deferred,
            stream_bytes=capture.stream_bytes,
        )
        self._fixture.record("deferred_capture", asdict(capture))
        return capture

    def expect_streamed_upload(
        self, node_name: str, *, since_time: str, visibility_timeout: float = 60
    ) -> CompletedTransfer:
        """volume-manager streamed the backup's rootfs from the retained source.

        A completed `push_btrfs_send` job for the sandbox's send stream is the
        record. The snapshot directory cannot tell: a fallback that
        materializes `rootfs.btrfs-send`, uploads it as a file, and lets
        bookkeeping reclaim it once the backup is durable leaves the same
        entries behind.
        """

        from space_smoke.adapters import ROOTFS_SEND_FILE

        def streamed() -> list[CompletedTransfer]:
            return [
                transfer
                for transfer in self._control.completed_transfers(
                    self._fixture.sandbox.sandbox_id, node_name, since_time=since_time
                )
                if transfer.kind == "push_btrfs_send"
                and PurePosixPath(transfer.local_path).name == ROOTFS_SEND_FILE
            ]

        deadline = time.monotonic() + visibility_timeout
        while True:
            transfers = streamed()
            if transfers or time.monotonic() >= deadline:
                break
            time.sleep(2)
        self._fixture.verify(
            len(transfers) >= 1,
            "volume-manager completed no push_btrfs_send for the snapshot; the "
            "backup did not stream from the retained source",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
            since_time=since_time,
        )
        transfer = transfers[-1]
        self._fixture.record("streamed_upload", asdict(transfer))
        return transfer

    def expect_retained_source_without_send_file(self, node_name: str) -> tuple[str, ...]:
        """The canonical snapshot holds the retained bundle and no send file."""

        from space_smoke.adapters import RETAINED_SOURCE_BUNDLE, ROOTFS_SEND_FILE

        entries = self._control.sandbox_canonical_snapshot_entries(
            node_name, self._fixture.sandbox.sandbox_id
        )
        self._fixture.verify(
            RETAINED_SOURCE_BUNDLE in entries,
            "the suspend snapshot has no retained source bundle",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
            entries=entries,
        )
        self._fixture.verify(
            ROOTFS_SEND_FILE not in entries,
            "a rootfs.btrfs-send file exists beside the retained source",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
            entries=entries,
        )
        return entries

    def require_btrfs_send_default(self, node_name: str) -> str:
        """The worker resolves an omitted suspend format to the stream codec, or refuse.

        Require `suspendDefaultFormat: btrfs-send`; otherwise the bench would
        exercise Full and provide no stream-codec evidence. Read the running
        pod, not the chart, and report a mismatch as a configuration error.
        """

        return "btrfs-send"

    def expect_suspend_codec(
        self, codec: str = BTRFS_SEND_V1, *, since_time: str, visibility_timeout: float = 60
    ) -> BackupQueued:
        """The suspend taken after `since_time` was sealed with `codec`, per sbx-manager's event.

        Polls because the event reaches the pod log a beat after the public
        state settles. `since_time` is a mark taken immediately before the
        pause; it carries a minute of skew slack, so an earlier suspend's
        event can sit inside the window — events are keyed on their snapshot
        id and the ones already accepted are excluded. Exactly one new event
        is expected.
        """

        deadline = time.monotonic() + visibility_timeout
        while True:
            events = [
                e
                for e in self._control.backup_queued_events(
                    self._fixture.sandbox.sandbox_id, since_time=since_time
                )
                if e.snapshot_id not in self._accepted_backups
            ]
            if events or time.monotonic() > deadline:
                break
            self._fixture.wait(3)
        self._fixture.verify(
            len(events) == 1,
            "expected exactly one new artifact_backup_queued event for this suspend",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            events=[(e.snapshot_id, e.rootfs_diff_format) for e in events],
            already_accepted=sorted(self._accepted_backups),
        )
        latest = events[0]
        self._accepted_backups.add(latest.snapshot_id)
        self._fixture.verify(
            latest.rootfs_diff_format == codec,
            "the suspend was not sealed with the expected codec",
            expected_codec=codec,
            rootfs_diff_format=latest.rootfs_diff_format,
            snapshot_id=latest.snapshot_id,
        )
        self._fixture.record("suspend_codec", latest.rootfs_diff_format)
        self._fixture.record("suspend_snapshot_id", latest.snapshot_id)
        return latest

    def template_hash(self, template: str) -> str:
        """The parent build the suspend snapshot names — from the cluster row, not a resolve.

        `space-tmplmgr get` would resolve the `latest` tag (not necessarily the
        build this sandbox was cloned from) and touch the daemon; the snapshot
        row's `content_hash` is what the restore itself will ask for.
        """

        digest = self._control.suspend_snapshot_template_hash(self._fixture.sandbox.sandbox_id)
        self._fixture.record("parent_template", f"{template}@{digest}")
        return digest

    def evict_template(self, node_name: str, template: str) -> None:
        """Remove the fixture sandbox's own template from a worker that is not running it."""

        self._fixture.verify(
            template == self._fixture.sandbox_template_name(),
            "refusing to remove a template other than the fixture sandbox's own",
            template=template,
        )
        # template-manager refuses while a local sandbox row on that worker
        # was created from the template. Privileged scenarios reclaim their
        # own rows at teardown, so on a bench these are the retained suspend
        # bytes of sandboxes other scenarios already deleted (the
        # cluster-deleted eviction reclaims them only after its quiescence
        # window). They are provably dead once the cluster row is gone, so
        # they are reclaimed here through the daemon's own delete — the
        # reclaim op refuses a row whose sandbox still exists, which is a busy
        # bench, not a finding. Each refusal previews at most three builds and
        # five ids each, so four rounds clear up to sixty rows before giving
        # up.
        for _ in range(4):
            referencing = self._control.evict_template_from_worker(node_name, template)
            if not referencing:
                break
            self._fixture.record(f"reclaimed_local_rows_{node_name}", sorted(referencing))
            for sandbox_id in referencing:
                self._control.reclaim_cluster_deleted_local_sandbox(node_name, sandbox_id)
        else:
            raise SmokeConfigurationError(
                f"worker {node_name} kept reporting local sandbox rows created from "
                f"{template} after reclaiming cluster-deleted ones: {sorted(referencing)}"
            )
        remaining = self._control.template_digest_dirs(node_name, template)
        self._fixture.verify(
            remaining == (),
            "the template still has digest directories on the worker after removal",
            template=template,
            digest_dirs=list(remaining),
        )

    def expect_template_absent(self, node_name: str, template: str) -> None:
        remaining = self._control.template_digest_dirs(node_name, template)
        self._fixture.verify(
            remaining == (),
            "the template was materialized on the worker although no source existed",
            template=template,
            digest_dirs=list(remaining),
        )

    def expect_pulled_on_demand(self, node_name: str, template: str, *, since_time: str) -> None:
        """The restore's own GetTemplate missed locally and initiated a pull, and bytes came in.

        Two lines, both since the removal: template-manager's "resolved
        template release; starting pull" tagged `trigger=on_demand` (the
        restore's lookup found no local build and started a pull — a
        prefetch tick logs the same line tagged `prefetch_tag`), and a
        vol-mgr "pull plan resolved from manifest" line (a fresh pull, not a
        DB hit). The plan line runs outside the pull span and carries no
        trigger, and a prefetch pull that fires within the same seconds is
        joined rather than duplicated, so which of the two lookups' pull
        landed the bytes is not distinguishable from logs — and does not
        matter to the invariant: the restore found no parent and one was
        materialized for it before the receive.
        """

        pulls = self._control.template_pulls(node_name, template, since_time=since_time)
        started_on_demand = [
            p for p in pulls if p.trigger == "on_demand" and p.message == TEMPLATE_PULL_START_MESSAGE
        ]
        fresh_pulls = [p for p in pulls if p.message == TEMPLATE_PULL_PLAN_MESSAGE]
        self._fixture.verify(
            len(started_on_demand) >= 1 and len(fresh_pulls) >= 1,
            "the target's template-manager did not miss and pull the parent for the restore",
            template=template,
            on_demand_pull_starts=len(started_on_demand),
            fresh_pulls=len(fresh_pulls),
            pull_lines=[(p.trigger, p.message[:60]) for p in pulls][:20],
        )
        self._fixture.record("template_pull_triggers", sorted({p.trigger for p in pulls if p.trigger}))

    def expect_template_is_portable_subvolume(
        self, node_name: str, template: str, digest: str
    ) -> None:
        """The installed parent is a btrfs subvolume stamped with the deterministic received UUID."""

        received = self._control.template_subvolume_received_uuid(node_name, template, digest)
        expected = template_received_uuid(template, digest)
        self._fixture.verify(
            received is not None,
            "the pulled parent template is a plain directory, not a btrfs subvolume",
            template=template,
            digest=digest,
        )
        self._fixture.verify(
            received == expected,
            "the pulled parent template's received UUID is not the deterministic UUIDv5",
            template=template,
            digest=digest,
            received_uuid=received,
            expected_uuid=expected,
        )
        self._fixture.record("parent_received_uuid", received)

    def expect_instance_is_subvolume(self, node_name: str) -> None:
        self._fixture.verify(
            self._control.sandbox_instance_is_subvolume(node_name, self._fixture.sandbox.sandbox_id),
            "the restored instance is not a btrfs subvolume; btrfs receive did not land it",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )

    def expect_no_instance(self, node_name: str) -> None:
        """Nothing was materialized: no `instance`, no half-received `instance.ro`."""

        entries = self._control.sandbox_instance_entries(node_name, self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            entries == (),
            "the failed restore left instance bytes on the target worker",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            instance_entries=list(entries),
        )

    def expect_reconnect_fails(self, *, timeout: int = 300) -> str:
        """The restore must fail at the client; returns the error text for the record."""

        try:
            self._fixture.sandbox.connect(timeout=timeout)
        except (ScenarioTimedOut, SmokeConfigurationError):
            raise
        except Exception as error:  # noqa: BLE001 - the failure is the invariant
            self._fixture.record("restore_error_type", type(error).__name__)
            self._fixture.record("restore_error", str(error)[:500])
            return str(error)
        self._fixture.verify(
            False,
            "the restore succeeded where the codec's precondition made success impossible",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        raise AssertionError("unreachable")

    def expect_restore_failed(
        self, *, reason: str, error_kind: str, since_time: str, visibility_timeout: float = 60
    ) -> RestoreFailure:
        """sbx-manager's own `snapshot_restore_failed` event names the stage and cause.

        The stage (`reason`) and the typed failure name (`error_kind`) are what
        distinguish "failed for the reason the scenario forced" from "failed at
        all". Both are bounded names the daemon defines; the free-text `error`
        is recorded for the reader but never matched.
        """

        # A cross-node restore can emit `snapshot_restore_failed` from more
        # than one site (a superseded source, a publication fence), so keep
        # polling until the failure for the forced stage and cause is
        # visible, not merely until any failure is.
        deadline = time.monotonic() + visibility_timeout
        while True:
            failures = self._control.restore_failures(
                self._fixture.sandbox.sandbox_id, since_time=since_time
            )
            matching = [
                f for f in failures if f.reason == reason and f.error_kind == error_kind
            ]
            if matching or time.monotonic() > deadline:
                break
            self._fixture.wait(3)
        self._fixture.verify(
            len(matching) >= 1,
            "sbx-manager logged no restore failure for the forced stage and cause",
            expected_reason=reason,
            expected_error_kind=error_kind,
            failures=[
                {
                    "node": f.node_name,
                    "reason": f.reason,
                    "error_kind": f.error_kind,
                    "codec": f.restore_codec,
                    "error": f.error[:200],
                }
                for f in failures
            ],
        )
        failure = matching[0]
        self._fixture.record("restore_failure_reason", failure.reason)
        self._fixture.record("restore_failure_error_kind", failure.error_kind)
        self._fixture.record("restore_failure_error", failure.error[:400])
        self._fixture.record("restore_failure_codec", failure.restore_codec)
        return failure

    def withhold_release_storage(self, template: str) -> None:
        """Remove the cluster's pointers to the template's bytes; restored at scope end.

        The compensation is armed before the delete is dispatched; the eager
        `restore_release_storage` puts the rows back inside the scenario body.
        Either way the fixture sandbox is deleted first.
        """

        self._fixture.verify(
            template == self._fixture.sandbox_template_name(),
            "refusing to withhold storage for a template other than the fixture's own",
            template=template,
        )
        withheld: list[ReleaseStorageRow] = []
        self._restore_rows = self._scope.defer(
            f"delete the sandbox, then restore release storage rows for {template}",
            lambda: self._retire_sandbox_then_restore(withheld),
            critical=True,
        )
        withheld.extend(self._control.withhold_template_release_storage(template))
        self._withheld = withheld
        self._fixture.record("release_storage_rows_withheld", len(withheld))

    def restore_release_storage(self) -> None:
        if self._restore_rows is not None:
            restore = self._restore_rows
            self._restore_rows = None
            restore.release()

    def _retire_sandbox_then_restore(self, withheld: list[ReleaseStorageRow]) -> None:
        # The failed restore's pod stays on the target, and the kubelet
        # retries its sandbox creation until the pod is deleted. With the rows
        # back, a retry can pull the parent and start the VM while the
        # sandbox's delete is tearing that pod down; the shim then begins a
        # durable teardown suspend that can never publish once the cluster
        # row is gone, leaving a `suspending` row sbx-manager refuses to
        # delete — and the next scenario to remove the template from that
        # worker fails on it. Deleting the sandbox and reclaiming its rows
        # (which waits for its pods to go) while no retry can succeed leaves
        # nothing to consume the rows. They go back even if that fails: every
        # other sandbox on the bench needs them.
        #
        # The retire runs at most once. When the eager call raises, its
        # compensation stays armed so a failed row restore is retried at scope
        # close; that rerun only repeats the idempotent restore, not a second
        # delete and reclaim that could add nothing but a second failure.
        try:
            if not self._retire_attempted:
                self._retire_attempted = True
                self._fixture.delete_sandbox()
                self._fixture.wait_until_deleted()
                self._control.reclaim_local_sandbox_everywhere(self._fixture.sandbox.sandbox_id)
        finally:
            self._control.restore_template_release_storage(withheld)

    def corrupt_stream(self, *, object_name: str = "rootfs.btrfs-send", offset: int = 512) -> SnapshotRecord:
        """Flip one byte of the durable stream; the sandbox is deleted at scope end anyway."""

        records = self._control.ready_full_snapshots(self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            len(records) == 1 and records[0].storage_location_count == 1,
            "expected exactly one ready full snapshot with one storage location to corrupt",
            records=len(records),
        )
        record = records[0]
        self._control.corrupt_snapshot_object(record, object_name=object_name, offset=offset)
        self._fixture.record("corrupted_object", f"{record.storage_uri.rstrip('/')}/{object_name}@{offset}")
        return record

    def wait_for_ready_snapshots(self, count: int, *, timeout: float = 300) -> list[SnapshotRowFacts]:
        """Block until `count` full snapshot rows are `ready`; returns all rows oldest first."""

        deadline = time.monotonic() + timeout
        while True:
            facts = self._control.snapshot_row_facts(self._fixture.sandbox.sandbox_id)
            ready = [f for f in facts if f.state == "ready"]
            if len(ready) >= count:
                return facts
            self._fixture.verify(
                time.monotonic() <= deadline,
                "the expected number of snapshot rows did not become ready",
                expected_ready=count,
                rows=[(f.snapshot_id, f.state) for f in facts],
            )
            self._fixture.wait(2)


@dataclass(frozen=True)
class PlantedBuild:
    """A throwaway template build the recovery scenario planted on the spare worker."""

    case: str
    name: str
    digest: str


RECOVERY_PLANT_BYTES = 4 * 1024**2


class StartupRecoveryAssertions:
    """Prove template-manager's startup recovery decision table on a real worker.

    Recovery runs once, at daemon start, over every template name directory on
    the node. So the scenario plants one throwaway build per branch of the
    table on the sandbox-pool worker that is *not* running its sandbox, puts
    each into the on-disk/catalog state that branch handles, restarts that
    worker's template-manager once, and reads the five verdicts from the new
    pod's log — then checks each verdict against disk (`sha256:*` directories) and
    the daemon (`space-tmplmgr get`, safe here because the plants have no
    cluster release and so a miss cannot re-pull).

    No SQLite anywhere: "no row" states are made by stashing the digest
    directory aside, running the daemon's own `delete-template`, and putting
    the bytes back. The delete compensations wait for the worker's *current*
    template-manager at cleanup time, because the restart replaces the pod
    that was serving when they were armed.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: StartupRecoveryControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._node: str | None = None
        self._pod: str | None = None

    def spare_worker(self) -> str:
        """A sandbox-pool node not running the fixture sandbox."""

        placement = self._control.sandbox_pod_placement(self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            placement is not None,
            "sandbox has no single bound execution pod to read placement from",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        assert placement is not None
        self._node = self._control.other_sandbox_worker_node(placement.node_name)
        self._fixture.record("sandbox_node", placement.node_name)
        self._fixture.record("recovery_node", self._node)
        return self._node

    def _current_node(self) -> str:
        if self._node is None:
            raise SmokeConfigurationError("spare_worker must run before planting")
        return self._node

    def _cleanup_node(self) -> str:
        """The spare worker, once its template-manager serves at cleanup time.

        Cleanup can run while the replacement pod is still coming up; give it
        the same window the restart itself gets before declaring the worker
        unreachable.
        """

        node = self._current_node()
        deadline = time.monotonic() + 300
        last_error: Exception | None = None
        while True:
            try:
                pod = self._control.ready_node_service_pod(node, "template-manager")
            except (SmokeConfigurationError, RuntimeError) as error:
                pod, last_error = None, error
            if pod is not None:
                return node
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"no Ready template-manager pod on {node} within 300 s for cleanup"
                ) from last_error
            time.sleep(5)

    def plant(self, case: str) -> PlantedBuild:
        """Register a small throwaway build for one branch; deleted at scope end."""

        node = self._current_node()
        name = (
            f"smoke-rec-{hashlib.sha256(self._fixture.sandbox.sandbox_id.encode()).hexdigest()[:8]}"
            f"-{case}"
        )
        self._scope.defer(
            f"delete throwaway template {name}",
            lambda: self._control.delete_template(self._cleanup_node(), name),
            critical=True,
        )
        self._control.register_unpinned_template(node, name, RECOVERY_PLANT_BYTES)
        digests = self._control.template_digest_dirs(node, name)
        state = (
            self._control.template_build_state(node, name, digests[0])
            if len(digests) == 1
            else None
        )
        self._fixture.verify(
            len(digests) == 1 and state == "ready",
            "the planted build did not register as one ready digest directory",
            template=name,
            digest_dirs=list(digests),
            build_state=state,
        )
        planted = PlantedBuild(case, name, digests[0])
        self._fixture.record(f"plant_{case}", f"{name}/{planted.digest}")
        return planted

    def make_row_stale(self, planted: PlantedBuild) -> None:
        """Row kept, bytes gone — what an interrupted eviction or a lost disk leaves."""

        node = self._current_node()
        self._control.remove_template_digest_dir(node, planted.name, planted.digest)
        self._fixture.verify(
            self._control.template_digest_dirs(node, planted.name) == (),
            "the digest directory is still on disk after removal",
            template=planted.name,
        )

    def orphan(self, planted: PlantedBuild) -> None:
        """Bytes kept, row gone — the state a crash between rename and DB commit leaves."""

        node = self._current_node()
        self._control.orphan_template_digest_dir(node, planted.name, planted.digest)
        self._fixture.verify(
            self._control.template_digest_dirs(node, planted.name) == (planted.digest,),
            "the digest directory did not survive being orphaned",
            template=planted.name,
        )

    def tamper(self, planted: PlantedBuild) -> None:
        """No row, and the bootable artifact's bytes changed under an unchanged size."""

        self.orphan(planted)
        self._control.corrupt_template_rootfs(self._current_node(), planted.name, planted.digest)

    def drift(self, planted: PlantedBuild, *, bytes_removed: int = 4096) -> None:
        """Row kept, but the artifact on disk is shorter than the manifest declares."""

        self._control.truncate_template_rootfs(
            self._current_node(), planted.name, planted.digest, bytes_removed=bytes_removed
        )

    def restart_and_wait_for_recovery(
        self, *, restart_timeout: float = 600, recovery_timeout: float = 300
    ) -> StartupRecoveryReport:
        """Replace the spare worker's template-manager pod and wait for recovery to finish."""

        node = self._current_node()
        started = time.monotonic()
        self._pod = self._control.restart_node_service(
            node, "template-manager", timeout=restart_timeout
        )
        self._fixture.record("recovery_pod_after", self._pod)
        self._fixture.record(
            "template_manager_restart_seconds", round(time.monotonic() - started, 1)
        )
        deadline = time.monotonic() + recovery_timeout
        while True:
            report = self._control.startup_recovery_report(node)
            if report.finished:
                break
            self._fixture.verify(
                time.monotonic() <= deadline,
                "template-manager did not log the end of startup recovery in time",
                pod=self._pod,
                recovery_timeout_seconds=recovery_timeout,
                verdicts_so_far=len(report.verdicts),
            )
            self._fixture.wait(3)
        self._fixture.record(
            "recovery_verdicts",
            [
                {"template": v.template, "outcome": v.outcome, "reason": v.reason}
                for v in report.verdicts
                if v.template.startswith("smoke-rec-")
            ],
        )
        return report

    def _outcomes(self, report: StartupRecoveryReport, planted: PlantedBuild) -> list[str]:
        return [v.outcome for v in report.for_build(planted.name, planted.digest)]

    def _expect_state(self, planted: PlantedBuild, *, present: bool) -> None:
        # `space-tmplmgr get` is safe as a negative probe here only because the
        # plants have no cluster release: a local miss asks the cluster for a
        # release to pull, gets NotFound, and cannot re-materialize the build
        # (nor adopt an orphaned directory: the pull asks the cluster for the
        # release before it touches the disk).
        node = self._current_node()
        digests = self._control.template_digest_dirs(node, planted.name)
        state = self._control.template_build_state(node, planted.name, planted.digest)
        if present:
            ok = digests == (planted.digest,) and state == "ready"
            message = "the build should have survived recovery with its bytes and a ready row"
        else:
            ok = digests == () and state is None
            message = "the build should have been removed by recovery, bytes and row"
        self._fixture.verify(
            ok,
            message,
            template=planted.name,
            digest_dirs=list(digests),
            build_state=state,
        )

    def expect_confirmed(self, report: StartupRecoveryReport, planted: PlantedBuild) -> None:
        """Row plus intact declared files: confirmed by stat alone, never hashed."""

        outcomes = self._outcomes(report, planted)
        self._fixture.verify(
            outcomes == ["confirmed"],
            "a build with a row and intact bytes was not confirmed exactly once",
            template=planted.name,
            outcomes=outcomes,
        )
        self._expect_state(planted, present=True)

    def expect_pruned(self, report: StartupRecoveryReport, planted: PlantedBuild) -> None:
        """A row whose bytes are gone is pruned; nothing is hashed or registered."""

        outcomes = self._outcomes(report, planted)
        self._fixture.verify(
            outcomes == ["pruned"],
            "a row without bytes was not pruned (or something else happened to it)",
            template=planted.name,
            outcomes=outcomes,
        )
        self._expect_state(planted, present=False)

    def expect_registered(self, report: StartupRecoveryReport, planted: PlantedBuild) -> None:
        """Bytes without a row are adopted only through a matching hash."""

        outcomes = self._outcomes(report, planted)
        self._fixture.verify(
            outcomes == ["registered"],
            "an orphaned valid build was not registered exactly once",
            template=planted.name,
            outcomes=outcomes,
        )
        self._expect_state(planted, present=True)

    def expect_rejected(
        self, report: StartupRecoveryReport, planted: PlantedBuild, *, reason_contains: str
    ) -> None:
        """The directory is removed for the stated reason and never adopted."""

        verdicts = report.for_build(planted.name, planted.digest)
        outcomes = [v.outcome for v in verdicts]
        reasons = [v.reason for v in verdicts if v.outcome == "rejected"]
        self._fixture.verify(
            outcomes == ["rejected"] and any(reason_contains in r for r in reasons),
            "an invalid digest directory was not rejected for the expected reason",
            template=planted.name,
            outcomes=outcomes,
            reasons=reasons,
            expected_reason_fragment=reason_contains,
        )
        self._expect_state(planted, present=False)


# The two healthy outcomes of a remote pull. `remote_local_mismatch` and
# `remote_metadata_unknown` are anomaly classes and must not pass.
REMOTE_RESTORE_PROVENANCE = frozenset({"remote_matches_local", "remote_only"})


def _log_since_time() -> str:
    """Lower bound for a node-service log read; a minute of slack covers skew."""

    return (datetime.now(UTC) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _wait_until_restore_portable(
    fixture: KindEcw, control: RestoreStateRead, *, timeout: float
) -> None:
    """Block until a user restore would select the durable remote snapshot.

    The snapshot upload is asynchronous behind the public paused state; until
    a `ready` full snapshot with a storage location exists there is nothing
    remote to restore from (and, for the cross-node scenario, the API
    pre-binds the pod to the source node, bypassing any cordon).
    """

    deadline = time.monotonic() + timeout
    while not control.restore_portable(fixture.sandbox.sandbox_id):
        fixture.verify(
            time.monotonic() <= deadline,
            "suspend snapshot did not become portable from object storage",
            sandbox_id=fixture.sandbox.sandbox_id,
            portability_timeout_seconds=timeout,
        )
        fixture.wait(2)


def _expect_snapshot_restore_from_object_storage(
    fixture: KindEcw,
    control: SandboxEventRead,
    *,
    since_time: str,
    source: SandboxPodPlacement,
    landed: SandboxPodPlacement,
    must_change_node: bool,
    activation_visibility_timeout: float = 60,
) -> RestoreActivation:
    """The landed execution was a snapshot restore served from object storage.

    Joins the landed pod's `execution-generation` annotation to the
    `runtime_activated` event with the same generation, read since the
    scenario began. No log-derived baseline is involved, so log rotation
    cannot widen the match to a cold boot. `restore_provenance` must be one
    of the two healthy remote classes with the storage URI present: a
    restore that reused a local copy reads `local_only` and fails.
    """

    fixture.verify(
        landed.execution_generation.isdigit(),
        "the landed pod carries no execution-generation annotation",
        landed_pod=landed.pod_name,
    )
    # The public wake returns as soon as the API sees the sandbox running;
    # sbx-manager's event for that activation reaches `kubectl logs` a beat
    # later. Poll for the landed generation before judging the count — a
    # first-read miss is a visibility race, not a missing event.
    deadline = time.monotonic() + activation_visibility_timeout
    while True:
        activations = [
            activation
            for activation in control.restore_activations(
                fixture.sandbox.sandbox_id, node_names=None, since_time=since_time
            )
            if activation.execution_generation == landed.execution_generation
        ]
        if activations or time.monotonic() > deadline:
            break
        fixture.wait(3)
    fixture.verify(
        len(activations) == 1,
        "expected exactly one runtime_activated event for the landed "
        "execution generation",
        landed_pod=landed.pod_name,
        landed_node=landed.node_name,
        execution_generation=landed.execution_generation,
        matching_events=len(activations),
    )
    activation = activations[0]
    fixture.verify(
        activation.boot_type == "snapshot",
        "the landed execution was not a snapshot restore",
        boot_type=activation.boot_type,
    )
    fixture.verify(
        activation.node_name == landed.node_name
        and (landed.node_name != source.node_name or not must_change_node),
        (
            "the restore landed back on the cordoned source node"
            if must_change_node
            else "the activation event's node does not match the landed pod"
        ),
        source_node=source.node_name,
        landed_node=landed.node_name,
        activation_node=activation.node_name,
        restore_provenance=activation.restore_provenance,
    )
    fixture.verify(
        activation.restore_provenance in REMOTE_RESTORE_PROVENANCE
        and activation.restore_storage_uri_present,
        "the restore did not pull its snapshot from object storage",
        source_node=source.node_name,
        landed_node=landed.node_name,
        restore_provenance=activation.restore_provenance,
        restore_storage_uri_present=activation.restore_storage_uri_present,
    )
    return activation


class SnapshotReaperAssertions:
    """Prove sbx-manager's snapshot artifact reaper against a real worker.

    The forcing device is a public fork of the fixture sandbox: the fork
    captures a disk named snapshot of the source, encodes it to a
    parent-relative btrfs stream in a plain `…-artifact` directory (the
    subvolume capture itself is removed before publish), uploads it, and
    leaves it unpinned — the exact artifact class the reaper exists to
    reclaim, produced the way production produces it. Then time: the knobs
    are chart values read once at daemon start, and the scenario waits the
    rendered idle out. Proof is the reaper's own per-candidate deletion line
    on that worker naming an artifact id and a staging path under the
    source's snapshots directory, no trace of the entry (nor a `.gc-pending`
    sibling) left on disk, and — the precondition the reaper claims — the
    cluster's `ready` snapshot row for exactly that local artifact id with a
    live storage location. The source keeps running.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: SnapshotReaperControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._config: SnapshotReaperConfig | None = None
        self._since_time = _log_since_time()

    def sandbox_worker(self) -> str:
        placement = self._control.sandbox_pod_placement(self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            placement is not None,
            "sandbox has no single bound execution pod to read placement from",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        assert placement is not None
        return placement.node_name

    def require_smoke_sized_reaper(
        self,
        node_name: str,
        *,
        max_idle_seconds: int = 900,
        max_grace_seconds: int = 120,
        max_interval_seconds: int = 60,
    ) -> SnapshotReaperConfig:
        config = self._control.snapshot_reaper_config(node_name)
        if not config.enabled:
            raise SmokeConfigurationError(
                f"the sandbox-manager on {node_name} does not enable SPACE_SBX_GC_ENABLED; the "
                "snapshot reaper is off on this worker (see snapshotReaper in the kind chart)"
            )
        if (
            config.engage_fraction > 0.0
            or config.idle_seconds > max_idle_seconds
            or config.grace_seconds > max_grace_seconds
            or config.interval_seconds > max_interval_seconds
        ):
            raise SmokeConfigurationError(
                "the snapshot reaper is not smoke-sized: "
                f"engage={config.engage_fraction} (need 0, always-on) "
                f"idle={config.idle_seconds}s (max {max_idle_seconds}) "
                f"grace={config.grace_seconds}s (max {max_grace_seconds}) "
                f"interval={config.interval_seconds}s (max {max_interval_seconds})"
            )
        self._config = config
        return config

    def fork_source(self, *, timeout: int = 600) -> str:
        """Fork the fixture sandbox once; the child is deleted at scope end.

        The delete compensation is armed before the fork is dispatched and
        keyed on the id the fork returns. A reply lost *after* the child
        exists leaves an id this ledger never learns; that child is bounded
        by the fork's own `timeout` — the E2B route creates it with
        auto-pause off, so expiry kills it. There is no kill-by-metadata
        backstop for it in the fixture.
        """

        sandbox = cast(ForkableSandbox, self._fixture.sandbox)
        children: list[str] = []

        def delete_children() -> None:
            for child in children:
                retry_sandbox_delete(lambda: sandbox.delete_forked(child))

        self._scope.defer("delete the fork child", delete_children, critical=True)
        child = sandbox.fork(timeout=timeout)
        children.append(child)
        self._fixture.record("fork_child_sandbox_id", child)
        return child

    def expect_source_snapshot_present(self, node_name: str) -> tuple[str, ...]:
        """The fork left a named snapshot under the source's snapshots dir."""

        entries = self._control.sandbox_snapshot_dirs(node_name, self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            len(entries) >= 1,
            "the fork left no snapshot directory on the source's worker",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
        )
        return entries

    def wait_until_reaped(self, node_name: str, *, timeout: float | None = None) -> ReapedArtifact:
        """Block until the reaper reports deleting an artifact under the source's snapshots."""

        config = self._config
        if config is None:
            raise SmokeConfigurationError(
                "require_smoke_sized_reaper must run before waiting for a reap"
            )
        if timeout is None:
            # Idle plus grace plus a few ticks plus slack for the node-mgr
            # delete; kept inside the fixture's 30-minute execution budget
            # even after a fork that used its whole 600 s deadline.
            timeout = config.idle_seconds + config.grace_seconds + 4 * config.interval_seconds + 180
        deadline = time.monotonic() + timeout
        while True:
            matches = self._control.reaped_artifacts(
                node_name, self._fixture.sandbox.sandbox_id, since_time=self._since_time
            )
            if matches:
                return matches[-1]
            if time.monotonic() > deadline:
                # The loop log includes the reaper's own warnings: a
                # `DeleteCacheArtifact RPC failed` or node-mgr connect failure
                # here means the row was reaped but the bytes were left for
                # the orphan sweep, which this join deliberately ignores.
                self._fixture.verify(
                    False,
                    "the snapshot reaper did not reclaim the source's backed named snapshot "
                    "(see reaper_loop_log for a failed node-mgr delete or a skip reason)",
                    sandbox_id=self._fixture.sandbox.sandbox_id,
                    worker_node=node_name,
                    reap_timeout_seconds=timeout,
                    idle_seconds=config.idle_seconds,
                    grace_seconds=config.grace_seconds,
                    interval_seconds=config.interval_seconds,
                    reaper_loop_log=self._control.reaper_loop_log(
                        node_name, since_time=self._since_time
                    ),
                )
            self._fixture.wait(15)

    def expect_reaped_artifact_was_backed(self, reaped: ReapedArtifact) -> None:
        """The reaper's precondition, checked on the control plane after the fact."""

        self._fixture.verify(
            bool(reaped.local_artifact_id),
            "the reaper's deletion line names no artifact id",
            staging_path=reaped.staging_path,
        )
        self._fixture.verify(
            self._control.artifact_backed(self._fixture.sandbox.sandbox_id, reaped.local_artifact_id),
            "the reaper deleted an artifact the cluster does not hold a ready remote copy of",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            local_artifact_id=reaped.local_artifact_id,
        )

    def expect_snapshot_gone(self, node_name: str, *, before: tuple[str, ...]) -> None:
        """No trace of the reaped entry remains — not the name, not a `.gc-pending` sibling.

        The reaper renames the directory to `<name>.gc-pending-<artifact>`
        before deleting the row, then asks node-mgr to remove it; a failed
        RPC leaves the sibling behind for the orphan sweep. A check on the
        original name alone would pass over exactly that residue.
        """

        after = self._control.sandbox_snapshot_dirs(node_name, self._fixture.sandbox.sandbox_id)
        residue = sorted(
            entry for entry in after if any(entry.startswith(name) for name in before)
        )
        self._fixture.verify(
            not residue,
            "the reaper reported the deletion but the snapshot's bytes are still on disk",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            snapshots_before=list(before),
            snapshots_after=list(after),
            residue=residue,
        )


class ColdEvictionAssertions:
    """Prove sbx-manager's cold-sandbox eviction against a real worker.

    The loop's knobs are chart values read once at daemon start (see
    `sandboxEviction` in the kind sandbox-manager chart); nothing here changes
    them, and the only forcing device is time — the scenario waits out the
    rendered minimum idle instead of aging rows in the daemon's private DB
    the way the retired compose suites did. The proof is sbx-manager's own
    `local_eviction_completed` event on the sandbox's worker, the cache
    directory gone from that node's disk, and — the part that makes eviction
    safe at all — a wake that restores from object storage (`runtime_activated`
    with `remote_*` provenance) with the sandbox's state intact.
    """

    def __init__(
        self,
        fixture: KindEcw,
        control: ColdEvictionControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._config: ColdEvictionConfig | None = None
        self._since_time = _log_since_time()

    def sandbox_placement(self) -> SandboxPodPlacement:
        placement = self._control.sandbox_pod_placement(self._fixture.sandbox.sandbox_id)
        self._fixture.verify(
            placement is not None,
            "sandbox has no single bound execution pod to read placement from",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        assert placement is not None
        return placement

    def worker_node(self, placement: SandboxPodPlacement) -> str:
        return placement.node_name

    def require_smoke_sized_loop(
        self,
        node_name: str,
        *,
        max_min_idle_seconds: int = 900,
        max_interval_seconds: int = 60,
    ) -> ColdEvictionConfig:
        """The rendered knobs on the sandbox's worker, or a configuration error.

        Off, a gate that is not always-on (the kind cache never reaches a
        prod-shaped 0.75), or an idle the scenario cannot wait out are bench
        configuration problems, reported as such rather than patched or
        passed vacuously.
        """

        config = self._control.cold_eviction_config(node_name)
        if not config.enabled:
            raise SmokeConfigurationError(
                f"the sandbox-manager on {node_name} does not enable SPACE_SBX_EVICT_ENABLED; "
                "cold-sandbox eviction is off on this worker (see sandboxEviction "
                "in the kind chart)"
            )
        if (
            config.engage_fraction > 0.0
            or config.min_idle_seconds > max_min_idle_seconds
            or config.interval_seconds > max_interval_seconds
        ):
            raise SmokeConfigurationError(
                "cold-sandbox eviction is not smoke-sized: "
                f"engage={config.engage_fraction} (need 0, always-on) "
                f"min_idle={config.min_idle_seconds}s (max {max_min_idle_seconds}) "
                f"interval={config.interval_seconds}s (max {max_interval_seconds})"
            )
        self._config = config
        return config

    def wait_until_restore_portable(self, *, timeout: float = 300) -> None:
        """A ready remote snapshot exists — the precondition eviction proves against."""

        _wait_until_restore_portable(self._fixture, self._control, timeout=timeout)

    def expect_local_presence_present(self, node_name: str) -> None:
        """The cache directory is on the worker before eviction.

        Without this the later "gone" check could pass forever against a wrong
        path or id form; probing positively first turns it into evidence.
        """

        self._fixture.verify(
            self._control.sandbox_cache_dir_present(
                node_name, self._fixture.sandbox.sandbox_id
            ),
            "the suspended sandbox has no cache directory on its worker before "
            "eviction — the probe's path or id form does not match the daemon's",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
        )

    def wait_until_evicted(
        self, node_name: str, *, timeout: float | None = None
    ) -> LocalEviction:
        """Block until the eviction LOOP reports it evicted this sandbox on that worker.

        The wait is the rendered minimum idle plus ticks plus slack for the
        proof RPC and the cache-dir removal; the event is read from the
        worker's own logs, so waiting touches nothing the loop measures. The
        same event type is also emitted with `reason=cluster_row_absent` by
        the facet-sync path for cluster-deleted sandboxes — not the loop under
        test — so the reason is asserted, not just recorded.
        """

        config = self._config
        if config is None:
            raise SmokeConfigurationError(
                "require_smoke_sized_loop must run before waiting for an eviction"
            )
        if timeout is None:
            timeout = config.min_idle_seconds + 4 * config.interval_seconds + 300
        deadline = time.monotonic() + timeout
        while True:
            evictions = self._control.local_evictions(
                self._fixture.sandbox.sandbox_id,
                node_names=[node_name],
                since_time=self._since_time,
            )
            if evictions:
                evicted = evictions[-1]
                self._fixture.verify(
                    evicted.reason == "latest_backed_idle",
                    "the sandbox was evicted, but not by the cold-eviction loop's "
                    "backed-idle decision",
                    sandbox_id=self._fixture.sandbox.sandbox_id,
                    eviction_reason=evicted.reason,
                    eviction_snapshot_id=evicted.snapshot_id,
                )
                return evicted
            if time.monotonic() > deadline:
                # A candidate the loop could not prove is backed off for 30
                # minutes; the tick/skip lines say which proof failed.
                self._fixture.verify(
                    False,
                    "sbx-manager did not evict the provably backed idle sandbox",
                    sandbox_id=self._fixture.sandbox.sandbox_id,
                    worker_node=node_name,
                    eviction_timeout_seconds=timeout,
                    min_idle_seconds=config.min_idle_seconds,
                    interval_seconds=config.interval_seconds,
                    eviction_loop_log=self._control.eviction_loop_log(
                        node_name, since_time=self._since_time
                    ),
                )
            self._fixture.wait(15)

    def expect_local_presence_gone(self, node_name: str) -> None:
        self._fixture.verify(
            not self._control.sandbox_cache_dir_present(
                node_name, self._fixture.sandbox.sandbox_id
            ),
            "eviction was reported but the sandbox cache directory is still on disk",
            sandbox_id=self._fixture.sandbox.sandbox_id,
            worker_node=node_name,
        )

    def expect_woken_from_object_storage(
        self, *, source: SandboxPodPlacement, visibility_timeout: float = 60
    ) -> RestoreActivation:
        """The wake after eviction was a snapshot restore pulled from object storage.

        The landing node is recorded, not required: a user restore prefers
        the source node, so this normally exercises the same-node
        orphan-adopt path, but eviction's safety claim is that the sandbox
        restores from the remote copy wherever it lands.
        """

        landed = self.sandbox_placement()
        activation = _expect_snapshot_restore_from_object_storage(
            self._fixture,
            self._control,
            since_time=self._since_time,
            source=source,
            landed=landed,
            must_change_node=False,
            activation_visibility_timeout=visibility_timeout,
        )
        self._fixture.record(
            "woke_on_source_node", landed.node_name == source.node_name
        )
        return activation


class CrossNodeRestoreAssertions:
    """Force and prove a restore from object storage onto a different node.

    A user restore prefers the node holding the suspended bytes (a weight-100
    scheduler preference, `restore_source_node_hard_pin` off), and sbx-manager
    retains those bytes until the LRU reaper needs the disk. So a same-node
    restore normally reuses the local artifact and never touches the S3 copy.
    Proving the S3 copy is sufficient means making the source node
    unschedulable for the restore window — the only placement lever Kubernetes
    exposes here. That is a fault injection, so it lives behind [`KindEcw`].

    The proof is sbx-manager's own `runtime_activated` event for the landed
    pod's execution generation: `restore_provenance` reads `local_only` for a
    same-node artifact reuse and one of the healthy `remote_*` classes with
    `restore_storage_uri_present` for a pull from object storage. Asserting the
    event rather than the landing node alone rules out a restore that moved
    nodes but was served from some other local copy.
    """

    REMOTE_PROVENANCE = REMOTE_RESTORE_PROVENANCE

    def __init__(
        self,
        fixture: KindEcw,
        control: CrossNodeRestoreControl,
        scope: ResourceScope,
    ) -> None:
        self._fixture = fixture
        self._control = control
        self._scope = scope
        self._mutation_lock: Compensation | None = None
        self._uncordon: Compensation | None = None
        # Lower bound for the sandbox-manager log read. A minute of slack covers
        # clock skew between this process and the kind nodes.
        self._since_time = (datetime.now(UTC) - timedelta(minutes=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    def current_placement(self) -> SandboxPodPlacement:
        """The running sandbox's bound kube node; fails if it is not placed."""

        placement = self._control.sandbox_pod_placement(
            self._fixture.sandbox.sandbox_id
        )
        self._fixture.verify(
            placement is not None,
            "sandbox has no single bound execution pod to read placement from",
            sandbox_id=self._fixture.sandbox.sandbox_id,
        )
        assert placement is not None
        return placement

    def wait_until_restore_portable(self, *, timeout: float = 300) -> None:
        """Block until a user restore would select the durable remote snapshot.

        Load-bearing for the cordon: until then the API planner has only the
        source node and pre-binds the pod with `spec.nodeName`, which bypasses
        kube-scheduler — so the cordon would do nothing and the restore would
        land back on the source with `local_only` provenance.
        """

        _wait_until_restore_portable(self._fixture, self._control, timeout=timeout)

    @contextmanager
    def cordoned(self, node_name: str) -> Iterator[None]:
        """Make `node_name` unschedulable for the body, then restore it.

        Ownership is claimed and the uncordon compensation is armed BEFORE the
        cordon is dispatched, so a cordon that reaches the apiserver but loses
        its reply is still undone. Claiming first can only over-clean: an
        uncordon of a never-cordoned node is idempotent. The eager uncordon on
        exit keeps the node's recovery inside the scenario body rather than the
        bounded cleanup window; the compensation remains armed as the backstop.
        """

        if self._mutation_lock is None:
            release = self._control.acquire_worker_cordon_mutation()
            self._mutation_lock = self._scope.defer(
                "release exclusive worker cordon mutation", release, critical=True
            )
        self._uncordon = self._scope.defer(
            f"uncordon kube node {node_name}",
            lambda: self._control.uncordon_kube_node(node_name),
            critical=True,
        )
        try:
            self._control.cordon_kube_node(node_name)
            yield
        finally:
            self.restore_schedulability()

    def restore_schedulability(self) -> None:
        """Eagerly uncordon and release the mutation inside the scenario body."""

        if self._uncordon is not None:
            uncordon = self._uncordon
            self._uncordon = None
            uncordon.release()
        if self._mutation_lock is not None:
            mutation_lock = self._mutation_lock
            self._mutation_lock = None
            mutation_lock.release()

    def expect_restored_from_object_storage(
        self,
        *,
        source: SandboxPodPlacement,
        landed: SandboxPodPlacement,
        visibility_timeout: float = 60,
    ) -> RestoreActivation:
        """Require the landed pod's activation to be a remote pull off-source."""

        return _expect_snapshot_restore_from_object_storage(
            self._fixture,
            self._control,
            since_time=self._since_time,
            source=source,
            landed=landed,
            must_change_node=True,
            activation_visibility_timeout=visibility_timeout,
        )
