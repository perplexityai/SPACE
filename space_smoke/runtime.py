"""Execution, evidence, subprocess, and cleanup primitives."""

from __future__ import annotations

import contextvars
import json
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Literal, Protocol, TypeVar

from space_smoke.model import ScenarioResult, Verdict

REPO_ROOT = Path(__file__).resolve().parents[1]


class SmokeConfigurationError(RuntimeError):
    """The scenario cannot safely run with the supplied configuration."""


class NoWarmPoolPod(SmokeConfigurationError):
    """No live pool pod matched a warm-pool lookup: for a sandbox, it booted cold."""


class InvariantViolation(AssertionError):
    """A completed observation disproved the scenario invariant."""


class InconclusiveSmoke(RuntimeError):
    """The scenario did not collect enough evidence for a verdict."""


class QuarantinedSmoke(RuntimeError):
    """A known, narrowly classified failure occurred and remains visible."""


class ScenarioTimedOut(RuntimeError):
    """The scenario exceeded its declared wall-clock budget."""


@dataclass(frozen=True)
class CommandResult:
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str

    def detail(self) -> str:
        return (self.stderr or self.stdout).strip()


class CommandExecutor(Protocol):
    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout: float | None = None,
        env: Mapping[str, str | None] | None = None,
    ) -> CommandResult: ...


class SubprocessExecutor:
    def __init__(
        self,
        *,
        cwd: Path = REPO_ROOT,
        base_env: Mapping[str, str] | None = None,
    ) -> None:
        self.cwd = cwd
        self.base_env = dict(base_env or {})

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout: float | None = None,
        env: Mapping[str, str | None] | None = None,
    ) -> CommandResult:
        command_env = dict(self.base_env)
        if env is not None:
            for name, value in env.items():
                if value is None:
                    command_env.pop(name, None)
                else:
                    command_env[name] = value
        children = _CALL_GROUP.get()
        try:
            if children is None:
                completed = subprocess.run(
                    list(command),
                    cwd=self.cwd,
                    env=command_env if command_env or env is not None else None,
                    input=input_text,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                    check=False,
                )
            else:
                completed = children.run(
                    list(command),
                    cwd=self.cwd,
                    env=command_env if command_env or env is not None else None,
                    input_text=input_text,
                    timeout=timeout,
                )
            return CommandResult(
                tuple(command),
                completed.returncode,
                completed.stdout,
                completed.stderr,
            )
        except subprocess.TimeoutExpired as error:
            stdout = error.stdout or ""
            stderr = error.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode(errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            return CommandResult(
                tuple(command),
                124,
                stdout,
                f"{stderr}\ncommand timed out after {timeout}s".lstrip(),
            )
        except OSError as error:
            return CommandResult(tuple(command), 127, "", str(error))


@dataclass(frozen=True)
class CleanupFailure:
    name: str
    detail: str
    critical: bool


class Compensation:
    def __init__(
        self,
        name: str,
        callback: Callable[[], None],
        *,
        critical: bool,
        timeout_seconds: float | None = None,
    ) -> None:
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("compensation timeout must be positive")
        self.name = name
        self.callback = callback
        self.critical = critical
        self.timeout_seconds = timeout_seconds
        self.armed = True

    def release(self) -> None:
        if not self.armed:
            return
        self.callback()
        self.armed = False

    def disarm(self) -> None:
        self.armed = False


class BackgroundWork:
    """Run a callable on its own thread and make its join a verdict.

    A scenario puts work on a thread to hold a window open — callers firing,
    a cap lifter waiting for the first refusal, a pod watch — while the
    scenario acts on the main thread. Its evidence counts only if the work
    finished (a timed `Thread.join` returns whether or not it did), did not die
    (a dead thread leaves an empty holder that satisfies any subset check), and
    surfaced its exception where the scenario can see it. `join` states all
    three, so a scenario never infers them from a short outcome list.
    """

    def __init__(self, name: str, target: Callable[[], None]) -> None:
        self.name = name
        self._target = target
        self._failure: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    def _run(self) -> None:
        try:
            self._target()
        except BaseException as error:  # noqa: BLE001 - re-raised on the scenario thread by join
            self._failure = error

    def start(self) -> BackgroundWork:
        self._thread.start()
        return self

    @property
    def running(self) -> bool:
        return self._thread.is_alive()

    def join(self, *, timeout: float) -> None:
        """Wait up to `timeout` seconds; fail if the work is still running, re-raise if it died."""

        self._thread.join(timeout)
        if self._thread.is_alive():
            raise InvariantViolation(
                f"{self.name} was still running {timeout:g} s after its join deadline"
            )
        if self._failure is not None:
            raise self._failure


def background(name: str, target: Callable[[], None]) -> BackgroundWork:
    """Start `target` on a named thread whose `join` is a verdict (see [`BackgroundWork`])."""

    return BackgroundWork(name, target).start()


T = TypeVar("T")


class CallsCancelled(BaseException):
    """The `run_concurrently` group this call runs in was interrupted.

    A `BaseException`, so a call's own `except Exception` retry or cleanup
    does not swallow it: the call ends at its next request or wait instead.
    """


class _CallGroup:
    """The calls of one `run_concurrently`, and the subprocesses they start.

    `subprocess.run` enforces its timeout from the thread that called it; a
    daemon worker left running when the process exits takes that enforcement
    with it, and its child outlives the deadline. `end` cancels the group,
    which every `cancellation_point` and `pause` in its calls then raises,
    kills and reaps every child still running, and refuses new ones.
    `detach` only kills, reaps and refuses: its calls run on, uncancelled.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._running: set[subprocess.Popen[str]] = set()
        self._detached = False
        self.cancelled = threading.Event()

    def run(
        self,
        command: list[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None,
        input_text: str | None,
        timeout: float | None,
    ) -> subprocess.CompletedProcess[str]:
        """`subprocess.run(..., text=True, capture_output=True)`, owned by this group."""

        with self._lock:
            if self.cancelled.is_set():
                raise CallsCancelled(f"not started after the concurrent calls ended: {command[0]}")
            if self._detached:
                raise RuntimeError(f"not started after the concurrent calls ended: {command[0]}")
            process = subprocess.Popen(
                command,
                cwd=cwd,
                env=env,
                text=True,
                stdin=subprocess.PIPE if input_text is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._running.add(process)
        # As subprocess.run does: on a timeout the exception already holds the
        # output so far, so kill and wait rather than read on, which a
        # grandchild still holding the pipes would hold past the timeout.
        try:
            with process:
                try:
                    stdout, stderr = process.communicate(input_text, timeout=timeout)
                except BaseException:
                    process.kill()
                    raise
        finally:
            with self._lock:
                self._running.discard(process)
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)

    def end(self) -> None:
        with self._lock:
            self.cancelled.set()
            running = list(self._running)
        self._kill(running)

    def detach(self) -> None:
        with self._lock:
            self._detached = True
            running = list(self._running)
        self._kill(running)

    @staticmethod
    def _kill(running: Sequence[subprocess.Popen[str]]) -> None:
        for process in running:
            process.kill()
        for process in running:
            process.wait()


_CALL_GROUP: contextvars.ContextVar[_CallGroup | None] = contextvars.ContextVar(
    "space_smoke_call_group", default=None
)
# Where `ScenarioContext` writes evidence on this thread: the scenario's own
# while its body runs, one call's own on a `run_concurrently` worker.
_EVIDENCE_SINK: contextvars.ContextVar[dict[str, object] | None] = contextvars.ContextVar(
    "space_smoke_evidence_sink", default=None
)

# How long an interrupted `run_concurrently` waits for its calls to end
# before cleanup starts. A cancelled call ends at its next request or wait,
# and most already in a request end within its timeout (Native's is 15 s,
# `NativeConfig.request_timeout_seconds`, per socket operation). One that
# takes longer, such as a response still arriving, is not waited for.
CANCEL_JOIN_SECONDS = 20.0


def cancellation_point() -> None:
    """Raise `CallsCancelled` if this thread's `run_concurrently` group was
    interrupted; nothing outside one. Call it before a request that creates
    or changes something."""

    group = _CALL_GROUP.get()
    if group is not None and group.cancelled.is_set():
        raise CallsCancelled("the concurrent calls were interrupted")


def pause(seconds: float) -> None:
    """`time.sleep`, which a call in an interrupted `run_concurrently` group
    leaves at once, raising `CallsCancelled`."""

    group = _CALL_GROUP.get()
    if group is None:
        time.sleep(seconds)
    elif group.cancelled.wait(seconds):
        raise CallsCancelled("the concurrent calls were interrupted")


def primary_failure(outcomes: Sequence[object]) -> BaseException | None:
    """The failure among `run_concurrently`'s outcomes that stands for them
    all: the first verdict (`InvariantViolation`), else the first error."""

    failures = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
    return next(
        (failure for failure in failures if isinstance(failure, InvariantViolation)),
        failures[0] if failures else None,
    )


def _merge_evidence(
    parent: dict[str, object],
    evidence: Sequence[dict[str, object]],
    outcomes: Sequence[object],
) -> None:
    """Each call's evidence into its caller's: the calls that returned, in
    order, then the failures, `primary_failure` last, so the failure a caller
    raises keeps the evidence it recorded where a sibling recorded the same
    name. Each failure's own evidence is also kept whole, by call, under
    `concurrent_failures`."""

    failed = [index for index, outcome in enumerate(outcomes) if isinstance(outcome, BaseException)]
    primary = primary_failure(outcomes)
    order = [index for index in range(len(outcomes)) if index not in failed]
    order += sorted(failed, key=lambda index: outcomes[index] is primary)
    for index in order:
        parent.update(dict(evidence[index]))
    if failed:
        listed = parent.get("concurrent_failures")
        earlier = listed if isinstance(listed, list) else []
        parent["concurrent_failures"] = [
            *earlier,
            *(
                {
                    "call": index,
                    "error": f"{type(outcomes[index]).__name__}: {outcomes[index]}"[:500],
                    "evidence": dict(evidence[index]),
                }
                for index in failed
            ),
        ]


def run_concurrently(
    calls: Sequence[Callable[[], T]],
    *,
    on_interrupt: Literal["cancel", "detach"] = "cancel",
) -> list[T | BaseException]:
    """Run each call on its own thread; each one's result, or what it raised, in order.

    For independent waits that would otherwise run one after another. Inside
    a scenario, each call records its evidence apart and it joins the
    scenario's once the calls end (`_merge_evidence`), so a sibling cannot
    overwrite what a failing call recorded.

    An exception that interrupts the wait, such as a scenario deadline or a
    SIGTERM, cancels the calls: every subprocess they started through
    `SubprocessExecutor` is killed and reaped, each call ends at its next
    `cancellation_point` or `pause`, and the wait for that is bounded
    (`CANCEL_JOIN_SECONDS`) before the exception is raised and cleanup
    starts. A call still running after the bound, such as one whose
    response is still arriving, is named on stderr and not waited for. A
    call that creates something owns its cleanup itself, registered before
    the call starts: Native creates do (`space_smoke.services`), and their
    cleanup waits for the create call to return, within cleanup's deadline.

    `on_interrupt="detach"` neither cancels the calls nor waits for them:
    once the subprocesses they started are killed and reaped (and new ones
    refused), the exception is raised at once, and the calls run on, their
    waits and requests unaffected. It is for a caller that settles calls
    still running itself. What a detached call records after the raise is
    not kept.
    """

    if on_interrupt not in ("cancel", "detach"):
        raise ValueError(f"on_interrupt must be 'cancel' or 'detach', not {on_interrupt!r}")

    group = _CallGroup()
    parent = _EVIDENCE_SINK.get()
    evidence: list[dict[str, object]] = [{} for _ in calls]
    outcomes: list[T | BaseException] = [
        RuntimeError("the call did not finish") for _ in calls
    ]
    # Set as each call ends. An interrupted `Thread.join` can leave CPython
    # reading a worker still in native I/O as stopped, so that a second join
    # returns at once and `is_alive` misses it: the bounded wait reads these
    # instead.
    finished = [threading.Event() for _ in calls]

    def run(index: int, call: Callable[[], T]) -> None:
        try:
            _CALL_GROUP.set(group)
            if parent is not None:
                _EVIDENCE_SINK.set(evidence[index])
            outcomes[index] = call()
        except BaseException as error:  # noqa: BLE001 - returned to the caller in order
            outcomes[index] = error
        finally:
            finished[index].set()

    threads = [
        threading.Thread(target=run, args=(index, call), daemon=True)
        for index, call in enumerate(calls)
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    except BaseException:
        if on_interrupt == "detach":
            group.detach()
            if parent is not None:
                _merge_evidence(parent, evidence, outcomes)
            raise
        group.end()
        deadline = time.monotonic() + CANCEL_JOIN_SECONDS
        started = [index for index, thread in enumerate(threads) if thread.ident is not None]
        for index in started:
            finished[index].wait(max(0.0, deadline - time.monotonic()))
        still = [index for index in started if not finished[index].is_set()]
        if still:
            print(
                f"NOTE: {len(still)} interrupted concurrent call(s) still running "
                f"{CANCEL_JOIN_SECONDS:g} s after the interrupt: calls {still}",
                file=sys.stderr,
                flush=True,
            )
        if parent is not None:
            _merge_evidence(parent, evidence, outcomes)
        raise
    if parent is not None:
        _merge_evidence(parent, evidence, outcomes)
    return outcomes


class ResourceScope:
    """Run cleanup in reverse registration order and report every failure.

    A compensation can read how long its step has left, until the cleanup's
    deadline or its own cap (`cleanup_seconds_left`), to bound a wait of its
    own inside it.
    """

    def __init__(self) -> None:
        self._compensations: list[Compensation] = []

    def defer(
        self,
        name: str,
        callback: Callable[[], None],
        *,
        critical: bool = False,
        timeout_seconds: float | None = None,
    ) -> Compensation:
        compensation = Compensation(
            name, callback, critical=critical, timeout_seconds=timeout_seconds
        )
        self._compensations.append(compensation)
        return compensation

    def redefer(self, *compensations: Compensation) -> None:
        """Move armed compensations to the top of LIFO teardown in one atomic update."""

        for compensation in compensations:
            if compensation not in self._compensations:
                raise ValueError(
                    f"compensation {compensation.name!r} is not registered in this scope"
                )
        promoted = [comp for comp in compensations if comp.armed]
        if not promoted:
            return
        promoted_ids = {id(comp) for comp in promoted}
        self._compensations = [
            comp for comp in self._compensations if id(comp) not in promoted_ids
        ] + promoted

    def close(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> tuple[CleanupFailure, ...]:
        failures: list[CleanupFailure] = []
        seen: set[int] = set()
        deadline = (
            time.monotonic() + timeout_seconds if timeout_seconds is not None else None
        )
        for compensation in reversed(self._compensations):
            if id(compensation) in seen:
                continue
            seen.add(id(compensation))
            if not compensation.armed:
                continue
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                failures.append(
                    CleanupFailure(
                        compensation.name,
                        "cleanup deadline exhausted before compensation ran",
                        compensation.critical,
                    )
                )
                continue
            step_timeout = _compensation_deadline_seconds(remaining, compensation.timeout_seconds)
            token = _CLEANUP_DEADLINE.set(
                None if step_timeout is None else time.monotonic() + step_timeout
            )
            try:
                with _scenario_deadline(step_timeout, label="cleanup"):
                    compensation.release()
            except Exception as error:  # noqa: BLE001 - cleanup must continue
                failures.append(
                    CleanupFailure(
                        compensation.name,
                        f"{type(error).__name__}: {error}",
                        compensation.critical,
                    )
                )
            finally:
                _CLEANUP_DEADLINE.reset(token)
        return tuple(failures)


# The deadline (monotonic) of the compensation `ResourceScope.close` is
# running: the step's own, the cleanup's or a shorter cap of its own, the one
# its alarm enforces; None if the step has none.
_CLEANUP_DEADLINE: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "space_smoke_cleanup_deadline", default=None
)


def cleanup_seconds_left() -> float | None:
    """How long the compensation now running in cleanup has left: until the
    cleanup's deadline, or its own cap (`timeout_seconds`) if that ends
    sooner. None outside cleanup, or for a step without a deadline."""

    deadline = _CLEANUP_DEADLINE.get()
    return None if deadline is None else max(0.0, deadline - time.monotonic())


def _compensation_deadline_seconds(
    remaining: float | None, step_cap: float | None
) -> float | None:
    if remaining is not None:
        return remaining if step_cap is None else min(remaining, step_cap)
    if step_cap is None:
        return None
    if threading.current_thread() is not threading.main_thread():
        return None
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        return None
    return step_cap


UNENCODABLE_EVIDENCE = "unencodable_evidence"
"""The evidence key naming the values a failing verdict kept as `repr`."""


def _json_error(value: object) -> Exception | None:
    """Why the runner can't print `value`, encoding as it does, if it can't."""

    try:
        json.dumps(value, sort_keys=True)
    except (TypeError, ValueError) as error:
        return error
    return None


def require_json_evidence(evidence: Mapping[str, object]) -> None:
    """Refuse evidence the runner can't print. It writes each verdict's
    evidence as JSON once the scenario ends, so a value it can't encode ends
    even a passed run with a traceback; refused here, it fails the call that
    recorded it, by name."""

    for name, value in evidence.items():
        error = _json_error(value)
        if error is not None:
            raise TypeError(
                f"evidence {name!r} is not JSON-serializable ({type(value).__name__}): {error}"
            ) from error


def printable_evidence(
    evidence: Mapping[str, object], already: Mapping[str, object]
) -> dict[str, object]:
    """`evidence` as a failing verdict keeps it: each value the runner can't
    print becomes its `repr`, and `UNENCODABLE_EVIDENCE` lists those names
    beside the ones `already` lists, so the verdict and its message stand."""

    printable: dict[str, object] = {}
    unencodable: list[str] = []
    for name, value in evidence.items():
        if _json_error(value) is None:
            printable[name] = value
        else:
            printable[name] = repr(value)
            unencodable.append(name)
    if unencodable:
        listed = already.get(UNENCODABLE_EVIDENCE)
        earlier = [str(name) for name in listed] if isinstance(listed, list) else []
        printable[UNENCODABLE_EVIDENCE] = sorted({*earlier, *unencodable})
    return printable


@dataclass
class ScenarioContext:
    scope: ResourceScope
    evidence: dict[str, object]

    def _sink(self) -> dict[str, object]:
        """Where this thread's evidence goes: its `run_concurrently` call's
        own, else the scenario's."""

        sink = _EVIDENCE_SINK.get()
        return self.evidence if sink is None else sink

    def record(self, name: str, value: object) -> None:
        require_json_evidence({name: value})
        self._sink()[name] = value

    def verify(
        self,
        condition: bool,
        message: str,
        **evidence: object,
    ) -> None:
        """Keep `evidence` and raise `message` unless `condition` holds. A
        held check refuses evidence the runner can't print, naming it; a
        failing one keeps such a value as its `repr` instead, so the failure
        and its message are what the run reports."""

        sink = self._sink()
        if condition:
            require_json_evidence(evidence)
            sink.update(evidence)
            return
        sink.update(printable_evidence(evidence, sink))
        raise InvariantViolation(message)

    def warn(self, message: str, **evidence: object) -> None:
        """Keep `evidence`, each value the runner can't print as its `repr`,
        and end the run with a warning saying `message`."""

        sink = self._sink()
        sink.update(printable_evidence(evidence, sink))
        raise QuarantinedSmoke(message)

    def wait(self, seconds: float) -> None:
        if seconds <= 0:
            raise ValueError("wait duration must be positive")
        pause(seconds)

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        print(f"--- {name}", flush=True)
        try:
            yield
        finally:
            self.evidence.setdefault("phases", [])
            phases = self.evidence["phases"]
            assert isinstance(phases, list)
            phases.append(
                {"name": name, "duration_seconds": time.monotonic() - started}
            )


Body = Callable[[ScenarioContext], None]


@contextmanager
def _scenario_deadline(
    timeout_seconds: float | None,
    *,
    label: str,
) -> Iterator[None]:
    if timeout_seconds is None:
        yield
        return
    if timeout_seconds <= 0:
        raise ValueError("scenario timeout must be positive")
    if threading.current_thread() is not threading.main_thread():
        raise SmokeConfigurationError(
            "scenario timeout enforcement requires the main thread"
        )
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise SmokeConfigurationError(
            "scenario timeout enforcement cannot replace an existing alarm"
        )

    previous_handler = signal.getsignal(signal.SIGALRM)

    def timeout(_signal_number: int, _frame: FrameType | None) -> None:
        raise ScenarioTimedOut(f"{label} timed out after {timeout_seconds:g}s")

    signal.signal(signal.SIGALRM, timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


@contextmanager
def _evidence_sink(evidence: dict[str, object]) -> Iterator[None]:
    """Make `evidence` this thread's evidence sink while the body runs, so
    `run_concurrently` can give each call its own and merge them back."""

    token = _EVIDENCE_SINK.set(evidence)
    try:
        yield
    finally:
        _EVIDENCE_SINK.reset(token)


@contextmanager
def _sigterm_raises() -> Iterator[None]:
    """Turn SIGTERM into `SystemExit` on the main thread while the body runs.

    Python's default SIGTERM action ends the process without unwinding, so a
    CI cancel or `timeout` would skip cleanup. Cleanup itself keeps the
    default action, so a second SIGTERM still ends a cleanup that hangs.
    """

    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def terminate(signal_number: int, _frame: FrameType | None) -> None:
        raise SystemExit(128 + signal_number)

    previous_handler = signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous_handler)


def execute_scenario(
    *,
    title: str,
    body: Body,
    timeout_seconds: float | None = None,
    cleanup_timeout_seconds: float | None = None,
) -> ScenarioResult:
    scope = ResourceScope()
    context = ScenarioContext(scope, {})
    if cleanup_timeout_seconds is not None and (
        cleanup_timeout_seconds <= 0
        or (timeout_seconds is not None and cleanup_timeout_seconds >= timeout_seconds)
    ):
        raise ValueError("cleanup reserve must be positive and smaller than the scenario timeout")
    cleanup_timeout = cleanup_timeout_seconds if cleanup_timeout_seconds is not None else (
        min(120.0, timeout_seconds * 0.1) if timeout_seconds is not None else None
    )
    execution_timeout = (
        timeout_seconds - cleanup_timeout
        if timeout_seconds is not None and cleanup_timeout is not None
        else None
    )
    try:
        with (
            _sigterm_raises(),
            _scenario_deadline(execution_timeout, label="scenario execution"),
            _evidence_sink(context.evidence),
        ):
            body(context)
        result = ScenarioResult(Verdict.PASS, f"{title} passed", dict(context.evidence))
    except InvariantViolation as error:
        result = ScenarioResult(Verdict.FAIL, str(error), dict(context.evidence))
    except QuarantinedSmoke as error:
        result = ScenarioResult(Verdict.WARN, str(error), dict(context.evidence))
    except ScenarioTimedOut as error:
        result = ScenarioResult(Verdict.INFRA_ERROR, str(error), dict(context.evidence))
    except (InconclusiveSmoke, SmokeConfigurationError) as error:
        result = ScenarioResult(
            Verdict.INCONCLUSIVE, str(error), dict(context.evidence)
        )
    except Exception as error:  # noqa: BLE001 - scenario boundary classifies infra
        result = ScenarioResult(
            Verdict.INFRA_ERROR,
            f"{type(error).__name__}: {error}",
            dict(context.evidence),
            diagnostics=(traceback.format_exc(),),
        )
    except BaseException:
        # An interrupt still restores what the body changed, then ends the
        # run without a verdict.
        for failure in scope.close(timeout_seconds=cleanup_timeout):
            print(f"CLEANUP FAILED: {failure.name}: {failure.detail}", file=sys.stderr, flush=True)
        raise

    cleanup_failures = scope.close(timeout_seconds=cleanup_timeout)
    if not cleanup_failures:
        return result

    details = tuple(f"{failure.name}: {failure.detail}" for failure in cleanup_failures)
    if any(failure.critical for failure in cleanup_failures) and result.verdict in (
        Verdict.PASS,
        Verdict.WARN,
        Verdict.INCONCLUSIVE,
    ):
        return ScenarioResult(
            Verdict.INFRA_ERROR,
            f"{result.summary}; critical cleanup failed",
            result.evidence,
            details,
            result.diagnostics,
        )
    return ScenarioResult(
        result.verdict,
        result.summary,
        result.evidence,
        details,
        result.diagnostics,
    )
