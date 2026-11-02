"""Concurrent and completed fork retries retain one child identity."""

from concurrent.futures import ThreadPoolExecutor
import secrets
from threading import Barrier

import space_smoke as smoke
from space_smoke.capabilities import EcwForkResult

PATH = "/tmp/space-smoke-fork-replay.txt"


# Forks are Kubernetes-only: the Direct substrate answers them Unsupported
# (crates/libs/api/src/controllers/substrate/direct.rs).
@smoke.scenario(
    "ecw.fork_idempotency", sandbox=smoke.SandboxConfig(timeout=600), substrates=("kubernetes",)
)
def fork_idempotency(test: smoke.Ecw) -> None:
    """A replay returns the existing child, including its post-fork writes."""

    key = f"space-smoke-fork-{secrets.token_hex(16)}"
    with test.phase("write state and race the same idempotency key"):
        test.sandbox.files.write(PATH, "source")
        gate = Barrier(2, timeout=10)

        def fork(_index: int) -> EcwForkResult:
            gate.wait()
            return test.sandboxes.fork(test.sandbox, idempotency_key=key)

        with ThreadPoolExecutor(max_workers=2) as executor:
            first, concurrent = executor.map(fork, range(2))
        test.verify(
            first.response == concurrent.response,
            "same-key concurrent forks returned different public results",
        )
        test.verify(first.sandbox.files.read(PATH) == "source", "fork lost the source disk state")
        test.verify(concurrent.sandbox.files.read(PATH) == "source", "concurrent fork is inaccessible")

    with test.phase("replay after child and source have both changed"):
        first.sandbox.files.write(PATH, "child-after-fork")
        test.sandbox.files.write(PATH, "source-after-fork")
        replay = test.sandboxes.fork(test.sandbox, idempotency_key=key)
        test.verify(replay.response == first.response, "completed replay changed the public result")
        test.verify(
            replay.sandbox.files.read(PATH) == "child-after-fork",
            "completed replay recaptured the source or reset the existing child",
        )
