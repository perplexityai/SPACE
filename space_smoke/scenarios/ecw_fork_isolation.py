"""Concurrent disk forks preserve bytes without sharing writable state."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import space_smoke as smoke
from space_smoke.capabilities import EcwSandbox

PATH = "/tmp/space-smoke-fork.bin"
CONTENT = bytes(range(256)) * 16


# Forks are Kubernetes-only: the Direct substrate answers them Unsupported
# (crates/libs/api/src/controllers/substrate/direct.rs).
@smoke.scenario(
    "ecw.fork_isolation", sandbox=smoke.SandboxConfig(timeout=600), substrates=("kubernetes",)
)
def fork_isolation(test: smoke.Ecw) -> None:
    """Different keys produce independent children while the source stays usable."""

    with test.phase("write the source disk state"):
        test.sandbox.files.write(PATH, CONTENT)

    with test.phase("fork twice concurrently with distinct keys"):
        gate = Barrier(2, timeout=10)

        def fork(_index: int) -> EcwSandbox:
            gate.wait()
            return test.sandboxes.fork(test.sandbox).sandbox

        with ThreadPoolExecutor(max_workers=2) as executor:
            left, right = executor.map(fork, range(2))
        test.verify(
            len({test.sandbox.sandbox_id, left.sandbox_id, right.sandbox_id}) == 3,
            "different-key forks did not create distinct child identities",
        )
        for child in (left, right):
            info = child.get_info()
            test.verify(
                info.sandbox_id == child.sandbox_id and info.state == smoke.SandboxState.RUNNING,
                "fork did not return an accessible running child",
            )
            test.verify(child.files.read_bytes(PATH) == CONTENT, "fork changed disk contents")

    with test.phase("child writes do not change the source or sibling"):
        left.files.write(PATH, b"child-only")
        test.verify(left.files.read_bytes(PATH) == b"child-only", "child write was lost")
        test.verify(test.sandbox.files.read_bytes(PATH) == CONTENT, "child write changed source")
        test.verify(right.files.read_bytes(PATH) == CONTENT, "child write changed sibling")

    with test.phase("source writes do not change either child"):
        test.sandbox.files.write(PATH, b"source-only")
        test.verify(test.sandbox.files.read_bytes(PATH) == b"source-only", "source write was lost")
        test.verify(left.files.read_bytes(PATH) == b"child-only", "source write changed child")
        test.verify(right.files.read_bytes(PATH) == CONTENT, "source write changed sibling")
