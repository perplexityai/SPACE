"""An encrypted restore can be suspended again without losing its memory base."""

import hashlib
import json
import os
from importlib.resources import files

import space_smoke as smoke
from space_smoke.runtime import SmokeConfigurationError

PROBE = "/tmp/space-smoke-snapshot-state.py"
WORKING_SET_ENV = "SPACE_SMOKE_SNAPSHOT_MEMORY_MIB"


def expected_digest(kind: str, generation: int, mib: int) -> str:
    digest = hashlib.sha256()
    for index in range(mib):
        digest.update(hashlib.shake_256(f"{kind}:{generation}:{index}".encode()).digest(1024**2))
    return digest.hexdigest()


# The first sealed volume is the base Firecracker loads. Reformatting on the
# second suspend makes a Diff snapshot without its base, breaking userspace.
@smoke.scenario(
    "local.encrypted_resuspend_preserves_cow",
    sandbox=smoke.SandboxConfig(timeout=900, template="seed", kind_byok=True),
    rerun_environment=(WORKING_SET_ENV,),
)
def encrypted_resuspend_preserves_cow(test: smoke.KindEcw) -> None:
    """A BYOK sandbox restores its live process and full RAM/disk contents
    from object storage, mutates them, and re-suspends into its sparse base."""

    encrypted = test.encrypted_resuspend
    restore = test.cross_node_restore
    try:
        memory_mib = int(os.environ.get(WORKING_SET_ENV, "32"))
    except ValueError as error:
        raise SmokeConfigurationError(f"{WORKING_SET_ENV} must be an integer") from error
    if not 1 <= memory_mib <= 8192:
        raise SmokeConfigurationError(f"{WORKING_SET_ENV} must be between 1 and 8192")
    # Two incompressible generations plus metadata must still remain sparse.
    allocation_bound = max(encrypted.MAX_DIFF_ALLOCATION_BYTES, memory_mib * 3 * 1024**2)
    # Full-payload checks may fault and rewrite every restored RAM page.
    # This bound is independent of the platform's data-plane readiness budget.
    payload_timeout = max(20, (memory_mib + 31) // 32)
    test.record("snapshot_guest_request_timeout_seconds", payload_timeout)
    identity: tuple[object, ...] | None = None

    def observe(generation: int, *, mutate: bool = False) -> None:
        nonlocal identity
        result = test.run_command_when_ready(
            f"python3 {PROBE} {'mutate' if mutate else 'read'} {payload_timeout}",
            timeout=max(60, payload_timeout + 5),
        )
        test.verify(result.exit_code == 0, "live snapshot-state process did not answer",
                    command_stderr=result.stderr)
        observed = json.loads(result.stdout)
        current_identity = tuple(observed[key] for key in ("pid", "nonce", "boot_id"))
        if identity is None:
            identity = current_identity
        test.verify(
            current_identity == identity
            and observed["generation"] == generation
            and observed["memory_mib"] == memory_mib
            and observed["memory_sha256"] == expected_digest("ram", generation, memory_mib)
            and observed["disk_sha256"] == expected_digest("disk", generation, 8),
            "restore changed the process identity or full RAM/disk contents",
            snapshot_guest_state=observed,
        )
        test.record(f"snapshot_guest_generation_{generation}", observed)

    with test.phase("start a process holding independently checked RAM and disk bytes"):
        source = files("space_smoke.guest").joinpath("snapshot_state.py").read_text()
        test.sandbox.files.write(PROBE, source)
        process = test.sandbox.start_background(
            f"python3 {PROBE} serve {memory_mib}", timeout=900,
        )
        process.disconnect()
        # Sandbox teardown owns the process even if suspend or a read fails.
        observe(0)

    with test.phase("seal the first encrypted snapshot volume"):
        source_placement = restore.current_placement()
        test.pause()
        first = encrypted.sealed_snapshot(
            source_placement.node_name, max_allocation_bytes=allocation_bound,
        )
        restore.wait_until_restore_portable()

    with restore.cordoned(source_placement.node_name):
        with test.phase("restore published encrypted bytes onto another worker"):
            test.reconnect_by_id()
            restore.expect_restored_from_object_storage(
                source=source_placement, landed=restore.current_placement(),
            )
            observe(0)
            encrypted.expect_prior_executions_retired()

    with test.phase("re-suspend into the existing sparse encrypted base"):
        observe(1, mutate=True)
        second_node = encrypted.current_node()
        test.pause()
        second = encrypted.sealed_snapshot(second_node, max_allocation_bytes=allocation_bound)
        encrypted.expect_base_reused(first, second)

    with test.phase("restore the re-suspended guest through the vsock bridge"):
        test.reconnect_by_id()
        observe(1)
        encrypted.expect_prior_executions_retired()
