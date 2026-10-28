"""Encrypted live forks preserve the source VM lifecycle."""

import space_smoke as smoke

from .local_encrypted_disk_fork import verify_encrypted_disk_fork


@smoke.scenario(
    "local.encrypted_live_disk_fork",
    sandbox=smoke.SandboxConfig(timeout=900, template="seed", kind_byok=True),
)
def encrypted_live_disk_fork(test: smoke.KindEcw) -> None:
    """Capture encrypted disk state without suspending or restarting its guest."""
    verify_encrypted_disk_fork(test, suspended=False)
