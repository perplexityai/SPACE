"""ECW metadata can request a larger guest root disk at creation."""

import space_smoke as smoke
from space_smoke.capabilities import EcwSandbox

MIB = 1024 * 1024


@smoke.scenario("ecw.disk_sizing")
def disk_sizing(test: smoke.Ecw) -> None:
    """Both the block device and usable filesystem grow through the stock SDK."""

    def sizes(sandbox: EcwSandbox) -> tuple[int, int]:
        result = sandbox.run(
            'blockdev --getsize64 "$(findmnt -n -o SOURCE /)" && stat -f -c "%S %b" /',
            user="root",
        )
        test.verify(
            result.exit_code == 0,
            "could not measure the guest root disk",
            stderr=result.stderr,
        )
        disk_bytes, block_bytes, blocks = map(int, result.stdout.split())
        return disk_bytes, block_bytes * blocks

    with test.phase("measure the default disk"):
        default_disk, default_filesystem = sizes(test.sandbox)
        requested_mib = (default_disk + MIB - 1) // MIB + 1024

    with test.phase("create with a larger disk through ECW metadata"):
        sandbox = test.sandboxes.create(metadata={"resources.disk_mib": str(requested_mib)})
        disk_bytes, filesystem_bytes = sizes(sandbox)
        test.verify(
            disk_bytes == requested_mib * MIB
            and default_filesystem < filesystem_bytes <= disk_bytes,
            "ECW did not expand the guest root disk and filesystem",
            requested_mib=requested_mib,
            disk_bytes=disk_bytes,
            filesystem_bytes=filesystem_bytes,
            default_filesystem_bytes=default_filesystem,
        )
