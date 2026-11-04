# Node-manager execution authority

Lifecycle mutations require the sandbox owner's persisted execution identity.
Node-manager, runtime-supervisor and sandbox-manager retain that authority across
process restarts; lease absence alone does not authorize cleanup of a live VM.
An explicit owner cleanup token can authorize an unbound, quiescent instance.
Missing or stale authority is not repaired by retrying with a newly observed token.

## Startup authority gate

Node-manager and sandbox-manager refuse startup with persisted state that has
no execution authority. Diagnostics report the missing authority count up to 64
("at least 64" when saturated) and at most eight sandbox IDs. Do not erase
stores, reset slots or manufacture identities to bypass the gate.

Node-manager's authority preflight waits for startup recovery to release each
sandbox, then rechecks its current lease/runtime evidence and persisted authority.
Contention does not restart the daemon or bypass the gate. Shutdown cancels the
ownership wait; a stuck recovery operation keeps admission unready. Qualify
restarts with dirty/resetting slots as well as clean startup.

## Snapshot receive

Snapshot restore validates a private anonymous reflink before invoking `btrfs
receive`. Upload and restore accept protocol v1 commands only, check command
CRCs, and require END followed by EOF. Validation has a separate bounded
120-second allowance; scrub and receive share a separate 120-second host
mutation budget. A framing or protocol rejection leaves the old instance
untouched; the private input disappears when its last descriptor closes.

This is a framing and decompression guard, not artifact authentication or a
validator for paths, parents and clone sources. Continue treating stored streams
as untrusted. The worker image supplies btrfs-progs 7.3. Direct-node installations
must supply the binary themselves;
verify the executable resolved in node-manager's runtime environment, its
version and hash, and the deployed image digest when rolling out or rolling back.

The source build in `docker/build-btrfs-progs.sh` is a temporary packaging
workaround for the inline-LZO receive fix (upstream `e69e94b`, included in 7.3).
Retire it when the worker and CI distributions offer supported packages with
that fix, either a fixed upstream release or a verified backport. Check package
source/changelog evidence and run the receive regression tests with the actual
packaged binary. Then remove the `space-btrfs-progs` build stages and binary
copies from both Dockerfiles, this build script, and its entry in
`ci/scripts/ci-image.sh`'s cache inputs; retain the existing package installs.
Confirm runtime PATH resolves the packaged binary and record its version/hash
and image identity. A direct-node package must meet the same fix requirement.
