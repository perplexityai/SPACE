# Node-manager execution authority

Btrfs restore receive has a separate lifecycle admission lane from general
filesystem work, including suspend capture/send. Defaults allow four general
filesystem operations plus four receives concurrently, eight operations in
total. Receive keeps its previous uncontended capacity while gaining independent
admission. Tune receive admission
with `SPACE_NODEMGR_LIMIT_RECEIVE_INSTANCE=<permits>:<max_queue>` (default
`4:16`); existing `SPACE_NODEMGR_LIMIT_FILESYSTEM` overrides no longer limit
receives. The two lanes do not enforce a shared disk-throughput ceiling.
Both lanes retain the same sandbox artifact lock, mutation claim and
blocking-work ownership, so operations on one sandbox remain serialized.

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
