# SQLite baseline cutover

Deploy the preparation release and complete its service and shared migrations
first. Each daemon's normal startup, or its migrator `up` command, then performs
the cutover automatically. No per-file SQL or replacement step is needed.

Startup checks the complete historical version set, success flags, accepted
checksums, and exact destination baseline. In one transaction it archives the
original rows in `_space_baseline_archive` and replaces the applicable history
with the public baseline. Application schema and rows are unchanged. Repeated
startup uses normal SQLx validation; fresh databases migrate normally.
Unknown/incomplete histories, integrity or FK violations, and archive collisions
stop startup without rewriting history. Sandbox-manager also requires staged
metadata for active snapshot uploads. Databases are never recreated on rejection.

Service and shared histories may convert together. Shared history stays unchanged
while its historical catalog is bundled, and converts when the shared baseline
arrives later. Already public service histories are preserved. A public shared
catalog cannot accompany a historical service catalog.

Schema comparison remains a rollout prerequisite: verify the prepared databases
against the baseline and retain recoverable backups before deployment. History
checks do not detect manual schema drift. Use normal canary and bounded rollout
controls. Returning offline nodes perform the same cutover when they start.

Prevent old-image and automatic rollback after adoption. Use a compatible forward
fix; recovery to old binaries requires a reviewed procedure with writers stopped.
Keep the cutover bridge until all retained databases have converted or retired.
The checksum adapter remains useful independently.

Do not deploy post-baseline service migrations until the final runtime cleanup
establishes rollback ordering across service and shared catalogs. The current
CLI attempts shared rollback first, so its baseline floor can block rollback
of a later service migration.
