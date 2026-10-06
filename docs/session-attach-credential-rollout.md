# Session attach credential retention

`data_plane_session_attach_credentials` retains hashes and original expiry times
for attachment credentials issued before an idempotent session replay. It is
keyed by session and hash; deleting a session cascades to its retained credentials.
The existing session row remains the authority for scope, tenant, and revocation.
No plaintext attachment token is persisted.

Apply the additive migration and rerun `space-db-migrator bootstrap-roles` before
deploying readers or writers. The read-only role cannot read the new token-hash
column. Existing binaries continue using the current credential on the session
row and do not need the new table.

Deploy internal API readers that recognize retained credentials before API
writers that retain them. The overlap guarantee is available after both fleets
have upgraded; old readers and writers retain the previous replay behavior.
Writers prune expired credentials on replay and bound the retained set per
session, rejecting excess issuance without revoking an unexpired credential.
Session revocation removes the retained credentials in the same transaction.

To roll back, restore the old application binaries before dropping the table.
Dropping it invalidates retained credentials and restores the previous behavior,
where only the latest attachment credential can be used.
