-- Drop `sandbox_encrypted_fields`: nothing reads or writes it. An earlier
-- migration dropped its foreign keys, so this locks only the table itself. It
-- sets no timeouts because it stays a single statement, the form the migration
-- guard accepts beside a CREDENTIAL_TABLES removal, used for all five drops
-- alike. Only a maintenance session could hold a lock on the table, and waiting
-- behind one queues nothing live; the session's own bounds apply.
-- squawk-ignore ban-drop-table, require-lock-timeout, require-statement-timeout
DROP TABLE sandbox_encrypted_fields;
