-- Retire user_credentials. Nothing reads or writes it now that the user
-- credential APIs are gone. The credential-fence guard requires this drop to
-- be the migration's only statement, so it runs under the migrator's session
-- timeouts instead of in-file bounds.
-- squawk-ignore ban-drop-table, require-statement-timeout, require-lock-timeout
DROP TABLE user_credentials;
