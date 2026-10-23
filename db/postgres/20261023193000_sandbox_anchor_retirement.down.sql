SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
REVOKE SELECT (lifecycle) ON sandboxes FROM space_credmgr;
