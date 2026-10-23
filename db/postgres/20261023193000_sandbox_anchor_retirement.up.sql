SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
GRANT SELECT (lifecycle) ON sandboxes TO space_credmgr;
