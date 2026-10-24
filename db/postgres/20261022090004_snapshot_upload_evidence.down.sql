SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE snapshot_artifact_digests DROP COLUMN upload_evidence;
