SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Recovery authority is separate from historical ownership and survives attempt
-- retention as an audit record. It is never inferred from worker inventory.
CREATE TABLE backup_recovery_grants (
    id UUID PRIMARY KEY,
    sandbox_id UUID NOT NULL,
    local_artifact_id UUID NOT NULL,
    execution_generation BIGINT NOT NULL CHECK (execution_generation > 0),
    uploader_node_id UUID NOT NULL,
    uploader_node_uid TEXT NOT NULL CHECK (length(uploader_node_uid) > 0),
    captured_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    approved_by UUID NOT NULL,
    approval_reason TEXT NOT NULL,
    capture_evidence JSONB NOT NULL,
    manifest JSONB NOT NULL,
    expected_current_artifact_id UUID,
    active_attempt_id UUID,
    consumed_attempt_id UUID,
    revoked_at TIMESTAMPTZ,
    revoked_by UUID,
    revoke_reason TEXT,
    CHECK (captured_at <= approved_at AND expires_at > approved_at),
    CHECK ((revoked_at IS NULL) = (revoked_by IS NULL))
);
CREATE INDEX backup_recovery_grants_artifact ON backup_recovery_grants (sandbox_id, local_artifact_id, approved_at);
ALTER TABLE snapshots ADD COLUMN recovery_grant_id UUID;
ALTER TABLE snapshots ADD CONSTRAINT snapshots_recovery_grant_id_fkey FOREIGN KEY (recovery_grant_id) REFERENCES backup_recovery_grants(id) NOT VALID;
