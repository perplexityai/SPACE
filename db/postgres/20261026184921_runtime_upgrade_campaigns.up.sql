SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Runtime upgrade campaigns move template releases or sandboxes from one
-- execution environment to another, paced per node and per cluster. Items track
-- each subject through its upgrade. Kind, phase and subject kind will be bound
-- through closed Rust enums by their store, so their value sets aren't enforced
-- here. The checks that relate a phase to another column stay, since updates
-- that each change one column can combine into a row neither validated.
CREATE TABLE runtime_upgrade_campaigns (
    id UUID NOT NULL DEFAULT space_uuid_v7(),
    from_environment_ref TEXT NOT NULL,
    to_environment_ref TEXT NOT NULL,
    kind TEXT NOT NULL,
    phase TEXT NOT NULL,
    max_in_flight_per_node BIGINT NOT NULL,
    max_in_flight_per_cluster BIGINT NOT NULL,
    failure_budget BIGINT NOT NULL,
    include_running BOOLEAN NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_user_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by_user_id UUID,
    finished_at TIMESTAMPTZ,
    CONSTRAINT runtime_upgrade_campaigns_pkey PRIMARY KEY (id),
    CONSTRAINT runtime_upgrade_campaigns_from_environment_ref_check CHECK (
        from_environment_ref ~ '^(sha256|blake3):[0-9a-f]{64}$'
    ),
    CONSTRAINT runtime_upgrade_campaigns_to_environment_ref_check CHECK (
        to_environment_ref ~ '^(sha256|blake3):[0-9a-f]{64}$'
    ),
    CONSTRAINT runtime_upgrade_campaigns_distinct_environments_check CHECK (
        from_environment_ref <> to_environment_ref
    ),
    CONSTRAINT runtime_upgrade_campaigns_max_in_flight_check CHECK (
        max_in_flight_per_node >= 1
        AND max_in_flight_per_node <= max_in_flight_per_cluster
    ),
    CONSTRAINT runtime_upgrade_campaigns_failure_budget_check CHECK (
        failure_budget >= 1
    ),
    CONSTRAINT runtime_upgrade_campaigns_reason_check CHECK (
        char_length(reason) BETWEEN 1 AND 1024
    ),
    CONSTRAINT runtime_upgrade_campaigns_finished_at_check CHECK (
        finished_at IS NULL OR finished_at >= created_at
    ),
    CONSTRAINT runtime_upgrade_campaigns_finished_at_phase_check CHECK (
        (finished_at IS NOT NULL) = (phase IN ('completed', 'aborted'))
    ),
    CONSTRAINT runtime_upgrade_campaigns_created_by_user_id_fkey
        FOREIGN KEY (created_by_user_id) REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT runtime_upgrade_campaigns_updated_by_user_id_fkey
        FOREIGN KEY (updated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TRIGGER runtime_upgrade_campaigns_set_updated_at
    BEFORE UPDATE ON runtime_upgrade_campaigns
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE runtime_upgrade_campaigns IS
    'Paced upgrades of template releases or sandboxes from one execution environment to another';
COMMENT ON COLUMN runtime_upgrade_campaigns.kind IS
    'templates or sandboxes';
COMMENT ON COLUMN runtime_upgrade_campaigns.phase IS
    'canary, rolling, paused, completed or aborted; finished_at is set exactly when the phase is completed or aborted';
COMMENT ON COLUMN runtime_upgrade_campaigns.max_in_flight_per_node IS
    'Items of this campaign in flight at once on any one node';
COMMENT ON COLUMN runtime_upgrade_campaigns.max_in_flight_per_cluster IS
    'Items of this campaign in flight at once across the cluster; concurrent campaigns each have their own';
COMMENT ON COLUMN runtime_upgrade_campaigns.failure_budget IS
    'Failed items at which the campaign pauses itself';
COMMENT ON COLUMN runtime_upgrade_campaigns.include_running IS
    'Whether a sandbox campaign also upgrades running and paused sandboxes, which restarts them unless the target environment is certified to load their memory snapshots';

-- Subjects aren't foreign keys: subject_kind says which table subject_id names,
-- and an item outlives a sandbox deleted mid-campaign.
CREATE TABLE runtime_upgrade_items (
    campaign_id UUID NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id UUID NOT NULL,
    phase TEXT NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT runtime_upgrade_items_pkey PRIMARY KEY (campaign_id, subject_kind, subject_id),
    CONSTRAINT runtime_upgrade_items_attempts_check CHECK (attempts >= 0),
    CONSTRAINT runtime_upgrade_items_last_error_check CHECK (
        last_error IS NULL OR char_length(last_error) BETWEEN 1 AND 4096
    ),
    CONSTRAINT runtime_upgrade_items_lease_pair_check CHECK (
        (lease_token IS NULL) = (lease_expires_at IS NULL)
    ),
    CONSTRAINT runtime_upgrade_items_lease_phase_check CHECK (
        (lease_token IS NOT NULL) = (phase = 'in_flight')
    ),
    CONSTRAINT runtime_upgrade_items_lease_expires_at_check CHECK (
        lease_expires_at IS NULL OR isfinite(lease_expires_at)
    ),
    CONSTRAINT runtime_upgrade_items_campaign_id_fkey
        FOREIGN KEY (campaign_id) REFERENCES runtime_upgrade_campaigns(id) ON DELETE CASCADE
);

-- Serves per-phase progress counts, picking pending items, and item listings
-- filtered by phase and paginated by subject.
CREATE INDEX runtime_upgrade_items_campaign_phase_subject_idx
    ON runtime_upgrade_items (campaign_id, phase, subject_id);

CREATE TRIGGER runtime_upgrade_items_set_updated_at
    BEFORE UPDATE ON runtime_upgrade_items
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE runtime_upgrade_items IS
    'One template release or sandbox in a runtime upgrade campaign';
COMMENT ON COLUMN runtime_upgrade_items.subject_kind IS
    'template_release or sandbox; says which table subject_id names';
COMMENT ON COLUMN runtime_upgrade_items.phase IS
    'pending, in_flight, done, failed or skipped';
COMMENT ON COLUMN runtime_upgrade_items.lease_token IS
    'Set with lease_expires_at exactly while the item is in_flight; the outcome that ends in_flight is recorded only under the matching token';
COMMENT ON COLUMN runtime_upgrade_items.lease_expires_at IS
    'An in_flight item past this time may be re-driven under a new token. The upgrade is idempotent, but the token fences only the recorded outcome, so a worker renews its lease while it works and stops once the lease is lost';
