SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Firewall rules: immutable revisions, the default for newly enrolled nodes,
-- each node's assignment and observation, and the rollouts that move nodes to a
-- revision. pplx-space-firewall-rules-store owns every table here. Nothing in
-- this migration assigns a revision, starts a rollout or wakes a node.
--
-- Item phase will be bound through a closed Rust enum by the store, so its value
-- set isn't enforced here; the checks that relate a phase to another column stay.
-- Rollout phase is the exception: the unfinished-slot check sorts every phase
-- into one that holds the slot or one that doesn't, so a phase it doesn't list
-- must be refused rather than treated as finished. A new rollout phase needs a
-- migration that says whether it holds the slot.

-- A complete ruleset, not a change to the previous one. Ordered: rules apply in
-- array order. ruleset_format_version says how to read the array.
CREATE TABLE firewall_rules_revisions (
    id UUID NOT NULL,
    ruleset_format_version BIGINT NOT NULL,
    ruleset JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT firewall_rules_revisions_pkey PRIMARY KEY (id),
    CONSTRAINT firewall_rules_revisions_ruleset_format_version_check CHECK (
        ruleset_format_version >= 1
    ),
    CONSTRAINT firewall_rules_revisions_ruleset_check CHECK (
        jsonb_typeof(ruleset) = 'array'
    )
);

COMMENT ON TABLE firewall_rules_revisions IS
    'Immutable firewall rulesets, each a complete ordered snapshot';

CREATE TABLE firewall_rules_control (
    singleton BOOLEAN NOT NULL,
    enrollment_default_revision_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT firewall_rules_control_pkey PRIMARY KEY (singleton),
    CONSTRAINT firewall_rules_control_singleton_check CHECK (singleton),
    CONSTRAINT firewall_rules_control_enrollment_default_revision_id_fkey
        FOREIGN KEY (enrollment_default_revision_id) REFERENCES firewall_rules_revisions(id)
);

CREATE TRIGGER firewall_rules_control_set_updated_at
    BEFORE UPDATE ON firewall_rules_control
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Seed the single row, so writers can lock it to serialize with each other and
-- the first write needs no upsert. No revision exists yet, so there's no
-- default.
INSERT INTO firewall_rules_control (singleton, enrollment_default_revision_id)
VALUES (TRUE, NULL);

COMMENT ON TABLE firewall_rules_control IS
    'One row for the whole database; firewall writers lock it to serialize with each other';
COMMENT ON COLUMN firewall_rules_control.enrollment_default_revision_id IS
    'Revision a newly enrolled node is assigned; NULL until one is approved';

-- The actor is a durable identity, not a users row, so a deleted user can't
-- free its keys for reuse.
CREATE TABLE firewall_rules_mutation_receipts (
    id UUID NOT NULL,
    actor TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    result JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT firewall_rules_mutation_receipts_pkey PRIMARY KEY (id),
    CONSTRAINT firewall_rules_mutation_receipts_actor_idempotency_key_key
        UNIQUE (actor, idempotency_key),
    CONSTRAINT firewall_rules_mutation_receipts_actor_check CHECK (
        char_length(actor) BETWEEN 1 AND 1024
    ),
    CONSTRAINT firewall_rules_mutation_receipts_idempotency_key_check CHECK (
        char_length(idempotency_key) BETWEEN 1 AND 255
    ),
    CONSTRAINT firewall_rules_mutation_receipts_request_fingerprint_check CHECK (
        request_fingerprint ~ '^[0-9a-f]{64}$'
    )
);

COMMENT ON TABLE firewall_rules_mutation_receipts IS
    'The first result of each operator firewall request, replayed for a retry with the same actor and idempotency key';
COMMENT ON COLUMN firewall_rules_mutation_receipts.request_fingerprint IS
    'Hex SHA-256 of the request; a retry whose fingerprint differs is refused rather than replayed';
COMMENT ON COLUMN firewall_rules_mutation_receipts.result IS
    'The result first returned, replayed unchanged';

-- At most one rollout in the database is unfinished: canary, rolling and paused
-- all hold slot 1, which the unique key admits once; completed and aborted hold
-- NULL, which it admits any number of times. The slot check is null-safe, so an
-- unfinished rollout without the slot is refused rather than escaping the key.
-- A writer changes phase, unfinished_slot and finished_at in one update. The
-- cap is cluster-wide in-flight items within this rollout; it doesn't admit a
-- second rollout. The creation receipt records who started the rollout and is
-- never reused.
CREATE TABLE firewall_rules_rollouts (
    id UUID NOT NULL,
    to_revision_id UUID NOT NULL,
    created_by_receipt_id UUID NOT NULL,
    phase TEXT NOT NULL,
    -- Only ever 1 or NULL.
    -- squawk-ignore prefer-bigint-over-smallint
    unfinished_slot SMALLINT,
    max_in_flight_per_cluster BIGINT NOT NULL,
    failure_budget BIGINT NOT NULL,
    reason TEXT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT firewall_rules_rollouts_pkey PRIMARY KEY (id),
    CONSTRAINT firewall_rules_rollouts_phase_check
        CHECK (phase IN ('canary', 'rolling', 'paused', 'completed', 'aborted')),
    CONSTRAINT firewall_rules_rollouts_unfinished_slot_check
        CHECK (
            CASE
                WHEN phase IN ('canary', 'rolling', 'paused')
                    THEN unfinished_slot IS NOT DISTINCT FROM 1
                ELSE unfinished_slot IS NULL
            END
        ),
    CONSTRAINT firewall_rules_rollouts_unfinished_slot_key
        UNIQUE (unfinished_slot),
    CONSTRAINT firewall_rules_rollouts_created_by_receipt_id_key
        UNIQUE (created_by_receipt_id),
    CONSTRAINT firewall_rules_rollouts_max_in_flight_per_cluster_check CHECK (
        max_in_flight_per_cluster >= 1
    ),
    CONSTRAINT firewall_rules_rollouts_failure_budget_check CHECK (
        failure_budget >= 1
    ),
    CONSTRAINT firewall_rules_rollouts_reason_check CHECK (
        char_length(reason) BETWEEN 1 AND 1024
    ),
    CONSTRAINT firewall_rules_rollouts_generation_check CHECK (generation >= 0),
    CONSTRAINT firewall_rules_rollouts_finished_at_check CHECK (
        finished_at IS NULL OR finished_at >= created_at
    ),
    CONSTRAINT firewall_rules_rollouts_finished_at_phase_check CHECK (
        (finished_at IS NOT NULL) = (phase IN ('completed', 'aborted'))
    ),
    CONSTRAINT firewall_rules_rollouts_to_revision_id_fkey
        FOREIGN KEY (to_revision_id) REFERENCES firewall_rules_revisions(id),
    CONSTRAINT firewall_rules_rollouts_created_by_receipt_id_fkey
        FOREIGN KEY (created_by_receipt_id) REFERENCES firewall_rules_mutation_receipts(id)
);

CREATE TRIGGER firewall_rules_rollouts_set_updated_at
    BEFORE UPDATE ON firewall_rules_rollouts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- The runtime campaigns' function, shared rather than copied: it raises
-- generation on any update that changes a column other than generation,
-- updated_at, created_by_user_id and updated_by_user_id. This table has no user
-- columns, so of those it ignores only generation and updated_at. Its comment
-- records that both tables use it, so a change to the columns it ignores is
-- weighed for both.
CREATE TRIGGER firewall_rules_rollouts_raise_generation
    BEFORE UPDATE ON firewall_rules_rollouts
    FOR EACH ROW EXECUTE FUNCTION runtime_upgrade_campaigns_raise_generation();

COMMENT ON FUNCTION runtime_upgrade_campaigns_raise_generation() IS
    'Trigger for runtime_upgrade_campaigns and firewall_rules_rollouts: raises generation by one on every update that changes a column other than generation, updated_at, created_by_user_id and updated_by_user_id (firewall_rules_rollouts has no user columns). Changing the ignored columns changes both tables';

COMMENT ON TABLE firewall_rules_rollouts IS
    'Paced moves of nodes to a firewall revision; at most one is unfinished';
COMMENT ON COLUMN firewall_rules_rollouts.created_by_receipt_id IS
    'Receipt of the request that started the rollout';
COMMENT ON COLUMN firewall_rules_rollouts.phase IS
    'canary, rolling, paused, completed or aborted; finished_at is set exactly when the phase is completed or aborted';
COMMENT ON COLUMN firewall_rules_rollouts.unfinished_slot IS
    '1 while the phase is canary, rolling or paused, NULL once it is completed or aborted; unique, so one rollout is unfinished at a time';
COMMENT ON COLUMN firewall_rules_rollouts.max_in_flight_per_cluster IS
    'Items of this rollout in flight at once across the cluster';
COMMENT ON COLUMN firewall_rules_rollouts.failure_budget IS
    'Failed items at which the rollout pauses itself';
COMMENT ON COLUMN firewall_rules_rollouts.generation IS
    'Raised by one by the runtime_upgrade_campaigns_raise_generation trigger on every update that changes another column (not updated_at); unrelated to node assignment generations';

-- The nodes a rollout targets, fixed when it starts, and each one's progress.
-- node_id isn't a foreign key: an item outlives a node that deregisters
-- mid-rollout.
CREATE TABLE firewall_rules_rollout_items (
    rollout_id UUID NOT NULL,
    node_id UUID NOT NULL,
    phase TEXT NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    issued_assignment_generation BIGINT,
    issued_at TIMESTAMPTZ,
    next_check_at TIMESTAMPTZ,
    last_counted_failure_round BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT firewall_rules_rollout_items_pkey PRIMARY KEY (rollout_id, node_id),
    CONSTRAINT firewall_rules_rollout_items_attempts_check CHECK (attempts >= 0),
    CONSTRAINT firewall_rules_rollout_items_last_error_check CHECK (
        last_error IS NULL OR char_length(last_error) BETWEEN 1 AND 4096
    ),
    CONSTRAINT firewall_rules_rollout_items_lease_pair_check CHECK (
        (lease_token IS NULL) = (lease_expires_at IS NULL)
    ),
    CONSTRAINT firewall_rules_rollout_items_lease_phase_check CHECK (
        (lease_token IS NOT NULL) = (phase = 'in_flight')
    ),
    CONSTRAINT firewall_rules_rollout_items_lease_expires_at_check CHECK (
        lease_expires_at IS NULL OR isfinite(lease_expires_at)
    ),
    CONSTRAINT firewall_rules_rollout_items_issued_pair_check CHECK (
        (issued_assignment_generation IS NULL) = (issued_at IS NULL)
    ),
    CONSTRAINT firewall_rules_rollout_items_issued_assignment_generation_check CHECK (
        issued_assignment_generation IS NULL OR issued_assignment_generation >= 0
    ),
    CONSTRAINT firewall_rules_rollout_items_issued_at_check CHECK (
        issued_at IS NULL OR isfinite(issued_at)
    ),
    CONSTRAINT firewall_rules_rollout_items_next_check_at_check CHECK (
        next_check_at IS NULL OR isfinite(next_check_at)
    ),
    CONSTRAINT firewall_rules_rollout_items_last_counted_failure_round_check CHECK (
        last_counted_failure_round IS NULL OR last_counted_failure_round >= 0
    ),
    CONSTRAINT firewall_rules_rollout_items_rollout_id_fkey
        FOREIGN KEY (rollout_id) REFERENCES firewall_rules_rollouts(id) ON DELETE CASCADE
);

-- Serves per-phase progress counts, picking pending items, and item listings
-- filtered by phase and paginated by node.
CREATE INDEX firewall_rules_rollout_items_rollout_phase_node_idx
    ON firewall_rules_rollout_items (rollout_id, phase, node_id);

CREATE TRIGGER firewall_rules_rollout_items_set_updated_at
    BEFORE UPDATE ON firewall_rules_rollout_items
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE firewall_rules_rollout_items IS
    'One node a firewall rollout targets, and its progress';
COMMENT ON COLUMN firewall_rules_rollout_items.phase IS
    'pending, in_flight, done or failed; in_flight exactly while a lease is held. A pending item with an issuance stamp has been assigned and awaits readback, and is not issued again';
COMMENT ON COLUMN firewall_rules_rollout_items.lease_token IS
    'Set with lease_expires_at exactly while the item is in_flight; the outcome that ends in_flight is recorded only under the matching token';
COMMENT ON COLUMN firewall_rules_rollout_items.issued_assignment_generation IS
    'Node assignment generation this item issued, set with issued_at';
COMMENT ON COLUMN firewall_rules_rollout_items.next_check_at IS
    'Earliest time the item is next considered; NULL when none is scheduled';
COMMENT ON COLUMN firewall_rules_rollout_items.last_counted_failure_round IS
    'Latest observation round whose failure was charged to the failure budget, so a round is charged once';

-- node_id isn't a foreign key here either: deleting a node doesn't reach into
-- firewall state.
CREATE TABLE firewall_rules_node_assignments (
    node_id UUID NOT NULL,
    assignment_generation BIGINT NOT NULL,
    revision_id UUID NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT firewall_rules_node_assignments_pkey PRIMARY KEY (node_id),
    CONSTRAINT firewall_rules_node_assignments_assignment_generation_check CHECK (
        assignment_generation >= 0
    ),
    CONSTRAINT firewall_rules_node_assignments_revision_id_fkey
        FOREIGN KEY (revision_id) REFERENCES firewall_rules_revisions(id)
);

CREATE TRIGGER firewall_rules_node_assignments_set_updated_at
    BEFORE UPDATE ON firewall_rules_node_assignments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE firewall_rules_node_assignments IS
    'The firewall revision each node is to enforce';
COMMENT ON COLUMN firewall_rules_node_assignments.assignment_generation IS
    'Raised by the writer on each change of this node''s assignment; independent of rollout generations';

-- The pending round and the latest accepted report are separate columns, so
-- opening a round keeps the evidence of the last one.
CREATE TABLE firewall_rules_node_observations (
    node_id UUID NOT NULL,
    manager_epoch BIGINT NOT NULL,
    pending_round BIGINT,
    pending_round_issued_at TIMESTAMPTZ,
    accepted_round BIGINT,
    accepted_round_issued_at TIMESTAMPTZ,
    accepted_report JSONB,
    accepted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT firewall_rules_node_observations_pkey PRIMARY KEY (node_id),
    CONSTRAINT firewall_rules_node_observations_manager_epoch_check CHECK (
        manager_epoch >= 0
    ),
    CONSTRAINT firewall_rules_node_observations_pending_round_check CHECK (
        (pending_round IS NULL) = (pending_round_issued_at IS NULL)
        AND (pending_round IS NULL OR pending_round >= 0)
        AND (pending_round_issued_at IS NULL OR isfinite(pending_round_issued_at))
    ),
    CONSTRAINT firewall_rules_node_observations_accepted_check CHECK (
        (accepted_round IS NULL) = (accepted_round_issued_at IS NULL)
        AND (accepted_round IS NULL) = (accepted_report IS NULL)
        AND (accepted_round IS NULL) = (accepted_at IS NULL)
        AND (accepted_round IS NULL OR accepted_round >= 0)
        AND (accepted_round_issued_at IS NULL OR isfinite(accepted_round_issued_at))
    ),
    CONSTRAINT firewall_rules_node_observations_accepted_report_check CHECK (
        accepted_report IS NULL OR jsonb_typeof(accepted_report) = 'object'
    )
);

CREATE TRIGGER firewall_rules_node_observations_set_updated_at
    BEFORE UPDATE ON firewall_rules_node_observations
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE firewall_rules_node_observations IS
    'What each node last reported enforcing, and the round it is being asked about';
COMMENT ON COLUMN firewall_rules_node_observations.manager_epoch IS
    'Node manager epoch the record belongs to';
COMMENT ON COLUMN firewall_rules_node_observations.pending_round_issued_at IS
    'When the pending round was first issued, set with pending_round';
COMMENT ON COLUMN firewall_rules_node_observations.accepted_report IS
    'The latest accepted report, a JSON object';
COMMENT ON COLUMN firewall_rules_node_observations.accepted_round_issued_at IS
    'When the round of the latest accepted report was first issued, set with accepted_round, accepted_report and accepted_at';
