SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Cluster-wide execution environment policy: a state per environment, plus one
-- policy row naming the default and carrying the policy generation.
CREATE TABLE runtime_environment_states (
    environment_ref TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by_user_id UUID,
    reason TEXT NOT NULL,
    CONSTRAINT runtime_environment_states_pkey PRIMARY KEY (environment_ref),
    CONSTRAINT runtime_environment_states_ref_state_key UNIQUE (environment_ref, state),
    CONSTRAINT runtime_environment_states_environment_ref_check CHECK (
        environment_ref ~ '^(sha256|blake3):[0-9a-f]{64}$'
    ),
    CONSTRAINT runtime_environment_states_reason_check CHECK (
        char_length(reason) BETWEEN 1 AND 1024
    ),
    CONSTRAINT runtime_environment_states_updated_by_user_id_fkey
        FOREIGN KEY (updated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TRIGGER runtime_environment_states_set_updated_at
    BEFORE UPDATE ON runtime_environment_states
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE runtime_environment_states IS
    'Per-environment policy state, keyed by environment reference';
COMMENT ON COLUMN runtime_environment_states.state IS
    'allowed and deprecated execute; blocked never executes';

CREATE TABLE runtime_environment_policy (
    singleton BOOLEAN NOT NULL,
    generation BIGINT NOT NULL,
    default_environment_ref TEXT,
    -- Always 'allowed': with default_environment_ref it references the default's
    -- state row, so the default can't be blocked, deprecated or deleted.
    default_environment_state TEXT GENERATED ALWAYS AS ('allowed') STORED,
    legacy_unbound_allowed BOOLEAN,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by_user_id UUID,
    reason TEXT,
    CONSTRAINT runtime_environment_policy_pkey PRIMARY KEY (singleton),
    CONSTRAINT runtime_environment_policy_singleton_check CHECK (singleton),
    CONSTRAINT runtime_environment_policy_generation_check CHECK (generation >= 0),
    CONSTRAINT runtime_environment_policy_reason_check CHECK (
        reason IS NULL OR char_length(reason) BETWEEN 1 AND 1024
    ),
    CONSTRAINT runtime_environment_policy_default_fkey
        FOREIGN KEY (default_environment_ref, default_environment_state)
        REFERENCES runtime_environment_states (environment_ref, state),
    CONSTRAINT runtime_environment_policy_updated_by_user_id_fkey
        FOREIGN KEY (updated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TRIGGER runtime_environment_policy_set_updated_at
    BEFORE UPDATE ON runtime_environment_policy
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Seed the single row, so policy writers can lock it to serialize with each other
-- and the first write needs no upsert. Generation 0 is the bootstrap policy,
-- which overrides nothing a node is configured with.
INSERT INTO runtime_environment_policy (
    singleton, generation, default_environment_ref, legacy_unbound_allowed
) VALUES (TRUE, 0, NULL, NULL);

COMMENT ON COLUMN runtime_environment_policy.generation IS
    'Policy version. Writers must increment it in the same transaction as any policy change and never decrease it; rolling this migration back and reapplying it restarts it at 0';
COMMENT ON COLUMN runtime_environment_policy.default_environment_ref IS
    'Environment new unbound work binds to; NULL means the cluster sets no default and each node keeps its configured one';
COMMENT ON COLUMN runtime_environment_policy.legacy_unbound_allowed IS
    'Whether unbound historical contracts that match no compiled environment may execute; NULL means each node keeps its configured permission';
