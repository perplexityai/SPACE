SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Every environment a node build has published, with its identity and the
-- edges an upgrade may follow. Retirement is the one operator-owned column.
-- Nothing writes the table yet.
--
-- The array CHECKs match the array's text form, which is the same under every
-- setting for TEXT and INT elements. It quotes any element that's empty, holds
-- whitespace or one of ,{}"\, or reads NULL in any case, and it writes NULL
-- elements, nested arrays and non-default bounds in shapes the patterns don't
-- match. So one pattern per column checks every element and the array's shape.
CREATE TABLE runtime_environments (
    environment_ref TEXT NOT NULL,
    runtime TEXT NOT NULL,
    runtime_version TEXT NOT NULL,
    -- NULL when the runtime adapter has no integer form for the version. The
    -- identifier rule admits versions such as 'nightly', and refusing the row
    -- would refuse the publish that reports it.
    -- squawk-ignore prefer-bigint-over-int
    version_sort_key INT[],
    kernel_version TEXT NOT NULL,
    state_format_name TEXT NOT NULL,
    state_format_version TEXT NOT NULL,
    upgrade_edges TEXT[] NOT NULL DEFAULT '{}',
    memory_edges TEXT[] NOT NULL DEFAULT '{}',
    retired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT runtime_environments_pkey PRIMARY KEY (environment_ref),
    CONSTRAINT runtime_environments_environment_ref_check CHECK (
        environment_ref ~ '^(sha256|blake3):[0-9a-f]{64}$'
    ),
    -- The execution metadata identifier rule: 1-128 ASCII alphanumerics, '.',
    -- '_', '-' or '+'.
    CONSTRAINT runtime_environments_identity_check CHECK (
        runtime ~ '^[A-Za-z0-9._+-]{1,128}$'
        AND runtime_version ~ '^[A-Za-z0-9._+-]{1,128}$'
        AND kernel_version ~ '^[A-Za-z0-9._+-]{1,128}$'
        AND state_format_name ~ '^[A-Za-z0-9._+-]{1,128}$'
        AND state_format_version ~ '^[A-Za-z0-9._+-]{1,128}$'
    ),
    CONSTRAINT runtime_environments_version_sort_key_check CHECK (
        version_sort_key::TEXT ~ '^\{[0-9]+(,[0-9]+)*\}$'
    ),
    CONSTRAINT runtime_environments_upgrade_edges_check CHECK (
        upgrade_edges::TEXT ~ '^\{((sha256|blake3):[0-9a-f]{64}(,(sha256|blake3):[0-9a-f]{64})*)?\}$'
        AND array_position(upgrade_edges, environment_ref) IS NULL
    ),
    -- A memory edge is an upgrade edge that also certifies the target as a
    -- loader for this environment's checkpoints, so it's always an upgrade
    -- edge too, and never a self-edge.
    CONSTRAINT runtime_environments_memory_edges_check CHECK (
        memory_edges::TEXT ~ '^\{((sha256|blake3):[0-9a-f]{64}(,(sha256|blake3):[0-9a-f]{64})*)?\}$'
        AND memory_edges <@ upgrade_edges
    )
);

CREATE TRIGGER runtime_environments_set_updated_at
    BEFORE UPDATE ON runtime_environments
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

COMMENT ON TABLE runtime_environments IS
    'Known execution environments, upserted from node publishes; edges are the intersection of what reporting node builds certify';
COMMENT ON COLUMN runtime_environments.version_sort_key IS
    'Runtime version as integers in comparison order, as on node_runtime_installations; NULL means the version has no integer form, not the lowest version';
COMMENT ON COLUMN runtime_environments.upgrade_edges IS
    'Environments this one can be cold-boot upgraded to';
COMMENT ON COLUMN runtime_environments.memory_edges IS
    'Upgrade edges whose target is also certified to load this environment''s checkpoints';
COMMENT ON COLUMN runtime_environments.retired_at IS
    'When an operator retired the environment; publishes never write it. A retired environment doesn''t count as usable on any node, and nodes may uninstall it';
