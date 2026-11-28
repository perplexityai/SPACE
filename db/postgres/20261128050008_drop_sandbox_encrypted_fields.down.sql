SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Re-create `sandbox_encrypted_fields` empty, with the baseline's columns,
-- constraints and index. Dropped rows are not recovered. An earlier migration's
-- down restores its foreign keys.
--
-- It is a credential table. Default privileges grant a re-created table to
-- `space_user` and `space_ro`, and no role bootstrap runs right after a down,
-- so this down revokes them itself. Every bootstrap from this release back to
-- the one that added RETIRED_CREDENTIAL_TABLES fences it from then on, as do
-- older releases' CREDENTIAL_TABLES.

CREATE TABLE public.sandbox_encrypted_fields (
    id uuid DEFAULT public.space_uuid_v7() NOT NULL,
    sandbox_id uuid NOT NULL,
    org_id uuid NOT NULL,
    master_key_id uuid NOT NULL,
    wrapped_dek bytea NOT NULL,
    env bytea NOT NULL,
    metadata bytea NOT NULL,
    network_policy_override_json bytea,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT sandbox_encrypted_fields_pkey PRIMARY KEY (id),
    CONSTRAINT sandbox_encrypted_fields_sandbox_id_key UNIQUE (sandbox_id)
);

CREATE INDEX idx_sandbox_encrypted_master_key ON public.sandbox_encrypted_fields USING btree (master_key_id, org_id);

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'space_user') THEN
        REVOKE ALL ON TABLE public.sandbox_encrypted_fields FROM space_user;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'space_ro') THEN
        REVOKE ALL ON TABLE public.sandbox_encrypted_fields FROM space_ro;
    END IF;
END
$$;
