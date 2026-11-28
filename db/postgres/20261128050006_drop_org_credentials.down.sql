SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Re-create `org_credentials` empty, with the baseline's columns, constraints,
-- index and grant. Dropped rows are not recovered. An earlier migration's down
-- restores its foreign keys.
--
-- It is a credential table. Default privileges grant a re-created table to
-- `space_user` and `space_ro`, and no role bootstrap runs right after a down,
-- so this down revokes them itself. Every bootstrap from this release back to
-- the one that added RETIRED_CREDENTIAL_TABLES fences it from then on, as do
-- older releases' CREDENTIAL_TABLES.

CREATE TABLE public.org_credentials (
    id uuid DEFAULT public.space_uuid_v7() NOT NULL,
    org_id uuid NOT NULL,
    key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    value bytea NOT NULL,
    wrapped_key bytea NOT NULL,
    CONSTRAINT org_credentials_org_id_key_key UNIQUE (org_id, key),
    CONSTRAINT org_credentials_pkey PRIMARY KEY (id)
);

CREATE INDEX idx_org_credentials_org_id ON public.org_credentials USING btree (org_id);

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.org_credentials TO space_credmgr;

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'space_user') THEN
        REVOKE ALL ON TABLE public.org_credentials FROM space_user;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'space_ro') THEN
        REVOKE ALL ON TABLE public.org_credentials FROM space_ro;
    END IF;
END
$$;
