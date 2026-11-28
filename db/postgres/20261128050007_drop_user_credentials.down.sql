SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Re-create `user_credentials` empty, with the baseline's columns, constraints,
-- index and grant. Dropped rows are not recovered. An earlier migration's down
-- restores its foreign keys.
--
-- It is a credential table. Default privileges grant a re-created table to
-- `space_user` and `space_ro`, and no role bootstrap runs right after a down,
-- so this down revokes them itself. Every bootstrap from this release back to
-- the one that added RETIRED_CREDENTIAL_TABLES fences it from then on, as do
-- older releases' CREDENTIAL_TABLES.

CREATE TABLE public.user_credentials (
    id uuid DEFAULT public.space_uuid_v7() NOT NULL,
    user_id uuid NOT NULL,
    key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    value bytea NOT NULL,
    wrapped_key bytea NOT NULL,
    org_id uuid NOT NULL,
    CONSTRAINT user_credentials_pkey PRIMARY KEY (id),
    CONSTRAINT user_credentials_user_id_key_key UNIQUE (user_id, key)
);

CREATE INDEX idx_user_credentials_user_id ON public.user_credentials USING btree (user_id);

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.user_credentials TO space_credmgr;

DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'space_user') THEN
        REVOKE ALL ON TABLE public.user_credentials FROM space_user;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'space_ro') THEN
        REVOKE ALL ON TABLE public.user_credentials FROM space_ro;
    END IF;
END
$$;
