SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Re-create `sandbox_archives` empty, with the baseline's columns, constraints
-- and indexes. Dropped rows are not recovered. An earlier migration's down
-- restores its foreign keys.

CREATE TABLE public.sandbox_archives (
    sandbox_id uuid NOT NULL,
    org_id uuid NOT NULL,
    user_id uuid NOT NULL,
    sandbox_created_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    purge_at timestamp with time zone,
    manifest_id uuid NOT NULL,
    manifest_sha256 bytea NOT NULL,
    stored_bytes bigint NOT NULL,
    template_release_id uuid,
    CONSTRAINT sandbox_archives_pkey PRIMARY KEY (sandbox_id)
);

CREATE INDEX sandbox_archives_org_list_idx ON public.sandbox_archives USING btree (org_id, sandbox_created_at DESC);

CREATE INDEX sandbox_archives_purge_idx ON public.sandbox_archives USING btree (purge_at);

CREATE INDEX sandbox_archives_template_release_idx ON public.sandbox_archives USING btree (template_release_id);

CREATE INDEX sandbox_archives_user_list_idx ON public.sandbox_archives USING btree (user_id, sandbox_created_at DESC);
