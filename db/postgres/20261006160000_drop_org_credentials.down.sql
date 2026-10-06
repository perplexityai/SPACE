SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Recreate org_credentials empty, as the baseline defined it. Dropped rows
-- are not recovered.
CREATE TABLE public.org_credentials (
    id uuid DEFAULT public.space_uuid_v7() NOT NULL,
    org_id uuid NOT NULL,
    key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    value bytea NOT NULL,
    wrapped_key bytea NOT NULL,
    CONSTRAINT org_credentials_org_id_key_key UNIQUE (org_id, key),
    CONSTRAINT org_credentials_pkey PRIMARY KEY (id),
    CONSTRAINT org_credentials_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE
);

CREATE INDEX idx_org_credentials_org_id ON public.org_credentials USING btree (org_id);

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.org_credentials TO space_credmgr;
