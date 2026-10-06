SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Recreate user_credentials empty, as the baseline defined it. Dropped rows
-- are not recovered.
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
    CONSTRAINT user_credentials_user_id_key_key UNIQUE (user_id, key),
    CONSTRAINT user_credentials_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(id) ON DELETE CASCADE,
    CONSTRAINT user_credentials_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE
);

CREATE INDEX idx_user_credentials_user_id ON public.user_credentials USING btree (user_id);

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE public.user_credentials TO space_credmgr;
