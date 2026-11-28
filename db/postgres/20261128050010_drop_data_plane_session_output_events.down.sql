SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Re-create `data_plane_session_output_events` empty, with the baseline's
-- columns and constraints. Dropped rows are not recovered. An earlier
-- migration's down restores its foreign keys.

CREATE TABLE public.data_plane_session_output_events (
    session_id text NOT NULL,
    sequence bigint NOT NULL,
    event_name text NOT NULL,
    frame jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT data_plane_session_output_events_sequence_check CHECK ((sequence > 0)),
    CONSTRAINT data_plane_session_output_events_pkey PRIMARY KEY (session_id, sequence)
);
