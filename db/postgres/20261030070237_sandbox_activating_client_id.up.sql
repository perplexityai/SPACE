SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The client id whose activation created the sandbox's current runtime: the
-- caller's X-Client-ID when it's a valid, unreserved label value, 'unknown'
-- otherwise, and 'native' for native /v1 and admin calls. Its writers are row
-- inserts, Kubernetes pod tracking (from the tracked pod's
-- space.pplx.net/client-id label) and Direct start and restore receipts. An
-- activation that inherits the sandbox's client reads it to charge that client
-- and label the pod it creates. It stores the id, not the limit key the id
-- resolves to, so a change to the configured key set needs no rewrite.
--
-- Nullable with no default, backfill or index: NULL means nothing is recorded,
-- and nothing filters on the column. The CHECK holds it to a non-empty
-- Kubernetes label value (at most 63 characters, alphanumeric at both ends,
-- with '-', '_' and '.' allowed inside), which the reserved ids satisfy. It's
-- NOT VALID so adding it doesn't scan sandboxes; every existing row is NULL,
-- and new writes are checked either way. A later migration can VALIDATE it.
ALTER TABLE sandboxes
    ADD COLUMN activating_client_id text,
    ADD CONSTRAINT sandboxes_activating_client_id_check
        CHECK (activating_client_id IS NULL
               OR (char_length(activating_client_id) <= 63
                   AND activating_client_id
                       ~ '^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$'))
        NOT VALID;
