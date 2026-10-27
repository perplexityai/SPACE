SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE template_releases
    ADD COLUMN chunk_representations JSONB NOT NULL DEFAULT '[]'::jsonb;
