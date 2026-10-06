SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Content digests the uploading node reports when a backup attempt is
-- finalized, one row per artifact file. Restore verifies the bytes it pulls
-- against these rows instead of trusting the object store.
CREATE TABLE public.snapshot_artifact_digests (
    snapshot_id uuid NOT NULL,
    artifact text NOT NULL,
    digest text NOT NULL,
    size_bytes bigint NOT NULL,
    CONSTRAINT snapshot_artifact_digests_pkey PRIMARY KEY (snapshot_id, artifact),
    CONSTRAINT snapshot_artifact_digests_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.snapshots(id) ON DELETE CASCADE,
    CONSTRAINT snapshot_artifact_digests_digest_check CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT snapshot_artifact_digests_size_bytes_check CHECK (size_bytes >= 0)
);
