-- Prototype state is disposable. Clear requests so old jobs cannot replay
-- against the isolated artifact store. Keep shared schema and migration history.
DELETE FROM jobs;
DROP TABLE IF EXISTS kernel_installs;
DROP TABLE IF EXISTS kernel_job_results;
DROP TABLE IF EXISTS operation_results;

CREATE TABLE operation_results (
    job_id TEXT PRIMARY KEY,
    phase TEXT,
    outcome TEXT,
    path TEXT NOT NULL,
    digest TEXT NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    CHECK ((phase IS NULL) <> (outcome IS NULL))
);
