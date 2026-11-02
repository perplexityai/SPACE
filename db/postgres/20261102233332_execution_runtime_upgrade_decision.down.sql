-- Finish or retire every active execution that records a runtime upgrade
-- decision while the image that writes these columns is still deployed, then
-- roll that image back, then run this.
-- Down runs once, without the lock retry up has; rerun it if it times out.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Refuse while an active execution records a decision. That's the leftover of a
-- stay-stopped upgrade attempt that failed partway, whose node may still hold
-- the upgraded disk without its backup, or such a leftover a start has since
-- taken over. Dropping the columns would discard the decision, and after a
-- later up the stopped sandbox's upgrade would read the NULLs as "no upgrade
-- was sent", retire the execution without the backup and decide again. A
-- leftover is finished by retrying the stopped sandbox's runtime upgrade while
-- that image is still deployed: the retry re-sends the recorded upgrade, and
-- once the node finishes it, the retry enqueues the disk backup and retires the
-- execution, which leaves it inactive. When the node answers that it rolled the
-- upgrade back or never started it, the retry retires it too, without a backup;
-- any other failure keeps it for another retry. The upgrade refuses a sandbox
-- that isn't stopped, and a retry for a sandbox being evacuated or with no disk
-- on a node leaves the leftover in place. A leftover a start took over is no
-- longer read, since that upgrade only claims executions targeting stopped, but
-- it still refuses this until the execution is retired or its sandbox is
-- deleted, which is also the way out when no retry can finish it. Only an image
-- with that upgrade path writes the columns, so one has been deployed whenever
-- this check refuses; if it has since been rolled back, deploy it again to
-- finish them. Clearing the columns by hand discards the decision this check
-- protects. Inactive executions' decisions are only history and may go. Holding
-- the lock, which DROP COLUMN takes anyway, keeps a decision from landing
-- between the check and the drop.
LOCK TABLE sandbox_k8s_executions IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    blocking BIGINT;
    sample UUID;
BEGIN
    SELECT count(*), min(sandbox_id::text)::uuid
      INTO blocking, sample
      FROM sandbox_k8s_executions
     WHERE active
       AND runtime_upgrade_operation_id IS NOT NULL;
    IF blocking > 0 THEN
        RAISE EXCEPTION '% active Kubernetes execution(s) record a runtime '
            'upgrade decision (one is sandbox %), and dropping the columns '
            'would discard it; finish or retire them as this migration''s '
            'comment describes, while the image that writes these columns is '
            'deployed, then roll back this migration',
            blocking, sample;
    END IF;
END;
$$;

ALTER TABLE sandbox_k8s_executions
    -- squawk-ignore ban-drop-column
    DROP COLUMN IF EXISTS runtime_upgrade_authorization_id,
    -- squawk-ignore ban-drop-column
    DROP COLUMN IF EXISTS runtime_upgrade_target_environment_ref,
    -- squawk-ignore ban-drop-column
    DROP COLUMN IF EXISTS runtime_upgrade_operation_id;
