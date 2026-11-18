-- Finish or abort every rollout, let its leases lapse, and clear node
-- assignments and the enrollment default while the code that writes them is
-- still deployed, then roll that code back, then run this.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Refuse while a rollout is unfinished (paused included), while any item holds
-- an unexpired lease, or while any node assignment or the enrollment default is
-- set. Dropping those would silently stop a rollout partway and lose which
-- revision each node is meant to enforce. Revisions, receipts, finished
-- rollouts and observations are only history and evidence, and may go. Once
-- that code is gone, a cluster administrator clears a refusal with direct SQL:
-- abort each unfinished rollout (phase 'aborted', unfinished_slot NULL,
-- finished_at now()), fail each leased item (phase 'failed', lease_token and
-- lease_expires_at NULL) once no worker can still hold it, delete the node
-- assignments, and set the enrollment default to NULL.
-- Holding the locks keeps any write from landing between the check and the
-- drop.
LOCK TABLE firewall_rules_revisions,
    firewall_rules_control,
    firewall_rules_mutation_receipts,
    firewall_rules_rollouts,
    firewall_rules_rollout_items,
    firewall_rules_node_assignments,
    firewall_rules_node_observations
    IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM firewall_rules_rollouts
                WHERE finished_at IS NULL) THEN
        RAISE EXCEPTION 'a firewall rules rollout is unfinished, and dropping it '
            'would stop it partway; finish or abort it before rolling back this '
            'migration, and if the code that drives rollouts is already gone, '
            'abort it by direct SQL (phase aborted, unfinished_slot NULL, '
            'finished_at now())';
    END IF;
    -- A lease is held only while in_flight, which the up migration's checks
    -- enforce, so probing each rollout's in_flight rows through the
    -- (rollout_id, phase, node_id) index finds every live lease without
    -- scanning item history.
    IF EXISTS (SELECT 1
                 FROM firewall_rules_rollouts AS r
                WHERE EXISTS (SELECT 1
                                FROM firewall_rules_rollout_items AS i
                               WHERE i.rollout_id = r.id
                                 AND i.phase = 'in_flight'
                                 AND i.lease_expires_at > now())) THEN
        RAISE EXCEPTION 'a firewall rules rollout item holds an unexpired lease, '
            'so a worker may still be issuing it; wait for every lease to expire '
            'before rolling back this migration, or once no worker can still be '
            'running, fail the item by direct SQL (phase failed, lease_token and '
            'lease_expires_at NULL)';
    END IF;
    IF EXISTS (SELECT 1 FROM firewall_rules_node_assignments)
       OR EXISTS (SELECT 1
                    FROM firewall_rules_control
                   WHERE enrollment_default_revision_id IS NOT NULL) THEN
        RAISE EXCEPTION 'a node firewall rules assignment or the enrollment '
            'default is set, and dropping it would lose which revision nodes '
            'are meant to enforce; delete every firewall_rules_node_assignments '
            'row and set firewall_rules_control.enrollment_default_revision_id '
            'to NULL before rolling back this migration';
    END IF;
END;
$$;

-- The function had no comment before the up migration gave it one naming both
-- tables, and runtime_upgrade_campaigns is again its only user.
COMMENT ON FUNCTION runtime_upgrade_campaigns_raise_generation() IS NULL;

-- Referencing tables first. Dropping a table drops its triggers but not their
-- functions, so set_updated_at and runtime_upgrade_campaigns_raise_generation
-- stay for the tables that still use them.
DROP TABLE firewall_rules_node_observations;
DROP TABLE firewall_rules_node_assignments;
DROP TABLE firewall_rules_rollout_items;
DROP TABLE firewall_rules_rollouts;
DROP TABLE firewall_rules_control;
DROP TABLE firewall_rules_mutation_receipts;
DROP TABLE firewall_rules_revisions;
