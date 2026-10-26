-- Finish or abort every campaign and let its leases lapse while the code that
-- drives campaigns is still deployed, then roll that code back, then run this.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Refuse while a campaign is unfinished, which is while its finished_at is
-- unset (paused included), or while any item holds an unexpired lease, which means a worker may
-- still be upgrading its subject. Dropping either would silently stop an upgrade
-- partway, such as moving sandboxes off a blocked environment, and lose which
-- subjects it had already moved. Finished campaigns whose leases have lapsed are
-- only history and may go. Once that code is gone, nothing can abort a
-- campaign, so a cluster administrator has to clear a refusal with direct SQL:
-- set phase to 'aborted' and finished_at to now() on each unfinished campaign.
-- Aborting doesn't clear leases, so the second check still waits for them; a
-- lease that won't lapse in time is cleared the same way, by setting the item's
-- phase to 'failed' and its lease_token and lease_expires_at to NULL.
-- Holding the locks keeps any write from landing between the check and the
-- drop. Users is locked first to match a user deletion's ON DELETE SET NULL
-- cascade, but ordinary campaign and item writes take these tables in the
-- opposite order, so no order is deadlock-free while that code runs; running
-- this only after it's gone is what avoids the deadlock.
LOCK TABLE users, runtime_upgrade_campaigns, runtime_upgrade_items
    IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1
                 FROM runtime_upgrade_campaigns
                WHERE finished_at IS NULL) THEN
        RAISE EXCEPTION 'a runtime upgrade campaign is unfinished, and dropping '
            'it would stop the upgrade partway; finish or abort every campaign '
            'before rolling back this migration, and if the code that drives '
            'campaigns is already gone, abort them by direct SQL (phase aborted, '
            'finished_at now())';
    END IF;
    -- A lease is held only while in_flight, which the up migration's checks
    -- enforce, so probing each campaign's in_flight rows through the
    -- (campaign_id, phase, subject_id) index finds every live lease without
    -- scanning item history.
    IF EXISTS (SELECT 1
                 FROM runtime_upgrade_campaigns AS c
                WHERE EXISTS (SELECT 1
                                FROM runtime_upgrade_items AS i
                               WHERE i.campaign_id = c.id
                                 AND i.phase = 'in_flight'
                                 AND i.lease_expires_at > now())) THEN
        RAISE EXCEPTION 'a runtime upgrade item holds an unexpired lease, so a '
            'worker may still be upgrading it; wait for every lease to expire '
            'before rolling back this migration, or once no worker can still be '
            'running, fail the item by direct SQL (phase failed, lease_token and '
            'lease_expires_at NULL)';
    END IF;
END;
$$;

DROP TABLE runtime_upgrade_items;
DROP TABLE runtime_upgrade_campaigns;
