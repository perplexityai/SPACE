-- Evacuation-path wake claim: move a drain-marked sandbox `suspended` (or a
-- released stopped evacuation, `stopped`) -> `restoring` and mint the lease
-- half of the placement fence ($3, expiring $2 seconds out). Candidate WHERE
-- is adapted from the marker-required branch of `begin_restore_lifecycle` in
-- the api substrate store; unlike that tokenless legacy writer this claim
-- mints a fence, so it also waits out an unexpired lease instead of stealing
-- it.

WITH candidate AS (
  SELECT id, lifecycle
  FROM sandboxes
  WHERE id = $1
    -- an already-restoring marked row is crash recovery: the drain died
    -- mid-restore and the re-drive must resume it
    AND (
      LOWER(TRIM(lifecycle)) IN ('suspended', 'restoring')
      -- a stopped evacuation the teardown has released: only its own marker
      -- value admits it (a markerless `stopped` row is user-owned), and a row
      -- still on its node would be placed twice
      OR (
        LOWER(TRIM(lifecycle)) = 'stopped'
        AND evacuation_restore_target = 'stopped'
        AND active_node_id IS NULL
      )
    )
    -- the drain marker is this claim's whole authority; a user wake clears
    -- it, and from then on the evacuation claim must lose
    AND evacuation_restore_target IS NOT NULL
    -- an unexpired lease means a live owner (user or prior evacuation
    -- attempt); replacing its token would cost that owner its completion
    AND (
      restore_lease_expires_at IS NULL
      OR restore_lease_expires_at <= NOW()
    )
    -- honor the cohort restore-backoff schedule; only evacuation claims are
    -- throttled by it, user demand never waits
    AND (
      next_restore_attempt_at IS NULL
      OR next_restore_attempt_at <= NOW()
    )
  FOR UPDATE
)

-- The marker deliberately survives the claim: it carries the pre-evacuation
-- lifecycle for the completion to stamp back and consume (a `'stopped'` one
-- has no completion yet; `complete_placement` refuses it).
UPDATE sandboxes s
SET lifecycle = 'restoring',
    restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
    restore_lease_token = $3
FROM candidate c
WHERE s.id = c.id

RETURNING CEIL(EXTRACT(EPOCH FROM s.restore_lease_expires_at) * 1000)::BIGINT,
          s.restore_attempt_count,
          s.target_node_id,
          c.lifecycle
