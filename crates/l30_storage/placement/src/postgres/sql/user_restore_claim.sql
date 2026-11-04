-- User-path wake claim: move one sandbox `suspended` -> `restoring` and mint
-- the lease half of the placement fence ($3, expiring $2 seconds out).
-- Candidate WHERE is byte-identical to the user branch of
-- `begin_restore_lifecycle` in the api substrate store.

WITH candidate AS (
  SELECT id, lifecycle
  FROM sandboxes
  WHERE id = $1
    AND (
      -- the resting shape: a suspended row is the normal claim target
      LOWER(TRIM(lifecycle)) = 'suspended'
      OR (
        -- an already-restoring row is a prior attempt that may be re-driven,
        LOWER(TRIM(lifecycle)) = 'restoring'
        -- but only a markerless one: the drain marker means an evacuation
        -- restore is in flight and the user claim must stay out
        AND evacuation_restore_target IS NULL
        -- and only once its lease has lapsed (or was never minted)
        AND (
          restore_lease_expires_at IS NULL
          OR restore_lease_expires_at <= NOW()
        )
      )
    )
    -- an unexpired lease means another attempt owns the row
    AND (
      restore_lease_expires_at IS NULL
      OR restore_lease_expires_at <= NOW()
    )
    -- a fresh active execution keeps ownership past the lease deadline: an
    -- already-issued node or Kubernetes request may still land on it
    AND NOT EXISTS (
      SELECT 1
      FROM sandbox_k8s_executions e
      WHERE e.sandbox_id = sandboxes.id
        AND e.active
        AND e.state IN ('creating', 'created', 'recovering', 'running')
        AND e.updated_at > NOW() - make_interval(secs => $4)
    )
  FOR UPDATE
)

UPDATE sandboxes s
SET lifecycle = 'restoring',
    -- a suspended row with a drain marker is a clean user takeover: clearing
    -- the marker makes the drain's marker-required claim lose
    evacuation_restore_target = NULL,
    -- Preserve lifecycle_changed_at on a re-drive: restore selection uses the
    -- first restoring transition as its immutable source-window boundary. The
    -- lifecycle trigger sets it for a fresh suspended -> restoring claim, and
    -- the lease protects an active retry from transient recovery.
    restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
    restore_lease_token = $3
FROM candidate c
WHERE s.id = c.id

RETURNING CEIL(EXTRACT(EPOCH FROM s.restore_lease_expires_at) * 1000)::BIGINT,
          s.restore_attempt_count,
          s.target_node_id,
          c.lifecycle
