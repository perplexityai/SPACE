SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The node drain's v2 engine. One authority per fact:
--
-- * A v2 drain writes the same drain columns a v1 drain does for what both
--   engines have: `drain_phase` (draining, drained, terminating),
--   `drain_mode` (forced or gentle), `drain_started_at` and
--   `drain_last_error`. They stay the only source for whether a node is
--   draining, since when, in which mode, and its last error, whichever
--   engine runs the drain.
-- * `drain_engine` says which engine owns the drain: NULL is the v1 engine,
--   `v2` a drain the admin route started with `engine: v2`. Every v1 writer
--   keyed on the drain phase skips a `v2` row.
-- * `drain_status` holds only the v2 facts no column has: the epoch the node
--   is sealed under (`epoch`, or `gentle_epoch` while a gentle drain hasn't
--   turned forced), whether the drain still waits for an evacuation slot
--   (`waiting`), a pending cancel (`cancel`), the node's last `DrainNode`
--   report (`report`), the terminate fence (`terminate_instance`,
--   `dispatched_at`, `terminated_observed_at`), and a gentle drain's
--   `deadline` and escalation (`escalated`, `escalated_at`). None of these
--   restates a column above.
--
-- Writers bind `drain_engine` through a closed enum and serialize
-- `drain_status` from a typed record, so the application enforces both; the
-- schema adds no constraint for either column.
--
-- Nullable with no default, backfill or index: a row stays NULL until a v2
-- drain starts on its node, so every existing reader behaves as before.
ALTER TABLE nodes
    ADD COLUMN drain_engine text,
    ADD COLUMN drain_status jsonb;
