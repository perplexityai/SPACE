SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The Kubernetes substrate's execution store and node drain's pod-less
-- give-up of the release that added these columns write and read them, so
-- roll those services back first.
ALTER TABLE sandbox_k8s_executions
    -- squawk-ignore ban-drop-column
    DROP COLUMN IF EXISTS pod_create_requested_at,
    -- squawk-ignore ban-drop-column
    DROP COLUMN IF EXISTS pod_create_intent_tracked;
