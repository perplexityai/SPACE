-- What the operation an attempt outcome belongs to asked for: its target
-- environment, target state and authorization, beside the sandbox the row
-- already names. `sandbox_runtime_upgrade_outcomes` keeps them too, but its
-- rows go with the sandbox's row, and an attempt outcome's don't, so a retry
-- after the sandbox's row was deleted is still checked against what the
-- operation asked for. A row that has them is final for its attempt: only a
-- release that records attempt outcomes as final writes them, and only while
-- attempt finality is on. A row without them is final for no attempt: a
-- build from before these columns writes one when a rollback finishes, and
-- never reads it as final.
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes ADD COLUMN target_environment TEXT;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes ADD COLUMN target_state TEXT;
ALTER TABLE sandbox_runtime_upgrade_attempt_outcomes ADD COLUMN authorization_id TEXT;
