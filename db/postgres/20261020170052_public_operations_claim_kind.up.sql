-- no-transaction
-- A dedicated create lane must not scan unrelated queued operations on every
-- claim tick. Keep the active-state and retry-time predicates indexable within
-- each kind, including expired claims that are still marked running.
-- SQLx requires one statement outside a transaction, so session timeouts apply.
-- Deliberately omit IF NOT EXISTS: a failed concurrent build can leave an
-- INVALID index, which must be dropped concurrently before retrying.
-- squawk-ignore transaction-nesting, prefer-robust-stmts, require-lock-timeout, require-statement-timeout
CREATE INDEX CONCURRENTLY idx_public_operations_kind_state_next_attempt
    ON public.public_operations (kind, state, next_attempt_at, requested_at);
