-- no-transaction
-- squawk-disable-assume-in-transaction
-- The active_node_id arm of the node template-state check; see
-- idx_sandboxes_template_target_node.
-- Single-statement nontransactional migration: use the migrator session's
-- lock/statement timeouts. A failed build can leave an INVALID index; repair it
-- with DROP INDEX CONCURRENTLY before retrying, never silently skip its name.
-- squawk-ignore prefer-robust-stmts,require-lock-timeout,require-statement-timeout
CREATE INDEX CONCURRENTLY idx_sandboxes_template_active_node
    ON sandboxes (template_id, active_node_id);
