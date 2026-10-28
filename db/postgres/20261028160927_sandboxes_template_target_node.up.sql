-- no-transaction
-- squawk-disable-assume-in-transaction
-- Node template-state polls check, for each template a node holds, whether any
-- sandbox with that template is targeted at or active on the node. Without an
-- index on template_id each check scans the whole table. Paired with
-- idx_sandboxes_template_active_node for the active_node_id arm of that OR.
-- Single-statement nontransactional migration: use the migrator session's
-- lock/statement timeouts. A failed build can leave an INVALID index; repair it
-- with DROP INDEX CONCURRENTLY before retrying, never silently skip its name.
-- squawk-ignore prefer-robust-stmts,require-lock-timeout,require-statement-timeout
CREATE INDEX CONCURRENTLY idx_sandboxes_template_target_node
    ON sandboxes (template_id, target_node_id);
