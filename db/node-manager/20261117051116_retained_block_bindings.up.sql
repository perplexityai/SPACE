-- No cascading FK: device cleanup authority must outlive execution retirement.
CREATE TABLE retained_block_bindings (
    sandbox_id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL,
    binding BLOB NOT NULL
);
