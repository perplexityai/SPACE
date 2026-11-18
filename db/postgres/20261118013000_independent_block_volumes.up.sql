-- Old controllers reconcile only their exact managed_by label. Independent
-- volumes use a versioned label until all rollback targets understand them.
ALTER TABLE sandbox_block_volumes
    ALTER COLUMN sandbox_id DROP NOT NULL,
    ALTER COLUMN mount_spec DROP NOT NULL,
    ADD COLUMN retained BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN size_gib INTEGER,
    ADD COLUMN delete_operation_id UUID,
    ADD CONSTRAINT independent_volume_size CHECK (NOT retained OR (size_gib IS NOT NULL AND size_gib = 128)),
    ADD CONSTRAINT volume_binding_pair CHECK ((sandbox_id IS NULL) = (mount_spec IS NULL)),
    ADD CONSTRAINT legacy_volume_binding CHECK (retained OR sandbox_id IS NOT NULL),
    ADD CONSTRAINT unbound_volume_released CHECK (
        sandbox_id IS NOT NULL OR
        (node_id IS NULL AND checkpoint_id IS NULL AND activation_id IS NULL)
    ),
    -- Fail closed if an old sandbox-cleanup writer tries to delete retained
    -- storage. Only explicit volume deletion records this operation receipt.
    ADD CONSTRAINT retained_volume_explicit_delete CHECK (
        NOT retained OR NOT delete_requested OR delete_operation_id IS NOT NULL
    );
