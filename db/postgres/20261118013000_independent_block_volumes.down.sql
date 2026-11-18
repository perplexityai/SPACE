DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sandbox_block_volumes WHERE retained) THEN
        RAISE EXCEPTION 'independent volume records require compatible lifecycle support';
    END IF;
END $$;
ALTER TABLE sandbox_block_volumes
    DROP CONSTRAINT retained_volume_explicit_delete,
    DROP CONSTRAINT unbound_volume_released,
    DROP CONSTRAINT legacy_volume_binding,
    DROP CONSTRAINT volume_binding_pair,
    DROP CONSTRAINT independent_volume_size,
    DROP COLUMN delete_operation_id,
    DROP COLUMN size_gib,
    DROP COLUMN retained,
    ALTER COLUMN sandbox_id SET NOT NULL,
    ALTER COLUMN mount_spec SET NOT NULL;
