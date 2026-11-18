DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM sandbox_block_volumes
        WHERE target_size_gib IS NOT NULL OR (retained AND size_gib != 128)
    ) THEN
        RAISE EXCEPTION 'pending or grown block volumes require compatible controllers';
    END IF;
END
$$;

DROP TRIGGER sandbox_block_volume_growth_lifecycle_delete ON sandboxes;
DROP TRIGGER sandbox_block_volume_growth_lifecycle_update ON sandboxes;
DROP FUNCTION sandbox_block_volume_growth_lifecycle_barrier();

ALTER TABLE sandbox_block_volumes
    DROP CONSTRAINT independent_volume_size,
    ADD CONSTRAINT independent_volume_size CHECK (
        NOT retained OR (size_gib IS NOT NULL AND size_gib = 128)
    ),
    DROP CONSTRAINT sandbox_block_volumes_growth_intent,
    DROP COLUMN backing_growth_ready,
    DROP COLUMN resize_operation_id,
    DROP COLUMN target_size_gib;
