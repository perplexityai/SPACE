ALTER TABLE sandbox_block_volumes
    DROP CONSTRAINT independent_volume_size,
    ADD CONSTRAINT independent_volume_size CHECK (
        NOT retained OR (size_gib IS NOT NULL AND size_gib BETWEEN 128 AND 256)
    ),
    ADD COLUMN target_size_gib INTEGER,
    ADD COLUMN resize_operation_id UUID,
    ADD COLUMN backing_growth_ready BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT sandbox_block_volumes_growth_intent CHECK (
        (target_size_gib IS NULL AND NOT backing_growth_ready)
        OR (target_size_gib IS NOT NULL
            AND target_size_gib > size_gib AND target_size_gib <= 256
            AND resize_operation_id IS NOT NULL
            AND retained AND NOT delete_requested AND checkpoint_id IS NULL
            AND (
                (sandbox_id IS NULL AND mount_spec IS NULL AND node_id IS NULL
                    AND initialization = 'fresh' AND NOT ever_exposed)
                OR (sandbox_id IS NOT NULL AND initialization = 'ready'
                    AND ever_exposed AND mount_spec->>'permissions' = 'read_write')
            ))
    );

-- Growth admission holds the sandbox row before publishing intent. Make the
-- reverse edge use that same row lock, including old lifecycle writers, so a
-- checkpoint/teardown cannot strand partially applied device geometry.
CREATE FUNCTION sandbox_block_volume_growth_lifecycle_barrier()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM sandbox_block_volumes
        WHERE sandbox_id = OLD.id AND target_size_gib IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'sandbox volume growth is still pending'
            USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sandbox_block_volume_growth_lifecycle_update
BEFORE UPDATE OF lifecycle ON sandboxes
FOR EACH ROW WHEN (OLD.lifecycle IS DISTINCT FROM NEW.lifecycle)
EXECUTE FUNCTION sandbox_block_volume_growth_lifecycle_barrier();

CREATE TRIGGER sandbox_block_volume_growth_lifecycle_delete
BEFORE DELETE ON sandboxes
FOR EACH ROW
EXECUTE FUNCTION sandbox_block_volume_growth_lifecycle_barrier();
