-- Roll back only after every cloud disk and its attachment is confirmed absent.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM sandbox_block_volumes WHERE phase != 'deleted') THEN
        RAISE EXCEPTION 'retained block volumes require compatible lifecycle support';
    END IF;
END $$;
DROP TABLE sandbox_block_volumes;
