ALTER TABLE sandbox_pool_adoptions ADD COLUMN volume_encryption INTEGER NOT NULL DEFAULT 1 CHECK (volume_encryption IN (0, 1));
