-- The node's activation seal: while `sealed_epoch` is set, sandbox-manager
-- admits no new VM activation (cold boot, snapshot restore or resident
-- resume). The service reads this row before it serves, so a seal survives a
-- restart. One row at most; no row means the node was never sealed.
--
-- An unseal clears `sealed_epoch` and keeps the row, so `sealed_since_ms`
-- still holds the start of the last seal. A new seal starts strictly after
-- it, which makes `(sealed_epoch, sealed_since_ms)` name one continuous seal
-- even across an unseal and a re-seal with the same epoch.
CREATE TABLE activation_seal (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    sealed_epoch INTEGER CHECK (
        sealed_epoch IS NULL
        OR (typeof(sealed_epoch) = 'integer' AND sealed_epoch > 0)
    ),
    -- Wall-clock Unix milliseconds at which the current or last seal began.
    sealed_since_ms INTEGER NOT NULL CHECK (
        typeof(sealed_since_ms) = 'integer' AND sealed_since_ms >= 0
    )
);
