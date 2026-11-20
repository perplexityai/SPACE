-- The node's activation seal: while `sealed`, sandbox-manager admits no new
-- VM activation (cold boot, snapshot restore or resident resume). The service
-- reads this row before it serves, so a seal survives a restart. One row at
-- most; no row means the node was never sealed.
--
-- `generation` is the drain generation of the last seal or cancel, and only
-- moves forward. While `sealed`, no seal under another generation is taken.
-- Once a drain is cancelled (`sealed` false), the row is its tombstone: no seal
-- at or below its generation is taken again, so a drain call that arrives
-- after its own undrain cannot seal the node again, and a generation names one
-- continuous seal.
CREATE TABLE activation_seal (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation INTEGER NOT NULL CHECK (typeof(generation) = 'integer' AND generation > 0),
    sealed INTEGER NOT NULL CHECK (sealed IN (0, 1))
);
