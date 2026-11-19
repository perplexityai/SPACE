-- The firewall assignment this node last accepted: one row, replaced in place.
-- No row means no assignment yet; an accepted revision may hold zero rules.
CREATE TABLE firewall_rules_assignment (
    id                     INTEGER PRIMARY KEY CHECK (id = 1),
    node_id                TEXT NOT NULL,
    assignment_generation  INTEGER NOT NULL CHECK (assignment_generation >= 0),
    revision_id            TEXT NOT NULL,
    revision_document      TEXT NOT NULL
);
