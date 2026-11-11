-- A down migration must not erase an operation that still owns admission.
CREATE TEMP TABLE guest_update_down_guard (unfinished INTEGER CHECK (unfinished = 0));
INSERT INTO guest_update_down_guard SELECT count(*) FROM guest_update_operations WHERE terminal = 0;
DROP TABLE guest_update_down_guard;
DROP TABLE guest_update_operations;
DROP TABLE guest_update_policy;
