-- Keeps firewall_rules_changed: Postgres cannot drop an enum value. Nothing may
-- enqueue it while a rollback target predates #1622 (706cab44), whose claim
-- first read the reasons as text; an older claim fails its batch on the value.
SELECT 1;
