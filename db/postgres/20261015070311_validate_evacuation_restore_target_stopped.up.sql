SET LOCAL lock_timeout = '1s';
-- Well inside the migration Job's 30-minute deadline, so a slow scan fails
-- with a reported `57014` instead of the Job dying silently and leaving the
-- scan holding its lock against the next attempt.
SET LOCAL statement_timeout = '10min';

ALTER TABLE sandboxes
    VALIDATE CONSTRAINT sandboxes_evacuation_restore_target_check;
