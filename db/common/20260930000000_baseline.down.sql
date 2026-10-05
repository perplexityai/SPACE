CREATE TEMP TABLE public_baseline_rollback_guard (value INTEGER);
CREATE TEMP TRIGGER public_baseline_rollback_floor
BEFORE INSERT ON public_baseline_rollback_guard
BEGIN
    SELECT RAISE(ABORT, 'public baseline is a rollback floor; restore a verified backup');
END;
INSERT INTO public_baseline_rollback_guard VALUES (1);
