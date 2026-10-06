SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Reverting drops the pre-dispatch base grants. With the table gone, a
-- ClusterApi release that still consults it cannot authorize a builder's
-- base pull for an org-specific base, so roll the API and internal-api
-- releases back together with this migration. The rows are derived from
-- build submissions and carry no fact that exists nowhere else.
DROP TABLE IF EXISTS template_build_base_grants;
