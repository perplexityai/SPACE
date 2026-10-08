SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Reverting drops the pre-dispatch system publication grants. With the table
-- gone, a ClusterApi release that still consults it denies build-manager's
-- system publications, so roll the API and internal-api releases back together
-- with this migration. The rows are derived from build submissions and carry
-- no fact that exists nowhere else.
DROP TABLE IF EXISTS template_system_publication_grants;
