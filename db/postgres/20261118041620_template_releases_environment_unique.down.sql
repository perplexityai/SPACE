-- Once a later migration drops idx_template_releases_template_name_hash_unique,
-- this is the only unique index covering a release's template and hash. That
-- migration's down must recreate the old index; SQLx runs downs newest first,
-- so it runs before this one.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DROP INDEX template_releases_template_name_hash_environment_key;
