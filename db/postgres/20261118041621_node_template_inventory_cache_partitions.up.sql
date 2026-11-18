SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The node cache partitions holding each published template build. One rootfs
-- digest can be cached once per execution environment: the legacy partition
-- holds unbound builds and builds bound to the initial environment, and every
-- other environment has its own (TemplateCachePartition). node-ctl publishes
-- one build per template name, preferring the legacy partition's, and the
-- row's other columns describe that build, so a twin of it bound to another
-- environment can show up only here, once publishers write this column. The
-- primary key stays (node_id, template_name, content_hash):
-- node_template_components references it, and a key with the partition would
-- need that foreign key rebuilt and a second contract migration.
--
-- Elements are TemplateCachePartition keys: the empty string for the legacy
-- partition, otherwise the environment reference. An empty array reports no
-- ready build in any partition; NULL is no report. The elements have no format
-- CHECK: Postgres evaluates every CHECK on every update of the row, including
-- each publication that changes it, and the writer stores only parsed
-- partitions.
--
-- The array is a placement hint, not evidence that a node holds a build. The
-- publication upsert updates a row only when a compared column changed, and
-- writers that predate this column neither compare nor write it. So while an
-- older internal API serves publications, during a rolling deploy or after a
-- rollback, a row can keep an array that no longer matches the node. Writers
-- that know the column include it in that comparison. Readers may use it to
-- prefer a node; a node that turns out not to hold the build pulls it, as a
-- node does today on a cache miss.
ALTER TABLE node_template_inventory
    ADD COLUMN cache_partitions TEXT[];

COMMENT ON COLUMN node_template_inventory.cache_partitions IS
    'Node cache partitions holding a ready build of this template and digest: the empty string is the legacy partition, otherwise an environment reference; NULL means no report. A placement hint: publishers that predate the column leave it unchanged';
