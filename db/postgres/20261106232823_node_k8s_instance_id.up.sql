SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The EC2 instance each Kubernetes node runs on, from its Node's AWS
-- providerID, as each registration of the node reports it. A lookup by private
-- DNS name can't tell a terminated instance EC2 has forgotten from a wrong
-- region, account or name, and a same-IP replacement reuses the node id; an
-- instance ID names exactly one instance. NULL means no report: Kind and other
-- providers have no AWS providerID, and older writers don't send one. A writer
-- with no instance ID leaves a recorded one alone.
--
-- Adding a nullable column without a default changes the catalog only; no row
-- is rewritten.
ALTER TABLE node_k8s
    ADD COLUMN instance_id TEXT;

-- Each instance a node_k8s row named before a report of another instance
-- replaced it: the old instance and the Kubernetes Node UID the row held then.
-- Written in the transaction that overwrites instance_id, before the overwrite,
-- with the row locked. A first report (NULL to a value), the same instance
-- again, and a UID change alone write none. Rows go with their node_k8s row.
CREATE TABLE node_k8s_superseded_instances (
    id UUID NOT NULL DEFAULT space_uuid_v7(),
    node_id UUID NOT NULL,
    instance_id TEXT NOT NULL,
    k8s_node_uid TEXT,
    superseded_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT node_k8s_superseded_instances_pkey PRIMARY KEY (id),
    CONSTRAINT node_k8s_superseded_instances_node_id_fkey
        FOREIGN KEY (node_id) REFERENCES node_k8s(node_id) ON DELETE CASCADE
);

-- A node's history, and the cascade from node_k8s.
CREATE INDEX node_k8s_superseded_instances_node_id_idx
    ON node_k8s_superseded_instances (node_id, superseded_at);

COMMENT ON COLUMN node_k8s.instance_id IS
    'EC2 instance ID from the Kubernetes Node''s AWS providerID; NULL means no report';
COMMENT ON TABLE node_k8s_superseded_instances IS
    'Instances a node_k8s row named before a different instance was reported for the node';
