SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The EC2 instance each Kubernetes node runs on, from its Node's AWS
-- providerID, as each registration of the node reports it. A lookup by private
-- DNS name can't tell a terminated instance EC2 has forgotten from a wrong
-- region, account or name, and a same-IP replacement reuses the node id; an
-- instance ID names exactly one instance. NULL means no report: providers
-- other than AWS have no AWS providerID, and older writers don't send one. A
-- writer with no instance ID leaves a recorded one alone.
--
-- Adding a nullable column without a default changes the catalog only; no row
-- is rewritten.
ALTER TABLE node_k8s
    ADD COLUMN instance_id TEXT;

COMMENT ON COLUMN node_k8s.instance_id IS
    'EC2 instance ID from the Kubernetes Node''s AWS providerID; NULL means no report';
