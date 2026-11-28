//! Main's Direct remote restore leg, frozen as it rendered at the base commit
//! of the change that added the capture-order rules: the statement
//! `direct_restore_candidates_for_mode` ran for the remote candidates, with
//! the environment-refusal fragment and the snapshot guard
//! (`no_newer_full_snapshot`, or `restore_retry_source_snapshot_is_current`
//! for a retry) as they were then. A rollback runs exactly this, so it's a
//! copy, not a call.
//!
//! `$1` is the sandbox, `$2` the candidate limit, `$3` the substrate kind and
//! `$4` whether the environment gate refuses.

/// A fresh restore's remote leg.
pub(super) const FRESH: &str = r#"WITH latest_snapshot AS (
                 SELECT sn.id AS snapshot_id,
                        storage.storage_connection_string AS storage_uri,
                        COALESCE(sn.source_node_id, s.target_node_id, s.active_node_id) AS source_node_id,
                        sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 JOIN LATERAL (
                     SELECT ssl.storage_connection_string
                     FROM snapshot_storage_locations ssl
                     WHERE ssl.snapshot_id = sn.id
                     ORDER BY ssl.created_at DESC, ssl.id DESC
                     LIMIT 1
                 ) storage ON TRUE
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
                   AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
)
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    latest_snapshot.storage_uri,
                    latest_snapshot.source_node_id,
                    environment_gate.refusal AS environment_refusal
             FROM latest_snapshot
             JOIN nodes n ON n.id IS DISTINCT FROM latest_snapshot.source_node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT CASE
WHEN latest_snapshot.execution_metadata->>'environment' IS NULL THEN NULL
WHEN nss.execution_inventory_status IS NULL THEN 'no_evidence'
WHEN NOT nss.execution_inventory_status = 'complete' THEN 'incomplete'
WHEN nss.environment_refs_received_at IS NULL THEN 'no_evidence'
WHEN NOT (nss.stale_after > now() AND nss.environment_refs_received_at
+ (nss.stale_after - nss.observed_at) > now()) THEN 'stale'
WHEN NOT COALESCE(latest_snapshot.execution_metadata->>'environment' = ANY(nss.usable_environment_refs), FALSE)
THEN 'environment_not_usable'
END AS refusal
             ) environment_gate
             WHERE n.managed_by = $3
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND (($4::BOOLEAN IS TRUE AND latest_snapshot.execution_metadata->>'environment' IS NOT NULL)
                    OR EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND EXISTS (
                              SELECT 1
                              FROM node_runtime_state_formats state_format
                              WHERE state_format.node_id = runtime.node_id
                                AND state_format.runtime = runtime.runtime
                                AND state_format.runtime_version = runtime.runtime_version
                                AND state_format.state_format_name = latest_snapshot.state_format_name
                                AND split_part(state_format.state_format_version, '.', 1)
                                    = split_part(latest_snapshot.state_format_version, '.', 1)
                                AND state_format.can_load IS TRUE
                          )
                   ))
                   -- Component compatibility reads the components array of the
                   -- execution_metadata document written by BeginArtifactBackup.
                   -- Omitted optional keys surface as NULL
                   -- columns: NULL digest/version/format arms are unconstrained,
                   -- and an absent role maps to node_artifact_components'
                   -- role_key ''. A NULL document (legacy rows) means no
                   -- compatibility requirements. Malformed documents fail
                   -- closed: a non-array components value errors the whole
                   -- candidate query rather than silently passing, and a
                   -- component with no kind can never match an installed
                   -- component, so it excludes the node.
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             ORDER BY random()
             LIMIT $2"#;

/// A retry's remote leg.
pub(super) const RETRY: &str = r#"WITH latest_snapshot AS (
                 SELECT sn.id AS snapshot_id,
                        storage.storage_connection_string AS storage_uri,
                        COALESCE(sn.source_node_id, s.target_node_id, s.active_node_id) AS source_node_id,
                        sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 JOIN LATERAL (
                     SELECT ssl.storage_connection_string
                     FROM snapshot_storage_locations ssl
                     WHERE ssl.snapshot_id = sn.id
                     ORDER BY ssl.created_at DESC, ssl.id DESC
                     LIMIT 1
                 ) storage ON TRUE
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
                   AND ((
(s.lifecycle_changed_at IS NULL
OR sn.timestamp < s.lifecycle_changed_at)
AND (
s.placement_changed_at IS NULL
OR s.lifecycle_changed_at IS NULL
OR s.placement_changed_at >= s.lifecycle_changed_at
OR sn.timestamp >= s.placement_changed_at
)
AND (
s.current_local_artifact_id IS NULL
OR (sn.local_artifact_id IS NOT NULL
AND sn.artifact_kind = 'full')
)
)
AND NOT EXISTS (
SELECT 1
FROM snapshots newer
WHERE newer.sandbox_id = s.id
AND newer.type = 'full'
AND newer.timestamp > sn.timestamp
AND (
(s.lifecycle_changed_at IS NULL
OR newer.timestamp < s.lifecycle_changed_at)
AND (
s.placement_changed_at IS NULL
OR s.lifecycle_changed_at IS NULL
OR s.placement_changed_at >= s.lifecycle_changed_at
OR newer.timestamp >= s.placement_changed_at
)
AND (
s.current_local_artifact_id IS NULL
OR (newer.local_artifact_id IS NOT NULL
AND newer.artifact_kind = 'full')
)
)
AND NOT (
sn.local_artifact_id IS NOT NULL
AND newer.state = 'failed'
AND newer.local_artifact_id = sn.local_artifact_id
AND newer.artifact_kind IS NOT DISTINCT FROM sn.artifact_kind
AND COALESCE(newer.artifact_origin, 'suspend') =
COALESCE(sn.artifact_origin, 'suspend')
)
))
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    latest_snapshot.storage_uri,
                    latest_snapshot.source_node_id,
                    environment_gate.refusal AS environment_refusal
             FROM latest_snapshot
             JOIN nodes n ON n.id IS DISTINCT FROM latest_snapshot.source_node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             CROSS JOIN LATERAL (
                 SELECT CASE
WHEN latest_snapshot.execution_metadata->>'environment' IS NULL THEN NULL
WHEN nss.execution_inventory_status IS NULL THEN 'no_evidence'
WHEN NOT nss.execution_inventory_status = 'complete' THEN 'incomplete'
WHEN nss.environment_refs_received_at IS NULL THEN 'no_evidence'
WHEN NOT (nss.stale_after > now() AND nss.environment_refs_received_at
+ (nss.stale_after - nss.observed_at) > now()) THEN 'stale'
WHEN NOT COALESCE(latest_snapshot.execution_metadata->>'environment' = ANY(nss.usable_environment_refs), FALSE)
THEN 'environment_not_usable'
END AS refusal
             ) environment_gate
             WHERE n.managed_by = $3
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND (($4::BOOLEAN IS TRUE AND latest_snapshot.execution_metadata->>'environment' IS NOT NULL)
                    OR EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND EXISTS (
                              SELECT 1
                              FROM node_runtime_state_formats state_format
                              WHERE state_format.node_id = runtime.node_id
                                AND state_format.runtime = runtime.runtime
                                AND state_format.runtime_version = runtime.runtime_version
                                AND state_format.state_format_name = latest_snapshot.state_format_name
                                AND split_part(state_format.state_format_version, '.', 1)
                                    = split_part(latest_snapshot.state_format_version, '.', 1)
                                AND state_format.can_load IS TRUE
                          )
                   ))
                   -- Component compatibility reads the components array of the
                   -- execution_metadata document written by BeginArtifactBackup.
                   -- Omitted optional keys surface as NULL
                   -- columns: NULL digest/version/format arms are unconstrained,
                   -- and an absent role maps to node_artifact_components'
                   -- role_key ''. A NULL document (legacy rows) means no
                   -- compatibility requirements. Malformed documents fail
                   -- closed: a non-array components value errors the whole
                   -- candidate query rather than silently passing, and a
                   -- component with no kind can never match an installed
                   -- component, so it excludes the node.
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    OR environment_gate.refusal IS NULL
               )
             ORDER BY random()
             LIMIT $2"#;
