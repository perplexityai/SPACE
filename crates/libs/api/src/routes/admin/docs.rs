use axum::{
    Json,
    http::{HeaderValue, header},
    response::IntoResponse,
};
use pplx_space_runtime_environment_policy_store::PolicyChangeReason;
use serde_json::{Map, Value, json};

mod cross_org;

const ADMIN_LLMS_TXT: &str = "SPACE Admin API\n\n\
- Admin OpenAPI: /openapi.json on admin.api.space hosts, /admin/api/openapi.json on shared hosts\n\
- Admin API base path: /v1 on admin.api.space hosts, /admin/api/v1 on shared hosts\n\
- SPACE API base path: /v1 on api.space hosts, /api/v1 on shared hosts\n\
- ECW (E2B-compatible) API base path: / on e2b.api.space hosts, /e2b/api on shared hosts\n";

pub(crate) async fn openapi_json() -> Json<Value> {
    Json(admin_openapi_value())
}

pub(crate) async fn llms_txt() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )],
        ADMIN_LLMS_TXT,
    )
}

fn admin_openapi_value() -> Value {
    let mut spec = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "SPACE Admin API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Break-glass and fleet-operation APIs for SPACE operators."
        },
        "servers": [
            { "url": "/v1" },
            {
                "url": "/admin/api/v1",
                "description": "Shared-host admin API root"
            }
        ],
        "security": [
            { "apiKey": [] }
        ],
        "paths": {
            "/nodes": {
                "get": {
                    "summary": "List node inventory and optional diagnostic counts",
                    "description": "Set include_occupancy=false for metadata-only inventory with null occupancy counts. Legacy requests compute counts; sort=drainability always computes diagnostics. Every node also carries runtime_installations and, when recorded, node_group_id and cordoned (the Kubernetes Node's spec.unschedulable from the API's reflector cache). reconcile_trigger is present when the node has a queued state-reconciliation trigger; per-facet work in node_reconcile_queue is not shown.",
                    "parameters": [
                        { "name": "include_occupancy", "in": "query", "schema": { "type": "boolean", "default": true } }
                    ],
                    "responses": {
                        "200": { "description": "Node page" }
                    }
                }
            },
            "/nodes/{node_id}": {
                "get": {
                    "summary": "Get one node and optional diagnostic counts",
                    "description": "Set include_occupancy=false to read metadata without sandbox or snapshot accounting; occupancy counts are then null. The node also carries runtime_installations and, when recorded, node_group_id and cordoned (the Kubernetes Node's spec.unschedulable from the API's reflector cache). reconcile_trigger is present when the node has a queued state-reconciliation trigger; per-facet work in node_reconcile_queue is not shown.",
                    "parameters": [
                        { "name": "include_occupancy", "in": "query", "schema": { "type": "boolean", "default": true } },
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Node" },
                        "404": { "description": "Node not found" }
                    }
                }
            },
            "/nodes/{node_id}:cordon": {
                "post": {
                    "summary": "Mark a node unschedulable for new sandbox placement",
                    "parameters": [
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Cordon accepted" },
                        "409": { "description": "Node transition conflict" },
                        "501": { "description": "Not available on the active substrate" }
                    }
                }
            },
            "/nodes/{node_id}:uncordon": {
                "post": {
                    "summary": "Return a node to normal scheduling",
                    "parameters": [
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Uncordon accepted" },
                        "409": { "description": "Node transition conflict" },
                        "501": { "description": "Not available on the active substrate" }
                    }
                }
            },
            "/nodes/{node_id}:drain": {
                "post": {
                    "summary": "Begin break-glass node drain",
                    "parameters": [
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "requestBody": {
                        "required": false,
                        "description": "Optional drain options. An omitted body means a gentle drain that auto-replaces the instance on termination. Also accepted against a node already draining: gentle -> forced escalates the drain in place and preserves drain_started_at; the same mode leaves the mode alone but still clears drain_last_error; forced -> gentle is refused. A drain's replace_after_terminate is fixed when it starts: on an already-draining node, omit the field to escalate on the existing terms; one that disagrees with the drain in flight is a 409.",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "additionalProperties": false,
                                    "properties": {
                                        "mode": {
                                            "type": "string",
                                            "enum": ["gentle", "forced"],
                                            "default": "gentle",
                                            "description": "gentle: cordon the node and wait for resident sandboxes to leave on their own (auto-suspend on inactivity, user suspend, delete); user calls against them keep working. forced: evacuate every resident started/paused sandbox immediately and reject user calls against them with 409 for the whole drain window. forced also enables the stale-transient sweep, which flips any resident wedged in a non-evacuable transient lifecycle past the cluster's abandon age to failed, unrecoverably. That age is measured from the sandbox's own last lifecycle change, not from the drain, so a resident already wedged longer than it is flipped on the first tick after the write — escalating a long-parked gentle drain has no grace period. Read both node_drain.sandboxes_remaining and node_drain.drain_blockers first: neither is a superset of what the sweep destroys (they differ in both the lifecycles and the node-pinning column they count), so two zeros is the only reassuring answer."
                                        },
                                        "replace_after_terminate": {
                                            "type": "boolean",
                                            "description": "false makes the eventual ASG terminate decrement desired capacity, for a blue/green rollout where the incumbent group must shrink monotonically. Omit the field rather than sending null. On the request that starts a drain, omitting it means true (the ASG auto-replaces the instance). On a node already draining the replacement intent is fixed and cannot be changed: omitting the field escalates on the existing terms, and sending one that disagrees with the drain in flight is a 409 — use :undrain and a fresh :drain to change the terms."
                                        },
                                        "require_undrained": {
                                            "type": "boolean",
                                            "default": false,
                                            "description": "true makes the request start-only: it is a 409 if the node already has any drain phase, checked under the same row lock as the write, so it can never escalate or repeat a drain somebody else started. For automation that starts drains; leave it false to escalate a drain in place."
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Drain accepted, escalated in place, or already in the requested mode" },
                        "409": { "description": "Node transition conflict: the node's drain_phase does not accept :drain, the request asked to downgrade an in-flight forced drain to gentle, it carried a replace_after_terminate disagreeing with the drain already in flight, or it set require_undrained and the node already has a drain phase" },
                        "501": { "description": "Not available on the active substrate" }
                    }
                }
            },
            "/nodes/{node_id}:undrain": {
                "post": {
                    "summary": "Cancel a draining or drained node state",
                    "parameters": [
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Undrain accepted" },
                        "409": { "description": "Node transition conflict" }
                    }
                }
            },
            "/nodes/{node_id}:terminate": {
                "post": {
                    "summary": "Terminate a fully drained node",
                    "parameters": [
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Terminate accepted" },
                        "409": { "description": "Node transition conflict" },
                        "501": { "description": "Not available on the active substrate" }
                    }
                }
            },
            "/nodes/{node_id}:force-upload-local-snapshots": {
                "post": {
                    "summary": "Trigger local snapshot upload repair for one node",
                    "parameters": [
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Repair trigger result" },
                        "409": { "description": "Node transition conflict" },
                        "501": { "description": "Not available on the active substrate" }
                    }
                }
            },
            "/operations": {
                "get": {
                    "summary": "List public operations across tenants for operator diagnostics",
                    "description": "actor_user_id is the user an operation runs as; for an admin sandbox create that is the sandbox owner. requested_by_user_id is set only on create_sandbox operations an administrator submitted on the owner's behalf while SPACE_ADMIN_CREATE_REQUESTED_BY_ENABLED was on, naming that administrator; it is null on every other operation. actor_email is the email actor_user_id's user has when the operation is read, so it follows a change of email; it is null when the operation has no actor, which includes once that user is deleted, since the delete also clears actor_user_id. It is not the actor_email of an admin audit event, which is a never-null copy of the actor's email taken when the request ran.",
                    "parameters": [
                        { "name": "org_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } },
                        { "name": "state", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "kind", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "target_type", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "target_id", "in": "query", "required": false, "schema": { "type": "string" }, "description": "Matches target_id exactly as given. When the value is also a sandbox id, as sbx_<uuid> or a bare UUID, it matches the other form too, so fork operations are found whichever form they recorded their source in." },
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 250 } }
                    ],
                    "responses": {
                        "200": { "description": "Operation list" },
                        "400": { "description": "Invalid filter" }
                    }
                }
            },
            "/operations/{operation_id}": {
                "get": {
                    "summary": "Get one public operation by id",
                    "description": "Returns the same operation shape as the list, including actor_email.",
                    "parameters": [
                        { "name": "operation_id", "in": "path", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "200": { "description": "Operation" },
                        "404": { "description": "Operation not found" }
                    }
                }
            },
            "/warm-pools": {
                "get": {
                    "summary": "List cluster warm-pool configs",
                    "responses": {
                        "200": { "description": "Warm-pool config list" }
                    }
                },
                "put": {
                    "summary": "Create or update a cluster warm-pool config",
                    "responses": {
                        "200": { "description": "Warm-pool config" },
                        "400": { "description": "Invalid config" },
                        "404": { "description": "Template release not found" }
                    }
                }
            },
            "/warm-pools/{pool_id}": {
                "delete": {
                    "summary": "Disable a cluster warm-pool config",
                    "parameters": [
                        { "name": "pool_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Disabled warm-pool config" },
                        "404": { "description": "Warm-pool config not found" }
                    }
                }
            },
            "/cluster": {
                "get": {
                    "summary": "Get cluster ownership metadata",
                    "responses": {
                        "200": { "description": "Cluster" }
                    }
                },
                "patch": {
                    "summary": "Update cluster ownership metadata",
                    "responses": {
                        "200": { "description": "Cluster" },
                        "403": { "description": "Caller is not a managing organization admin" }
                    }
                }
            },
            "/orgs": {
                "get": {
                    "summary": "List organizations visible to the caller",
                    "responses": {
                        "200": { "description": "Organization list" }
                    }
                },
                "post": {
                    "summary": "Create an organization",
                    "description": "theme_primary and theme_secondary must be #rgb or #rrggbb hex colors, and logo_url an absolute https URL without credentials (http only on localhost, 127.0.0.1 or [::1]).",
                    "responses": {
                        "201": { "description": "Organization" },
                        "400": { "description": "Invalid branding value; field names the request field" },
                        "403": { "description": "Caller is not a managing organization admin" }
                    }
                }
            },
            "/orgs/{org_id}": {
                "get": {
                    "summary": "Get an organization",
                    "responses": {
                        "200": { "description": "Organization" },
                        "404": { "description": "Organization not found" }
                    }
                },
                "patch": {
                    "summary": "Update an organization",
                    "description": "theme_primary and theme_secondary must be #rgb or #rrggbb hex colors, and logo_url an absolute https URL without credentials (http only on localhost, 127.0.0.1 or [::1]). Only a value that differs from the stored one is checked; null clears a field.",
                    "responses": {
                        "200": { "description": "Organization" },
                        "400": { "description": "Invalid branding value; field names the request field" },
                        "403": { "description": "Caller cannot manage the organization" }
                    }
                },
                "delete": {
                    "summary": "Delete an organization",
                    "responses": {
                        "204": { "description": "Deleted" },
                        "403": { "description": "Caller is not a managing organization admin, or the organization is the managing organization" },
                        "409": { "description": "Not deleted: rows still reference the organization. reason is org_has_users while it has users (delete them or move them to another organization); org_has_sandboxes while it has sandboxes, including deleted ones whose records are kept for retention; org_has_archived_sandboxes while it has archived sandboxes; org_has_templates while it owns a template that is not deleted; org_has_retained_deleted_templates when every template it owns is deleted but retention still keeps the records, with retained_until naming about when the last goes, omitted while a node still caches one (retry later); org_has_orphaned_snapshot_storage when it has no sandboxes but its master key still encrypts snapshot storage locations its deleted snapshots left behind, which the snapshot storage sweeper does not remove; org_has_node_groups while it owns node groups (release each with :assign-org and a null org_id, or delete it once its nodes are relabeled or retired); or org_manages_cluster when it became the managing organization during the request. When several remain, the next delete names the next. API keys, credentials and operations are deleted with the organization, and its child organizations are detached (their parent_id becomes null)" }
                    }
                }
            },
            "/orgs/{org_id}/users": {
                "get": {
                    "summary": "List users in an organization",
                    "responses": {
                        "200": { "description": "User list" }
                    }
                },
                "post": {
                    "summary": "Create a user in an organization",
                    "responses": {
                        "201": { "description": "User" },
                        "403": { "description": "Caller cannot manage the organization" },
                        "409": { "description": "Another user has the email; reason is email_in_use" }
                    }
                }
            },
            "/users/{user_id}": {
                "get": {
                    "summary": "Get a user",
                    "responses": {
                        "200": { "description": "User" },
                        "404": { "description": "User not found" }
                    }
                },
                "delete": {
                    "summary": "Delete a user",
                    "responses": {
                        "204": { "description": "Deleted" },
                        "409": { "description": "Not deleted. reason is self_mutation when the caller targeted itself, user_owns_sandboxes when the user still owns sandboxes, user_owns_retained_deleted_sandboxes when its only sandboxes are deleted ones whose records are kept for retention until retained_until (retry after that), or user_owns_archived_sandboxes when it still owns archived sandboxes" }
                    }
                }
            },
            "/users/{user_id}/role": {
                "put": {
                    "summary": "Update a user's role",
                    "responses": {
                        "200": { "description": "User" },
                        "409": { "description": "Caller attempted to change its own role" }
                    }
                }
            }
        },
        "components": {
            "securitySchemes": {
                "apiKey": {
                    "type": "http",
                    "scheme": "bearer"
                }
            }
        }
    });
    let paths = spec
        .get_mut("paths")
        .and_then(Value::as_object_mut)
        .expect("paths should be an object");
    insert_node_maintenance_paths(paths);
    insert_template_admin_paths(paths);
    insert_warm_pool_policy_paths(paths);
    insert_org_master_key_paths(paths);
    insert_api_key_paths(paths);
    insert_admin_sandbox_create_paths(paths);
    insert_admin_sandbox_action_paths(paths);
    insert_identity_profile_paths(paths);
    insert_audit_event_paths(paths);
    insert_debug_paths(paths);
    insert_runtime_environment_paths(paths);
    cross_org::insert_admin_sandbox_session_paths(paths);
    spec
}

fn insert_admin_sandbox_action_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/sandboxes/{sandbox_id}:force-{action}".to_string(),
        json!({
            "post": {
                "summary": "Enqueue an audited break-glass sandbox lifecycle operation",
                "description": "Runs under the sandbox's owning organization. rollback takes the public :rollback body ({snapshot_name, auto_restart?}), part of the idempotency fingerprint, and refuses with 404 unless the rollback hidden operation is enabled for that organization; other actions ignore the body.",
                "parameters": [
                    { "name": "sandbox_id", "in": "path", "required": true, "description": "sbx_<uuid>, a dashed or undashed UUID in either case, or the sandbox pod name (space-sbx-<undashed uuid>, optionally with its -g<generation> suffix); surrounding whitespace is ignored", "schema": { "type": "string" } },
                    { "name": "action", "in": "path", "required": true, "schema": { "type": "string", "enum": ["start", "stop", "pause", "resume", "suspend", "restore", "wake", "kill", "rollback", "delete"] } },
                    { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string" } }
                ],
                "responses": {
                    "202": { "description": "Operation enqueued" },
                    "400": { "description": "Invalid sandbox id, action, rollback body or idempotency key" },
                    "404": { "description": "Sandbox not found, or rollback not enabled for its organization" },
                    "409": { "description": "Idempotency conflict" }
                }
            }
        }),
    );
}

fn insert_api_key_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/orgs/{org_id}/api-keys".to_string(),
        json!({
            "get": {
                "summary": "List organization API keys without secret material",
                "description": "Any organization's keys, newest first. Revoked keys are listed, with revoked_at, only with include_revoked=true. last_used_at is when the key last authenticated through the API, recorded at most once every five minutes; data-plane uses are not recorded.",
                "parameters": [
                    { "name": "include_revoked", "in": "query", "required": false, "schema": { "type": "boolean", "default": false } }
                ],
                "responses": {
                    "200": { "description": "API key metadata list" },
                    "404": { "description": "Organization not found" }
                }
            },
            "post": {
                "summary": "Create an organization API key",
                "description": "Creates a key for any organization and returns the raw secret exactly once. The key calls only the allowed_surfaces APIs and acts as user_id, a user of the organization, or the organization's oldest user when omitted; that user is stored as created_by. The audit event names the calling admin, the organization, the key and its user, never the secret.",
                "responses": {
                    "201": { "description": "API key metadata and one-time secret" },
                    "400": { "description": "Invalid name or API surfaces, or user_id is not a user of the organization" },
                    "403": { "description": "Caller is not a managing organization admin" },
                    "404": { "description": "Organization not found" },
                    "409": { "description": "The organization has no users; create one first" }
                }
            }
        }),
    );
    paths.insert(
        "/orgs/{org_id}/api-keys/{api_key_id}".to_string(),
        json!({
            "delete": {
                "summary": "Revoke an organization API key",
                "description": "The key stops authenticating at once and is listed with revoked_at when include_revoked=true. Revoking a revoked key succeeds and keeps its first revocation time.",
                "responses": {
                    "204": { "description": "Revoked" },
                    "404": { "description": "API key not found" }
                }
            }
        }),
    );
}

fn insert_node_maintenance_paths(paths: &mut Map<String, Value>) {
    paths.insert("/nodes/{node_id}/sandboxes".to_string(), json!({
                "get": {
                    "summary": "List active and target sandbox placements for one node",
                    "description": "At most 100 placement rows, ordered by sandbox UUID and deduplicated across active and target associations. No durability accounting or live census. Use next_after as the after cursor; null means the last page. Concurrent placement changes may affect pagination.",
                    "parameters": [
                        { "name": "node_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } },
                        { "name": "after", "in": "query", "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Object containing sandboxes (id, lifecycle, active_node_id, target_node_id) and nullable next_after cursor" },
                        "400": { "description": "Invalid UUID or cursor" },
                        "403": { "description": "Managing organization administrator required" },
                        "404": { "description": "Node not found" }
                    }
                }
            }));

    if let Some(nodes) = paths.get_mut("/nodes").and_then(Value::as_object_mut) {
        nodes.insert(
            "post".to_string(),
            json!({
                "summary": "Register a direct/self-managed node",
                "responses": {
                    "201": { "description": "Node registered" },
                    "400": { "description": "Invalid node registration" },
                    "501": { "description": "Not available on the active substrate" }
                }
            }),
        );
    }
    if let Some(node) = paths
        .get_mut("/nodes/{node_id}")
        .and_then(Value::as_object_mut)
    {
        node.insert(
            "patch".to_string(),
            json!({
                "summary": "Update a direct/self-managed node registration",
                "responses": {
                    "200": { "description": "Node" },
                    "404": { "description": "Node not found" },
                    "501": { "description": "Not available on the active substrate" }
                }
            }),
        );
        node.insert(
            "delete".to_string(),
            json!({
                "summary": "Tombstone a direct/self-managed node registration",
                "responses": {
                    "204": { "description": "Deleted" },
                    "404": { "description": "Node not found" },
                    "501": { "description": "Not available on the active substrate" }
                }
            }),
        );
    }
    for (path, summary, response) in [
        (
            "/nodes:refresh-status",
            "Enqueue node status, sandbox inventory, and template inventory refresh",
            "Refresh enqueue summary",
        ),
        (
            "/nodes:reconcile",
            "Enqueue selected node reconciliation facets",
            "Reconcile enqueue summary",
        ),
        (
            "/nodes/{node_id}/network-policy:reconcile",
            "Enqueue network-policy reconciliation on one node",
            "Network-policy reconcile enqueue result",
        ),
    ] {
        paths.insert(
            path.to_string(),
            json!({
                "post": {
                    "summary": summary,
                    "responses": {
                        "200": { "description": response },
                        "400": { "description": "Invalid reconcile request" },
                        "404": { "description": "Node not found" }
                    }
                }
            }),
        );
    }
    paths.insert(
        "/nodes/{node_id}/health".to_string(),
        json!({
            "get": {
                "summary": "Read the latest cached node health publication",
                "responses": {
                    "200": { "description": "Node health" },
                    "404": { "description": "Node not found" }
                }
            }
        }),
    );
    paths.insert(
        "/nodes/templates".to_string(),
        json!({
            "get": {
                "summary": "List cached template inventory per node",
                "responses": {
                    "200": { "description": "Node template inventory list" }
                }
            }
        }),
    );
    paths.insert(
        "/nodes/{node_id}/template-pulls".to_string(),
        json!({
            "post": {
                "summary": "Pull a template build from another node into a direct/self-managed node",
                "responses": {
                    "200": { "description": "Template pull result" },
                    "404": { "description": "Node not found" },
                    "501": { "description": "Not available on the active substrate" }
                }
            }
        }),
    );
    paths.insert(
        "/nodes/{node_id}/templates/{template_name}".to_string(),
        json!({
            "delete": {
                "summary": "Delete a template or template build from one direct/self-managed node",
                "responses": {
                    "204": { "description": "Deleted" },
                    "404": { "description": "Node or template not found" },
                    "501": { "description": "Not available on the active substrate" }
                }
            }
        }),
    );
}

fn insert_template_admin_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/system-templates/{template}/builds".to_string(),
        json!({
            "post": {
                "summary": "Build seed or code-runtime using the recipe embedded in the deployed API",
                "description": "Requires a managing-organization administrator. Publishes an immutable digest; publish_tags explicitly assigns mutable tags. For code-runtime, base defaults to seed:latest and is resolved to an exact digest before dispatch. Poll /templates/{template}/builds/{job_id} until succeeded, failed, or publish_failed.",
                "parameters": [
                    { "name": "template", "in": "path", "required": true, "schema": { "type": "string", "enum": ["seed", "code-runtime"] } },
                    { "name": "Idempotency-Key", "in": "header", "required": false, "schema": { "type": "string" }, "description": "Optional. A repeat with the same key and body within 24 hours returns the original job instead of starting another; the same key with a different body answers 409. A repeat answers a retryable 409 while the first request may still be submitting. It answers a non-retryable 409 once the API has recorded that the first request failed without proof that the builder did not take the build, and in any case 15 minutes after the key was reserved; its reserved_at detail names when the key was reserved, and job_id the job when the builder named one. The build may have started and may still be running, so check Template Releases (/template-releases) or the build's status before submitting again with a new key. The key is freed for a retry only when the builder certainly did not take the build. Send the header only once every API replica runs a release that honours it: older ones ignore it" }
                ],
                "requestBody": {
                    "required": true,
                    "content": { "application/json": { "schema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "base": { "type": ["string", "null"], "description": "Seed reference for code-runtime only; defaults to seed:latest" },
                            "force": { "type": "boolean", "default": false, "description": "Accepted for compatibility and ignored" },
                            "publish_tags": { "type": "array", "items": { "type": "string" }, "default": [] }
                        }
                    }, "example": { "publish_tags": ["latest"] } } }
                },
                "responses": {
                    "202": { "description": "Build accepted", "content": { "application/json": { "schema": {
                        "type": "object", "required": ["job_id", "template_name"],
                        "properties": { "job_id": { "type": "string" }, "template_name": { "type": "string" } }
                    } } } },
                    "400": { "description": "Unknown system template, malformed JSON, unsupported base selection, or invalid tag" },
                    "422": { "description": "Invalid JSON field values, malformed base reference, or unknown request fields", "content": {
                        "text/plain": { "schema": { "type": "string" } }
                    } },
                    "403": { "description": "Caller is not a managing-organization administrator" },
                    "404": { "description": "Seed base release unavailable" },
                    "409": { "description": "Template belongs to another organization or is deleting or deleted, or Idempotency-Key was already used with a different body, or its first build is still being submitted (retryable), or its outcome is unknown (see Idempotency-Key)" },
                    "503": { "description": "Builder unavailable" }
                }
            }
        }),
    );
    paths.insert(
        "/templates".to_string(),
        json!({
            "get": {
                "summary": "List templates visible to the admin caller",
                "responses": { "200": { "description": "Template list" } }
            },
            "post": {
                "summary": "Create or upsert a template definition",
                "responses": {
                    "201": { "description": "Template" },
                    "400": { "description": "Invalid template" }
                }
            }
        }),
    );
    paths.insert(
        "/templates/{template_name}".to_string(),
        json!({
            "get": {
                "summary": "Get a template definition",
                "responses": {
                    "200": { "description": "Template" },
                    "404": { "description": "Template not found" }
                }
            },
            "patch": {
                "summary": "Update template metadata or default network policy",
                "responses": {
                    "200": { "description": "Template" },
                    "409": { "description": "Generation mismatch" }
                }
            },
            "delete": {
                "summary": "Enqueue template deletion and reconcile node-local copies",
                "parameters": [
                    { "name": "template_name", "in": "path", "required": true, "schema": { "type": "string" } },
                    { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string" } }
                ],
                "responses": {
                    "202": { "description": "Delete operation enqueued" },
                    "400": { "description": "Invalid idempotency key" },
                    "404": { "description": "Template not found" },
                    "409": { "description": "Idempotency conflict" }
                }
            }
        }),
    );
    paths.insert(
        "/templates/{template_name}/builds".to_string(),
        json!({
            "get": {
                "summary": "List a template's recent build jobs",
                "description": "Newest first. Each entry carries the job's recorded builder and created_at, and the state, content_hash (on success) and error (on failure) its builder reports now; when the builder cannot report the job, state is null and status_error says why.",
                "parameters": [
                    { "name": "template_name", "in": "path", "required": true, "schema": { "type": "string" } },
                    { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 50, "default": 20 } }
                ],
                "responses": {
                    "200": { "description": "Build jobs; empty for a template with none" },
                    "400": { "description": "Invalid limit" },
                    "404": { "description": "Template not found" }
                }
            },
            "post": {
                "summary": "Start a template build through the selected builder node",
                "parameters": [
                    { "name": "template_name", "in": "path", "required": true, "schema": { "type": "string" } }
                ],
                "responses": {
                    "202": { "description": "Build job accepted" },
                    "400": { "description": "Invalid build recipe" },
                    "409": { "description": "Template is deleting or deleted" },
                    "503": { "description": "Builder unavailable" }
                }
            }
        }),
    );
    paths.insert(
        "/templates/{template_name}/builds/{job_id}".to_string(),
        json!({
            "get": {
                "summary": "Get template build job status",
                "parameters": [
                    { "name": "template_name", "in": "path", "required": true, "schema": { "type": "string" } },
                    { "name": "job_id", "in": "path", "required": true, "schema": { "type": "string" } }
                ],
                "responses": {
                    "200": { "description": "Build job status" },
                    "404": { "description": "Build job not found" },
                    "503": { "description": "Builder unavailable" }
                }
            }
        }),
    );
    paths.insert(
        "/templates/{template_name}/base-snapshot-policy".to_string(),
        json!({
            "get": {
                "summary": "Get desired base-snapshot policy for a template",
                "responses": { "200": { "description": "Base-snapshot policy" } }
            },
            "patch": {
                "summary": "Update desired base-snapshot policy for a template",
                "responses": { "200": { "description": "Base-snapshot policy" } }
            }
        }),
    );
    paths.insert(
        "/templates/{template_name}/base-snapshot-policy:reconcile".to_string(),
        json!({
            "post": {
                "summary": "Enqueue base-snapshot policy reconciliation for a template",
                "responses": { "202": { "description": "Reconcile queued" } }
            }
        }),
    );
    paths.insert(
        "/templates/{template_name}/pool".to_string(),
        json!({
            "get": {
                "summary": "Get template warm-pool policy and observed status",
                "responses": { "200": { "description": "Template pool status" } }
            },
            "post": {
                "summary": "Create or update a template warm-pool policy",
                "responses": { "200": { "description": "Template pool policy" } }
            },
            "delete": {
                "summary": "Delete a template warm-pool policy",
                "responses": { "204": { "description": "Deleted" } }
            }
        }),
    );
    paths.insert(
        "/template-releases".to_string(),
        json!({
            "get": {
                "summary": "List template releases and tags",
                "responses": { "200": { "description": "Template release list" } }
            }
        }),
    );
    paths.insert(
        "/template-releases/{release_id}".to_string(),
        json!({
            "delete": {
                "summary": "Delete a template release",
                "responses": {
                    "204": { "description": "Deleted" },
                    "409": { "description": "Release is still referenced" }
                }
            }
        }),
    );
    paths.insert(
        "/template-releases/{release_id}/tag".to_string(),
        json!({
            "put": {
                "summary": "Move or create a template release tag",
                "description": "Omit expected_current_release_id to move the tag unconditionally. Set it to a release id to move the tag only while it points at that release, or to null to create the tag only if it does not exist yet; the check and the move are one transaction. The admin audit log entry for the move names the release the tag pointed at before it (previous_release_id, empty when the move created the tag).",
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["tag"],
                                "properties": {
                                    "tag": { "type": "string" },
                                    "expected_current_release_id": { "type": ["string", "null"], "format": "uuid", "description": "Compare-and-set precondition on the tag's current release; null requires that the tag not exist" }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": { "description": "Template release" },
                    "404": { "description": "Release not found or its template is not present" },
                    "409": { "description": "The tag changed concurrently; read it again and retry" },
                    "412": { "description": "The tag does not point at expected_current_release_id. details carries current_release_id when the tag exists" }
                }
            }
        }),
    );
    paths.insert(
        "/template-releases/{release_id}/tags/{tag}".to_string(),
        json!({
            "delete": {
                "summary": "Delete one template release tag",
                "responses": { "204": { "description": "Deleted" } }
            }
        }),
    );
}

fn insert_warm_pool_policy_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/warm-pool-policies".to_string(),
        json!({
            "get": {
                "summary": "List default warm-pool policies",
                "responses": { "200": { "description": "Warm-pool policy list" } }
            },
            "put": {
                "summary": "Create or update a default warm-pool policy",
                "responses": { "200": { "description": "Warm-pool policy" } }
            }
        }),
    );
    paths.insert(
        "/warm-pool-policy-capabilities".to_string(),
        json!({
            "get": {
                "summary": "Get configured warm-pool policy capabilities",
                "responses": { "200": { "description": "Warm-pool policy capabilities" } }
            }
        }),
    );
}

fn insert_admin_sandbox_create_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/sandboxes".to_string(),
        json!({
            "get": {
                "summary": "List sandboxes across every organization",
                "description": "Newest first by created_at, then id. Without cursor the response is a bare array of at most limit sandboxes; with no filter, it is followed by the sandboxes nodes report that have no sandbox row. Unknown parameters are then ignored, as before paging existed. Pass cursor, empty for the first page, to page: the response is then {sandboxes, next_cursor}, with next_cursor null on the last page. The limit and the estimated byte budget apply to the rows from the database, and the first such row is kept even when it is larger than the budget. The rows nodes report without a database row are all appended to the first unfiltered page, outside those bounds. The estimate does not count each row's network policy. With cursor, an unknown parameter answers 400. Each page reads the current matching rows strictly below the supplied (created_at, id) position. Paging is not a snapshot: concurrent changes can affect membership. Keep the filters unchanged between pages. Filters, org_id included, combine and leave out the node-reported rows. A filter parameter given empty, except org_id, is unset. A malformed parameter answers 400 with an error message.",
                "parameters": [
                    { "name": "limit", "in": "query", "required": false, "description": "1 to 5000, default 1000", "schema": { "type": "integer" } },
                    { "name": "cursor", "in": "query", "required": false, "description": "Opaque; empty for the first page, then the previous page's next_cursor", "schema": { "type": "string" } },
                    { "name": "org_id", "in": "query", "required": false, "schema": { "type": "string" } },
                    { "name": "lifecycle", "in": "query", "required": false, "description": "Comma-separated values of the stored lifecycle, including deleted and cleanup_required. No response field shows the stored lifecycle: lifecycle and status report the node's observed status when there is one, so a row can show another value.", "schema": { "type": "string" } },
                    { "name": "template", "in": "query", "required": false, "description": "The sandbox row's own template. A row without one does not match, although the response's template, like the spaced-version count, falls back to the template its node reports.", "schema": { "type": "string" } },
                    { "name": "node_id", "in": "query", "required": false, "description": "The node the response reports as active_node_id: a sandbox's current Direct route node when it has a route, whatever its row records, else the node its row records", "schema": { "type": "string", "format": "uuid" } },
                    { "name": "assigned", "in": "query", "required": false, "description": "true for sandboxes the response reports with an active_node_id, false for those without; false cannot be combined with node_id. Like every filter, it lists sandbox rows only, so false leaves out the node-reported pool slots and observed-local sandboxes that the unfiltered list appends, although they report no active_node_id", "schema": { "type": "boolean" } },
                    { "name": "user_id", "in": "query", "required": false, "schema": { "type": "string" } },
                    { "name": "created_after", "in": "query", "required": false, "description": "RFC 3339; exclusive", "schema": { "type": "string" } },
                    { "name": "created_before", "in": "query", "required": false, "description": "RFC 3339; exclusive, later than created_after", "schema": { "type": "string" } },
                    { "name": "prefix", "in": "query", "required": false, "description": "Case-sensitive name prefix, or an ID prefix with optional sbx_ and dashes where a UUID has them. Like every filter, it searches sandbox rows only: the node-reported pool slots and observed-local sandboxes never match, whatever their ID", "schema": { "type": "string" } }
                ],
                "responses": {
                    "200": { "description": "Sandbox array, or {sandboxes, next_cursor} when cursor is set" },
                    "400": { "description": "Invalid cursor or filter" }
                }
            },
            "post": {
                "summary": "Enqueue a sandbox create under an arbitrary organization ({org_id, user_id?, sandbox})",
                "parameters": [
                    { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string" } }
                ],
                "responses": {
                    "202": { "description": "Create operation enqueued" },
                    "400": { "description": "Owner user missing or not in the organization" },
                    "404": { "description": "Organization not found" },
                    "409": { "description": "Idempotency conflict" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/spaced-versions".to_string(),
        json!({
            "get": {
                "summary": "Count sandboxes by template, spaced version and status",
                "description": "The tally of the full sandbox list. Every row whose status is not deleted is grouped by the fields the list response shows for it: template, spaced_version (null before its node reports one), status (the node's observed status, else the stored lifecycle) and resource_type. Without org_id this includes the rows nodes report with no sandbox row, such as warm-pool slots, as the unscoped list does. Unknown parameters answer 400.",
                "parameters": [
                    { "name": "org_id", "in": "query", "required": false, "schema": { "type": "string" } }
                ],
                "responses": {
                    "200": { "description": "Object with counts (template, spaced_version, status, resource_type, count)" },
                    "400": { "description": "Invalid org_id or unknown parameter" }
                }
            }
        }),
    );
    paths.insert(
        "/sandboxes/{sandbox_id}".to_string(),
        json!({
            "get": {
                "summary": "Get a sandbox in any organization",
                "parameters": [
                    { "name": "sandbox_id", "in": "path", "required": true, "description": "sbx_<uuid>, a dashed or undashed UUID in either case, or the sandbox pod name (space-sbx-<undashed uuid>, optionally with its -g<generation> suffix); surrounding whitespace is ignored", "schema": { "type": "string" } }
                ],
                "responses": {
                    "200": { "description": "Sandbox" },
                    "400": { "description": "Invalid sandbox id" },
                    "404": { "description": "Sandbox not found" }
                }
            },
            "patch": {
                "summary": "Enqueue a sandbox update (network policy, auto-lifecycle, metadata) under the sandbox's owning organization",
                "parameters": [
                    { "name": "sandbox_id", "in": "path", "required": true, "description": "sbx_<uuid>, a dashed or undashed UUID in either case, or the sandbox pod name (space-sbx-<undashed uuid>, optionally with its -g<generation> suffix); surrounding whitespace is ignored", "schema": { "type": "string" } },
                    { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string" } }
                ],
                "responses": {
                    "202": { "description": "Update operation enqueued" },
                    "400": { "description": "Invalid sandbox id, immutable resource, or idempotency key" },
                    "404": { "description": "Sandbox not found" },
                    "409": { "description": "Idempotency conflict" }
                }
            }
        }),
    );
}

fn insert_org_master_key_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/orgs/{org_id}/master-key".to_string(),
        json!({
            "get": {
                "summary": "Get the organization's active master key (null when unset)",
                "responses": {
                    "200": { "description": "Active OrgMasterKey or null" },
                    "404": { "description": "Organization not found" }
                }
            }
        }),
    );
}

fn insert_identity_profile_paths(paths: &mut Map<String, Value>) {
    if let Some(user) = paths
        .get_mut("/users/{user_id}")
        .and_then(Value::as_object_mut)
    {
        user.insert(
            "patch".to_string(),
            json!({
                "summary": "Update a user's profile",
                "responses": {
                    "200": { "description": "User" },
                    "403": { "description": "Caller cannot update this user" },
                    "404": { "description": "User not found" },
                    "409": { "description": "Another user has the email; reason is email_in_use" }
                }
            }),
        );
    }
}

fn insert_debug_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/debug/sandbox/{sandbox_id}".to_string(),
        json!({
                "get": {
                    "summary": "Read one sandbox's control-plane diagnostics",
                    "description": "Requires a managing-organization administrator. Every section is present; a section whose source failed is { error, data: null }. resources.env lists the sandbox's environment variable names, not values: each name maps to null.",
                    "parameters": [
                        { "name": "sandbox_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } }
                    ],
                    "responses": {
                        "200": { "description": "Sandbox diagnostics" },
                        "403": { "description": "Caller is not a managing-organization administrator" },
                        "404": { "description": "Sandbox not found" }
                    }
                }
            }),
    );
}

fn insert_audit_event_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/audit-events".to_string(),
        json!({
            "get": {
                "summary": "List persisted admin request audit events",
                "description": "Every admin write, and the reads that name a target (the sandbox snapshot list). target_org_id, target_type (api_key or sandbox), target_id and target_user_id name what the request acted on once it resolved; they are null otherwise.",
                "parameters": [
                    { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 250 } }
                ],
                "responses": {
                    "200": { "description": "Audit event list" },
                    "400": { "description": "Invalid pagination limit" },
                    "403": { "description": "Caller is not a managing organization admin" }
                }
            }
        }),
    );
}

fn insert_runtime_environment_paths(paths: &mut Map<String, Value>) {
    let environment_ref = json!({
        "type": "string",
        "pattern": "^(sha256|blake3):[0-9a-f]{64}$",
        "description": "Execution environment reference"
    });
    let reason = json!({
        "type": "string",
        "minLength": 1,
        "maxLength": PolicyChangeReason::MAX_CHARS,
        "description": "Why the policy changed. Recorded with the caller on the policy row, and on the environment's state row when the write changes it."
    });
    let if_match = json!({
        "name": "If-Match",
        "in": "header",
        "required": false,
        "schema": { "type": "string", "pattern": "^\"?[0-9]+\"?$" },
        "description": "The policy generation the caller last read, optionally quoted. The write applies only at that generation, compared under the policy row lock; absent, it's unconditional."
    });
    paths.insert(
        "/runtime-environments".to_string(),
        json!({
            "get": {
                "summary": "Get the cluster execution environment policy",
                "description": "The generation, the default environment, the legacy unbound permission, and every environment with a state, each with who last changed it and why. A null default or legacy permission means nodes keep their configured one. updated_at is trigger-maintained and isn't policy history.",
                "responses": {
                    "200": { "description": "Object with generation, default_environment_ref, legacy_unbound_allowed, last_change and environments (environment_ref, state, executes, is_default, last_change)" },
                    "403": { "description": "Managing organization administrator required" },
                    "500": { "description": "A stored policy value this server can't read; the error names it" }
                }
            }
        }),
    );
    paths.insert(
        "/runtime-environments/default".to_string(),
        json!({
            "put": {
                "summary": "Set the cluster default execution environment",
                "parameters": [if_match.clone()],
                "description": "Records the environment as allowed if it isn't already and increments the generation in the same transaction. The previous default stays allowed. The default can be replaced but never cleared. The first default starts narrowing nodes to the environments that execute, so it needs strand: true while sandboxes, or template releases in use (tagged or pinned by an enabled pool config), are bound to an environment with no state or one that doesn't execute, or while any sandbox's environment can't be told; mark the environments to keep allowed first. Later defaults stop nothing. No check yet that every node can run the new default: a node that applies the policy is to refuse a default it hasn't installed and verified.",
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["environment_ref", "reason"],
                                "properties": {
                                    "environment_ref": environment_ref,
                                    "reason": reason,
                                    "strand": {
                                        "type": "boolean",
                                        "default": false,
                                        "description": "Set the first default even though work may depend on an environment it stops."
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": { "description": "Object with the new generation and default_environment_ref" },
                    "400": { "description": "Invalid body, environment reference or reason" },
                    "403": { "description": "Managing organization administrator required" },
                    "409": { "description": "The environment is blocked or has a stored state this server doesn't recognize, or the first default would stop work (at_risk carries the environments and counts)" },
                    "412": { "description": "If-Match named a generation other than the policy's current one; nothing changed" }
                }
            }
        }),
    );
    paths.insert(
        "/runtime-environments/{environment_ref}/state".to_string(),
        json!({
            "put": {
                "summary": "Set an execution environment's state",
                "description": "allowed and deprecated execute; blocked never executes. Increments the generation in the same transaction. The default must stay allowed, and blocking requires a cluster default. Blocking an environment that still has sandboxes, or template releases in use (a tag points to it or an enabled pool config pins it), bound to it, or while any sandbox has no environment binding, an unreadable one, or no binding record, needs strand: true: unbound legacy work executes as the environment it matches, which the server can't resolve, and a sandbox with no observation from its node may be bound to this one. In-use releases with no or an unreadable binding gate too, since new creates from them may run on the environment that stops. Other template releases, and sandboxes that can't run again (cleanup_required or deleting, or failed with no live evidence and nothing the repair loop could revert to), are reported in at_risk but don't require strand. Blocking an environment that already doesn't execute (blocked, or with no state under a default) normally needs no strand; if another policy write lands while the block waits, the check runs in full. The counts are read just before the write and sandboxes keep changing, so strand is an acknowledgement, not a guarantee.",
                "parameters": [
                    { "name": "environment_ref", "in": "path", "required": true, "schema": environment_ref },
                    if_match
                ],
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["state", "reason"],
                                "properties": {
                                    "state": { "type": "string", "enum": ["allowed", "deprecated", "blocked"] },
                                    "reason": reason,
                                    "strand": {
                                        "type": "boolean",
                                        "default": false,
                                        "description": "Block even though work may still depend on the environment: work bound to it, or a sandbox whose environment can't be told. Only accepted with state blocked."
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "200": { "description": "Object with the new generation, environment_ref and state" },
                    "400": { "description": "Invalid body, environment reference, state or reason, or strand without blocked" },
                    "403": { "description": "Managing organization administrator required" },
                    "409": { "description": "The environment is the default, blocking needs a default first, or work may still be bound to it (at_risk carries the environments and counts)" },
                    "412": { "description": "If-Match named a generation other than the policy's current one; nothing changed" }
                }
            }
        }),
    );
    paths.insert(
        "/runtime-environments/nodes".to_string(),
        json!({
            "get": {
                "summary": "List each node's published execution policy against the cluster policy",
                "description": "At most 100 nodes, ordered by node UUID, soft-deleted direct nodes excluded. conformance is unknown when a node hasn't published a readable policy or reports it can't read the cluster policy it persisted, conforms when it doesn't allow legacy unbound work the cluster revoked and, once the cluster has a default, its default matches and it executes nothing the cluster doesn't; it diverges otherwise, with the differences. applied_generation is the cluster generation the node reports applying, null for a node on its startup policy or one that can't read the cluster policy it persisted, and applied_current says whether it's the generation compared with. Conformance compares content, so a node behind on generation still conforms when nothing it would change has changed. status_fresh is false when the node's status snapshot is past stale_after. Use next_after as the after cursor; null means the last page.",
                "parameters": [
                    { "name": "after", "in": "query", "schema": { "type": "string", "format": "uuid" } }
                ],
                "responses": {
                    "200": { "description": "Object with generation, default_environment_ref, nodes and a nullable next_after cursor" },
                    "400": { "description": "Invalid cursor (plain-text body)" },
                    "403": { "description": "Managing organization administrator required" }
                }
            }
        }),
    );
    paths.insert(
        "/runtime-environments/population".to_string(),
        json!({
            "get": {
                "summary": "Count sandboxes and template releases per execution environment",
                "description": "Aggregate counts, never a row list. A suspended or restoring sandbox counts under the environment of the snapshot its substrate's restore would pick. On Direct that's the newest full snapshot in any upload state, since restore runs the source node's artifact, or, for a restoring sandbox, the newest from before the restore began. On Kubernetes it's the newest ready full snapshot with a live storage location: while a node drain evacuates the sandbox, the one holding its current local artifact or, with no artifact marker, the newest full snapshot; otherwise, for a suspended sandbox, the one holding its current local artifact or, with no marker, one taken since the last placement, and for a restoring sandbox, the newest from before the restore began. A failed sandbox whose observation the repair loop would read as live (a status other than failed, a pid, or a node assignment) counts under that observation, and one with a live Kubernetes execution but no such observation counts as unrecorded. Otherwise a failed Kubernetes sandbox the repair loop can revert counts under the snapshot it would restore from: its newest ready full snapshot with a live location, the marked one if a current backup holds its marker. Any other failed sandbox can't run again and has no record. Every other sandbox counts under the execution metadata its node last published. unbound counts records with no environment binding, unrecorded counts sandboxes with no record, including suspended ones with no snapshot a restore could pick, and invalid counts bindings this server can't parse. Deletion tombstones aren't counted. Unbound legacy work isn't attributed to the environment it matches.",
                "responses": {
                    "200": { "description": "Object with generation, environments (environment_ref, state, is_default, sandboxes, sandboxes_by_lifecycle, sandboxes_not_runnable, template_releases, template_releases_in_use), unbound, unrecorded and invalid" },
                    "403": { "description": "Managing organization administrator required" }
                }
            }
        }),
    );
}

#[cfg(test)]
mod tests;
