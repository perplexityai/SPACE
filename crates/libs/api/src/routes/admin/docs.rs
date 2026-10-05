use axum::{
    Json,
    http::{HeaderValue, header},
    response::IntoResponse,
};
use serde_json::{Map, Value, json};

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
                    "description": "Set include_occupancy=false for metadata-only inventory with null occupancy counts. Legacy requests compute counts; sort=drainability always computes diagnostics.",
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
                    "description": "Set include_occupancy=false to read metadata without sandbox or snapshot accounting; occupancy counts are then null.",
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
                        "409": { "description": "Node transition conflict" }
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
                        "409": { "description": "Node transition conflict" }
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
                        "description": "Optional drain options. An omitted body means a gentle drain that auto-replaces the instance on termination.",
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
                                            "description": "gentle: cordon the node and wait for resident sandboxes to leave on their own (auto-suspend on inactivity, user suspend, delete); user calls against them keep working. forced: evacuate every resident started/paused sandbox immediately and reject user calls against them with 409 for the whole drain window."
                                        },
                                        "replace_after_terminate": {
                                            "type": "boolean",
                                            "default": true,
                                            "description": "false makes the eventual ASG terminate decrement desired capacity, for a blue/green rollout where the incumbent group must shrink monotonically."
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": { "description": "Drain accepted" },
                        "409": { "description": "Node transition conflict" }
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
                        "409": { "description": "Node transition conflict" }
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
                        "409": { "description": "Node transition conflict" }
                    }
                }
            },
            "/operations": {
                "get": {
                    "summary": "List public operations across tenants for operator diagnostics",
                    "parameters": [
                        { "name": "org_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } },
                        { "name": "state", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "kind", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "target_type", "in": "query", "required": false, "schema": { "type": "string" } },
                        { "name": "target_id", "in": "query", "required": false, "schema": { "type": "string" } },
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
                    "responses": {
                        "201": { "description": "Organization" },
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
                    "responses": {
                        "200": { "description": "Organization" },
                        "403": { "description": "Caller cannot manage the organization" }
                    }
                },
                "delete": {
                    "summary": "Delete an organization",
                    "responses": {
                        "204": { "description": "Deleted" },
                        "403": { "description": "Caller is not a managing organization admin" }
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
                        "403": { "description": "Caller cannot manage the organization" }
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
                        "409": { "description": "Caller attempted to delete itself" }
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
            },
            "/sandboxes/{sandbox_id}:force-{action}": {
                "post": {
                    "summary": "Enqueue an audited break-glass sandbox lifecycle operation",
                    "parameters": [
                        { "name": "sandbox_id", "in": "path", "required": true, "schema": { "type": "string", "pattern": "^sbx_" } },
                        { "name": "action", "in": "path", "required": true, "schema": { "type": "string", "enum": ["start", "stop", "pause", "resume", "suspend", "restore", "wake", "kill", "delete"] } },
                        { "name": "Idempotency-Key", "in": "header", "required": true, "schema": { "type": "string" } }
                    ],
                    "responses": {
                        "202": { "description": "Operation enqueued" },
                        "400": { "description": "Invalid action or idempotency key" },
                        "404": { "description": "Sandbox not found" },
                        "409": { "description": "Idempotency conflict" }
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
    insert_identity_profile_paths(paths);
    insert_audit_event_paths(paths);
    spec
}

fn insert_api_key_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/orgs/{org_id}/api-keys".to_string(),
        json!({
            "get": {
                "summary": "List organization API keys without secret material",
                "responses": {
                    "200": { "description": "API key metadata list" },
                    "404": { "description": "Organization not found" }
                }
            },
            "post": {
                "summary": "Create an organization API key",
                "description": "Returns the raw secret exactly once. The key is attributed to the calling admin and can only be created for that admin's organization.",
                "responses": {
                    "201": { "description": "API key metadata and one-time secret" },
                    "400": { "description": "Invalid name or API surfaces" },
                    "403": { "description": "Cannot create API keys for another organization" }
                }
            }
        }),
    );
    paths.insert(
        "/orgs/{org_id}/api-keys/{api_key_id}".to_string(),
        json!({
            "delete": {
                "summary": "Revoke an organization API key",
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
                    { "name": "template", "in": "path", "required": true, "schema": { "type": "string", "enum": ["seed", "code-runtime"] } }
                ],
                "requestBody": {
                    "required": true,
                    "content": { "application/json": { "schema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "base": { "type": ["string", "null"], "description": "Seed reference for code-runtime only; defaults to seed:latest" },
                            "force": { "type": "boolean", "default": false },
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
                    "409": { "description": "Template belongs to another organization or is deleting or deleted" },
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
                "responses": { "200": { "description": "Template release" } }
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
                "parameters": [
                    { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer" } },
                    { "name": "org_id", "in": "query", "required": false, "schema": { "type": "string" } }
                ],
                "responses": {
                    "200": { "description": "Sandbox list" }
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
        "/sandboxes/{sandbox_id}".to_string(),
        json!({
            "patch": {
                "summary": "Enqueue a sandbox update (network policy, auto-lifecycle, metadata) under the sandbox's owning organization",
                "parameters": [
                    { "name": "sandbox_id", "in": "path", "required": true, "schema": { "type": "string", "pattern": "^sbx_" } },
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
                    "404": { "description": "User not found" }
                }
            }),
        );
    }
}

fn insert_audit_event_paths(paths: &mut Map<String, Value>) {
    paths.insert(
        "/audit-events".to_string(),
        json!({
            "get": {
                "summary": "List persisted admin request audit events",
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

#[cfg(test)]
mod tests;
