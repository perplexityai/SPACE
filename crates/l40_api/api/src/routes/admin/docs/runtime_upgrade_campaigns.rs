//! Admin OpenAPI entries for `/runtime-upgrade-campaigns`.

use pplx_space_runtime_upgrade_campaign_store::REASON_MAX_CHARS;
use serde_json::{Map, Value, json};

pub(super) fn insert_runtime_upgrade_campaign_paths(paths: &mut Map<String, Value>) {
    let environment_ref = super::environment_ref_schema();
    let reason = json!({
        "type": "string",
        "minLength": 1,
        "maxLength": REASON_MAX_CHARS,
        "description": "Why. A start records it on the campaign; a pause, resume or abort records it in the audit log."
    });
    let if_match = super::if_match_param(
        "The campaign generation the caller last read, optionally quoted. The change applies only at that generation, compared under the campaign row lock; absent, it's unconditional. The reconciler raises the generation too, when it pauses a campaign at its failure budget or completes it.",
    );
    let campaign_id = json!({ "name": "campaign_id", "in": "path", "required": true, "schema": { "type": "string", "format": "uuid" } });
    let campaign = "Object with id, kind, from_environment_ref, to_environment_ref, phase (canary, rolling, paused, completed or aborted), generation, max_in_flight_per_node, max_in_flight_per_cluster, failure_budget, include_running, reason, created_at, created_by_user_id, updated_at, updated_by_user_id (null when the reconciler made the last change), finished_at, items (pending, in_flight, done, failed, skipped), stranded and held. stranded counts the pending items of an unfinished sandbox campaign whose upgrade edge, from its source to its target, the catalog doesn't certify (0 for a finished campaign); a certified edge doesn't promise every node can make the move. It's null for template campaigns. held is {reason, detail} when this deployment can't move the unfinished campaign whatever its policy or catalog: twins_not_booted for a template campaign where creates don't boot a twin's environment (a Kubernetes substrate running with SPACE_ENVIRONMENT_PLACEMENT_GATE=on does, once the substrate accepts that mode), and otherwise nodes_dont_read_twins while a worker of the substrate doesn't report that its template-manager reads a twin's own metadata, its node status isn't current, or no worker is registered. It's null otherwise; the reconciler's holds that depend on the policy or the catalog (a target that isn't the default or isn't compiled) aren't reported here.";
    paths.insert(
        "/runtime-upgrade-campaigns".to_string(),
        json!({
            "get": {
                "summary": "List runtime upgrade campaigns",
                "description": format!("At most 100 campaigns in id order, which is creation order to the millisecond. Each is {campaign} Use next_after as the after cursor; null means the last page."),
                "parameters": [
                    { "name": "unfinished", "in": "query", "schema": { "type": "boolean", "default": false }, "description": "Only canary, rolling and paused campaigns" },
                    { "name": "after", "in": "query", "schema": { "type": "string", "format": "uuid" } }
                ],
                "responses": {
                    "200": { "description": "Object with campaigns and a nullable next_after cursor" },
                    "400": { "description": "Invalid query (plain-text body)" },
                    "403": { "description": "Managing organization administrator required" }
                }
            },
            "post": {
                "summary": "Start a runtime upgrade campaign",
                "description": "Moves a kind's subjects (template releases or sandboxes) from one execution environment to another, driven by the cluster controller's runtime_upgrade loop. With canary_size 1, the default, the campaign upgrades one subject and then waits in canary for a resume; with 0 it starts rolling. At most max_in_flight_per_cluster items are in flight at once, and, for sandboxes, max_in_flight_per_node on any one node. The campaign pauses itself once failure_budget items have failed. Refused while an unfinished campaign of the same kind moves the same source, and for a kind no cluster-controller driver moves yet.",
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["kind", "from_environment_ref", "to_environment_ref", "max_in_flight_per_cluster", "failure_budget", "reason"],
                                "properties": {
                                    "kind": { "type": "string", "enum": ["templates", "sandboxes"] },
                                    "from_environment_ref": environment_ref.clone(),
                                    "to_environment_ref": environment_ref,
                                    "canary_size": { "type": "integer", "enum": [0, 1], "default": 1, "description": "Subjects upgraded before the campaign waits for a resume: 1, or 0 to start rolling. Only 0 and 1 are accepted, since a canary upgrades exactly one subject." },
                                    "max_in_flight_per_cluster": { "type": "integer", "minimum": 1, "maximum": i64::MAX, "description": "The wave size: items in flight at once across the cluster" },
                                    "max_in_flight_per_node": { "type": "integer", "minimum": 1, "maximum": i64::MAX, "default": 1, "description": "Items in flight at once on any one node, at most max_in_flight_per_cluster. Only sandbox campaigns, whose subjects have a node, apply it, but it must stay in that range for every kind." },
                                    "failure_budget": { "type": "integer", "minimum": 1, "maximum": i64::MAX, "description": "Failed items at which the campaign pauses itself" },
                                    "include_running": { "type": "boolean", "default": false, "description": "For a sandbox campaign, also take running and paused sandboxes" },
                                    "reason": reason.clone()
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "201": { "description": "Object with the new campaign's id, phase and generation (0)" },
                    "400": { "description": "Invalid body, kind, environment reference, canary size, pacing (max_in_flight_per_cluster 0, max_in_flight_per_node 0 or over max_in_flight_per_cluster, failure_budget 0, or a count over the maximum) or reason, the same source and target, or include_running on a templates campaign" },
                    "403": { "description": "Managing organization administrator required" },
                    "409": { "description": "No driver moves this kind yet, or an unfinished campaign of the kind already moves the source" }
                }
            }
        }),
    );
    paths.insert(
        "/runtime-upgrade-campaigns/{campaign_id}".to_string(),
        json!({
            "get": {
                "summary": "Get a runtime upgrade campaign",
                "description": campaign,
                "parameters": [campaign_id.clone()],
                "responses": {
                    "200": { "description": "The campaign" },
                    "400": { "description": "Invalid campaign id" },
                    "403": { "description": "Managing organization administrator required" },
                    "404": { "description": "No such campaign" }
                }
            }
        }),
    );
    let action = |summary: &str, description: &str, properties: Value| {
        json!({
            "post": {
                "summary": summary,
                "description": description,
                "parameters": [
                    campaign_id.clone(),
                    if_match.clone()
                ],
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["reason"],
                                "properties": properties
                            }
                        }
                    }
                },
                "responses": {
                    "200": { "description": "Object with id, phase, the new generation, retried (the failed items a resume returned to pending) and draining (the in-flight items an abort left to stop at their next lease renewal)" },
                    "400": { "description": "Invalid body, reason, campaign id or If-Match" },
                    "403": { "description": "Managing organization administrator required" },
                    "404": { "description": "No such campaign, or an unknown action" },
                    "409": { "description": "The phase doesn't allow it: the campaign is finished, already paused, rolling (for a resume), a canary with no item done yet (for a resume), or at its failure budget without retry_failed" },
                    "412": { "description": "If-Match named a generation other than the campaign's, and the change would otherwise apply; nothing changed. A change the phase refuses answers 409 whatever the generation" }
                }
            }
        })
    };
    paths.insert(
        "/runtime-upgrade-campaigns/{campaign_id}:pause".to_string(),
        action(
            "Pause a runtime upgrade campaign",
            "Moves a canary or rolling campaign to paused. Nothing new is leased; items already in flight finish under their leases.",
            json!({ "reason": reason.clone() }),
        ),
    );
    paths.insert(
        "/runtime-upgrade-campaigns/{campaign_id}:resume".to_string(),
        action(
            "Resume a runtime upgrade campaign",
            "Resumes a paused campaign, or moves a canary whose item is done on to rolling. A campaign with a done item rolls; one with none goes back to canary, upgrading one subject and waiting again. Refused while the failed items are at or over the failure budget, since the campaign would pause again at once, unless retry_failed returns them to pending with their attempts reset; the budget then counts only new failures.",
            json!({
                "reason": reason.clone(),
                "retry_failed": { "type": "boolean", "default": false, "description": "Return every failed item to pending, keeping its last error until a new outcome" }
            }),
        ),
    );
    paths.insert(
        "/runtime-upgrade-campaigns/{campaign_id}:abort".to_string(),
        action(
            "Abort a runtime upgrade campaign",
            "Ends a canary, rolling or paused campaign for good: aborted is final, and finished_at is set. Nothing already upgraded is undone, and nothing leases the campaign's items again. In-flight items keep their leases: a finished campaign's lease can't be renewed, so each worker drops its work at its next renewal attempt (or at its own deadline, before the lease lapses, if the database is out of reach), and the item returns to pending once the lease lapses. The source is free for a new campaign at once, which doesn't lease a subject while the aborted campaign's lease on it lives, so one worker at a time upgrades it.",
            json!({ "reason": reason }),
        ),
    );
}
