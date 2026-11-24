//! Admin OpenAPI entries for `/firewall-rules` and `/firewall-rollouts`.

use pplx_space_firewall_rules_store::REASON_MAX_CHARS;
use serde_json::{Map, Value, json};

pub(super) fn insert_firewall_rules_paths(paths: &mut Map<String, Value>) {
    let revision = "A complete firewall rules document: schema_version and the ordered rules, as the firewall rules protocol defines it";
    paths.insert(
        "/firewall-rules".to_string(),
        json!({
            "get": {
                "summary": "Get the adopted firewall rules revision",
                "description": "The revision newly enrolled nodes are assigned, which a push names as expected_adopted_revision_id. A started rollout's candidate isn't adopted, so it isn't returned. Read without a lock: a push compares the precondition again.",
                "responses": {
                    "200": { "description": format!("Object with adopted_revision_id and revision, both null when no revision is adopted. An adopted revision with no rules is {{\"schema_version\":1,\"rules\":[]}}, not null. revision is {revision}, in canonical form.") },
                    "403": { "description": "Managing organization administrator required" },
                    "500": { "description": "The adopted revision can't be read; this is never answered as a first push" }
                }
            }
        }),
    );
    paths.insert(
        "/firewall-rollouts".to_string(),
        json!({
            "post": {
                "summary": "Start a firewall rules rollout",
                "description": "Saves the candidate and starts rolling it out if the adopted revision is still expected_adopted_revision_id and no rollout is unfinished. Starting adopts nothing and enforces nothing. Production starts are refused until target enrollment and qualification are approved. A lost response is an unknown outcome: read GET /firewall-rules before pushing again.",
                "requestBody": {
                    "required": true,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["expected_adopted_revision_id", "reason", "revision"],
                                "properties": {
                                    "expected_adopted_revision_id": { "type": ["string", "null"], "format": "uuid", "description": "Required but nullable: the adopted_revision_id the candidate was reviewed against, or null when none was adopted. Omitting it is a 400." },
                                    "reason": { "type": "string", "minLength": 1, "maxLength": REASON_MAX_CHARS, "description": "Why, recorded on the rollout. No NUL." },
                                    "revision": { "type": "object", "description": format!("{revision}. Never a patch or a diff; duplicate keys and unknown fields are refused.") }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    "201": { "description": "Object with revision_id, rollout_id, generation and review. review is {\"kind\":\"first_push\"} when nothing was adopted, or {\"kind\":\"against\"} with base_revision_id, changes (each {change: added, replaced or removed, rule_id}) and reordered, which is true when shared rules changed order even with no changes" },
                    "400": { "description": "Invalid body, precondition, reason or revision" },
                    "403": { "description": "Managing organization administrator required" },
                    "409": { "description": "The adopted revision isn't the expected one (adopted_revision_id, possibly null, is the current one; review again), a rollout is unfinished (rollout_id when known), another rollout took the slot first, production starts aren't enabled yet, or a target can't enforce the candidate" }
                }
            }
        }),
    );
}
