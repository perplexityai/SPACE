use pplx_space_common_core::types::NodeGroupId;
use pplx_space_common_core::types::{OrgId, UserId};

use pplx_space_observability::test_capture::{CapturedEvent, CapturedEventsLayer, field_value};
use tracing_subscriber::layer::SubscriberExt;

use crate::user_role::UserRole;

use super::*;

#[test]
fn audit_event_limit_is_bounded() {
    assert_eq!(
        ListAdminAuditEventsQuery { limit: None }
            .normalized_limit()
            .unwrap(),
        DEFAULT_AUDIT_EVENT_LIMIT
    );
    assert_eq!(
        ListAdminAuditEventsQuery { limit: Some(250) }
            .normalized_limit()
            .unwrap(),
        250
    );
    ListAdminAuditEventsQuery { limit: Some(0) }
        .normalized_limit()
        .unwrap_err();
    ListAdminAuditEventsQuery { limit: Some(251) }
        .normalized_limit()
        .unwrap_err();
}

/// An operator entry and a system entry must be indistinguishable to a
/// consumer except in the `actor_user_id` value: same target, same `event`,
/// same field names in the same order. This is what lets
/// [`record_node_group_system`] ship without touching any audit consumer, so
/// it is pinned rather than asserted in prose.
#[test]
fn system_node_group_entry_matches_the_operator_field_shape() {
    let layer = CapturedEventsLayer::new().with_target("space.admin.audit");
    let events = layer.events();
    let subscriber = tracing_subscriber::registry().with(layer);
    let actor = User {
        id: UserId::new(),
        email: "operator@example.com".to_string(),
        password_hash: String::new(),
        display_name: "operator".to_string(),
        org_id: OrgId::new(),
        role: UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    };
    let node_group_id = NodeGroupId::new();
    let to_state = "org=generic policy=default default=true";

    tracing::subscriber::with_default(subscriber, || {
        record_node_group(AdminNodeGroupAuditEntry {
            actor: &actor,
            action: "node-group-create",
            node_group_id,
            node_group_name: Some("workers"),
            from_state: None,
            to_state: Some(to_state),
            success: true,
            error: None,
        });
        record_node_group_system(SystemNodeGroupAuditEntry {
            actor: SYSTEM_ACTOR_NODE_GROUP_LABELER,
            action: "node-group-seed-default",
            node_group_id,
            node_group_name: Some("workers"),
            from_state: None,
            to_state: Some(to_state),
            success: true,
            error: None,
        });
    });

    let captured = events.events();
    assert_eq!(captured.len(), 2, "both emitters use the same target");
    let field_names = |event: &CapturedEvent| -> Vec<String> {
        event.iter().map(|(name, _)| name.clone()).collect()
    };
    assert_eq!(
        field_names(&captured[0]),
        field_names(&captured[1]),
        "the system entry must carry the operator entry's fields, in order"
    );
    assert_eq!(field_value(&captured[1], "event"), Some("admin_audit"));
    assert_eq!(
        field_value(&captured[1], "actor_user_id"),
        Some("system:node-group-labeler"),
        "the machine principal is the stable string consumers key on"
    );
    assert_eq!(
        field_value(&captured[1], "actor_email"),
        Some(""),
        "no fabricated email in an attribution stream"
    );
    assert_eq!(
        field_value(&captured[1], "node_group_name"),
        Some("workers")
    );
    assert_eq!(field_value(&captured[1], "to_state"), Some(to_state));
}
