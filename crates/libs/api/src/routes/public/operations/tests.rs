use crate::public_operations::{
    ApiProduct, CreatePublicOperation, CreatePublicOperationResult, PublicOperationStore,
};
use futures_util::StreamExt;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::OrgId;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_public_api::{OperationKind, OperationTargetType};
use uuid::Uuid;

use super::*;

fn operation_event(sequence: i64) -> PublicOperationEvent {
    PublicOperationEvent {
        operation_id: OperationId::new(),
        sequence,
        event_type: PublicOperationEventType::Claimed,
        event: serde_json::json!({}),
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

#[test]
fn operation_event_query_defaults_and_caps_limit() {
    assert_eq!(
        ListOperationEventsQuery {
            cursor: None,
            limit: None,
        }
        .limit(),
        DEFAULT_OPERATION_EVENT_LIMIT
    );
    assert_eq!(
        ListOperationEventsQuery {
            cursor: None,
            limit: Some(MAX_OPERATION_EVENT_LIMIT + 1),
        }
        .limit(),
        MAX_OPERATION_EVENT_LIMIT
    );
}

#[test]
fn operation_event_page_cursor_requires_overfetch_row() {
    let page = operation_event_page(vec![operation_event(1), operation_event(2)], 2).unwrap();
    assert_eq!(page.data.len(), 2);
    assert!(page.next_cursor.is_none());

    let page = operation_event_page(
        vec![operation_event(1), operation_event(2), operation_event(3)],
        2,
    )
    .unwrap();
    assert_eq!(page.data.len(), 2);
    assert_eq!(
        page.next_cursor.as_ref().and_then(|cursor| decode_cursor(
            CursorKind::OperationEvent,
            cursor.as_str()
        )
        .ok()),
        Some("2".to_string())
    );
}

#[test]
fn operation_event_query_parses_sequence_cursor() {
    let cursor = encode_cursor(CursorKind::OperationEvent, "42".to_string()).unwrap();
    assert_eq!(
        ListOperationEventsQuery {
            cursor: Some(cursor.to_string()),
            limit: None,
        }
        .after_sequence(&HeaderMap::new())
        .unwrap(),
        Some(42)
    );
    ListOperationEventsQuery {
        cursor: Some("-1".to_string()),
        limit: None,
    }
    .after_sequence(&HeaderMap::new())
    .unwrap_err();
}

#[test]
fn operation_event_query_uses_last_event_id_when_cursor_missing() {
    let mut headers = HeaderMap::new();
    headers.insert("last-event-id", "17".parse().unwrap());
    assert_eq!(
        ListOperationEventsQuery {
            cursor: None,
            limit: None,
        }
        .after_sequence(&headers)
        .unwrap(),
        Some(17)
    );

    let cursor = encode_cursor(CursorKind::OperationEvent, "21".to_string()).unwrap();
    assert_eq!(
        ListOperationEventsQuery {
            cursor: Some(cursor.to_string()),
            limit: None,
        }
        .after_sequence(&headers)
        .unwrap(),
        Some(21)
    );
}

#[test]
fn accept_header_detects_event_stream() {
    let mut headers = HeaderMap::new();
    assert!(!accepts_event_stream(&headers));
    headers.insert(
        header::ACCEPT,
        "application/json, text/event-stream; q=0.9"
            .parse()
            .unwrap(),
    );
    assert!(accepts_event_stream(&headers));
}

#[test]
fn operation_terminal_helpers_cover_states_and_events() {
    assert!(operation_state_is_terminal(OperationState::Succeeded));
    assert!(operation_state_is_terminal(OperationState::Failed));
    assert!(operation_state_is_terminal(OperationState::Canceled));
    assert!(operation_state_is_terminal(OperationState::Expired));
    assert!(!operation_state_is_terminal(OperationState::Queued));
    assert!(!operation_state_is_terminal(OperationState::Running));

    let mut event = PublicOperationEvent {
        operation_id: OperationId::new(),
        sequence: 1,
        event_type: PublicOperationEventType::Completed,
        event: serde_json::json!({}),
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    assert!(operation_event_is_terminal(&event));
    event.event_type = PublicOperationEventType::Claimed;
    assert!(!operation_event_is_terminal(&event));
}

#[tokio::test]
async fn operation_event_stream_stops_when_operation_state_is_terminal() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let org_id = seed_org(&db.pool).await?;
    let store = PublicOperationStore::new(db.pool.clone());
    let CreatePublicOperationResult::Created(operation_id) = store
        .create_operation(CreatePublicOperation {
            org_id: OrgId::from_uuid(org_id),
            actor_user_id: None,
            api_product: ApiProduct::Native,
            kind: OperationKind::WakeSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some("sbx_terminal"),
            payload_json: serde_json::json!({}),
            idempotency: None,
            traceparent: None,
        })
        .await?
    else {
        panic!("test operation should be created");
    };
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = $2, updated_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(OperationState::Canceled.as_str())
    .execute(&db.pool)
    .await?;

    let initial_events = store
        .fetch_operation_events(
            OrgId::from_uuid(org_id),
            operation_id,
            None,
            OPERATION_EVENT_STREAM_BATCH_LIMIT,
        )
        .await?
        .unwrap_or_default();
    let stream = operation_event_stream(
        store,
        OrgId::from_uuid(org_id),
        operation_id,
        false,
        initial_events,
        None,
    );
    futures_util::pin_mut!(stream);
    assert!(stream.next().await.is_some());
    let next = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(
        matches!(next, Ok(None)),
        "terminal operation without a terminal event should close the stream"
    );
    Ok(())
}

use pplx_space_test_env::TestResult;

async fn seed_org(pool: &sqlx::PgPool) -> Result<Uuid, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO organizations (
             id,
             name,
             slug,
             byok_enabled
         )
         VALUES ($1, $2, $3, false)"
    ))
    .bind(id)
    .bind(format!("Org {id}"))
    .bind(format!("org-{}", id.simple()))
    .execute(pool)
    .await?;
    Ok(id)
}
