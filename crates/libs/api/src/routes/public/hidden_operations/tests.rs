use super::*;
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Method, Request, StatusCode},
};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, TestDb, TestDbResult, pg_test_db};
use pplx_space_feature_flags::{
    AttributeValue, FeatureFlagClient, FeatureFlagProvider, ProviderState, Unresolved,
};
use pplx_space_public_api::{
    PublicSandboxId,
    routes::{NATIVE_V1_OPERATIONS, NativeSurface},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tower::ServiceExt;
use uuid::Uuid;

use crate::federated_identity::{
    FEDERATED_ORG_ID_HEADER, FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
    FEDERATED_SERVICE_GRANT_HEADER, FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
    FEDERATED_USER_ID_HEADER,
};

const KEY: &str = "hidden-operations-test-key";
const FEDERATED_ORG: &str = "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01";
/// Every native router mount the tests exercise, including one under a prefix
/// the gate has never heard of: it must not depend on where it is nested.
const ROOTS: [&str; 3] = [
    "/v1",
    crate::routes::PUBLIC_API_SHARED_ROOT,
    "/elsewhere/v1",
];

#[test]
fn the_env_override_names_every_org_or_a_list_of_them() {
    let org = OrgId::from_uuid(SEED_ORG_ID);
    let other = OrgId::from_uuid(Uuid::from_u128(2));
    for (raw, expected) in [
        (None, Some(HiddenOperationsGate::FlagOnly)),
        (Some(""), Some(HiddenOperationsGate::FlagOnly)),
        (Some(" "), Some(HiddenOperationsGate::FlagOnly)),
        (Some("*"), Some(HiddenOperationsGate::AllOrgs)),
        (
            Some(&format!("{org}, {other}")),
            Some(HiddenOperationsGate::Orgs(BTreeSet::from([org, other]))),
        ),
        (Some(&format!("{org},*")), None),
        (Some(&format!("{org},")), None),
        (Some("not-an-org"), None),
    ] {
        assert_eq!(HiddenOperationsGate::parse(raw).ok(), expected, "{raw:?}");
    }
}

/// Each hidden operation is enforced by exactly one of the two checks: it owns
/// its mount and gets the route gate, or it is a sandbox action, which
/// `parse_action_segment` checks. Any other shared mount refuses to build.
#[test]
fn every_hidden_operation_is_enforced_by_its_route_or_its_action() {
    let mut mounts = BTreeMap::<_, Vec<NativeOperation>>::new();
    for operation in NATIVE_V1_OPERATIONS
        .iter()
        .filter(|operation| operation.surface() == NativeSurface::ControlPlane)
    {
        mounts
            .entry((operation.relative_runtime_path(), operation.method()))
            .or_default()
            .push(*operation);
    }
    for operations in mounts.values() {
        let gate = route_gate(operations);
        for operation in operations {
            if !operation.status().is_hidden_by_default() {
                assert_ne!(gate, Some(*operation), "{operation:?}");
            } else if gate != Some(*operation) {
                let action = operation
                    .path()
                    .rsplit_once(':')
                    .and_then(|(_, action)| {
                        pplx_space_public_api::SandboxLifecycleAction::parse_known(action).ok()
                    })
                    .unwrap_or_else(|| panic!("{operation:?} is not enforced"));
                assert_eq!(
                    super::super::sandboxes::lifecycle_operation(action),
                    *operation
                );
            }
        }
    }
    let uncovered =
        std::panic::catch_unwind(|| route_gate(&[native::LIST_SNAPSHOTS, native::LIST_PROCESSES]));
    assert!(uncovered.is_err(), "a hidden operation on a shared mount");
}

/// A flag backend that opens hidden operations only for `subject`, and only
/// the operation `operation_id` names.
struct Targeted {
    subject: String,
    operation_id: &'static str,
}

impl FeatureFlagProvider for Targeted {
    fn state(&self) -> ProviderState {
        ProviderState::Ready
    }

    fn bool_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<bool, Unresolved> {
        assert_eq!(flag_key, SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG.key());
        Ok(subject_key == self.subject
            && attributes.get("operation_id")
                == Some(&AttributeValue::String(self.operation_id.to_owned())))
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn json_value(&self, _: &str, _: &str, _: &Attributes) -> Result<Value, Unresolved> {
        Err(Unresolved::NoAssignment)
    }
}

fn owner_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(FEDERATED_ORG_ID_HEADER, FEDERATED_ORG.parse().unwrap());
    headers.insert(FEDERATED_USER_ID_HEADER, "101".parse().unwrap());
    headers
}

/// A local caller in the integration key's own org, and a federated end user
/// the same key asserts, each with a sandbox of their own.
struct Fixture {
    _db: TestDb,
    app: Router,
    local_sandbox: String,
    federated_sandbox: String,
}

impl Fixture {
    /// `gate` and `flags` are handed the federated caller's effective org,
    /// which only exists once that caller has made a request.
    async fn new(
        gate: impl FnOnce(OrgId) -> HiddenOperationsGate,
        flags: impl FnOnce(OrgId) -> FeatureFlagClient,
    ) -> TestDbResult<Self> {
        let db = pg_test_db().await?;
        sqlx::query(sql!(
            "INSERT INTO api_keys (
                 id, org_id, name, key_hash, allowed_surfaces, federated_owner_assertions_enabled
             ) VALUES ($1, $2, 'hidden-operations-test', $3, ARRAY['native'], TRUE)"
        ))
        .bind(Uuid::now_v7())
        .bind(SEED_ORG_ID)
        .bind(crate::auth::hash_token(KEY))
        .execute(&db.pool)
        .await?;
        let mut state = AppState::from_pool(db.pool.clone()).await?;
        state.federated_identity = crate::federated_identity::FederatedIdentityConfig {
            enabled: true,
            policy: crate::federated_identity::FederatedIdentityPolicy::parse(
                Some("test-issuer"),
                None,
                Some(r#"["0"]"#),
            )?,
        };
        let local_sandbox = Self::seed(&state, &HeaderMap::new()).await?;
        let mut grant = owner_headers();
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        grant.insert(FEDERATED_SERVICE_GRANT_HEADER, "opaque-grant".parse()?);
        grant.insert(
            FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
            (now + 3600).to_string().parse()?,
        );
        grant.insert(
            FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
            now.to_string().parse()?,
        );
        let federated_sandbox = Self::seed(&state, &grant).await?;
        let federated_org =
            sqlx::query_scalar::<_, Uuid>(sql!("SELECT org_id FROM sandboxes WHERE id = $1"))
                .bind(federated_sandbox.parse::<PublicSandboxId>()?.as_uuid())
                .fetch_one(&db.pool)
                .await?;
        let federated_org = OrgId::from_uuid(federated_org);
        assert_ne!(federated_org, OrgId::from_uuid(SEED_ORG_ID));
        state.native_hidden_operations = gate(federated_org);
        state.feature_flags = flags(federated_org);
        Ok(Self {
            _db: db,
            app: app(state),
            local_sandbox,
            federated_sandbox,
        })
    }

    /// Admit a create as `headers` names the caller, and record its sandbox
    /// as started.
    async fn seed(state: &AppState, headers: &HeaderMap) -> TestDbResult<String> {
        let (status, created) = send(
            &app(state.clone()),
            Method::POST,
            "/v1/sandboxes",
            headers,
            json!({"template": "seed:latest"}),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{created}");
        let sandbox_id: PublicSandboxId = created["sandbox_id"].as_str().unwrap().parse()?;
        let operation_id: pplx_space_public_api::OperationId =
            created["operation_id"].as_str().unwrap().parse()?;
        sqlx::query(sql!(
            "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_override_json,
                 network_policy_generation)
             SELECT $1, actor_user_id, org_id, 'started', $3, 1
             FROM public_operations WHERE operation_id = $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(operation_id.as_uuid())
        .bind(sqlx::types::Json(
            pplx_space_net_common::NetworkPolicyV1::default(),
        ))
        .execute(&state.pool)
        .await?;
        Ok(sandbox_id.to_string())
    }
}

fn app(state: AppState) -> Router {
    ROOTS
        .into_iter()
        .fold(Router::new(), |router, root| {
            router.nest(root, crate::routes::public::v1_router(state.clone()))
        })
        .with_state(state)
}

async fn send(
    app: &Router,
    method: Method,
    path: &str,
    headers: &HeaderMap,
    body: Value,
) -> TestDbResult<(StatusCode, Value)> {
    let mut request = Request::builder()
        .method(method.clone())
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .header("idempotency-key", format!("key-{method}-{path}"))
        .header("x-request-id", "req-hidden");
    request.headers_mut().unwrap().extend(headers.clone());
    let response = app
        .clone()
        .oneshot(request.body(Body::from(serde_json::to_vec(&body)?))?)
        .await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body)?
    };
    Ok((status, body))
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Caller {
    Local,
    Federated,
}

type GateFor = Box<dyn Fn(OrgId) -> HiddenOperationsGate>;
type FlagsFor = Box<dyn Fn(OrgId) -> FeatureFlagClient>;
type Enabled = fn(Caller, NativeOperation) -> bool;

#[tokio::test]
async fn hidden_operations_answer_as_nonexistent_unless_the_effective_org_is_enabled()
-> TestDbResult<()> {
    let local_org = OrgId::from_uuid(SEED_ORG_ID);
    let unrelated = OrgId::from_uuid(Uuid::from_u128(2));
    let cases: [(&str, GateFor, FlagsFor, Enabled); 7] = [
        (
            "nothing enabled",
            Box::new(|_| HiddenOperationsGate::FlagOnly),
            Box::new(|_| FeatureFlagClient::disabled()),
            |_, _| false,
        ),
        (
            "an unrelated org listed",
            Box::new(move |_| HiddenOperationsGate::Orgs(BTreeSet::from([unrelated]))),
            Box::new(|_| FeatureFlagClient::disabled()),
            |_, _| false,
        ),
        // The key's own org owns only the key's local data, not its end users'.
        (
            "the integration key's org listed",
            Box::new(move |_| HiddenOperationsGate::Orgs(BTreeSet::from([local_org]))),
            Box::new(|_| FeatureFlagClient::disabled()),
            |caller, _| caller == Caller::Local,
        ),
        (
            "the federated owner's org listed",
            Box::new(|federated| HiddenOperationsGate::Orgs(BTreeSet::from([federated]))),
            Box::new(|_| FeatureFlagClient::disabled()),
            |caller, _| caller == Caller::Federated,
        ),
        (
            "every org",
            Box::new(|_| HiddenOperationsGate::AllOrgs),
            Box::new(|_| FeatureFlagClient::disabled()),
            |_, _| true,
        ),
        (
            "the flag on for everyone",
            Box::new(|_| HiddenOperationsGate::FlagOnly),
            Box::new(|_| {
                FeatureFlagClient::fixed_bools([(SPACE_NATIVE_HIDDEN_OPERATIONS_FLAG.key(), true)])
            }),
            |_, _| true,
        ),
        (
            "the flag targeting the federated org's snapshot list",
            Box::new(|_| HiddenOperationsGate::FlagOnly),
            Box::new(|federated| {
                FeatureFlagClient::from_provider(Targeted {
                    subject: federated.to_string(),
                    operation_id: native::LIST_SNAPSHOTS.operation_id(),
                })
            }),
            |caller, operation| caller == Caller::Federated && operation == native::LIST_SNAPSHOTS,
        ),
    ];
    for (case, gate, flags, enabled) in cases {
        let fixture = Fixture::new(gate, flags).await?;
        for (caller, headers, own, foreign) in [
            (
                Caller::Local,
                HeaderMap::new(),
                &fixture.local_sandbox,
                &fixture.federated_sandbox,
            ),
            (
                Caller::Federated,
                owner_headers(),
                &fixture.federated_sandbox,
                &fixture.local_sandbox,
            ),
        ] {
            let app = &fixture.app;
            let ids = [own.as_str(), "not-a-sandbox-id", foreign.as_str()];
            for root in ROOTS {
                let case = format!("{case}: {caller:?} under {root}");

                // A route only a hidden operation owns answers, for every
                // method and every id, as a route that does not exist.
                let visible = enabled(caller, native::LIST_SNAPSHOTS);
                for (method, id, visible_status) in [
                    (Method::GET, ids[0], StatusCode::OK),
                    (Method::GET, ids[1], StatusCode::BAD_REQUEST),
                    (Method::GET, ids[2], StatusCode::NOT_FOUND),
                    (Method::HEAD, ids[0], StatusCode::OK),
                    (Method::OPTIONS, ids[0], StatusCode::METHOD_NOT_ALLOWED),
                ] {
                    let path = format!("{root}/sandboxes/{id}/snapshots");
                    let answer = send(app, method.clone(), &path, &headers, Value::Null).await?;
                    if visible {
                        assert_eq!(
                            answer.0, visible_status,
                            "{case}: {method} {path}: {answer:?}"
                        );
                    } else {
                        let unmounted = format!("{root}/sandboxes/{id}/no-such-route");
                        let absent =
                            send(app, method.clone(), &unmounted, &headers, Value::Null).await?;
                        assert_eq!(absent.0, StatusCode::NOT_FOUND, "{case}: {absent:?}");
                        assert_eq!(answer, absent, "{case}: {method} {path}");
                    }
                }

                // A hidden action answers, for every id, as an unknown one.
                let visible = enabled(caller, native::ROLLBACK_SANDBOX);
                for id in ids {
                    let path = format!("{root}/sandboxes/{id}:rollback");
                    let body = json!({"snapshot_name": "s"});
                    let answer = send(app, Method::POST, &path, &headers, body.clone()).await?;
                    if visible && id == own.as_str() {
                        assert_eq!(answer.0, StatusCode::ACCEPTED, "{case}: {path}: {answer:?}");
                    } else {
                        let unknown = format!("{root}/sandboxes/{id}:no_such_action");
                        let absent = send(app, Method::POST, &unknown, &headers, body).await?;
                        assert_ne!(absent.0, StatusCode::ACCEPTED, "{case}: {absent:?}");
                        assert_eq!(answer, absent, "{case}: {path}");
                    }
                }
            }

            // A visible incubating action on the same mount is served regardless.
            let path = format!("/v1/sandboxes/{own}:stop");
            let answer = send(app, Method::POST, &path, &headers, Value::Null).await?;
            assert_eq!(
                answer.0,
                StatusCode::ACCEPTED,
                "{case}: {caller:?}: {answer:?}"
            );
        }
    }
    Ok(())
}

/// The full document lists hidden operations, so it needs the same
/// credentials as `/v1`; the default one stays public.
#[tokio::test]
async fn only_the_full_contract_requires_authentication() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         VALUES ($1, $2, 'hidden-operations-docs-test', $3, ARRAY['native'])"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    let app = crate::routes::router(AppState::from_pool(db.pool.clone()).await?);
    let operations = |document: &Value| {
        document["paths"]
            .as_object()
            .expect("paths should be an object")
            .values()
            .flat_map(|item| item.as_object().expect("path item").values())
            .filter(|operation| operation.get("operationId").is_some())
            .count()
    };
    let get = async |path: String, token: Option<&str>| -> TestDbResult<(StatusCode, Value)> {
        let mut request = Request::builder().uri(path);
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        let response = app.clone().oneshot(request.body(Body::empty())?).await?;
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024).await?;
        Ok((status, serde_json::from_slice(&body)?))
    };
    for root in ["", "/api"] {
        let (status, default) = get(format!("{root}/openapi.json"), None).await?;
        assert_eq!(status, StatusCode::OK, "{root}/openapi.json");
        let (status, body) = get(format!("{root}/openapi-full.json"), None).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{root}: {body}");
        assert_eq!(body["code"], "unauthorized", "{root}: {body}");
        let (status, full) = get(format!("{root}/openapi-full.json"), Some(KEY)).await?;
        assert_eq!(status, StatusCode::OK, "{root}/openapi-full.json: {full}");
        assert_eq!(operations(&full), NATIVE_V1_OPERATIONS.len(), "{root}");
        assert!(operations(&default) < operations(&full), "{root}");
    }
    Ok(())
}
