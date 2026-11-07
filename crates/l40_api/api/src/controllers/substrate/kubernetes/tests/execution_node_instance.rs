//! Pod placement records on the active execution the EC2 instance of the Node
//! it read the node's name and UID from: the reflector's Node, or, when the
//! reflector hasn't seen it, the apiserver's. A Node with no AWS `providerID`
//! names none, and a Node neither knows fails placement before any write.

use axum::http::{Method, Request, Response, StatusCode};
use k8s_openapi::api::core::v1::{
    Node, NodeAddress as K8sNodeAddress, NodeSpec as K8sNodeSpec, NodeStatus as K8sNodeStatus, Pod,
    PodSpec as K8sPodSpec,
};
use kube::Client;
use kube::client::{Body, ClientBuilder};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_test_env::TestResult;

use super::*;
use crate::controllers::substrate::kubernetes::store::PlacementRetryDisposition;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

const NODE_NAME: &str = "space-test-execution-instance-node";

fn node(provider_id: Option<&str>) -> Node {
    Node {
        metadata: kube::api::ObjectMeta {
            name: Some(NODE_NAME.to_string()),
            uid: Some("uid-1".to_string()),
            ..Default::default()
        },
        spec: Some(K8sNodeSpec {
            provider_id: provider_id.map(str::to_string),
            ..Default::default()
        }),
        status: Some(K8sNodeStatus {
            addresses: Some(vec![K8sNodeAddress {
                type_: "InternalIP".to_string(),
                address: "10.0.0.80".to_string(),
            }]),
            ..Default::default()
        }),
    }
}

/// An apiserver that serves `node` to a GET of it and answers 404 otherwise.
fn apiserver_serving(node: Option<Node>) -> Client {
    let service = tower::service_fn(move |req: Request<Body>| {
        let node = node.clone();
        async move {
            let path = format!("/api/v1/nodes/{NODE_NAME}");
            let response = match (req.method(), node) {
                (&Method::GET, Some(node)) if req.uri().path() == path => {
                    Response::new(Body::from(serde_json::to_vec(&node)?))
                }
                _ => {
                    let status = serde_json::json!({
                        "kind": "Status",
                        "apiVersion": "v1",
                        "status": "Failure",
                        "reason": "NotFound",
                        "code": 404,
                    });
                    let mut response = Response::new(Body::from(serde_json::to_vec(&status)?));
                    *response.status_mut() = StatusCode::NOT_FOUND;
                    response
                }
            };
            Ok::<_, BoxError>(response)
        }
    });
    ClientBuilder::new(service, "space").build()
}

/// Where the Node comes from, and the instance the execution records.
struct Case {
    reflected: Option<Node>,
    served: Option<Node>,
    /// `None` when placement fails.
    recorded: Option<Option<&'static str>>,
}

#[tokio::test(flavor = "current_thread")]
async fn pod_placement_records_the_instance_of_the_node_it_read() -> TestResult {
    let aws = || Some(node(Some("aws:///us-west-2a/i-0e")));
    let cases = [
        Case {
            reflected: aws(),
            served: None,
            recorded: Some(Some("i-0e")),
        },
        Case {
            reflected: Some(node(Some("kind://docker/space/node"))),
            served: None,
            recorded: Some(None),
        },
        Case {
            reflected: Some(node(None)),
            served: None,
            recorded: Some(None),
        },
        Case {
            reflected: None,
            served: aws(),
            recorded: Some(Some("i-0e")),
        },
        Case {
            reflected: None,
            served: None,
            recorded: None,
        },
    ];
    for (index, case) in cases.into_iter().enumerate() {
        let db = pplx_space_db_test_fixtures::pg_test_db().await?;
        let sandbox_id = SandboxId::new();
        insert_test_sandbox_row(&db.pool, sandbox_id, "started").await?;
        insert_k8s_execution(
            &db.pool,
            sandbox_id,
            1,
            "space-sbx-execution-instance-g1",
            "pod-uid-1",
            None,
            true,
        )
        .await?;
        let substrate = substrate_with_store_client_pods_and_nodes(
            db.pool.clone(),
            apiserver_serving(case.served),
            Vec::new(),
            case.reflected.into_iter().collect(),
        );
        let pod = SandboxPodView::from(Pod {
            metadata: kube::api::ObjectMeta {
                name: Some("space-sbx-execution-instance-g1".to_string()),
                ..Default::default()
            },
            spec: Some(K8sPodSpec {
                node_name: Some(NODE_NAME.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let placed = substrate
            .remember_pod_node_placement(
                sandbox_id,
                &pod,
                PlacementRetryDisposition::RetryConverges,
            )
            .await;

        let recorded: (Option<String>, Option<String>) = sqlx::query_as(sql!(
            "SELECT k8s_node_name, node_instance_id FROM sandbox_k8s_executions
             WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        match case.recorded {
            Some(instance) => {
                assert!(placed?.is_some(), "case {index}");
                assert_eq!(
                    recorded,
                    (Some(NODE_NAME.to_string()), instance.map(str::to_string)),
                    "case {index}"
                );
            }
            None => {
                assert!(placed.is_err(), "case {index}");
                assert_eq!(recorded, (None, None), "case {index}");
            }
        }
    }
    Ok(())
}
