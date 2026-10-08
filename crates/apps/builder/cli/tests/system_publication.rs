use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    http::{HeaderMap, Response},
    routing::post,
};
use http_body_util::StreamBody;
use hyper::body::Frame;
use pplx_space_build_mgr_client::proto::{StartBuildRequest, StartBuildResponse};
use pplx_space_node_api_client::proto::{
    CreateSystemTemplatePublicationGrantRequest, CreateSystemTemplatePublicationGrantResponse,
};
use prost::Message;

const GRANT: &str = "018f0000-0000-7000-8000-000000000004";
const ORG: &str = "018f0000-0000-7000-8000-000000000002";

fn grpc_response(message: impl Message) -> Response<Body> {
    let payload = message.encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("small gRPC fixture response")
            .to_be_bytes(),
    );
    frame.extend_from_slice(&payload);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", "0".parse().expect("valid gRPC status"));
    Response::builder()
        .header("content-type", "application/grpc")
        .body(Body::new(StreamBody::new(tokio_stream::iter([
            Ok::<_, Infallible>(Frame::data(Bytes::from(frame))),
            Ok(Frame::trailers(trailers)),
        ]))))
        .expect("valid gRPC response")
}

/// ClusterApi's authenticated RPC tests own issuer policy. This boundary test
/// runs the actual CLI and checks credential selection and pre-submission failure.
#[tokio::test]
async fn direct_build_obtains_and_forwards_operator_authority()
-> Result<(), Box<dyn std::error::Error>> {
    for (case, cluster_configured, operator_token, org, succeeds, expected_grants) in [
        ("system", true, Some("operator-token"), None, true, 1),
        ("local", false, None, None, true, 0),
        ("missing-token", true, None, None, false, 0),
        ("denied", true, Some("builder-token"), None, false, 1),
        ("tenant", true, None, Some(ORG), true, 0),
    ] {
        let directory = tempfile::tempdir()?;
        let recipe = directory.path().join("recipe.toml");
        std::fs::write(
            &recipe,
            "[metadata]\nname = 'direct-custom'\nfrom = 'template'\nbase = 'seed'\n",
        )?;
        let build_token = directory.path().join("build.token");
        let cluster_token = directory.path().join("operator.token");
        std::fs::write(&build_token, "submit-token")?;
        if let Some(token) = operator_token {
            std::fs::write(&cluster_token, token)?;
        }
        let grants = Arc::new(Mutex::new(Vec::new()));
        let builds = Arc::new(Mutex::new(Vec::new()));
        let observed_grants = grants.clone();
        let observed_builds = builds.clone();
        let router = Router::new()
            .route(
                "/space.node.api.v1.ClusterApi/CreateSystemTemplatePublicationGrant",
                post(move |headers: HeaderMap, body: Bytes| {
                    let grants = observed_grants.clone();
                    async move {
                        let request =
                            CreateSystemTemplatePublicationGrantRequest::decode(&body[5..])
                                .unwrap();
                        assert_eq!(request.template_name, "direct-custom");
                        let auth = headers["authorization"].to_str().unwrap().to_string();
                        grants.lock().unwrap().push(auth.clone());
                        if auth != "Bearer operator-token" {
                            return Response::builder()
                                .header("content-type", "application/grpc")
                                .header("grpc-status", "7")
                                .body(Body::empty())
                                .unwrap();
                        }
                        grpc_response(CreateSystemTemplatePublicationGrantResponse {
                            grant_id: GRANT.into(),
                        })
                    }
                }),
            )
            .route(
                "/build.manager.v1.BuildManager/StartBuild",
                post(move |headers: HeaderMap, body: Bytes| {
                    let builds = observed_builds.clone();
                    async move {
                        assert_eq!(headers["authorization"], "Bearer submit-token");
                        builds
                            .lock()
                            .unwrap()
                            .push(StartBuildRequest::decode(&body[5..]).unwrap());
                        grpc_response(StartBuildResponse {
                            job_id: "018f0000-0000-7000-8000-000000000005".into(),
                        })
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        });
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_space-buildmgr"));
        command
            .env_clear()
            .kill_on_drop(true)
            .env("SPACE_INTERNAL_RPC_INSECURE_DEV", "true")
            .env("SPACE_BUILD_MANAGER_ENDPOINT", &endpoint)
            .env("SPACE_BUILD_MANAGER_TOKEN_PATH", &build_token)
            .env("SPACE_CLUSTER_API_TOKEN_PATH", &cluster_token)
            .args(["build", "--id", "direct-custom", "--async", "--recipe"])
            .arg(&recipe);
        if cluster_configured {
            command.env("SPACE_INTERNAL_API_ENDPOINT", &endpoint);
        }
        if let Some(org) = org {
            command.args(["--org-id", org]);
        }
        let result = tokio::time::timeout(Duration::from_secs(10), command.output()).await;
        let _ = stop.send(());
        server.await??;
        let output = result??;
        assert_eq!(
            output.status.success(),
            succeeds,
            "{case}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(grants.lock().unwrap().len(), expected_grants, "{case}");
        let builds = builds.lock().unwrap();
        assert_eq!(builds.len(), usize::from(succeeds), "{case}");
        if let Some(request) = builds.first() {
            assert_eq!(request.template_id, "direct-custom");
            assert_eq!(request.org_id.as_deref(), org);
            assert_eq!(
                request.system_publication_grant_id.as_deref(),
                if case == "system" { Some(GRANT) } else { None }
            );
        }
    }
    Ok(())
}
