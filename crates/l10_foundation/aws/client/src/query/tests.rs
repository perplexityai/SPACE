use std::time::Duration;

use tokio::net::TcpListener;

use super::*;

// Verify against the URI, headers and body received by the server, not the
// endpoint string the client meant to send.
async fn verify_wire_signature(
    uri: http::Uri,
    headers: http::HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    let timestamp =
        jiff::civil::DateTime::strptime("%Y%m%dT%H%M%SZ", headers["x-amz-date"].as_bytes())
            .unwrap()
            .to_zoned(jiff::tz::TimeZone::UTC)
            .unwrap()
            .timestamp();
    let identity: Identity = test_support::credentials_provider()
        .provide_credentials()
        .await
        .unwrap()
        .into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("ec2")
        .time(timestamp.into())
        .settings(SigningSettings::default())
        .build()
        .unwrap()
        .into();
    let endpoint = format!("http://{}{uri}", headers["host"].to_str().unwrap());
    let received = SignableRequest::new(
        "POST",
        endpoint,
        [("content-type", headers["content-type"].to_str().unwrap())].into_iter(),
        SignableBody::Bytes(&body),
    )
    .unwrap();
    let (_, signature) = sign_http_request(received, &params).unwrap().into_parts();
    if headers["authorization"]
        .to_str()
        .unwrap()
        .ends_with(&format!("Signature={signature}"))
    {
        StatusCode::OK
    } else {
        StatusCode::FORBIDDEN
    }
}

#[tokio::test]
async fn custom_endpoint_signature_matches_the_transmitted_path() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let endpoint = format!("http://{}/edge^case", listener.local_addr().unwrap());
    let app =
        axum::Router::new().route("/edge%5Ecase/", axum::routing::post(verify_wire_signature));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = SdkConfig::builder()
        .region(aws_types::region::Region::new("us-east-1"))
        .credentials_provider(test_support::credentials_provider())
        .endpoint_url(endpoint)
        .build();
    let mut client = AwsQueryClient::from_config(AwsQueryService::Ec2, &config);
    client.transport.client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let result = client
        .send_action("DescribeInstances", "2016-11-15", Vec::new())
        .await;
    server.abort();
    result.expect("the signature must verify against the request on the wire");
}

#[tokio::test(flavor = "current_thread")]
async fn reqwest_transport_times_out_hung_response() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let transport =
        ReqwestTransport::with_timeouts(Duration::from_secs(1), Duration::from_millis(50));
    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/"))
        .body(Vec::new())
        .unwrap();

    let err = transport.send(request).await.unwrap_err();
    server.abort();

    match err {
        AwsClientError::Http(err) => {
            assert!(err.is_timeout(), "expected reqwest timeout, got {err}");
        }
        err => panic!("expected reqwest timeout, got {err}"),
    }
}

/// aws-config's environment loader, reduced to one service's
/// `AWS_ENDPOINT_URL_<SERVICE>` setting.
#[derive(Debug)]
struct ServiceEndpoint {
    service_id: &'static str,
    url: &'static str,
}

impl aws_types::service_config::LoadServiceConfig for ServiceEndpoint {
    fn load_config(&self, key: ServiceConfigKey<'_>) -> Option<String> {
        (key.service_id() == self.service_id
            && key.env() == "AWS_ENDPOINT_URL"
            && key.profile() == "endpoint_url")
            .then(|| self.url.to_string())
    }
}

#[test]
fn service_endpoint_setting_redirects_only_its_own_service() {
    let config = SdkConfig::builder()
        .region(aws_types::region::Region::new("us-east-1"))
        .endpoint_url("https://global.example")
        .service_config(ServiceEndpoint {
            service_id: "EC2",
            url: "https://fake-ec2:8443",
        })
        .build();

    for (service, expected) in [
        (AwsQueryService::Ec2, "https://fake-ec2:8443/"),
        (AwsQueryService::AutoScaling, "https://global.example/"),
    ] {
        let client = AwsQueryClient::from_config(service, &config);
        assert_eq!(client.endpoint().unwrap(), expected, "{service:?}");
    }
}
