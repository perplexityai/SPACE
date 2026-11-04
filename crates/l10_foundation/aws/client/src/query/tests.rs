use std::time::Duration;

use tokio::net::TcpListener;

use super::*;

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
