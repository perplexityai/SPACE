use std::time::Duration;

use aws_smithy_types::date_time::Format;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::test_support::{RecordingTransport, credentials_provider};
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn transport_times_out_hung_response() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let transport = HttpTransport::with_timeouts(Duration::from_secs(1), Duration::from_millis(50));
    let request = Request::builder()
        .method("POST")
        .uri(format!("http://{addr}/"))
        .body(Vec::new())
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), transport.send(request)).await;
    server.abort();

    match result.expect("the request timeout fired") {
        Err(AwsClientError::Http(err)) => {
            assert!(err.is_timeout(), "expected a timeout, got {err}");
        }
        result => panic!("expected a timeout, got {result:?}"),
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

fn client_with_endpoint<T: QueryTransport>(endpoint: String, transport: T) -> AwsQueryClient<T> {
    AwsQueryClient {
        endpoint_url: Some(endpoint),
        ..AwsQueryClient::with_transport(
            AwsQueryService::Ec2,
            "us-east-1",
            credentials_provider(),
            transport,
        )
    }
}

/// Accepts one request, answers 200, and returns its target and whether its
/// SigV4 signature matches one recomputed from the bytes received.
async fn receive_and_verify(listener: TcpListener) -> (String, bool) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let head_end = loop {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "connection closed mid-request");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
    };
    let head = String::from_utf8(bytes[..head_end].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let target = lines.next().unwrap().split(' ').nth(1).unwrap().to_string();
    let headers: Vec<(String, String)> = lines
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_ascii_lowercase(), value.trim().to_string())
        })
        .collect();
    let header = |name: &str| {
        headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
            .unwrap()
    };
    let body_end = head_end + 4 + header("content-length").parse::<usize>().unwrap();
    while bytes.len() < body_end {
        let read = socket.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "connection closed mid-body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    socket
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();

    let authorization = header("authorization");
    let field = |name: &str| {
        authorization
            .split(name)
            .nth(1)
            .and_then(|rest| rest.split(',').next())
            .unwrap()
    };
    // x-amz-date is YYYYMMDDTHHMMSSZ; RFC 3339 adds the separators.
    let mut rfc3339 = String::new();
    for (index, digit) in header("x-amz-date").chars().enumerate() {
        match index {
            4 | 6 => rfc3339.push('-'),
            11 | 13 => rfc3339.push(':'),
            _ => {}
        }
        rfc3339.push(digit);
    }
    let time = aws_smithy_types::DateTime::from_str(&rfc3339, Format::DateTime)
        .unwrap()
        .try_into()
        .unwrap();
    let identity: Identity = credentials_provider()
        .provide_credentials()
        .await
        .unwrap()
        .into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("ec2")
        .time(time)
        .settings(SigningSettings::default())
        .build()
        .unwrap()
        .into();
    let received = SignableRequest::new(
        "POST",
        format!("http://{}{target}", header("host")),
        field("SignedHeaders=")
            .split(';')
            .map(|name| (name, header(name))),
        SignableBody::Bytes(&bytes[head_end + 4..body_end]),
    )
    .unwrap();
    let (_, signature) = sign_http_request(received, &params).unwrap().into_parts();
    (target, signature == field("Signature="))
}

#[tokio::test(flavor = "current_thread")]
async fn sends_and_signs_the_url_2_5_8_target() {
    for (endpoint, sent) in [
        // Catches sending through url 2.5.9 or reqwest, or restoring left to right.
        ("http://127.0.0.1:{port}/a^b^c", "/a^b^c/"),
        // Catches restoring every `%5E` rather than those url encoded.
        ("http://127.0.0.1:{port}/a%5Eb", "/a%5Eb/"),
        // Catches a signer that decodes before it signs.
        ("http://127.0.0.1:{port}/a%255Eb", "/a%255Eb/"),
        // Catches sending the written URI: url 2.5.8 encoded non-ASCII.
        ("http://127.0.0.1:{port}/café", "/caf%C3%A9/"),
        // Catches restoring by position in the written path rather than url's.
        ("http://127.0.0.1:{port}/x/../a^b%5E", "/a^b%5E/"),
        // Catches signing the written host: url lowercases it.
        ("http://LOCALHOST:{port}", "/"),
    ] {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let endpoint =
            endpoint.replace("{port}", &listener.local_addr().unwrap().port().to_string());
        let server = tokio::spawn(receive_and_verify(listener));
        let client = client_with_endpoint(
            endpoint.clone(),
            HttpTransport::with_timeouts(Duration::from_secs(1), Duration::from_secs(5)),
        );

        client
            .send_action(
                "DescribeInstances",
                "2016-11-15",
                vec![("Filter.1.Value.1", "a^b".to_string())],
            )
            .await
            .unwrap();

        let (target, verifies) = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the server read the request")
            .unwrap();
        assert_eq!(target, sent, "{endpoint}");
        assert!(verifies, "{endpoint}: signature does not cover {target}");
    }
}

#[tokio::test]
async fn refuses_endpoints_before_sending() {
    for (endpoint, http_rejects) in [
        // Catches url parsing first: it would repair these.
        ("http://127.0.0.1:9/a b", true),
        (" http://127.0.0.1:9/x", true),
        ("http://127.0.0.1:9/a\tb", true),
        ("http:127.0.0.1:9/normal", true),
        // Catches sending userinfo, which reqwest sent as a Basic Authorization header.
        ("http://user:pw@127.0.0.1:9/", false),
        // Catches signing an endpoint with no authority, which panics in aws-sigv4.
        ("/relative", false),
    ] {
        let transport = RecordingTransport::new(vec![QueryResponse::new(StatusCode::OK, "")]);
        let client = client_with_endpoint(endpoint.to_string(), transport.clone());

        let result = client
            .send_action("DescribeInstances", "2016-11-15", Vec::new())
            .await;

        assert_eq!(transport.requests(), Vec::new(), "{endpoint:?} was sent");
        match result {
            Err(AwsClientError::HttpBuild(_)) if http_rejects => {}
            Err(AwsClientError::InvalidEndpoint(_)) if !http_rejects => {}
            result => panic!("{endpoint:?}: {result:?}"),
        }
    }
}
