use http::StatusCode;

use super::*;
use crate::query::QueryResponse;
use crate::query::test_support::{RecordingTransport, credentials_provider, parse_body};

const PAGE_ONE: &str = r#"
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
<requestId>req-1</requestId>
<reservationSet>
<item>
  <instancesSet>
    <item>
      <instanceId>i-old</instanceId>
      <instanceState><code>48</code><name>terminated</name></instanceState>
    </item>
    <item>
      <instanceId>i-live</instanceId>
      <instanceState><code>16</code><name>running</name></instanceState>
    </item>
  </instancesSet>
</item>
</reservationSet>
<nextToken>token-2</nextToken>
</DescribeInstancesResponse>
"#;

const PAGE_TWO: &str = r#"
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
<reservationSet>
<item>
  <instancesSet>
    <item>
      <instanceId>i-unknown</instanceId>
    </item>
  </instancesSet>
</item>
</reservationSet>
</DescribeInstancesResponse>
"#;

#[test]
fn parses_instances_and_next_token() {
    let page = parse_describe_instances_response(PAGE_ONE).unwrap();
    assert_eq!(
        page.instances,
        vec![
            Instance {
                instance_id: "i-old".to_string(),
                state: "terminated".to_string(),
            },
            Instance {
                instance_id: "i-live".to_string(),
                state: "running".to_string(),
            },
        ]
    );
    assert_eq!(page.next_token.as_deref(), Some("token-2"));
}

#[test]
fn missing_state_is_unknown_and_missing_id_is_skipped() {
    let page = parse_describe_instances_response(
        r#"
<DescribeInstancesResponse>
<reservationSet>
<item>
  <instancesSet>
    <item><instanceId>i-no-state</instanceId></item>
    <item><instanceState><name>running</name></instanceState></item>
  </instancesSet>
</item>
</reservationSet>
</DescribeInstancesResponse>
"#,
    )
    .unwrap();
    assert_eq!(
        page.instances,
        vec![Instance {
            instance_id: "i-no-state".to_string(),
            state: "unknown".to_string(),
        }]
    );
}

#[tokio::test]
async fn describe_instances_paginates_and_preserves_filter_shape() {
    let transport = RecordingTransport::new(vec![
        QueryResponse::new(StatusCode::OK, PAGE_ONE),
        QueryResponse::new(StatusCode::OK, PAGE_TWO),
    ]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());

    let instances = client
        .describe_instances_by_private_dns("ip-10-0-0-1.us-west-2.compute.internal")
        .await
        .unwrap();

    assert_eq!(instances.len(), 3);
    let requests = transport.requests();
    assert_eq!(requests.len(), 2);

    let first_body = parse_body(&requests[0].body);
    assert_eq!(first_body["Action"], "DescribeInstances");
    assert_eq!(first_body["Version"], EC2_API_VERSION);
    assert_eq!(first_body["Filter.1.Name"], "private-dns-name");
    assert_eq!(
        first_body["Filter.1.Value.1"],
        "ip-10-0-0-1.us-west-2.compute.internal"
    );
    assert_eq!(first_body["MaxResults"], DESCRIBE_PAGE_SIZE);
    assert!(!first_body.contains_key("NextToken"));

    let second_body = parse_body(&requests[1].body);
    assert_eq!(second_body["NextToken"], "token-2");
}

/// The id lookup filters on `instance-id` with one value per id, pages
/// within a call, and splits more ids than one filter takes into calls of at
/// most [`MAX_FILTER_VALUES`].
#[tokio::test]
async fn describe_instances_by_ids_filters_pages_and_chunks() {
    let ids: Vec<String> = (0..=MAX_FILTER_VALUES)
        .map(|n| format!("i-{n:017x}"))
        .collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let transport = RecordingTransport::new(vec![
        QueryResponse::new(StatusCode::OK, PAGE_ONE),
        QueryResponse::new(StatusCode::OK, PAGE_TWO),
        QueryResponse::new(StatusCode::OK, PAGE_TWO),
    ]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());

    let instances = client.describe_instances_by_ids(&ids).await.unwrap();

    assert_eq!(
        instances
            .iter()
            .map(|instance| instance.instance_id.as_str())
            .collect::<Vec<_>>(),
        ["i-old", "i-live", "i-unknown", "i-unknown"]
    );
    let bodies: Vec<_> = transport
        .requests()
        .iter()
        .map(|request| parse_body(&request.body))
        .collect();
    assert_eq!(bodies.len(), 3);
    for body in &bodies {
        assert_eq!(body["Action"], "DescribeInstances");
        assert_eq!(body["Filter.1.Name"], "instance-id");
        assert!(!body.contains_key("InstanceId.1"), "{body:?}");
    }
    let values = |body: &std::collections::HashMap<String, String>| {
        (1..)
            .map_while(|n| body.get(&format!("Filter.1.Value.{n}")).cloned())
            .collect::<Vec<_>>()
    };
    // The first call's two pages carry the first chunk; the second call
    // carries the one id left.
    assert_eq!(values(&bodies[0]), ids[..MAX_FILTER_VALUES]);
    assert!(!bodies[0].contains_key("NextToken"));
    assert_eq!(values(&bodies[1]), ids[..MAX_FILTER_VALUES]);
    assert_eq!(bodies[1]["NextToken"], "token-2");
    assert_eq!(values(&bodies[2]), ids[MAX_FILTER_VALUES..]);
    assert!(!bodies[2].contains_key("NextToken"));
}

/// No ids asks nothing.
#[tokio::test]
async fn describe_instances_by_no_ids_sends_no_request() {
    let transport = RecordingTransport::new(vec![]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());
    assert_eq!(client.describe_instances_by_ids(&[]).await.unwrap(), vec![]);
    assert!(transport.requests().is_empty());
}

#[tokio::test]
async fn aws_error_response_includes_code_and_message() {
    let transport = RecordingTransport::new(vec![QueryResponse::new(
        StatusCode::BAD_REQUEST,
        r#"
<Response>
<Errors>
<Error>
  <Code>InvalidParameterValue</Code>
  <Message>bad private dns</Message>
</Error>
</Errors>
</Response>
"#,
    )]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport);

    let err = client
        .describe_instances_by_private_dns("bad")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("InvalidParameterValue"));
    assert!(msg.contains("bad private dns"));
}
