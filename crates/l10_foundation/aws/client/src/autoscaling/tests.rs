use http::StatusCode;

use super::*;
use crate::query::QueryResponse;
use crate::query::test_support::{RecordingTransport, credentials_provider, parse_body};

#[tokio::test]
async fn terminate_instance_request_preserves_decrement_flag() {
    let transport = RecordingTransport::new(vec![QueryResponse::new(
        StatusCode::OK,
        r#"<TerminateInstanceInAutoScalingGroupResponse/>"#,
    )]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport.clone());

    client
        .terminate_instance_in_auto_scaling_group("i-abc123", false)
        .await
        .unwrap();

    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    let body = parse_body(&requests[0].body);
    assert_eq!(body["Action"], TERMINATE_INSTANCE_ACTION);
    assert_eq!(body["Version"], AUTOSCALING_API_VERSION);
    assert_eq!(body["InstanceId"], "i-abc123");
    assert_eq!(body["ShouldDecrementDesiredCapacity"], "false");
}

#[tokio::test]
async fn terminate_error_response_includes_code_and_message() {
    let transport = RecordingTransport::new(vec![QueryResponse::new(
        StatusCode::BAD_REQUEST,
        r#"
<ErrorResponse>
<Error>
<Code>ValidationError</Code>
<Message>instance is not in an Auto Scaling group</Message>
</Error>
</ErrorResponse>
"#,
    )]);
    let client = Client::with_transport("us-west-2", credentials_provider(), transport);

    let err = client
        .terminate_instance_in_auto_scaling_group("i-missing", true)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("ValidationError"));
    assert!(msg.contains("instance is not in an Auto Scaling group"));
}
