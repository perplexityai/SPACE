use super::*;

#[test]
fn encoded_response_boundary_is_checked_including_proto_envelope() {
    let mut response_value = crate::proto::ListResponse {
        sandboxes: vec![crate::proto::SandboxListElement {
            sandbox_id: "x".repeat(MAX_ENCODED_BYTES - 10),
            ..Default::default()
        }],
        next_page_token: String::new(),
    };
    response(response_value.clone()).unwrap();
    response_value.sandboxes[0]
        .sandbox_id
        .push_str("overflowing");
    assert_eq!(
        response(response_value).unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );
}

#[test]
fn unary_list_response_serves_an_oversized_message() {
    let message = crate::proto::ListResponse {
        sandboxes: vec![crate::proto::SandboxListElement {
            sandbox_id: "x".repeat(MAX_ENCODED_BYTES + 1),
            ..Default::default()
        }],
        next_page_token: String::new(),
    };
    assert_eq!(
        unary_list_response("List", message.clone()).into_inner(),
        message
    );
}
