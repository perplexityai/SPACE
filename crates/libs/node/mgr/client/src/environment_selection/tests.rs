use super::*;
use prost::Message;

fn fixture() -> ResolveExecutionEnvironmentResponse {
    let definition = EnvironmentDefinition::from_document(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../../proto/execution/v1/fixtures/e0.json"
    )))
    .unwrap();
    let execution = definition.execution_requirements().unwrap();
    ResolveExecutionEnvironmentResponse {
        definition: Some((&definition).into()),
        execution: Some((&execution).into()),
        accepted_selection: ExecutionEnvironmentSelection::ColdBoot as i32,
    }
}

#[test]
fn compatible_selection_requires_intent_and_exact_execution_acknowledgement() {
    let response = fixture();
    let request = Compatible::ColdBootSource(response.execution.clone().unwrap());
    decode(&request, response.clone()).unwrap();
    for acknowledgement in [0, ExecutionEnvironmentSelection::Restore as i32, 99] {
        let mut response = response.clone();
        response.accepted_selection = acknowledgement;
        decode(&request, response).unwrap_err();
    }
    for mutation in [
        (|response: &mut ResolveExecutionEnvironmentResponse| {
            response.execution.as_mut().unwrap().environment = None
        }) as fn(&mut ResolveExecutionEnvironmentResponse),
        |response| {
            response
                .execution
                .as_mut()
                .unwrap()
                .runtime
                .as_mut()
                .unwrap()
                .version = "99.0.0".into()
        },
        |response| response.execution.as_mut().unwrap().components.clear(),
    ] {
        let mut response = response.clone();
        mutation(&mut response);
        decode(&request, response).unwrap_err();
    }
}

#[test]
fn old_readers_ignore_selection_and_new_readers_refuse_missing_acknowledgement() {
    #[derive(Clone, PartialEq, Message)]
    struct OldRequest {
        #[prost(string, optional, tag = "1")]
        reference: Option<String>,
    }
    #[derive(Clone, PartialEq, Message)]
    struct OldResponse {
        #[prost(message, optional, tag = "1")]
        definition: Option<pplx_space_execution_common::proto::EnvironmentDefinition>,
        #[prost(message, optional, tag = "2")]
        execution: Option<pplx_space_execution_common::proto::ExecutionMetadata>,
    }
    let response = fixture();
    let compatible = Compatible::ColdBootSource(response.execution.clone().unwrap());
    let request = crate::proto::ResolveExecutionEnvironmentRequest {
        reference: None,
        compatible: Some(compatible.clone()),
    };
    assert_eq!(
        OldRequest::decode(request.encode_to_vec().as_slice())
            .unwrap()
            .reference,
        None
    );
    let old_response = OldResponse::decode(response.encode_to_vec().as_slice()).unwrap();
    let response =
        ResolveExecutionEnvironmentResponse::decode(old_response.encode_to_vec().as_slice())
            .unwrap();
    assert!(matches!(
        decode(&compatible, response),
        Err(SelectionResponseError::Unsupported)
    ));
}
