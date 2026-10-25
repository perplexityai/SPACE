use super::*;
use futures_util::StreamExt;
use pplx_space_spaced_common::proto::bridge_guest::{
    ProcessConnected, ProcessEvent, ProcessExit, ProcessOutput, ProcessStarted,
    process_event::Event,
};

#[tokio::test]
async fn output_gap_opt_in_requires_acknowledgement_before_relay() {
    let event = |event| ProcessEvent {
        process_id: "process".into(),
        event,
        ..Default::default()
    };
    let acknowledged = event(Some(Event::Connected(ProcessConnected {
        resume_after_output_gap: true,
        ..Default::default()
    })));
    for output_cursor in [None, Some(0)] {
        for (first, expected) in [
            (Some(Ok(acknowledged.clone())), None),
            (
                Some(Ok(event(Some(Event::Connected(
                    ProcessConnected::default(),
                ))))),
                Some(tonic::Code::FailedPrecondition),
            ),
            (
                Some(Ok(event(Some(Event::Started(ProcessStarted::default()))))),
                output_cursor.map(|_| tonic::Code::FailedPrecondition),
            ),
            (
                Some(Ok(event(Some(Event::Stdout(ProcessOutput::default()))))),
                Some(tonic::Code::FailedPrecondition),
            ),
            (
                Some(Ok(event(Some(Event::Exit(ProcessExit::default()))))),
                Some(tonic::Code::FailedPrecondition),
            ),
            (Some(Ok(event(None))), Some(tonic::Code::FailedPrecondition)),
            (None, Some(tonic::Code::FailedPrecondition)),
            (
                Some(Err(Status::not_found("process expired"))),
                Some(tonic::Code::NotFound),
            ),
        ] {
            let input = futures_util::stream::iter(first.clone());
            match require_output_gap_resume_ack(input, output_cursor).await {
                Ok(stream) => {
                    assert_eq!(
                        expected, None,
                        "unconfirmed stream must be refused: {first:?}"
                    );
                    futures_util::pin_mut!(stream);
                    assert_eq!(
                        stream.next().await.unwrap().unwrap(),
                        first.unwrap().unwrap()
                    );
                    assert!(stream.next().await.is_none());
                }
                Err(error) => {
                    assert_eq!(Some(error.code()), expected);
                    if error.code() == tonic::Code::FailedPrecondition {
                        assert_eq!(
                            pplx_space_common_grpc::GuestCapabilityUnsupported::from_status(&error)
                                .unwrap()
                                .capability,
                            GuestCapability::ProcessOutputGapResume
                        );
                    } else {
                        assert_eq!(error.message(), "process expired");
                    }
                }
            }
        }
    }
}
