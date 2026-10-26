use super::*;

#[test]
fn fallback_requires_a_missing_method_and_firecracker_and_never_replays_uncertain_work() {
    pplx_space_tokio_runtime::build_multi_thread_runtime()
        .unwrap()
        .block_on(async {
            for runtime in [RuntimeKind::Firecracker, RuntimeKind::CloudHypervisor] {
                for code in [
                    tonic::Code::Unimplemented,
                    tonic::Code::Unavailable,
                    tonic::Code::DeadlineExceeded,
                    tonic::Code::Internal,
                    tonic::Code::FailedPrecondition,
                    tonic::Code::InvalidArgument,
                    tonic::Code::Cancelled,
                ] {
                    let mut fallback_called = false;
                    let result = compatible(
                        runtime,
                        Err::<(), _>(Status::new(code, "peer reply")),
                        async || {
                            fallback_called = true;
                            Ok(())
                        },
                    )
                    .await;
                    if runtime == RuntimeKind::Firecracker && code == tonic::Code::Unimplemented {
                        result.unwrap();
                        assert!(fallback_called);
                    } else {
                        assert_eq!(result.unwrap_err().code(), code);
                        assert!(!fallback_called);
                    }
                }
            }
            compatible(RuntimeKind::Firecracker, Ok(()), async || {
                panic!("successful new call must not execute a fallback")
            })
            .await
            .unwrap();
        });
}
