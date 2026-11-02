use super::*;

#[test]
fn batch_entry_budget_is_enforced_before_forwarding() {
    use pplx_space_spaced_common::proto::operations::WriteBatchEntry;
    let limit = pplx_space_common_core::spaced::MAX_WRITE_BATCH_ENTRIES;
    for count in [limit, limit + 1] {
        let result = bridge_write_batch_request(WriteBatchRequest {
            entries: vec![
                WriteBatchEntry {
                    path: "/tmp/file".into(),
                    data: Vec::new(),
                    append: false
                };
                count
            ],
            ..Default::default()
        });
        if count <= limit {
            assert_eq!(result.expect("within budget").entries.len(), count);
        } else {
            assert_eq!(
                result.expect_err("over budget").code(),
                tonic::Code::InvalidArgument
            );
        }
    }
}

#[tokio::test]
async fn relay_is_lazy_and_stops_at_first_error() {
    use futures_util::StreamExt;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let source = futures_util::stream::iter([Ok(1_u8), Ok(2), Ok(3)]).inspect(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
    });
    let mut stream = relay_stream(source, |value| {
        if value == 2 {
            Err(Status::invalid_argument("bad event"))
        } else {
            Ok(value)
        }
    });
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(stream.next().await.unwrap().unwrap(), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert!(stream.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

// A guest's UNAVAILABLE stays a failure for the trace layers here and
// upstream, whatever metadata the guest attached.
#[tokio::test]
async fn relayed_guest_statuses_lose_the_not_ready_mark() {
    use futures_util::StreamExt;
    use pplx_space_common_grpc::RetryableNotReady;
    let guest_status = || RetryableNotReady.mark(Status::unavailable("guest not ready"));

    let unary = host_relay_status(guest_status(), "bridge_guest_ping", "bridge_guest_meta");
    let mut stream = relay_stream(
        futures_util::stream::iter([Err::<u8, _>(guest_status())]),
        Ok::<_, Status>,
    );
    let streamed = stream.next().await.unwrap().unwrap_err();

    for relayed in [unary, streamed] {
        assert_eq!(relayed.code(), tonic::Code::Unavailable);
        assert_eq!(RetryableNotReady::from_status(&relayed), None);
    }
}

#[test]
fn descriptive_wire_enums_preserve_future_values() {
    for value in [1, 2, 42, -1] {
        assert_eq!(
            bridge_file_type_to_host(value).expect("descriptive type"),
            value
        );
        assert_eq!(
            bridge_file_event_kind_to_host(value).expect("descriptive event"),
            value
        );
    }
    assert_eq!(
        bridge_file_type_to_host(0)
            .expect_err("missing type")
            .code(),
        tonic::Code::Internal
    );
    assert_eq!(
        bridge_file_event_kind_to_host(0)
            .expect_err("missing kind")
            .code(),
        tonic::Code::Internal
    );
    for value in [0, 42, -1] {
        assert_eq!(
            host_resume_reason_to_bridge(value)
                .expect_err("unknown control")
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}
