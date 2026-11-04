use super::*;
use std::sync::Mutex;

struct StubClient {
    result: Mutex<Result<Vec<MatchedInstance>, String>>,
}

impl StubClient {
    fn ok(matches: Vec<MatchedInstance>) -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(Ok(matches)),
        })
    }

    fn err(msg: &str) -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(Err(msg.to_string())),
        })
    }
}

impl DescribeInstancesClient for StubClient {
    fn describe_instances_by_private_dns(
        &self,
        _private_dns_name: String,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<MatchedInstance>, String>> + Send + '_>> {
        let result = self.result.lock().unwrap().clone();
        Box::pin(async move { result })
    }
}

#[tokio::test]
async fn no_matches_is_gone() {
    let lookup = Ec2InstanceLookup::new(StubClient::ok(vec![]));
    assert_eq!(
        lookup
            .lookup_by_private_dns("ip-10-0-0-1.us-west-2.compute.internal".into())
            .await
            .unwrap(),
        InstanceLookupOutcome::Gone,
    );
}

#[tokio::test]
async fn only_terminated_match_is_gone() {
    let lookup = Ec2InstanceLookup::new(StubClient::ok(vec![MatchedInstance {
        instance_id: "i-old".into(),
        state: "terminated".into(),
    }]));
    assert_eq!(
        lookup
            .lookup_by_private_dns("ip-10-0-0-1.us-west-2.compute.internal".into())
            .await
            .unwrap(),
        InstanceLookupOutcome::Gone,
    );
}

#[tokio::test]
async fn running_match_is_live() {
    let lookup = Ec2InstanceLookup::new(StubClient::ok(vec![MatchedInstance {
        instance_id: "i-live".into(),
        state: "running".into(),
    }]));
    match lookup
        .lookup_by_private_dns("ip-10-0-0-1.us-west-2.compute.internal".into())
        .await
        .unwrap()
    {
        InstanceLookupOutcome::Live { instance_id, state } => {
            assert_eq!(instance_id, "i-live");
            assert_eq!(state, "running");
        }
        other => panic!("expected Live, got {other:?}"),
    }
}

#[tokio::test]
async fn recycled_ip_returns_live_when_any_match_is_live() {
    // EC2 returns one terminated and one running with the same private
    // DNS — IP recycle scenario. Must refuse (Live).
    let lookup = Ec2InstanceLookup::new(StubClient::ok(vec![
        MatchedInstance {
            instance_id: "i-old".into(),
            state: "terminated".into(),
        },
        MatchedInstance {
            instance_id: "i-new".into(),
            state: "running".into(),
        },
    ]));
    assert!(matches!(
        lookup
            .lookup_by_private_dns("ip-10-0-0-1.us-west-2.compute.internal".into())
            .await
            .unwrap(),
        InstanceLookupOutcome::Live { .. }
    ));
}

#[tokio::test]
async fn unknown_state_fails_closed_to_live() {
    // EC2 may add states in the future. An unknown state must count
    // as live, not as gone — that's the entire fail-closed contract.
    let lookup = Ec2InstanceLookup::new(StubClient::ok(vec![MatchedInstance {
        instance_id: "i-unknown".into(),
        state: "future-mystery-state".into(),
    }]));
    match lookup
        .lookup_by_private_dns("ip-10-0-0-1.us-west-2.compute.internal".into())
        .await
        .unwrap()
    {
        InstanceLookupOutcome::Live { instance_id, state } => {
            assert_eq!(instance_id, "i-unknown");
            assert_eq!(state, "future-mystery-state");
        }
        other => panic!("expected Live for unknown state, got {other:?}"),
    }
}

#[tokio::test]
async fn backend_error_propagates() {
    let lookup = Ec2InstanceLookup::new(StubClient::err("ec2: throttled"));
    match lookup
        .lookup_by_private_dns("ip-10-0-0-1.us-west-2.compute.internal".into())
        .await
        .unwrap_err()
    {
        InstanceLookupError::Backend(msg) => assert_eq!(msg, "ec2: throttled"),
    }
}
