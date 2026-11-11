use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn destination(host: &str) -> DestinationMatcher {
    DestinationMatcher {
        exact_host: host.to_string(),
        port: 443,
    }
}

struct SecretFetch {
    value: &'static str,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl CredentialFetch<&'static str> for SecretFetch {
    async fn fetch(self: Box<Self>) -> Result<&'static str, ResolutionError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.value)
    }
}

#[tokio::test]
async fn prepared_credential_requires_nonempty_policy_destinations() {
    let calls = Arc::new(AtomicUsize::new(0));
    let make = || {
        PreparedCredential::new(SecretFetch {
            value: "must-not-appear",
            calls: calls.clone(),
        })
    };

    let debug = format!("{:?}", make());
    assert!(!debug.contains("must-not-appear"));
    assert!(debug.contains("<redacted>"));
    assert_eq!(
        make().resolve_for_destinations(&[]).await,
        Err(ResolutionError::ReferenceUnauthorized)
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        make()
            .resolve_for_destinations(&[destination("api.example.com")])
            .await
            .unwrap(),
        "must-not-appear"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
