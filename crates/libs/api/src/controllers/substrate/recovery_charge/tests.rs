use std::sync::Mutex;

use super::*;

/// Counts admissions by class.
#[derive(Default)]
struct RecordingAdmission(Mutex<Vec<ActivationAdmissionClass>>);

impl ActivationAdmission for RecordingAdmission {
    fn admit(&self, class: ActivationAdmissionClass) -> ActivationAdmissionFuture<'_> {
        self.0.lock().unwrap().push(class);
        Box::pin(async { Ok(()) })
    }
}

fn alpha() -> ClientId {
    ClientId::from_header(Some("alpha"))
}

/// Only an enforcing policy with a configured limiter reaches admission, and
/// a policy that has charged is paid for the same client.
#[tokio::test(flavor = "current_thread")]
async fn only_an_enforcing_policy_charges() {
    let admission = Arc::new(RecordingAdmission::default());
    let enforce = RecoveryCharge::enforce(
        Some(&admission),
        CapPolicy::ReserveAtClaim,
        ActivationAttribution::client(alpha()),
    );
    enforce.charge_restore_claim().await.expect("admitted");
    let paid = enforce.after_charge();
    paid.charge_restore_claim().await.expect("paid");
    RecoveryCharge::paid(CapPolicy::Skip, ActivationAttribution::native())
        .charge_restore_claim()
        .await
        .expect("paid");
    RecoveryCharge::exempt(CapPolicy::Skip, ActivationAttribution::inherit_unresolved())
        .after_charge()
        .charge_restore_claim()
        .await
        .expect("exempt");
    RecoveryCharge::enforce::<RecordingAdmission>(
        None,
        CapPolicy::Check,
        ActivationAttribution::native(),
    )
    .charge_restore_claim()
    .await
    .expect("no limiter");

    assert_eq!(
        *admission.0.lock().unwrap(),
        vec![ActivationAdmissionClass::Restore]
    );
    assert_eq!(paid.state(), ChargeState::Paid);
    assert_eq!(paid.cap(), CapPolicy::ReserveAtClaim);
    assert_eq!(paid.attribution(), &ActivationAttribution::client(alpha()));
    assert_eq!(
        RecoveryCharge::exempt(CapPolicy::Skip, ActivationAttribution::native())
            .after_charge()
            .state(),
        ChargeState::Exempt
    );
}

/// Each kind of activation resolves to one client id (Decision 1's table).
#[test]
fn each_activation_client_resolves_to_one_id() {
    let beta = ClientId::from_header(Some("beta"));
    let cases = [
        (
            ActivationClient::Client(alpha()),
            Some(&beta),
            Some(alpha()),
        ),
        (ActivationClient::Client(alpha()), None, Some(alpha())),
        (ActivationClient::Inherit, Some(&beta), Some(beta.clone())),
        (
            ActivationClient::Inherit,
            Some(&ClientId::native()),
            Some(ClientId::native()),
        ),
        (ActivationClient::Inherit, None, None),
        (
            ActivationClient::Native,
            Some(&beta),
            Some(ClientId::native()),
        ),
    ];
    for (client, record, label) in cases {
        let attribution = ActivationAttribution::resolve(client.clone(), record);
        assert_eq!(attribution.activation_client(), &client);
        assert_eq!(
            attribution.key(),
            &KeyResolution::Resolved(label.clone()),
            "{client:?} with record {record:?}"
        );
        // A resolved key ignores whatever the substrate later reads.
        assert_eq!(
            attribution.key().pod_label(Some(&ClientId::unknown())),
            label
        );
    }
    assert_eq!(
        KeyResolution::Unresolved.pod_label(Some(&beta)),
        Some(beta.clone())
    );
    assert_eq!(KeyResolution::Unresolved.pod_label(None), None);
}
