use std::sync::Mutex;

use super::*;

/// Records the client each admission was charged to.
#[derive(Default)]
struct RecordingAdmission(Mutex<Vec<(ActivationAdmissionClass, ClientKey)>>);

impl ActivationAdmission for RecordingAdmission {
    fn admit<'a>(
        &'a self,
        class: ActivationAdmissionClass,
        client: &'a ClientKey,
    ) -> ActivationAdmissionFuture<'a> {
        self.0.lock().unwrap().push((class, client.clone()));
        Box::pin(async { Ok(()) })
    }
}

/// Admission sees the activating client, and a charge with no client in hand
/// is charged as `unknown`. Exempt charges never reach admission, but still
/// carry their client to the substrate.
#[tokio::test(flavor = "current_thread")]
async fn a_charge_admits_on_behalf_of_its_client() {
    let admission = Arc::new(RecordingAdmission::default());
    let background = ClientKey::allowlisted("background-client").expect("valid client key");

    RecoveryCharge::enforce(Some(&admission), Some(background.clone()))
        .charge_restore_claim()
        .await
        .expect("admitted");
    RecoveryCharge::enforce(Some(&admission), None)
        .charge_restore_claim()
        .await
        .expect("admitted");
    let exempt = RecoveryCharge::exempt(Some(background.clone()));
    exempt.charge_restore_claim().await.expect("exempt");

    assert_eq!(
        *admission.0.lock().unwrap(),
        vec![
            (ActivationAdmissionClass::Restore, background.clone()),
            (ActivationAdmissionClass::Restore, ClientKey::unknown()),
        ]
    );
    assert_eq!(exempt.client(), Some(&background));
    assert_eq!(RecoveryCharge::EXEMPT.client(), None);
}
