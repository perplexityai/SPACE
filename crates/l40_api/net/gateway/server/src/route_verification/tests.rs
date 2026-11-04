use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::Ed25519KeyPair;

use super::*;
use pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Signer;

fn key(key_id: &str) -> SpaceApiRouteEd25519VerificationKey {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    SpaceApiRouteEd25519Signer::from_pkcs8(
        SpaceApiRouteEd25519KeyId::parse(key_id).unwrap(),
        pkcs8.as_ref(),
    )
    .unwrap()
    .public_key()
}

#[test]
fn key_updates_are_monotonic_idempotent_and_bounded() {
    let state = RouteVerificationState::default();
    let cluster_id = ClusterId::parse("testing").unwrap();
    let keys = vec![key("current"), key("next")];
    let generation = SpaceApiRouteEd25519VerificationGeneration::new(7).unwrap();
    let stale_generation = SpaceApiRouteEd25519VerificationGeneration::new(6).unwrap();
    let issuer = SpaceApiRouteEd25519Issuer::parse("cluster-api").unwrap();
    assert_eq!(
        state
            .update(generation, issuer.clone(), cluster_id.clone(), keys.clone())
            .unwrap(),
        RouteVerificationUpdate::Updated { generation }
    );
    assert_eq!(
        state
            .update(generation, issuer.clone(), cluster_id.clone(), keys.clone())
            .unwrap(),
        RouteVerificationUpdate::Unchanged { generation }
    );
    assert!(matches!(
        state.update(stale_generation, issuer, cluster_id.clone(), keys.clone()),
        Err(RouteVerificationUpdateError::StaleGeneration { .. })
    ));
    assert!(matches!(
        state.update(
            generation,
            SpaceApiRouteEd25519Issuer::parse("other").unwrap(),
            cluster_id,
            keys
        ),
        Err(RouteVerificationUpdateError::GenerationConflict { .. })
    ));
    assert_eq!(state.generation(), 7);
}
