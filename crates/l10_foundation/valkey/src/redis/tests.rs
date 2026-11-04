use super::cluster_client_from_url;

const URL: &str = "rediss://:test-token@configuration-endpoint.example:6379";

#[test]
fn cluster_client_construction_validates_without_connecting() {
    let _client = cluster_client_from_url(URL).unwrap();
    let error = cluster_client_from_url("https://example.com")
        .err()
        .expect("non-Redis URL should fail validation");

    assert_eq!(error.kind(), ::redis::ErrorKind::InvalidClientConfig);
}
