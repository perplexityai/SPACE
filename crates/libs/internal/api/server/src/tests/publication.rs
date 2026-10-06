use super::*;

#[tokio::test]
async fn publication_requires_authenticated_authority_before_any_write() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let service = ClusterApiService::new(db.pool.clone());
    let error = service
        .register_template(Request::new(RegisterTemplateRequest {
            system_publication_grant_id: None,
            name: "seed".into(),
            description: None,
            org_id: None,
        }))
        .await
        .expect_err("an unauthenticated caller cannot reserve an operator output");
    assert_eq!(error.code(), Code::PermissionDenied);
    let error = service
        .register_template_release(Request::new(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            template_name: "seed".into(),
            runtime: "fc".into(),
            hash: format!("sha256:{}", "a".repeat(64)),
            tags: vec!["latest".into()],
            storage_connection_strings: vec![],
            org_id: None,
            execution: None,
        }))
        .await
        .expect_err("publication must authenticate before looking up the template");
    assert_eq!(error.code(), Code::PermissionDenied);
    let count: i64 = sqlx::query_scalar(sql!("SELECT count(*) FROM templates WHERE name = 'seed'"))
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[cfg(feature = "kubernetes")]
mod rpc;
