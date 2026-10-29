use super::*;

#[tokio::test]
async fn publication_requires_authenticated_authority_before_any_write() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let service = ClusterApiService::new(db.pool.clone());
    let error = service
        .register_template(Request::new(RegisterTemplateRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            name: "seed".into(),
            description: None,
            org_id: None,
        }))
        .await
        .expect_err("an unauthenticated caller cannot reserve an operator output");
    assert_eq!(error.code(), Code::PermissionDenied);
    let error = service
        .register_template_release(Request::new(RegisterTemplateReleaseRequest {
            vcpu: None,
            memory_mib: None,
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
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

mod direct;

#[tokio::test]
async fn publication_preserves_release_resources_across_retries() -> TestResult {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let service = ClusterApiService::new(db.pool.clone());
    register_template(&service, "code-runtime").await?;
    let request = RegisterTemplateReleaseRequest {
        template_name: "code-runtime".into(),
        runtime: "fc".into(),
        hash: format!("sha256:{}", "b".repeat(64)),
        tags: vec!["latest".into()],
        vcpu: Some(4),
        memory_mib: Some(4096),
        ..Default::default()
    };
    for (cpu, memory, expected) in [
        (Some(4), None, Some(Code::InvalidArgument)),
        (Some(0), Some(4096), Some(Code::InvalidArgument)),
        (Some(4), Some(4096), None),
        (Some(4), Some(4096), None),
        (Some(8), Some(8192), Some(Code::AlreadyExists)),
        (None, None, None),
    ] {
        let result = service
            .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
                vcpu: cpu,
                memory_mib: memory,
                ..request.clone()
            }))
            .await;
        assert_eq!(
            result.as_ref().err().map(|e| e.code()),
            expected,
            "{result:?}"
        );
    }
    let stored: (i32, i32) = sqlx::query_as(sql!(
        "SELECT vcpu, memory_mib FROM template_releases WHERE template_name = 'code-runtime'"
    ))
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(stored, (4, 4096));
    Ok(())
}
