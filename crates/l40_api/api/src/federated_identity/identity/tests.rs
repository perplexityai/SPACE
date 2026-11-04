use super::*;
use pplx_space_common_core::{
    sql,
    types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId},
};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;

fn owner(issuer: &str, org: &str, subject: &str) -> TestResult<FederatedOwner> {
    Ok(FederatedOwner {
        issuer: UpstreamIssuer::parse(issuer)?,
        org_id: UpstreamOrgId::parse(org)?,
        subject_id: UpstreamSubjectId::parse(subject)?,
    })
}

#[tokio::test]
async fn concurrent_materialization_converges_without_crossing_owner_namespaces() -> TestResult {
    let db = pg_test_db().await?;
    let owner = owner("example-issuer", "tenant", "subject")?;
    let results =
        futures_util::future::try_join_all((0..8).map(|_| materialize_owner(&db.pool, &owner)))
            .await?;
    let first = results.first().ok_or("no materialization results")?;
    assert!(results.iter().all(|result| result == first));
    let (user, org) = resolve_principal(&db.pool, &owner).await?;
    assert_eq!(
        (user.id, org.id, user.email),
        (first.user_id, first.org_id, first.email.clone())
    );

    let other_subject = FederatedOwner {
        subject_id: UpstreamSubjectId::parse("another-subject")?,
        ..owner.clone()
    };
    let other_tenant = FederatedOwner {
        org_id: UpstreamOrgId::parse("another-tenant")?,
        ..owner.clone()
    };
    let other_issuer = FederatedOwner {
        issuer: UpstreamIssuer::parse("another-issuer")?,
        ..owner.clone()
    };
    for (different, same_org) in [
        (other_subject, true),
        (other_tenant, false),
        (other_issuer, false),
    ] {
        let mapped = materialize_owner(&db.pool, &different).await?;
        assert_eq!(mapped.org_id == first.org_id, same_org);
        assert_ne!(mapped.user_id, first.user_id);
    }
    Ok(())
}

#[tokio::test]
async fn resolution_materializes_a_missing_subject_in_an_existing_organization() -> TestResult {
    let db = pg_test_db().await?;
    let owner = owner("example-issuer", "tenant", "subject")?;
    let first = materialize_owner(&db.pool, &owner).await?;
    sqlx::query(sql!("DELETE FROM users WHERE id = $1"))
        .bind(first.user_id)
        .execute(&db.pool)
        .await?;
    let (user, org) = resolve_principal(&db.pool, &owner).await?;
    assert_eq!(org.id, first.org_id);
    assert_eq!(user.org_id, first.org_id);
    assert_ne!(user.id, first.user_id);
    let repeated = materialize_owner(&db.pool, &owner).await?;
    assert_eq!(repeated.user_id, user.id);
    assert_eq!(repeated.email, user.email);
    Ok(())
}

#[tokio::test]
async fn failed_subject_insert_rolls_back_the_new_organization() -> TestResult {
    let db = pg_test_db().await?;
    sqlx::query(sql!(
        "CREATE FUNCTION reject_federated_subject() RETURNS trigger AS $$
         BEGIN RAISE EXCEPTION 'synthetic subject failure'; END;
         $$ LANGUAGE plpgsql"
    ))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER reject_federated_subject BEFORE INSERT ON users
         FOR EACH ROW EXECUTE FUNCTION reject_federated_subject()"
    ))
    .execute(&db.pool)
    .await?;
    let owner = owner("example-issuer", "rollback-tenant", "subject")?;
    let error = materialize_owner(&db.pool, &owner)
        .await
        .expect_err("the injected subject failure must abort materialization");
    assert!(matches!(error, MaterializeOwnerError::User(_)));
    let count: i64 = sqlx::query_scalar(sql!(
        "SELECT count(*) FROM organizations WHERE upstream_issuer = $1 AND upstream_org_id = $2"
    ))
    .bind(owner.issuer.as_str())
    .bind(owner.org_id.as_str())
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(count, 0);
    Ok(())
}
