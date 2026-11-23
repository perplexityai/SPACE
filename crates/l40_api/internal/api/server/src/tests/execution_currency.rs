//! `GetExecutionCurrency`: whether an execution generation is still the
//! sandbox's active execution, answered only to the node its pod was placed
//! on.

use super::*;

use crate::proto::{ExecutionCurrency, GetExecutionCurrencyRequest};

/// A retired generation reads as retired and the active one as current, to
/// the node the pods were placed on. A generation with no row, and any other
/// caller, get no answer.
#[tokio::test(flavor = "current_thread")]
async fn execution_currency_is_answered_to_the_execution_node() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, other_ip) = seed_sandbox_on_source_node(pool).await?;
    name_node(pool, source_ip, "worker-a").await?;
    name_node(pool, other_ip, "worker-b").await?;
    insert_execution_on(pool, sandbox_id, 1, false, "worker-a").await?;
    insert_execution_on(pool, sandbox_id, 2, true, "worker-a").await?;
    let currency = |generation, node_ip| {
        fixture.service.get_execution_currency(request_from_node(
            GetExecutionCurrencyRequest {
                sandbox_id: sandbox_id.to_string(),
                execution_generation: generation,
            },
            node_ip,
        ))
    };

    assert_eq!(
        currency(1, source_ip).await?.into_inner().currency(),
        ExecutionCurrency::Retired
    );
    assert_eq!(
        currency(2, source_ip).await?.into_inner().currency(),
        ExecutionCurrency::Current
    );
    assert_eq!(
        currency(3, source_ip).await?.into_inner().currency(),
        ExecutionCurrency::Unspecified,
        "no execution of that generation"
    );
    assert_eq!(
        currency(1, other_ip).await?.into_inner().currency(),
        ExecutionCurrency::Unspecified,
        "another node learns nothing about the execution"
    );
    assert_eq!(
        currency(0, source_ip)
            .await
            .expect_err("a generation names a positive execution")
            .code(),
        tonic::Code::InvalidArgument
    );
    Ok(())
}
