//! Shared Postgres testcontainer fixture for migrator tests.
//!
//! Starts a throwaway Postgres via Docker. When no container runtime is
//! reachable it fails the test, or returns `Ok(None)` for the test to skip
//! when the run opted out, by the same rule as the shared fixtures
//! (`pplx_space_db_test_fixtures::report_unavailable_dependency`).

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection as _, PgConnection, PgPool};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres as PostgresImage;

pub(crate) use pplx_space_test_env::TestResult;

pub(crate) struct PostgresFixture {
    pub(crate) pool: PgPool,
    host: String,
    port: u16,
    _container: ContainerAsync<PostgresImage>,
}

impl PostgresFixture {
    /// Connection URL for `user`. The fixture container trusts all host
    /// connections, so any LOGIN role connects without a password.
    pub(crate) fn database_url(&self, user: &str) -> String {
        format!(
            "postgres://{user}@{}:{}/postgres?sslmode=disable",
            self.host, self.port
        )
    }

    pub(crate) async fn connect_as(&self, user: &str) -> TestResult<PgConnection> {
        Ok(PgConnection::connect(&self.database_url(user)).await?)
    }
}

pub(crate) async fn postgres_fixture() -> TestResult<Option<PostgresFixture>> {
    let (image_name, image_tag) = pplx_space_db_test_fixtures::test_image_ref(
        "TESTCONTAINERS_POSTGRES_IMAGE",
        "public.ecr.aws/docker/library/postgres:16-alpine",
    )?;
    let container = match PostgresImage::default()
        .with_host_auth()
        .with_name(image_name)
        .with_tag(image_tag)
        .start()
        .await
    {
        Ok(container) => container,
        Err(error) => {
            pplx_space_db_test_fixtures::report_unavailable_dependency("postgres", &error)?;
            return Ok(None);
        }
    };
    let host = container.get_host().await?.to_string();
    let port = container.get_host_port_ipv4(5432).await?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "postgres://postgres@{host}:{port}/postgres?sslmode=disable"
        ))
        .await?;
    Ok(Some(PostgresFixture {
        pool,
        host,
        port,
        _container: container,
    }))
}
