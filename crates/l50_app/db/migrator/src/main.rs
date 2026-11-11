use clap::Parser;
use pplx_space_db_migrate::{Database, DbMigrateRunConfig, SERVICE_NAME};

#[derive(Parser)]
#[command(
    name = "space-db-migrator",
    about = "SPACE SQLx migration runner",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    #[command(subcommand)]
    database: Database,
}

impl From<Cli> for DbMigrateRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            database: cli.database,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pplx_space_common_cli::init_logging_with_startup_banner(SERVICE_NAME);
    let cli = Cli::parse();

    Ok(pplx_space_db_migrate::run(cli.into()).await?)
}
