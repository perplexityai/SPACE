//! Thin `main` for `space-template-publish`. The job itself is
//! [`pplx_space_template_publisher`]; this binary only parses flags.

use clap::Parser;
use pplx_space_template_publisher::{SERVICE_NAME, TemplatePin, TemplatePublishRunConfig};

#[derive(Parser, Debug)]
#[command(
    name = "space-template-publish",
    about = "Publish and register a cluster's pinned template releases",
    version
)]
struct Cli {
    /// Template pin as `<name>@<content-hash>`, repeatable. Order is
    /// preserved, so a template may be listed after the one it derives from.
    #[arg(long = "template", value_name = "NAME@CONTENT_HASH", required = true)]
    templates: Vec<TemplatePin>,

    /// Destination bucket: this cluster's own template-artifacts bucket.
    #[arg(long)]
    bucket: String,

    /// Destination bucket region.
    #[arg(long)]
    region: String,

    /// Bucket already holding the pinned builds. Defaults to `--bucket`, which
    /// makes the copy step a no-op for a cluster that already has its own
    /// artifacts. A cluster in another account needs both a bucket policy on
    /// the source and this process's own read grant.
    #[arg(long)]
    source_bucket: Option<String>,

    /// Source bucket region. Defaults to `--region`.
    #[arg(long)]
    source_region: Option<String>,
}

impl From<Cli> for TemplatePublishRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            templates: cli.templates,
            bucket: cli.bucket,
            region: cli.region,
            source_bucket: cli.source_bucket,
            source_region: cli.source_region,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    pplx_space_common_cli::init_logging_with_startup_banner(SERVICE_NAME);

    // Returned rather than only logged: the Job's exit reason is what an
    // operator reads out of `kubectl logs`, and anyhow prints the whole source
    // chain while the default filter keeps library `info` traces quiet.
    Ok(pplx_space_template_publisher::run(cli.into()).await?)
}
