use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use clap::Parser;
use pplx_space_build_mgr::recipe::{RecipeAdapterError, TomlRecipe, TomlStep};
use pplx_space_build_mgr_client::proto::{
    BuildOptions, BuildState, ClientContext, GetBuildStatusRequest, StartBuildRequest,
    UploadBlobChunk, recipe as recipe_proto,
};
use pplx_space_build_mgr_client::{BLOB_UPLOAD_CHUNK_BYTES, BlobUploadSender};
use tokio::io::AsyncReadExt;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Parser)]
#[command(
    name = "space-buildmgr",
    about = "Build Firecracker sandbox templates",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Build a template from a recipe file
    Build {
        /// Path to the recipe TOML file
        #[arg(long)]
        recipe: String,

        /// Template identifier (stored in /space/cache/firecracker/templates/<id>/)
        #[arg(long)]
        id: String,

        /// Base template to build on (mutates recipe.origin.from_template.base)
        #[arg(long)]
        base: Option<String>,

        /// Overwrite existing template
        #[arg(long)]
        force: bool,

        /// Extra release tag to register in addition to the immutable digest tag.
        #[arg(long = "publish-tag")]
        publish_tags: Vec<String>,

        /// Return immediately with a job id instead of waiting for completion.
        #[arg(long = "async")]
        async_mode: bool,

        /// Organization UUID that owns the published cluster template.
        #[arg(long)]
        org_id: Option<String>,
    },
    /// Show status for a build job
    Status {
        #[arg(long)]
        job_id: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pplx_space_common_cli::init_logging("space-buildmgr");
    let cli = Cli::parse();

    match cli.command {
        Commands::Build {
            recipe,
            id,
            base,
            force,
            publish_tags,
            async_mode,
            org_id,
        } => {
            let recipe = canonicalize_recipe_path(&recipe)?;
            let toml_recipe = TomlRecipe::from_file(&recipe)?;
            let mut client = pplx_space_build_mgr_client::connect().await?;
            let copy_blobs = upload_copy_sources(&mut client, &recipe, &toml_recipe).await?;
            let parsed_recipe =
                toml_recipe.into_proto(&recipe, base.as_deref(), |index, source| {
                    let sha = copy_blobs.get(&index).ok_or_else(|| {
                        RecipeAdapterError::MissingCopyPayload {
                            index,
                            source_path: source.to_string(),
                        }
                    })?;
                    Ok(recipe_proto::copy::Payload::BlobSha256(sha.clone()))
                })?;
            let system_publication_grant_id = if org_id.is_none() {
                pplx_space_build_mgr::publish::authorize_operator_system_build(&id)
                    .await?
                    .map(|grant| grant.to_string())
            } else {
                None
            };
            let response = client
                .start_build(StartBuildRequest {
                    system_publication_grant_id,
                    recipe: Some(parsed_recipe),
                    template_id: id,
                    options: Some(BuildOptions {
                        force,
                        publish_tags,
                    }),
                    client: Some(ClientContext {
                        source_path: recipe,
                        cli_version: env!("CARGO_PKG_VERSION").to_string(),
                        hostname: hostname(),
                    }),
                    org_id,
                })
                .await?
                .into_inner();

            println!("Build job submitted");
            println!("  Job id: {}", response.job_id);

            if !async_mode {
                wait_for_job(&mut client, &response.job_id).await?;
            }
        }
        Commands::Status { job_id } => {
            let mut client = pplx_space_build_mgr_client::connect().await?;
            let status = client
                .get_build_status(GetBuildStatusRequest {
                    job_id: job_id.clone(),
                })
                .await?
                .into_inner();
            print_job_status(&job_id, &status)?;
        }
    }

    Ok(())
}

async fn wait_for_job(
    client: &mut pplx_space_build_mgr_client::BuildManagerGrpcClient,
    job_id: &str,
) -> anyhow::Result<()> {
    let mut last_state = None;
    loop {
        let status = client
            .get_build_status(GetBuildStatusRequest {
                job_id: job_id.to_string(),
            })
            .await?
            .into_inner();

        if last_state != Some(status.state) {
            print_job_status(job_id, &status)?;
            last_state = Some(status.state);
        }

        match decode_build_state(status.state)? {
            BuildState::Succeeded => return Ok(()),
            BuildState::Failed | BuildState::PublishFailed => anyhow::bail!(
                "{}",
                if status.error.is_empty() {
                    if matches!(decode_build_state(status.state)?, BuildState::PublishFailed) {
                        "publish failed"
                    } else {
                        "build failed"
                    }
                } else {
                    &status.error
                }
            ),
            _ => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
        }
    }
}

fn print_job_status(
    job_id: &str,
    status: &pplx_space_build_mgr_client::proto::GetBuildStatusResponse,
) -> anyhow::Result<()> {
    let state = decode_build_state(status.state)?;
    println!("Job:      {job_id}");
    println!("Template: {}", status.template_id);
    println!("State:    {}", build_state_label(state));
    if !status.content_hash.is_empty() {
        println!("Hash:     {}", status.content_hash);
    }
    if !status.error.is_empty() {
        println!("Error:    {}", status.error);
    }
    Ok(())
}

fn decode_build_state(raw: i32) -> anyhow::Result<BuildState> {
    match BuildState::try_from(raw) {
        Ok(BuildState::Unspecified) => {
            anyhow::bail!("daemon returned unspecified build state");
        }
        Ok(state) => Ok(state),
        Err(_) => anyhow::bail!("daemon returned unknown build state value: {raw}"),
    }
}

fn build_state_label(state: BuildState) -> &'static str {
    match state {
        BuildState::Pending => "PENDING",
        BuildState::Running => "RUNNING",
        BuildState::Succeeded => "SUCCEEDED",
        BuildState::Failed => "FAILED",
        BuildState::PublishFailed => "PUBLISH_FAILED",
        BuildState::Unspecified => "UNSPECIFIED",
    }
}

fn canonicalize_recipe_path(recipe: &str) -> anyhow::Result<String> {
    let path = std::fs::canonicalize(recipe)
        .map_err(|error| anyhow::anyhow!("failed to resolve recipe path '{recipe}': {error}"))?;
    let path = path
        .into_os_string()
        .into_string()
        .map_err(|path| anyhow::anyhow!("recipe path is not valid UTF-8: {path:?}"))?;
    Ok(path)
}

async fn upload_copy_sources(
    client: &mut pplx_space_build_mgr_client::BuildManagerGrpcClient,
    recipe_path: &str,
    recipe: &TomlRecipe,
) -> anyhow::Result<HashMap<usize, String>> {
    let mut uploaded = HashMap::new();
    for (index, source) in toml_copy_sources(recipe) {
        let source_path = resolve_copy_source(recipe_path, source)?;
        let archive = archive_copy_source(&source_path)?;
        let file = tokio::fs::File::open(archive.path())
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "failed to open archived copy source {}: {error}",
                    archive.path().display()
                )
            })?;
        let (chunks, reader) = blob_upload_chunks(file);
        let response = client.upload_blob(chunks).await;
        reader
            .await
            .map_err(|error| anyhow::anyhow!("copy source upload reader task failed: {error}"))??;
        let response = response?.into_inner();
        uploaded.insert(index, response.sha256);
    }
    Ok(uploaded)
}

fn blob_upload_chunks(
    mut file: tokio::fs::File,
) -> (
    ReceiverStream<UploadBlobChunk>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let reader = tokio::spawn(async move {
        let Ok(upload) = BlobUploadSender::begin(tx).await else {
            return Ok(());
        };
        loop {
            let mut data = vec![0; BLOB_UPLOAD_CHUNK_BYTES];
            let len = file.read(&mut data).await?;
            if len == 0 {
                break;
            }
            data.truncate(len);
            if upload.send_data(data).await.is_err() {
                break;
            }
        }
        let _ = upload.commit().await;
        Ok(())
    });
    (ReceiverStream::new(rx), reader)
}

fn toml_copy_sources(recipe: &TomlRecipe) -> Vec<(usize, &str)> {
    recipe
        .steps
        .iter()
        .enumerate()
        .filter_map(|(index, step)| match &step.step {
            TomlStep::Copy { source, .. } => Some((index + 1, source.as_str())),
            _ => None,
        })
        .collect()
}

fn resolve_copy_source(recipe_path: &str, source: &str) -> anyhow::Result<PathBuf> {
    let source_path = Path::new(source);
    let path = if source_path.is_absolute() {
        source_path.to_path_buf()
    } else {
        Path::new(recipe_path)
            .parent()
            .ok_or_else(|| anyhow::anyhow!("recipe path has no parent: {recipe_path}"))?
            .join(source_path)
    };
    std::fs::canonicalize(&path).map_err(|error| {
        anyhow::anyhow!(
            "failed to resolve copy source '{}': {error}",
            path.display()
        )
    })
}

fn archive_copy_source(source: &Path) -> anyhow::Result<tempfile::NamedTempFile> {
    let parent = source
        .parent()
        .ok_or_else(|| anyhow::anyhow!("copy source has no parent: {}", source.display()))?;
    let name = source
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("copy source has no file name: {}", source.display()))?;
    let archive = tempfile::Builder::new()
        .prefix("space-buildmgr-copy-")
        .suffix(".tar")
        .tempfile()
        .map_err(|error| anyhow::anyhow!("failed to create copy archive temp file: {error}"))?;
    let stdout = archive
        .reopen()
        .map_err(|error| anyhow::anyhow!("failed to reopen copy archive temp file: {error}"))?;
    let output = Command::new("tar")
        .env("COPYFILE_DISABLE", "1")
        .arg("-C")
        .arg(parent)
        .arg("-cf")
        .arg("-")
        .arg(name)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .output()
        .map_err(|error| {
            anyhow::anyhow!(
                "failed to archive copy source {}: {error}",
                source.display()
            )
        })?;
    if !output.status.success() {
        anyhow::bail!(
            "failed to archive copy source {}: tar exited with {}",
            source.display(),
            output.status
        );
    }
    Ok(archive)
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            Command::new("hostname")
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .output()
                .ok()
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
