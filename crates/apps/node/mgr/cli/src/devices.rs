use pplx_space_node_mgr_client::{NodeManagerGrpcClient, proto::*};
use tonic::Status;

#[derive(clap::Subcommand)]
pub(super) enum Command {
    /// Show enrolled device-unit capacity and unresolved owners.
    List {
        /// Print one enriched JSON record per parent instead of tables.
        #[arg(long)]
        json: bool,
    },
    /// Inspect and enroll a complete device parent in quarantine.
    Enroll {
        #[arg(long)]
        profile: String,
        #[arg(long = "member", required = true)]
        members: Vec<String>,
    },
    /// Run a compiled qualification procedure for the observed parent revision.
    Requalify {
        parent_id: String,
        #[arg(long)]
        revision: u64,
        #[arg(long)]
        procedure: String,
    },
    /// Retire an unheld parent, retaining its ownership and qualification history.
    Retire {
        parent_id: String,
        #[arg(long)]
        revision: u64,
    },
}

fn parent_json(parent: &DeviceParentRecord) -> serde_json::Value {
    let state = DeviceParentState::try_from(parent.state).unwrap_or(DeviceParentState::Unspecified);
    serde_json::json!({
        "parent_id": parent.parent_id,
        "profile": parent.profile,
        "members": parent.host_device_ids,
        "state": state.as_str_name(),
        "revision": parent.revision,
        "qualification_digest": parent.qualification_digest,
        "usage": parent.usage.as_ref().map(|usage| serde_json::json!({
            "allocatable": usage.allocatable,
            "allocations": usage.allocations.iter().map(|allocation| {
                let (kind, owner) = owner(allocation);
                serde_json::json!({
                    "lease_id": allocation.assignment.as_ref().map(|a| &a.lease_id),
                    "profile": allocation.assignment.as_ref().map(|a| &a.profile),
                    "members": allocation.assignment.as_ref().map(|a| &a.host_device_ids),
                    "state": allocation_state(allocation),
                    "execution_id": allocation.execution_id,
                    "owner_kind": kind,
                    "owner": owner,
                })
            }).collect::<Vec<_>>(),
        })),
    })
}

fn display_parent(parent: &DeviceParentRecord) {
    println!("{}", parent_json(parent));
}

mod display;
use display::{allocation_state, owner};

pub(super) async fn run(
    client: &mut NodeManagerGrpcClient,
    command: Command,
) -> Result<(), Status> {
    match command {
        Command::List { json } => {
            let response = client
                .list_device_parents(ListDeviceParentsRequest {})
                .await?
                .into_inner();
            if json {
                for parent in &response.parents {
                    display_parent(parent);
                }
            } else {
                println!("{}", display::tables(&response.parents));
            }
        }
        Command::Enroll { profile, members } => {
            let mut expected_members = members.clone();
            expected_members.sort();
            let response = client
                .enroll_device_parent(EnrollDeviceParentRequest {
                    profile: profile.clone(),
                    host_device_ids: members,
                })
                .await?
                .into_inner();
            let parent = response
                .parent
                .ok_or_else(|| Status::data_loss("enrollment acknowledgement missing"))?;
            let mut observed_members = parent.host_device_ids.clone();
            observed_members.sort();
            if parent.parent_id.is_empty()
                || parent.profile != profile
                || observed_members != expected_members
            {
                return Err(Status::data_loss(
                    "enrollment acknowledgement differs from the requested profile/members",
                ));
            }
            display_parent(&parent);
        }
        Command::Requalify {
            parent_id,
            revision,
            procedure,
        } => {
            let response = client
                .requalify_device_parent(RequalifyDeviceParentRequest {
                    parent_id: parent_id.clone(),
                    expected_revision: revision,
                    qualification_digest: procedure,
                })
                .await?
                .into_inner();
            if response.parent_id != parent_id || response.revision <= revision {
                return Err(Status::data_loss(
                    "qualification acknowledgement differs from the requested parent/revision",
                ));
            }
            println!(
                "{}",
                serde_json::json!({"parent_id": response.parent_id, "revision": response.revision})
            );
        }
        Command::Retire {
            parent_id,
            revision,
        } => {
            let response = client
                .retire_device_parent(RetireDeviceParentRequest {
                    parent_id: parent_id.clone(),
                    expected_revision: revision,
                })
                .await?
                .into_inner();
            if response.parent_id != parent_id || Some(response.revision) != revision.checked_add(1)
            {
                return Err(Status::data_loss(
                    "retirement acknowledgement differs from the requested parent/revision",
                ));
            }
            println!(
                "{}",
                serde_json::json!({"parent_id": response.parent_id, "revision": response.revision})
            );
        }
    }
    Ok(())
}
