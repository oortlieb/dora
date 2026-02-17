use clap::Args;
use eyre::{Context, bail};

use crate::{
    command::{Executable, default_tracing},
    common::{CoordinatorOptions, resolve_dataflow_identifier_interactive},
};
use dora_message::{
    cli_to_coordinator::ControlRequest,
    coordinator_to_cli::ControlRequestReply,
    id::NodeId,
};

/// Start a stopped or restarting node in a dataflow. This re-enables the node's restart policy.
#[derive(Debug, Args)]
pub struct Start {
    /// The node ID to start
    node_id: String,

    /// UUID or name of the dataflow
    #[clap(long, short = 'd', value_name = "UUID_OR_NAME")]
    dataflow: Option<String>,

    #[clap(flatten)]
    coordinator: CoordinatorOptions,
}

impl Executable for Start {
    fn execute(self) -> eyre::Result<()> {
        default_tracing()?;
        let mut session = self
            .coordinator
            .connect()
            .wrap_err("could not connect to dora coordinator")?;

        let dataflow_uuid = resolve_dataflow_identifier_interactive(
            &mut *session,
            self.dataflow.as_deref(),
        )?;
        let node_id: NodeId = self.node_id.into();

        let reply_raw = session
            .request(&serde_json::to_vec(&ControlRequest::NodeStart {
                dataflow_uuid,
                node_id: node_id.clone(),
            })?)
            .wrap_err("failed to send node start request to coordinator")?;

        let reply: ControlRequestReply =
            serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;

        match reply {
            ControlRequestReply::NodeStarted { uuid, node_id } => {
                println!("Started node `{node_id}` in dataflow `{uuid}`");
                Ok(())
            }
            ControlRequestReply::Error(err) => {
                bail!("Failed to start node: {err}")
            }
            other => {
                bail!("Unexpected reply: {other:?}")
            }
        }
    }
}
