use clap::Args;
use duration_str::parse;
use eyre::{Context, bail};
use std::time::Duration;

use crate::{
    command::{Executable, default_tracing},
    common::{CoordinatorOptions, resolve_dataflow_identifier_interactive},
};
use dora_message::{
    cli_to_coordinator::ControlRequest,
    coordinator_to_cli::ControlRequestReply,
    id::NodeId,
};

/// Stop a running node in a dataflow. This disables the node's restart policy.
#[derive(Debug, Args)]
pub struct Stop {
    /// The node ID to stop
    node_id: String,

    /// UUID or name of the dataflow
    #[clap(long, short = 'd', value_name = "UUID_OR_NAME")]
    dataflow: Option<String>,

    /// Grace duration before force-killing the node process
    #[clap(long, value_name = "DURATION")]
    #[arg(value_parser = parse)]
    grace_duration: Option<Duration>,

    #[clap(flatten)]
    coordinator: CoordinatorOptions,
}

impl Executable for Stop {
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
            .request(&serde_json::to_vec(&ControlRequest::NodeStop {
                dataflow_uuid,
                node_id: node_id.clone(),
                grace_duration: self.grace_duration,
            })?)
            .wrap_err("failed to send node stop request to coordinator")?;

        let reply: ControlRequestReply =
            serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;

        match reply {
            ControlRequestReply::NodeStopped { uuid, node_id } => {
                println!("Stopped node `{node_id}` in dataflow `{uuid}`");
                Ok(())
            }
            ControlRequestReply::Error(err) => {
                bail!("Failed to stop node: {err}")
            }
            other => {
                bail!("Unexpected reply: {other:?}")
            }
        }
    }
}
