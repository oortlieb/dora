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

use super::wait;

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// Kill a running node in a dataflow. Unlike `stop`, this does NOT disable the restart policy.
///
/// If the node has a restart policy (e.g. `always` or `on-failure`), it will be restarted
/// according to that policy after being killed.
#[derive(Debug, Args)]
pub struct Kill {
    /// The node ID to kill
    node_id: String,

    /// UUID or name of the dataflow
    #[clap(long, short = 'd', value_name = "UUID_OR_NAME")]
    dataflow: Option<String>,

    /// Grace duration before force-killing the node process
    #[clap(long, value_name = "DURATION")]
    #[arg(value_parser = parse)]
    grace_duration: Option<Duration>,

    /// Block until the node process has exited
    #[clap(long, short = 'w')]
    wait: bool,

    /// Maximum time to wait for the node to reach the desired state (requires --wait)
    #[clap(long, value_name = "DURATION", default_value = "30s")]
    #[arg(value_parser = parse)]
    wait_timeout: Duration,

    #[clap(flatten)]
    coordinator: CoordinatorOptions,
}

impl Executable for Kill {
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
            .request(&serde_json::to_vec(&ControlRequest::NodeKill {
                dataflow_uuid,
                node_id: node_id.clone(),
                grace_duration: self.grace_duration,
            })?)
            .wrap_err("failed to send node kill request to coordinator")?;

        let reply: ControlRequestReply =
            serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;

        match reply {
            ControlRequestReply::NodeKilled { uuid, node_id } => {
                if self.wait {
                    println!("Killing node `{node_id}` in dataflow `{uuid}`, waiting for process to exit...");
                    wait::wait_for_node_state(
                        &mut *session,
                        uuid,
                        &node_id,
                        wait::is_killed,
                        self.wait_timeout,
                        "killed",
                    )?;
                    println!("Node `{node_id}` process has exited.");
                } else {
                    println!("Killed node `{node_id}` in dataflow `{uuid}`");
                }
                Ok(())
            }
            ControlRequestReply::Error(err) => {
                bail!("Failed to kill node: {err}")
            }
            other => {
                bail!("Unexpected reply: {other:?}")
            }
        }
    }
}
