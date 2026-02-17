use std::time::{Duration, Instant};

use communication_layer_request_reply::TcpRequestReplyConnection;
use dora_message::{
    cli_to_coordinator::ControlRequest,
    coordinator_to_cli::{ControlRequestReply, NodeInfo},
    id::NodeId,
};
use eyre::{bail, Context};
use uuid::Uuid;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Poll the coordinator until the node reaches a desired state or the timeout expires.
///
/// `description` is used in error/status messages (e.g. "running", "stopped").
pub fn wait_for_node_state(
    session: &mut TcpRequestReplyConnection,
    dataflow_uuid: Uuid,
    node_id: &NodeId,
    predicate: impl Fn(&NodeInfo) -> bool,
    timeout: Duration,
    description: &str,
) -> eyre::Result<()> {
    let start = Instant::now();

    loop {
        if start.elapsed() >= timeout {
            bail!(
                "Timed out after {:.1}s waiting for node `{node_id}` to become {description}",
                timeout.as_secs_f64()
            );
        }

        std::thread::sleep(DEFAULT_POLL_INTERVAL);

        let reply_raw = session
            .request(&serde_json::to_vec(&ControlRequest::GetNodeInfo).unwrap())
            .wrap_err("failed to poll node info from coordinator")?;

        let reply: ControlRequestReply =
            serde_json::from_slice(&reply_raw).wrap_err("failed to parse node info reply")?;

        let node_infos = match reply {
            ControlRequestReply::NodeInfoList(infos) => infos,
            ControlRequestReply::Error(err) => bail!("Error polling node info: {err}"),
            other => bail!("Unexpected reply while polling node info: {other:?}"),
        };

        if let Some(info) = node_infos
            .iter()
            .find(|n| n.dataflow_id == dataflow_uuid && n.node_id == *node_id)
        {
            if predicate(info) {
                return Ok(());
            }
        } else {
            // Node not found in the list at all — it may have been removed.
            // For stop/kill this could mean the dataflow itself ended.
            bail!("Node `{node_id}` not found in dataflow `{dataflow_uuid}` while waiting");
        }
    }
}

/// Check if a node is in "running" state (has metrics).
pub fn is_running(info: &NodeInfo) -> bool {
    info.metrics.is_some()
}

/// Check if a node is in "stopped (manual)" state.
pub fn is_manually_stopped(info: &NodeInfo) -> bool {
    info.manually_stopped
}

/// Check if a node has been killed (process exited — either stopped or restarting).
pub fn is_killed(info: &NodeInfo) -> bool {
    info.stopped || info.restarting || info.manually_stopped
}
