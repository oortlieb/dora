use crate::{
    CoreNodeKindExt, DoraEvent, Event, NodeCommand, OutputId, ProcessOperation, RunningNode,
    log::{self, NodeLogger},
};
use aligned_vec::{AVec, ConstAlign};
use crossbeam::queue::ArrayQueue;
use dora_arrow_convert::IntoArrow;
use dora_core::{
    config::DataId,
    descriptor::{ResolvedNode, ResolvedNodeExt},
    uhlc::HLC,
};
use dora_message::{
    DataflowId,
    common::{LogLevel, LogMessage, LogMessageHelper},
    daemon_to_coordinator::{DataMessage, NodeExitStatus, Timestamped},
    daemon_to_node::NodeConfig,
    descriptor::RestartPolicy,
    id::NodeId,
};
use dora_node_api::{
    Metadata,
    arrow::array::ArrayData,
    arrow_utils::{copy_array_into_sample, required_data_size},
};
use eyre::{ContextCompat, WrapErr};
use process_wrap::tokio::TokioCommandWrap;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{self, AtomicBool, AtomicU32},
    },
};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

enum NormalExitOutcome {
    /// Node was successfully respawned; contains the new finished_rx.
    Respawned(oneshot::Receiver<NodeProcessFinished>),
    /// Loop should break (node won't restart or fatal error).
    Break,
    /// Command channel closed, exit immediately.
    ChannelClosed,
}

enum RespawnOutcome {
    /// Respawn succeeded; contains the new finished_rx.
    Ok(oneshot::Receiver<NodeProcessFinished>),
    /// Respawn failed fatally.
    Fatal,
}

/// systemd-style restart rate limiter (`StartLimitBurst` / `StartLimitIntervalSec`).
///
/// Records a restart attempt at `now` and returns whether it is allowed: at most `burst` restarts
/// are permitted within any rolling `interval` window. Timestamps older than `interval` are pruned
/// before the check, so failures spaced farther apart than the window never accumulate. When the
/// attempt is allowed the timestamp is recorded; when denied the deque is left unchanged.
fn within_restart_limit(
    restart_times: &mut VecDeque<Instant>,
    now: Instant,
    burst: u32,
    interval: Duration,
) -> bool {
    while let Some(&front) = restart_times.front() {
        if now.duration_since(front) >= interval {
            restart_times.pop_front();
        } else {
            break;
        }
    }
    if restart_times.len() as u32 >= burst {
        return false;
    }
    restart_times.push_back(now);
    true
}

#[derive(Clone)]
pub struct PreparedNode {
    pub(super) command: Option<clonable_command::Command>,
    pub(super) spawn_error_msg: String,
    pub(super) node_working_dir: PathBuf,
    pub(super) dataflow_id: DataflowId,
    pub(super) node: ResolvedNode,
    pub(super) node_config: NodeConfig,
    pub(super) clock: Arc<HLC>,
    pub(super) daemon_tx: mpsc::Sender<Timestamped<Event>>,
    pub(super) node_stderr_most_recent: Arc<ArrayQueue<String>>,
}

impl PreparedNode {
    pub fn node_id(&self) -> &NodeId {
        &self.node.id
    }

    pub fn dynamic(&self) -> bool {
        self.node.kind.dynamic()
    }

    pub async fn spawn(self, mut logger: NodeLogger<'static>) -> eyre::Result<RunningNode> {
        let (op_tx, op_rx) = flume::bounded(2);
        let (finished_tx, finished_rx) = oneshot::channel();
        let kind = self
            .clone()
            .spawn_inner(&mut logger, op_rx, finished_tx)
            .await?;

        let disable_restart = Arc::new(AtomicBool::new(false));
        let pid = Arc::new(AtomicU32::new(0));
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let running_node = RunningNode {
            process: match &kind {
                NodeKind::Dynamic => None,
                NodeKind::Spawned { .. } => Some(crate::ProcessHandle::new(op_tx)),
            },
            node_config: self.node_config.clone(),
            restart_policy: self.restart_policy(),
            disable_restart: disable_restart.clone(),
            command_tx,
            manually_stopped: false,
            grace_timer_handle: None,
            pid: match kind {
                NodeKind::Dynamic => None,
                NodeKind::Spawned { pid: new_pid } => {
                    pid.store(new_pid, atomic::Ordering::Release);
                    Some(pid.clone())
                }
            },
        };

        tokio::spawn(self.restart_loop(
            logger,
            finished_rx,
            disable_restart,
            pid,
            command_rx,
        ));

        Ok(running_node)
    }

    fn restart_policy(&self) -> RestartPolicy {
        match &self.node.kind {
            dora_core::descriptor::CoreNodeKind::Custom(n) => n.restart_policy,
            dora_core::descriptor::CoreNodeKind::Runtime(_) => RestartPolicy::Never,
        }
    }

    fn restart_sec(&self) -> u64 {
        match &self.node.kind {
            dora_core::descriptor::CoreNodeKind::Custom(n) => n.restart_sec.unwrap_or(0),
            dora_core::descriptor::CoreNodeKind::Runtime(_) => 0,
        }
    }

    /// systemd-style restart rate limit as `(burst, interval)`, or `None` when unlimited.
    ///
    /// Active only when both `start_limit_burst` and `start_limit_interval_sec` are set and
    /// positive; otherwise restarts are unlimited (the historical behavior).
    fn start_limit(&self) -> Option<(u32, Duration)> {
        let n = match &self.node.kind {
            dora_core::descriptor::CoreNodeKind::Custom(n) => n,
            dora_core::descriptor::CoreNodeKind::Runtime(_) => return None,
        };
        match (n.start_limit_burst, n.start_limit_interval_sec) {
            (Some(burst), Some(interval_sec)) if burst > 0 && interval_sec > 0 => {
                Some((burst, Duration::from_secs(interval_sec)))
            }
            _ => None,
        }
    }

    async fn restart_loop(
        self,
        mut logger: NodeLogger<'static>,
        finished_rx: oneshot::Receiver<NodeProcessFinished>,
        disable_restart: Arc<AtomicBool>,
        pid: Arc<AtomicU32>,
        mut command_rx: mpsc::UnboundedReceiver<NodeCommand>,
    ) {
        // Tracks the op_rx for passing to the next spawn_inner call.
        let mut last_op_rx: Option<flume::Receiver<ProcessOperation>> = None;
        // Rolling record of recent restart timestamps for systemd-style rate limiting.
        let mut restart_times: VecDeque<Instant> = VecDeque::new();
        // Use Option to allow taking by value from the loop.
        let mut finished_rx_opt = Some(finished_rx);

        loop {
            let mut finished_rx = match finished_rx_opt.take() {
                Some(rx) => rx,
                None => break,
            };

            // ── State 1: Running ──
            // Wait for the process to exit OR a control command.
            enum RunningOutcome {
                ProcessExited(NodeProcessFinished),
                StopRequested(oneshot::Receiver<NodeProcessFinished>),
                KillRequested(oneshot::Receiver<NodeProcessFinished>),
                ChannelClosed,
            }

            let outcome = tokio::select! {
                result = &mut finished_rx => {
                    match result {
                        Ok(finished) => RunningOutcome::ProcessExited(finished),
                        Err(_) => {
                            logger
                                .log(
                                    LogLevel::Error,
                                    Some("daemon".into()),
                                    "failed to receive finished signal".to_string(),
                                )
                                .await;
                            RunningOutcome::ChannelClosed
                        }
                    }
                }
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(NodeCommand::Stop) => RunningOutcome::StopRequested(finished_rx),
                        Some(NodeCommand::Kill) => RunningOutcome::KillRequested(finished_rx),
                        Some(NodeCommand::Start) => {
                            // Already running, put finished_rx back and continue
                            finished_rx_opt = Some(finished_rx);
                            continue;
                        }
                        None => RunningOutcome::ChannelClosed,
                    }
                }
            };

            match outcome {
                RunningOutcome::ChannelClosed => break,
                RunningOutcome::StopRequested(rx) => {
                    // The daemon has already sent NodeEvent::Stop to the process.
                    // Start commands are rejected at the daemon level while the
                    // grace timer handle is present, so we only need to wait for
                    // the process to exit.
                    let Ok(finished) = rx.await else {
                        logger
                            .log(
                                LogLevel::Error,
                                Some("daemon".into()),
                                "failed to receive finished signal after stop command".to_string(),
                            )
                            .await;
                        break;
                    };
                    let exit_status = finished.exit_status;
                    last_op_rx = Some(finished.op_rx);

                    // Send SpawnedNodeResult with manually_stopped=true so the daemon
                    // keeps outputs open and does not remove us from running_nodes.
                    let event = DoraEvent::SpawnedNodeResult {
                        dataflow_id: self.dataflow_id,
                        node_id: self.node.id.clone(),
                        exit_status,
                        dynamic_node: self.node.kind.dynamic(),
                        restart: false,
                        manually_stopped: true,
                    }
                    .into();
                    let event = Timestamped {
                        inner: event,
                        timestamp: self.clock.clone().new_timestamp(),
                    };
                    let _ = self.daemon_tx.clone().send(event).await;

                    logger
                        .log(
                            LogLevel::Info,
                            Some("daemon".into()),
                            "node manually stopped, entering parked state".to_string(),
                        )
                        .await;

                    // ── State 3: Parked ──
                    // Wait for a Start command to respawn.
                    loop {
                        match command_rx.recv().await {
                            Some(NodeCommand::Start) => {
                                logger
                                    .log(
                                        LogLevel::Info,
                                        Some("daemon".into()),
                                        "received start command, respawning node".to_string(),
                                    )
                                    .await;
                                break;
                            }
                            Some(NodeCommand::Stop) | Some(NodeCommand::Kill) => {
                                // Already stopped, ignore
                                continue;
                            }
                            None => {
                                // Channel closed, exit loop
                                return;
                            }
                        }
                    }

                    // Respawn the node
                    let op_rx = last_op_rx.take().expect("op_rx should be set in parked state");
                    match self.do_respawn(&mut logger, op_rx, &pid).await {
                        RespawnOutcome::Ok(new_rx) => {
                            finished_rx_opt = Some(new_rx);
                            continue;
                        }
                        RespawnOutcome::Fatal => break,
                    }
                }
                RunningOutcome::KillRequested(rx) => {
                    // The daemon has already killed the process.
                    // Wait for the process to actually exit, then handle restart normally.
                    let Ok(finished) = rx.await else {
                        logger
                            .log(
                                LogLevel::Error,
                                Some("daemon".into()),
                                "failed to receive finished signal after kill command".to_string(),
                            )
                            .await;
                        break;
                    };
                    let exit_status = finished.exit_status;
                    last_op_rx = Some(finished.op_rx);

                    match self
                        .handle_normal_exit(
                            &mut logger,
                            exit_status,
                            &disable_restart,
                            &pid,
                            &mut command_rx,
                            &mut last_op_rx,
                            &mut restart_times,
                        )
                        .await
                    {
                        NormalExitOutcome::Respawned(new_rx) => {
                            finished_rx_opt = Some(new_rx);
                            continue;
                        }
                        NormalExitOutcome::Break | NormalExitOutcome::ChannelClosed => break,
                    }
                }
                RunningOutcome::ProcessExited(finished) => {
                    let exit_status = finished.exit_status;
                    last_op_rx = Some(finished.op_rx);

                    match self
                        .handle_normal_exit(
                            &mut logger,
                            exit_status,
                            &disable_restart,
                            &pid,
                            &mut command_rx,
                            &mut last_op_rx,
                            &mut restart_times,
                        )
                        .await
                    {
                        NormalExitOutcome::Respawned(new_rx) => {
                            finished_rx_opt = Some(new_rx);
                            continue;
                        }
                        NormalExitOutcome::Break | NormalExitOutcome::ChannelClosed => break,
                    }
                }
            }
        }
    }

    /// Handle a normal process exit (not a manual stop).
    async fn handle_normal_exit(
        &self,
        logger: &mut NodeLogger<'_>,
        exit_status: NodeExitStatus,
        disable_restart: &Arc<AtomicBool>,
        pid: &Arc<AtomicU32>,
        command_rx: &mut mpsc::UnboundedReceiver<NodeCommand>,
        last_op_rx: &mut Option<flume::Receiver<ProcessOperation>>,
        restart_times: &mut VecDeque<Instant>,
    ) -> NormalExitOutcome {
        let restart = match self.restart_policy() {
            RestartPolicy::Always => true,
            RestartPolicy::OnFailure if exit_status.is_success() => false,
            RestartPolicy::OnFailure => true,
            RestartPolicy::Never => false,
        };

        let restart_disabled = disable_restart.load(atomic::Ordering::Acquire);
        if restart && restart_disabled {
            logger
                .log(
                    LogLevel::Info,
                    Some("daemon".into()),
                    "not restarting node because all inputs are already closed".to_string(),
                )
                .await;
        }
        let mut restart = restart && !restart_disabled;

        // systemd-style rate limit: give up if the node has restarted too many times recently.
        if restart {
            if let Some((burst, interval)) = self.start_limit() {
                if !within_restart_limit(restart_times, Instant::now(), burst, interval) {
                    logger
                        .log(
                            LogLevel::Error,
                            Some("daemon".into()),
                            format!(
                                "node exceeded restart limit ({burst} restarts within {}s), \
                                 giving up and leaving it stopped",
                                interval.as_secs()
                            ),
                        )
                        .await;
                    restart = false;
                }
            }
        }
        let success = exit_status.is_success();

        if !success {
            let _span = tracing::error_span!(
                "node_failure",
                node_id = %self.node.id,
                dataflow_id = %self.dataflow_id
            )
            .entered();
            tracing::error!("node exited with error: {:?}", exit_status);
        }

        let event = DoraEvent::SpawnedNodeResult {
            dataflow_id: self.dataflow_id,
            node_id: self.node.id.clone(),
            exit_status,
            dynamic_node: self.node.kind.dynamic(),
            restart,
            manually_stopped: false,
        }
        .into();
        let event = Timestamped {
            inner: event,
            timestamp: self.clock.clone().new_timestamp(),
        };
        let _ = self.daemon_tx.clone().send(event).await;

        if !restart {
            return NormalExitOutcome::Break;
        }

        // ── State 2: WaitingRestart ──
        let restart_sec = self.restart_sec();
        if restart_sec > 0 {
            logger
                .log(
                    LogLevel::Info,
                    Some("daemon".into()),
                    format!("waiting {restart_sec}s before restarting node"),
                )
                .await;

            // Interruptible sleep: listen for commands during the delay.
            let sleep = tokio::time::sleep(Duration::from_secs(restart_sec));
            tokio::pin!(sleep);

            loop {
                tokio::select! {
                    _ = &mut sleep => {
                        // Delay elapsed, proceed to respawn
                        break;
                    }
                    cmd = command_rx.recv() => {
                        match cmd {
                            Some(NodeCommand::Stop) => {
                                logger
                                    .log(
                                        LogLevel::Info,
                                        Some("daemon".into()),
                                        "stop command received during restart delay, entering parked state".to_string(),
                                    )
                                    .await;

                                // Send manually_stopped event
                                let event = DoraEvent::SpawnedNodeResult {
                                    dataflow_id: self.dataflow_id,
                                    node_id: self.node.id.clone(),
                                    exit_status: NodeExitStatus::Success,
                                    dynamic_node: self.node.kind.dynamic(),
                                    restart: false,
                                    manually_stopped: true,
                                }
                                .into();
                                let event = Timestamped {
                                    inner: event,
                                    timestamp: self.clock.clone().new_timestamp(),
                                };
                                let _ = self.daemon_tx.clone().send(event).await;

                                // Enter parked state
                                loop {
                                    match command_rx.recv().await {
                                        Some(NodeCommand::Start) => {
                                            logger
                                                .log(
                                                    LogLevel::Info,
                                                    Some("daemon".into()),
                                                    "received start command, respawning node".to_string(),
                                                )
                                                .await;
                                            break;
                                        }
                                        Some(_) => continue,
                                        None => return NormalExitOutcome::ChannelClosed,
                                    }
                                }
                                // Fall through to respawn below
                                break;
                            }
                            Some(NodeCommand::Start) => {
                                logger
                                    .log(
                                        LogLevel::Info,
                                        Some("daemon".into()),
                                        "start command received during restart delay, spawning immediately".to_string(),
                                    )
                                    .await;
                                // Skip remaining delay, proceed to respawn
                                break;
                            }
                            Some(NodeCommand::Kill) => {
                                // Process is already dead, restart will proceed normally
                                continue;
                            }
                            None => return NormalExitOutcome::ChannelClosed,
                        }
                    }
                }
            }
        }

        // Check if restart was disabled during the delay (e.g. all inputs closed)
        if disable_restart.load(atomic::Ordering::Acquire) {
            logger
                .log(
                    LogLevel::Info,
                    Some("daemon".into()),
                    "restart disabled during delay, not restarting".to_string(),
                )
                .await;
            return NormalExitOutcome::Break;
        }

        if success {
            logger
                .log(
                    LogLevel::Info,
                    Some("daemon".into()),
                    "restarting node after successful exit".to_string(),
                )
                .await;
        } else {
            logger
                .log(
                    LogLevel::Warn,
                    Some("daemon".into()),
                    "restarting node after failure".to_string(),
                )
                .await;
        }

        let op_rx = last_op_rx
            .take()
            .expect("op_rx should be set after process exit");
        match self.do_respawn(logger, op_rx, pid).await {
            RespawnOutcome::Ok(new_rx) => NormalExitOutcome::Respawned(new_rx),
            RespawnOutcome::Fatal => NormalExitOutcome::Break,
        }
    }

    /// Respawn the node process.
    async fn do_respawn(
        &self,
        logger: &mut NodeLogger<'_>,
        op_rx: flume::Receiver<ProcessOperation>,
        pid: &Arc<AtomicU32>,
    ) -> RespawnOutcome {
        let (finished_tx, finished_rx_new) = oneshot::channel();
        let result = self
            .clone()
            .spawn_inner(logger, op_rx, finished_tx)
            .await;
        match result {
            Ok(NodeKind::Spawned { pid: new_pid }) => {
                pid.store(new_pid, atomic::Ordering::Release);
                RespawnOutcome::Ok(finished_rx_new)
            }
            Ok(NodeKind::Dynamic) => {
                logger
                    .log(
                        LogLevel::Error,
                        Some("daemon".into()),
                        "cannot restart dynamic node".to_string(),
                    )
                    .await;
                RespawnOutcome::Fatal
            }
            Err(err) => {
                logger
                    .log(
                        LogLevel::Error,
                        Some("daemon".into()),
                        format!("failed to restart node: {err:?}"),
                    )
                    .await;
                RespawnOutcome::Fatal
            }
        }
    }

    async fn spawn_inner(
        mut self,
        logger: &mut NodeLogger<'_>,
        op_rx: flume::Receiver<ProcessOperation>,
        finished_tx: oneshot::Sender<NodeProcessFinished>,
    ) -> eyre::Result<NodeKind> {
        let mut child = match &mut self.command {
            Some(command) => {
                let std_command = command.to_std();
                logger
                    .log(
                        LogLevel::Info,
                        Some("spawner".into()),
                        format!(
                            "spawning `{}` in `{}`",
                            std_command.get_program().to_string_lossy(),
                            std_command
                                .get_current_dir()
                                .unwrap_or(Path::new("<unknown>"))
                                .display(),
                        ),
                    )
                    .await;
                let mut command =
                    TokioCommandWrap::from(tokio::process::Command::from(std_command));

                #[cfg(unix)]
                {
                    // Set the process group to 0 to ensure that the spawned process does not exit immediately on CTRL-C
                    // command.process_group(0);

                    command.wrap(process_wrap::tokio::ProcessGroup::leader());
                }
                #[cfg(windows)]
                {
                    command
                        .wrap(process_wrap::tokio::CreationFlags(
                            windows::Win32::System::Threading::CREATE_NEW_PROCESS_GROUP,
                        ))
                        .wrap(process_wrap::tokio::JobObject);
                }

                command.spawn().wrap_err(self.spawn_error_msg)?
            }
            None => {
                return Ok(NodeKind::Dynamic);
            }
        };

        let pid = child.id().context(
            "Could not get the pid for the just spawned node and indicate that there is an error",
        )?;
        logger
            .log(
                LogLevel::Debug,
                Some("spawner".into()),
                format!("spawned node with pid {pid}"),
            )
            .await;

        let dataflow_dir: PathBuf = self
            .node_working_dir
            .join("out")
            .join(self.dataflow_id.to_string());
        if !dataflow_dir.exists() {
            std::fs::create_dir_all(&dataflow_dir).context("could not create dataflow_dir")?;
        }
        let (tx, mut rx) = mpsc::channel(10);
        let mut file = File::create(log::log_path(
            &self.node_working_dir,
            &self.dataflow_id,
            &self.node.id,
        ))
        .await
        .expect("Failed to create log file");
        let mut child_stdout =
            tokio::io::BufReader::new(child.stdout().take().expect("failed to take stdout"));
        let stdout_tx = tx.clone();
        let node_id = self.node.id.clone();
        let mut logger_c = logger.try_clone().await?;
        // Stdout listener stream
        tokio::spawn(async move {
            let mut buffer = String::new();
            let mut finished = false;
            while !finished {
                let mut raw = Vec::new();
                finished = match child_stdout
                    .read_until(b'\n', &mut raw)
                    .await
                    .wrap_err_with(|| {
                        format!("failed to read stdout line from spawned node {node_id}")
                    }) {
                    Ok(0) => true,
                    Ok(_) => false,
                    Err(err) => {
                        logger_c
                            .log(LogLevel::Warn, Some("daemon".into()), format!("{err:?}"))
                            .await;
                        false
                    }
                };

                match String::from_utf8(raw) {
                    Ok(s) => buffer.push_str(&s),
                    Err(err) => {
                        let lossy = String::from_utf8_lossy(err.as_bytes());
                        logger_c
                            .log(
                                LogLevel::Warn,
                                Some("daemon".into()),
                                format!(
                                    "stdout not valid UTF-8 string ({}: {lossy}",
                                    err.utf8_error()
                                ),
                            )
                            .await;
                        buffer.push_str(&lossy)
                    }
                };

                // send the buffered lines
                let lines = std::mem::take(&mut buffer);
                let sent = stdout_tx.send(lines.clone()).await;
                if sent.is_err() {
                    println!("Could not log: {lines}");
                }
            }
        });

        let mut child_stderr =
            tokio::io::BufReader::new(child.stderr().take().expect("failed to take stderr"));

        // Stderr listener stream
        let stderr_tx = tx.clone();
        let node_id = self.node.id.clone();
        let daemon_tx_log = self.daemon_tx.clone();
        tokio::spawn(async move {
            let mut buffer = String::new();
            let mut finished = false;
            while !finished {
                let mut raw = Vec::new();
                finished = match child_stderr
                    .read_until(b'\n', &mut raw)
                    .await
                    .wrap_err_with(|| {
                        format!("failed to read stderr line from spawned node {node_id}")
                    }) {
                    Ok(0) => true,
                    Ok(_) => false,
                    Err(err) => {
                        tracing::warn!("{err:?}");
                        true
                    }
                };

                let new = match String::from_utf8(raw) {
                    Ok(s) => s,
                    Err(err) => {
                        let lossy = String::from_utf8_lossy(err.as_bytes());
                        tracing::warn!(
                            "stderr not valid UTF-8 string (node {node_id}): {}: {lossy}",
                            err.utf8_error()
                        );
                        lossy.into_owned()
                    }
                };

                buffer.push_str(&new);

                self.node_stderr_most_recent.force_push(new);

                // send the buffered lines
                let lines = std::mem::take(&mut buffer);
                let sent = stderr_tx.send(lines.clone()).await;
                if sent.is_err() {
                    println!("Could not log: {lines}");
                }
            }
        });

        let (log_finish_tx, log_finish_rx) = oneshot::channel();
        let dataflow_id = self.dataflow_id;

        tokio::spawn(async move {
            let exit_status: NodeExitStatus = loop {
                tokio::select! {
                    status = Box::into_pin(child.wait()) => {
                        break status.into();
                    }
                    result = op_rx.recv_async() => {
                        match result {
                            Ok(op) => op.execute(child.as_mut()),
                            Err(_) => {
                                // Sender dropped
                                break Box::into_pin(child.wait()).await.into();
                            }
                        }
                    }
                }
            };

            let _ = log_finish_rx.await;
            let _ = finished_tx.send(NodeProcessFinished { exit_status, op_rx });
        });

        let node_id = self.node.id.clone();
        let daemon_id = logger.inner().inner().daemon_id().clone();
        let mut cloned_logger = logger
            .inner()
            .inner()
            .inner()
            .try_clone()
            .await
            .context("failed to clone logger")?;

        let send_stdout_to = self
            .node
            .send_stdout_as()
            .context("Could not resolve `send_stdout_as` configuration")?;
        let uhlc = self.clock.clone();
        let mut logger_c = logger.try_clone().await?;
        // Log to file stream.
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                // If log is an output, we're sending the logs to the dataflow
                if let Some(stdout_output_name) = &send_stdout_to {
                    // Convert logs to DataMessage
                    let array = message.as_str().into_arrow();

                    let array: ArrayData = array.into();
                    let total_len = required_data_size(&array);
                    let mut sample: AVec<u8, ConstAlign<128>> =
                        AVec::__from_elem(128, 0, total_len);

                    let type_info = copy_array_into_sample(&mut sample, &array);

                    let metadata = Metadata::new(uhlc.new_timestamp(), type_info);
                    let output_id = OutputId(
                        node_id.clone(),
                        DataId::from(stdout_output_name.to_string()),
                    );
                    let event = DoraEvent::Logs {
                        dataflow_id,
                        output_id,
                        metadata,
                        message: DataMessage::Vec(sample),
                    }
                    .into();
                    let event = Timestamped {
                        inner: event,
                        timestamp: uhlc.new_timestamp(),
                    };
                    let _ = daemon_tx_log.send(event).await;
                }

                match file.write_all(message.as_bytes()).await {
                    Ok(_) => {}
                    Err(err) => {
                        logger_c
                            .log(
                                LogLevel::Error,
                                Some("daemon".into()),
                                format!("Could not log {message} to file due to {err}"),
                            )
                            .await;
                    }
                }

                let formatted = message.lines().fold(String::default(), |mut output, line| {
                    output.push_str(line);
                    output
                });

                if std::env::var("DORA_QUIET").is_err() {
                    match serde_json::de::from_str::<LogMessageHelper>(&formatted) {
                        Ok(log_msg) => {
                            let mut message = LogMessage::from(log_msg);
                            message.dataflow_id = Some(dataflow_id);
                            message.node_id = Some(node_id.clone());
                            message.daemon_id = Some(daemon_id.clone());
                            cloned_logger.log(message).await;
                        }
                        Err(_err) => {
                            cloned_logger
                                .log(LogMessage {
                                    daemon_id: Some(daemon_id.clone()),
                                    dataflow_id: Some(dataflow_id),
                                    build_id: None,
                                    level: dora_core::build::LogLevelOrStdout::Stdout,
                                    node_id: Some(node_id.clone()),
                                    target: None,
                                    message: formatted,
                                    file: None,
                                    line: None,
                                    module_path: None,
                                    timestamp: uhlc
                                        .new_timestamp()
                                        .get_time()
                                        .to_system_time()
                                        .into(),
                                    fields: None,
                                })
                                .await;
                        }
                    }
                }
                // Make sure that all data has been synced to disk.
                let _ = file.sync_all().await.map_err(|err| {
                    logger_c.log(
                        LogLevel::Error,
                        Some("daemon".into()),
                        format!("Could not sync logs to file due to {err}"),
                    )
                });
            }
            let _ = log_finish_tx.send(()).map_err(|_| {
                logger_c.log(
                    LogLevel::Error,
                    Some("daemon".into()),
                    "Could not inform that log file thread finished".to_string(),
                )
            });
        });
        Ok(NodeKind::Spawned { pid })
    }
}

#[must_use]
enum NodeKind {
    Dynamic,
    Spawned { pid: u32 },
}

struct NodeProcessFinished {
    exit_status: NodeExitStatus,
    op_rx: flume::Receiver<ProcessOperation>,
}

#[cfg(test)]
mod tests {
    use super::within_restart_limit;
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    #[test]
    fn allows_up_to_burst_then_denies_within_window() {
        let mut q = VecDeque::new();
        let t0 = Instant::now();
        let burst = 3;
        let win = Duration::from_secs(10);
        // burst restarts within the window are allowed
        assert!(within_restart_limit(&mut q, t0, burst, win));
        assert!(within_restart_limit(&mut q, t0 + Duration::from_secs(1), burst, win));
        assert!(within_restart_limit(&mut q, t0 + Duration::from_secs(2), burst, win));
        // the next restart still inside the window is denied
        assert!(!within_restart_limit(&mut q, t0 + Duration::from_secs(3), burst, win));
        // a denied attempt must not consume budget
        assert_eq!(q.len() as u32, burst);
    }

    #[test]
    fn slow_crash_loop_never_trips() {
        // Crashes spaced wider than the window must restart forever (old timestamps prune away).
        let mut q = VecDeque::new();
        let t0 = Instant::now();
        let burst = 3;
        let win = Duration::from_secs(10);
        for i in 0..100 {
            let now = t0 + Duration::from_secs(i * 11);
            assert!(within_restart_limit(&mut q, now, burst, win), "iteration {i}");
            assert_eq!(q.len(), 1);
        }
    }

    #[test]
    fn entry_exactly_at_interval_is_pruned() {
        let mut q = VecDeque::new();
        let t0 = Instant::now();
        let burst = 1;
        let win = Duration::from_secs(10);
        assert!(within_restart_limit(&mut q, t0, burst, win));
        // exactly `interval` later the old entry ages out (half-open window), so allowed again
        assert!(within_restart_limit(&mut q, t0 + Duration::from_secs(10), burst, win));
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn budget_recovers_after_window_passes() {
        let mut q = VecDeque::new();
        let t0 = Instant::now();
        let burst = 5;
        let win = Duration::from_secs(30);
        for i in 0..5 {
            assert!(within_restart_limit(&mut q, t0 + Duration::from_secs(i), burst, win));
        }
        // 6th within the window is denied
        assert!(!within_restart_limit(&mut q, t0 + Duration::from_secs(5), burst, win));
        // once the earliest restart ages out, a new attempt is allowed again
        assert!(within_restart_limit(&mut q, t0 + Duration::from_secs(31), burst, win));
    }
}
