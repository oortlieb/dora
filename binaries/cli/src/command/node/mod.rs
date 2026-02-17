use crate::command::Executable;

mod kill;
mod list;
mod start;
mod stop;

pub use kill::Kill;
pub use list::List;
pub use start::Start;
pub use stop::Stop;

/// Manage and inspect dataflow nodes.
#[derive(Debug, clap::Subcommand)]
pub enum Node {
    List(List),
    /// Stop a running node, disabling its restart policy.
    Stop(Stop),
    /// Start a stopped or restarting node, re-enabling its restart policy.
    Start(Start),
    /// Kill a running node without disabling its restart policy.
    Kill(Kill),
}

impl Executable for Node {
    fn execute(self) -> eyre::Result<()> {
        match self {
            Node::List(cmd) => cmd.execute(),
            Node::Stop(cmd) => cmd.execute(),
            Node::Start(cmd) => cmd.execute(),
            Node::Kill(cmd) => cmd.execute(),
        }
    }
}
