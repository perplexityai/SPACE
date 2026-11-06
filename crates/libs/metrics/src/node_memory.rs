//! OOM kills of sandboxes on a node, split by whose memory limit forced them.
//!
//! `node_memory.sandbox_oom_kills` (counter): processes the kernel OOM killer
//! ended in a sandbox cgroup, counted once when the cgroup is removed. Tags
//! `cgroup` (`vm` | `sidecar`) and `cause`, from [`SandboxOomCause`]. The
//! sandbox id is in the warning logged alongside it, not in a tag.

use crate::emit::{counter, tag};

metric_vocabulary!(
    /// The sandbox cgroup an OOM kill happened in.
    SandboxOomCgroup {
        /// The cgroup holding Firecracker and its vCPU threads.
        Vm => "vm",
        /// The `sbx-{id}` cgroup holding the sandbox's sidecars.
        Sidecar => "sidecar",
    }
);

metric_vocabulary!(
    /// Whose limit forced a sandbox OOM kill.
    SandboxOomCause {
        /// The sandbox cgroup, or one inside it, reached its own `memory.max`.
        SandboxLimit => "sandbox_limit",
        /// The sandbox stayed within its own limit, so the node ran out: the
        /// shared sandbox parent reached its cap, or the whole host did.
        NodePressure => "node_pressure",
    }
);

pub fn sandbox_oom_kills(cgroup: SandboxOomCgroup, cause: SandboxOomCause, kills: u64) {
    counter(
        "node_memory.sandbox_oom_kills",
        kills,
        &[tag("cgroup", cgroup), tag("cause", cause)],
    );
}
