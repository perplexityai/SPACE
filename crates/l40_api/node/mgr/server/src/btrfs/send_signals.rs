//! A send ioctl cannot be replayed after any bytes have reached its output.
use std::io;
use std::marker::PhantomData;

use nix::sys::signal::{SigSet, SigmaskHow, Signal};

pub(super) struct BlockedSignals {
    previous: SigSet,
    // Signal masks belong to the calling OS thread, including during unwind.
    _thread: PhantomData<*mut ()>,
}

impl BlockedSignals {
    pub(super) fn new() -> io::Result<Self> {
        // A backpressured streaming write can return ERESTARTSYS after earlier
        // commands have escaped. SA_RESTART then replays the whole send ioctl,
        // producing partial prefixes followed by a valid stream, yet success.
        // Block application signals only on this dedicated blocking thread.
        // This also delays profiler timer signals until the send finishes.
        // The pipe producer cancels by closing its reader, failing the write.
        let mut blocked = SigSet::all();
        // Synchronous faults must still reach Rust's stack-overflow reporting,
        // debuggers and crash handlers while the ioctl is running.
        for signal in [
            Signal::SIGSEGV,
            Signal::SIGBUS,
            Signal::SIGILL,
            Signal::SIGFPE,
            Signal::SIGTRAP,
            Signal::SIGSYS,
        ] {
            blocked.remove(signal);
        }
        let previous = blocked.thread_swap_mask(SigmaskHow::SIG_BLOCK)?;
        Ok(Self {
            previous,
            _thread: PhantomData,
        })
    }
}

impl Drop for BlockedSignals {
    fn drop(&mut self) {
        if let Err(error) = self.previous.thread_set_mask() {
            tracing::error!(%error, "failed to restore btrfs send thread signal mask");
        }
    }
}

#[cfg(test)]
mod tests;
