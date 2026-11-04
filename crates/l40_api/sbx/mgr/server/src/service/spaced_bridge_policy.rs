use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SpacedBridgePolicy {
    RouteTokenRequired,
    TrustedWaitReadyLifecycle,
    NodeCtlExempt,
}

macro_rules! spaced_rpcs {
    ($($variant:ident => ($operation:literal, $policy:ident),)+) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) enum SpacedRpc {
            $($variant,)+
        }

        impl SpacedRpc {
            #[cfg(test)]
            pub(super) const ALL: &[Self] = &[$(Self::$variant,)+];

            pub(super) const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $operation,)+
                }
            }

            pub(super) const fn policy(self) -> SpacedBridgePolicy {
                match self {
                    $(Self::$variant => SpacedBridgePolicy::$policy,)+
                }
            }
        }

        impl fmt::Display for SpacedRpc {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

    };
}

spaced_rpcs! {
    PingGuest => ("SpacedPingGuest", RouteTokenRequired),
    GetStatus => ("SpacedGetStatus", RouteTokenRequired),
    WaitReady => ("SpacedWaitReady", TrustedWaitReadyLifecycle),
    Stat => ("SpacedStat", RouteTokenRequired),
    List => ("SpacedList", RouteTokenRequired),
    Read => ("SpacedRead", RouteTokenRequired),
    Write => ("SpacedWrite", RouteTokenRequired),
    WriteBatch => ("SpacedWriteBatch", RouteTokenRequired),
    Mkdir => ("SpacedMkdir", RouteTokenRequired),
    Remove => ("SpacedRemove", RouteTokenRequired),
    Rename => ("SpacedRename", RouteTokenRequired),
    NormalizeWatchRoot => ("SpacedNormalizeWatchRoot", RouteTokenRequired),
    Watch => ("SpacedWatch", RouteTokenRequired),
    CreateWatcher => ("SpacedCreateWatcher", RouteTokenRequired),
    GetWatcherEvents => ("SpacedGetWatcherEvents", RouteTokenRequired),
    RemoveWatcher => ("SpacedRemoveWatcher", RouteTokenRequired),
    EnableFileChanges => ("SpacedEnableFileChanges", RouteTokenRequired),
    DisableFileChanges => ("SpacedDisableFileChanges", RouteTokenRequired),
    GetModifiedFiles => ("SpacedGetModifiedFiles", RouteTokenRequired),
    StartProcess => ("SpacedStartProcess", RouteTokenRequired),
    ConnectProcess => ("SpacedConnectProcess", RouteTokenRequired),
    ListProcesses => ("SpacedListProcesses", RouteTokenRequired),
    SendSignal => ("SpacedSendSignal", RouteTokenRequired),
    SendInput => ("SpacedSendInput", RouteTokenRequired),
    CloseStdin => ("SpacedCloseStdin", RouteTokenRequired),
    ResizePty => ("SpacedResizePty", RouteTokenRequired),
    KillProcess => ("SpacedKillProcess", RouteTokenRequired),
    OpenLoopbackBridge => ("SpacedOpenLoopbackBridge", NodeCtlExempt),
    CloseLoopbackBridge => ("SpacedCloseLoopbackBridge", NodeCtlExempt),
}

#[cfg(test)]
mod tests;
