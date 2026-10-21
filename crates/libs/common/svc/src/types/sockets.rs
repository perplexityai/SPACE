use std::fmt;

pub use pplx_space_unix_socket_path::{UnixSocketPath, UnixSocketPathParseError};

macro_rules! socket_path_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(UnixSocketPath);

        impl $name {
            #[doc = concat!("Builds a `", stringify!($name), "` from a validated socket path.")]
            #[doc = ""]
            #[doc = "```"]
            #[doc = concat!(
                "# use pplx_space_common_svc::types::{",
                stringify!($name),
                ", UnixSocketPath};"
            )]
            #[doc = "let path = UnixSocketPath::parse(\"/run/space/service.sock\").unwrap();"]
            #[doc = concat!("let typed = ", stringify!($name), "::new(path);")]
            #[doc = "assert_eq!(typed.socket_path().as_path().to_str(), Some(\"/run/space/service.sock\"));"]
            #[doc = "```"]
            pub const fn new(value: UnixSocketPath) -> Self {
                Self(value)
            }

            #[doc = concat!("Returns the underlying `UnixSocketPath` for this `", stringify!($name), "`.")]
            #[doc = ""]
            #[doc = "```"]
            #[doc = concat!(
                "# use pplx_space_common_svc::types::{",
                stringify!($name),
                ", UnixSocketPath};"
            )]
            #[doc = "let path = UnixSocketPath::parse(\"/run/space/service.sock\").unwrap();"]
            #[doc = concat!("let typed = ", stringify!($name), "::new(path);")]
            #[doc = "assert!(typed.socket_path().as_path().is_absolute());"]
            #[doc = "```"]
            pub const fn socket_path(&self) -> &UnixSocketPath {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.as_path().display().fmt(f)
            }
        }
    };
}

socket_path_type!(
    /// Unix socket path used to expose a vsock bridge over UDS.
    ///
    /// ```
    /// # use pplx_space_common_svc::types::{UnixSocketPath, VsockUdsPath};
    /// let path = UnixSocketPath::parse("/run/space/vsock.sock").unwrap();
    /// let uds = VsockUdsPath::new(path);
    /// assert!(uds.socket_path().as_path().is_absolute());
    /// ```
    VsockUdsPath
);
socket_path_type!(
    /// Unix socket path for the spaced-host service.
    ///
    /// ```
    /// # use pplx_space_common_svc::types::{SpacedHostSocketPath, UnixSocketPath};
    /// let path = UnixSocketPath::parse("/run/space/spaced-host.sock").unwrap();
    /// let socket = SpacedHostSocketPath::new(path);
    /// assert!(socket.socket_path().as_path().is_absolute());
    /// ```
    SpacedHostSocketPath
);
socket_path_type!(
    /// Unix socket path for the network gateway control service.
    ///
    /// ```
    /// # use pplx_space_common_svc::types::{NetGatewayControlSocketPath, UnixSocketPath};
    /// let path = UnixSocketPath::parse("/run/space/net-gateway.sock").unwrap();
    /// let socket = NetGatewayControlSocketPath::new(path);
    /// assert!(socket.socket_path().as_path().is_absolute());
    /// ```
    NetGatewayControlSocketPath
);
socket_path_type!(
    /// Unix socket path for the bridge control service.
    ///
    /// ```
    /// # use pplx_space_common_svc::types::{BridgeControlSocketPath, UnixSocketPath};
    /// let path = UnixSocketPath::parse("/run/space/bridge.sock").unwrap();
    /// let socket = BridgeControlSocketPath::new(path);
    /// assert!(socket.socket_path().as_path().is_absolute());
    /// ```
    BridgeControlSocketPath
);
