//! TLS interception exemptions, optionally scoped to one destination port.

#[cfg(feature = "serde")]
use std::net::IpAddr;
use std::num::NonZeroU16;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[cfg(feature = "serde")]
use crate::{DomainName, IpCidr, WildcardDomainName};
use crate::{
    EgressDestinationV1, NetworkPolicyProtoError, NetworkPolicyValidationError,
    ProtoDestinationError, proto, proto_destination_error,
};

/// A destination whose TLS is relayed without termination after normal egress
/// admission, on every destination port or on one.
///
/// The JSON form is the destination object with an optional `port`, for example
/// `{"domain":"push.example.test","port":5228}`. Without a port it serializes
/// exactly as the [`EgressDestinationV1`] it wraps, so existing policies keep
/// their identity hash. An older Rust reader rejects a `port` key rather than
/// dropping it, because a destination object has exactly one key. The runtime
/// shim reads this form too, so it must be upgraded before any policy has a port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlsInterceptionExemptionV1 {
    pub destination: EgressDestinationV1,
    /// `None` exempts every destination port.
    pub port: Option<NonZeroU16>,
}

impl TlsInterceptionExemptionV1 {
    pub const fn all_ports(destination: EgressDestinationV1) -> Self {
        Self {
            destination,
            port: None,
        }
    }

    pub fn new(
        destination: EgressDestinationV1,
        port: Option<u32>,
    ) -> Result<Self, NetworkPolicyValidationError> {
        let port = port
            .map(|port| {
                u16::try_from(port)
                    .ok()
                    .and_then(NonZeroU16::new)
                    .ok_or(NetworkPolicyValidationError::InvalidTlsInterceptionExemptionPort)
            })
            .transpose()?;
        Ok(Self { destination, port })
    }

    pub fn matches_port(&self, port: u16) -> bool {
        self.port.is_none_or(|exempt| exempt.get() == port)
    }
}

impl From<EgressDestinationV1> for TlsInterceptionExemptionV1 {
    fn from(destination: EgressDestinationV1) -> Self {
        Self::all_ports(destination)
    }
}

#[cfg(feature = "serde")]
impl Serialize for TlsInterceptionExemptionV1 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(1 + usize::from(self.port.is_some())))?;
        match &self.destination {
            EgressDestinationV1::Ip(ip) => map.serialize_entry("ip", ip)?,
            EgressDestinationV1::Cidr(cidr) => map.serialize_entry("cidr", cidr)?,
            EgressDestinationV1::Domain(domain) => map.serialize_entry("domain", domain)?,
            EgressDestinationV1::WildcardDomain(domain) => {
                map.serialize_entry("wildcard_domain", domain)?;
            }
        }
        if let Some(port) = self.port {
            map.serialize_entry("port", &port.get())?;
        }
        map.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for TlsInterceptionExemptionV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // An absent key is `None`; an explicit `null` is rejected, so
        // `"port": null` cannot read as every port.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Helper {
            #[serde(default, deserialize_with = "present")]
            ip: Option<IpAddr>,
            #[serde(default, deserialize_with = "present")]
            cidr: Option<IpCidr>,
            #[serde(default, deserialize_with = "present")]
            domain: Option<DomainName>,
            #[serde(default, deserialize_with = "present")]
            wildcard_domain: Option<WildcardDomainName>,
            #[serde(default, deserialize_with = "present")]
            port: Option<u32>,
        }

        fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
        where
            D: Deserializer<'de>,
            T: Deserialize<'de>,
        {
            T::deserialize(deserializer).map(Some)
        }

        let value = Helper::deserialize(deserializer)?;
        let mut destinations = [
            value.ip.map(EgressDestinationV1::Ip),
            value.cidr.map(EgressDestinationV1::Cidr),
            value.domain.map(EgressDestinationV1::Domain),
            value
                .wildcard_domain
                .map(EgressDestinationV1::WildcardDomain),
        ]
        .into_iter()
        .flatten();
        let (Some(destination), None) = (destinations.next(), destinations.next()) else {
            return Err(de::Error::custom(
                "TLS interception exemption must name exactly one of ip, cidr, domain or wildcard_domain",
            ));
        };
        Self::new(destination, value.port).map_err(de::Error::custom)
    }
}

impl From<&TlsInterceptionExemptionV1> for proto::TlsInterceptionExemptionV1 {
    fn from(value: &TlsInterceptionExemptionV1) -> Self {
        Self {
            destination: Some((&value.destination).into()),
            port: value.port.map(|port| u32::from(port.get())),
        }
    }
}

/// The protobuf `tls_interception_exemptions` field: only exemptions that cover
/// every port, so a reader that predates ports never exempts a port-scoped one.
pub(crate) fn all_port_destinations(
    exemptions: &[TlsInterceptionExemptionV1],
) -> Vec<proto::EgressDestinationV1> {
    exemptions
        .iter()
        .filter(|exemption| exemption.port.is_none())
        .map(|exemption| (&exemption.destination).into())
        .collect()
}

/// The protobuf `tls_interception_exemptions_with_ports` field: the ordered
/// list, set only when an exemption is port-scoped so existing encodings are
/// unchanged.
pub(crate) fn with_ports(
    exemptions: &[TlsInterceptionExemptionV1],
) -> Vec<proto::TlsInterceptionExemptionV1> {
    if exemptions.iter().any(|exemption| exemption.port.is_some()) {
        exemptions.iter().map(Into::into).collect()
    } else {
        Vec::new()
    }
}

/// Without port-scoped exemptions the legacy field is the whole list. With them,
/// the ordered field is authoritative and the legacy field must be exactly its
/// port-independent projection, so each policy has one encoding and a reader
/// that ignores the ordered field never exempts a port-scoped destination.
pub(crate) fn from_proto(
    policy: &proto::NetworkPolicyV1,
) -> Result<Vec<TlsInterceptionExemptionV1>, NetworkPolicyProtoError> {
    let all_ports = crate::proto_destinations_to_common(
        "tls_interception_exemptions",
        &policy.tls_interception_exemptions,
    )?;
    if policy.tls_interception_exemptions_with_ports.is_empty() {
        return Ok(all_ports
            .into_iter()
            .map(TlsInterceptionExemptionV1::all_ports)
            .collect());
    }
    let exemptions = policy
        .tls_interception_exemptions_with_ports
        .iter()
        .enumerate()
        .map(|(index, exemption)| {
            let destination = exemption
                .destination
                .as_ref()
                .ok_or(ProtoDestinationError::MissingDestination)
                .and_then(EgressDestinationV1::try_from)
                .map_err(|error| {
                    proto_destination_error("tls_interception_exemptions_with_ports", index, error)
                })?;
            TlsInterceptionExemptionV1::new(destination, exemption.port)
                .map_err(|_| NetworkPolicyProtoError::InvalidTlsInterceptionExemptionPort { index })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let projection = exemptions
        .iter()
        .filter(|exemption| exemption.port.is_none())
        .map(|exemption| &exemption.destination);
    if !exemptions.iter().any(|exemption| exemption.port.is_some())
        || !projection.eq(all_ports.iter())
    {
        return Err(NetworkPolicyProtoError::InconsistentTlsInterceptionExemptions);
    }
    Ok(exemptions)
}
