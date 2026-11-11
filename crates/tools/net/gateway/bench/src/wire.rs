//! The request header the bench upstream speaks.
//!
//! One fixed-size header per connection tells the upstream what to do with it.
//! Deliberately trivial: the harness measures the gateway, so the upstream must
//! never be the thing under test.

/// What the upstream should do with a connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Op {
    /// Echo every byte back, until the client half-closes.
    Echo,
    /// Read and discard until EOF, then reply with the total as a big-endian
    /// `u64` so the client can assert nothing was lost.
    Sink,
    /// Write `arg` bytes, then close.
    Source,
}

impl Op {
    const ECHO: u8 = 1;
    const SINK: u8 = 2;
    const SOURCE: u8 = 3;

    pub const fn code(self) -> u8 {
        match self {
            Self::Echo => Self::ECHO,
            Self::Sink => Self::SINK,
            Self::Source => Self::SOURCE,
        }
    }

    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            Self::ECHO => Some(Self::Echo),
            Self::SINK => Some(Self::Sink),
            Self::SOURCE => Some(Self::Source),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Echo => "echo",
            Self::Sink => "sink",
            Self::Source => "source",
        }
    }
}

impl std::fmt::Display for Op {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub const HEADER_LEN: usize = 9;

pub fn encode(op: Op, arg: u64) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    let (code, rest) = header.split_at_mut(1);
    code.fill(op.code());
    rest.copy_from_slice(&arg.to_be_bytes());
    header
}

pub fn decode(header: &[u8; HEADER_LEN]) -> Option<(Op, u64)> {
    let (code, rest) = header.split_at(1);
    let op = Op::from_code(*code.first()?)?;
    let arg = u64::from_be_bytes(rest.try_into().ok()?);
    Some((op, arg))
}

#[cfg(test)]
mod tests;
