pub(crate) mod direct;
pub(crate) mod lobby;
pub mod protocol;
pub(crate) mod relay;
pub mod transport;

use std::fmt;
use std::io;
use std::net::SocketAddr;

#[derive(Debug)]
pub(crate) struct ListenError {
    address: SocketAddr,
    pub(crate) kind: io::ErrorKind,
}

impl fmt::Display for ListenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self.kind {
            io::ErrorKind::AddrInUse => "port is already in use",
            io::ErrorKind::AddrNotAvailable => "address is unavailable on this host",
            io::ErrorKind::PermissionDenied => "access denied or port reserved",
            _ => return write!(formatter, "Cannot listen on {}", self.address),
        };
        write!(formatter, "Cannot listen on {}: {reason}", self.address)
    }
}

pub(crate) fn listen_error(address: SocketAddr, error: io::Error) -> anyhow::Error {
    let kind = error.kind();
    anyhow::Error::new(error).context(ListenError { address, kind })
}

#[derive(Debug)]
struct NetworkIo;

impl fmt::Display for NetworkIo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("network I/O failed")
    }
}

// Метка ставится только на I/O самого канала: EOF декодера или исходного файла
// не должен превращать ошибку протокола/хранилища в повтор всей передачи.
pub(crate) fn network_io(error: io::Error) -> anyhow::Error {
    anyhow::Error::new(error).context(NetworkIo)
}

pub(crate) fn is_retryable_error(error: &anyhow::Error) -> bool {
    if error.is::<crate::runtime::Cancelled>() || !error.is::<NetworkIo>() {
        return false;
    }
    error.downcast_ref::<io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::NotConnected
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::TimedOut
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::WriteZero
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::Interrupted
                | io::ErrorKind::NetworkDown
                | io::ErrorKind::NetworkUnreachable
                | io::ErrorKind::HostUnreachable
        )
    })
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    #[test]
    fn retry_requires_network_origin_and_transient_kind() {
        for kind in [
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::WouldBlock,
        ] {
            assert!(!is_retryable_error(&anyhow::Error::new(io::Error::from(
                kind
            ))));
            let error = network_io(io::Error::from(kind)).context("outer session context");
            assert!(is_retryable_error(&error));
            assert_eq!(error.downcast_ref::<io::Error>().unwrap().kind(), kind);
        }
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AddrInUse,
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::InvalidData,
            io::ErrorKind::Other,
        ] {
            assert!(!is_retryable_error(&network_io(io::Error::from(kind))));
        }
        assert!(!is_retryable_error(&anyhow::Error::new(
            crate::runtime::Cancelled
        )));
    }
}
