pub(crate) mod access;
mod client;
mod identity;
mod server;
#[cfg(test)]
mod tests;

use std::fs::File;
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::network::network_io;
use crate::role::Role;

pub(crate) use client::establish;
pub(crate) use server::{serve, serve_with_status};

pub(crate) const WINDOWS_CLIENT: &str = "Windows x86-64 (CMD / PowerShell 7)";
pub(crate) const LINUX_CLIENT: &str = "Linux x86-64 (Bash)";
pub(crate) const MANUAL_ADDRESS: &str = "Replace relay.example with a reachable IP or hostname.";

pub(crate) struct ClientCommands {
    pub(crate) endpoint: String,
    pub(crate) needs_address: bool,
    pub(crate) windows: String,
    pub(crate) linux: String,
}

pub(crate) struct RelayStatus {
    pub(crate) listen: SocketAddr,
    pub(crate) token: Option<String>,
    pub(crate) clients: Option<ClientCommands>,
}

impl RelayStatus {
    pub(crate) fn print(self) {
        eprintln!("Relay listening on {}", self.listen);
        if let Some(token) = self.token {
            eprintln!("Access token: {token}");
        }
        if let Some(clients) = self.clients {
            eprintln!("Client address: {}", clients.endpoint);
            if clients.needs_address {
                eprintln!("{MANUAL_ADDRESS}");
            }
            eprintln!(
                "\n{WINDOWS_CLIENT}\n{}\n\n{LINUX_CLIENT}\n{}",
                clients.windows, clients.linux
            );
        }
    }
}

const MAGIC: [u8; 4] = *b"FDR1";
const WAIT_PEER: Duration = Duration::from_secs(60);
const ATTACH_DATA: Duration = Duration::from_secs(10);
const HANDSHAKE: Duration = Duration::from_secs(5);
const MAX_ROOM_LEN: usize = 64;
const CLOSE_GRACE: Duration = Duration::from_secs(2);
const KEY_LEN: usize = 32;
type Key = [u8; KEY_LEN];
type Attempt = [u8; 16];

const MATCHED: u8 = 1;
const READY: u8 = 2;
const DENIED: u8 = 3;
const BUSY: u8 = 4;
const EXPIRED: u8 = 5;

pub(crate) fn validate_endpoint(endpoint: &str) -> Result<()> {
    server_name(endpoint).map(|_| ())
}

fn server_name(endpoint: &str) -> Result<ServerName<'static>> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .context("relay address must be HOST:PORT")?;
    ensure!(
        port.parse::<u16>().is_ok_and(|port| port != 0),
        "invalid relay port"
    );
    let host = if let Some(ip) = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
    {
        ip.parse::<std::net::Ipv6Addr>()
            .context("invalid relay IPv6 address")?;
        ip
    } else {
        ensure!(
            !host.contains(':'),
            "enclose a relay IPv6 address in brackets"
        );
        host
    };
    ServerName::try_from(host.to_owned()).context("invalid relay host name")
}

pub(crate) fn validate_room(room: &str) -> Result<()> {
    ensure!(
        (1..=MAX_ROOM_LEN).contains(&room.len())
            && room
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "relay room must be 1–64 ASCII letters, digits, hyphens or underscores"
    );
    Ok(())
}

fn read_key(path: &Path) -> Result<Key> {
    let mut text = Vec::with_capacity(KEY_LEN * 2 + 2);
    let result = File::open(path)
        .and_then(|file| file.take((KEY_LEN * 2 + 3) as u64).read_to_end(&mut text));
    result.with_context(|| format!("read relay key file {}", path.display()))?;
    let hex = text
        .strip_suffix(b"\r\n")
        .or_else(|| text.strip_suffix(b"\n"))
        .unwrap_or(&text);
    let error = || {
        anyhow::anyhow!(
            "relay key file {} must contain exactly 64 hexadecimal digits",
            path.display()
        )
    };
    ensure!(hex.len() == KEY_LEN * 2, error());
    let mut result = [0; KEY_LEN];
    for (dst, pair) in result.iter_mut().zip(hex.as_chunks::<2>().0) {
        let high = (pair[0] as char).to_digit(16).ok_or_else(error)?;
        let low = (pair[1] as char).to_digit(16).ok_or_else(error)?;
        *dst = ((high << 4) | low) as u8;
    }
    Ok(result)
}

struct Registration {
    role: Role,
    room: String,
    key: Key,
    attempt: Option<Attempt>,
}

impl Registration {
    async fn read(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Self> {
        let mut header = [0; 7 + KEY_LEN];
        stream.read_exact(&mut header).await?;
        let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid relay registration");
        let room_len = usize::from(header[6]);
        if header[..4] != MAGIC || !(1..=MAX_ROOM_LEN).contains(&room_len) {
            return Err(invalid());
        }
        let role = Role::from_u8(header[4]).map_err(|_| invalid())?;
        let attempt = match header[5] {
            0 => None,
            1 => {
                let mut attempt = [0; 16];
                stream.read_exact(&mut attempt).await?;
                Some(attempt)
            }
            _ => return Err(invalid()),
        };
        let mut room = vec![0; room_len];
        stream.read_exact(&mut room).await?;
        let room = String::from_utf8(room).map_err(|_| invalid())?;
        validate_room(&room).map_err(|_| invalid())?;
        Ok(Self {
            role,
            room,
            key: header[7..].try_into().unwrap(),
            attempt,
        })
    }

    async fn write(&self, stream: &mut (impl AsyncWrite + Unpin)) -> io::Result<()> {
        let mut packet = [0; 7 + KEY_LEN + 16 + MAX_ROOM_LEN];
        packet[..4].copy_from_slice(&MAGIC);
        packet[4] = self.role.as_u8();
        packet[5] = u8::from(self.attempt.is_some());
        packet[6] = self.room.len() as u8;
        packet[7..7 + KEY_LEN].copy_from_slice(&self.key);
        let mut end = 7 + KEY_LEN;
        if let Some(attempt) = &self.attempt {
            packet[end..end + attempt.len()].copy_from_slice(attempt);
            end += attempt.len();
        }
        packet[end..end + self.room.len()].copy_from_slice(self.room.as_bytes());
        stream.write_all(&packet[..end + self.room.len()]).await?;
        stream.flush().await
    }
}

async fn receive_reply(
    stream: &mut (impl AsyncRead + Unpin),
    expected: u8,
    limit: Duration,
) -> Result<()> {
    let reply = timeout(limit, stream.read_u8())
        .await
        .map_err(|_| timed_out("waiting for relay"))?
        .map_err(network_io)?;
    match reply {
        code if code == expected => Ok(()),
        DENIED => anyhow::bail!("relay rejected room or role key"),
        BUSY => Err(network_io(io::Error::new(
            io::ErrorKind::WouldBlock,
            "relay room is busy",
        ))),
        EXPIRED => Err(timed_out("relay pairing expired")),
        _ => anyhow::bail!("unexpected relay response"),
    }
}

fn timed_out(message: &'static str) -> anyhow::Error {
    network_io(io::Error::new(io::ErrorKind::TimedOut, message))
}
