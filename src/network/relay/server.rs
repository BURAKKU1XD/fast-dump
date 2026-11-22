use std::collections::HashMap;
use std::fs;
use std::io as std_io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tokio::io::{
    self, AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

use super::{
    ATTACH_DATA, Attempt, BUSY, CLOSE_GRACE, DENIED, EXPIRED, HANDSHAKE, Key, MATCHED, READY,
    Registration, RelayStatus, WAIT_PEER, read_key, validate_room,
};
use crate::cli::RelayArgs;
use crate::role::Role;
use crate::runtime::RunControl;

const MAX_ROOMS: usize = 64;
const MAX_UNREGISTERED: usize = 64;
const ROOM_QUEUE: usize = 4;
const CONTROL_BUFFER: usize = 8 * 1024;
const DATA_BUFFER: usize = 64 * 1024;

type Stream = TlsStream<TcpStream>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    certificate: PathBuf,
    private_key: PathBuf,
    #[serde(default)]
    rooms: HashMap<String, RoomConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RoomConfig {
    sender_key_file: PathBuf,
    receiver_key_file: PathBuf,
}

struct Room {
    keys: [Key; 2],
    incoming: mpsc::Sender<Join>,
}

struct Join {
    stream: Stream,
    role: Role,
    attempt: Option<Attempt>,
}

pub(crate) fn serve(args: RelayArgs, control: &RunControl) -> Result<()> {
    serve_with_status(args, control, RelayStatus::print)
}

pub(crate) fn serve_with_status(
    args: RelayArgs,
    control: &RunControl,
    ready: impl FnOnce(RelayStatus),
) -> Result<()> {
    let automatic = args.access
        && args.config.is_none()
        && args.certificate.is_none()
        && args.private_key.is_none();
    let (tls, keys) = if automatic {
        let (tls, _) = super::identity::generate()?;
        (tls, HashMap::new())
    } else {
        let (tls, keys) = configured_tls(&args)?;
        (tls, keys)
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(std::thread::available_parallelism().map_or(1, |n| n.get().min(4)))
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let listener = TcpListener::bind(args.listen)
            .await
            .map_err(|error| crate::network::listen_error(args.listen, error))?;
        if args.access {
            super::access::listen(listener, tls, control, automatic, ready).await
        } else {
            ready(RelayStatus {
                listen: listener.local_addr()?,
                token: None,
                clients: None,
            });
            listen(listener, tls, keys, control).await
        }
    })
}

fn configured_tls(args: &RelayArgs) -> Result<(TlsAcceptor, HashMap<String, [Key; 2]>)> {
    let (config, dir) = if let Some(file) = args.config.as_deref() {
        ensure!(
            args.certificate.is_none() && args.private_key.is_none(),
            "choose either relay TOML or certificate and private key"
        );
        let text = fs::read_to_string(file).context("read relay configuration")?;
        let config = toml::from_str(&text).context("parse relay configuration")?;
        (config, file.parent().unwrap_or_else(|| Path::new(".")))
    } else {
        ensure!(args.access, "role-key relay requires --config");
        let config = Config {
            certificate: args
                .certificate
                .clone()
                .context("relay TLS certificate is required")?,
            private_key: args
                .private_key
                .clone()
                .context("relay TLS private key is required")?,
            rooms: HashMap::new(),
        };
        (config, Path::new("."))
    };
    let certificates = CertificateDer::pem_file_iter(dir.join(&config.certificate))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(dir.join(&config.private_key))?;
    let tls = acceptor(certificates, key)?;
    let mut keys = HashMap::new();
    if !args.access {
        ensure!(
            (1..=MAX_ROOMS).contains(&config.rooms.len()),
            "configure 1–64 relay rooms"
        );
        for (name, room) in config.rooms {
            validate_room(&name)?;
            let sender = read_key(&dir.join(room.sender_key_file))?;
            let receiver = read_key(&dir.join(room.receiver_key_file))?;
            ensure!(
                sender != receiver,
                "relay room {name} must have distinct role keys"
            );
            ensure!(
                !keys
                    .values()
                    .any(|pair: &[Key; 2]| pair.contains(&sender) || pair.contains(&receiver)),
                "relay keys must be unique across rooms"
            );
            keys.insert(name, [sender, receiver]);
        }
    }
    Ok((tls, keys))
}

pub(super) fn acceptor(
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<TlsAcceptor> {
    let tls =
        ServerConfig::builder_with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certificates, key)?;
    Ok(TlsAcceptor::from(Arc::new(tls)))
}

pub(super) async fn listen(
    listener: TcpListener,
    tls: TlsAcceptor,
    keys: HashMap<String, [Key; 2]>,
    control: &RunControl,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    let rooms = Arc::new(
        keys.into_iter()
            .map(|(name, keys)| {
                let (incoming, queue) = mpsc::channel(ROOM_QUEUE);
                tasks.spawn(serve_room(name.clone(), queue));
                (name, Room { keys, incoming })
            })
            .collect::<HashMap<_, _>>(),
    );
    let max_tasks = rooms.len() + MAX_UNREGISTERED;
    let mut poll_cancel = tokio::time::interval(Duration::from_millis(50));
    let result = loop {
        tokio::select! {
            _ = poll_cancel.tick() => {
                if let Err(error) = control.check() { break Err(error); }
            }
            result = tasks.join_next() => {
                if let Some(Err(error)) = result { break Err(error.into()); }
            }
            result = listener.accept(), if tasks.len() < max_tasks => {
                let (socket, _) = match result {
                    Ok(connection) => connection,
                    Err(error) => break Err(error).context("accept relay connection"),
                };
                let tls = tls.clone();
                let rooms = Arc::clone(&rooms);
                tasks.spawn(async move {
                    if let Err(error) = timeout(HANDSHAKE, register(socket, tls, &rooms)).await {
                        log::debug!("relay registration expired: {error}");
                    }
                });
            }
        }
    };
    tasks.shutdown().await;
    result
}

async fn register(
    socket: TcpStream,
    tls: TlsAcceptor,
    rooms: &HashMap<String, Room>,
) -> std_io::Result<()> {
    socket.set_nodelay(true)?;
    let mut stream = tls.accept(socket).await?;
    let request = Registration::read(&mut stream).await?;
    let Some(room) = rooms.get(&request.room) else {
        return reject(stream, DENIED).await;
    };
    if !bool::from(room.keys[role_index(request.role)].ct_eq(&request.key)) {
        return reject(stream, DENIED).await;
    }
    let join = Join {
        stream,
        role: request.role,
        attempt: request.attempt,
    };
    if let Err(error) = room.incoming.try_send(join) {
        return reject(error.into_inner().stream, BUSY).await;
    }
    Ok(())
}

const fn role_index(role: Role) -> usize {
    (role.as_u8() - 1) as usize
}

async fn reject(mut stream: Stream, code: u8) -> std_io::Result<()> {
    stream.write_u8(code).await?;
    stream.flush().await?;
    stream.shutdown().await
}

async fn reject_briefly(stream: Stream, code: u8) {
    let _ = timeout(Duration::from_millis(100), reject(stream, code)).await;
}

async fn serve_room(name: String, mut queue: mpsc::Receiver<Join>) {
    while let Some(first) = queue.recv().await {
        if first.attempt.is_some() {
            reject_briefly(first.stream, EXPIRED).await;
            continue;
        }
        if let Err(error) = pair(first, &mut queue).await {
            log::debug!("relay room {name}: {error:#}");
        }
    }
}

async fn pair(mut first: Join, queue: &mut mpsc::Receiver<Join>) -> Result<()> {
    let deadline = Instant::now() + WAIT_PEER;
    let second = loop {
        tokio::select! {
            biased;
            _ = first.stream.read_u8() => return Ok(()),
            _ = sleep_until(deadline) => {
                reject_briefly(first.stream, EXPIRED).await;
                return Ok(());
            }
            message = queue.recv() => {
                let Some(next) = message else { return Ok(()); };
                if next.attempt.is_some() {
                    reject_briefly(next.stream, EXPIRED).await;
                } else if next.role == first.role {
                    reject_briefly(next.stream, BUSY).await;
                } else {
                    break next;
                }
            }
        }
    };

    let (mut sender_control, mut receiver_control) = match first.role {
        Role::Sender => (first.stream, second.stream),
        Role::Receiver => (second.stream, first.stream),
    };
    let mut attempt = [0; 16];
    SystemRandom::new()
        .fill(&mut attempt)
        .map_err(|_| anyhow::anyhow!("generate relay attempt ID"))?;
    let mut matched = [MATCHED; 17];
    matched[1..].copy_from_slice(&attempt);
    timeout(HANDSHAKE, async {
        tokio::try_join!(
            send(&mut sender_control, &matched),
            send(&mut receiver_control, &matched)
        )
    })
    .await??;

    let deadline = Instant::now() + ATTACH_DATA;
    let mut data = [None, None];
    while data.iter().any(Option::is_none) {
        tokio::select! {
            biased;
            _ = sender_control.read_u8() => return Ok(()),
            _ = receiver_control.read_u8() => return Ok(()),
            _ = sleep_until(deadline) => return Ok(()),
            message = queue.recv() => {
                let Some(join) = message else { return Ok(()); };
                let slot = &mut data[role_index(join.role)];
                match join.attempt {
                    None => reject_briefly(join.stream, BUSY).await,
                    Some(id) if id != attempt => reject_briefly(join.stream, EXPIRED).await,
                    Some(_) if slot.is_some() => reject_briefly(join.stream, BUSY).await,
                    Some(_) => *slot = Some(join.stream),
                }
            }
        }
    }
    let [Some(mut sender_data), Some(mut receiver_data)] = data else {
        unreachable!()
    };
    timeout(HANDSHAKE, async {
        tokio::try_join!(
            send(&mut sender_control, &[READY]),
            send(&mut receiver_control, &[READY]),
            send(&mut sender_data, &[READY]),
            send(&mut receiver_data, &[READY]),
        )
    })
    .await??;

    let forwarding = forward(sender_control, receiver_control, sender_data, receiver_data);
    tokio::pin!(forwarding);
    loop {
        tokio::select! {
            result = &mut forwarding => return result,
            message = queue.recv() => {
                let Some(join) = message else { return Ok(()); };
                // Пересылка продолжает работать, пока новому подключению отправляется отказ.
                tokio::select! {
                    result = &mut forwarding => return result,
                    _ = reject_briefly(join.stream, BUSY) => {}
                }
            }
        }
    }
}

async fn send(stream: &mut Stream, reply: &[u8]) -> std_io::Result<()> {
    stream.write_all(reply).await?;
    stream.flush().await
}

pub(super) async fn forward<S: AsyncRead + AsyncWrite + Unpin>(
    sc: S,
    rc: S,
    sd: S,
    rd: S,
) -> Result<()> {
    let (sc_read, sc_write) = io::split(sc);
    let (rc_read, rc_write) = io::split(rc);
    let (sd_read, sd_write) = io::split(sd);
    let (rd_read, rd_write) = io::split(rd);
    let eof = Notify::new();
    let pipes = async {
        tokio::try_join!(
            pipe(
                &eof,
                BufReader::with_capacity(CONTROL_BUFFER, sc_read),
                rc_write
            ),
            pipe(
                &eof,
                BufReader::with_capacity(CONTROL_BUFFER, rc_read),
                sc_write
            ),
            pipe(
                &eof,
                BufReader::with_capacity(DATA_BUFFER, sd_read),
                rd_write
            ),
            pipe(
                &eof,
                BufReader::with_capacity(CONTROL_BUFFER, rd_read),
                sd_write
            ),
        )?;
        Ok(())
    };
    tokio::pin!(pipes);
    tokio::select! {
        result = &mut pipes => result,
        _ = eof.notified() => {
            // Чистый EOF даёт другому каналу время доставить уже записанный FinishAck.
            tokio::select! {
                result = &mut pipes => result,
                _ = sleep(CLOSE_GRACE) => Ok(()),
            }
        }
    }
}

async fn pipe(
    eof: &Notify,
    mut input: impl AsyncBufRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
) -> Result<()> {
    io::copy_buf(&mut input, &mut output).await?;
    eof.notify_one();
    output.shutdown().await?;
    Ok(())
}
