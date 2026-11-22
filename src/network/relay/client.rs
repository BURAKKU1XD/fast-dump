use std::future::Future;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{Mutex, watch};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, version};
use tokio_rustls::{TlsConnector, client::TlsStream};

use super::{
    ATTACH_DATA, HANDSHAKE, MATCHED, READY, Registration, WAIT_PEER, read_key, receive_reply,
    server_name, timed_out,
};
use crate::cli::TransportArgs;
use crate::network::network_io;
use crate::network::protocol::wire::ChannelKind;
use crate::network::transport::{
    CloneableStream, Session, assemble_session, handshake_channel, next_session_nonce,
};
use crate::progress::{self, Phase, PipelineStage, PipelineState};
use crate::role::Role;
use crate::runtime::{SessionControl, SocketRegistration};

pub(super) type Stream = TlsStream<TcpStream>;
pub(super) type TransferStream = tokio_rustls::TlsStream<TcpStream>;

pub(crate) fn establish(
    args: &TransportArgs,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
    control: &SessionControl,
) -> Result<(Session<RelayStream>, [SocketRegistration; 2])> {
    let endpoint = args.relay.as_deref().context("missing relay address")?;
    let name = server_name(endpoint)?;
    let mut registration = Registration {
        role,
        room: args.room.clone().context("missing relay room")?,
        key: read_key(
            args.relay_token_file
                .as_deref()
                .context("missing relay key file")?,
        )?,
        attempt: None,
    };
    let tls = connector(args.relay_ca_file.as_deref())?;
    let runtime = ClientRuntime::create()?;

    if let Some(session) = control.progress() {
        session.set_phase(Phase::Connecting);
        control.log(
            log::Level::Info,
            "Waiting for the other peer through the relay",
        );
    } else {
        eprintln!("Waiting for the other peer through the relay at {endpoint}");
    }
    let waiting = progress::stage(
        Some(control),
        PipelineStage::Control,
        PipelineState::WaitingConnection,
    );
    let joined = runtime.handle().block_on(async {
        let (mut control_stream, control_socket) =
            open(endpoint, None, name.clone(), &tls, &registration, control).await?;
        receive_reply(&mut control_stream, MATCHED, WAIT_PEER + HANDSHAKE).await?;
        let mut attempt = [0; 16];
        timeout(HANDSHAKE, control_stream.read_exact(&mut attempt))
            .await
            .map_err(|_| timed_out("waiting for relay attempt"))?
            .map_err(network_io)?;
        registration.attempt = Some(attempt);
        let address = control_stream.get_ref().0.peer_addr().map_err(network_io)?;
        let (mut data_stream, data_socket) =
            open(endpoint, Some(address), name, &tls, &registration, control).await?;
        tokio::try_join!(
            receive_reply(&mut control_stream, READY, ATTACH_DATA + HANDSHAKE),
            receive_reply(&mut data_stream, READY, ATTACH_DATA + HANDSHAKE),
        )?;
        Ok::<_, anyhow::Error>((control_stream, data_stream, [control_socket, data_socket]))
    });
    drop(waiting);
    control.check()?;
    let (control_stream, data_stream, sockets) = joined?;
    establish_channels(
        [control_stream.into(), data_stream.into()],
        sockets,
        runtime,
        role,
        dump_id,
        token,
        control,
    )
}

pub(super) fn connector(ca_file: Option<&Path>) -> Result<TlsConnector> {
    let mut roots = RootCertStore::empty();
    if let Some(file) = ca_file {
        for cert in CertificateDer::pem_file_iter(file).context("read relay trust anchors")? {
            roots.add(cert?)?;
        }
        ensure!(!roots.is_empty(), "relay trust file has no certificates");
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let tls = ClientConfig::builder_with_protocol_versions(&[&version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(tls)))
}

pub(super) fn establish_channels(
    [control_stream, data_stream]: [TransferStream; 2],
    sockets: [SocketRegistration; 2],
    runtime: Arc<ClientRuntime>,
    role: Role,
    dump_id: Option<&str>,
    token: Option<&str>,
    control: &SessionControl,
) -> Result<(Session<RelayStream>, [SocketRegistration; 2])> {
    let _handshake = progress::stage(
        Some(control),
        PipelineStage::Control,
        PipelineState::Handshake,
    );
    let nonce = next_session_nonce();
    let deadline = Instant::now() + HANDSHAKE;
    let establish_channel = |stream: TransferStream, kind| {
        let local_addr = stream.get_ref().0.local_addr().map_err(network_io)?;
        let peer_addr = stream.get_ref().0.peer_addr().map_err(network_io)?;
        let mut channel = handshake_channel(
            RelayStream::new(stream, Arc::clone(&runtime), deadline),
            role,
            dump_id,
            token,
            Some((kind, nonce)),
            local_addr,
            peer_addr,
        )?;
        channel.stream.deadline = None;
        Ok::<_, anyhow::Error>(channel)
    };
    let established = (|| {
        let ctrl = establish_channel(control_stream, ChannelKind::Control)?;
        let data = establish_channel(data_stream, ChannelKind::Data)?;
        assemble_session(ctrl, data)
    })();
    control.check()?;
    Ok((established?, sockets))
}

async fn open(
    endpoint: &str,
    address: Option<SocketAddr>,
    name: ServerName<'static>,
    tls: &TlsConnector,
    request: &Registration,
    control: &SessionControl,
) -> Result<(Stream, SocketRegistration)> {
    within_handshake(control, async {
        let (mut stream, registration) = open_stream(endpoint, address, name, tls, control).await?;
        request.write(&mut stream).await.map_err(network_io)?;
        Ok((stream, registration))
    })
    .await
}

pub(super) async fn open_stream(
    endpoint: &str,
    address: Option<SocketAddr>,
    name: ServerName<'static>,
    tls: &TlsConnector,
    control: &SessionControl,
) -> Result<(Stream, SocketRegistration)> {
    let (socket, registration) = open_socket(endpoint, address, control).await?;
    let stream = tls
        .connect(name, socket)
        .await
        .map_err(network_io)
        .context("relay TLS handshake")?;
    Ok((stream, registration))
}

pub(super) async fn open_socket(
    endpoint: &str,
    address: Option<SocketAddr>,
    control: &SessionControl,
) -> Result<(TcpStream, SocketRegistration)> {
    // Канал данных приходит на тот же адрес, даже если DNS вернул несколько адресов.
    let socket = match address {
        Some(address) => TcpStream::connect(address).await,
        None => TcpStream::connect(endpoint).await,
    }
    .map_err(network_io)
    .context("connect to relay")?;
    socket.set_nodelay(true)?;
    let socket = socket.into_std()?;
    let registration = control.register(&socket)?;
    Ok((TcpStream::from_std(socket)?, registration))
}

pub(super) async fn within_handshake<T>(
    control: &SessionControl,
    connect: impl Future<Output = Result<T>>,
) -> Result<T> {
    let cancel = async {
        while !control.is_cancelled() {
            sleep(Duration::from_millis(50)).await;
        }
        control.check()?;
        unreachable!()
    };
    tokio::select! {
        result = timeout(HANDSHAKE, connect) => result.map_err(|_| timed_out("connecting to relay"))?,
        result = cancel => result,
    }
}

pub(super) struct ClientRuntime(Option<Runtime>);

impl ClientRuntime {
    pub(super) fn create() -> Result<Arc<Self>> {
        Ok(Arc::new(Self(Some(
            Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(1)
                .enable_all()
                .build()?,
        ))))
    }

    pub(super) fn handle(&self) -> &Handle {
        self.0.as_ref().unwrap().handle()
    }
}

impl Drop for ClientRuntime {
    fn drop(&mut self) {
        // Отмена сетевого ожидания не должна ждать системный DNS resolver.
        self.0.take().unwrap().shutdown_background();
    }
}

struct Channel {
    reader: Mutex<ReadHalf<TransferStream>>,
    writer: Mutex<WriteHalf<TransferStream>>,
    stop: watch::Sender<bool>,
    runtime: Arc<ClientRuntime>,
}

impl Channel {
    fn close(&self) -> io::Result<()> {
        if !self
            .stop
            .send_if_modified(|closed| !std::mem::replace(closed, true))
        {
            return Ok(());
        }
        // Будим блокирующее чтение сразу. Закрытие TLS остаётся ограниченным по времени.
        self.runtime.handle().block_on(async {
            timeout(Duration::from_millis(250), async {
                self.writer.lock().await.shutdown().await
            })
            .await
            .unwrap_or_else(|_| Err(io::Error::from(io::ErrorKind::TimedOut)))
        })
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[derive(Clone)]
pub(crate) struct RelayStream {
    channel: Arc<Channel>,
    deadline: Option<Instant>,
}

impl RelayStream {
    fn new(stream: TransferStream, runtime: Arc<ClientRuntime>, deadline: Instant) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            channel: Arc::new(Channel {
                reader: Mutex::new(reader),
                writer: Mutex::new(writer),
                stop: watch::channel(false).0,
                runtime,
            }),
            deadline: Some(deadline),
        }
    }

    fn blocking<T>(&self, operation: impl Future<Output = io::Result<T>>) -> io::Result<T> {
        let mut stopped = self.channel.stop.subscribe();
        self.channel.runtime.handle().block_on(async {
            tokio::select! {
                biased;
                _ = stopped.wait_for(|closed| *closed) => Err(io::Error::from(io::ErrorKind::NotConnected)),
                result = async {
                    match self.deadline {
                        Some(deadline) => timeout_at(deadline, operation).await
                            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
                        None => operation.await,
                    }
                } => result,
            }
        })
    }
}

impl Read for RelayStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.blocking(async { self.channel.reader.lock().await.read(buffer).await })
    }
}

impl Write for RelayStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.blocking(async {
            let mut writer = self.channel.writer.lock().await;
            let count = writer.write(buffer).await?;
            // Для v4 запись в сокет видна peer без отдельного flush.
            writer.flush().await?;
            Ok(count)
        })
    }

    fn write_all(&mut self, buffer: &[u8]) -> io::Result<()> {
        self.blocking(async {
            let mut writer = self.channel.writer.lock().await;
            writer.write_all(buffer).await?;
            writer.flush().await
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.blocking(async { self.channel.writer.lock().await.flush().await })
    }
}

impl CloneableStream for RelayStream {
    fn try_clone_stream(&self) -> Result<Self> {
        Ok(self.clone())
    }

    fn shutdown_stream(&self) -> Result<()> {
        self.channel.close().context("close relay channel")
    }
}
