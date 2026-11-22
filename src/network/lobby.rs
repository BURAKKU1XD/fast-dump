use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) type PeerId = u32;
pub(crate) const BODY_LEN: usize = 60;
const FRAME_LEN: usize = 5 + BODY_LEN;

pub(crate) const INCOMING: u8 = 15;
pub(crate) const DECLINED: u8 = 18;
pub(crate) const BUSY: u8 = 19;
pub(crate) const OFFER: u8 = 20;
pub(crate) const ACCEPT: u8 = 21;
pub(crate) const DECLINE: u8 = 22;
pub(crate) const RELEASED: u8 = 23;
pub(crate) const FAILED: u8 = 24;
pub(crate) const PEER_NAME: u8 = 25;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Peer {
    pub(crate) id: PeerId,
    pub(crate) name: String,
}

pub(crate) enum Event<T> {
    Incoming { peer: PeerId, source: String },
    Start(T),
    Declined(PeerId),
    Busy(PeerId),
    Released(PeerId),
    Failed(PeerId),
}

impl<T> Event<T> {
    pub(crate) fn map_ticket<U>(self, map: impl FnOnce(T) -> U) -> Event<U> {
        match self {
            Self::Incoming { peer, source } => Event::Incoming { peer, source },
            Self::Start(ticket) => Event::Start(map(ticket)),
            Self::Declined(peer) => Event::Declined(peer),
            Self::Busy(peer) => Event::Busy(peer),
            Self::Released(peer) => Event::Released(peer),
            Self::Failed(peer) => Event::Failed(peer),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Frame {
    pub(crate) kind: u8,
    pub(crate) id: PeerId,
    pub(crate) body: [u8; BODY_LEN],
}

impl Frame {
    pub(crate) fn new(kind: u8, id: PeerId) -> Self {
        Self {
            kind,
            id,
            body: [0; BODY_LEN],
        }
    }

    pub(crate) fn with_text(kind: u8, id: PeerId, text: &str) -> Self {
        let mut frame = Self::new(kind, id);
        let mut written = 0;
        for ch in text.chars().filter(|ch| !ch.is_control()) {
            let end = written + ch.len_utf8();
            if end > BODY_LEN {
                break;
            }
            ch.encode_utf8(&mut frame.body[written..end]);
            written = end;
        }
        frame
    }

    pub(crate) fn text(&self) -> String {
        let end = self
            .body
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(BODY_LEN);
        String::from_utf8_lossy(&self.body[..end])
            .chars()
            .filter(|ch| !ch.is_control())
            .collect()
    }

    pub(crate) async fn read(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Self> {
        let mut bytes = [0; FRAME_LEN];
        stream.read_exact(&mut bytes).await?;
        Ok(Self {
            kind: bytes[0],
            id: u32::from_be_bytes(bytes[1..5].try_into().unwrap()),
            body: bytes[5..].try_into().unwrap(),
        })
    }

    pub(crate) async fn write(self, stream: &mut (impl AsyncWrite + Unpin)) -> io::Result<()> {
        let mut bytes = [0; FRAME_LEN];
        bytes[0] = self.kind;
        bytes[1..5].copy_from_slice(&self.id.to_be_bytes());
        bytes[5..].copy_from_slice(&self.body);
        stream.write_all(&bytes).await?;
        stream.flush().await
    }
}

#[cfg(windows)]
pub(crate) fn hostname() -> String {
    use windows_sys::Win32::System::WindowsProgramming::GetComputerNameW;

    let mut buffer = [0u16; 256];
    let mut len = buffer.len() as u32;
    // Системный вызов записывает имя и его длину в буфер текущего процесса.
    if unsafe { GetComputerNameW(buffer.as_mut_ptr(), &mut len) } != 0 {
        String::from_utf16_lossy(&buffer[..len as usize])
    } else {
        String::new()
    }
}

#[cfg(unix)]
pub(crate) fn hostname() -> String {
    let mut buffer = [0u8; 256];
    // gethostname получает вместимость буфера; нулевой хвост ищем после вызова.
    if unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) } == 0 {
        let end = buffer
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(buffer.len());
        String::from_utf8_lossy(&buffer[..end]).into_owned()
    } else {
        String::new()
    }
}
