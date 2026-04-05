use std::fs;
use std::io::{self, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::{self, FrameChunk, FrameEnd, FrameStart, Message};
use crate::sender::{PreparedFrame, SendOptions};

use super::shared::test_hooks;

pub(crate) struct TestDir {
    path: PathBuf,
}

pub(crate) struct FsFailureGuard {
    _lock: MutexGuard<'static, ()>,
}

pub(crate) struct ScriptedStream {
    read: Cursor<Vec<u8>>,
    pub(crate) written: Vec<u8>,
}

impl TestDir {
    pub(crate) fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fast-dump-receiver-tests-{}-{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl FsFailureGuard {
    pub(crate) fn new() -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        test_hooks::clear();
        Self { _lock: lock }
    }
}

impl Drop for FsFailureGuard {
    fn drop(&mut self) {
        test_hooks::clear();
    }
}

impl ScriptedStream {
    pub(crate) fn new(read_bytes: Vec<u8>) -> Self {
        Self {
            read: Cursor::new(read_bytes),
            written: Vec::new(),
        }
    }
}

impl Read for ScriptedStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read.read(buf)
    }
}

impl Write for ScriptedStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.written.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encode_message(message: &Message) -> Vec<u8> {
    let mut bytes = Vec::new();
    protocol::write_message(&mut bytes, message).unwrap();
    bytes
}

pub(crate) fn decode_reply(bytes: &[u8], request_len: usize) -> Message {
    protocol::read_message(&mut Cursor::new(bytes[request_len..].to_vec())).unwrap()
}

pub(crate) fn decode_messages(bytes: &[u8]) -> Vec<Message> {
    let mut cursor = Cursor::new(bytes.to_vec());
    let mut messages = Vec::new();
    while (cursor.position() as usize) < bytes.len() {
        messages.push(protocol::read_message(&mut cursor).unwrap());
    }
    messages
}

pub(crate) fn archive_entries(path: &Path) -> Vec<(String, Vec<u8>)> {
    let file = fs::File::open(path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        std::io::copy(&mut entry, &mut bytes).unwrap();
        entries.push((path, bytes));
    }
    entries
}

pub(crate) fn frame_end(
    frame_no: u64,
    last_file_index: u64,
    last_file_path: &str,
    chunks: &[&[u8]],
) -> FrameEnd {
    let mut hasher = Xxh3Default::new();
    let mut compressed_len = 0_u64;
    for chunk in chunks {
        hasher.update(chunk);
        compressed_len += chunk.len() as u64;
    }

    FrameEnd {
        frame_no,
        last_file_index,
        last_file_path: last_file_path.to_owned(),
        compressed_len,
        compressed_xxh3_64: hasher.digest(),
    }
}

pub(crate) fn encode_prepared_frame(frame: &PreparedFrame) -> Vec<u8> {
    let mut request = encode_message(&Message::FrameStart(FrameStart {
        frame_no: frame.outbound.frame_no,
        first_file_index: frame.outbound.first_file_index,
        first_file_path: frame.outbound.first_file_path.clone(),
        level: frame.outbound.level,
        threads: frame.outbound.threads,
    }));
    for chunk in &frame.chunks {
        request.extend_from_slice(&encode_message(&Message::FrameChunk(FrameChunk {
            bytes: chunk.clone(),
        })));
    }
    let chunk_refs: Vec<&[u8]> = frame.chunks.iter().map(Vec::as_slice).collect();
    request.extend_from_slice(&encode_message(&Message::FrameEnd(frame_end(
        frame.outbound.frame_no,
        frame.outbound.last_file_index,
        &frame.outbound.last_file_path,
        &chunk_refs,
    ))));
    request
}

pub(crate) fn sample_send_options() -> SendOptions {
    SendOptions {
        level: 3,
        threads: 1,
        frame_target_bytes: 1,
        min_readers: 2,
        max_readers: 4,
        read_buffer_bytes: 1024,
        manifest_chunk_bytes: 256 * 1024,
        manifest_ack_records: 4096,
        manifest_ack_bytes: 1024 * 1024,
    }
}
