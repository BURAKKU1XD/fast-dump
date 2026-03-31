use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use std::{
    fs,
    io::{Read, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};
use fast_dump::network::protocol::{
    FrameChunk, FrameEnd, FrameStart, Message, ResumeState, write_message,
};
use fast_dump::network::transport::{BoundListener, Session, TransportMode, establish_session};
use fast_dump::receiver::{ReceiveReport, receive_dump};
use fast_dump::role::Role;
use fast_dump::sender::{PreparedFrame, SendOptions};
use xxhash_rust::xxh3::Xxh3Default;

// Параллельные интеграционные тесты могут стартовать в один и тот же тик системных часов,
// поэтому к timestamp добавляем процесс-локальный счётчик и исключаем коллизии temp-директорий.
static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

pub(crate) struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub(crate) fn new() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fast-dump-connectivity-tests-{}-{}-{}",
            std::process::id(),
            unique,
            counter
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

pub(crate) fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_fast-dump")
}

pub(crate) fn reserve_loopback_addr() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    addr
}

pub(crate) fn connect_with_retry(
    role: Role,
    addr: SocketAddr,
    dump_id: Option<&str>,
    token: Option<&str>,
) -> Result<Session> {
    let mut last_error = None;

    for _ in 0..40 {
        match establish_session(role, TransportMode::Connect(addr), dump_id, token) {
            Ok(session) => return Ok(session),
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    match last_error {
        Some(error) => Err(error),
        None => bail!("connect retry loop finished without an attempt"),
    }
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

pub(crate) fn sample_archive_entries() -> Vec<(String, Vec<u8>)> {
    vec![
        ("alpha.txt".to_owned(), b"alpha".to_vec()),
        ("nested/beta.txt".to_owned(), b"beta".to_vec()),
    ]
}

pub(crate) fn wait_for_path_with_timeout(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }

    panic!("timed out waiting for {}", path.display());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileHash {
    pub(crate) size: u64,
    pub(crate) xxh3_64: u64,
}

struct DeterministicBytes {
    state: u64,
}

impl DeterministicBytes {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        // Детерминированный генератор сохраняет воспроизводимость больших файлов
        // без зависимости от внешнего RNG между разными запусками тестов.
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn fill(&mut self, buffer: &mut [u8]) {
        for chunk in buffer.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

pub(crate) fn write_random_file(path: &Path, size: u64, seed: u64) -> FileHash {
    let mut file = fs::File::create(path).unwrap();
    let mut generator = DeterministicBytes::new(seed);
    let mut hasher = Xxh3Default::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut remaining = size;

    while remaining != 0 {
        let chunk_len = remaining.min(buffer.len() as u64) as usize;
        generator.fill(&mut buffer[..chunk_len]);
        file.write_all(&buffer[..chunk_len]).unwrap();
        hasher.update(&buffer[..chunk_len]);
        remaining -= chunk_len as u64;
    }

    file.sync_all().unwrap();
    FileHash {
        size,
        xxh3_64: hasher.digest(),
    }
}

fn hash_file(path: &Path) -> FileHash {
    let mut file = fs::File::open(path).unwrap();
    let mut hasher = Xxh3Default::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut size = 0_u64;

    loop {
        let read = file.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += read as u64;
    }

    FileHash {
        size,
        xxh3_64: hasher.digest(),
    }
}

fn normalized_relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap()
        .components()
        .map(|component| match component {
            std::path::Component::Normal(value) => value.to_string_lossy().into_owned(),
            other => panic!("unexpected path component while hashing extracted archive: {other:?}"),
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn collect_file_hashes_recursive(root: &Path, dir: &Path, hashes: &mut BTreeMap<String, FileHash>) {
    let mut entries = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort();

    for path in entries {
        if path.is_dir() {
            collect_file_hashes_recursive(root, &path, hashes);
            continue;
        }
        if path.is_file() {
            hashes.insert(normalized_relative_path(root, &path), hash_file(&path));
        }
    }
}

pub(crate) fn collect_file_hashes(root: &Path) -> BTreeMap<String, FileHash> {
    let mut hashes = BTreeMap::new();
    collect_file_hashes_recursive(root, root, &mut hashes);
    hashes
}

pub(crate) fn unpack_archive(path: &Path, output_dir: &Path) {
    let file = fs::File::open(path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(output_dir).unwrap();
}

pub(crate) struct BinaryRoundtripResult {
    pub(crate) _receiver_dir: TestDir,
    pub(crate) archive_path: PathBuf,
    pub(crate) sender_output: Output,
    pub(crate) receiver_output: Output,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SenderBinaryOptions {
    pub(crate) level: i32,
    pub(crate) threads: u32,
    pub(crate) min_readers: usize,
    pub(crate) max_readers: usize,
    pub(crate) read_buffer_kb: usize,
    pub(crate) frame_target_mb: u64,
}

impl SenderBinaryOptions {
    pub(crate) const fn defaults(frame_target_mb: u64) -> Self {
        Self {
            level: 3,
            threads: 1,
            min_readers: 2,
            max_readers: 4,
            read_buffer_kb: 1024,
            frame_target_mb,
        }
    }
}

pub(crate) fn empty_resume_state() -> ResumeState {
    ResumeState {
        last_committed_frame: 0,
        last_committed_file_index: 0,
        last_committed_file_path: String::new(),
        archive_len: 0,
    }
}

pub(crate) fn default_send_options(frame_target_bytes: u64) -> SendOptions {
    SendOptions {
        level: 3,
        threads: 1,
        frame_target_bytes,
        min_readers: 2,
        max_readers: 4,
        read_buffer_bytes: 1024 * 1024,
    }
}

fn write_hashed_bytes(path: &Path, bytes: &[u8]) -> FileHash {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, bytes).unwrap();
    hash_file(path)
}

pub(crate) fn insert_expected_file(
    expected: &mut BTreeMap<String, FileHash>,
    root: &Path,
    rel_path: &str,
    bytes: &[u8],
) {
    let hash = write_hashed_bytes(&root.join(rel_path), bytes);
    expected.insert(rel_path.replace('\\', "/"), hash);
}

pub(crate) fn insert_expected_random_file(
    expected: &mut BTreeMap<String, FileHash>,
    root: &Path,
    rel_path: &str,
    size: u64,
    seed: u64,
) {
    let path = root.join(rel_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let hash = write_random_file(&path, size, seed);
    expected.insert(rel_path.replace('\\', "/"), hash);
}

pub(crate) fn assert_archive_hashes(archive_path: &Path, expected: &BTreeMap<String, FileHash>) {
    let unpacked_dir = TestDir::new();
    unpack_archive(archive_path, unpacked_dir.path());
    let actual_hashes = collect_file_hashes(unpacked_dir.path());
    assert_eq!(actual_hashes, *expected);
}

fn spawn_receiver_binary(addr: SocketAddr, data_dir: &Path, debug: bool) -> std::process::Child {
    let mut command = Command::new(binary_path());
    if debug {
        command.arg("--debug");
    }
    command
        .args([
            "receiver",
            "--listen",
            &addr.to_string(),
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--max-sessions",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn run_sender_binary(
    addr: SocketAddr,
    source: &Path,
    dump_id: &str,
    options: &SenderBinaryOptions,
    debug: bool,
) -> Output {
    let mut command = Command::new(binary_path());
    if debug {
        command.arg("--debug");
    }
    command
        .args([
            "sender",
            "--connect",
            &addr.to_string(),
            "--source",
            source.to_str().unwrap(),
            "--dump-id",
            dump_id,
            "--level",
            &options.level.to_string(),
            "--threads",
            &options.threads.to_string(),
            "--min-readers",
            &options.min_readers.to_string(),
            "--max-readers",
            &options.max_readers.to_string(),
            "--read-buffer-kb",
            &options.read_buffer_kb.to_string(),
            "--frame-target-mb",
            &options.frame_target_mb.to_string(),
        ])
        .output()
        .unwrap()
}

pub(crate) fn run_push_roundtrip_via_binaries_with_sender_options(
    source: &Path,
    dump_id: &str,
    sender_options: SenderBinaryOptions,
    debug: bool,
) -> BinaryRoundtripResult {
    let addr = reserve_loopback_addr();
    let receiver_dir = TestDir::new();
    let archive_path = receiver_dir.path().join(format!("{dump_id}.tar.zst"));
    let mut receiver = spawn_receiver_binary(addr, receiver_dir.path(), debug);

    thread::sleep(Duration::from_millis(250));
    let sender_output = run_sender_binary(addr, source, dump_id, &sender_options, debug);
    wait_for_path_with_timeout(&archive_path, Duration::from_secs(60));

    let _ = receiver.kill();
    let receiver_output = receiver.wait_with_output().unwrap();

    BinaryRoundtripResult {
        _receiver_dir: receiver_dir,
        archive_path,
        sender_output,
        receiver_output,
    }
}

pub(crate) fn run_push_roundtrip_via_binaries(
    source: &Path,
    dump_id: &str,
    frame_target_mb: u64,
    debug: bool,
) -> BinaryRoundtripResult {
    run_push_roundtrip_via_binaries_with_sender_options(
        source,
        dump_id,
        SenderBinaryOptions::defaults(frame_target_mb),
        debug,
    )
}

pub(crate) fn send_prepared_frame_without_waiting_for_ack<T>(stream: &mut T, frame: &PreparedFrame)
where
    T: Write,
{
    write_message(
        stream,
        &Message::FrameStart(FrameStart {
            frame_no: frame.outbound.frame_no,
            first_file_index: frame.outbound.first_file_index,
            first_file_path: frame.outbound.first_file_path.clone(),
            level: frame.outbound.level,
            threads: frame.outbound.threads,
        }),
    )
    .unwrap();

    let mut hasher = Xxh3Default::new();
    let mut compressed_len = 0_u64;
    for chunk in &frame.chunks {
        write_message(
            stream,
            &Message::FrameChunk(FrameChunk {
                bytes: chunk.clone(),
            }),
        )
        .unwrap();
        hasher.update(chunk);
        compressed_len += chunk.len() as u64;
    }

    write_message(
        stream,
        &Message::FrameEnd(FrameEnd {
            frame_no: frame.outbound.frame_no,
            last_file_index: frame.outbound.last_file_index,
            last_file_path: frame.outbound.last_file_path.clone(),
            compressed_len,
            compressed_xxh3_64: hasher.digest(),
        }),
    )
    .unwrap();
    stream.flush().unwrap();
}

pub(crate) fn send_partial_prepared_frame<T>(
    stream: &mut T,
    frame: &PreparedFrame,
    chunk_count: usize,
) where
    T: Write,
{
    assert!(chunk_count > 0, "chunk_count must be at least 1");
    assert!(
        chunk_count <= frame.chunks.len(),
        "chunk_count must not exceed prepared chunk count"
    );

    write_message(
        stream,
        &Message::FrameStart(FrameStart {
            frame_no: frame.outbound.frame_no,
            first_file_index: frame.outbound.first_file_index,
            first_file_path: frame.outbound.first_file_path.clone(),
            level: frame.outbound.level,
            threads: frame.outbound.threads,
        }),
    )
    .unwrap();

    for chunk in frame.chunks.iter().take(chunk_count) {
        write_message(
            stream,
            &Message::FrameChunk(FrameChunk {
                bytes: chunk.clone(),
            }),
        )
        .unwrap();
    }
    stream.flush().unwrap();
}

pub(crate) fn send_prepared_frame_with_bad_checksum<T>(stream: &mut T, frame: &PreparedFrame)
where
    T: Write,
{
    write_message(
        stream,
        &Message::FrameStart(FrameStart {
            frame_no: frame.outbound.frame_no,
            first_file_index: frame.outbound.first_file_index,
            first_file_path: frame.outbound.first_file_path.clone(),
            level: frame.outbound.level,
            threads: frame.outbound.threads,
        }),
    )
    .unwrap();

    let mut hasher = Xxh3Default::new();
    let mut compressed_len = 0_u64;
    for chunk in &frame.chunks {
        write_message(
            stream,
            &Message::FrameChunk(FrameChunk {
                bytes: chunk.clone(),
            }),
        )
        .unwrap();
        hasher.update(chunk);
        compressed_len += chunk.len() as u64;
    }

    write_message(
        stream,
        &Message::FrameEnd(FrameEnd {
            frame_no: frame.outbound.frame_no,
            last_file_index: frame.outbound.last_file_index,
            last_file_path: frame.outbound.last_file_path.clone(),
            compressed_len,
            compressed_xxh3_64: hasher.digest().wrapping_add(1),
        }),
    )
    .unwrap();
    stream.flush().unwrap();
}

pub(crate) fn spawn_receiver_dump_once(
    data_dir: &Path,
) -> (SocketAddr, thread::JoinHandle<Result<ReceiveReport>>) {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let receiver_path = data_dir.to_path_buf();
    let handle = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        receive_dump(&mut session.stream, receiver_path)
    });
    (addr, handle)
}
