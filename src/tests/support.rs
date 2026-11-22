use crate::network::protocol::wire::{
    self, BundleCommit, BundlePlan, Message, SkipAck, SkipEntries,
};
use crate::network::protocol::{BundleCommitDescriptor, BundlePlanDescriptor};
use crate::network::transport::{BoundListener, connect_once};
use crate::receiver::{self, ReceiveReport};
use crate::role::Role;
use crate::sender;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tar::EntryType;

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArchiveEntrySummary {
    pub(crate) path: String,
    pub(crate) entry_type: EntryType,
    pub(crate) bytes: Vec<u8>,
    pub(crate) link_name: Option<String>,
}

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
            "fast-dump-v4-tests-{}-{}-{}",
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

pub(crate) fn read_archive_entries(path: &Path) -> Vec<ArchiveEntrySummary> {
    let file = fs::File::open(path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();

    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let entry_type = entry.header().entry_type();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let link_name = entry
            .link_name()
            .unwrap()
            .map(|link| link.to_string_lossy().into_owned());
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut bytes).unwrap();
        entries.push(ArchiveEntrySummary {
            path,
            entry_type,
            bytes,
            link_name,
        });
    }

    // tar остановился после первого нулевого блока; дочитываем второй и EOF
    // всех zstd frames, чтобы проверки содержимого не скрывали оборванный footer.
    let mut decoder = archive.into_inner();
    let mut tail = [0_u8; 8192];
    let mut tail_bytes = 0_usize;
    loop {
        let count = std::io::Read::read(&mut decoder, &mut tail).unwrap();
        if count == 0 {
            break;
        }
        assert!(
            tail[..count].iter().all(|byte| *byte == 0),
            "nonzero data after tar termination"
        );
        tail_bytes += count;
    }
    assert!(
        tail_bytes >= 512,
        "archive is missing its final tar zero block"
    );
    entries
}

pub(super) fn spawn_v4_receiver_once(
    data_dir: &Path,
) -> (
    std::net::SocketAddr,
    thread::JoinHandle<anyhow::Result<ReceiveReport>>,
) {
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let receiver_path = data_dir.to_path_buf();
    let handle = thread::spawn(move || {
        let session = listener.accept(Role::Receiver, None, None).unwrap();
        let peer_dump_id = session.peer_dump_id.clone();
        receiver::receive_dump(
            session.control_stream,
            session.data_stream,
            receiver_path,
            peer_dump_id.as_deref().unwrap(),
        )
    });
    (addr, handle)
}

pub(super) fn send_first_bundle_then_disconnect(
    addr: std::net::SocketAddr,
    source_dir: &Path,
    dump_id: &str,
    options: sender::BundlePlanOptions,
    send_terminal_skips: bool,
) {
    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    send_first_bundle(
        &mut sender_session,
        source_dir,
        dump_id,
        options,
        send_terminal_skips,
    );
}

pub(crate) fn send_first_bundle<T: std::io::Read + Write>(
    sender_session: &mut crate::network::transport::Session<T>,
    source_dir: &Path,
    dump_id: &str,
    options: sender::BundlePlanOptions,
    send_terminal_skips: bool,
) {
    let resume_state =
        wire::request_resume_state(&mut sender_session.control_stream, dump_id).unwrap();
    let prepared = sender::prepare_dump(source_dir, &resume_state, options).unwrap();
    assert!(prepared.bundles.len() >= 2);

    if send_terminal_skips {
        wire::write_message(
            &mut sender_session.control_stream,
            &Message::SkipEntries(SkipEntries {
                entries: prepared.new_terminal_skips.clone(),
            }),
        )
        .unwrap();
        sender_session.control_stream.flush().unwrap();
        let reply = wire::read_message(&mut sender_session.control_stream).unwrap();
        assert!(
            matches!(reply, Message::SkipAck(SkipAck { .. })),
            "reply={reply:?}"
        );
    }

    let bundle = &prepared.bundles[0];
    wire::write_message(
        &mut sender_session.control_stream,
        &Message::BundlePlan(BundlePlan {
            attempt_no: 1,
            descriptor: BundlePlanDescriptor::from_bundle_descriptor(&bundle.descriptor),
        }),
    )
    .unwrap();
    sender_session.control_stream.flush().unwrap();
    let mut chunks = bundle
        .compressed_payload
        .chunks(wire::MAX_DATA_CHUNK_BYTES)
        .peekable();
    while let Some(chunk) = chunks.next() {
        wire::write_bundle_data_message(
            &mut sender_session.data_stream,
            bundle.descriptor.bundle_no,
            1,
            chunks.peek().is_none(),
            chunk,
        )
        .unwrap();
    }
    sender_session.data_stream.flush().unwrap();
    wire::write_message(
        &mut sender_session.control_stream,
        &Message::BundleCommit(BundleCommit {
            attempt_no: 1,
            descriptor: BundleCommitDescriptor::from_bundle_descriptor(&bundle.descriptor),
        }),
    )
    .unwrap();
    sender_session.control_stream.flush().unwrap();
    let reply = wire::read_message(&mut sender_session.control_stream).unwrap();
    assert!(
        matches!(reply, Message::BundleAck(_)),
        "expected bundle ack, got {reply:?}"
    );
}

pub(super) fn send_bundles_then_disconnect_without_reading_acks(
    addr: std::net::SocketAddr,
    source_dir: &Path,
    dump_id: &str,
    options: sender::BundlePlanOptions,
    bundles_to_send: usize,
) {
    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let resume_state =
        wire::request_resume_state(&mut sender_session.control_stream, dump_id).unwrap();
    let prepared = sender::prepare_dump(source_dir, &resume_state, options).unwrap();
    assert!(prepared.bundles.len() > bundles_to_send);

    for bundle in prepared.bundles.iter().take(bundles_to_send) {
        wire::write_message(
            &mut sender_session.control_stream,
            &Message::BundlePlan(BundlePlan {
                attempt_no: 1,
                descriptor: BundlePlanDescriptor::from_bundle_descriptor(&bundle.descriptor),
            }),
        )
        .unwrap();
        sender_session.control_stream.flush().unwrap();
        let mut chunks = bundle
            .compressed_payload
            .chunks(wire::MAX_DATA_CHUNK_BYTES)
            .peekable();
        while let Some(chunk) = chunks.next() {
            wire::write_bundle_data_message(
                &mut sender_session.data_stream,
                bundle.descriptor.bundle_no,
                1,
                chunks.peek().is_none(),
                chunk,
            )
            .unwrap();
        }
        sender_session.data_stream.flush().unwrap();
        wire::write_message(
            &mut sender_session.control_stream,
            &Message::BundleCommit(BundleCommit {
                attempt_no: 1,
                descriptor: BundleCommitDescriptor::from_bundle_descriptor(&bundle.descriptor),
            }),
        )
        .unwrap();
    }
    sender_session.control_stream.flush().unwrap();
    thread::sleep(Duration::from_millis(75));
    drop(sender_session);
}

#[cfg(unix)]
pub(super) fn maybe_create_symlink(target: impl AsRef<Path>, link: &Path) {
    std::os::unix::fs::symlink(target.as_ref(), link).unwrap();
}

#[cfg(windows)]
pub(super) fn maybe_create_symlink(target: impl AsRef<Path>, link: &Path) {
    let _ = std::os::windows::fs::symlink_file(target.as_ref(), link);
}

#[cfg(unix)]
pub(super) fn symlink_supported() -> bool {
    true
}

#[cfg(windows)]
pub(super) fn symlink_supported() -> bool {
    false
}

pub(super) fn rename_canonical_receiver_state_to_legacy(data_dir: &Path, dump_id: &str) {
    rename_if_exists(
        &data_dir.join(format!("{dump_id}.tar.zst.part")),
        &data_dir.join(format!("{dump_id}.v4.tar.zst.part")),
    );
    rename_if_exists(
        &data_dir.join(format!("{dump_id}.tar.zst")),
        &data_dir.join(format!("{dump_id}.v4.tar.zst")),
    );
    rename_if_exists(
        &data_dir.join(format!("{dump_id}.wal.part")),
        &data_dir.join(format!("{dump_id}.v4.wal.part")),
    );
    rename_if_exists(
        &data_dir.join(format!("{dump_id}.catalog.part")),
        &data_dir.join(format!("{dump_id}.v4.catalog.part")),
    );
    rename_if_exists(
        &data_dir.join(format!("{dump_id}.tar.zst.issues.tsv.part")),
        &data_dir.join(format!("{dump_id}.v4.tar.zst.issues.tsv.part")),
    );
    rename_if_exists(
        &data_dir.join(format!("{dump_id}.tar.zst.issues.tsv")),
        &data_dir.join(format!("{dump_id}.v4.tar.zst.issues.tsv")),
    );
}

pub(super) fn rename_if_exists(from: &Path, to: &Path) {
    if from.exists() {
        fs::rename(from, to).unwrap();
    }
}
