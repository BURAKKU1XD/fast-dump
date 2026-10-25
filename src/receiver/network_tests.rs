use super::events::receive_next_event;
use super::readers::{DATA_BUFFER_COUNT, DataBuffer, READER_QUEUE_CAPACITY, spawn_data_reader};
use crate::network::protocol::wire::BundleAck;
use crate::network::protocol::{BundleCommitDescriptor, BundleDescriptor, BundlePlanDescriptor};
use crate::tests::TestDir;
use crossbeam_channel::bounded;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use super::*;
use crate::network::protocol::PreparedBundle;

#[test]
fn failed_error_reply_preserves_the_primary_protocol_failure() {
    struct ResetReply(Cursor<Vec<u8>>);

    impl Read for ResetReply {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            self.0.read(bytes)
        }
    }
    impl Write for ResetReply {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::ConnectionReset.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut input = Vec::new();
    wire::write_message(
        &mut input,
        &Message::ResumeQuery(ResumeQuery {
            dump_id: "wrong".into(),
        }),
    )
    .unwrap();
    let error = respond_resume_query(
        &mut ResetReply(Cursor::new(input)),
        "expected",
        ResumeState::default(),
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("resume query dump id mismatch"));
    assert!(!crate::network::is_retryable_error(&error));
}

struct TestDirectory(TestDir);

impl TestDirectory {
    fn new() -> Self {
        Self(TestDir::new())
    }

    fn open(&self) -> ReceiverSession {
        ReceiverSession::open(self.0.path().to_path_buf(), "boundaries", 1).unwrap()
    }

    fn archive_bytes(&self) -> Vec<u8> {
        fs::read(self.0.path().join("boundaries.tar.zst.part")).unwrap()
    }
}

#[derive(Default)]
struct PendingState {
    bundles: VecDeque<PendingBundle>,
    orphan_data: HashMap<AttemptKey, Vec<PendingDataChunk>>,
    aborted: HashSet<AttemptKey>,
}

impl PendingState {
    fn plan(&mut self, bundle: &PreparedBundle, attempt_no: u32) {
        self.bundles.push_back(PendingBundle {
            key: AttemptKey::new(bundle.descriptor.bundle_no, attempt_no),
            descriptor: BundlePlanDescriptor::from_bundle_descriptor(&bundle.descriptor),
            bytes_received: 0,
            archive_len_before_append: None,
            archive_len_after_append: None,
            checksum: Xxh3Default::new(),
            write_duration: Duration::ZERO,
            periodic_sync_duration: Duration::ZERO,
            periodic_sync_count: 0,
            commit: Some(BundleCommitDescriptor::from_bundle_descriptor(
                &bundle.descriptor,
            )),
            data_finished: false,
        });
    }

    fn data(
        &mut self,
        receiver: &mut ReceiverSession,
        bundle_no: u64,
        attempt_no: u32,
        bytes: &[u8],
        is_last: bool,
    ) -> Result<()> {
        apply_data_chunk(
            receiver,
            &mut self.bundles,
            &mut self.orphan_data,
            &self.aborted,
            PendingDataChunk {
                key: AttemptKey::new(bundle_no, attempt_no),
                is_last,
                bytes: bytes.to_vec().into(),
            },
        )
    }

    fn abort(&mut self, receiver: &mut ReceiverSession, bundle_no: u64, attempt_no: u32) {
        abort_bundle(
            receiver,
            &mut self.bundles,
            &mut self.orphan_data,
            &mut self.aborted,
            AttemptKey::new(bundle_no, attempt_no),
        )
        .unwrap();
    }

    fn commit(&mut self, receiver: &mut ReceiverSession) -> BundleAck {
        let mut output = Vec::new();
        drain_ready_commits(
            &mut output,
            receiver,
            &mut self.bundles,
            &mut ReceiverProfile::default(),
            false,
        )
        .unwrap();
        let Message::BundleAck(ack) = wire::read_message(&mut Cursor::new(output)).unwrap() else {
            panic!("expected bundle ack");
        };
        ack
    }
}

fn bundle(bundle_no: u64) -> PreparedBundle {
    let raw = vec![bundle_no as u8; 128 + bundle_no as usize];
    let payload = zstd::stream::encode_all(raw.as_slice(), 1).unwrap();
    PreparedBundle {
        descriptor: BundleDescriptor {
            bundle_no,
            first_work_ordinal: bundle_no,
            last_work_ordinal: bundle_no,
            last_relative_path: format!("{bundle_no}.bin"),
            entries: Vec::new(),
            raw_payload_bytes: raw.len() as u64,
            tar_payload_bytes: raw.len() as u64,
            compressed_payload_bytes: payload.len() as u64,
            compressed_xxh3_64: xxhash_rust::xxh3::xxh3_64(&payload),
        },
        compressed_payload: payload,
    }
}

#[test]
fn progress_tracks_written_payload_and_durable_entries_without_using_ordinals() {
    use crate::network::protocol::{BundleEntryDescriptor, BundleEntryKind};
    use crate::progress::ProgressHub;
    use crate::role::Role;
    use crate::runtime::RunControl;
    let directory = TestDirectory::new();
    let hub = std::sync::Arc::new(ProgressHub::new());
    let run = RunControl::new(Some(hub));
    let control = run.session(Role::Receiver, "boundaries", "peer");
    let progress = control.progress().unwrap();
    let mut receiver = ReceiverSession::open_controlled(
        directory.0.path().to_path_buf(),
        "boundaries",
        1,
        Some(&control),
    )
    .unwrap();
    let mut payload = bundle(1);
    payload.descriptor.first_work_ordinal = 3;
    payload.descriptor.last_work_ordinal = 9;
    payload.descriptor.entries = [3, 9]
        .into_iter()
        .map(|work_ordinal| BundleEntryDescriptor {
            work_ordinal,
            relative_path: format!("{work_ordinal}.bin"),
            kind: BundleEntryKind::Regular,
            size_at_open: 1,
            best_effort: false,
        })
        .collect();
    let mut pending = PendingState::default();
    pending.plan(&payload, 1);
    let split = payload.compressed_payload.len() / 2;
    pending
        .data(
            &mut receiver,
            1,
            1,
            &payload.compressed_payload[..split],
            false,
        )
        .unwrap();
    assert_eq!(
        progress.transferred_bytes.load(Ordering::Relaxed),
        split as u64
    );
    assert_eq!(progress.durable_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(progress.committed_entries.load(Ordering::Relaxed), 0);
    pending
        .data(
            &mut receiver,
            1,
            1,
            &payload.compressed_payload[split..],
            true,
        )
        .unwrap();
    assert_eq!(progress.sent_entries.load(Ordering::Relaxed), 2);
    assert_eq!(progress.committed_entries.load(Ordering::Relaxed), 0);
    pending.abort(&mut receiver, 1, 1);
    assert_eq!(progress.sent_entries.load(Ordering::Relaxed), 0);
    assert_eq!(progress.durable_bytes.load(Ordering::Relaxed), 0);
    pending.plan(&payload, 2);
    pending
        .data(&mut receiver, 1, 2, &payload.compressed_payload, true)
        .unwrap();
    assert_eq!(progress.sent_entries.load(Ordering::Relaxed), 2);
    assert_eq!(
        progress.transferred_bytes.load(Ordering::Relaxed),
        2 * payload.compressed_payload.len() as u64
    );
    let ack = pending.commit(&mut receiver);
    assert_eq!(
        progress.durable_bytes.load(Ordering::Relaxed),
        ack.archive_len
    );
    assert_eq!(progress.committed_entries.load(Ordering::Relaxed), 2);
    assert_eq!(progress.bundles.load(Ordering::Relaxed), 1);
    drop(receiver);
    let resumed = run.session(Role::Receiver, "boundaries", "peer");
    let _receiver = ReceiverSession::open_controlled(
        directory.0.path().to_path_buf(),
        "boundaries",
        1,
        Some(&resumed),
    )
    .unwrap();
    let resumed_progress = resumed.progress().unwrap();
    assert_eq!(
        resumed_progress.resumed_bytes.load(Ordering::Relaxed),
        ack.archive_len
    );
    assert_eq!(
        resumed_progress.committed_entries.load(Ordering::Relaxed),
        0
    );
}

#[test]
fn grouped_wal_preserves_exact_boundaries_with_speculative_suffix_and_torn_tail() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let bundles = (1..=5).map(bundle).collect::<Vec<_>>();
    for bundle in &bundles {
        state.plan(bundle, 1);
    }
    let mut expected_ends = Vec::new();
    let mut expected_bytes = Vec::new();
    for bundle in &bundles[..4] {
        state
            .data(
                &mut receiver,
                bundle.descriptor.bundle_no,
                1,
                &bundle.compressed_payload,
                true,
            )
            .unwrap();
        expected_bytes.extend_from_slice(&bundle.compressed_payload);
        expected_ends.push(expected_bytes.len() as u64);
    }
    state
        .data(
            &mut receiver,
            5,
            1,
            &bundles[4].compressed_payload[..5],
            false,
        )
        .unwrap();
    let ack = state.commit(&mut receiver);
    assert_eq!(ack.bundle_no, 4);
    assert_eq!(ack.archive_len, expected_ends[3]);
    assert_eq!(receiver.current_archive_len(), expected_ends[3] + 5);
    drop(receiver);

    let wal_path = directory.0.path().join("boundaries.wal.part");
    let wal = fs::read_to_string(&wal_path).unwrap();
    let recorded_ends = wal
        .lines()
        .skip(1)
        .map(|line| line.split('\t').nth(4).unwrap().parse::<u64>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(recorded_ends, expected_ends);

    let valid_prefix = wal.split_inclusive('\n').take(3).collect::<String>();
    fs::write(&wal_path, format!("{valid_prefix}commit\t3\t3\t3\t")).unwrap();
    let mut receiver = directory.open();
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 2);
    assert_eq!(
        directory.archive_bytes(),
        expected_bytes[..expected_ends[1] as usize]
    );
    for bundle in &bundles[2..4] {
        receiver.commit_bundle(bundle).unwrap();
    }
    drop(receiver);
    let receiver = directory.open();
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 4);
    assert_eq!(directory.archive_bytes(), expected_bytes);
    let raw = zstd::stream::decode_all(directory.archive_bytes().as_slice()).unwrap();
    assert_eq!(
        raw,
        (1..=4)
            .flat_map(|n| vec![n as u8; 128 + n])
            .collect::<Vec<_>>()
    );
}

#[test]
fn abort_after_data_preserves_prior_bundle_and_ignores_late_attempt_data() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let first = bundle(1);
    let second = bundle(2);
    state.plan(&first, 1);
    state.plan(&second, 1);
    state
        .data(&mut receiver, 1, 1, &first.compressed_payload, true)
        .unwrap();
    state
        .data(&mut receiver, 2, 1, &second.compressed_payload[..5], false)
        .unwrap();
    state.abort(&mut receiver, 2, 1);
    state
        .data(&mut receiver, 2, 1, &second.compressed_payload[5..], true)
        .unwrap();
    assert_eq!(
        state.commit(&mut receiver).archive_len,
        first.compressed_payload.len() as u64
    );
    assert_eq!(directory.archive_bytes(), first.compressed_payload);

    state.plan(&second, 2);
    state
        .data(&mut receiver, 2, 2, &second.compressed_payload, true)
        .unwrap();
    assert_eq!(state.commit(&mut receiver).bundle_no, 2);
    assert_eq!(
        directory.archive_bytes(),
        [first.compressed_payload, second.compressed_payload].concat()
    );
}

#[test]
fn abort_before_data_does_not_truncate_preceding_incomplete_bundle() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let first = bundle(1);
    let second = bundle(2);
    state.plan(&first, 1);
    state.plan(&second, 1);
    state
        .data(&mut receiver, 1, 1, &first.compressed_payload[..5], false)
        .unwrap();
    state.abort(&mut receiver, 2, 1);
    assert_eq!(directory.archive_bytes(), first.compressed_payload[..5]);
    state
        .data(&mut receiver, 1, 1, &first.compressed_payload[5..], true)
        .unwrap();
    state
        .data(&mut receiver, 2, 1, &second.compressed_payload, true)
        .unwrap();
    assert_eq!(state.commit(&mut receiver).bundle_no, 1);
    assert_eq!(directory.archive_bytes(), first.compressed_payload);
}

#[test]
fn recovery_ignores_parseable_record_without_newline() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    receiver.commit_bundle(&bundle(2)).unwrap();
    drop(receiver);
    let path = directory.0.path().join("boundaries.wal.part");
    let mut bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.pop(), Some(b'\n'));
    fs::write(path, bytes).unwrap();
    assert_eq!(directory.open().resume_state().last_committed_bundle_no, 1);
}

#[test]
fn completed_archive_with_stale_wal_is_preserved() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    drop(receiver);
    let part = directory.0.path().join("boundaries.tar.zst.part");
    let final_path = directory.0.path().join("boundaries.tar.zst");
    let expected = fs::read(&part).unwrap();
    fs::rename(&part, &final_path).unwrap();
    assert!(ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1).is_err());
    assert_eq!(fs::read(final_path).unwrap(), expected);
    assert!(!part.exists());
}

#[test]
fn completed_archive_and_partial_state_are_both_preserved_on_reopen() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    receiver.stage_bundle(&bundle(2)).unwrap();
    drop(receiver);
    let expected_part = directory.archive_bytes();
    let wal_path = directory.0.path().join("boundaries.wal.part");
    let expected_wal = fs::read(&wal_path).unwrap();
    let final_path = directory.0.path().join("boundaries.tar.zst");
    fs::write(&final_path, b"previous completed archive").unwrap();

    assert!(ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1).is_err());
    assert_eq!(fs::read(final_path).unwrap(), b"previous completed archive");
    assert_eq!(directory.archive_bytes(), expected_part);
    assert_eq!(fs::read(wal_path).unwrap(), expected_wal);
}

#[test]
fn finalize_preserves_existing_archive_and_resume_state() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    let final_path = directory.0.path().join("boundaries.tar.zst");
    // Имитируем посторонний готовый файл, появившийся после открытия сессии.
    fs::write(&final_path, b"another completed archive").unwrap();
    let wal_path = directory.0.path().join("boundaries.wal.part");
    let expected_wal = fs::read(&wal_path).unwrap();

    assert!(receiver.finalize().is_err());
    assert_eq!(fs::read(final_path).unwrap(), b"another completed archive");
    assert_eq!(fs::read(wal_path).unwrap(), expected_wal);
    assert!(
        directory
            .archive_bytes()
            .starts_with(&bundle(1).compressed_payload)
    );
}

#[test]
fn complete_corrupt_wal_record_fails_without_truncating_archive_or_wal() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    receiver.commit_bundle(&bundle(2)).unwrap();
    drop(receiver);
    let expected_archive = directory.archive_bytes();
    let wal_path = directory.0.path().join("boundaries.wal.part");
    let complete_wal = fs::read(&wal_path).unwrap();
    let prefix_end = complete_wal
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(1)
        .unwrap()
        .0
        + 1;
    for invalid in [
        b"commit\t2\tinvalid\n".as_slice(),
        b"\xff\n".as_slice(),
        b"\n".as_slice(),
        b"  \t  \n".as_slice(),
    ] {
        let mut corrupt = complete_wal[..prefix_end].to_vec();
        corrupt.extend_from_slice(invalid);
        // Ошибка в середине тоже не должна скрывать дальнейшие записи.
        corrupt.extend_from_slice(&complete_wal[prefix_end..]);
        fs::write(&wal_path, &corrupt).unwrap();
        let error = ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1)
            .err()
            .expect("complete corrupt WAL must fail recovery");
        assert!(format!("{error:#}").contains("complete v4 wal record"));
        assert_eq!(directory.archive_bytes(), expected_archive);
        assert_eq!(fs::read(&wal_path).unwrap(), corrupt);
    }
    // Ошибочный open освобождает lock; после восстановления WAL можно продолжить.
    fs::write(wal_path, complete_wal).unwrap();
    assert_eq!(directory.open().resume_state().last_committed_bundle_no, 2);
}

#[test]
fn numeric_wal_corruption_is_rejected_but_skipped_bundle_numbers_resume() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    // Bundle 2 мог завершиться terminal skip: номера commit не обязаны идти подряд.
    receiver.commit_bundle(&bundle(3)).unwrap();
    drop(receiver);
    let expected_archive = directory.archive_bytes();
    let wal_path = directory.0.path().join("boundaries.wal.part");
    let valid = fs::read_to_string(&wal_path).unwrap();
    let mut lines = valid.lines().map(str::to_owned).collect::<Vec<_>>();
    let original_columns = lines[2].split('\t').map(str::to_owned).collect::<Vec<_>>();
    for (column, invalid) in [
        (1, "1"),
        (2, "1"),
        (3, "2"),
        (4, "1"),
        (5, "0"),
        (5, "18446744073709551615"),
    ] {
        let mut columns = original_columns.clone();
        columns[column] = invalid.to_owned();
        lines[2] = columns.join("\t");
        let corrupt = format!("{}\n", lines.join("\n"));
        fs::write(&wal_path, &corrupt).unwrap();
        let error = ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1)
            .err()
            .expect("inconsistent WAL must fail recovery");
        assert!(format!("{error:#}").contains("inconsistent complete v4 wal record"));
        assert_eq!(directory.archive_bytes(), expected_archive);
        assert_eq!(fs::read_to_string(&wal_path).unwrap(), corrupt);
    }
    fs::write(wal_path, valid).unwrap();
    let receiver = directory.open();
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 3);
    assert_eq!(receiver.resume_state().last_committed_work_ordinal, 3);
    assert_eq!(directory.archive_bytes(), expected_archive);
}

#[test]
fn dump_lock_child_probe() {
    let Some(path) = std::env::var_os("FAST_DUMP_TEST_RECEIVER_LOCK_DIR") else {
        return;
    };
    let result = ReceiverSession::open(PathBuf::from(path), "boundaries", 1);
    match std::env::var("FAST_DUMP_TEST_RECEIVER_LOCK_EXPECT")
        .unwrap()
        .as_str()
    {
        "busy" => {
            let error = result.err().expect("another process owns the dump lock");
            assert!(
                error
                    .to_string()
                    .contains("already active or cannot be locked")
            );
        }
        "available" => assert_eq!(result.unwrap().resume_state().last_committed_bundle_no, 1),
        unexpected => panic!("unexpected lock probe mode: {unexpected}"),
    }
}

#[test]
fn dump_lock_prevents_cross_process_recovery_until_owner_closes() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    receiver.stage_bundle(&bundle(2)).unwrap();
    let expected_archive = directory.archive_bytes();
    let wal_path = directory.0.path().join("boundaries.wal.part");
    let expected_wal = fs::read(&wal_path).unwrap();
    let probe = |mode: &str| {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "receiver::network::tests::dump_lock_child_probe",
                "--nocapture",
            ])
            .env("FAST_DUMP_TEST_RECEIVER_LOCK_DIR", directory.0.path())
            .env("FAST_DUMP_TEST_RECEIVER_LOCK_EXPECT", mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child probe failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    };
    probe("busy");
    assert!(ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1).is_err());
    assert_eq!(directory.archive_bytes(), expected_archive);
    assert_eq!(fs::read(&wal_path).unwrap(), expected_wal);

    drop(receiver);
    assert!(directory.0.path().join("boundaries.lock").exists());
    probe("available");
    assert_eq!(directory.archive_bytes(), bundle(1).compressed_payload);
    assert_eq!(fs::read(wal_path).unwrap(), expected_wal);
    assert!(directory.0.path().join("boundaries.lock").exists());
}

#[test]
fn invalid_dump_id_is_rejected_before_creating_receiver_directory() {
    let directory = TestDirectory::new();
    let data_dir = directory.0.path().join("not-created");
    for dump_id in ["../escaped", "nested/dump", "nested\\dump"] {
        assert!(ReceiverSession::open(data_dir.clone(), dump_id, 1).is_err());
    }
    assert!(!data_dir.exists());
}

#[test]
fn legacy_migration_never_takes_files_from_active_or_closed_v4_suffix_dump() {
    let directory = TestDirectory::new();
    let mut suffixed =
        ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries.v4", 1).unwrap();
    suffixed.commit_bundle(&bundle(1)).unwrap();
    suffixed.stage_bundle(&bundle(2)).unwrap();
    let suffixed_part = directory.0.path().join("boundaries.v4.tar.zst.part");
    let suffixed_wal = directory.0.path().join("boundaries.v4.wal.part");
    let expected_part = fs::read(&suffixed_part).unwrap();
    let expected_wal = fs::read(&suffixed_wal).unwrap();

    // Оба transfer могут работать одновременно после короткого serialized open.
    let mut base = directory.open();
    assert_eq!(base.resume_state().last_committed_bundle_no, 0);
    assert_eq!(fs::read(&suffixed_part).unwrap(), expected_part);
    assert_eq!(fs::read(&suffixed_wal).unwrap(), expected_wal);
    base.commit_bundle(&bundle(3)).unwrap();
    drop(base);
    drop(suffixed);

    // Marker сохраняет смысл имён и после закрытия владельца/рестарта receiver.
    let base = directory.open();
    assert_eq!(base.resume_state().last_committed_bundle_no, 3);
    assert_eq!(fs::read(&suffixed_part).unwrap(), expected_part);
    assert_eq!(fs::read(&suffixed_wal).unwrap(), expected_wal);
    let suffixed =
        ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries.v4", 1).unwrap();
    assert_eq!(suffixed.resume_state().last_committed_bundle_no, 1);
    assert_eq!(
        fs::read(suffixed_part).unwrap(),
        bundle(1).compressed_payload
    );
    assert_eq!(directory.archive_bytes(), bundle(3).compressed_payload);
}

#[test]
fn corrupt_skip_catalog_blocks_recovery_without_truncating_speculative_bytes() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    receiver.stage_bundle(&bundle(2)).unwrap();
    drop(receiver);
    let expected_archive = directory.archive_bytes();
    let path = directory.0.path().join("boundaries.catalog.part");
    for corrupt in [
        "",
        "fast-dump-v4-catalog-1",
        "fast-dump-v4-catalog-1\nskip\t3\topen_failed\tx",
    ] {
        fs::write(&path, corrupt).unwrap();
        assert!(ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1).is_err());
        assert_eq!(directory.archive_bytes(), expected_archive);
        assert_eq!(fs::read_to_string(&path).unwrap(), corrupt);
    }
}

#[test]
fn recovery_does_not_extend_short_archive_to_wal_boundary() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    drop(receiver);
    let path = directory.0.path().join("boundaries.tar.zst.part");
    let bytes = directory.archive_bytes();
    fs::write(&path, &bytes[..5]).unwrap();
    assert!(ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1).is_err());
    assert_eq!(directory.archive_bytes(), bytes[..5]);
    fs::remove_file(&path).unwrap();
    assert!(ReceiverSession::open(directory.0.path().to_path_buf(), "boundaries", 1).is_err());
    assert!(!path.exists());
}

#[test]
fn orphan_payload_applies_backpressure_until_its_control_plan_arrives() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let bundle = bundle(1);
    state
        .data(&mut receiver, 1, 1, &bundle.compressed_payload[..5], false)
        .unwrap();
    assert_eq!(state.orphan_data.len(), 1);
    assert_eq!(receiver.current_archive_len(), 0);

    let (control_tx, control_rx) = bounded(1);
    let (data_tx, data_rx) = bounded(1);
    control_tx
        .send(Ok(Message::BundlePlan(wire::BundlePlan {
            attempt_no: 1,
            descriptor: BundlePlanDescriptor::from_bundle_descriptor(&bundle.descriptor),
        })))
        .unwrap();
    data_tx
        .send(Ok(PendingDataChunk {
            key: AttemptKey::new(1, 1),
            is_last: true,
            bytes: bundle.compressed_payload[5..].to_vec().into(),
        }))
        .unwrap();

    // Оба канала готовы: orphan требует обработать именно план, не второй payload.
    let ReceiveEvent::Control(Message::BundlePlan(plan)) = receive_next_event(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        !state.orphan_data.is_empty(),
    )
    .unwrap() else {
        panic!("receiver drained data while waiting for its plan");
    };
    assert_eq!(data_rx.len(), 1);
    state.plan(&bundle, plan.attempt_no);
    apply_orphan_data(
        &mut receiver,
        &mut state.bundles,
        &mut state.orphan_data,
        &state.aborted,
        AttemptKey::new(plan.descriptor.bundle_no, plan.attempt_no),
    )
    .unwrap();
    assert!(state.orphan_data.is_empty());
    assert_eq!(receiver.current_archive_len(), 5);

    let ReceiveEvent::Data(data) = receive_next_event(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        !state.orphan_data.is_empty(),
    )
    .unwrap() else {
        panic!("receiver did not resume reading bundle data");
    };
    state
        .data(
            &mut receiver,
            data.key.bundle_no,
            data.key.attempt_no,
            &data.bytes,
            data.is_last,
        )
        .unwrap();
    assert_eq!(state.commit(&mut receiver).bundle_no, 1);
    assert_eq!(directory.archive_bytes(), bundle.compressed_payload);
}

#[test]
fn queued_plan_and_commit_coalesce_with_the_ready_bundle() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let first = bundle(1);
    let second = bundle(2);
    state.plan(&first, 1);
    state
        .data(&mut receiver, 1, 1, &first.compressed_payload, true)
        .unwrap();
    let (control_tx, control_rx) = bounded(READER_QUEUE_CAPACITY);
    let (data_tx, data_rx) = bounded(1);
    control_tx
        .send(Ok(Message::BundlePlan(wire::BundlePlan {
            attempt_no: 1,
            descriptor: BundlePlanDescriptor::from_bundle_descriptor(&second.descriptor),
        })))
        .unwrap();
    control_tx
        .send(Ok(Message::BundleCommit(BundleCommit {
            attempt_no: 1,
            descriptor: BundleCommitDescriptor::from_bundle_descriptor(&second.descriptor),
        })))
        .unwrap();
    data_tx
        .send(Ok(PendingDataChunk {
            key: AttemptKey::new(2, 1),
            is_last: true,
            bytes: second.compressed_payload.clone().into(),
        }))
        .unwrap();

    let mut output = Vec::new();
    let mut profile = ReceiverProfile::default();
    let mut consecutive_controls = 0;
    for step in 0..4 {
        let event = receive_event_before_commit(
            &control_rx,
            &data_rx,
            &crossbeam_channel::never(),
            false,
            &mut consecutive_controls,
            || {
                drain_ready_commits(
                    &mut output,
                    &mut receiver,
                    &mut state.bundles,
                    &mut profile,
                    false,
                )?;
                // Peer посылает Finish только после durable ACK: никакого таймера.
                if profile.committed_bundles == 2 {
                    control_tx
                        .send(Ok(Message::Finish(Finish {
                            final_bundle_no: 2,
                            final_work_ordinal: Some(2),
                        })))
                        .unwrap();
                }
                Ok(())
            },
        )
        .unwrap();
        match (step, event) {
            (0, ReceiveEvent::Control(Message::BundlePlan(_))) => {
                state.plan(&second, 1);
                state.bundles.back_mut().unwrap().commit = None;
            }
            (1, ReceiveEvent::Control(Message::BundleCommit(commit))) => {
                state.bundles.back_mut().unwrap().commit = Some(commit.descriptor);
            }
            (2, ReceiveEvent::Data(data)) => {
                state
                    .data(&mut receiver, 2, 1, &data.bytes, data.is_last)
                    .unwrap();
            }
            (3, ReceiveEvent::Control(Message::Finish(_))) => {}
            _ => panic!("unexpected event order at step {step}"),
        }
        if step < 3 {
            assert!(output.is_empty(), "premature singleton ACK");
        }
    }
    assert_eq!(profile.committed_bundles, 2);
    assert_eq!(profile.bundle_sync.bytes, 1);
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 2);
    let mut acknowledgements = Cursor::new(output);
    let Message::BundleAck(ack) = wire::read_message(&mut acknowledgements).unwrap() else {
        panic!("missing group ACK");
    };
    assert_eq!(ack.bundle_no, 2);
    assert_eq!(
        acknowledgements.position(),
        acknowledgements.get_ref().len() as u64
    );
    assert_eq!(
        directory.archive_bytes(),
        [first.compressed_payload, second.compressed_payload].concat()
    );
}

#[test]
fn queued_commit_does_not_make_incomplete_payload_durable() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let first = bundle(1);
    let second = bundle(2);
    state.plan(&first, 1);
    state.plan(&second, 1);
    state.bundles.back_mut().unwrap().commit = None;
    state
        .data(&mut receiver, 1, 1, &first.compressed_payload, true)
        .unwrap();
    let (control_tx, control_rx) = bounded(1);
    let (data_tx, data_rx) = bounded(1);
    control_tx
        .send(Ok(Message::BundleCommit(BundleCommit {
            attempt_no: 1,
            descriptor: BundleCommitDescriptor::from_bundle_descriptor(&second.descriptor),
        })))
        .unwrap();
    data_tx
        .send(Ok(PendingDataChunk {
            key: AttemptKey::new(2, 1),
            is_last: false,
            bytes: second.compressed_payload[..5].to_vec().into(),
        }))
        .unwrap();
    let mut consecutive_controls = 0;
    let ReceiveEvent::Control(Message::BundleCommit(commit)) = receive_event_before_commit(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        false,
        &mut consecutive_controls,
        || panic!("queued commit must be handled first"),
    )
    .unwrap() else {
        panic!("missing queued commit");
    };
    state.bundles.back_mut().unwrap().commit = Some(commit.descriptor);
    let mut output = Vec::new();
    let mut profile = ReceiverProfile::default();
    let ReceiveEvent::Data(data) = receive_event_before_commit(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        false,
        &mut consecutive_controls,
        || {
            drain_ready_commits(
                &mut output,
                &mut receiver,
                &mut state.bundles,
                &mut profile,
                false,
            )
        },
    )
    .unwrap() else {
        panic!("missing partial payload");
    };
    state
        .data(&mut receiver, 2, 1, &data.bytes, data.is_last)
        .unwrap();
    assert!(output.is_empty());
    assert_eq!(profile.committed_bundles, 0);
    assert_eq!(receiver.resume_state().archive_len, 0);
    assert!(!state.bundles.back().unwrap().data_finished);
}

#[test]
fn last_ready_bundle_commits_before_waiting_for_peer_finish() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let bundle = bundle(1);
    state.plan(&bundle, 1);
    state
        .data(&mut receiver, 1, 1, &bundle.compressed_payload, true)
        .unwrap();
    let (control_tx, control_rx) = bounded(1);
    let (_data_tx, data_rx) = bounded(1);
    let mut consecutive_controls = 0;
    let event = receive_event_before_commit(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        false,
        &mut consecutive_controls,
        || {
            assert_eq!(state.commit(&mut receiver).bundle_no, 1);
            control_tx
                .send(Ok(Message::Finish(Finish {
                    final_bundle_no: 1,
                    final_work_ordinal: Some(1),
                })))
                .unwrap();
            Ok(())
        },
    )
    .unwrap();
    assert!(matches!(event, ReceiveEvent::Control(Message::Finish(_))));
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 1);
}

#[test]
fn bounded_control_priority_yields_to_data_and_preserves_orphan_backpressure() {
    let (control_tx, control_rx) = bounded(READER_QUEUE_CAPACITY + 1);
    let (data_tx, data_rx) = bounded(1);
    for _ in 0..=READER_QUEUE_CAPACITY {
        control_tx
            .send(Ok(Message::ResumeQuery(ResumeQuery {
                dump_id: "queued".into(),
            })))
            .unwrap();
    }
    let data = || PendingDataChunk {
        key: AttemptKey::new(1, 1),
        is_last: true,
        bytes: vec![1].into(),
    };
    data_tx.send(Ok(data())).unwrap();
    let mut consecutive_controls = 0;
    for _ in 0..READER_QUEUE_CAPACITY {
        assert!(matches!(
            receive_event_before_commit(
                &control_rx,
                &data_rx,
                &crossbeam_channel::never(),
                false,
                &mut consecutive_controls,
                || panic!("control budget was not exhausted"),
            )
            .unwrap(),
            ReceiveEvent::Control(_)
        ));
    }
    let mut commit_checks = 0;
    assert!(matches!(
        receive_event_before_commit(
            &control_rx,
            &data_rx,
            &crossbeam_channel::never(),
            false,
            &mut consecutive_controls,
            || {
                commit_checks += 1;
                Ok(())
            },
        )
        .unwrap(),
        ReceiveEvent::Data(_)
    ));
    assert_eq!(commit_checks, 1);
    assert_eq!(control_rx.len(), 1);
    data_tx.send(Ok(data())).unwrap();
    consecutive_controls = READER_QUEUE_CAPACITY;
    assert!(matches!(
        receive_event_before_commit(
            &control_rx,
            &data_rx,
            &crossbeam_channel::never(),
            true,
            &mut consecutive_controls,
            || Ok(()),
        )
        .unwrap(),
        ReceiveEvent::Control(_)
    ));
    assert_eq!(data_rx.len(), 1, "orphan must still wait for its plan");
}

#[test]
fn queued_invalid_control_fails_without_committing_ready_payload() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let bundle = bundle(1);
    state.plan(&bundle, 1);
    state
        .data(&mut receiver, 1, 1, &bundle.compressed_payload, true)
        .unwrap();
    let (control_tx, control_rx) = bounded(1);
    let (_data_tx, data_rx) = bounded(1);
    control_tx
        .send(wire::read_message(&mut Cursor::new([0xff; 12])))
        .unwrap();
    let result = receive_event_before_commit(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        false,
        &mut 0,
        || panic!("invalid queued control must not advance durable state"),
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("invalid control was accepted"),
    };
    assert!(error.to_string().contains("invalid v4 packet magic"));
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 0);
    assert_eq!(state.bundles.len(), 1);
}

#[test]
fn queued_abort_preserves_ready_prefix_and_ignores_late_payload() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    let mut state = PendingState::default();
    let first = bundle(1);
    let second = bundle(2);
    state.plan(&first, 1);
    state.plan(&second, 1);
    state.bundles.back_mut().unwrap().commit = None;
    state
        .data(&mut receiver, 1, 1, &first.compressed_payload, true)
        .unwrap();
    state
        .data(&mut receiver, 2, 1, &second.compressed_payload[..5], false)
        .unwrap();
    let (control_tx, control_rx) = bounded(1);
    let (data_tx, data_rx) = bounded(1);
    control_tx
        .send(Ok(Message::BundleAbort(BundleAbort {
            bundle_no: 2,
            attempt_no: 1,
        })))
        .unwrap();
    data_tx
        .send(Ok(PendingDataChunk {
            key: AttemptKey::new(2, 1),
            is_last: true,
            bytes: second.compressed_payload[5..].to_vec().into(),
        }))
        .unwrap();
    let mut consecutive_controls = 0;
    let ReceiveEvent::Control(Message::BundleAbort(abort)) = receive_event_before_commit(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        false,
        &mut consecutive_controls,
        || panic!("queued abort must be processed first"),
    )
    .unwrap() else {
        panic!("missing queued abort");
    };
    state.abort(&mut receiver, abort.bundle_no, abort.attempt_no);
    let ReceiveEvent::Data(data) = receive_event_before_commit(
        &control_rx,
        &data_rx,
        &crossbeam_channel::never(),
        false,
        &mut consecutive_controls,
        || {
            assert_eq!(state.commit(&mut receiver).bundle_no, 1);
            Ok(())
        },
    )
    .unwrap() else {
        panic!("missing late payload");
    };
    state
        .data(&mut receiver, 2, 1, &data.bytes, data.is_last)
        .unwrap();
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 1);
    assert_eq!(directory.archive_bytes(), first.compressed_payload);
}

#[test]
fn data_reader_recycles_bounded_storage_without_changing_payload() {
    let mut packets = Vec::new();
    for bundle_no in 1..=64 {
        wire::write_bundle_data_message(
            &mut packets,
            bundle_no,
            3,
            true,
            &vec![bundle_no as u8; 2048],
        )
        .unwrap();
    }
    let rx = spawn_data_reader(Cursor::new(packets)).unwrap();
    let mut allocations = HashSet::new();
    for bundle_no in 1..=64 {
        let data = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(data.key, AttemptKey::new(bundle_no, 3));
        assert!(data.is_last);
        assert_eq!(data.bytes.len(), 2048);
        assert!(data.bytes.iter().all(|byte| *byte == bundle_no as u8));
        allocations.insert(data.bytes.storage.as_ptr() as usize);
        drop(data);
    }
    assert!(allocations.len() <= DATA_BUFFER_COUNT);
    assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap().is_err());
    assert!(matches!(
        rx.recv_timeout(Duration::from_secs(5)),
        Err(crossbeam_channel::RecvTimeoutError::Disconnected)
    ));
}

#[test]
fn abandoned_orphan_returns_its_buffer_and_disconnected_pool_does_not_block() {
    let (recycle, returned) = bounded(1);
    let storage = vec![0xa5; 1024];
    let allocation = storage.as_ptr();
    let mut orphan = HashMap::new();
    let key = AttemptKey::new(1, 1);
    orphan.insert(
        key,
        PendingDataChunk {
            key,
            is_last: false,
            bytes: DataBuffer {
                storage,
                payload_len: 8,
                recycle: Some(recycle.clone()),
            },
        },
    );
    drop(orphan.remove(&key));
    let storage = returned.try_recv().unwrap();
    assert_eq!(storage.as_ptr(), allocation);
    assert_eq!(storage.len(), 1024);
    drop(returned);
    drop(DataBuffer {
        storage,
        payload_len: 0,
        recycle: Some(recycle),
    });
}

impl CloneableStream for Cursor<Vec<u8>> {
    fn try_clone_stream(&self) -> Result<Self> {
        Ok(self.clone())
    }

    fn shutdown_stream(&self) -> Result<()> {
        // Чтение конечного буфера никогда не ждёт внешнего события.
        Ok(())
    }
}

#[path = "storage_fault_tests.rs"]
mod storage_fault_tests;
