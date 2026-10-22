use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::network::protocol::PreparedBundle;

static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "fast-dump-receiver-boundaries-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn open(&self) -> ReceiverSession {
        ReceiverSession::open(self.0.clone(), "boundaries", 1).unwrap()
    }

    fn archive_bytes(&self) -> Vec<u8> {
        fs::read(self.0.join("boundaries.tar.zst.part")).unwrap()
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
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
                bytes: bytes.to_vec(),
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

    let wal_path = directory.0.join("boundaries.wal.part");
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
    let path = directory.0.join("boundaries.wal.part");
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
    let part = directory.0.join("boundaries.tar.zst.part");
    let final_path = directory.0.join("boundaries.tar.zst");
    let expected = fs::read(&part).unwrap();
    fs::rename(&part, &final_path).unwrap();
    assert!(ReceiverSession::open(directory.0.clone(), "boundaries", 1).is_err());
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
    let wal_path = directory.0.join("boundaries.wal.part");
    let expected_wal = fs::read(&wal_path).unwrap();
    let final_path = directory.0.join("boundaries.tar.zst");
    fs::write(&final_path, b"previous completed archive").unwrap();

    assert!(ReceiverSession::open(directory.0.clone(), "boundaries", 1).is_err());
    assert_eq!(fs::read(final_path).unwrap(), b"previous completed archive");
    assert_eq!(directory.archive_bytes(), expected_part);
    assert_eq!(fs::read(wal_path).unwrap(), expected_wal);
}

#[test]
fn finalize_preserves_existing_archive_and_resume_state() {
    let directory = TestDirectory::new();
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    let final_path = directory.0.join("boundaries.tar.zst");
    // Имитируем посторонний готовый файл, появившийся после открытия сессии.
    fs::write(&final_path, b"another completed archive").unwrap();
    let wal_path = directory.0.join("boundaries.wal.part");
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
    let wal_path = directory.0.join("boundaries.wal.part");
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
        let error = ReceiverSession::open(directory.0.clone(), "boundaries", 1)
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
    let wal_path = directory.0.join("boundaries.wal.part");
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
        let error = ReceiverSession::open(directory.0.clone(), "boundaries", 1)
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
    let wal_path = directory.0.join("boundaries.wal.part");
    let expected_wal = fs::read(&wal_path).unwrap();
    let probe = |mode: &str| {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "receiver::network::tests::dump_lock_child_probe",
                "--nocapture",
            ])
            .env("FAST_DUMP_TEST_RECEIVER_LOCK_DIR", &directory.0)
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
    assert!(ReceiverSession::open(directory.0.clone(), "boundaries", 1).is_err());
    assert_eq!(directory.archive_bytes(), expected_archive);
    assert_eq!(fs::read(&wal_path).unwrap(), expected_wal);

    drop(receiver);
    assert!(directory.0.join("boundaries.lock").exists());
    probe("available");
    assert_eq!(directory.archive_bytes(), bundle(1).compressed_payload);
    assert_eq!(fs::read(wal_path).unwrap(), expected_wal);
    assert!(directory.0.join("boundaries.lock").exists());
}

#[test]
fn invalid_dump_id_is_rejected_before_creating_receiver_directory() {
    let directory = TestDirectory::new();
    let data_dir = directory.0.join("not-created");
    for dump_id in ["../escaped", "nested/dump", "nested\\dump"] {
        assert!(ReceiverSession::open(data_dir.clone(), dump_id, 1).is_err());
    }
    assert!(!data_dir.exists());
}

#[test]
fn legacy_migration_never_takes_files_from_active_or_closed_v4_suffix_dump() {
    let directory = TestDirectory::new();
    let mut suffixed = ReceiverSession::open(directory.0.clone(), "boundaries.v4", 1).unwrap();
    suffixed.commit_bundle(&bundle(1)).unwrap();
    suffixed.stage_bundle(&bundle(2)).unwrap();
    let suffixed_part = directory.0.join("boundaries.v4.tar.zst.part");
    let suffixed_wal = directory.0.join("boundaries.v4.wal.part");
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
    let suffixed = ReceiverSession::open(directory.0.clone(), "boundaries.v4", 1).unwrap();
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
    let path = directory.0.join("boundaries.catalog.part");
    for corrupt in [
        "",
        "fast-dump-v4-catalog-1",
        "fast-dump-v4-catalog-1\nskip\t3\topen_failed\tx",
    ] {
        fs::write(&path, corrupt).unwrap();
        assert!(ReceiverSession::open(directory.0.clone(), "boundaries", 1).is_err());
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
    let path = directory.0.join("boundaries.tar.zst.part");
    let bytes = directory.archive_bytes();
    fs::write(&path, &bytes[..5]).unwrap();
    assert!(ReceiverSession::open(directory.0.clone(), "boundaries", 1).is_err());
    assert_eq!(directory.archive_bytes(), bytes[..5]);
    fs::remove_file(&path).unwrap();
    assert!(ReceiverSession::open(directory.0.clone(), "boundaries", 1).is_err());
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
        .send(Ok(BundleData {
            bundle_no: 1,
            attempt_no: 1,
            is_last: true,
            bytes: bundle.compressed_payload[5..].to_vec(),
        }))
        .unwrap();

    // Оба канала готовы: orphan требует обработать именно план, не второй payload.
    let ReceiveEvent::Control(Message::BundlePlan(plan)) =
        receive_next_event(&control_rx, &data_rx, !state.orphan_data.is_empty()).unwrap()
    else {
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

    let ReceiveEvent::Data(data) =
        receive_next_event(&control_rx, &data_rx, !state.orphan_data.is_empty()).unwrap()
    else {
        panic!("receiver did not resume reading bundle data");
    };
    state
        .data(
            &mut receiver,
            data.bundle_no,
            data.attempt_no,
            &data.bytes,
            data.is_last,
        )
        .unwrap();
    assert_eq!(state.commit(&mut receiver).bundle_no, 1);
    assert_eq!(directory.archive_bytes(), bundle.compressed_payload);
}
