use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tar::EntryType;

use crate::network::protocol::wire::{
    self, BundleCommit, BundlePlan, Message, SkipAck, SkipEntries,
};
use crate::network::protocol::{BundleCommitDescriptor, BundleEntryKind, BundlePlanDescriptor};
use crate::network::transport::{BoundListener, connect_once};
use crate::receiver::{self, ReceiveReport, ReceiverSession};
use crate::role::Role;
use crate::sender;
use crate::sender::test_hooks;
use crate::{OfflineDumpOptions, run_offline_dump};

mod parallel;
mod streamed_abort;

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

#[test]
fn offline_roundtrip_builds_valid_tar_zst_in_deterministic_order() {
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::create_dir_all(source_dir.path().join("nested")).unwrap();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("nested").join("beta.txt"), b"beta").unwrap();
    fs::write(source_dir.path().join("nested").join("gamma.txt"), b"gamma").unwrap();
    maybe_create_symlink(
        Path::new("..").join("alpha.txt"),
        &source_dir.path().join("nested").join("link-to-alpha.txt"),
    );

    let report = run_offline_dump(
        source_dir.path().to_path_buf(),
        data_dir.path().to_path_buf(),
        "offline-basic",
        OfflineDumpOptions {
            bundle_target_bytes: 4,
            compression_level: 3,
        },
    )
    .unwrap();

    let entries = read_archive_entries(&report.final_archive_path);
    let paths = entries
        .iter()
        .map(|entry| entry.path.clone())
        .collect::<Vec<_>>();
    let mut expected_paths = vec![
        "alpha.txt".to_owned(),
        "nested/beta.txt".to_owned(),
        "nested/gamma.txt".to_owned(),
    ];
    if symlink_supported() {
        expected_paths.push("nested/link-to-alpha.txt".to_owned());
    }
    assert_eq!(paths, expected_paths);

    let regulars = entries
        .iter()
        .filter(|entry| entry.entry_type.is_file())
        .map(|entry| (entry.path.clone(), entry.bytes.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        regulars,
        vec![
            ("alpha.txt".to_owned(), b"alpha".to_vec()),
            ("nested/beta.txt".to_owned(), b"beta".to_vec()),
            ("nested/gamma.txt".to_owned(), b"gamma".to_vec()),
        ]
    );

    if symlink_supported() {
        let symlink = entries
            .iter()
            .find(|entry| entry.path == "nested/link-to-alpha.txt")
            .unwrap();
        assert!(symlink.entry_type.is_symlink());
        assert_eq!(symlink.link_name.as_deref(), Some("../alpha.txt"));
    }
}

#[test]
fn offline_recovery_truncates_uncommitted_bundle_and_resumes() {
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"111").unwrap();
    fs::write(source_dir.path().join("beta.txt"), b"222").unwrap();
    fs::write(source_dir.path().join("gamma.txt"), b"333").unwrap();

    let options = sender::BundlePlanOptions {
        bundle_target_bytes: 1,
        compression_level: 3,
        ..Default::default()
    };
    let mut receiver =
        ReceiverSession::open(data_dir.path().to_path_buf(), "resume-dump", 3).unwrap();
    let plan = sender::prepare_dump(source_dir.path(), receiver.resume_state(), options).unwrap();
    assert!(plan.bundles.len() >= 2);
    receiver
        .record_terminal_skips(&plan.new_terminal_skips)
        .unwrap();
    receiver.commit_bundle(&plan.bundles[0]).unwrap();
    receiver.stage_bundle(&plan.bundles[1]).unwrap();
    drop(receiver);

    let mut resumed_receiver =
        ReceiverSession::open(data_dir.path().to_path_buf(), "resume-dump", 3).unwrap();
    assert_eq!(resumed_receiver.resume_state().last_committed_bundle_no, 1);
    assert_eq!(
        fs::metadata(data_dir.path().join("resume-dump.tar.zst.part"))
            .unwrap()
            .len(),
        resumed_receiver.resume_state().archive_len
    );

    let resume_plan =
        sender::prepare_dump(source_dir.path(), resumed_receiver.resume_state(), options).unwrap();
    resumed_receiver
        .record_terminal_skips(&resume_plan.new_terminal_skips)
        .unwrap();
    for bundle in &resume_plan.bundles {
        resumed_receiver.commit_bundle(bundle).unwrap();
    }
    let report = resumed_receiver.finalize().unwrap();
    let regulars = read_archive_entries(&report.final_archive_path)
        .into_iter()
        .filter(|entry| entry.entry_type.is_file())
        .map(|entry| (entry.path, entry.bytes))
        .collect::<Vec<_>>();
    assert_eq!(
        regulars,
        vec![
            ("alpha.txt".to_owned(), b"111".to_vec()),
            ("beta.txt".to_owned(), b"222".to_vec()),
            ("gamma.txt".to_owned(), b"333".to_vec()),
        ]
    );
}

#[test]
fn offline_recovery_migrates_legacy_v4_partial_state_into_canonical_names() {
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"111").unwrap();
    fs::write(source_dir.path().join("beta.txt"), b"222").unwrap();
    fs::write(source_dir.path().join("gamma.txt"), b"333").unwrap();

    let options = sender::BundlePlanOptions {
        bundle_target_bytes: 1,
        compression_level: 3,
        ..Default::default()
    };
    let mut receiver =
        ReceiverSession::open(data_dir.path().to_path_buf(), "legacy-resume-dump", 3).unwrap();
    let plan = sender::prepare_dump(source_dir.path(), receiver.resume_state(), options).unwrap();
    receiver.commit_bundle(&plan.bundles[0]).unwrap();
    drop(receiver);

    rename_canonical_receiver_state_to_legacy(data_dir.path(), "legacy-resume-dump");
    assert!(
        data_dir
            .path()
            .join("legacy-resume-dump.v4.tar.zst.part")
            .exists()
    );
    assert!(
        !data_dir
            .path()
            .join("legacy-resume-dump.tar.zst.part")
            .exists()
    );

    let resumed_receiver =
        ReceiverSession::open(data_dir.path().to_path_buf(), "legacy-resume-dump", 3).unwrap();
    assert_eq!(resumed_receiver.resume_state().last_committed_bundle_no, 1);
    assert!(
        data_dir
            .path()
            .join("legacy-resume-dump.tar.zst.part")
            .exists()
    );
    assert!(data_dir.path().join("legacy-resume-dump.wal.part").exists());
    assert!(
        !data_dir
            .path()
            .join("legacy-resume-dump.v4.tar.zst.part")
            .exists()
    );
    assert!(
        !data_dir
            .path()
            .join("legacy-resume-dump.v4.wal.part")
            .exists()
    );
}

#[test]
fn terminal_skips_persist_across_resume_without_reopening_the_file() {
    let _guard = test_hooks::lock();
    test_hooks::clear();

    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("skip.txt"), b"skip-me").unwrap();
    fs::write(source_dir.path().join("omega.txt"), b"omega").unwrap();
    let skip_path = source_dir.path().join("skip.txt");
    for _ in 0..3 {
        test_hooks::fail_next_open(&skip_path);
    }

    let options = sender::BundlePlanOptions {
        bundle_target_bytes: 1,
        compression_level: 3,
        ..Default::default()
    };
    let mut receiver =
        ReceiverSession::open(data_dir.path().to_path_buf(), "skip-dump", 3).unwrap();
    let first_plan =
        sender::prepare_dump(source_dir.path(), receiver.resume_state(), options).unwrap();
    assert_eq!(
        first_plan
            .new_terminal_skips
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect::<Vec<_>>(),
        vec!["skip.txt"]
    );
    receiver
        .record_terminal_skips(&first_plan.new_terminal_skips)
        .unwrap();
    receiver.commit_bundle(&first_plan.bundles[0]).unwrap();
    drop(receiver);

    test_hooks::clear();
    fs::write(&skip_path, b"readable-now").unwrap();

    let mut resumed_receiver =
        ReceiverSession::open(data_dir.path().to_path_buf(), "skip-dump", 3).unwrap();
    assert_eq!(
        resumed_receiver
            .resume_state()
            .terminal_skips
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect::<Vec<_>>(),
        vec!["skip.txt"]
    );

    let resume_plan =
        sender::prepare_dump(source_dir.path(), resumed_receiver.resume_state(), options).unwrap();
    assert!(resume_plan.new_terminal_skips.is_empty());
    assert!(
        resume_plan
            .bundles
            .iter()
            .flat_map(|bundle| bundle.descriptor.entries.iter())
            .all(|entry| match &entry.kind {
                BundleEntryKind::Regular | BundleEntryKind::Symlink { .. } => {
                    entry.relative_path != "skip.txt"
                }
            })
    );

    resumed_receiver
        .record_terminal_skips(&resume_plan.new_terminal_skips)
        .unwrap();
    for bundle in &resume_plan.bundles {
        resumed_receiver.commit_bundle(bundle).unwrap();
    }
    let report = resumed_receiver.finalize().unwrap();
    let regular_paths = read_archive_entries(&report.final_archive_path)
        .into_iter()
        .filter(|entry| entry.entry_type.is_file())
        .map(|entry| entry.path)
        .collect::<Vec<_>>();
    assert_eq!(
        regular_paths,
        vec!["alpha.txt".to_owned(), "omega.txt".to_owned()]
    );

    let issue_report_path = report.issue_report_path.unwrap();
    let issue_report = fs::read_to_string(issue_report_path).unwrap();
    assert!(issue_report.contains("skip.txt"));
    assert!(issue_report.contains("open_failed"));
}

#[test]
fn single_channel_network_roundtrip_builds_valid_archive() {
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::create_dir_all(source_dir.path().join("nested")).unwrap();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("nested").join("beta.txt"), b"beta").unwrap();
    fs::write(source_dir.path().join("nested").join("gamma.txt"), b"gamma").unwrap();

    let dump_id = "network-basic";
    let (addr, receiver) = spawn_v4_receiver_once(data_dir.path());
    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let send_report = sender::send_dump(
        &mut sender_session.control_stream,
        &mut sender_session.data_stream,
        source_dir.path().to_path_buf(),
        dump_id,
        sender::BundlePlanOptions {
            bundle_target_bytes: 1,
            compression_level: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let receive_report = receiver.join().unwrap().unwrap();

    assert_eq!(send_report.final_work_ordinal, 3);
    assert_eq!(receive_report.resume_state.last_committed_bundle_no, 3);
    let regulars = read_archive_entries(&receive_report.final_archive_path)
        .into_iter()
        .filter(|entry| entry.entry_type.is_file())
        .map(|entry| (entry.path, entry.bytes))
        .collect::<Vec<_>>();
    assert_eq!(
        regulars,
        vec![
            ("alpha.txt".to_owned(), b"alpha".to_vec()),
            ("nested/beta.txt".to_owned(), b"beta".to_vec()),
            ("nested/gamma.txt".to_owned(), b"gamma".to_vec()),
        ]
    );
}

#[test]
fn network_roundtrip_in_pull_mode_builds_valid_archive() {
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::create_dir_all(source_dir.path().join("nested")).unwrap();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("nested").join("beta.txt"), b"beta").unwrap();

    let dump_id = "network-pull-basic";
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let sender_path = source_dir.path().to_path_buf();
    let sender = thread::spawn(move || {
        let mut session = listener.accept(Role::Sender, Some(dump_id), None).unwrap();
        sender::send_dump(
            &mut session.control_stream,
            &mut session.data_stream,
            sender_path,
            dump_id,
            sender::BundlePlanOptions {
                bundle_target_bytes: 1,
                compression_level: 3,
                ..Default::default()
            },
        )
    });

    let receiver_session = connect_once(Role::Receiver, addr, None, None).unwrap();
    let peer_dump_id = receiver_session.peer_dump_id.clone().unwrap();
    let receive_report = receiver::receive_dump(
        receiver_session.control_stream,
        receiver_session.data_stream,
        data_dir.path().to_path_buf(),
        &peer_dump_id,
    )
    .unwrap();
    let send_report = sender.join().unwrap().unwrap();

    assert_eq!(send_report.final_work_ordinal, 2);
    assert_eq!(receive_report.resume_state.last_committed_bundle_no, 2);
    assert_eq!(
        read_archive_entries(&receive_report.final_archive_path)
            .into_iter()
            .filter(|entry| entry.entry_type.is_file())
            .map(|entry| (entry.path, entry.bytes))
            .collect::<Vec<_>>(),
        vec![
            ("alpha.txt".to_owned(), b"alpha".to_vec()),
            ("nested/beta.txt".to_owned(), b"beta".to_vec()),
        ]
    );
}

#[test]
fn single_channel_network_resume_after_disconnect_uses_committed_bundle_boundary() {
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"111").unwrap();
    fs::write(source_dir.path().join("beta.txt"), b"222").unwrap();
    fs::write(source_dir.path().join("gamma.txt"), b"333").unwrap();

    let dump_id = "network-resume";
    let options = sender::BundlePlanOptions {
        bundle_target_bytes: 1,
        compression_level: 3,
        ..Default::default()
    };

    let (addr1, receiver1) = spawn_v4_receiver_once(data_dir.path());
    send_first_bundle_then_disconnect(addr1, source_dir.path(), dump_id, options, false);
    assert!(receiver1.join().unwrap().is_err());

    let (addr2, receiver2) = spawn_v4_receiver_once(data_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let send_report = sender::send_dump(
        &mut sender2.control_stream,
        &mut sender2.data_stream,
        source_dir.path().to_path_buf(),
        dump_id,
        options,
    )
    .unwrap();
    let receive_report = receiver2.join().unwrap().unwrap();

    assert_eq!(send_report.final_bundle_no, 3);
    assert_eq!(receive_report.resume_state.last_committed_bundle_no, 3);
    assert_eq!(
        read_archive_entries(&receive_report.final_archive_path)
            .into_iter()
            .filter(|entry| entry.entry_type.is_file())
            .map(|entry| (entry.path, entry.bytes))
            .collect::<Vec<_>>(),
        vec![
            ("alpha.txt".to_owned(), b"111".to_vec()),
            ("beta.txt".to_owned(), b"222".to_vec()),
            ("gamma.txt".to_owned(), b"333".to_vec()),
        ]
    );
}

#[test]
fn single_channel_network_persists_skip_catalog_before_bundle_ack() {
    let _guard = test_hooks::lock();
    test_hooks::clear();

    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("skip.txt"), b"skip-me").unwrap();
    fs::write(source_dir.path().join("omega.txt"), b"omega").unwrap();
    let skip_path = source_dir.path().join("skip.txt");
    for _ in 0..3 {
        test_hooks::fail_next_open(&skip_path);
    }

    let dump_id = "network-skip-resume";
    let options = sender::BundlePlanOptions {
        bundle_target_bytes: 1,
        compression_level: 3,
        ..Default::default()
    };

    let (addr1, receiver1) = spawn_v4_receiver_once(data_dir.path());
    send_first_bundle_then_disconnect(addr1, source_dir.path(), dump_id, options, true);
    assert!(receiver1.join().unwrap().is_err());

    test_hooks::clear();
    fs::write(&skip_path, b"readable-now").unwrap();

    let (addr2, receiver2) = spawn_v4_receiver_once(data_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let send_report = sender::send_dump(
        &mut sender2.control_stream,
        &mut sender2.data_stream,
        source_dir.path().to_path_buf(),
        dump_id,
        options,
    )
    .unwrap();
    let receive_report = receiver2.join().unwrap().unwrap();

    assert_eq!(send_report.terminal_skip_count, 1);
    assert_eq!(
        receive_report
            .resume_state
            .terminal_skips
            .iter()
            .map(|entry| entry.relative_path.as_str())
            .collect::<Vec<_>>(),
        vec!["skip.txt"]
    );
    assert_eq!(
        read_archive_entries(&receive_report.final_archive_path)
            .into_iter()
            .filter(|entry| entry.entry_type.is_file())
            .map(|entry| entry.path)
            .collect::<Vec<_>>(),
        vec!["alpha.txt".to_owned(), "omega.txt".to_owned()]
    );

    let issue_report = fs::read_to_string(receive_report.issue_report_path.unwrap()).unwrap();
    assert!(issue_report.contains("skip.txt"));
    assert!(issue_report.contains("open_failed"));
}

#[test]
fn single_channel_network_resume_after_disconnect_with_unread_bundle_acks() {
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"111").unwrap();
    fs::write(source_dir.path().join("beta.txt"), b"222").unwrap();
    fs::write(source_dir.path().join("gamma.txt"), b"333").unwrap();
    fs::write(source_dir.path().join("delta.txt"), b"444").unwrap();

    let dump_id = "network-window-resume";
    let options = sender::BundlePlanOptions {
        bundle_target_bytes: 1,
        compression_level: 3,
        ..Default::default()
    };

    let (addr1, receiver1) = spawn_v4_receiver_once(data_dir.path());
    send_bundles_then_disconnect_without_reading_acks(
        addr1,
        source_dir.path(),
        dump_id,
        options,
        2,
    );
    assert!(receiver1.join().unwrap().is_err());

    let recovered = ReceiverSession::open(data_dir.path().to_path_buf(), dump_id, 3).unwrap();
    assert_eq!(recovered.resume_state().last_committed_bundle_no, 2);
    drop(recovered);

    let (addr2, receiver2) = spawn_v4_receiver_once(data_dir.path());
    let mut sender2 = connect_once(Role::Sender, addr2, Some(dump_id), None).unwrap();
    let send_report = sender::send_dump(
        &mut sender2.control_stream,
        &mut sender2.data_stream,
        source_dir.path().to_path_buf(),
        dump_id,
        options,
    )
    .unwrap();
    let receive_report = receiver2.join().unwrap().unwrap();

    assert_eq!(send_report.final_bundle_no, 4);
    assert_eq!(receive_report.resume_state.last_committed_bundle_no, 4);
    assert_eq!(
        read_archive_entries(&receive_report.final_archive_path)
            .into_iter()
            .filter(|entry| entry.entry_type.is_file())
            .map(|entry| (entry.path, entry.bytes))
            .collect::<Vec<_>>(),
        vec![
            ("alpha.txt".to_owned(), b"111".to_vec()),
            ("beta.txt".to_owned(), b"222".to_vec()),
            ("delta.txt".to_owned(), b"444".to_vec()),
            ("gamma.txt".to_owned(), b"333".to_vec()),
        ]
    );
}

#[test]
fn single_channel_network_retries_streamed_large_file_after_abort() {
    let _guard = test_hooks::lock();
    test_hooks::clear();

    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    let large_path = source_dir.path().join("large.bin");
    let large_bytes = vec![b'x'; wire::MAX_DATA_CHUNK_BYTES + 73];
    let logical_bytes = large_bytes.len() as u64;
    let short_read_bytes = logical_bytes / 2;
    fs::write(&large_path, &large_bytes).unwrap();
    test_hooks::force_short_read_after_next_open(&large_path, short_read_bytes);

    let dump_id = "network-streamed-retry";
    let (addr, receiver) = spawn_v4_receiver_once(data_dir.path());
    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let (send_report, profile) = sender::send_dump_profile(
        &mut sender_session.control_stream,
        &mut sender_session.data_stream,
        source_dir.path().to_path_buf(),
        dump_id,
        sender::BundlePlanOptions {
            bundle_target_bytes: 1024,
            compression_level: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let receive_report = receiver.join().unwrap().unwrap();

    assert_eq!(send_report.bundles_sent, 1);
    assert_eq!(send_report.terminal_skip_count, 0);
    assert_eq!(profile.bundles_sent, 1);
    assert_eq!(profile.terminal_skips, 0);
    assert_eq!(profile.bundle_aborts, 1);
    assert_eq!(profile.bundle_rebuilds, 1);
    // Повторное чтение увеличивает стоимость I/O, но не размер полезного дампа.
    assert_eq!(profile.total.bytes, logical_bytes);
    assert_eq!(profile.read.bytes, logical_bytes + short_read_bytes);
    assert!(profile.read.bytes > profile.total.bytes);
    assert_eq!(receive_report.resume_state.last_committed_bundle_no, 1);
    assert_eq!(
        read_archive_entries(&receive_report.final_archive_path)
            .into_iter()
            .filter(|entry| entry.entry_type.is_file())
            .map(|entry| (entry.path, entry.bytes))
            .collect::<Vec<_>>(),
        vec![("large.bin".to_owned(), large_bytes)]
    );
}

#[test]
fn single_channel_network_skips_streamed_large_file_after_abort_retry_budget() {
    let _guard = test_hooks::lock();
    test_hooks::clear();

    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    let large_path = source_dir.path().join("large.bin");
    let large_bytes = vec![b'y'; wire::MAX_DATA_CHUNK_BYTES + 91];
    let short_read_bytes = large_bytes.len() as u64 / 2;
    let surviving_bytes = b"omega";
    fs::write(&large_path, &large_bytes).unwrap();
    fs::write(source_dir.path().join("omega.txt"), surviving_bytes).unwrap();
    for _ in 0..3 {
        test_hooks::force_short_read_after_next_open(&large_path, short_read_bytes);
    }

    let dump_id = "network-streamed-skip";
    let (addr, receiver) = spawn_v4_receiver_once(data_dir.path());
    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let (send_report, profile) = sender::send_dump_profile(
        &mut sender_session.control_stream,
        &mut sender_session.data_stream,
        source_dir.path().to_path_buf(),
        dump_id,
        sender::BundlePlanOptions {
            bundle_target_bytes: 1024,
            compression_level: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let receive_report = receiver.join().unwrap().unwrap();

    assert_eq!(send_report.terminal_skip_count, 1);
    assert_eq!(send_report.bundles_sent, 1);
    assert_eq!(profile.bundles_sent, 1);
    assert_eq!(profile.terminal_skips, 1);
    assert_eq!(profile.bundle_aborts, 3);
    assert_eq!(profile.bundle_rebuilds, 2);
    // Все три оборванных чтения остаются в read; пропущенный файл не попадает в total.
    assert_eq!(profile.total.bytes, surviving_bytes.len() as u64);
    assert_eq!(
        profile.read.bytes,
        3 * short_read_bytes + surviving_bytes.len() as u64
    );
    assert!(profile.read.bytes > profile.total.bytes);
    assert_eq!(
        read_archive_entries(&receive_report.final_archive_path)
            .into_iter()
            .filter(|entry| entry.entry_type.is_file())
            .map(|entry| (entry.path, entry.bytes))
            .collect::<Vec<_>>(),
        vec![("omega.txt".to_owned(), b"omega".to_vec())]
    );

    let issue_report = fs::read_to_string(receive_report.issue_report_path.unwrap()).unwrap();
    assert!(issue_report.contains("large.bin"));
    assert!(issue_report.contains("unstable_after_retries"));
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

fn spawn_v4_receiver_once(
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

fn send_first_bundle_then_disconnect(
    addr: std::net::SocketAddr,
    source_dir: &Path,
    dump_id: &str,
    options: sender::BundlePlanOptions,
    send_terminal_skips: bool,
) {
    let mut sender_session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
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
    drop(sender_session);
}

fn send_bundles_then_disconnect_without_reading_acks(
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
fn maybe_create_symlink(target: impl AsRef<Path>, link: &Path) {
    std::os::unix::fs::symlink(target.as_ref(), link).unwrap();
}

#[cfg(windows)]
fn maybe_create_symlink(target: impl AsRef<Path>, link: &Path) {
    let _ = std::os::windows::fs::symlink_file(target.as_ref(), link);
}

#[cfg(unix)]
fn symlink_supported() -> bool {
    true
}

#[cfg(windows)]
fn symlink_supported() -> bool {
    false
}

fn rename_canonical_receiver_state_to_legacy(data_dir: &Path, dump_id: &str) {
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

fn rename_if_exists(from: &Path, to: &Path) {
    if from.exists() {
        fs::rename(from, to).unwrap();
    }
}
