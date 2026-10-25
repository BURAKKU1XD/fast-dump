use std::fs;
use std::path::Path;
use std::thread;

use crate::network::protocol::BundleEntryKind;
use crate::network::protocol::wire;
use crate::network::transport::{BoundListener, connect_once};
use crate::receiver::{self, ReceiverSession};
use crate::role::Role;
use crate::sender;
use crate::sender::test_hooks;
use offline::{OfflineDumpOptions, run_offline_dump};

mod offline;
mod parallel;
mod streamed_abort;
mod support;

pub(crate) use support::{ArchiveEntrySummary, TestDir, read_archive_entries};
use support::{
    maybe_create_symlink, rename_canonical_receiver_state_to_legacy,
    send_bundles_then_disconnect_without_reading_acks, send_first_bundle_then_disconnect,
    spawn_v4_receiver_once, symlink_supported,
};

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
