use super::{
    ArchiveEntrySummary, TestDir, maybe_create_symlink, read_archive_entries,
    send_first_bundle_then_disconnect, spawn_v4_receiver_once,
};
use crate::network::transport::connect_once;
use crate::receiver::ReceiverSession;
use crate::role::Role;
use crate::sender::{self, BundlePlanOptions};
use std::fs;
use std::path::Path;
use tar::EntryType;

const BUNDLE_TARGET_BYTES: usize = 512 * 1024;

fn patterned_bytes(size: usize, seed: usize) -> Vec<u8> {
    (0..size)
        .map(|index| (index.wrapping_mul(31) ^ (index >> 8) ^ seed) as u8)
        .collect()
}

fn regular(path: impl Into<String>, bytes: Vec<u8>) -> ArchiveEntrySummary {
    ArchiveEntrySummary {
        path: path.into(),
        entry_type: EntryType::Regular,
        bytes,
        link_name: None,
    }
}

fn make_source(root: &Path) -> Vec<ArchiveEntrySummary> {
    // Ожидаемый DFS задаётся независимо от scanner, включая a/... перед a.txt.
    let mut expected = vec![regular("00-empty.bin", Vec::new())];
    for group in 0..16 {
        if group == 0 {
            expected.push(regular(
                "a/group-00/deeper/nested.bin",
                patterned_bytes(1025, 91),
            ));
        }
        for file in 0..12 {
            let index = group * 12 + file;
            expected.push(regular(
                format!("a/group-{group:02}/file-{file:02}.bin"),
                patterned_bytes(index * 911 % 8193, index),
            ));
        }
    }
    expected.push(regular("a.txt", b"sibling after the a directory".to_vec()));
    expected.push(regular("m-medium.bin", patterned_bytes(300 * 1024, 203)));
    expected.push(regular(
        "z-streamed.bin",
        patterned_bytes(1024 * 1024 + 37, 207),
    ));

    // Создаём в обратном порядке: результат не должен зависеть от read_dir.
    for entry in expected.iter().rev() {
        let path = root.join(&entry.path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, &entry.bytes).unwrap();
    }
    fs::create_dir_all(root.join("empty/nested")).unwrap();
    fs::create_dir_all(root.join("a/group-07/empty")).unwrap();

    let link = root.join("s-link");
    maybe_create_symlink("a.txt", &link);
    if fs::symlink_metadata(&link).is_ok() {
        expected.insert(
            expected.len() - 1,
            ArchiveEntrySummary {
                path: "s-link".into(),
                entry_type: EntryType::Symlink,
                bytes: Vec::new(),
                link_name: Some("a.txt".into()),
            },
        );
    }
    expected
}

fn options(readers: usize) -> BundlePlanOptions {
    BundlePlanOptions {
        bundle_target_bytes: BUNDLE_TARGET_BYTES,
        compression_level: 3,
        compression_threads: if readers == 1 { 1 } else { 4 },
        readers,
    }
}

#[test]
fn serial_and_parallel_network_dumps_include_every_source_entry_and_byte() {
    let source = TestDir::new();
    let expected = make_source(source.path());
    assert!(expected.len() >= 160);
    let expected_bytes = expected
        .iter()
        .map(|entry| entry.bytes.len() as u64)
        .sum::<u64>();
    let mut serial_entries = None;

    for readers in [1, 8] {
        let data = TestDir::new();
        let dump_id = format!("complete-readers-{readers}");
        let (addr, receiver) = spawn_v4_receiver_once(data.path());
        let mut session = connect_once(Role::Sender, addr, Some(&dump_id), None).unwrap();
        let (report, profile) = sender::send_dump_profile(
            &mut session.control_stream,
            &mut session.data_stream,
            source.path().to_path_buf(),
            &dump_id,
            options(readers),
        )
        .unwrap();
        let received = receiver.join().unwrap().unwrap();

        assert_eq!(report.terminal_skip_count, 0);
        assert!(report.bundles_sent >= 3);
        assert_eq!(report.final_work_ordinal, expected.len() as u64);
        assert_eq!(profile.discovered_entries, expected.len() as u64);
        assert_eq!(profile.bundled_entries, expected.len() as u64);
        assert_eq!(profile.read.bytes, expected_bytes);
        assert_eq!(
            received.resume_state.last_committed_work_ordinal,
            expected.len() as u64
        );
        assert!(received.resume_state.terminal_skips.is_empty());
        assert!(received.issue_report_path.is_none());

        let actual = read_archive_entries(&received.final_archive_path);
        assert_eq!(actual, expected, "readers={readers}");
        if let Some(serial) = &serial_entries {
            assert_eq!(&actual, serial);
        } else {
            serial_entries = Some(actual);
        }
    }
}

#[test]
fn parallel_network_resume_keeps_all_entries_without_duplicates() {
    let source = TestDir::new();
    let data = TestDir::new();
    let expected = make_source(source.path());
    let dump_id = "complete-parallel-resume";
    let (addr, receiver) = spawn_v4_receiver_once(data.path());
    send_first_bundle_then_disconnect(addr, source.path(), dump_id, options(8), false);
    assert!(receiver.join().unwrap().is_err());

    let state = ReceiverSession::open(data.path().to_path_buf(), dump_id, 3).unwrap();
    assert_eq!(state.resume_state().last_committed_bundle_no, 1);
    let committed_files = state.resume_state().last_committed_work_ordinal;
    assert!(committed_files > 0);
    assert!(expected.len() as u64 - committed_files >= 32);
    drop(state);

    let (addr, receiver) = spawn_v4_receiver_once(data.path());
    let mut session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let (report, profile) = sender::send_dump_profile(
        &mut session.control_stream,
        &mut session.data_stream,
        source.path().to_path_buf(),
        dump_id,
        options(8),
    )
    .unwrap();
    let received = receiver.join().unwrap().unwrap();

    assert_eq!(report.terminal_skip_count, 0);
    assert_eq!(
        profile.bundled_entries,
        expected.len() as u64 - committed_files
    );
    assert_eq!(report.final_bundle_no, 1 + report.bundles_sent as u64);
    assert_eq!(report.final_work_ordinal, expected.len() as u64);
    assert_eq!(
        received.resume_state.last_committed_work_ordinal,
        expected.len() as u64
    );
    assert!(received.resume_state.terminal_skips.is_empty());
    assert_eq!(read_archive_entries(&received.final_archive_path), expected);
}
