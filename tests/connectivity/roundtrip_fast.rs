use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::thread;

use crate::support::*;
use fast_dump::network::transport::BoundListener;
use fast_dump::role::Role;
use fast_dump::sender::{BundlePlanOptions, send_dump};

const BOUNDARY_SMALL_FILE_COUNT: u64 = 100;
const BOUNDARY_FRAME_TARGET_MB: u64 = 2;
const V4_DATA_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

fn boundary_sender_options() -> SenderBinaryOptions {
    SenderBinaryOptions {
        level: 1,
        threads: 1,
        min_readers: 2,
        max_readers: 8,
        read_buffer_kb: 1024,
        frame_target_mb: BOUNDARY_FRAME_TARGET_MB,
    }
}

fn insert_boundary_triplet(
    expected: &mut BTreeMap<String, FileHash>,
    root: &Path,
    group: &str,
    boundary: u64,
    seed: &mut u64,
) {
    for (suffix, size) in [
        ("minus-1", boundary - 1),
        ("exact", boundary),
        ("plus-1", boundary + 1),
    ] {
        insert_expected_random_file(
            expected,
            root,
            &format!("boundaries/{group}/{suffix}.bin"),
            size,
            *seed,
        );
        *seed += 1;
    }
}

fn build_boundary_fixture(root: &Path) -> BTreeMap<String, FileHash> {
    let mut expected = BTreeMap::new();
    let mut seed = 1_000_u64;

    insert_expected_file(&mut expected, root, "special/empty.bin", b"");

    for index in 0..BOUNDARY_SMALL_FILE_COUNT {
        let size = 17 + ((index * 37) % 3072);
        insert_expected_random_file(
            &mut expected,
            root,
            &format!("small/{:02}/file-{index:03}.bin", index % 10),
            size,
            seed,
        );
        seed += 1;
    }

    let frame_target_bytes = BOUNDARY_FRAME_TARGET_MB * 1024 * 1024;
    for (group, boundary) in [
        ("tar-block", 512_u64),
        ("page", 4 * 1024),
        ("data-chunk", V4_DATA_CHUNK_BYTES),
        ("bundle-target", frame_target_bytes),
    ] {
        insert_boundary_triplet(&mut expected, root, group, boundary, &mut seed);
    }

    insert_expected_random_file(
        &mut expected,
        root,
        "large/over-16x-bundle.bin",
        frame_target_bytes * 16,
        seed,
    );

    expected
}

#[test]
fn sender_and_receiver_binaries_roundtrip_boundary_fixture_and_verify_hashes() {
    let source_dir = TestDir::new();
    let expected = build_boundary_fixture(source_dir.path());
    let dump_id = "dump-binary-boundary-fixture";

    assert_eq!(expected.len(), 114);

    let result = run_push_roundtrip_via_binaries_with_sender_options(
        source_dir.path(),
        dump_id,
        boundary_sender_options(),
        false,
    );

    assert!(
        result.sender_output.status.success(),
        "status={:?}, stderr={}",
        result.sender_output.status.code(),
        String::from_utf8_lossy(&result.sender_output.stderr)
    );

    let sender_stdout = String::from_utf8_lossy(&result.sender_output.stdout);
    assert!(
        sender_stdout.contains(&format!("dump_id={dump_id}")),
        "{sender_stdout}"
    );
    assert!(sender_stdout.contains("status=clean"), "{sender_stdout}");

    assert_archive_hashes(&result.archive_path, &expected);
}

#[test]
#[ignore = "large file-count and streamed-payload stress; run explicitly or in scheduled CI"]
fn sender_and_receiver_roundtrip_random_files_and_verify_hashes() {
    let source_dir = TestDir::new();
    let mut expected = build_boundary_fixture(source_dir.path());
    // Мелкие файлы нагружают обход/очереди; большие проверяют streaming и смену bundles.
    for index in 0..10_000_u64 {
        insert_expected_random_file(
            &mut expected,
            source_dir.path(),
            &format!("stress/{:03}/file-{index:05}.bin", index % 100),
            1 + (index * 37) % 8192,
            20_000 + index,
        );
    }
    for (index, size) in [32 * 1024 * 1024, 64 * 1024 * 1024].into_iter().enumerate() {
        insert_expected_random_file(
            &mut expected,
            source_dir.path(),
            &format!("stress-large/{index}.bin"),
            size,
            40_000 + index as u64,
        );
    }
    assert_eq!(expected.len(), 10_116);
    let result = run_push_roundtrip_via_binaries_with_sender_options(
        source_dir.path(),
        "dump-random-stress",
        boundary_sender_options(),
        false,
    );
    assert!(
        result.sender_output.status.success(),
        "status={:?}, stderr={}",
        result.sender_output.status.code(),
        String::from_utf8_lossy(&result.sender_output.stderr)
    );
    let stdout = String::from_utf8_lossy(&result.sender_output.stdout);
    assert!(stdout.contains("status=clean"), "{stdout}");
    assert_archive_hashes(&result.archive_path, &expected);
}

#[test]
fn receiver_binary_roundtrips_source_into_valid_archive_in_pull_mode() {
    let source_dir = TestDir::new();
    fs::create_dir_all(source_dir.path().join("nested")).unwrap();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("nested").join("beta.txt"), b"beta").unwrap();

    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let sender_path = source_dir.path().to_path_buf();
    let receiver_dir = TestDir::new();
    let final_archive_path = receiver_dir.path().join("dump-04.tar.zst");

    let sender = thread::spawn(move || {
        let mut session = listener
            .accept(Role::Sender, Some("dump-04"), None)
            .unwrap();
        send_dump(
            &mut session.control_stream,
            &mut session.data_stream,
            sender_path,
            "dump-04",
            BundlePlanOptions {
                bundle_target_bytes: 1,
                compression_level: 3,
                ..Default::default()
            },
        )
        .unwrap()
    });

    let output = Command::new(binary_path())
        .args([
            "receiver",
            "--connect",
            &addr.to_string(),
            "--data-dir",
            receiver_dir.path().to_str().unwrap(),
            "--max-sessions",
            "1",
        ])
        .output()
        .unwrap();

    let report = sender.join().unwrap();

    assert!(
        output.status.success(),
        "status={:?}, stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("dump_id=dump-04"), "{stdout}");
    assert!(stdout.contains("final_bundle=2"), "{stdout}");
    assert!(stdout.contains("final_work_ordinal=2"), "{stdout}");
    assert_eq!(report.final_bundle_no, 2);
    assert_eq!(
        archive_entries(&final_archive_path),
        sample_archive_entries()
    );
}

#[cfg(windows)]
#[test]
fn sender_and_receiver_binaries_persist_issue_journal_for_terminal_skip() {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    let source_dir = TestDir::new();
    let dump_id = "dump-binary-issue-journal";
    let mut expected = BTreeMap::new();
    insert_expected_file(&mut expected, source_dir.path(), "alpha.txt", b"alpha");
    insert_expected_file(&mut expected, source_dir.path(), "nested/beta.txt", b"beta");

    let locked_path = source_dir.path().join("locked.bin");
    fs::write(&locked_path, b"locked").unwrap();
    let lock_handle = OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&locked_path)
        .unwrap();

    let result = run_push_roundtrip_via_binaries(source_dir.path(), dump_id, 64, false);
    drop(lock_handle);

    assert!(
        result.sender_output.status.success(),
        "status={:?}, stderr={}",
        result.sender_output.status.code(),
        String::from_utf8_lossy(&result.sender_output.stderr)
    );

    let sender_stdout = String::from_utf8_lossy(&result.sender_output.stdout);
    assert!(sender_stdout.contains("status=completed_with_warnings"));
    assert!(sender_stdout.contains("files_skipped=1"));

    assert_archive_hashes(&result.archive_path, &expected);

    let issue_journal_path = result
        .archive_path
        .parent()
        .unwrap()
        .join(format!("{dump_id}.tar.zst.issues.tsv"));
    let issue_journal = fs::read_to_string(&issue_journal_path).unwrap();

    assert!(issue_journal_path.exists());
    assert!(issue_journal.contains("work_ordinal\treason\trelative_path"));
    assert!(issue_journal.contains("\topen_failed\tlocked.bin"));
}
