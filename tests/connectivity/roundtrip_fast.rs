use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::thread;

use crate::support::*;
use fast_dump::network::protocol::MAX_FRAME_CHUNK_BYTES;
use fast_dump::network::transport::BoundListener;
use fast_dump::role::Role;
use fast_dump::sender::{SendOptions, send_dump};

const BOUNDARY_SMALL_FILE_COUNT: u64 = 100;
const BOUNDARY_READ_BUFFER_KB: usize = 64;
const BOUNDARY_MIN_READERS: usize = 2;
const BOUNDARY_MAX_READERS: usize = 4;
const BOUNDARY_FRAME_TARGET_MB: u64 = 2;

fn boundary_sender_options() -> SenderBinaryOptions {
    SenderBinaryOptions {
        level: 1,
        threads: 1,
        min_readers: BOUNDARY_MIN_READERS,
        max_readers: BOUNDARY_MAX_READERS,
        read_buffer_kb: BOUNDARY_READ_BUFFER_KB,
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

    // Один пустой файл нужен как дешёвая проверка нулевого payload в tar/zstd пути.
    insert_expected_file(&mut expected, root, "special/empty.bin", b"");

    // Набор из 100 мелких файлов держит нагрузку на reader pool и проверяет, что
    // archive не теряет ни один entry при частых переключениях между файлами.
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

    let read_buffer_bytes = (BOUNDARY_READ_BUFFER_KB as u64) * 1024;
    let reader_window_bytes = read_buffer_bytes * BOUNDARY_MAX_READERS as u64;
    let frame_target_bytes = BOUNDARY_FRAME_TARGET_MB * 1024 * 1024;

    // Здесь собираем n-1 / n / n+1 вокруг ключевых byte-boundary sender pipeline:
    // tar block, page-size, read buffer, агрегированное окно reader pool,
    // протокольный chunk limit и frame target.
    for (group, boundary) in [
        ("tar-block", 512_u64),
        ("page", 4 * 1024),
        ("read-buffer", read_buffer_bytes),
        ("reader-window", reader_window_bytes),
        ("frame-chunk", MAX_FRAME_CHUNK_BYTES as u64),
        ("frame-target", frame_target_bytes),
    ] {
        insert_boundary_triplet(&mut expected, root, group, boundary, &mut seed);
    }

    // Один файл заметно больше frame target проверяет, что sender не режет файл
    // посередине логической границы и receiver получает валидный итоговый архив.
    insert_expected_random_file(
        &mut expected,
        root,
        "large/over-16x-frame.bin",
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

    assert_eq!(expected.len(), 120);

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

    let receiver_stdout = String::from_utf8_lossy(&result.receiver_output.stdout);
    assert!(
        receiver_stdout.contains(&format!("dump_id={dump_id}")),
        "stdout={receiver_stdout}"
    );

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
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 1,
            min_readers: 2,
            max_readers: 8,
            read_buffer_bytes: 1024 * 1024,
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
        };
        send_dump(&mut session.stream, sender_path, "dump-04", &options).unwrap()
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
    assert!(stdout.contains("resume frame=0"), "{stdout}");
    assert!(stdout.contains("final frame=2"), "{stdout}");
    assert_eq!(report.final_frame_no, 2);
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
    assert!(sender_stdout.contains("files_skipped_open_failed=1"));
    assert!(sender_stdout.contains("discovery_issues_total=0"));
    assert!(sender_stdout.contains("subtrees_skipped=0"));
    assert!(sender_stdout.contains("manifest_resets_total=0"));

    assert_archive_hashes(&result.archive_path, &expected);

    let issue_journal_path = result
        .archive_path
        .parent()
        .unwrap()
        .join(format!("{dump_id}.tar.zst.issues.tsv"));
    let issue_journal = fs::read_to_string(&issue_journal_path).unwrap();

    assert!(issue_journal_path.exists());
    assert!(issue_journal.contains("file_index\tattempts\treason\tfile_path\tlast_error"));
    assert!(issue_journal.contains("open_failed\tlocked.bin"));
}
