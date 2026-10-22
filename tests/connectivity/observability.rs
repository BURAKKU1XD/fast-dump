use std::fs;
use std::process::Command;
use std::time::Duration;

use crate::support::*;

#[test]
fn debug_flag_toggles_sender_and_receiver_logs() {
    for (label, dump_id, debug) in [
        ("debug-enabled", "dump-debug-enabled", true),
        ("debug-disabled", "dump-debug-disabled", false),
    ] {
        let source_dir = TestDir::new();
        fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
        fs::create_dir_all(source_dir.path().join("nested")).unwrap();
        fs::write(source_dir.path().join("nested").join("beta.txt"), b"beta").unwrap();

        let result = run_push_roundtrip_via_binaries(source_dir.path(), dump_id, 64, debug);

        assert!(
            result.sender_output.status.success(),
            "case={label} status={:?}, stderr={}",
            result.sender_output.status.code(),
            String::from_utf8_lossy(&result.sender_output.stderr)
        );
        assert!(result.archive_path.exists(), "case={label}");

        let sender_stdout = String::from_utf8_lossy(&result.sender_output.stdout);
        let sender_stderr = String::from_utf8_lossy(&result.sender_output.stderr);
        let receiver_stderr = String::from_utf8_lossy(&result.receiver_output.stderr);

        assert!(
            sender_stdout.contains("files_skipped=0"),
            "case={label} sender_stdout={sender_stdout}"
        );
        assert!(
            sender_stdout.contains("bundles_sent="),
            "case={label} sender_stdout={sender_stdout}"
        );
        assert!(
            sender_stdout.contains("final_bundle="),
            "case={label} sender_stdout={sender_stdout}"
        );
        assert_eq!(
            sender_stderr.contains("sender starting:"),
            debug,
            "case={label} sender_stderr={sender_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("receiver starting in listen mode:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("v4 receiver listener entering serve loop:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
    }
}

#[test]
fn v4_profiles_report_bundle_and_resume_boundaries() {
    let _env = EnvVarGuard::set(&[]);
    let source_dir = TestDir::new();
    let mut expected = std::collections::BTreeMap::new();
    insert_expected_random_file(&mut expected, source_dir.path(), "alpha.bin", 8 * 1024, 1);
    insert_expected_random_file(
        &mut expected,
        source_dir.path(),
        "nested/beta.bin",
        8 * 1024,
        2,
    );
    insert_expected_random_file(
        &mut expected,
        source_dir.path(),
        "nested/gamma.bin",
        8 * 1024,
        3,
    );

    let dump_id = "dump-v4-observability";
    let result =
        run_push_roundtrip_with_options(source_dir.path(), dump_id, default_send_options(1));

    assert_archive_hashes(&result.receive_report.final_archive_path, &expected);
    assert_eq!(result.sender_report.bundles_sent, 3);
    assert_eq!(result.sender_report.final_work_ordinal, 3);
    assert!(result.sender_profile.scan.duration >= Duration::ZERO);
    assert!(result.sender_profile.read.bytes > 0);
    assert!(result.sender_profile.compress.duration >= Duration::ZERO);
    assert!(result.sender_profile.ack_wait.duration >= Duration::ZERO);
    assert_eq!(result.sender_profile.bundled_entries, 3);
    assert!(result.receive_report.final_archive_path.exists());
    assert_eq!(result.receive_profile.committed_bundles, 3);
    assert!(result.receive_profile.bundle_commit.duration >= Duration::ZERO);
    assert!(result.receive_profile.bundle_ack_flush.duration >= Duration::ZERO);
    assert!(result.receive_profile.finalize_duration >= Duration::ZERO);
}

#[test]
fn sender_binary_rejects_invalid_runtime_bounds_before_connect() {
    let source_dir = TestDir::new();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();

    for (label, extra_args, expected_fragment) in [
        (
            "threads-zero",
            vec!["--threads", "0"],
            "threads must be at least 1",
        ),
        (
            "min-readers-zero",
            vec!["--min-readers", "0"],
            "min_readers must be at least 1",
        ),
        (
            "max-readers-zero",
            vec!["--max-readers", "0"],
            "max_readers must be at least 1",
        ),
        (
            "reader-bounds-inverted",
            vec!["--min-readers", "5", "--max-readers", "4"],
            "min_readers must not be greater than max_readers",
        ),
        (
            "read-buffer-zero",
            vec!["--read-buffer-kb", "0"],
            "read_buffer_kb must be at least 1",
        ),
        (
            "frame-target-zero",
            vec!["--frame-target-mb", "0"],
            "frame_target_mb must be at least 1",
        ),
    ] {
        let output = Command::new(binary_path())
            .args([
                "sender",
                "--connect",
                "127.0.0.1:9",
                "--source",
                source_dir.path().to_str().unwrap(),
            ])
            .args(extra_args)
            .output()
            .unwrap();

        assert!(
            !output.status.success(),
            "case={label} stdout={}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected_fragment),
            "case={label} stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
