use std::fs;
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
            sender_stdout.contains("discovery_issues_total=0"),
            "case={label} sender_stdout={sender_stdout}"
        );
        assert!(
            sender_stdout.contains("subtrees_skipped=0"),
            "case={label} sender_stdout={sender_stdout}"
        );
        assert!(
            sender_stdout.contains("manifest_resets_total=0"),
            "case={label} sender_stdout={sender_stdout}"
        );
        assert_eq!(
            sender_stderr.contains("sender starting:"),
            debug,
            "case={label} sender_stderr={sender_stderr}"
        );
        assert_eq!(
            sender_stderr.contains("sender v3 dump session bootstrapping:"),
            debug,
            "case={label} sender_stderr={sender_stderr}"
        );
        assert_eq!(
            sender_stderr.contains("sender v3 frame acknowledged:"),
            debug,
            "case={label} sender_stderr={sender_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("receiver starting in listen mode:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("receiver v3 dump session ready:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("receiver v3 frame committed:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
    }
}

#[test]
fn v3_profile_reports_manifest_overlap_and_durable_boundaries() {
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

    let dump_id = "dump-v3-observability";
    let mut options = default_send_options(1);
    options.manifest_chunk_bytes = 64;
    options.manifest_ack_records = 1;
    options.manifest_ack_bytes = 64;

    let result = run_push_roundtrip_with_options(source_dir.path(), dump_id, options);

    assert_archive_hashes(&result.receive_report.final_archive_path, &expected);
    assert_eq!(result.sender_report.sent_frames, 3);
    assert_eq!(result.sender_report.final_file_index, 3);
    assert_eq!(result.receive_report.finish.final_work_ordinal, Some(3));
    assert!(result.sender_profile.manifest_build.bytes > 0);
    assert_eq!(result.sender_profile.manifest_spool_read.bytes, 0);
    assert_eq!(result.sender_profile.manifest_replay.bytes, 0);
    assert!(result.sender_profile.ack_wait.duration >= Duration::ZERO);
    assert!(result.receive_report.state.journal.manifest_sealed);
    assert_eq!(
        result
            .receive_report
            .state
            .journal
            .last_manifest_work_ordinal,
        result.receive_report.finish.final_work_ordinal
    );
    assert_eq!(
        result
            .receive_report
            .state
            .journal
            .last_committed_work_ordinal,
        result.receive_report.finish.final_work_ordinal
    );
    assert!(result.receive_report.state.journal.manifest_bytes_persisted > 0);
}
