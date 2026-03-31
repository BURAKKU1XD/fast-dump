use std::fs;

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

        let sender_stderr = String::from_utf8_lossy(&result.sender_output.stderr);
        let receiver_stderr = String::from_utf8_lossy(&result.receiver_output.stderr);

        assert_eq!(
            sender_stderr.contains("sender starting:"),
            debug,
            "case={label} sender_stderr={sender_stderr}"
        );
        assert_eq!(
            sender_stderr.contains("sender dump session bootstrapping:"),
            debug,
            "case={label} sender_stderr={sender_stderr}"
        );
        assert_eq!(
            sender_stderr.contains("sender frame acknowledged:"),
            debug,
            "case={label} sender_stderr={sender_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("receiver starting in listen mode:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("receiver dump session ready:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
        assert_eq!(
            receiver_stderr.contains("receiver frame committed:"),
            debug,
            "case={label} receiver_stderr={receiver_stderr}"
        );
    }
}
