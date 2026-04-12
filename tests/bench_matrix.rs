#![allow(dead_code)]

mod pipeline {
    pub mod results {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/benches/pipeline/results.rs"
        ));
    }

    pub mod shared {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/benches/pipeline/shared.rs"
        ));
    }

    pub mod stats {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/benches/pipeline/stats.rs"
        ));
    }

    pub mod loopback {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/benches/pipeline/loopback.rs"
        ));
    }
}

use pipeline::loopback::resolve_dataset_names;
use pipeline::results::{
    RawResultRow, ScenarioKey, aggregate_summary_rows, append_raw_result, load_raw_results,
};
use pipeline::shared::BenchDir;

fn sample_row(repeat_index: u32, total_duration_ms: f64, sent_frames: u64) -> RawResultRow {
    RawResultRow {
        scenario: ScenarioKey {
            dataset: "mixed".to_owned(),
            protocol_runtime: "v3".to_owned(),
            network_profile: "wan-high-rtt".to_owned(),
            executor: "default".to_owned(),
            threads: 4,
            frame_target_mb: 16,
            read_buffer_kb: 256,
            manifest_chunk_kb: 256,
            manifest_ack_records: 4096,
            manifest_ack_kb: 1024,
            large_file_single_frame_threshold_mib: 16,
        },
        repeat_index,
        files_count: 1024,
        skipped_files: 3,
        sent_frames,
        frame_aborts: 2,
        frame_rebuilds: 2,
        avg_files_per_frame: 85.0 + repeat_index as f64,
        raw_read_bytes: 10_000_000,
        tar_input_bytes: 10_500_000,
        compressed_payload_bytes: 7_000_000,
        wire_bytes: 7_050_000,
        wire_overhead_bytes: 50_000,
        read_duration_ms: 100.0 + repeat_index as f64,
        compress_duration_ms: 200.0 + repeat_index as f64,
        send_duration_ms: 300.0 + repeat_index as f64,
        total_duration_ms,
        traversal_wait_ms: 20.0 + repeat_index as f64,
        ack_wait_ms: 30.0 + repeat_index as f64,
        flush_count: 0,
        max_inflight_dirs: 6,
        max_dirfd_depth: 4,
        read_mib_s: 12.5,
        compress_mib_s: 23.5,
        send_mib_s: 34.5,
        total_mib_s: 45.5,
        avg_cpu_pct: 55.5,
        max_cpu_pct: 66.5,
        avg_rss_mib: 77.5,
        max_rss_mib: 88.5,
        avg_private_mib: 99.5,
        max_private_mib: 111.5,
        compression_ratio_pct: 30.0,
        wire_pct_of_read: 70.5,
        wire_overhead_pct: 0.5,
        manifest_build_bytes: 120_000,
        manifest_build_duration_ms: 41.0 + repeat_index as f64,
        manifest_transfer_bytes: 98_000,
        manifest_transfer_duration_ms: 51.0 + repeat_index as f64,
        manifest_spool_read_bytes: 97_000,
        manifest_spool_read_duration_ms: 61.0 + repeat_index as f64,
        manifest_replay_bytes: 12_000,
        manifest_replay_duration_ms: 71.0 + repeat_index as f64,
        manifest_replay_records: 20 + repeat_index as u64,
        receiver_manifest_bytes: 99_000,
        receiver_issue_tail_bytes: 123,
        receiver_total_duration_ms: 111.0 + repeat_index as f64,
        receiver_manifest_stage_duration_ms: 12.0 + repeat_index as f64,
        receiver_manifest_commit_duration_ms: 13.0 + repeat_index as f64,
        receiver_manifest_ack_flush_duration_ms: 14.0 + repeat_index as f64,
        receiver_frame_duration_ms: 15.0 + repeat_index as f64,
        receiver_frame_write_duration_ms: 16.0 + repeat_index as f64,
        receiver_frame_sync_duration_ms: 17.0 + repeat_index as f64,
        receiver_frame_journal_persist_duration_ms: 18.0 + repeat_index as f64,
        receiver_frame_ack_flush_duration_ms: 19.0 + repeat_index as f64,
        receiver_issue_tail_duration_ms: 20.0 + repeat_index as f64,
        receiver_finish_duration_ms: 21.0 + repeat_index as f64,
        receiver_archive_finalize_duration_ms: 22.0 + repeat_index as f64,
        receiver_issue_finalize_duration_ms: 23.0 + repeat_index as f64,
        receiver_finish_ack_flush_duration_ms: 24.0 + repeat_index as f64,
        disk_budget_wait_ms: 81.0 + repeat_index as f64,
        time_to_first_frame_ms: 91.0 + repeat_index as f64,
        overlap_efficiency_pct: 42.0 + repeat_index as f64,
    }
}

#[test]
fn resolve_dataset_names_defaults_to_all_supported_cases() {
    let names = resolve_dataset_names(&[]).unwrap();

    assert_eq!(names, vec!["manySmallFiles", "bigFiles", "mixed"]);
}

#[test]
fn resolve_dataset_names_rejects_unknown_case() {
    let error = resolve_dataset_names(&["weird".to_owned()]).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("unsupported dataset `weird`; currently implemented datasets")
    );
}

#[test]
fn raw_results_roundtrip_preserves_duration_and_wire_metrics() {
    let bench_dir = BenchDir::new("bench-matrix-results");
    let raw_path = bench_dir.path().join("results_raw.csv");
    let row = sample_row(1, 456.0, 9);

    append_raw_result(&raw_path, &row).unwrap();

    let loaded = load_raw_results(&raw_path).unwrap();
    assert_eq!(loaded.len(), 1);

    let parsed = &loaded[0];
    assert_eq!(parsed.scenario.protocol_runtime, "v3");
    assert_eq!(parsed.scenario.network_profile, "wan-high-rtt");
    assert_eq!(parsed.skipped_files, 3);
    assert_eq!(parsed.sent_frames, 9);
    assert_eq!(parsed.frame_aborts, 2);
    assert_eq!(parsed.frame_rebuilds, 2);
    assert_eq!(parsed.avg_files_per_frame, 86.0);
    assert_eq!(parsed.wire_overhead_bytes, 50_000);
    assert_eq!(parsed.read_duration_ms, 101.0);
    assert_eq!(parsed.compress_duration_ms, 201.0);
    assert_eq!(parsed.send_duration_ms, 301.0);
    assert_eq!(parsed.total_duration_ms, 456.0);
    assert_eq!(parsed.traversal_wait_ms, 21.0);
    assert_eq!(parsed.ack_wait_ms, 31.0);
    assert_eq!(parsed.flush_count, 0);
    assert_eq!(parsed.max_inflight_dirs, 6);
    assert_eq!(parsed.max_dirfd_depth, 4);
    assert_eq!(parsed.wire_overhead_pct, 0.5);
    assert_eq!(parsed.manifest_build_bytes, 120_000);
    assert_eq!(parsed.manifest_build_duration_ms, 42.0);
    assert_eq!(parsed.manifest_transfer_bytes, 98_000);
    assert_eq!(parsed.manifest_transfer_duration_ms, 52.0);
    assert_eq!(parsed.manifest_spool_read_bytes, 97_000);
    assert_eq!(parsed.manifest_spool_read_duration_ms, 62.0);
    assert_eq!(parsed.manifest_replay_bytes, 12_000);
    assert_eq!(parsed.manifest_replay_duration_ms, 72.0);
    assert_eq!(parsed.manifest_replay_records, 21);
    assert_eq!(parsed.receiver_manifest_bytes, 99_000);
    assert_eq!(parsed.receiver_issue_tail_bytes, 123);
    assert_eq!(parsed.receiver_total_duration_ms, 112.0);
    assert_eq!(parsed.receiver_manifest_stage_duration_ms, 13.0);
    assert_eq!(parsed.receiver_manifest_commit_duration_ms, 14.0);
    assert_eq!(parsed.receiver_manifest_ack_flush_duration_ms, 15.0);
    assert_eq!(parsed.receiver_frame_duration_ms, 16.0);
    assert_eq!(parsed.receiver_frame_write_duration_ms, 17.0);
    assert_eq!(parsed.receiver_frame_sync_duration_ms, 18.0);
    assert_eq!(parsed.receiver_frame_journal_persist_duration_ms, 19.0);
    assert_eq!(parsed.receiver_frame_ack_flush_duration_ms, 20.0);
    assert_eq!(parsed.receiver_issue_tail_duration_ms, 21.0);
    assert_eq!(parsed.receiver_finish_duration_ms, 22.0);
    assert_eq!(parsed.receiver_archive_finalize_duration_ms, 23.0);
    assert_eq!(parsed.receiver_issue_finalize_duration_ms, 24.0);
    assert_eq!(parsed.receiver_finish_ack_flush_duration_ms, 25.0);
    assert_eq!(parsed.disk_budget_wait_ms, 82.0);
    assert_eq!(parsed.time_to_first_frame_ms, 92.0);
    assert_eq!(parsed.overlap_efficiency_pct, 43.0);
    assert_eq!(parsed.scenario.manifest_chunk_kb, 256);
    assert_eq!(parsed.scenario.manifest_ack_records, 4096);
    assert_eq!(parsed.scenario.manifest_ack_kb, 1024);
}

#[test]
fn aggregate_summary_rows_averages_new_schema_fields() {
    let rows = vec![sample_row(1, 400.0, 10), sample_row(2, 600.0, 14)];

    let summaries = aggregate_summary_rows(&rows);
    assert_eq!(summaries.len(), 1);

    let summary = &summaries[0];
    assert_eq!(summary.scenario.protocol_runtime, "v3");
    assert_eq!(summary.scenario.network_profile, "wan-high-rtt");
    assert_eq!(summary.skipped_files, 3);
    assert_eq!(summary.sent_frames, 12);
    assert_eq!(summary.frame_aborts, 2);
    assert_eq!(summary.frame_rebuilds, 2);
    assert_eq!(summary.avg_files_per_frame, 86.5);
    assert_eq!(summary.wire_overhead_bytes, 50_000);
    assert_eq!(summary.read_duration_ms, 101.5);
    assert_eq!(summary.compress_duration_ms, 201.5);
    assert_eq!(summary.send_duration_ms, 301.5);
    assert_eq!(summary.total_duration_ms, 500.0);
    assert_eq!(summary.traversal_wait_ms, 21.5);
    assert_eq!(summary.ack_wait_ms, 31.5);
    assert_eq!(summary.flush_count, 0);
    assert_eq!(summary.max_inflight_dirs, 6);
    assert_eq!(summary.max_dirfd_depth, 4);
    assert_eq!(summary.wire_overhead_pct, 0.5);
    assert_eq!(summary.manifest_build_bytes, 120_000);
    assert_eq!(summary.manifest_build_duration_ms, 42.5);
    assert_eq!(summary.manifest_transfer_bytes, 98_000);
    assert_eq!(summary.manifest_transfer_duration_ms, 52.5);
    assert_eq!(summary.manifest_spool_read_bytes, 97_000);
    assert_eq!(summary.manifest_spool_read_duration_ms, 62.5);
    assert_eq!(summary.manifest_replay_bytes, 12_000);
    assert_eq!(summary.manifest_replay_duration_ms, 72.5);
    assert_eq!(summary.manifest_replay_records, 21);
    assert_eq!(summary.receiver_manifest_bytes, 99_000);
    assert_eq!(summary.receiver_issue_tail_bytes, 123);
    assert_eq!(summary.receiver_total_duration_ms, 112.5);
    assert_eq!(summary.receiver_manifest_stage_duration_ms, 13.5);
    assert_eq!(summary.receiver_manifest_commit_duration_ms, 14.5);
    assert_eq!(summary.receiver_manifest_ack_flush_duration_ms, 15.5);
    assert_eq!(summary.receiver_frame_duration_ms, 16.5);
    assert_eq!(summary.receiver_frame_write_duration_ms, 17.5);
    assert_eq!(summary.receiver_frame_sync_duration_ms, 18.5);
    assert_eq!(summary.receiver_frame_journal_persist_duration_ms, 19.5);
    assert_eq!(summary.receiver_frame_ack_flush_duration_ms, 20.5);
    assert_eq!(summary.receiver_issue_tail_duration_ms, 21.5);
    assert_eq!(summary.receiver_finish_duration_ms, 22.5);
    assert_eq!(summary.receiver_archive_finalize_duration_ms, 23.5);
    assert_eq!(summary.receiver_issue_finalize_duration_ms, 24.5);
    assert_eq!(summary.receiver_finish_ack_flush_duration_ms, 25.5);
    assert_eq!(summary.disk_budget_wait_ms, 82.5);
    assert_eq!(summary.time_to_first_frame_ms, 92.5);
    assert_eq!(summary.overlap_efficiency_pct, 43.5);
    assert_eq!(summary.scenario.manifest_chunk_kb, 256);
    assert_eq!(summary.scenario.manifest_ack_records, 4096);
    assert_eq!(summary.scenario.manifest_ack_kb, 1024);
}
