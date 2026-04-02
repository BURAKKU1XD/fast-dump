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
            threads: 4,
            frame_target_mb: 16,
            read_buffer_kb: 256,
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
    assert_eq!(parsed.wire_overhead_pct, 0.5);
}

#[test]
fn aggregate_summary_rows_averages_new_schema_fields() {
    let rows = vec![sample_row(1, 400.0, 10), sample_row(2, 600.0, 14)];

    let summaries = aggregate_summary_rows(&rows);
    assert_eq!(summaries.len(), 1);

    let summary = &summaries[0];
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
    assert_eq!(summary.wire_overhead_pct, 0.5);
}
