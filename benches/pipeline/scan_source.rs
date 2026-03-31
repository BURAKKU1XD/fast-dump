use criterion::{Criterion, Throughput, black_box};
use fast_dump::sender::scan_source;

use super::shared::{
    BenchDir, MAX_SMALL_FILE_DEPTH, PREPARE_OVERSIZED_FILE_BYTES, PREPARE_SMALL_FILE_COUNT,
    create_nested_small_files_fixture_set, create_single_large_file_fixture, empty_resume_state,
    scan_group_sample_size,
};

pub(crate) fn bench_scan_source(c: &mut Criterion) {
    let nested_small_dir = BenchDir::new("scan-nested-small");
    let nested_small_bytes = create_nested_small_files_fixture_set(
        nested_small_dir.path(),
        PREPARE_SMALL_FILE_COUNT,
        MAX_SMALL_FILE_DEPTH,
    );
    let oversized_dir = BenchDir::new("scan-oversized-single");
    let oversized_bytes =
        create_single_large_file_fixture(oversized_dir.path(), PREPARE_OVERSIZED_FILE_BYTES);

    let mut group = c.benchmark_group("scan_source");
    group.sample_size(scan_group_sample_size());

    group.throughput(Throughput::Bytes(nested_small_bytes));
    group.bench_function("nested_small_files_40000_depth7", |b| {
        b.iter(|| {
            let report =
                scan_source(nested_small_dir.path().to_path_buf(), &empty_resume_state()).unwrap();
            black_box(report.pending_files);
            black_box(report.pending_bytes);
            black_box(report.final_file_index);
        });
    });

    group.throughput(Throughput::Bytes(oversized_bytes));
    group.bench_function("oversized_single_128mib", |b| {
        b.iter(|| {
            let report =
                scan_source(oversized_dir.path().to_path_buf(), &empty_resume_state()).unwrap();
            black_box(report.pending_files);
            black_box(report.pending_bytes);
            black_box(report.final_file_index);
        });
    });

    group.finish();
}
