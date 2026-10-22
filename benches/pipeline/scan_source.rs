use criterion::{Criterion, Throughput, black_box};
use fast_dump::sender::walk_source;

use super::shared::{
    BenchDir, MAX_SMALL_FILE_DEPTH, PREPARE_OVERSIZED_FILE_BYTES, PREPARE_SMALL_FILE_COUNT,
    create_nested_small_files_fixture_set, create_single_large_file_fixture,
    scan_group_sample_size,
};

pub(crate) fn bench_scan_source(c: &mut Criterion) {
    let nested_small_dir = BenchDir::new("scan-nested-small");
    create_nested_small_files_fixture_set(
        nested_small_dir.path(),
        PREPARE_SMALL_FILE_COUNT,
        MAX_SMALL_FILE_DEPTH,
    );
    let oversized_dir = BenchDir::new("scan-oversized-single");
    create_single_large_file_fixture(oversized_dir.path(), PREPARE_OVERSIZED_FILE_BYTES);

    // Измеряем runtime walk, а не старый scan_source с metadata для каждого файла.
    let mut group = c.benchmark_group("scan_source_walk");
    group.sample_size(scan_group_sample_size());

    group.throughput(Throughput::Elements(PREPARE_SMALL_FILE_COUNT as u64));
    group.bench_function("nested_small_files_40000_depth7", |b| {
        b.iter(|| {
            black_box(walk_source(nested_small_dir.path()).unwrap());
        });
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("oversized_single_128mib", |b| {
        b.iter(|| {
            black_box(walk_source(oversized_dir.path()).unwrap());
        });
    });

    group.finish();
}
