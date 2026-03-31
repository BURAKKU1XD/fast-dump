use criterion::{Criterion, Throughput, black_box};
use fast_dump::sender::prepare_dump;

use super::shared::{
    BenchDir, MAX_SMALL_FILE_DEPTH, PREPARE_FRAME_TARGET_BYTES, PREPARE_OVERSIZED_FILE_BYTES,
    PREPARE_SMALL_FILE_COUNT, create_nested_small_files_fixture_set,
    create_single_large_file_fixture, empty_resume_state, prepare_group_sample_size,
    prepare_profile_sample_count,
};
use super::stats::{collect_profile_samples, print_profile_summary, sample_prepare_dump_profile};

pub(crate) fn bench_prepare_dump(c: &mut Criterion) {
    let nested_small_dir = BenchDir::new("prepare-nested-small");
    let nested_small_bytes = create_nested_small_files_fixture_set(
        nested_small_dir.path(),
        PREPARE_SMALL_FILE_COUNT,
        MAX_SMALL_FILE_DEPTH,
    );
    let oversized_dir = BenchDir::new("prepare-oversized-single");
    let oversized_bytes =
        create_single_large_file_fixture(oversized_dir.path(), PREPARE_OVERSIZED_FILE_BYTES);

    let nested_small_profiles = collect_profile_samples(prepare_profile_sample_count(), || {
        sample_prepare_dump_profile(nested_small_dir.path(), 8 * 1024 * 1024)
    });
    print_profile_summary(
        "prepare_dump/nested_small_files_40000_depth7",
        &nested_small_profiles,
    );

    let oversized_profiles = collect_profile_samples(prepare_profile_sample_count(), || {
        sample_prepare_dump_profile(oversized_dir.path(), PREPARE_FRAME_TARGET_BYTES)
    });
    print_profile_summary(
        "prepare_dump/oversized_single_128mib_frame64",
        &oversized_profiles,
    );

    let mut group = c.benchmark_group("prepare_dump");
    group.sample_size(prepare_group_sample_size());

    group.throughput(Throughput::Bytes(nested_small_bytes));
    group.bench_function("nested_small_files_40000_depth7", |b| {
        b.iter(|| {
            let dump = prepare_dump(
                nested_small_dir.path().to_path_buf(),
                &empty_resume_state(),
                3,
                1,
                8 * 1024 * 1024,
            )
            .unwrap();
            black_box(dump.frames.len());
            black_box(dump.final_frame_no);
            black_box(dump.final_file_index);
        });
    });

    group.throughput(Throughput::Bytes(oversized_bytes));
    group.bench_function("oversized_single_128mib_frame64", |b| {
        b.iter(|| {
            let dump = prepare_dump(
                oversized_dir.path().to_path_buf(),
                &empty_resume_state(),
                3,
                1,
                PREPARE_FRAME_TARGET_BYTES,
            )
            .unwrap();
            black_box(dump.frames.len());
            black_box(dump.final_frame_no);
            black_box(dump.final_file_index);
        });
    });

    group.finish();
}
